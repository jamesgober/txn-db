//! Property and concurrency tests for the snapshot-isolation contract.
//!
//! These cover the v0.2 exit criteria: snapshots stay consistent as other
//! transactions commit, lost updates are prevented under real contention, and a
//! write-write conflict aborts the later committer with a typed, retryable
//! error.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::thread;

use proptest::prelude::*;
use txn_db::{Db, TxnError};

/// One generated mutation against a small key space.
#[derive(Debug, Clone)]
enum Op {
    Put(u8, u8),
    Delete(u8),
}

/// Keys range over a small alphabet so conflicts and overwrites are dense.
const KEY_SPACE: u8 = 8;

fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (0..KEY_SPACE, any::<u8>()).prop_map(|(k, v)| Op::Put(k, v)),
        (0..KEY_SPACE).prop_map(Op::Delete),
    ]
}

/// Read a single-byte key out of the database as an `Option<u8>`.
fn read(db: &Db, key: u8) -> Option<u8> {
    db.snapshot().get(&[key]).unwrap().map(|bytes| bytes[0])
}

proptest! {
    /// After a sequence of single-operation transactions, the database matches a
    /// straightforward reference model. This pins down the visibility rule and
    /// tombstone handling across arbitrary interleavings of puts and deletes.
    #[test]
    fn committed_state_matches_reference_model(ops in prop::collection::vec(op_strategy(), 0..64)) {
        let db = Db::new();
        let mut model: BTreeMap<u8, u8> = BTreeMap::new();

        for op in ops {
            let mut tx = db.begin();
            match op {
                Op::Put(k, v) => {
                    tx.put(vec![k], vec![v]);
                    let _ = model.insert(k, v);
                }
                Op::Delete(k) => {
                    tx.delete(vec![k]);
                    let _ = model.remove(&k);
                }
            }
            // Each op is its own transaction over a fresh snapshot, so commits
            // never conflict.
            prop_assert!(tx.commit().is_ok());
        }

        for k in 0..KEY_SPACE {
            prop_assert_eq!(read(&db, k), model.get(&k).copied());
        }
    }

    /// A snapshot returns the same values no matter how much commits afterward.
    #[test]
    fn snapshot_is_stable_under_later_commits(
        base in prop::collection::vec((0..KEY_SPACE, any::<u8>()), 0..16),
        churn in prop::collection::vec((0..KEY_SPACE, any::<u8>()), 0..32),
    ) {
        let db = Db::new();

        // Establish a base state.
        let mut tx = db.begin();
        for (k, v) in &base {
            tx.put(vec![*k], vec![*v]);
        }
        prop_assert!(tx.commit().is_ok());

        // Capture a snapshot and record what it sees.
        let snap = db.snapshot();
        let observed: Vec<Option<u8>> = (0..KEY_SPACE)
            .map(|k| snap.get(&[k]).unwrap().map(|b| b[0]))
            .collect();

        // Churn the database with many more commits.
        for (k, v) in churn {
            let mut tx = db.begin();
            tx.put(vec![k], vec![v]);
            prop_assert!(tx.commit().is_ok());
        }

        // The snapshot is unmoved.
        for k in 0..KEY_SPACE {
            let again = snap.get(&[k]).unwrap().map(|b| b[0]);
            prop_assert_eq!(again, observed[k as usize]);
        }
    }

    /// Two transactions from the same snapshot that write the same key: the
    /// first to commit wins, the second is aborted with a retryable conflict,
    /// and the winner's value is what survives.
    #[test]
    fn write_write_conflict_aborts_the_later_committer(k in 0..KEY_SPACE, v1 in any::<u8>(), v2 in any::<u8>()) {
        let db = Db::new();

        let mut a = db.begin();
        let mut b = db.begin();
        a.put(vec![k], vec![v1]);
        b.put(vec![k], vec![v2]);

        prop_assert!(a.commit().is_ok());

        let err = b.commit().expect_err("second committer must be rejected");
        prop_assert!(err.is_retryable());
        let is_conflict = matches!(err, TxnError::Conflict { .. });
        prop_assert!(is_conflict);

        prop_assert_eq!(read(&db, k), Some(v1));
    }

    /// Read-your-own-writes: inside a transaction, reads reflect its own pending
    /// puts and deletes before it commits.
    #[test]
    fn transaction_reads_its_own_pending_writes(k in 0..KEY_SPACE, v in any::<u8>()) {
        let db = Db::new();
        let mut tx = db.begin();

        prop_assert_eq!(tx.get(&[k]).unwrap(), None);
        tx.put(vec![k], vec![v]);
        prop_assert_eq!(tx.get(&[k]).unwrap().map(|b| b[0]), Some(v));
        tx.delete(vec![k]);
        prop_assert_eq!(tx.get(&[k]).unwrap(), None);
    }
}

/// Under real multi-threaded contention, a read-modify-write loop that retries
/// on conflict never loses an update: the final counter equals the exact number
/// of increments issued. This is the lost-update guarantee the conflict check
/// exists to provide.
#[test]
fn concurrent_increments_never_lose_an_update() {
    const THREADS: u64 = 8;
    const PER_THREAD: u64 = 200;

    let db = Db::new();
    {
        let mut tx = db.begin();
        tx.put(b"counter".to_vec(), 0u64.to_le_bytes().to_vec());
        tx.commit().unwrap();
    }

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    loop {
                        let mut tx = db.begin();
                        let current = tx.get(b"counter").unwrap().map(read_u64).unwrap_or(0);
                        tx.put(b"counter".to_vec(), (current + 1).to_le_bytes().to_vec());
                        match tx.commit() {
                            Ok(_) => break,
                            Err(e) if e.is_retryable() => continue,
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().expect("worker thread panicked");
    }

    let total = read_u64(db.snapshot().get(b"counter").unwrap().unwrap());
    assert_eq!(total, THREADS * PER_THREAD);
}

/// A long-lived reader sees a stable snapshot while writers race ahead of it.
#[test]
fn snapshot_is_isolated_from_concurrent_writers() {
    let db = Db::new();
    {
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), vec![1]);
        tx.commit().unwrap();
    }

    let snap = db.snapshot();

    let writers: Vec<_> = (2u8..10)
        .map(|i| {
            let db = db.clone();
            thread::spawn(move || {
                let mut tx = db.begin();
                tx.put(b"k".to_vec(), vec![i]);
                // Independent of the conflict outcome; some of these race.
                let _ = tx.commit();
            })
        })
        .collect();
    for w in writers {
        w.join().expect("writer panicked");
    }

    // The snapshot still sees the value as of when it was taken.
    assert_eq!(snap.get(b"k").unwrap().as_deref(), Some(&[1u8][..]));
}

fn read_u64(bytes: Arc<[u8]>) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(buf)
}
