//! Backing-store integration: the transaction engine over a from-scratch
//! [`VersionStore`].
//!
//! `txn-db` is the transaction layer; the version store is a pluggable seam (the
//! roadmap names `lsm-db` as a natural fit, but the store itself is a consumer
//! concern). This test proves the seam is real by implementing a *complete,
//! independent* `VersionStore` — a single-locked `BTreeMap` of version chains,
//! sharing no code with the shipped `MemoryStore` — and showing the full
//! transaction semantics compose over it: snapshot isolation, read-your-own
//! writes, write-write conflict detection, and (under the feature) serializable
//! read-set validation.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use txn_db::{Db, Timestamp, TxnError, VersionStore, WriteEntry};

/// One key's versions, in ascending commit-timestamp order.
type Chain = Vec<(Timestamp, Option<Arc<[u8]>>)>;

/// A minimal version store: one lock over a map from key to its version chain.
/// Correct but unsharded — the point is that the trait alone is enough to back
/// the engine on an independent structure.
#[derive(Default)]
struct BTreeStore {
    chains: RwLock<BTreeMap<Arc<[u8]>, Chain>>,
}

fn visible(chain: Option<&Chain>, read_ts: Timestamp) -> Option<Arc<[u8]>> {
    let chain = chain?;
    let idx = chain
        .partition_point(|(ts, _)| *ts <= read_ts)
        .checked_sub(1)?;
    chain[idx].1.clone()
}

fn changed_since(chain: Option<&Chain>, read_ts: Timestamp) -> bool {
    matches!(chain.and_then(|c| c.last()), Some((ts, _)) if *ts > read_ts)
}

impl VersionStore for BTreeStore {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>, TxnError> {
        let chains = self.chains.read().unwrap_or_else(|p| p.into_inner());
        Ok(visible(chains.get(key), read_ts))
    }

    fn try_commit(
        &self,
        read_ts: Timestamp,
        commit_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<(), TxnError> {
        let mut chains = self.chains.write().unwrap_or_else(|p| p.into_inner());
        for (key, _) in &writes {
            if changed_since(chains.get(key.as_ref()), read_ts) {
                return Err(TxnError::conflict(key.len()));
            }
        }
        for key in reads {
            if changed_since(chains.get(key.as_ref()), read_ts) {
                return Err(TxnError::conflict(key.len()));
            }
        }
        for (key, value) in writes {
            chains.entry(key).or_default().push((commit_ts, value));
        }
        Ok(())
    }
}

fn db() -> Db<BTreeStore> {
    Db::with_store(BTreeStore::default())
}

#[test]
fn test_basic_read_write_over_custom_store() {
    let db = db();
    let mut tx = db.begin();
    tx.put(b"k".to_vec(), b"v".to_vec());
    tx.commit().unwrap();

    let tx = db.begin();
    assert_eq!(tx.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    assert_eq!(tx.get(b"absent").unwrap(), None);
}

#[test]
fn test_snapshot_isolation_over_custom_store() {
    let db = db();
    let mut tx = db.begin();
    tx.put(b"k".to_vec(), b"v1".to_vec());
    tx.commit().unwrap();

    let snap = db.snapshot();
    let mut tx = db.begin();
    tx.put(b"k".to_vec(), b"v2".to_vec());
    tx.commit().unwrap();

    // The held snapshot is unaffected by the later commit.
    assert_eq!(snap.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
    assert_eq!(
        db.snapshot().get(b"k").unwrap().as_deref(),
        Some(&b"v2"[..])
    );
}

#[test]
fn test_write_write_conflict_over_custom_store() {
    let db = db();
    let mut a = db.begin();
    let mut b = db.begin();
    a.put(b"k".to_vec(), b"a".to_vec());
    b.put(b"k".to_vec(), b"b".to_vec());

    assert!(a.commit().is_ok());
    let err = b.commit().expect_err("second committer must lose");
    assert!(err.is_retryable());

    let snap = db.snapshot();
    assert_eq!(snap.get(b"k").unwrap().as_deref(), Some(&b"a"[..]));
}

#[test]
fn test_disjoint_keys_commit_over_custom_store() {
    let db = db();
    let mut a = db.begin();
    let mut b = db.begin();
    a.put(b"a".to_vec(), b"1".to_vec());
    b.put(b"b".to_vec(), b"2".to_vec());
    assert!(a.commit().is_ok());
    assert!(b.commit().is_ok());
}

#[cfg(feature = "serializable")]
#[test]
fn test_serializable_write_skew_over_custom_store() {
    let db = db();
    let mut seed = db.begin();
    seed.put(b"x".to_vec(), vec![1]);
    seed.put(b"y".to_vec(), vec![1]);
    seed.commit().unwrap();

    let mut t1 = db.begin_serializable();
    let mut t2 = db.begin_serializable();
    let _ = (t1.get(b"x").unwrap(), t1.get(b"y").unwrap());
    let _ = (t2.get(b"x").unwrap(), t2.get(b"y").unwrap());
    t1.put(b"x".to_vec(), vec![0]);
    t2.put(b"y".to_vec(), vec![0]);

    assert!(t1.commit().is_ok());
    assert!(t2.commit().is_err()); // read set validation catches the skew
}
