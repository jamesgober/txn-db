//! Loom concurrency model checks for the commit path.
//!
//! These run only under `--cfg loom`, where the crate swaps its `std::sync`
//! primitives — the timestamp oracle's atomics, the watermark mutex, and the
//! store's per-shard locks — for loom's instrumented versions. Loom then
//! explores every legal interleaving of the operations below and fails if any
//! of them violates the asserted invariant. They are excluded from the normal
//! test run because exhaustive interleaving exploration is far slower than a
//! unit test.
//!
//! Each test captures its transactions' snapshots *before* spawning, so the two
//! commits genuinely race from the same starting point — otherwise loom could
//! schedule one commit entirely before the other begins, which is a valid
//! sequential history rather than a concurrency test.
//!
//! Run with:
//!
//! ```text
//! RUSTFLAGS="--cfg loom" cargo test --test loom_txn --release --features serializable
//! ```

#![cfg(loom)]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use txn_db::Db;

/// Two transactions take the same (empty) snapshot and both write the same key,
/// then commit concurrently. Whatever the interleaving of timestamp allocation,
/// shard locking, and watermark advancement, exactly one must commit and the
/// other must be rejected — never both — and the committed value must be the one
/// that ends up visible.
#[test]
fn loom_same_key_commit_has_one_winner() {
    loom::model(|| {
        let db = Db::new();

        // Both snapshots are taken now, before either commits.
        let mut a = db.begin();
        let mut b = db.begin();
        a.put(vec![7u8], vec![1u8]);
        b.put(vec![7u8], vec![2u8]);

        let handle = loom::thread::spawn(move || a.commit().is_ok());
        let b_committed = b.commit().is_ok();
        let a_committed = handle.join().unwrap();

        assert_eq!(
            usize::from(a_committed) + usize::from(b_committed),
            1,
            "exactly one writer of a contended key must win"
        );

        let value = db.snapshot().get(&[7u8]).unwrap();
        let value = value.expect("the winning write must be visible");
        assert!(value.as_ref() == [1u8] || value.as_ref() == [2u8]);
    });
}

/// A reader runs concurrently with garbage collection. Whatever the interleaving
/// of the snapshot's read (a shard read-lock) against the collector's pruning (a
/// shard write-lock plus the reader-registry and watermark bookkeeping), the
/// reader must always see the newest committed version — never a torn, empty, or
/// reclaimed-out-from-under-it state.
#[test]
fn loom_gc_never_disturbs_a_concurrent_reader() {
    loom::model(|| {
        let db = Db::new();
        // Two versions of one key, so the collector has an older version it is
        // free to reclaim.
        {
            let mut tx = db.begin();
            tx.put(vec![1u8], vec![1u8]);
            tx.commit().unwrap();
        }
        {
            let mut tx = db.begin();
            tx.put(vec![1u8], vec![2u8]);
            tx.commit().unwrap();
        }

        let reader = db.clone();
        let handle = loom::thread::spawn(move || {
            let snap = reader.snapshot();
            snap.get(&[1u8]).unwrap()
        });

        db.collect_garbage();

        let seen = handle.join().unwrap();
        assert_eq!(seen.as_deref(), Some(&[2u8][..]));
    });
}

/// Two transactions take the same snapshot and commit to disjoint keys
/// concurrently. Neither conflicts, so both must commit, and once both have
/// finished the read watermark must have advanced far enough that a fresh
/// snapshot observes both writes — proof that out-of-order commit completion
/// never makes a write timestamp visible before its data is applied.
#[test]
fn loom_disjoint_commits_both_succeed_and_are_visible() {
    loom::model(|| {
        let db = Db::new();

        let mut a = db.begin();
        let mut b = db.begin();
        a.put(vec![1u8], vec![10u8]);
        b.put(vec![2u8], vec![20u8]);

        let handle = loom::thread::spawn(move || a.commit().is_ok());
        let b_committed = b.commit().is_ok();
        let a_committed = handle.join().unwrap();

        assert!(
            a_committed && b_committed,
            "disjoint commits never conflict"
        );

        let snap = db.snapshot();
        assert_eq!(snap.get(&[1u8]).unwrap().as_deref(), Some(&[10u8][..]));
        assert_eq!(snap.get(&[2u8]).unwrap().as_deref(), Some(&[20u8][..]));
    });
}

/// The read-only anomaly schedule of Fekete, O'Neil and O'Neil, all three
/// transactions serializable: W withdraws from checking `X` with a penalty when
/// it sees `X + Y < 10`, D deposits into savings `Y`, and a read-only R reads
/// both while the two commits race. Whatever the interleaving, R's view must be
/// a state of the serial order the outcome implies, and that order must be the
/// commit-timestamp order. Commits take their timestamps under the store's
/// shard locks, which is what makes this hold.
#[cfg(feature = "serializable")]
#[test]
fn loom_reader_between_overlapping_serializable_commits_sees_a_serial_state() {
    fn int(v: Option<std::sync::Arc<[u8]>>) -> i8 {
        v.map_or(0, |b| b[0] as i8)
    }

    let mut builder = loom::model::Builder::new();
    builder.preemption_bound = Some(2);
    builder.check(|| {
        let db = Db::new();
        {
            let mut tx = db.begin();
            tx.put(vec![b'X'], vec![0u8]);
            tx.put(vec![b'Y'], vec![0u8]);
            tx.commit().unwrap();
        }

        // Both writers take their snapshots before either commits.
        let mut w = db.begin_serializable();
        let x = int(w.get(b"X").unwrap());
        let y = int(w.get(b"Y").unwrap());
        let nx = if x + y < 10 { x - 11 } else { x - 10 };
        w.put(vec![b'X'], vec![nx as u8]);
        let mut d = db.begin_serializable();
        let dy = int(d.get(b"Y").unwrap());
        d.put(vec![b'Y'], vec![(dy + 20) as u8]);

        let wh = loom::thread::spawn(move || w.commit().ok());
        let dh = loom::thread::spawn(move || d.commit().ok());

        let r = db.begin_serializable();
        let view = (int(r.get(b"X").unwrap()), int(r.get(b"Y").unwrap()));
        let _ = r.commit().unwrap();

        let w_ts = wh.join().unwrap();
        let d_ts = dh.join().unwrap().expect("D validates only its own write");
        let snap = db.snapshot();
        let fin = (int(snap.get(b"X").unwrap()), int(snap.get(b"Y").unwrap()));

        match w_ts {
            None => {
                assert_eq!(fin, (0, 20));
                assert!(view == (0, 0) || view == (0, 20), "{view:?}");
            }
            Some(w_ts) if fin.0 == -11 => {
                assert!(w_ts < d_ts);
                assert!([(0, 0), (-11, 0), (-11, 20)].contains(&view), "{view:?}");
            }
            Some(w_ts) => {
                assert!(d_ts < w_ts);
                assert!([(0, 0), (0, 20), (-10, 20)].contains(&view), "{view:?}");
            }
        }
    });
}
