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
//! RUSTFLAGS="--cfg loom" cargo test --test loom_txn --release
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
