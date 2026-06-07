//! Property tests for serializable isolation.
//!
//! These cover the v0.3 SSI exit criterion: no anomaly that serializability
//! forbids slips through. The headline case is write skew — two transactions
//! that each read a shared pair and write the half the other read, which plain
//! snapshot isolation allows and serializable isolation must reject.
//!
//! The whole file is gated on the `serializable` feature.

#![cfg(feature = "serializable")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use proptest::prelude::*;
use txn_db::Db;

const KEY_SPACE: u8 = 8;

proptest! {
    /// The write-skew schedule: two serializable transactions begin from the
    /// same snapshot, each reads two rows, and each writes one of them. Under
    /// serializability they cannot both commit — the second committer read a row
    /// the first changed, so its read-set validation must abort it.
    #[test]
    fn write_skew_never_lets_both_commit(
        x in 0..KEY_SPACE,
        y in 0..KEY_SPACE,
        vx in any::<u8>(),
        vy in any::<u8>(),
    ) {
        prop_assume!(x != y);

        let db = Db::new();
        let mut seed = db.begin();
        seed.put(vec![x], vec![1]);
        seed.put(vec![y], vec![1]);
        prop_assert!(seed.commit().is_ok());

        let mut t1 = db.begin_serializable();
        let mut t2 = db.begin_serializable();
        // Each reads both rows.
        let _ = t1.get(&[x])?;
        let _ = t1.get(&[y])?;
        let _ = t2.get(&[x])?;
        let _ = t2.get(&[y])?;
        // Each writes the row the other also read.
        t1.put(vec![x], vec![vx]);
        t2.put(vec![y], vec![vy]);

        let r1 = t1.commit();
        let r2 = t2.commit();

        // At least one must be rejected; both committing would be write skew.
        prop_assert!(!(r1.is_ok() && r2.is_ok()));
        // The first committer is the one that wins here.
        prop_assert!(r1.is_ok());
        let r2_err = r2.unwrap_err();
        prop_assert!(r2_err.is_retryable());
    }

    /// Two serializable transactions over disjoint key sets do not interfere:
    /// reading and writing unrelated rows, both commit.
    #[test]
    fn disjoint_serializable_transactions_both_commit(
        a in 0..KEY_SPACE,
        b in 0..KEY_SPACE,
        va in any::<u8>(),
        vb in any::<u8>(),
    ) {
        prop_assume!(a != b);

        let db = Db::new();

        let mut t1 = db.begin_serializable();
        let mut t2 = db.begin_serializable();
        let _ = t1.get(&[a])?;
        let _ = t2.get(&[b])?;
        t1.put(vec![a], vec![va]);
        t2.put(vec![b], vec![vb]);

        prop_assert!(t1.commit().is_ok());
        prop_assert!(t2.commit().is_ok());
    }

    /// A serializable transaction running with no concurrent committer always
    /// commits: its reads are still current, so validation passes.
    #[test]
    fn uncontended_serializable_transaction_commits(
        reads in prop::collection::vec(0..KEY_SPACE, 0..8),
        key in 0..KEY_SPACE,
        value in any::<u8>(),
    ) {
        let db = Db::new();
        let mut tx = db.begin_serializable();
        for k in reads {
            let _ = tx.get(&[k])?;
        }
        tx.put(vec![key], vec![value]);
        prop_assert!(tx.commit().is_ok());
    }

    /// A serializable read-only transaction commits trivially — it observed a
    /// consistent snapshot and writes nothing, so there is nothing to validate.
    #[test]
    fn serializable_read_only_always_commits(reads in prop::collection::vec(0..KEY_SPACE, 0..8)) {
        let db = Db::new();
        let mut seed = db.begin();
        seed.put(vec![0], vec![9]);
        prop_assert!(seed.commit().is_ok());

        // Concurrent writer commits between this snapshot and its commit.
        let snapshot_tx = {
            let tx = db.begin_serializable();
            for k in &reads {
                let _ = tx.get(&[*k])?;
            }
            tx
        };
        let mut writer = db.begin();
        writer.put(vec![0], vec![5]);
        prop_assert!(writer.commit().is_ok());

        // The read-only serializable transaction still commits.
        prop_assert!(snapshot_tx.commit().is_ok());
    }
}
