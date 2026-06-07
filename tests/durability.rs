//! Crash-recovery tests for the durable commit log.
//!
//! These cover the v0.4 exit criteria: after a "crash" (dropping the database
//! and reopening from the same log), every committed transaction is durable and
//! nothing uncommitted is visible, and recovery from a log truncated at an
//! arbitrary point yields a consistent prefix of commits rather than a torn or
//! partial transaction.
//!
//! The whole file is gated on the `durability` feature.

#![cfg(feature = "durability")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::path::Path;

use proptest::prelude::*;
use txn_db::{Db, Timestamp};

/// Read a single-byte key as `Option<u8>` from a fresh snapshot.
fn read(db: &Db, key: &[u8]) -> Option<u8> {
    db.snapshot().get(key).unwrap().map(|bytes| bytes[0])
}

/// Commit one `key = value` write durably.
fn commit_one(db: &Db, key: u8, value: u8) {
    let mut tx = db.begin();
    tx.put(vec![key], vec![value]);
    tx.commit().unwrap();
}

#[test]
fn test_committed_transactions_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn.wal");

    {
        let db = Db::open(&path).unwrap();
        for i in 0..16u8 {
            commit_one(&db, i, i.wrapping_mul(3));
        }
    } // db dropped — simulates a clean shutdown / crash after commits.

    let db = Db::open(&path).unwrap();
    for i in 0..16u8 {
        assert_eq!(read(&db, &[i]), Some(i.wrapping_mul(3)));
    }
}

#[test]
fn test_uncommitted_work_is_not_durable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn.wal");

    {
        let db = Db::open(&path).unwrap();

        // A committed write.
        commit_one(&db, 1, 10);

        // A transaction that is rolled back.
        let mut rolled_back = db.begin();
        rolled_back.put(vec![2], vec![20]);
        rolled_back.rollback();

        // A transaction simply dropped without committing.
        let mut dropped = db.begin();
        dropped.put(vec![3], vec![30]);
        drop(dropped);
    }

    let db = Db::open(&path).unwrap();
    assert_eq!(read(&db, &[1]), Some(10)); // committed: durable
    assert_eq!(read(&db, &[2]), None); // rolled back: gone
    assert_eq!(read(&db, &[3]), None); // dropped: gone
}

#[test]
fn test_tombstones_survive_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn.wal");

    {
        let db = Db::open(&path).unwrap();
        commit_one(&db, 7, 99);
        let mut tx = db.begin();
        tx.delete(vec![7]);
        tx.commit().unwrap();
    }

    let db = Db::open(&path).unwrap();
    assert_eq!(read(&db, &[7]), None);
}

#[test]
fn test_timestamps_continue_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn.wal");

    let last = {
        let db = Db::open(&path).unwrap();
        commit_one(&db, 1, 1);
        commit_one(&db, 2, 2);
        db.last_committed()
    };
    assert!(last > Timestamp::ZERO);

    let db = Db::open(&path).unwrap();
    // Recovery restores the watermark to the highest committed timestamp.
    assert_eq!(db.last_committed(), last);

    // New commits continue strictly after it.
    let mut tx = db.begin();
    tx.put(vec![3], vec![3]);
    let next = tx.commit().unwrap();
    assert!(next > last);
}

#[test]
fn test_reopen_empty_log_is_empty_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn.wal");
    let db = Db::open(&path).unwrap();
    assert_eq!(db.last_committed(), Timestamp::ZERO);
    assert_eq!(read(&db, &[0]), None);
}

/// Open a database from a copy of `source` truncated to `len` bytes.
fn open_truncated(source: &Path, len: usize, dest: &Path) -> Db {
    let bytes = std::fs::read(source).unwrap();
    let len = len.min(bytes.len());
    std::fs::write(dest, &bytes[..len]).unwrap();
    Db::open(dest).unwrap()
}

proptest! {
    /// Write `n` commits to a log (single-threaded, so log order is commit
    /// order), then recover from the log truncated to an arbitrary length. The
    /// result must be a clean prefix: there is some `k` such that keys `0..k` are
    /// present with their committed values and keys `k..n` are absent — never a
    /// half-applied transaction, and never a panic.
    #[test]
    fn recovery_from_truncated_log_is_a_clean_prefix(
        n in 1u8..20,
        cut in any::<prop::sample::Index>(),
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("txn.wal");

        {
            let db = Db::open(&path).unwrap();
            for i in 0..n {
                commit_one(&db, i, i.wrapping_add(1));
            }
        }

        let total = std::fs::metadata(&path).unwrap().len() as usize;
        let len = cut.index(total + 1); // 0..=total

        let recovered = open_truncated(&path, len, &dir.path().join("recovered.wal"));

        // Find the prefix length: keys present, scanning from 0.
        let mut k = 0u8;
        while k < n && read(&recovered, &[k]) == Some(k.wrapping_add(1)) {
            k += 1;
        }
        // Everything from k onward must be absent — a true prefix.
        for i in k..n {
            prop_assert_eq!(read(&recovered, &[i]), None);
        }
    }
}
