//! The version store: where committed versions live.
//!
//! `txn-db` is the transaction layer, not the storage layer. It owns
//! visibility, conflict detection, and commit ordering, but it delegates the
//! actual keeping of versioned bytes to a [`VersionStore`]. That trait is the
//! crate's Tier-3 seam: implement it over an LSM tree, a B-tree, a remote
//! service — anything that can keep multiple timestamped versions of a key —
//! and the transaction semantics compose on top unchanged.
//!
//! A [`MemoryStore`] ships for the common in-process case, for tests, and for
//! examples. It is the default backing store of [`Db::new`](crate::Db::new).
//!
//! ## The contract a store must uphold
//!
//! A correct [`VersionStore`] keeps, for each key, the full history of versions
//! it has been asked to apply, each tagged with the commit timestamp it was
//! applied at. Its three obligations are:
//!
//! - [`get`](VersionStore::get) returns the *newest* version whose commit
//!   timestamp is less than or equal to the caller's snapshot timestamp — the
//!   snapshot-read rule. A tombstone (a delete) at that position reads as
//!   "absent".
//! - [`latest_commit_ts`](VersionStore::latest_commit_ts) returns the timestamp
//!   of the most recent version of a key. The commit path uses it to detect
//!   write-write conflicts, so it must reflect every applied write.
//! - [`apply`](VersionStore::apply) installs a batch of versions at one commit
//!   timestamp. The database calls it with strictly increasing timestamps and
//!   never concurrently with itself, so an implementation may assume applied
//!   versions arrive in commit order.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};

use crate::error::Result;
use crate::timestamp::Timestamp;

/// One entry in a commit batch handed to [`VersionStore::apply`].
///
/// A key paired with the value to write at the commit timestamp (`Some`) or a
/// tombstone marking a delete (`None`).
pub type WriteEntry = (Arc<[u8]>, Option<Arc<[u8]>>);

/// A keeper of timestamped versions, the backend a [`Db`](crate::Db) is built on.
///
/// This is the extension point for plugging `txn-db` onto a real storage
/// engine. The transaction layer calls these three methods and supplies all of
/// the isolation logic itself; an implementation only has to store versions and
/// answer the snapshot-read query honestly. The three methods below state the
/// precise contract.
///
/// Implementations must be `Send + Sync`: a [`Db`](crate::Db) shares one store
/// across every thread that holds a clone of it.
///
/// # Examples
///
/// Driving the shipped [`MemoryStore`] directly through the trait:
///
/// ```
/// use std::sync::Arc;
/// use txn_db::{MemoryStore, Timestamp, VersionStore};
///
/// let store = MemoryStore::new();
/// let key: Arc<[u8]> = Arc::from(&b"k"[..]);
///
/// // Apply one version at commit timestamp 1.
/// store.apply(Timestamp::from_raw(1), vec![(key.clone(), Some(Arc::from(&b"v1"[..])))])?;
///
/// // A reader at timestamp 1 sees it; a reader at timestamp 0 does not.
/// assert_eq!(store.get(b"k", Timestamp::from_raw(1))?.as_deref(), Some(&b"v1"[..]));
/// assert_eq!(store.get(b"k", Timestamp::ZERO)?, None);
/// # Ok::<(), txn_db::TxnError>(())
/// ```
pub trait VersionStore: Send + Sync {
    /// Return the value of `key` visible at `read_ts`.
    ///
    /// The result is the value of the newest version of `key` whose commit
    /// timestamp is `<= read_ts`, or `None` if there is no such version or the
    /// newest visible version is a tombstone (the key was deleted as of
    /// `read_ts`).
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Store`](crate::TxnError::Store) if the backend fails
    /// to service the read. [`MemoryStore`] never fails.
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>>;

    /// Return the commit timestamp of the most recent version of `key`.
    ///
    /// Returns `None` if the key has never been written. The commit path uses
    /// this to decide whether a key was modified after a transaction's
    /// snapshot, so it must account for every version ever applied — including
    /// tombstones.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Store`](crate::TxnError::Store) if the backend fails.
    /// [`MemoryStore`] never fails.
    fn latest_commit_ts(&self, key: &[u8]) -> Result<Option<Timestamp>>;

    /// Install a batch of versions at `commit_ts`.
    ///
    /// Each entry is a key paired with either `Some(value)` (a write) or `None`
    /// (a tombstone marking a delete). The database guarantees that `apply` is
    /// called with strictly increasing `commit_ts` and is never run
    /// concurrently with another `apply` on the same store, so versions arrive
    /// in commit order.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Store`](crate::TxnError::Store) if the backend fails
    /// to persist the batch. [`MemoryStore`] never fails.
    fn apply(&self, commit_ts: Timestamp, writes: Vec<WriteEntry>) -> Result<()>;
}

/// One stored version of a key: the timestamp it became visible and its value.
///
/// A `value` of `None` is a tombstone — the key was deleted at `commit_ts`.
#[derive(Debug, Clone)]
struct Version {
    commit_ts: Timestamp,
    value: Option<Arc<[u8]>>,
}

/// An in-memory [`VersionStore`] backed by a hash map of version chains.
///
/// Each key maps to its versions in ascending commit-timestamp order, so a
/// snapshot read is a binary search for the newest version at or below the
/// snapshot timestamp. This is the default store of [`Db::new`](crate::Db::new)
/// and is well suited to caches, tests, and workloads that fit in memory.
///
/// `MemoryStore` is thread-safe and is meant to be shared: a [`Db`](crate::Db)
/// holds it behind an [`Arc`] and clones that handle to every thread. Versions
/// accumulate until garbage collection lands (a later roadmap phase), so a
/// long-lived store under heavy overwrite grows without bound for now.
///
/// # Examples
///
/// ```
/// use txn_db::{Db, MemoryStore};
///
/// // `Db::new()` uses a `MemoryStore`; this is the explicit form.
/// let db = Db::with_store(MemoryStore::new());
/// let mut tx = db.begin();
/// tx.put(b"hello".to_vec(), b"world".to_vec());
/// tx.commit()?;
/// # Ok::<(), txn_db::TxnError>(())
/// ```
#[derive(Debug, Default)]
pub struct MemoryStore {
    chains: RwLock<HashMap<Arc<[u8]>, Vec<Version>>>,
}

impl MemoryStore {
    /// Create an empty in-memory store.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::MemoryStore;
    ///
    /// let store = MemoryStore::new();
    /// # let _ = store;
    /// ```
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        MemoryStore {
            chains: RwLock::new(HashMap::new()),
        }
    }

    /// Number of distinct keys that have ever been written.
    ///
    /// Counts keys, not versions, and includes keys whose latest version is a
    /// tombstone. Primarily useful in tests and diagnostics.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::{Db, MemoryStore};
    ///
    /// let store = MemoryStore::new();
    /// assert_eq!(store.key_count(), 0);
    /// ```
    #[must_use]
    pub fn key_count(&self) -> usize {
        read_guard(&self.chains).len()
    }
}

impl VersionStore for MemoryStore {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>> {
        let chains = read_guard(&self.chains);
        let Some(versions) = chains.get(key) else {
            return Ok(None);
        };
        // Versions are kept sorted ascending by commit timestamp, so the newest
        // version visible at `read_ts` is the last one that is <= read_ts.
        let visible = versions.partition_point(|v| v.commit_ts <= read_ts);
        match visible.checked_sub(1).map(|i| &versions[i]) {
            Some(version) => Ok(version.value.clone()),
            None => Ok(None),
        }
    }

    fn latest_commit_ts(&self, key: &[u8]) -> Result<Option<Timestamp>> {
        let chains = read_guard(&self.chains);
        Ok(chains.get(key).and_then(|v| v.last()).map(|v| v.commit_ts))
    }

    fn apply(&self, commit_ts: Timestamp, writes: Vec<WriteEntry>) -> Result<()> {
        let mut chains = write_guard(&self.chains);
        for (key, value) in writes {
            chains
                .entry(key)
                .or_default()
                .push(Version { commit_ts, value });
        }
        Ok(())
    }
}

/// Take a read guard, recovering the data if a previous holder panicked.
///
/// The store's critical sections never panic, so poisoning can only originate
/// from a panic elsewhere while a guard was held. The protected map is still
/// structurally valid in that case, so recovering the guard is the resilient
/// choice and keeps the store usable rather than turning one panic into a
/// permanent failure.
#[inline]
fn read_guard<T>(lock: &RwLock<T>) -> std::sync::RwLockReadGuard<'_, T> {
    lock.read().unwrap_or_else(PoisonError::into_inner)
}

#[inline]
fn write_guard<T>(lock: &RwLock<T>) -> std::sync::RwLockWriteGuard<'_, T> {
    lock.write().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn k(b: &[u8]) -> Arc<[u8]> {
        Arc::from(b)
    }

    fn v(b: &[u8]) -> Option<Arc<[u8]>> {
        Some(Arc::from(b))
    }

    #[test]
    fn test_get_on_missing_key_returns_none() {
        let store = MemoryStore::new();
        assert_eq!(store.get(b"absent", Timestamp::from_raw(10)).unwrap(), None);
    }

    #[test]
    fn test_read_sees_only_versions_at_or_before_snapshot() {
        let store = MemoryStore::new();
        store
            .apply(Timestamp::from_raw(2), vec![(k(b"x"), v(b"a"))])
            .unwrap();
        store
            .apply(Timestamp::from_raw(4), vec![(k(b"x"), v(b"b"))])
            .unwrap();

        assert_eq!(store.get(b"x", Timestamp::from_raw(1)).unwrap(), None);
        assert_eq!(
            store.get(b"x", Timestamp::from_raw(2)).unwrap().as_deref(),
            Some(&b"a"[..])
        );
        assert_eq!(
            store.get(b"x", Timestamp::from_raw(3)).unwrap().as_deref(),
            Some(&b"a"[..])
        );
        assert_eq!(
            store.get(b"x", Timestamp::from_raw(4)).unwrap().as_deref(),
            Some(&b"b"[..])
        );
        assert_eq!(
            store.get(b"x", Timestamp::from_raw(99)).unwrap().as_deref(),
            Some(&b"b"[..])
        );
    }

    #[test]
    fn test_tombstone_reads_as_absent() {
        let store = MemoryStore::new();
        store
            .apply(Timestamp::from_raw(1), vec![(k(b"x"), v(b"a"))])
            .unwrap();
        store
            .apply(Timestamp::from_raw(2), vec![(k(b"x"), None)])
            .unwrap();

        assert_eq!(
            store.get(b"x", Timestamp::from_raw(1)).unwrap().as_deref(),
            Some(&b"a"[..])
        );
        assert_eq!(store.get(b"x", Timestamp::from_raw(2)).unwrap(), None);
    }

    #[test]
    fn test_latest_commit_ts_tracks_newest_write() {
        let store = MemoryStore::new();
        assert_eq!(store.latest_commit_ts(b"x").unwrap(), None);
        store
            .apply(Timestamp::from_raw(3), vec![(k(b"x"), v(b"a"))])
            .unwrap();
        store
            .apply(Timestamp::from_raw(7), vec![(k(b"x"), None)])
            .unwrap();
        assert_eq!(
            store.latest_commit_ts(b"x").unwrap(),
            Some(Timestamp::from_raw(7))
        );
    }

    #[test]
    fn test_key_count_counts_distinct_keys() {
        let store = MemoryStore::new();
        store
            .apply(
                Timestamp::from_raw(1),
                vec![(k(b"a"), v(b"1")), (k(b"b"), v(b"2"))],
            )
            .unwrap();
        store
            .apply(Timestamp::from_raw(2), vec![(k(b"a"), v(b"3"))])
            .unwrap();
        assert_eq!(store.key_count(), 2);
    }
}
