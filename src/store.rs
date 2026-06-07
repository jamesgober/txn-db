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
//! A correct [`VersionStore`] keeps, for each key, the history of versions it
//! has been asked to apply, each tagged with the commit timestamp it was applied
//! at. Its two obligations are:
//!
//! - [`get`](VersionStore::get) returns the *newest* version whose commit
//!   timestamp is less than or equal to the caller's snapshot timestamp — the
//!   snapshot-read rule. A tombstone (a delete) at that position reads as
//!   "absent".
//! - [`try_commit`](VersionStore::try_commit) validates a transaction's read and
//!   write sets against its snapshot and, if nothing conflicts, installs its
//!   writes at the commit timestamp — atomically with respect to other commits
//!   touching the same keys. This single method is what makes the store the
//!   serialization point for concurrent commits.
//!
//! ## Sharding
//!
//! [`MemoryStore`] partitions keys across independent shards, each with its own
//! lock. Reads and commits that touch disjoint shards proceed without
//! contending; a commit locks only the shards its keys fall in, in a fixed order
//! so concurrent commits cannot deadlock. This is the sharded commit path the
//! single global commit lock of the foundation release grew into.

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::{Result, TxnError};
use crate::sync::{self, RwLock, RwLockWriteGuard};
use crate::timestamp::Timestamp;

/// One entry in a commit batch handed to [`VersionStore::try_commit`].
///
/// A key paired with the value to write at the commit timestamp (`Some`) or a
/// tombstone marking a delete (`None`).
pub type WriteEntry = (Arc<[u8]>, Option<Arc<[u8]>>);

/// Default number of shards. A power of two so the shard index is a mask, not a
/// division. Sixteen spreads contention well for in-process workloads without
/// the per-commit cost of locking a long list of shards. Loom builds use far
/// fewer to keep the interleaving search tractable.
#[cfg(not(loom))]
const DEFAULT_SHARDS: usize = 16;
#[cfg(loom)]
const DEFAULT_SHARDS: usize = 2;

/// A keeper of timestamped versions, the backend a [`Db`](crate::Db) is built on.
///
/// This is the extension point for plugging `txn-db` onto a real storage
/// engine. The transaction layer supplies the snapshot timestamps and the read
/// and write sets; the store stores versions and enforces, atomically, that a
/// commit only lands when nothing it depends on has changed. The two methods
/// below state the precise contract.
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
/// // Commit one version at timestamp 1 (snapshot 0, no reads to validate).
/// store.try_commit(
///     Timestamp::ZERO,
///     Timestamp::from_raw(1),
///     vec![(key.clone(), Some(Arc::from(&b"v1"[..])))],
///     &[],
/// )?;
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

    /// Validate a transaction and, if it does not conflict, apply its writes.
    ///
    /// The store must perform the following as one step, atomic with respect to
    /// any other `try_commit` that touches an overlapping key:
    ///
    /// 1. **Validate.** For every key in `writes` and every key in `reads`,
    ///    check that the key has no version with a commit timestamp greater than
    ///    `read_ts` — that is, that nothing the transaction wrote or read has
    ///    changed since its snapshot. `reads` is empty for snapshot-isolation
    ///    transactions and carries the read set for serializable ones.
    /// 2. **Apply.** If validation passes, install each write in `writes` as a
    ///    new version stamped `commit_ts` (`Some` is a value, `None` a
    ///    tombstone). The database guarantees `commit_ts` is unique and that
    ///    timestamps are handed out in increasing order.
    ///
    /// If any key fails validation, the store applies nothing and reports the
    /// conflict.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Conflict`](crate::TxnError::Conflict) if validation
    /// fails; no writes are applied. Returns
    /// [`TxnError::Store`](crate::TxnError::Store) if the backend fails to apply
    /// the batch.
    fn try_commit(
        &self,
        read_ts: Timestamp,
        commit_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<()>;
}

/// One stored version of a key: the timestamp it became visible and its value.
///
/// A `value` of `None` is a tombstone — the key was deleted at `commit_ts`.
#[derive(Debug, Clone)]
struct Version {
    commit_ts: Timestamp,
    value: Option<Arc<[u8]>>,
}

/// One shard's map from key to its version chain, kept in ascending
/// commit-timestamp order.
type Chains = HashMap<Arc<[u8]>, Vec<Version>>;

/// One shard's slice of the keyspace.
struct Shard {
    chains: RwLock<Chains>,
}

/// An in-memory [`VersionStore`] that shards the keyspace for concurrency.
///
/// Each key is hashed to one of a fixed number of shards; each shard holds its
/// keys' version chains behind its own reader-writer lock. Reads lock a single
/// shard; a commit locks only the shards its keys fall in. Commits to disjoint
/// shards therefore run in parallel, and the snapshot read of a key is a binary
/// search within its shard for the newest version at or below the snapshot
/// timestamp.
///
/// This is the default store of [`Db::new`](crate::Db::new) and suits caches,
/// tests, and workloads that fit in memory. Versions accumulate until garbage
/// collection lands (a later roadmap phase), so a long-lived store under heavy
/// overwrite grows without bound for now.
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
pub struct MemoryStore {
    shards: Box<[Shard]>,
    /// `shard_count - 1`; ANDed with a key hash to pick a shard.
    mask: usize,
}

impl Default for MemoryStore {
    fn default() -> Self {
        MemoryStore::new()
    }
}

impl MemoryStore {
    /// Create an empty in-memory store with the default shard count.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::MemoryStore;
    ///
    /// let store = MemoryStore::new();
    /// # let _ = store;
    /// ```
    #[must_use]
    pub fn new() -> Self {
        MemoryStore::with_shards(DEFAULT_SHARDS)
    }

    /// Create an empty store with a specific number of shards.
    ///
    /// `shards` is rounded up to a power of two (and at least one). More shards
    /// reduce contention between commits that touch unrelated keys, at the cost
    /// of a larger fixed footprint. The default of [`MemoryStore::new`] suits
    /// most workloads; tune this only with a benchmark in hand.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::MemoryStore;
    ///
    /// let store = MemoryStore::with_shards(64);
    /// # let _ = store;
    /// ```
    #[must_use]
    pub fn with_shards(shards: usize) -> Self {
        let count = shards.max(1).next_power_of_two();
        let shards = (0..count)
            .map(|_| Shard {
                chains: RwLock::new(HashMap::new()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        MemoryStore {
            shards,
            mask: count - 1,
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
    /// use txn_db::MemoryStore;
    ///
    /// let store = MemoryStore::new();
    /// assert_eq!(store.key_count(), 0);
    /// ```
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| sync::read(&shard.chains).len())
            .sum()
    }

    /// The shard a key belongs to.
    #[inline]
    fn shard_of(&self, key: &[u8]) -> usize {
        (hash_key(key) as usize) & self.mask
    }
}

impl VersionStore for MemoryStore {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>> {
        let shard = &self.shards[self.shard_of(key)];
        let chains = sync::read(&shard.chains);
        Ok(visible_value(chains.get(key), read_ts))
    }

    fn try_commit(
        &self,
        read_ts: Timestamp,
        commit_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<()> {
        // Shard of every touched key, computed once.
        let write_shards: Vec<usize> = writes.iter().map(|(k, _)| self.shard_of(k)).collect();
        let read_shards: Vec<usize> = reads.iter().map(|k| self.shard_of(k)).collect();

        // The distinct shards to lock, in ascending order so concurrent commits
        // acquire shared shards in the same sequence and cannot deadlock.
        let mut to_lock: Vec<usize> = write_shards
            .iter()
            .copied()
            .chain(read_shards.iter().copied())
            .collect();
        to_lock.sort_unstable();
        to_lock.dedup();

        let mut guards: Vec<RwLockWriteGuard<'_, Chains>> = Vec::with_capacity(to_lock.len());
        for &shard in &to_lock {
            guards.push(sync::write(&self.shards[shard].chains));
        }

        // Validate the write set, then the read set: abort if any touched key
        // gained a version after the transaction's snapshot.
        for (entry, &shard) in writes.iter().zip(&write_shards) {
            if let Ok(pos) = to_lock.binary_search(&shard) {
                if newer_than(guards[pos].get(entry.0.as_ref()), read_ts) {
                    return Err(TxnError::conflict(entry.0.len()));
                }
            }
        }
        for (key, &shard) in reads.iter().zip(&read_shards) {
            if let Ok(pos) = to_lock.binary_search(&shard) {
                if newer_than(guards[pos].get(key.as_ref()), read_ts) {
                    return Err(TxnError::conflict(key.len()));
                }
            }
        }

        // Apply: append a new version for each write under the held locks.
        for ((key, value), &shard) in writes.into_iter().zip(&write_shards) {
            if let Ok(pos) = to_lock.binary_search(&shard) {
                guards[pos]
                    .entry(key)
                    .or_default()
                    .push(Version { commit_ts, value });
            }
        }
        Ok(())
    }
}

/// Whether `key`'s newest version (if any) was committed after `read_ts` — the
/// condition that makes a commit conflict.
#[inline]
fn newer_than(versions: Option<&Vec<Version>>, read_ts: Timestamp) -> bool {
    matches!(versions.and_then(|v| v.last()), Some(v) if v.commit_ts > read_ts)
}

/// The value of the newest version at or below `read_ts`, or `None` if there is
/// none or it is a tombstone.
#[inline]
fn visible_value(versions: Option<&Vec<Version>>, read_ts: Timestamp) -> Option<Arc<[u8]>> {
    let versions = versions?;
    // Versions are ascending by commit timestamp; the newest visible one is the
    // last entry whose timestamp is `<= read_ts`.
    let visible = versions.partition_point(|v| v.commit_ts <= read_ts);
    let idx = visible.checked_sub(1)?;
    versions[idx].value.clone()
}

/// FNV-1a hash of a key, used only to pick a shard. A non-cryptographic spread
/// is all the shard index needs.
#[inline]
fn hash_key(key: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325;
    for &byte in key {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn k(b: &[u8]) -> Arc<[u8]> {
        Arc::from(b)
    }

    fn commit(store: &MemoryStore, ts: u64, writes: Vec<WriteEntry>) {
        store
            .try_commit(
                Timestamp::from_raw(ts - 1),
                Timestamp::from_raw(ts),
                writes,
                &[],
            )
            .expect("commit");
    }

    #[test]
    fn test_get_on_missing_key_returns_none() {
        let store = MemoryStore::new();
        assert_eq!(store.get(b"absent", Timestamp::from_raw(10)).unwrap(), None);
    }

    #[test]
    fn test_read_sees_only_versions_at_or_before_snapshot() {
        let store = MemoryStore::new();
        commit(&store, 2, vec![(k(b"x"), Some(k(b"a")))]);
        commit(&store, 4, vec![(k(b"x"), Some(k(b"b")))]);

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
        commit(&store, 1, vec![(k(b"x"), Some(k(b"a")))]);
        commit(&store, 2, vec![(k(b"x"), None)]);

        assert_eq!(
            store.get(b"x", Timestamp::from_raw(1)).unwrap().as_deref(),
            Some(&b"a"[..])
        );
        assert_eq!(store.get(b"x", Timestamp::from_raw(2)).unwrap(), None);
    }

    #[test]
    fn test_write_write_conflict_is_detected() {
        let store = MemoryStore::new();
        commit(&store, 5, vec![(k(b"x"), Some(k(b"a")))]);

        // A transaction whose snapshot predates the existing version conflicts.
        let err = store
            .try_commit(
                Timestamp::from_raw(4),
                Timestamp::from_raw(6),
                vec![(k(b"x"), Some(k(b"b")))],
                &[],
            )
            .unwrap_err();
        assert!(matches!(err, TxnError::Conflict { .. }));
        // Nothing was applied.
        assert_eq!(
            store.get(b"x", Timestamp::from_raw(99)).unwrap().as_deref(),
            Some(&b"a"[..])
        );
    }

    #[test]
    fn test_read_set_validation_detects_skew() {
        let store = MemoryStore::new();
        commit(&store, 5, vec![(k(b"y"), Some(k(b"1")))]);

        // Snapshot 4, write x, but read y which changed at ts 5 -> conflict.
        let err = store
            .try_commit(
                Timestamp::from_raw(4),
                Timestamp::from_raw(6),
                vec![(k(b"x"), Some(k(b"a")))],
                &[k(b"y")],
            )
            .unwrap_err();
        assert!(matches!(err, TxnError::Conflict { .. }));
    }

    #[test]
    fn test_multi_shard_commit_applies_all_keys() {
        let store = MemoryStore::with_shards(8);
        let writes: Vec<WriteEntry> = (0u8..32).map(|i| (k(&[i]), Some(k(&[i])))).collect();
        commit(&store, 1, writes);
        for i in 0u8..32 {
            assert_eq!(
                store.get(&[i], Timestamp::from_raw(1)).unwrap().as_deref(),
                Some(&[i][..])
            );
        }
        assert_eq!(store.key_count(), 32);
    }

    #[test]
    fn test_with_shards_rounds_up_to_power_of_two() {
        let store = MemoryStore::with_shards(5);
        assert_eq!(store.shards.len(), 8);
        assert_eq!(store.mask, 7);
    }
}
