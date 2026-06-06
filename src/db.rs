//! The database handle and the commit coordinator behind it.
//!
//! [`Db`] is the Tier-1 entry point: construct one, [`begin`](Db::begin)
//! transactions against it, [`commit`](crate::Transaction::commit) them. A `Db`
//! is a cheap, clonable handle to shared state — clone it freely and hand a
//! clone to every thread that needs to read or write.
//!
//! The shared state itself lives in [`Inner`], which owns the version store and
//! the small amount of coordination that snapshot isolation needs: a monotonic
//! timestamp counter and a commit serialization point. Keeping that logic in
//! one place is deliberate — commit ordering and conflict detection are the
//! crate's correctness core, and they are easier to reason about when they are
//! not scattered across the read and write handles.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use crate::error::{Result, TxnError};
use crate::store::{MemoryStore, VersionStore, WriteEntry};
use crate::timestamp::Timestamp;
use crate::txn::{Snapshot, Transaction};

/// Shared, reference-counted state for one logical database.
///
/// A [`Db`] is a handle to an `Arc<Inner>`; every clone of the `Db`, every
/// [`Transaction`], and every [`Snapshot`] holds a clone of the same `Inner`,
/// so they all read and commit against one version store and one timestamp
/// sequence.
pub(crate) struct Inner<S: VersionStore> {
    /// The backing version store. Reads go straight to it; commits apply to it.
    pub(crate) store: S,
    /// The next commit timestamp to hand out. Only ever advances.
    next_ts: AtomicU64,
    /// The highest timestamp whose writes are fully applied and visible. A new
    /// transaction reads at this timestamp.
    last_committed: AtomicU64,
    /// Serializes the validate-then-apply commit critical section so two
    /// commits cannot both pass conflict detection and then overwrite each
    /// other. This single lock is the snapshot-isolation baseline; a sharded,
    /// lock-free commit path is a later roadmap phase.
    commit_lock: Mutex<()>,
}

impl<S: VersionStore> Inner<S> {
    fn new(store: S) -> Self {
        Inner {
            store,
            next_ts: AtomicU64::new(1),
            last_committed: AtomicU64::new(Timestamp::ZERO.get()),
            commit_lock: Mutex::new(()),
        }
    }

    /// The timestamp a transaction beginning now should read at.
    #[inline]
    fn read_ts(&self) -> Timestamp {
        Timestamp::from_raw(self.last_committed.load(Ordering::Acquire))
    }

    /// Validate and apply a transaction's writes under the commit lock.
    ///
    /// Holding `commit_lock` makes the sequence — check every written key for a
    /// conflicting newer commit, allocate a commit timestamp, apply the batch,
    /// publish the new high-water mark — atomic with respect to other commits.
    pub(crate) fn commit_writes(
        &self,
        read_ts: Timestamp,
        writes: std::collections::HashMap<Arc<[u8]>, Option<Arc<[u8]>>>,
    ) -> Result<Timestamp> {
        let _guard = self
            .commit_lock
            .lock()
            .unwrap_or_else(PoisonError::into_inner);

        // First-committer-wins: if any written key already has a version newer
        // than this transaction's snapshot, another transaction beat it to the
        // commit and this one must abort without applying anything.
        for key in writes.keys() {
            if let Some(latest) = self.store.latest_commit_ts(key)? {
                if latest > read_ts {
                    return Err(TxnError::conflict(key.len()));
                }
            }
        }

        let commit_ts = Timestamp::from_raw(self.next_ts.fetch_add(1, Ordering::Relaxed));
        let batch: Vec<WriteEntry> = writes.into_iter().collect();
        self.store.apply(commit_ts, batch)?;

        // Publish only after the writes are applied, so any transaction that
        // observes this timestamp also observes the data it stamps.
        self.last_committed
            .store(commit_ts.get(), Ordering::Release);
        Ok(commit_ts)
    }
}

/// A transactional, multi-version key-value database.
///
/// `Db` is the front door. [`Db::new`] gives you an in-memory database;
/// [`Db::with_store`] builds one over any [`VersionStore`]. From there the whole
/// common case is [`begin`](Db::begin) / [`get`](crate::Transaction::get) /
/// [`put`](crate::Transaction::put) / [`commit`](crate::Transaction::commit),
/// with [`snapshot`](Db::snapshot) for read-only point-in-time views.
///
/// A `Db` is a clonable handle over shared state, like an [`Arc`]. Cloning it
/// is cheap and every clone refers to the same database, so the idiomatic way
/// to use it across threads is to clone a handle per thread.
///
/// # Examples
///
/// The four-call common case:
///
/// ```
/// use txn_db::Db;
///
/// let db = Db::new();
///
/// let mut tx = db.begin();
/// tx.put(b"greeting".to_vec(), b"hei".to_vec());
/// tx.commit()?;
///
/// let tx = db.begin();
/// assert_eq!(tx.get(b"greeting")?.as_deref(), Some(&b"hei"[..]));
/// # Ok::<(), txn_db::TxnError>(())
/// ```
///
/// Sharing one database across threads:
///
/// ```
/// use std::thread;
/// use txn_db::Db;
///
/// let db = Db::new();
/// let handles: Vec<_> = (0..4u8)
///     .map(|i| {
///         let db = db.clone();
///         thread::spawn(move || {
///             let mut tx = db.begin();
///             tx.put(vec![i], vec![i]);
///             // Independent keys never conflict.
///             tx.commit().expect("commit");
///         })
///     })
///     .collect();
/// for h in handles {
///     h.join().expect("thread");
/// }
/// # Ok::<(), txn_db::TxnError>(())
/// ```
pub struct Db<S: VersionStore = MemoryStore> {
    inner: Arc<Inner<S>>,
}

impl Db<MemoryStore> {
    /// Create an empty in-memory database.
    ///
    /// This is the default configuration: a [`MemoryStore`] backing store, ready
    /// for [`begin`](Db::begin).
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// assert_eq!(db.last_committed(), txn_db::Timestamp::ZERO);
    /// ```
    #[must_use]
    pub fn new() -> Self {
        Db::with_store(MemoryStore::new())
    }
}

impl Default for Db<MemoryStore> {
    fn default() -> Self {
        Db::new()
    }
}

impl<S: VersionStore> Db<S> {
    /// Create a database over a custom [`VersionStore`].
    ///
    /// This is the Tier-3 seam: supply any backing store and the transaction
    /// semantics — snapshot isolation, read-your-own-writes, write-write
    /// conflict detection — compose on top of it unchanged.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::{Db, MemoryStore};
    ///
    /// let db = Db::with_store(MemoryStore::new());
    /// let mut tx = db.begin();
    /// tx.put(b"k".to_vec(), b"v".to_vec());
    /// tx.commit()?;
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    #[must_use]
    pub fn with_store(store: S) -> Self {
        Db {
            inner: Arc::new(Inner::new(store)),
        }
    }

    /// Begin a read-write transaction over the current state of the database.
    ///
    /// The transaction takes its snapshot at this moment: it reads as of the
    /// most recent commit and is unaffected by commits that happen afterward.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// let mut tx = db.begin();
    /// tx.put(b"k".to_vec(), b"v".to_vec());
    /// tx.commit()?;
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn begin(&self) -> Transaction<S> {
        Transaction::new(Arc::clone(&self.inner), self.inner.read_ts())
    }

    /// Take a read-only snapshot of the current state of the database.
    ///
    /// The returned [`Snapshot`] reads as of this instant and never changes,
    /// even as other transactions commit. Use it to read several keys at one
    /// consistent point in time without the overhead of a transaction.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// let snap = db.snapshot();
    /// assert_eq!(snap.get(b"k")?, None);
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    #[must_use]
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::new(Arc::clone(&self.inner), self.inner.read_ts())
    }

    /// The timestamp of the most recent successful commit.
    ///
    /// Returns [`Timestamp::ZERO`] for a database that has never been written.
    /// This is the timestamp a transaction beginning now would read at.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// assert_eq!(db.last_committed(), txn_db::Timestamp::ZERO);
    ///
    /// let mut tx = db.begin();
    /// tx.put(b"k".to_vec(), b"v".to_vec());
    /// let ts = tx.commit()?;
    /// assert_eq!(db.last_committed(), ts);
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    #[must_use]
    pub fn last_committed(&self) -> Timestamp {
        self.inner.read_ts()
    }
}

impl<S: VersionStore> Clone for Db<S> {
    /// Clone the handle, not the data: the clone shares the same underlying
    /// database.
    fn clone(&self) -> Self {
        Db {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_new_database_is_empty_at_zero() {
        let db = Db::new();
        assert_eq!(db.last_committed(), Timestamp::ZERO);
        assert_eq!(db.begin().get(b"k").unwrap(), None);
    }

    #[test]
    fn test_commit_makes_writes_visible_to_later_transactions() {
        let db = Db::new();
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), b"v".to_vec());
        let ts = tx.commit().unwrap();
        assert!(ts > Timestamp::ZERO);
        assert_eq!(db.begin().get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    }

    #[test]
    fn test_snapshot_is_isolated_from_later_commits() {
        let db = Db::new();
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), b"v1".to_vec());
        let _ = tx.commit().unwrap();

        let snap = db.snapshot();
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), b"v2".to_vec());
        let _ = tx.commit().unwrap();

        assert_eq!(snap.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
    }

    #[test]
    fn test_write_write_conflict_aborts_later_committer() {
        let db = Db::new();
        // Both transactions take the same empty snapshot.
        let mut a = db.begin();
        let mut b = db.begin();
        a.put(b"k".to_vec(), b"a".to_vec());
        b.put(b"k".to_vec(), b"b".to_vec());

        assert!(a.commit().is_ok());
        let err = b.commit().expect_err("second committer must lose");
        assert!(err.is_retryable());
        // First committer's value stands.
        assert_eq!(db.begin().get(b"k").unwrap().as_deref(), Some(&b"a"[..]));
    }

    #[test]
    fn test_disjoint_keys_do_not_conflict() {
        let db = Db::new();
        let mut a = db.begin();
        let mut b = db.begin();
        a.put(b"a".to_vec(), b"1".to_vec());
        b.put(b"b".to_vec(), b"2".to_vec());
        assert!(a.commit().is_ok());
        assert!(b.commit().is_ok());
    }

    #[test]
    fn test_read_only_commit_returns_snapshot_timestamp() {
        let db = Db::new();
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), b"v".to_vec());
        let ts = tx.commit().unwrap();

        let ro = db.begin();
        assert_eq!(ro.commit().unwrap(), ts);
    }

    #[test]
    fn test_rollback_discards_writes() {
        let db = Db::new();
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), b"v".to_vec());
        tx.rollback();
        assert_eq!(db.begin().get(b"k").unwrap(), None);
    }

    #[test]
    fn test_clone_shares_state() {
        let db = Db::new();
        let db2 = db.clone();
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), b"v".to_vec());
        let _ = tx.commit().unwrap();
        assert_eq!(db2.begin().get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
    }
}
