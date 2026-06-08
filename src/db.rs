//! The database handle and the commit coordinator behind it.
//!
//! [`Db`] is the Tier-1 entry point: construct one, [`begin`](Db::begin)
//! transactions against it, [`commit`](crate::Transaction::commit) them. A `Db`
//! is a cheap, clonable handle to shared state — clone it freely and hand a
//! clone to every thread that needs to read or write.
//!
//! The shared state itself lives in [`Inner`], which owns the version store and
//! the [`Oracle`](crate::oracle::Oracle) that allocates timestamps and tracks
//! the read watermark. Commit coordination is split deliberately: the oracle
//! hands out timestamps lock-free, and the version store is the serialization
//! point that validates and applies each commit atomically. The single global
//! commit lock of the foundation release is gone.

use std::sync::Arc;

use crate::error::Result;
use crate::oracle::Oracle;
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
    /// The backing version store. Reads go to it; commits validate and apply
    /// through it.
    pub(crate) store: S,
    /// Allocates timestamps and tracks the consistent-read watermark.
    oracle: Oracle,
    /// The durable commit log, present only for a database opened with
    /// [`Db::open`]. `None` for an in-memory database.
    #[cfg(feature = "durability")]
    log: Option<crate::durable::CommitLog>,
}

impl<S: VersionStore> Inner<S> {
    fn new(store: S) -> Self {
        Inner {
            store,
            oracle: Oracle::new(),
            #[cfg(feature = "durability")]
            log: None,
        }
    }

    /// The timestamp a transaction beginning now should read at.
    #[inline]
    fn read_ts(&self) -> Timestamp {
        self.oracle.read_ts()
    }

    /// Register a live reader and return its read timestamp.
    #[inline]
    pub(crate) fn begin_reader(&self) -> Timestamp {
        self.oracle.begin_reader()
    }

    /// Unregister a reader that began at `read_ts`.
    #[inline]
    pub(crate) fn end_reader(&self, read_ts: Timestamp) {
        self.oracle.end_reader(read_ts);
    }

    /// Reclaim versions no live reader can observe, returning the count removed.
    fn collect_garbage(&self) -> usize {
        self.store.collect_garbage(self.oracle.low_watermark())
    }

    /// Allocate a commit timestamp, validate-and-apply through the store, then
    /// release the timestamp into the watermark.
    ///
    /// The timestamp is reported to the oracle on both outcomes — a successful
    /// commit and a rejected one — so a conflict never stalls the read watermark
    /// behind the timestamp it consumed.
    pub(crate) fn commit_writes(
        &self,
        read_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<Timestamp> {
        let commit_ts = self.oracle.alloc_commit_ts();

        // Encode the durable record before the write set is consumed by the
        // store. No cost for an in-memory database (no log).
        #[cfg(feature = "durability")]
        let record = self
            .log
            .as_ref()
            .map(|_| crate::durable::encode_for_log(commit_ts, &writes));

        let outcome = self.store.try_commit(read_ts, commit_ts, writes, reads);

        // Make the commit durable before it is acknowledged. The validate-and-
        // apply has already happened in memory but is not yet visible — the
        // watermark only advances at `commit_done` below — so a crash before the
        // sync completes leaves a transaction that was never acknowledged and is
        // recovered as absent.
        #[cfg(feature = "durability")]
        if outcome.is_ok() {
            if let (Some(log), Some(record)) = (self.log.as_ref(), record) {
                if let Err(err) = log.append_committed(&record) {
                    self.oracle.commit_done(commit_ts);
                    return Err(err);
                }
            }
        }

        self.oracle.commit_done(commit_ts);
        outcome.map(|()| commit_ts)
    }

    /// Build the shared inner state for a database recovered from a durable log.
    #[cfg(feature = "durability")]
    fn recovered(store: S, oracle: Oracle, log: crate::durable::CommitLog) -> Self {
        Inner {
            store,
            oracle,
            log: Some(log),
        }
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
/// Transactions default to **snapshot isolation**. With the `serializable`
/// feature enabled, `begin_serializable` starts a transaction whose read set is
/// validated at commit, rejecting write skew and the other anomalies snapshot
/// isolation permits.
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

    /// Open a durable database backed by a write-ahead log at `path`, replaying
    /// any committed transactions already in the log.
    ///
    /// Every transaction committed against the returned database appends its
    /// record to the log and syncs it before [`commit`](crate::Transaction::commit)
    /// returns, so an acknowledged commit survives a crash. On open, the log is
    /// replayed: each committed transaction is reinstated, and a transaction that
    /// never reached the log — because it aborted, or because the process crashed
    /// before its record was made durable — is simply absent. The recovered data
    /// lives in memory; the log is the durable record from which it is rebuilt.
    ///
    /// Available with the `durability` feature.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Durability`](crate::TxnError::Durability) if the log
    /// cannot be opened or a record read back from it does not decode.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "durability")]
    /// # {
    /// # let dir = tempfile::tempdir().expect("tempdir");
    /// # let path = dir.path().join("txn.wal");
    /// use txn_db::Db;
    ///
    /// // Commit, then drop the database.
    /// {
    ///     let db = Db::open(&path)?;
    ///     let mut tx = db.begin();
    ///     tx.put(b"k".to_vec(), b"v".to_vec());
    ///     tx.commit()?;
    /// }
    ///
    /// // Reopening replays the log: the committed write is still there.
    /// let db = Db::open(&path)?;
    /// assert_eq!(db.begin().get(b"k")?.as_deref(), Some(&b"v"[..]));
    /// # }
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    #[cfg(feature = "durability")]
    #[cfg_attr(docsrs, doc(cfg(feature = "durability")))]
    pub fn open(path: impl AsRef<std::path::Path>) -> Result<Db<MemoryStore>> {
        let (log, mut recovered) = crate::durable::CommitLog::open(path)?;

        // Replay in ascending commit-timestamp order; records may sit in the log
        // out of that order because commits append after applying, concurrently.
        recovered.sort_by_key(|commit| commit.commit_ts);

        let store = MemoryStore::new();
        let mut highest = Timestamp::ZERO;
        for commit in recovered {
            highest = highest.max(commit.commit_ts);
            store.install_recovered(commit.commit_ts, commit.writes);
        }

        Ok(Db {
            inner: Arc::new(Inner::recovered(store, Oracle::recovered(highest), log)),
        })
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
    /// semantics — snapshot isolation, read-your-own-writes, conflict detection
    /// — compose on top of it unchanged.
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

    /// Begin a snapshot-isolation transaction over the current state.
    ///
    /// The transaction takes its snapshot at this moment: it reads as of the
    /// most recent commit and is unaffected by commits that happen afterward.
    /// Its writes are checked for write-write conflicts at commit, but its reads
    /// are not validated — use `begin_serializable` (with the `serializable`
    /// feature) when you need serializability.
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
        Transaction::new(Arc::clone(&self.inner), false)
    }

    /// Begin a serializable transaction over the current state.
    ///
    /// A serializable transaction tracks every key it reads and, at commit,
    /// validates that none of them changed since its snapshot — in addition to
    /// the write-write check every transaction gets. That read-set validation is
    /// what rejects write skew and the read-only anomaly that plain snapshot
    /// isolation permits, giving serializable behavior for the transactions that
    /// commit writes. A serializable transaction that writes nothing commits
    /// trivially, exactly like a read-only snapshot.
    ///
    /// Available with the `serializable` feature. Snapshot isolation remains the
    /// default and is unaffected.
    ///
    /// # Examples
    ///
    /// ```
    /// # #[cfg(feature = "serializable")]
    /// # {
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// // Seed two rows that an invariant ties together.
    /// let mut tx = db.begin();
    /// tx.put(b"on_call:alice".to_vec(), vec![1]);
    /// tx.put(b"on_call:bob".to_vec(), vec![1]);
    /// tx.commit()?;
    ///
    /// // A serializable transaction validates the rows it read at commit.
    /// let mut tx = db.begin_serializable();
    /// let _alice = tx.get(b"on_call:alice")?;
    /// let _bob = tx.get(b"on_call:bob")?;
    /// tx.put(b"on_call:alice".to_vec(), vec![0]);
    /// tx.commit()?;
    /// # }
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    #[cfg(feature = "serializable")]
    #[cfg_attr(docsrs, doc(cfg(feature = "serializable")))]
    pub fn begin_serializable(&self) -> Transaction<S> {
        Transaction::new(Arc::clone(&self.inner), true)
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
    pub fn snapshot(&self) -> Snapshot<S> {
        Snapshot::new(Arc::clone(&self.inner))
    }

    /// The timestamp of the most recent commit visible to a new transaction.
    ///
    /// Returns [`Timestamp::ZERO`] for a database that has never been written.
    /// This is the read watermark: the timestamp a transaction beginning now
    /// would read at.
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

    /// Reclaim versions that no live transaction or snapshot can observe,
    /// returning how many were removed.
    ///
    /// `txn-db` keeps every version of a key so that an in-flight reader sees a
    /// stable snapshot. Once no live reader can observe an old version — because
    /// every active transaction and snapshot reads at a timestamp newer than a
    /// later version of that key — the old one is unreachable and this reclaims
    /// it. A key deleted before the oldest live reader's snapshot is dropped
    /// entirely.
    ///
    /// Call it periodically, or after retiring long-running snapshots, to bound
    /// memory. It is safe to call at any time and from any thread: a version a
    /// live reader can still reach is never reclaimed. With the default
    /// in-memory store this prunes the version chains; a custom
    /// [`VersionStore`](crate::VersionStore) that keeps no history can leave the
    /// default no-op in place.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// // Overwrite the same key several times.
    /// for v in 0..5u8 {
    ///     let mut tx = db.begin();
    ///     tx.put(b"k".to_vec(), vec![v]);
    ///     tx.commit()?;
    /// }
    ///
    /// // No snapshot is held, so only the newest version need be kept.
    /// let reclaimed = db.collect_garbage();
    /// assert!(reclaimed > 0);
    /// assert_eq!(db.begin().get(b"k")?.as_deref(), Some(&[4u8][..]));
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn collect_garbage(&self) -> usize {
        self.inner.collect_garbage()
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

#[cfg(all(test, not(loom)))]
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
        let mut a = db.begin();
        let mut b = db.begin();
        a.put(b"k".to_vec(), b"a".to_vec());
        b.put(b"k".to_vec(), b"b".to_vec());

        assert!(a.commit().is_ok());
        let err = b.commit().expect_err("second committer must lose");
        assert!(err.is_retryable());
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
    fn test_gc_reclaims_when_no_reader_is_held() {
        let db = Db::new();
        for v in 0..5u8 {
            let mut tx = db.begin();
            tx.put(b"k".to_vec(), vec![v]);
            let _ = tx.commit().unwrap();
        }
        let reclaimed = db.collect_garbage();
        assert!(reclaimed > 0);
        assert_eq!(db.begin().get(b"k").unwrap().as_deref(), Some(&[4u8][..]));
    }

    #[test]
    fn test_held_snapshot_pins_gc() {
        let db = Db::new();
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), vec![1]);
        let _ = tx.commit().unwrap();

        // Hold a snapshot of this state, then overwrite the key.
        let snap = db.snapshot();
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), vec![2]);
        let _ = tx.commit().unwrap();

        // GC must not reclaim the version the held snapshot still observes.
        let _ = db.collect_garbage();
        assert_eq!(snap.get(b"k").unwrap().as_deref(), Some(&[1u8][..]));

        // Once the snapshot is dropped, the old version becomes reclaimable.
        drop(snap);
        let reclaimed = db.collect_garbage();
        assert!(reclaimed > 0);
        assert_eq!(db.begin().get(b"k").unwrap().as_deref(), Some(&[2u8][..]));
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

    #[cfg(feature = "serializable")]
    #[test]
    fn test_serializable_rejects_write_skew() {
        let db = Db::new();
        let mut seed = db.begin();
        seed.put(b"x".to_vec(), vec![1]);
        seed.put(b"y".to_vec(), vec![1]);
        let _ = seed.commit().unwrap();

        // Two serializable transactions from the same snapshot each read both
        // rows and write the one the other read.
        let mut t1 = db.begin_serializable();
        let mut t2 = db.begin_serializable();
        let _ = t1.get(b"x").unwrap();
        let _ = t1.get(b"y").unwrap();
        let _ = t2.get(b"x").unwrap();
        let _ = t2.get(b"y").unwrap();
        t1.put(b"x".to_vec(), vec![0]);
        t2.put(b"y".to_vec(), vec![0]);

        assert!(t1.commit().is_ok());
        // t2 read x, which t1 changed -> serializable validation aborts it.
        let err = t2.commit().expect_err("write skew must be rejected");
        assert!(err.is_retryable());
    }

    #[cfg(feature = "serializable")]
    #[test]
    fn test_snapshot_txn_allows_write_skew() {
        let db = Db::new();
        let mut seed = db.begin();
        seed.put(b"x".to_vec(), vec![1]);
        seed.put(b"y".to_vec(), vec![1]);
        let _ = seed.commit().unwrap();

        // The same schedule under plain snapshot isolation: both commit, because
        // SI does not validate the read set.
        let mut t1 = db.begin();
        let mut t2 = db.begin();
        let _ = t1.get(b"x").unwrap();
        let _ = t1.get(b"y").unwrap();
        let _ = t2.get(b"x").unwrap();
        let _ = t2.get(b"y").unwrap();
        t1.put(b"x".to_vec(), vec![0]);
        t2.put(b"y".to_vec(), vec![0]);

        assert!(t1.commit().is_ok());
        assert!(t2.commit().is_ok());
    }
}
