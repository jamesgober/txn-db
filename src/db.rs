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
//! point that validates and applies each commit atomically, taking the commit's
//! timestamp while it holds the commit's locks so that overlapping commits apply
//! in timestamp order.

use std::cell::Cell;
use std::sync::Arc;

use crate::error::Result;
use crate::oracle::Oracle;
use crate::store::{MemoryStore, VersionStore, WriteEntry};
use crate::timestamp::Timestamp;
use crate::txn::{Snapshot, Transaction};

/// How many times [`Db::put`] and [`Db::delete`] attempt a commit before they
/// give up and return the conflict. Each retry first waits until every commit
/// that had taken a timestamp when the conflict was seen is visible, so the next
/// attempt reads past the version it lost to. Reaching the bound therefore takes
/// this many other commits winning the same key during one call, or a store
/// that reports conflicts that do not exist.
const AUTOCOMMIT_ATTEMPTS: u32 = 10_000;

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
    log: Option<DurableLog<S>>,
}

/// The durable commit log of a database opened with [`Db::open`], with what the
/// commit path needs to keep it consistent with memory.
#[cfg(feature = "durability")]
struct DurableLog<S> {
    log: crate::durable::CommitLog,
    /// Removes the versions a commit applied, for a commit whose append failed.
    undo: fn(&S, Timestamp, &[Arc<[u8]>]),
    /// Set by the first append or sync failure. The log file's state is in doubt
    /// from then on, so every later commit is refused until the database is
    /// reopened.
    failed: std::sync::atomic::AtomicBool,
}

/// A commit's timestamp, taken at most once and always released.
///
/// The store calls [`get`](Stamp::get) while it holds the commit's locks; later
/// calls return the same timestamp. Dropping the stamp reports an allocated
/// timestamp to the oracle, so the watermark moves past it on every path out
/// of the commit: success, conflict, log failure, or a panic unwinding out of
/// the store.
struct Stamp<'a> {
    oracle: &'a Oracle,
    ts: Cell<Option<Timestamp>>,
}

impl<'a> Stamp<'a> {
    fn new(oracle: &'a Oracle) -> Self {
        Stamp {
            oracle,
            ts: Cell::new(None),
        }
    }

    fn get(&self) -> Timestamp {
        if let Some(ts) = self.ts.get() {
            return ts;
        }
        let ts = self.oracle.alloc_commit_ts();
        self.ts.set(Some(ts));
        ts
    }
}

impl Drop for Stamp<'_> {
    fn drop(&mut self) {
        if let Some(ts) = self.ts.get() {
            self.oracle.commit_done(ts);
        }
    }
}

/// A single registered point read, unregistered on drop (also on unwind).
struct PointRead<'a, S: VersionStore> {
    inner: &'a Inner<S>,
    read_ts: Timestamp,
    slot: usize,
}

impl<S: VersionStore> Drop for PointRead<'_, S> {
    fn drop(&mut self) {
        self.inner.end_reader(self.read_ts, self.slot);
    }
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

    /// Register a live reader and return its read timestamp and registry slot.
    #[inline]
    pub(crate) fn begin_reader(&self) -> (Timestamp, usize) {
        self.oracle.begin_reader()
    }

    /// Unregister a reader that began at `read_ts` in `slot`.
    #[inline]
    pub(crate) fn end_reader(&self, read_ts: Timestamp, slot: usize) {
        self.oracle.end_reader(read_ts, slot);
    }

    /// Reclaim versions no live reader can observe, returning the count removed.
    fn collect_garbage(&self) -> usize {
        self.store.collect_garbage(self.oracle.low_watermark())
    }

    /// Read one key at the current watermark, registered as a reader for the
    /// duration of the read only.
    fn point_get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>> {
        let (read_ts, slot) = self.begin_reader();
        let read = PointRead {
            inner: self,
            read_ts,
            slot,
        };
        self.store.get(key, read.read_ts)
    }

    /// Validate and apply a commit through the store, make it durable when the
    /// database has a log, release its timestamp into the watermark, and wait
    /// until it is visible.
    ///
    /// The store takes the commit timestamp while it holds the commit's locks
    /// ([`VersionStore::try_commit_ordered`]), so commits that touch
    /// overlapping keys apply in timestamp order. The timestamp is released on
    /// every outcome, including a store panic, so a failed attempt never stalls
    /// the watermark behind it.
    pub(crate) fn commit_writes(
        &self,
        read_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<Timestamp> {
        // Check the log's state and the record size, and encode the record,
        // before the write set is consumed by the store. No cost for an
        // in-memory database (no log).
        #[cfg(feature = "durability")]
        let prepared = match self.log.as_ref() {
            Some(durable) => Some(durable.prepare(&writes)?),
            None => None,
        };

        let stamp = Stamp::new(&self.oracle);
        let _applied_at = self
            .store
            .try_commit_ordered(read_ts, writes, reads, &|| stamp.get())?;
        // The stamp is the authority on the timestamp, whatever the store
        // returned, so the oracle's bookkeeping cannot be confused by it.
        let commit_ts = stamp.get();

        // Make the commit durable before it becomes visible. The writes are
        // applied but hidden: the watermark cannot pass `commit_ts` until the
        // stamp is dropped. If the append fails, the writes are removed before
        // that, so no reader ever sees them.
        // The undo guard is declared after the stamp, so on an error return or
        // a panic it runs first and the writes are gone before the timestamp
        // is released.
        #[cfg(feature = "durability")]
        if let (Some(durable), Some((mut record, keys))) = (self.log.as_ref(), prepared) {
            crate::durable::set_commit_ts(&mut record, commit_ts);
            let mut undo = UndoUnlessLogged {
                durable,
                store: &self.store,
                commit_ts,
                keys,
                logged: false,
            };
            durable.log.append_committed(&record)?;
            undo.logged = true;
        }

        drop(stamp);
        self.oracle.wait_visible(commit_ts);
        Ok(commit_ts)
    }

    /// Build the shared inner state for a database recovered from a durable log.
    #[cfg(feature = "durability")]
    fn recovered(
        store: S,
        oracle: Oracle,
        log: crate::durable::CommitLog,
        undo: fn(&S, Timestamp, &[Arc<[u8]>]),
    ) -> Self {
        Inner {
            store,
            oracle,
            log: Some(DurableLog {
                log,
                undo,
                failed: std::sync::atomic::AtomicBool::new(false),
            }),
        }
    }

    /// The durable log, for tests of its failure path.
    #[cfg(all(test, feature = "durability"))]
    fn durable_log(&self) -> Option<&crate::durable::CommitLog> {
        self.log.as_ref().map(|durable| &durable.log)
    }
}

/// An encoded commit record awaiting its timestamp, with the keys it writes.
#[cfg(feature = "durability")]
type PreparedRecord = (Vec<u8>, Vec<Arc<[u8]>>);

/// Removes a durable commit's applied writes and fails the log, unless the
/// commit's record was appended and synced. Runs on an append error and on a
/// panic alike.
#[cfg(feature = "durability")]
struct UndoUnlessLogged<'a, S> {
    durable: &'a DurableLog<S>,
    store: &'a S,
    commit_ts: Timestamp,
    keys: Vec<Arc<[u8]>>,
    logged: bool,
}

#[cfg(feature = "durability")]
impl<S> Drop for UndoUnlessLogged<'_, S> {
    fn drop(&mut self) {
        if !self.logged {
            self.durable
                .failed
                .store(true, std::sync::atomic::Ordering::SeqCst);
            (self.durable.undo)(self.store, self.commit_ts, &self.keys);
        }
    }
}

#[cfg(feature = "durability")]
impl<S> DurableLog<S> {
    /// Refuse the commit if the log has failed or its record would be too
    /// large; otherwise encode the record (with a placeholder timestamp) and
    /// collect the written keys for a possible undo.
    fn prepare(&self, writes: &[WriteEntry]) -> Result<PreparedRecord> {
        use crate::durable::{MAX_RECORD_BYTES, encode_for_log, encoded_len};
        use crate::error::TxnError;

        if self.failed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(TxnError::durability(
                "an earlier commit-log failure left the log in doubt; \
                 commits are refused until the database is reopened",
            ));
        }
        let len = encoded_len(writes);
        if len > MAX_RECORD_BYTES {
            return Err(TxnError::too_large(len, MAX_RECORD_BYTES));
        }
        let record = encode_for_log(Timestamp::ZERO, writes);
        let keys = writes.iter().map(|(key, _)| Arc::clone(key)).collect();
        Ok((record, keys))
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
/// validated at commit, rejecting write skew and the read-only anomaly that
/// snapshot isolation permits.
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
    /// returns, and before any reader can see its writes, so an acknowledged
    /// commit survives a crash and nothing a reader saw can be lost by one. On
    /// open, the log is replayed: each committed transaction is reinstated, and a
    /// transaction that never reached the log (because it aborted, or because
    /// the process crashed before its record was made durable) is simply
    /// absent. The recovered data lives in memory; the log is the durable record
    /// from which it is rebuilt. Old versions are collected once after replay,
    /// so the recovered store holds the current value of each key, not its
    /// history.
    ///
    /// The log is never truncated: it grows by one record per commit, and
    /// opening it replays every record, so open time grows with the number of
    /// commits ever made.
    ///
    /// One commit's record must fit in 64 MiB, the `wal-db` default record
    /// limit. A larger commit is refused with
    /// [`TxnError::TooLarge`](crate::TxnError::TooLarge) before anything is
    /// applied or logged. If appending or syncing a record fails, the commit
    /// returns [`TxnError::Durability`](crate::TxnError::Durability), its writes
    /// are removed before any reader can see them, and the database refuses
    /// every later commit until it is reopened.
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
        // No reader exists yet, so only the newest version of each key can ever
        // be read: drop the replayed history now rather than holding all of it
        // until the caller's first collection.
        let _ = store.collect_garbage(highest);

        Ok(Db {
            inner: Arc::new(Inner::recovered(
                store,
                Oracle::recovered(highest),
                log,
                MemoryStore::remove_versions,
            )),
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
    /// most recent commit (every commit that has returned is included) and is
    /// unaffected by commits that happen afterward.
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
    /// validates that none of them changed since its snapshot, in addition to
    /// the write-write check every transaction gets. This is optimistic
    /// read-set validation (backward validation), not the dangerous-structure
    /// test of serializable snapshot isolation: it aborts whenever a read key
    /// changed, so it can abort transactions that SSI would let commit.
    ///
    /// Commits that touch overlapping keys take their timestamps in the order
    /// they apply, so the serialization order of serializable transactions is
    /// their commit-timestamp order, and every snapshot shows a state that
    /// order produces. That rejects write skew and the read-only anomaly that
    /// plain snapshot isolation permits. A serializable transaction that writes
    /// nothing commits without validation, exactly like a read-only snapshot,
    /// and sees a serializable state. The guarantee covers serializable
    /// transactions; a snapshot-isolation writer is not validated and can still
    /// produce write skew with them.
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

    /// Read one key without opening a transaction.
    ///
    /// A convenience for the common single-read case: it takes a snapshot of the
    /// current state and reads `key` from it, returning the newest committed
    /// value or `None` if the key is absent. For reading several keys at one
    /// consistent instant, take a [`snapshot`](Db::snapshot) and reuse it.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Store`](crate::TxnError::Store) if the backing store
    /// fails the read. The default in-memory store never fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// db.put(b"k".to_vec(), b"v".to_vec())?;
    /// assert_eq!(db.get(b"k")?.as_deref(), Some(&b"v"[..]));
    /// assert_eq!(db.get(b"absent")?, None);
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>> {
        self.inner.point_get(key)
    }

    /// Write one key in its own transaction, retrying on conflict, and return the
    /// commit timestamp.
    ///
    /// A convenience for the common single-write case: it begins a transaction,
    /// buffers the write, and commits. If a concurrent transaction wins the
    /// commit race, it waits until that commit is visible and retries against
    /// the fresher snapshot, so this is last-writer-wins and does not busy-spin.
    /// When you need to read-then-write atomically, or to control the conflict
    /// outcome yourself, use [`begin`](Db::begin) instead.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Store`](crate::TxnError::Store) if the backing store
    /// fails to apply the write,
    /// [`TxnError::Durability`](crate::TxnError::Durability) for a durable
    /// database whose commit cannot be made durable, or
    /// [`TxnError::TooLarge`](crate::TxnError::TooLarge) for a durable commit
    /// over the log's record limit. Conflicts are retried, not returned, unless
    /// 10,000 attempts in a row conflict; the last conflict is then returned
    /// rather than retrying forever.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// let ts = db.put(b"k".to_vec(), b"v".to_vec())?;
    /// assert!(ts > txn_db::Timestamp::ZERO);
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn put(&self, key: impl Into<Arc<[u8]>>, value: impl Into<Arc<[u8]>>) -> Result<Timestamp> {
        self.autocommit(key.into(), Some(value.into()))
    }

    /// Delete one key in its own transaction, retrying on conflict, and return
    /// the commit timestamp.
    ///
    /// The delete counterpart of [`put`](Db::put): last-writer-wins, conflicts
    /// retried the same way. After it returns the key reads as absent until
    /// written again.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Store`](crate::TxnError::Store) if the backing store
    /// fails, or [`TxnError::Durability`](crate::TxnError::Durability) for a
    /// durable database whose commit cannot be made durable. Conflicts are
    /// retried, not returned, unless 10,000 attempts in a row conflict, as for
    /// [`put`](Db::put).
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// db.put(b"k".to_vec(), b"v".to_vec())?;
    /// db.delete(b"k".to_vec())?;
    /// assert_eq!(db.get(b"k")?, None);
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn delete(&self, key: impl Into<Arc<[u8]>>) -> Result<Timestamp> {
        self.autocommit(key.into(), None)
    }

    /// Commit one write (`Some`) or delete (`None`) of `key`, retrying on
    /// conflict as [`put`](Db::put) documents.
    fn autocommit(&self, key: Arc<[u8]>, value: Option<Arc<[u8]>>) -> Result<Timestamp> {
        let mut attempts = 0u32;
        loop {
            let mut tx = self.begin();
            match &value {
                Some(value) => tx.put(Arc::clone(&key), Arc::clone(value)),
                None => tx.delete(Arc::clone(&key)),
            }
            match tx.commit() {
                Ok(ts) => return Ok(ts),
                Err(e) if e.is_retryable() => {
                    attempts += 1;
                    if attempts >= AUTOCOMMIT_ATTEMPTS {
                        return Err(e);
                    }
                    // The version this attempt lost to was stamped with a
                    // timestamp that is already allocated. Wait until it is
                    // visible, so the next attempt reads past it instead of
                    // spinning against it.
                    let oracle = &self.inner.oracle;
                    oracle.wait_visible(oracle.last_allocated());
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// The timestamp of the most recent commit visible to a new transaction.
    ///
    /// Returns [`Timestamp::ZERO`] for a database that has never been written.
    /// This is the read watermark: the timestamp a transaction beginning now
    /// would read at. It is at least the timestamp of every commit that has
    /// returned.
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
    fn test_autocommit_put_get_delete() {
        let db = Db::new();
        let ts = db.put(b"k".to_vec(), b"v".to_vec()).unwrap();
        assert!(ts > Timestamp::ZERO);
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v"[..]));
        assert_eq!(db.get(b"absent").unwrap(), None);

        let ts2 = db.delete(b"k".to_vec()).unwrap();
        assert!(ts2 > ts);
        assert_eq!(db.get(b"k").unwrap(), None);
    }

    #[test]
    fn test_autocommit_put_is_last_writer_wins_under_contention() {
        use std::thread;
        let db = Db::new();
        let handles: Vec<_> = (0..8u8)
            .map(|t| {
                let db = db.clone();
                thread::spawn(move || {
                    for _ in 0..100 {
                        // All threads write the same hot key; autocommit retries
                        // internally, so none of these ever fail.
                        let _ = db.put(b"hot".to_vec(), vec![t]).unwrap();
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        // The key exists and holds one of the written values.
        let v = db.get(b"hot").unwrap().unwrap();
        assert!(v.len() == 1 && v[0] < 8);
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

    #[cfg(feature = "durability")]
    #[test]
    fn test_failed_log_append_is_never_visible_and_fails_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("txn.wal");
        {
            let db = Db::open(&path).unwrap();
            let _ = db.put(b"kept".to_vec(), b"1".to_vec()).unwrap();
            let before = db.last_committed();

            db.inner.durable_log().unwrap().fail_next_append();
            let mut tx = db.begin();
            tx.put(b"kept".to_vec(), b"2".to_vec());
            tx.put(b"lost".to_vec(), b"x".to_vec());
            let err = tx.commit().unwrap_err();
            assert!(matches!(err, crate::TxnError::Durability { .. }));

            // The failed commit's writes were removed before its timestamp was
            // released, so no reader sees them.
            assert_eq!(db.get(b"kept").unwrap().as_deref(), Some(&b"1"[..]));
            assert_eq!(db.get(b"lost").unwrap(), None);
            assert!(db.last_committed() > before);

            // The database now refuses commits until it is reopened, and an
            // autocommit write returns the error instead of looping.
            let err = db.put(b"other".to_vec(), b"y".to_vec()).unwrap_err();
            assert!(matches!(err, crate::TxnError::Durability { .. }));
            assert_eq!(db.get(b"other").unwrap(), None);

            // Reads keep working.
            assert_eq!(
                db.snapshot().get(b"kept").unwrap().as_deref(),
                Some(&b"1"[..])
            );
        }
        let db = Db::open(&path).unwrap();
        assert_eq!(db.get(b"kept").unwrap().as_deref(), Some(&b"1"[..]));
        assert_eq!(db.get(b"lost").unwrap(), None);
        let _ = db.put(b"other".to_vec(), b"y".to_vec()).unwrap();
    }

    #[cfg(feature = "durability")]
    #[test]
    fn test_failed_append_removes_only_its_own_versions() {
        let dir = tempfile::tempdir().unwrap();
        let db = Db::open(dir.path().join("txn.wal")).unwrap();
        for v in 0..3u8 {
            let _ = db.put(b"k".to_vec(), vec![v]).unwrap();
        }
        let snap = db.snapshot();
        db.inner.durable_log().unwrap().fail_next_append();
        assert!(db.put(b"k".to_vec(), vec![9]).is_err());
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&[2u8][..]));
        assert_eq!(snap.get(b"k").unwrap().as_deref(), Some(&[2u8][..]));
    }

    #[test]
    fn test_commit_timestamps_follow_apply_order_on_one_key() {
        // Sequential commits to one key get increasing timestamps and the
        // newest is what a fresh reader sees.
        let db = Db::new();
        let mut last = Timestamp::ZERO;
        for v in 0..20u8 {
            let ts = db.put(b"k".to_vec(), vec![v]).unwrap();
            assert!(ts > last);
            assert_eq!(db.last_committed(), ts);
            last = ts;
        }
        assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&[19u8][..]));
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
