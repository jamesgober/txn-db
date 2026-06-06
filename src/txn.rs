//! Transactions and snapshots — the read and write handles a [`Db`](crate::Db)
//! hands out.
//!
//! A [`Transaction`] is the read-write unit of work. It takes a snapshot of the
//! database when it begins, serves every read from that snapshot (plus its own
//! uncommitted writes), buffers writes locally, and applies them atomically at
//! [`commit`](Transaction::commit) — or discards them on
//! [`rollback`](Transaction::rollback) or drop. Because reads come from a fixed
//! snapshot, a transaction never blocks writers and is never blocked by them.
//!
//! A [`Snapshot`] is the read-only counterpart: a consistent, point-in-time
//! view with no write buffer and nothing to commit. Use it when you only need
//! to read several keys as of one instant.

use std::collections::HashMap;
use std::sync::Arc;

use crate::db::Inner;
use crate::error::Result;
use crate::store::{MemoryStore, VersionStore};
use crate::timestamp::Timestamp;

/// A read-write transaction over a consistent snapshot of the database.
///
/// A transaction is created by [`Db::begin`](crate::Db::begin). It reads as of
/// the snapshot timestamp captured at that moment, so concurrent commits by
/// other transactions are invisible to it — this is snapshot isolation. Writes
/// are buffered in the transaction and become visible to others only when
/// [`commit`](Transaction::commit) succeeds; within the transaction, a read of a
/// key it has written returns that pending write (read-your-own-writes).
///
/// At commit the database checks every written key for a write-write conflict:
/// if another transaction committed a change to any of those keys after this
/// transaction's snapshot, the commit is rejected with a retryable
/// [`TxnError::Conflict`](crate::TxnError::Conflict) and none of the writes are
/// applied. This is what prevents lost updates.
///
/// Dropping a transaction without committing discards its buffered writes; it
/// is equivalent to [`rollback`](Transaction::rollback).
///
/// # Examples
///
/// ```
/// use txn_db::Db;
///
/// let db = Db::new();
///
/// let mut tx = db.begin();
/// tx.put(b"account:1".to_vec(), 100u64.to_le_bytes().to_vec());
/// tx.put(b"account:2".to_vec(), 50u64.to_le_bytes().to_vec());
/// let commit_ts = tx.commit()?;
///
/// // A fresh transaction sees the committed state.
/// let tx = db.begin();
/// assert!(tx.get(b"account:1")?.is_some());
/// assert!(commit_ts > txn_db::Timestamp::ZERO);
/// # Ok::<(), txn_db::TxnError>(())
/// ```
#[must_use = "a transaction buffers writes that are discarded unless it is committed"]
pub struct Transaction<S: VersionStore = MemoryStore> {
    inner: Arc<Inner<S>>,
    read_ts: Timestamp,
    writes: HashMap<Arc<[u8]>, Option<Arc<[u8]>>>,
}

impl<S: VersionStore> Transaction<S> {
    /// Construct a transaction over `inner` reading at `read_ts`.
    pub(crate) fn new(inner: Arc<Inner<S>>, read_ts: Timestamp) -> Self {
        Transaction {
            inner,
            read_ts,
            writes: HashMap::new(),
        }
    }

    /// The snapshot timestamp this transaction reads at.
    ///
    /// Every read that is not served from the transaction's own write buffer
    /// observes the database as of this timestamp.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// let tx = db.begin();
    /// // Nothing has committed yet, so the snapshot is the empty database.
    /// assert_eq!(tx.read_timestamp(), txn_db::Timestamp::ZERO);
    /// ```
    #[inline]
    #[must_use]
    pub fn read_timestamp(&self) -> Timestamp {
        self.read_ts
    }

    /// Read the value of `key` as this transaction sees it.
    ///
    /// If the transaction has written `key`, the pending write is returned
    /// (read-your-own-writes), including `None` if it has deleted the key.
    /// Otherwise the value is read from the transaction's snapshot: the newest
    /// version committed at or before the snapshot timestamp, or `None` if the
    /// key does not exist as of the snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Store`](crate::TxnError::Store) if the backing
    /// [`VersionStore`](crate::VersionStore) fails the read. The default
    /// in-memory store never fails.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// let mut tx = db.begin();
    ///
    /// assert_eq!(tx.get(b"k")?, None);          // absent
    /// tx.put(b"k".to_vec(), b"v".to_vec());
    /// assert_eq!(tx.get(b"k")?.as_deref(), Some(&b"v"[..])); // its own write
    /// tx.delete(b"k".to_vec());
    /// assert_eq!(tx.get(b"k")?, None);          // its own delete
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>> {
        if let Some(pending) = self.writes.get(key) {
            return Ok(pending.clone());
        }
        self.inner.store.get(key, self.read_ts)
    }

    /// Buffer a write of `value` to `key`, to be applied at commit.
    ///
    /// The write is local to this transaction until [`commit`](Self::commit)
    /// succeeds; other transactions do not see it. Writing the same key twice
    /// keeps the last value. Both arguments accept anything convertible into an
    /// `Arc<[u8]>` — passing an owned `Vec<u8>` or `Arc<[u8]>` moves it in
    /// without copying the bytes.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// let mut tx = db.begin();
    /// tx.put(b"city".to_vec(), b"oslo".to_vec());
    /// tx.put(b"city".to_vec(), b"bergen".to_vec()); // overwrites within the txn
    /// assert_eq!(tx.get(b"city")?.as_deref(), Some(&b"bergen"[..]));
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn put(&mut self, key: impl Into<Arc<[u8]>>, value: impl Into<Arc<[u8]>>) {
        let _ = self.writes.insert(key.into(), Some(value.into()));
    }

    /// Buffer a delete of `key`, to be applied at commit.
    ///
    /// After this call the transaction reads `key` as absent. At commit a
    /// tombstone is written so that snapshots taken after the commit also see
    /// the key as absent. Deleting a key that does not exist is a no-op that
    /// still participates in conflict detection, so a delete races other
    /// writers the same way a `put` does.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// let mut setup = db.begin();
    /// setup.put(b"k".to_vec(), b"v".to_vec());
    /// setup.commit()?;
    ///
    /// let mut tx = db.begin();
    /// tx.delete(b"k".to_vec());
    /// tx.commit()?;
    ///
    /// assert_eq!(db.begin().get(b"k")?, None);
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn delete(&mut self, key: impl Into<Arc<[u8]>>) {
        let _ = self.writes.insert(key.into(), None);
    }

    /// Commit the transaction, applying all buffered writes atomically.
    ///
    /// On success every buffered write becomes visible to transactions that
    /// begin afterward, and the commit timestamp is returned. A transaction
    /// that buffered no writes commits trivially and returns its snapshot
    /// timestamp without allocating a new one.
    ///
    /// # Errors
    ///
    /// Returns [`TxnError::Conflict`](crate::TxnError::Conflict) — which is
    /// retryable — if any written key was changed by another transaction that
    /// committed after this one's snapshot; in that case no writes are applied.
    /// Returns [`TxnError::Store`](crate::TxnError::Store) if the backing store
    /// fails to apply the batch.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// let mut tx = db.begin();
    /// tx.put(b"k".to_vec(), b"v".to_vec());
    /// let ts = tx.commit()?;
    /// assert!(ts > txn_db::Timestamp::ZERO);
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn commit(self) -> Result<Timestamp> {
        if self.writes.is_empty() {
            return Ok(self.read_ts);
        }
        self.inner.commit_writes(self.read_ts, self.writes)
    }

    /// Discard the transaction and all of its buffered writes.
    ///
    /// This is explicit; simply dropping the transaction has the same effect.
    /// Rolling back never fails and never touches the shared store.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// let mut tx = db.begin();
    /// tx.put(b"k".to_vec(), b"v".to_vec());
    /// tx.rollback();
    ///
    /// // The write never reached the database.
    /// assert_eq!(db.begin().get(b"k")?, None);
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    #[inline]
    pub fn rollback(self) {
        // Dropping `self` releases the buffered writes; this method documents
        // the intent and consumes the transaction so it cannot be used again.
    }
}

/// A read-only, point-in-time view of the database.
///
/// A snapshot is created by [`Db::snapshot`](crate::Db::snapshot) and reads as
/// of the moment it was taken. It has no write buffer and nothing to commit, so
/// it is cheaper than a transaction when all you need is to read several keys at
/// one consistent instant. Multiple snapshots and transactions coexist without
/// blocking each other.
///
/// # Examples
///
/// ```
/// use txn_db::Db;
///
/// let db = Db::new();
/// let mut tx = db.begin();
/// tx.put(b"k".to_vec(), b"v1".to_vec());
/// tx.commit()?;
///
/// // Capture a snapshot, then change the database.
/// let snap = db.snapshot();
/// let mut tx = db.begin();
/// tx.put(b"k".to_vec(), b"v2".to_vec());
/// tx.commit()?;
///
/// // The snapshot still sees the value as of when it was taken.
/// assert_eq!(snap.get(b"k")?.as_deref(), Some(&b"v1"[..]));
/// assert_eq!(db.snapshot().get(b"k")?.as_deref(), Some(&b"v2"[..]));
/// # Ok::<(), txn_db::TxnError>(())
/// ```
pub struct Snapshot<S: VersionStore = MemoryStore> {
    inner: Arc<Inner<S>>,
    read_ts: Timestamp,
}

impl<S: VersionStore> Snapshot<S> {
    /// Construct a snapshot over `inner` reading at `read_ts`.
    pub(crate) fn new(inner: Arc<Inner<S>>, read_ts: Timestamp) -> Self {
        Snapshot { inner, read_ts }
    }

    /// The timestamp this snapshot reads at.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Db;
    ///
    /// let db = Db::new();
    /// assert_eq!(db.snapshot().read_timestamp(), txn_db::Timestamp::ZERO);
    /// ```
    #[inline]
    #[must_use]
    pub fn read_timestamp(&self) -> Timestamp {
        self.read_ts
    }

    /// Read the value of `key` as of this snapshot.
    ///
    /// Returns the newest version committed at or before the snapshot
    /// timestamp, or `None` if the key does not exist as of that instant.
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
    /// assert_eq!(db.snapshot().get(b"missing")?, None);
    /// # Ok::<(), txn_db::TxnError>(())
    /// ```
    pub fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>> {
        self.inner.store.get(key, self.read_ts)
    }
}
