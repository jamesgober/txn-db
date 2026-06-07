//! # txn-db
//!
//! A multi-version concurrency control (MVCC) transaction engine: the layer
//! that turns a key-value store into a transactional database.
//!
//! Every write produces a new version tagged with a commit
//! [`Timestamp`], so readers get a stable snapshot of the data without ever
//! blocking writers, and writers detect conflicts at commit time instead of
//! holding locks for the lifetime of a transaction. `txn-db` is deliberately a
//! *layer*, not a store: the version store is the [`VersionStore`] trait, so it
//! composes on top of an LSM tree, a B-tree, or any backend that can keep
//! timestamped versions of a key. Snapshot isolation is the model.
//!
//! ## The common case
//!
//! Begin a transaction, read and write through it, commit. Conflicts surface as
//! a typed, retryable error.
//!
//! ```
//! use txn_db::Db;
//!
//! let db = Db::new();
//!
//! // Write two keys in one atomic transaction.
//! let mut tx = db.begin();
//! tx.put(b"user:1:name".to_vec(), b"ada".to_vec());
//! tx.put(b"user:1:role".to_vec(), b"admin".to_vec());
//! tx.commit()?;
//!
//! // A later transaction reads a consistent snapshot.
//! let tx = db.begin();
//! assert_eq!(tx.get(b"user:1:name")?.as_deref(), Some(&b"ada"[..]));
//! # Ok::<(), txn_db::TxnError>(())
//! ```
//!
//! ## Snapshot isolation
//!
//! A transaction reads the database as of the instant it began. Commits made by
//! other transactions afterward are invisible to it, and its own buffered
//! writes are visible only to itself until it commits. At commit, the engine
//! applies *first-committer-wins*: if any key the transaction wrote was changed
//! by another transaction that committed after this one's snapshot, the commit
//! is rejected with a retryable [`TxnError::Conflict`]. That rule is what
//! prevents lost updates.
//!
//! ```
//! use txn_db::{Db, TxnError};
//!
//! let db = Db::new();
//!
//! // Two transactions start from the same snapshot and write the same key.
//! let mut a = db.begin();
//! let mut b = db.begin();
//! a.put(b"counter".to_vec(), b"1".to_vec());
//! b.put(b"counter".to_vec(), b"2".to_vec());
//!
//! a.commit()?;                          // the first committer wins
//! let err = b.commit().unwrap_err();    // the second is told to retry
//! assert!(err.is_retryable());
//! # Ok::<(), TxnError>(())
//! ```
//!
//! ## The three tiers
//!
//! - **Tier 1** is the whole common case: [`Db::new`], [`Db::begin`], and the
//!   [`Transaction`] methods. No builder, no generics to name.
//! - **Tier 2** is configuration through a builder, arriving in a later phase.
//! - **Tier 3** is the [`VersionStore`] trait, the seam for custom backends,
//!   reachable through [`Db::with_store`].
//!
//! ## Status
//!
//! This is the `0.2` foundation: the public surface, the MVCC core, snapshot
//! isolation, and write-write conflict detection over an in-memory store.
//! Serializable isolation, a durable commit log via `wal-db`, and version
//! garbage collection follow in later phases. The shape of the Tier-1 API is
//! settled and will not change before `1.0`.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(warnings)]
#![deny(missing_docs)]
#![deny(unused_must_use)]
#![deny(unused_results)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::todo)]
#![deny(clippy::unimplemented)]
#![deny(clippy::print_stdout)]
#![deny(clippy::print_stderr)]
#![deny(clippy::dbg_macro)]
#![deny(clippy::unreachable)]
#![forbid(unsafe_code)]

mod db;
mod error;
mod oracle;
mod store;
mod sync;
mod timestamp;
mod txn;

pub use crate::db::Db;
pub use crate::error::{Result, TxnError};
pub use crate::store::{MemoryStore, VersionStore, WriteEntry};
pub use crate::timestamp::Timestamp;
pub use crate::txn::{Snapshot, Transaction};

/// The crate's common imports in one `use`.
///
/// Pulls in the database handle, the transaction and snapshot types, the
/// timestamp, and the error type — everything the Tier-1 common case touches.
///
/// # Examples
///
/// ```
/// use txn_db::prelude::*;
///
/// let db = Db::new();
/// let mut tx = db.begin();
/// tx.put(b"k".to_vec(), b"v".to_vec());
/// let _ts: Timestamp = tx.commit()?;
/// # Ok::<(), TxnError>(())
/// ```
pub mod prelude {
    pub use crate::db::Db;
    pub use crate::error::{Result, TxnError};
    pub use crate::store::{MemoryStore, VersionStore, WriteEntry};
    pub use crate::timestamp::Timestamp;
    pub use crate::txn::{Snapshot, Transaction};
}
