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
//! - **Tier 2**, configuration through a builder, does not exist yet. The
//!   durable database (`Db::open`, `durability` feature) uses fixed settings,
//!   including a 64 MiB limit on one commit's log record.
//! - **Tier 3** is the [`VersionStore`] trait, the seam for custom backends,
//!   reachable through [`Db::with_store`].
//!
//! ## Features
//!
//! - `serializable`: `Db::begin_serializable`, optimistic read-set
//!   validation that rejects write skew and the read-only anomaly among
//!   serializable transactions.
//! - `durability`: `Db::open`, a durable commit log on `wal-db`, replayed on
//!   open. A commit's writes become visible only after its record is synced.
//!
//! Garbage collection of old versions ([`Db::collect_garbage`]) is always
//! available.
//!
//! ## Status
//!
//! Stable. The public API is frozen until `2.0` and the durable commit-log
//! format is frozen for the `1.x` series.

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
#[cfg(feature = "durability")]
mod durable;
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
