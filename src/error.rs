//! The crate error type.
//!
//! Every fallible operation in `txn-db` returns [`Result<T>`], whose error is
//! [`TxnError`]. The type integrates with the portfolio's `error-forge`
//! framework — it implements [`error_forge::ForgeError`], so callers get the
//! stable `kind` / `is_fatal` metadata other crates rely on.
//!
//! The error a caller meets most often is [`TxnError::Conflict`]: under
//! snapshot isolation, two transactions that wrote the same key race at commit
//! time, and the later committer is aborted. That outcome is *expected* and
//! *retryable* — the contract is that the caller re-runs the transaction
//! against a fresher snapshot rather than treating it as a failure. The
//! [`TxnError::is_retryable`] helper makes that decision a single call in a
//! retry loop.

use core::fmt;

use error_forge::ForgeError;

/// A specialised [`Result`](core::result::Result) for transaction operations.
///
/// Defaults its error to [`TxnError`], so most signatures read `Result<T>`.
pub type Result<T, E = TxnError> = core::result::Result<T, E>;

/// Everything that can go wrong while running a transaction.
///
/// The type is [`#[non_exhaustive]`](https://doc.rust-lang.org/reference/attributes/type_system.html#the-non_exhaustive-attribute):
/// later versions may add variants without a major bump, so a `match` over it
/// must include a wildcard arm. Each variant documents what the caller should
/// do when they encounter it.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxnError {
    /// A write-write conflict aborted the transaction at commit time.
    ///
    /// Under snapshot isolation the database applies *first-committer-wins*:
    /// when a transaction commits, every key it wrote is checked against the
    /// version store, and if any of those keys was written by a different
    /// transaction that committed *after* this one took its snapshot, this
    /// commit is rejected. None of its writes are applied.
    ///
    /// This is the mechanism that prevents lost updates, and it is a normal
    /// part of operating under optimistic concurrency control. The correct
    /// response is to retry: begin a fresh transaction, re-read, re-apply the
    /// logic, and commit again. [`TxnError::is_retryable`] returns `true` for
    /// this variant.
    ///
    /// Only the length of the conflicting key is carried, never its bytes, so
    /// the error is safe to log even when keys hold sensitive data.
    Conflict {
        /// Length in bytes of the key whose conflict aborted the commit.
        key_len: usize,
    },

    /// The backing version store failed to service a read or apply a write.
    ///
    /// The in-memory store that ships with `txn-db` never produces this; it is
    /// the channel through which a custom [`VersionStore`](crate::VersionStore)
    /// — for example one backed by an on-disk engine — surfaces a failure
    /// through the same [`Result`]. `context` names the operation that was
    /// attempted (such as `"read visible version"`); `detail` carries the
    /// store's own message. Whether to retry depends on the store, so this
    /// variant is reported as non-fatal and left for the caller to judge.
    Store {
        /// The operation the store was performing when it failed.
        context: &'static str,
        /// The store's human-readable description of the failure.
        detail: String,
    },

    /// The durable commit log failed, or a record read back from it was not
    /// intact.
    ///
    /// Produced only with the `durability` feature: when appending or syncing a
    /// commit record fails, or when recovery on [`Db::open`](crate::Db) reads a
    /// record whose bytes do not decode. A commit that fails to become durable
    /// is *not* acknowledged — the contract that an acknowledged commit survives
    /// a crash holds — but the failure is fatal in the sense that the database's
    /// durability guarantee is in question, so treat it as unrecoverable rather
    /// than retrying blindly.
    Durability {
        /// A human-readable description of the durability failure.
        detail: String,
    },
}

impl TxnError {
    /// Build a [`TxnError::Conflict`] for a key of the given length.
    #[inline]
    #[must_use]
    pub(crate) fn conflict(key_len: usize) -> Self {
        TxnError::Conflict { key_len }
    }

    /// Build a [`TxnError::Store`] from a static context and a store message.
    ///
    /// Intended for [`VersionStore`](crate::VersionStore) implementations that
    /// can fail; the in-memory store never calls it.
    #[inline]
    #[must_use]
    pub fn store(context: &'static str, detail: impl fmt::Display) -> Self {
        TxnError::Store {
            context,
            detail: detail.to_string(),
        }
    }

    /// Build a [`TxnError::Durability`] from a description of the failure.
    #[cfg(feature = "durability")]
    #[inline]
    #[must_use]
    pub(crate) fn durability(detail: impl fmt::Display) -> Self {
        TxnError::Durability {
            detail: detail.to_string(),
        }
    }

    /// Returns `true` if re-running the transaction is the right response.
    ///
    /// A [`Conflict`](TxnError::Conflict) is retryable: another transaction won
    /// the race, and a fresh attempt against the newer snapshot will typically
    /// succeed. Backing-store failures are reported as not retryable here
    /// because their recoverability is store-specific; inspect the variant when
    /// a store can distinguish transient from permanent faults.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::{Db, TxnError};
    ///
    /// let db = Db::new();
    ///
    /// // The common retry loop: keep trying while the commit is retryable.
    /// let outcome = loop {
    ///     let mut tx = db.begin();
    ///     let current = tx.get(b"counter")?.map_or(0u64, |v| {
    ///         let mut buf = [0u8; 8];
    ///         buf.copy_from_slice(&v);
    ///         u64::from_le_bytes(buf)
    ///     });
    ///     tx.put(b"counter".to_vec(), (current + 1).to_le_bytes().to_vec());
    ///     match tx.commit() {
    ///         Ok(ts) => break ts,
    ///         Err(e) if e.is_retryable() => continue,
    ///         Err(e) => return Err(e),
    ///     }
    /// };
    /// # let _ = outcome;
    /// # Ok::<(), TxnError>(())
    /// ```
    #[inline]
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        matches!(self, TxnError::Conflict { .. })
    }
}

impl fmt::Display for TxnError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TxnError::Conflict { key_len } => write!(
                f,
                "write-write conflict on a {key_len}-byte key; retry the transaction"
            ),
            TxnError::Store { context, detail } => {
                write!(f, "version store error while {context}: {detail}")
            }
            TxnError::Durability { detail } => {
                write!(f, "durable commit log error: {detail}")
            }
        }
    }
}

impl core::error::Error for TxnError {}

impl ForgeError for TxnError {
    fn kind(&self) -> &'static str {
        match self {
            TxnError::Conflict { .. } => "Conflict",
            TxnError::Store { .. } => "Store",
            TxnError::Durability { .. } => "Durability",
        }
    }

    fn caption(&self) -> &'static str {
        "transaction error"
    }

    /// A [`Conflict`](TxnError::Conflict) is the retry signal and a
    /// [`Store`](TxnError::Store) failure is the store's to classify, so neither
    /// is fatal. A [`Durability`](TxnError::Durability) failure puts the crash
    /// guarantee in doubt and is reported as fatal.
    fn is_fatal(&self) -> bool {
        matches!(self, TxnError::Durability { .. })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_conflict_is_retryable() {
        assert!(TxnError::conflict(8).is_retryable());
    }

    #[test]
    fn test_store_error_is_not_retryable() {
        assert!(!TxnError::store("read", "disk gone").is_retryable());
    }

    #[test]
    fn test_conflict_display_reports_key_len_not_bytes() {
        let msg = TxnError::conflict(16).to_string();
        assert!(msg.contains("16-byte"));
        assert!(msg.contains("retry"));
    }

    #[test]
    fn test_kind_matches_variant() {
        assert_eq!(TxnError::conflict(1).kind(), "Conflict");
        assert_eq!(TxnError::store("x", "y").kind(), "Store");
    }

    #[test]
    fn test_no_variant_is_fatal() {
        assert!(!TxnError::conflict(1).is_fatal());
        assert!(!TxnError::store("x", "y").is_fatal());
    }

    #[test]
    fn test_error_is_clonable_and_comparable() {
        let a = TxnError::conflict(4);
        assert_eq!(a.clone(), a);
    }
}
