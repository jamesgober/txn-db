//! Logical timestamps that order transactions.
//!
//! `txn-db` is a multi-version store: every committed write is tagged with the
//! [`Timestamp`] at which it became visible, and every reader carries the
//! timestamp of the snapshot it is reading. A read of a key returns the newest
//! version whose commit timestamp is less than or equal to the reader's
//! snapshot timestamp. Ordering transactions is therefore the whole job of this
//! type, which is why it is a distinct, totally ordered value rather than a bare
//! integer threaded through the code.
//!
//! Timestamps are *logical*, not wall-clock: they come from a single
//! monotonic counter inside the database, so they are dense, gap-free in
//! issuance order, and never run backwards. Wall-clock time plays no part in
//! visibility, which keeps the isolation contract independent of clock skew.

use core::fmt;

/// A logical timestamp marking a point in a database's commit history.
///
/// Timestamps are issued by the database as a strictly increasing sequence.
/// [`Timestamp::ZERO`] is the timestamp of the empty database before any commit;
/// a reader that begins against an empty database reads at `ZERO` and sees
/// nothing. The first commit is stamped `1`, the next `2`, and so on.
///
/// The type is `Copy` and totally ordered, so comparing visibility is a single
/// integer compare on the hot read path.
///
/// # Examples
///
/// ```
/// use txn_db::Timestamp;
///
/// let t = Timestamp::ZERO;
/// assert_eq!(t.get(), 0);
/// assert!(Timestamp::ZERO < Timestamp::from_raw(1));
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The timestamp of the empty database, before any transaction has
    /// committed. A snapshot taken at `ZERO` observes no keys.
    pub const ZERO: Timestamp = Timestamp(0);

    /// Wrap a raw counter value as a timestamp.
    ///
    /// This is the inverse of [`Timestamp::get`] and exists for tests,
    /// serialization of the commit log, and custom
    /// [`VersionStore`](crate::VersionStore) implementations that persist
    /// timestamps. Application code rarely constructs timestamps directly — the
    /// database issues them.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Timestamp;
    ///
    /// let ts = Timestamp::from_raw(42);
    /// assert_eq!(ts.get(), 42);
    /// ```
    #[inline]
    #[must_use]
    pub const fn from_raw(value: u64) -> Self {
        Timestamp(value)
    }

    /// The raw counter value behind this timestamp.
    ///
    /// # Examples
    ///
    /// ```
    /// use txn_db::Timestamp;
    ///
    /// assert_eq!(Timestamp::from_raw(7).get(), 7);
    /// ```
    #[inline]
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "@{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_is_least() {
        assert!(Timestamp::ZERO <= Timestamp::from_raw(0));
        assert!(Timestamp::ZERO < Timestamp::from_raw(1));
    }

    #[test]
    fn test_roundtrips_through_raw() {
        for v in [0u64, 1, 2, u64::MAX] {
            assert_eq!(Timestamp::from_raw(v).get(), v);
        }
    }

    #[test]
    fn test_ordering_matches_integer_ordering() {
        assert!(Timestamp::from_raw(5) < Timestamp::from_raw(9));
        assert!(Timestamp::from_raw(9) > Timestamp::from_raw(5));
    }

    #[test]
    fn test_display_prefixes_at_sign() {
        assert_eq!(Timestamp::from_raw(12).to_string(), "@12");
    }
}
