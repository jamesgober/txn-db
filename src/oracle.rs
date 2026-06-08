//! The timestamp oracle: read-snapshot selection, commit-timestamp allocation,
//! and the read watermark.
//!
//! Snapshot isolation needs two timestamp services. A transaction that begins
//! must pick a *read timestamp* — the latest point at which the database is
//! fully consistent — and a transaction that commits must be handed a unique,
//! strictly increasing *commit timestamp*. The oracle provides both.
//!
//! Commit timestamps are allocated lock-free with a single atomic increment, so
//! many transactions can take a timestamp at once without contending. Because
//! commits then apply their writes concurrently — under the version store's
//! sharded locks, possibly out of timestamp order — the read timestamp cannot
//! simply be "the last timestamp allocated": a reader must never observe a
//! timestamp whose writes are not all applied yet. The oracle therefore tracks a
//! **watermark**, the largest timestamp `T` such that every commit with a
//! timestamp `<= T` has finished. New transactions read at the watermark, which
//! is published in its own atomic so begin and snapshot are lock-free too. Only
//! the bookkeeping that advances the watermark as commits complete takes a
//! short mutex.

use std::collections::{BTreeMap, HashSet};

use crate::sync::{self, AtomicU64, Mutex, Ordering};
use crate::timestamp::Timestamp;

/// Allocates timestamps and tracks the consistent-read watermark for one
/// database.
pub(crate) struct Oracle {
    /// The next commit timestamp to hand out. Advanced lock-free.
    next_ts: AtomicU64,
    /// The published read watermark: every commit `<= read_ts` is fully applied.
    /// Read by `begin`/`snapshot` without taking a lock.
    read_ts: AtomicU64,
    /// Bookkeeping for advancing the watermark as commits finish out of order.
    pending: Mutex<Pending>,
    /// The read timestamps of every live transaction and snapshot, each with a
    /// reference count. The smallest key is the oldest snapshot still in use and
    /// drives garbage collection: a version older than what that snapshot can
    /// observe is unreachable. A reader's timestamp is read from the watermark
    /// under this lock when it registers, so garbage collection can never run
    /// against a low watermark a not-yet-registered reader would undercut.
    readers: Mutex<BTreeMap<u64, usize>>,
}

/// The mutable watermark state, guarded by [`Oracle::pending`].
struct Pending {
    /// Largest timestamp such that every timestamp `<= done_upto` is complete.
    /// Mirrored into [`Oracle::read_ts`] whenever it advances.
    done_upto: u64,
    /// Completed timestamps that are not yet contiguous with `done_upto`.
    ahead: HashSet<u64>,
}

impl Oracle {
    /// Create an oracle for an empty database: the first commit timestamp is 1,
    /// and the watermark starts at [`Timestamp::ZERO`].
    pub(crate) fn new() -> Self {
        Oracle {
            next_ts: AtomicU64::new(1),
            read_ts: AtomicU64::new(Timestamp::ZERO.get()),
            pending: Mutex::new(Pending {
                done_upto: Timestamp::ZERO.get(),
                ahead: HashSet::new(),
            }),
            readers: Mutex::new(BTreeMap::new()),
        }
    }

    /// Create an oracle for a database recovered from a durable log, where every
    /// commit up to and including `highest` is already applied.
    ///
    /// The next commit hands out `highest + 1`, and the read watermark starts at
    /// `highest` so a transaction beginning right after recovery sees all
    /// recovered state.
    #[cfg(feature = "durability")]
    pub(crate) fn recovered(highest: Timestamp) -> Self {
        let highest = highest.get();
        Oracle {
            next_ts: AtomicU64::new(highest + 1),
            read_ts: AtomicU64::new(highest),
            pending: Mutex::new(Pending {
                done_upto: highest,
                ahead: HashSet::new(),
            }),
            readers: Mutex::new(BTreeMap::new()),
        }
    }

    /// The timestamp a transaction beginning now should read at: the current
    /// watermark. Lock-free.
    #[inline]
    pub(crate) fn read_ts(&self) -> Timestamp {
        Timestamp::from_raw(self.read_ts.load(Ordering::Acquire))
    }

    /// Allocate a fresh, unique, strictly increasing commit timestamp. Lock-free.
    #[inline]
    pub(crate) fn alloc_commit_ts(&self) -> Timestamp {
        Timestamp::from_raw(self.next_ts.fetch_add(1, Ordering::Relaxed))
    }

    /// Register a new reader and return the timestamp it should read at.
    ///
    /// Reading the watermark while holding the registry lock is what makes
    /// garbage collection safe: a reader is recorded at the same instant its
    /// timestamp is chosen, so [`low_watermark`](Self::low_watermark) can never
    /// observe an empty registry and reclaim versions that this reader — having
    /// just taken the same or an earlier timestamp — would still need.
    pub(crate) fn begin_reader(&self) -> Timestamp {
        let mut readers = sync::lock(&self.readers);
        let ts = self.read_ts.load(Ordering::Acquire);
        *readers.entry(ts).or_insert(0) += 1;
        Timestamp::from_raw(ts)
    }

    /// Drop a reader registered at `ts`.
    pub(crate) fn end_reader(&self, ts: Timestamp) {
        let key = ts.get();
        let mut readers = sync::lock(&self.readers);
        let now_zero = match readers.get_mut(&key) {
            Some(count) => {
                *count -= 1;
                *count == 0
            }
            None => false,
        };
        if now_zero {
            let _ = readers.remove(&key);
        }
    }

    /// The oldest timestamp any live reader can still observe.
    ///
    /// This is the smallest registered reader timestamp, or — when no reader is
    /// live — the current watermark, since any reader that begins next will read
    /// at or after it. Versions strictly older than the newest version at or
    /// below this timestamp are unreachable and may be reclaimed.
    pub(crate) fn low_watermark(&self) -> Timestamp {
        let readers = sync::lock(&self.readers);
        match readers.keys().next() {
            Some(&oldest) => Timestamp::from_raw(oldest),
            None => Timestamp::from_raw(self.read_ts.load(Ordering::Acquire)),
        }
    }

    /// Record that the commit (or aborted attempt) holding `ts` has finished, and
    /// advance the watermark across any now-contiguous run of completed
    /// timestamps.
    ///
    /// Every allocated commit timestamp must be reported here exactly once —
    /// including timestamps whose commit was rejected by conflict detection, so
    /// an aborted attempt does not stall the watermark behind it.
    pub(crate) fn commit_done(&self, ts: Timestamp) {
        let ts = ts.get();
        let mut p = sync::lock(&self.pending);
        if ts == p.done_upto + 1 {
            p.done_upto = ts;
            // Advance across any contiguous run of timestamps that already
            // completed out of order.
            let mut next = ts + 1;
            while p.ahead.remove(&next) {
                p.done_upto = next;
                next += 1;
            }
            self.read_ts.store(p.done_upto, Ordering::Release);
        } else {
            let _ = p.ahead.insert(ts);
        }
    }
}

#[cfg(all(test, not(loom)))]
mod tests {
    use super::*;

    #[test]
    fn test_new_oracle_reads_at_zero() {
        let o = Oracle::new();
        assert_eq!(o.read_ts(), Timestamp::ZERO);
    }

    #[test]
    fn test_commit_ts_is_strictly_increasing() {
        let o = Oracle::new();
        let a = o.alloc_commit_ts();
        let b = o.alloc_commit_ts();
        assert!(b > a);
        assert_eq!(a, Timestamp::from_raw(1));
        assert_eq!(b, Timestamp::from_raw(2));
    }

    #[test]
    fn test_watermark_advances_on_in_order_completion() {
        let o = Oracle::new();
        let t1 = o.alloc_commit_ts();
        o.commit_done(t1);
        assert_eq!(o.read_ts(), Timestamp::from_raw(1));
    }

    #[test]
    fn test_watermark_waits_for_earlier_timestamp() {
        let o = Oracle::new();
        let t1 = o.alloc_commit_ts();
        let t2 = o.alloc_commit_ts();

        // The later timestamp finishes first: the watermark must not jump ahead
        // of the still-pending earlier one.
        o.commit_done(t2);
        assert_eq!(o.read_ts(), Timestamp::ZERO);

        // Once the earlier one completes, the watermark advances across both.
        o.commit_done(t1);
        assert_eq!(o.read_ts(), Timestamp::from_raw(2));
    }
}
