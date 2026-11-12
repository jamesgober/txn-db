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
//! is published in its own atomic so reading it is lock-free. Only the
//! bookkeeping that advances the watermark as commits complete takes a short
//! mutex, and a committer that must wait for an earlier timestamp to finish
//! before its own write is visible parks on a condition variable beside it.
//!
//! The live-reader registry that garbage collection consults is split into
//! independent slots, each on its own cache line, and a thread registers in the
//! slot assigned to it. Readers on different threads therefore do not contend
//! on one lock.

use std::cell::Cell;
use std::collections::HashSet;

use crate::sync::{self, AtomicU64, Condvar, Mutex, Ordering};
use crate::timestamp::Timestamp;

/// Number of live-reader registry slots. A power of two so the slot index is a
/// mask. Thirty-two slots spread the threads of a typical host across distinct
/// locks; loom builds use two to keep the interleaving search small.
#[cfg(not(loom))]
const READER_SLOTS: usize = 32;
#[cfg(loom)]
const READER_SLOTS: usize = 2;

/// How many busy-wait rounds, then yield rounds, a committer spends waiting for
/// an earlier timestamp before it parks on the condition variable.
#[cfg(not(loom))]
const SPIN_ROUNDS: u32 = 256;
#[cfg(not(loom))]
const YIELD_ROUNDS: u32 = 16;

/// A value on its own cache line (128 bytes covers adjacent-line prefetch).
#[repr(align(128))]
struct Padded<T>(T);

impl<T> core::ops::Deref for Padded<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.0
    }
}

/// One registry slot: the read timestamps registered through it, each with a
/// reference count, on its own cache line so slots used by different threads do
/// not share one.
#[repr(align(128))]
struct ReaderSlot {
    readers: Mutex<Vec<(u64, usize)>>,
}

/// Allocates timestamps and tracks the consistent-read watermark for one
/// database.
pub(crate) struct Oracle {
    /// The next commit timestamp to hand out. Advanced lock-free, while the
    /// committing store holds its shard locks, so it sits on its own cache line
    /// away from the watermark every reader loads.
    next_ts: Padded<AtomicU64>,
    /// The published read watermark: every commit `<= read_ts` is fully applied.
    /// Read by `begin`/`snapshot` without taking a lock.
    read_ts: Padded<AtomicU64>,
    /// Bookkeeping for advancing the watermark as commits finish out of order.
    pending: Mutex<Pending>,
    /// Signalled whenever the watermark advances while a committer is waiting
    /// for it to reach that committer's own timestamp.
    advanced: Condvar,
    /// The read timestamps of every live transaction and snapshot, split across
    /// slots. The smallest registered timestamp is the oldest snapshot still in
    /// use and drives garbage collection. A reader's timestamp is read from the
    /// watermark while it holds its slot's lock, which is what keeps garbage
    /// collection from undercutting a reader that is registering right now (see
    /// [`low_watermark`](Self::low_watermark)).
    slots: Box<[ReaderSlot]>,
}

/// The mutable watermark state, guarded by [`Oracle::pending`].
struct Pending {
    /// Largest timestamp such that every timestamp `<= done_upto` is complete.
    /// Mirrored into [`Oracle::read_ts`] whenever it advances.
    done_upto: u64,
    /// Completed timestamps that are not yet contiguous with `done_upto`.
    ahead: HashSet<u64>,
    /// Committers parked in [`Oracle::wait_visible`].
    waiters: usize,
}

impl Oracle {
    /// Create an oracle for an empty database: the first commit timestamp is 1,
    /// and the watermark starts at [`Timestamp::ZERO`].
    pub(crate) fn new() -> Self {
        Oracle::starting_at(Timestamp::ZERO.get())
    }

    /// Create an oracle for a database recovered from a durable log, where every
    /// commit up to and including `highest` is already applied.
    ///
    /// The next commit hands out `highest + 1`, and the read watermark starts at
    /// `highest` so a transaction beginning right after recovery sees all
    /// recovered state.
    #[cfg(feature = "durability")]
    pub(crate) fn recovered(highest: Timestamp) -> Self {
        Oracle::starting_at(highest.get())
    }

    fn starting_at(done: u64) -> Self {
        let slots = (0..READER_SLOTS)
            .map(|_| ReaderSlot {
                readers: Mutex::new(Vec::new()),
            })
            .collect::<Vec<_>>()
            .into_boxed_slice();
        Oracle {
            next_ts: Padded(AtomicU64::new(done + 1)),
            read_ts: Padded(AtomicU64::new(done)),
            pending: Mutex::new(Pending {
                done_upto: done,
                ahead: HashSet::new(),
                waiters: 0,
            }),
            advanced: Condvar::new(),
            slots,
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
        Timestamp::from_raw(self.next_ts.fetch_add(1, Ordering::AcqRel))
    }

    /// The most recently allocated commit timestamp, or the starting point when
    /// none has been allocated since.
    #[inline]
    pub(crate) fn last_allocated(&self) -> Timestamp {
        Timestamp::from_raw(self.next_ts.load(Ordering::Acquire) - 1)
    }

    /// Register a new reader and return the timestamp it should read at, with
    /// the registry slot it was recorded in (pass both to
    /// [`end_reader`](Self::end_reader)).
    ///
    /// The watermark is read while the slot's lock is held. That is what makes
    /// garbage collection safe: see [`low_watermark`](Self::low_watermark).
    pub(crate) fn begin_reader(&self) -> (Timestamp, usize) {
        let slot = current_slot();
        let mut readers = sync::lock(&self.slots[slot].readers);
        let ts = self.read_ts.load(Ordering::Acquire);
        // The watermark only rises, so a new registration nearly always matches
        // the newest entry; search from the back.
        match readers.iter_mut().rev().find(|(t, _)| *t == ts) {
            Some((_, count)) => *count += 1,
            None => readers.push((ts, 1)),
        }
        (Timestamp::from_raw(ts), slot)
    }

    /// Drop a reader registered at `ts` in `slot`.
    pub(crate) fn end_reader(&self, ts: Timestamp, slot: usize) {
        let key = ts.get();
        let mut readers = sync::lock(&self.slots[slot].readers);
        if let Some(pos) = readers.iter().position(|(t, _)| *t == key) {
            readers[pos].1 -= 1;
            if readers[pos].1 == 0 {
                let _ = readers.swap_remove(pos);
            }
        }
    }

    /// The oldest timestamp any live reader can still observe.
    ///
    /// This is the smallest registered reader timestamp, bounded above by the
    /// watermark read at the start of the scan. The watermark is read first,
    /// then each slot is locked in turn. A reader that registers in a slot
    /// before the scan reaches it is seen by the scan. One that registers after
    /// the scan has passed its slot reads the watermark under that slot's lock,
    /// after this scan read it, and the watermark never falls, so that reader's
    /// timestamp is at least the bound returned here. Either way no live reader
    /// reads below the result.
    pub(crate) fn low_watermark(&self) -> Timestamp {
        let mut low = self.read_ts.load(Ordering::Acquire);
        for slot in self.slots.iter() {
            let readers = sync::lock(&slot.readers);
            for &(ts, _) in readers.iter() {
                low = low.min(ts);
            }
        }
        Timestamp::from_raw(low)
    }

    /// Record that the commit (or aborted attempt) holding `ts` has finished, and
    /// advance the watermark across any now-contiguous run of completed
    /// timestamps.
    ///
    /// Every allocated commit timestamp must be reported here exactly once,
    /// including timestamps whose commit was rejected by conflict detection or
    /// whose store call panicked, so a failed attempt never stalls the watermark
    /// behind it.
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
            if p.waiters > 0 {
                self.advanced.notify_all();
            }
        } else {
            let _ = p.ahead.insert(ts);
        }
    }

    /// Block until the watermark reaches `ts`, so a commit acknowledged at `ts`
    /// is visible to every transaction that begins afterward.
    ///
    /// Returns at once in the common case, where no earlier timestamp is still in
    /// flight. Otherwise it parks until the earlier commits finish. Every
    /// allocated timestamp is reported to [`commit_done`](Self::commit_done)
    /// exactly once, even when a store panics, so the wait always ends.
    pub(crate) fn wait_visible(&self, ts: Timestamp) {
        let ts = ts.get();
        if self.read_ts.load(Ordering::Acquire) >= ts {
            return;
        }
        // The earlier commit is usually a few instructions from done (applying
        // its last write, or releasing its locks), far less than the cost of
        // parking and waking a thread. Spin briefly, then yield, and park only
        // if it is still not done (a durable commit syncing its log, or a
        // preempted committer).
        #[cfg(not(loom))]
        for round in 0..SPIN_ROUNDS + YIELD_ROUNDS {
            if round < SPIN_ROUNDS {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
            }
            if self.read_ts.load(Ordering::Acquire) >= ts {
                return;
            }
        }
        let mut p = sync::lock(&self.pending);
        p.waiters += 1;
        while p.done_upto < ts {
            p = sync::wait(&self.advanced, p);
        }
        p.waiters -= 1;
    }
}

/// The registry slot of the calling thread.
///
/// Each thread is given a slot the first time it registers a reader, round
/// robin, and keeps it. The assignment is only a spreading hint: correctness
/// does not depend on which slot a reader uses, because the slot is recorded
/// with the reader and passed back when it unregisters.
fn current_slot() -> usize {
    use std::sync::atomic::{AtomicUsize, Ordering as StdOrdering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    std::thread_local! {
        static SLOT: Cell<usize> = const { Cell::new(usize::MAX) };
    }
    SLOT.with(|slot| {
        let mut index = slot.get();
        if index == usize::MAX {
            index = NEXT.fetch_add(1, StdOrdering::Relaxed);
            slot.set(index);
        }
        index & (READER_SLOTS - 1)
    })
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
    fn test_low_watermark_is_oldest_registered_reader() {
        let o = Oracle::new();
        let t1 = o.alloc_commit_ts();
        o.commit_done(t1);
        let (r1, s1) = o.begin_reader();
        let t2 = o.alloc_commit_ts();
        o.commit_done(t2);
        let (r2, s2) = o.begin_reader();
        assert_eq!(o.low_watermark(), r1);
        o.end_reader(r1, s1);
        assert_eq!(o.low_watermark(), r2);
        o.end_reader(r2, s2);
        assert_eq!(o.low_watermark(), Timestamp::from_raw(2));
    }

    #[test]
    fn test_reader_registered_on_one_thread_ends_on_another() {
        let o = std::sync::Arc::new(Oracle::new());
        let (ts, slot) = o.begin_reader();
        let o2 = std::sync::Arc::clone(&o);
        let handle = std::thread::spawn(move || o2.end_reader(ts, slot));
        assert!(handle.join().is_ok());
        let t1 = o.alloc_commit_ts();
        o.commit_done(t1);
        assert_eq!(o.low_watermark(), t1);
    }

    #[test]
    fn test_wait_visible_returns_once_earlier_timestamp_finishes() {
        let o = std::sync::Arc::new(Oracle::new());
        let t1 = o.alloc_commit_ts();
        let t2 = o.alloc_commit_ts();
        o.commit_done(t2);
        let o2 = std::sync::Arc::clone(&o);
        let waiter = std::thread::spawn(move || {
            o2.wait_visible(t2);
            o2.read_ts()
        });
        std::thread::sleep(std::time::Duration::from_millis(20));
        o.commit_done(t1);
        assert_eq!(waiter.join().ok(), Some(t2));
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
