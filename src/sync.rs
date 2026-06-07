//! Synchronization primitives, swapped for `loom`'s instrumented versions under
//! `--cfg loom`.
//!
//! The commit path uses an atomic counter, a set of sharded reader-writer locks,
//! and a small mutex for the read-timestamp watermark. To let `loom` explore
//! every interleaving of those operations, the whole crate imports its sync
//! types from here rather than from `std::sync` directly. Under a normal build
//! these are exactly the standard-library types; under `--cfg loom` they are
//! loom's models, which a concurrency test drives through `loom::model`.

#[cfg(loom)]
pub(crate) use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(loom)]
pub(crate) use loom::sync::{Mutex, RwLock};

#[cfg(not(loom))]
pub(crate) use std::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
pub(crate) use std::sync::{Mutex, RwLock};

/// Lock a [`Mutex`], recovering the guard if a previous holder panicked.
///
/// The critical sections this crate takes never panic, so poisoning can only
/// come from a panic elsewhere while a guard was held. The protected data is
/// still structurally valid in that case, so recovering the guard keeps the
/// database usable instead of turning one panic into a permanent failure. The
/// `match` form works for both the standard and loom guard types.
#[inline]
pub(crate) fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Take a read guard, recovering it if a previous holder panicked.
#[inline]
pub(crate) fn read<T>(lock: &RwLock<T>) -> RwLockReadGuard<'_, T> {
    match lock.read() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Take a write guard, recovering it if a previous holder panicked.
#[inline]
pub(crate) fn write<T>(lock: &RwLock<T>) -> RwLockWriteGuard<'_, T> {
    match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(loom)]
pub(crate) use loom::sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};
#[cfg(not(loom))]
pub(crate) use std::sync::{MutexGuard, RwLockReadGuard, RwLockWriteGuard};
