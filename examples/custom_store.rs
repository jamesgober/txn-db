//! Plugging a custom backing store into the engine through the [`VersionStore`]
//! trait — the Tier-3 seam. Here the custom store is an instrumented wrapper
//! that counts reads and applied versions while delegating the actual keeping
//! of data to the shipped in-memory store. The same shape lets you back the
//! engine with an on-disk store, a remote service, or anything that can hold
//! timestamped versions of a key.
//!
//! Run with: `cargo run --example custom_store`

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use txn_db::{Db, MemoryStore, Timestamp, TxnError, VersionStore, WriteEntry};

/// Counters shared between the store and whoever wants to observe it.
#[derive(Clone, Default)]
struct Counters {
    reads: Arc<AtomicU64>,
    versions_applied: Arc<AtomicU64>,
}

/// A [`VersionStore`] that records how often it is read and written, then
/// forwards every call to an inner [`MemoryStore`].
struct CountingStore {
    inner: MemoryStore,
    counters: Counters,
}

impl VersionStore for CountingStore {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>, TxnError> {
        let _ = self.counters.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key, read_ts)
    }

    fn latest_commit_ts(&self, key: &[u8]) -> Result<Option<Timestamp>, TxnError> {
        self.inner.latest_commit_ts(key)
    }

    fn apply(&self, commit_ts: Timestamp, writes: Vec<WriteEntry>) -> Result<(), TxnError> {
        let _ = self
            .counters
            .versions_applied
            .fetch_add(writes.len() as u64, Ordering::Relaxed);
        self.inner.apply(commit_ts, writes)
    }
}

fn main() -> Result<(), TxnError> {
    // Keep a handle to the counters, then move the store into the database.
    let counters = Counters::default();
    let db = Db::with_store(CountingStore {
        inner: MemoryStore::new(),
        counters: counters.clone(),
    });

    let mut tx = db.begin();
    tx.put(b"a".to_vec(), b"1".to_vec());
    tx.put(b"b".to_vec(), b"2".to_vec());
    tx.commit()?;

    let tx = db.begin();
    let _ = tx.get(b"a")?;
    let _ = tx.get(b"b")?;
    let _ = tx.get(b"c")?;

    // The wrapper observed everything the engine did, without changing any of
    // the transaction semantics.
    println!(
        "reads served:     {}",
        counters.reads.load(Ordering::Relaxed)
    );
    println!(
        "versions applied: {}",
        counters.versions_applied.load(Ordering::Relaxed)
    );

    Ok(())
}
