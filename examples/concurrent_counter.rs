//! Many threads increment one shared counter through the database. Each
//! increment is a read-modify-write transaction that retries when it loses a
//! commit race, so no update is ever lost: the final value equals the exact
//! number of increments issued.
//!
//! Run with: `cargo run --example concurrent_counter`

use std::sync::Arc;
use std::thread;

use txn_db::{Db, TxnError};

const THREADS: u64 = 8;
const PER_THREAD: u64 = 1_000;

fn main() -> Result<(), TxnError> {
    let db = Db::new();

    // Initialise the counter to zero.
    let mut tx = db.begin();
    tx.put(b"counter".to_vec(), 0u64.to_le_bytes().to_vec());
    tx.commit()?;

    // Spawn workers; each clones the cheap database handle.
    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let db = db.clone();
            thread::spawn(move || increment_many(&db, PER_THREAD))
        })
        .collect();

    let mut total_retries = 0u64;
    for handle in handles {
        total_retries += handle.join().expect("worker thread panicked");
    }

    let final_value = read_u64(&db.snapshot().get(b"counter")?.expect("counter exists"));
    println!("final counter: {final_value}");
    println!("expected:      {}", THREADS * PER_THREAD);
    println!("commit retries caused by contention: {total_retries}");
    assert_eq!(final_value, THREADS * PER_THREAD);

    Ok(())
}

/// Increment the counter `count` times; return how many commits had to retry.
fn increment_many(db: &Db, count: u64) -> u64 {
    let mut retries = 0;
    for _ in 0..count {
        loop {
            let mut tx = db.begin();
            let current = tx
                .get(b"counter")
                .expect("read")
                .map_or(0, |b| read_u64(&b));
            tx.put(b"counter".to_vec(), (current + 1).to_le_bytes().to_vec());
            match tx.commit() {
                Ok(_) => break,
                Err(e) if e.is_retryable() => {
                    retries += 1;
                    continue;
                }
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
    }
    retries
}

fn read_u64(bytes: &Arc<[u8]>) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(buf)
}
