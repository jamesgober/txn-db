//! A durable database that survives a restart.
//!
//! `Db::open` backs the database with a write-ahead log: every committed
//! transaction is appended and synced before `commit` returns, so an
//! acknowledged commit is on disk. This example commits some data, drops the
//! database (standing in for a process exit or crash), then reopens from the
//! same log and shows the data was recovered — while an uncommitted transaction
//! leaves no trace.
//!
//! Run with: `cargo run --example durable_store --features durability`

use std::sync::Arc;

use txn_db::{Db, TxnError};

fn main() -> Result<(), TxnError> {
    // A temporary log file for the demonstration.
    let dir = std::env::temp_dir().join("txn-db-durable-example");
    std::fs::create_dir_all(&dir).map_err(|e| TxnError::store("create example dir", e))?;
    let path = dir.join("accounts.wal");
    let _ = std::fs::remove_file(&path); // start clean for a repeatable run

    // First "session": open, commit two accounts, then leave one transaction
    // uncommitted before the database is dropped.
    {
        let db = Db::open(&path)?;

        let mut tx = db.begin();
        tx.put(b"alice".to_vec(), 100u64.to_le_bytes().to_vec());
        tx.put(b"bob".to_vec(), 50u64.to_le_bytes().to_vec());
        let ts = tx.commit()?;
        println!("session 1: committed alice + bob at {ts}");

        // This one is never committed — it must not survive.
        let mut pending = db.begin();
        pending.put(b"carol".to_vec(), 999u64.to_le_bytes().to_vec());
        drop(pending);
        println!("session 1: left carol uncommitted, dropping database");
    }

    // Second "session": reopen from the same log. Committed state is back.
    {
        let db = Db::open(&path)?;
        println!("session 2: reopened, watermark at {}", db.last_committed());

        let snap = db.snapshot();
        println!("  alice = {}", balance(&snap.get(b"alice")?));
        println!("  bob   = {}", balance(&snap.get(b"bob")?));
        println!("  carol present: {}", snap.get(b"carol")?.is_some());

        // Continue committing; timestamps pick up where recovery left off.
        let mut tx = db.begin();
        let alice = balance(&tx.get(b"alice")?);
        let bob = balance(&tx.get(b"bob")?);
        tx.put(b"alice".to_vec(), (alice - 25).to_le_bytes().to_vec());
        tx.put(b"bob".to_vec(), (bob + 25).to_le_bytes().to_vec());
        let ts = tx.commit()?;
        println!("session 2: transferred 25 alice -> bob at {ts}");
    }

    let _ = std::fs::remove_file(&path);
    Ok(())
}

fn balance(value: &Option<Arc<[u8]>>) -> u64 {
    value.as_ref().map_or(0, |bytes| {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[..8]);
        u64::from_le_bytes(buf)
    })
}
