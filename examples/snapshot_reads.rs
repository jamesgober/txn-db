//! A snapshot is a stable, point-in-time view: it keeps returning the values
//! that were current when it was taken, even as the database moves on. This is
//! what lets a long read — a report, a backup, a consistency check — run
//! without blocking writers and without seeing a torn, half-updated state.
//!
//! Run with: `cargo run --example snapshot_reads`

use txn_db::{Db, TxnError};

fn main() -> Result<(), TxnError> {
    let db = Db::new();

    // Version 1 of the configuration.
    let mut tx = db.begin();
    tx.put(b"config:mode".to_vec(), b"safe".to_vec());
    tx.put(b"config:limit".to_vec(), b"10".to_vec());
    tx.commit()?;

    // Take a snapshot of version 1.
    let v1 = db.snapshot();

    // Roll the configuration forward to version 2.
    let mut tx = db.begin();
    tx.put(b"config:mode".to_vec(), b"fast".to_vec());
    tx.put(b"config:limit".to_vec(), b"1000".to_vec());
    tx.commit()?;

    // A snapshot of version 2.
    let v2 = db.snapshot();

    println!("snapshot v1 reads as of {}", v1.read_timestamp());
    print_config(&v1)?;
    println!();
    println!("snapshot v2 reads as of {}", v2.read_timestamp());
    print_config(&v2)?;

    Ok(())
}

fn print_config(snap: &txn_db::Snapshot) -> Result<(), TxnError> {
    let mode = snap.get(b"config:mode")?;
    let limit = snap.get(b"config:limit")?;
    println!("  mode  = {}", text(mode.as_deref()));
    println!("  limit = {}", text(limit.as_deref()));
    Ok(())
}

fn text(bytes: Option<&[u8]>) -> String {
    bytes.map_or_else(
        || "<absent>".to_string(),
        |b| String::from_utf8_lossy(b).into_owned(),
    )
}
