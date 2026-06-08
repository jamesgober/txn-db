//! Bounding version growth with garbage collection.
//!
//! Every write to a key creates a new version, and old versions are kept so
//! in-flight readers see a stable snapshot. `Db::collect_garbage` reclaims the
//! versions no live reader can observe — but never one a held snapshot still
//! needs. This example overwrites a key many times, shows that a held snapshot
//! pins the versions it can see, and shows collection reclaiming them once the
//! snapshot is released.
//!
//! Run with: `cargo run --example garbage_collection`

use txn_db::{Db, TxnError};

fn main() -> Result<(), TxnError> {
    let db = Db::new();

    // Write the same key 1,000 times — 1,000 versions accumulate.
    for v in 0..1_000u32 {
        let mut tx = db.begin();
        tx.put(b"counter".to_vec(), v.to_le_bytes().to_vec());
        tx.commit()?;
    }

    // Take a snapshot now, then keep writing. The snapshot pins every version
    // it can observe, so collection cannot reclaim them yet.
    let pinned = db.snapshot();
    for v in 1_000..1_500u32 {
        let mut tx = db.begin();
        tx.put(b"counter".to_vec(), v.to_le_bytes().to_vec());
        tx.commit()?;
    }

    let reclaimed_while_pinned = db.collect_garbage();
    println!("reclaimed while a snapshot is held: {reclaimed_while_pinned}");
    println!("snapshot still reads its value: {}", read(&pinned)?);

    // Release the snapshot; now the old versions are unreachable.
    drop(pinned);
    let reclaimed_after = db.collect_garbage();
    println!("reclaimed after releasing the snapshot: {reclaimed_after}");

    // The current value is intact and a second collection finds nothing to do.
    let latest = db.snapshot();
    println!("latest value: {}", read(&latest)?);
    println!("reclaimed on a second pass: {}", db.collect_garbage());

    Ok(())
}

fn read(snap: &txn_db::Snapshot) -> Result<u32, TxnError> {
    let value = snap.get(b"counter")?.expect("counter exists");
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&value[..4]);
    Ok(u32::from_le_bytes(buf))
}
