//! The shortest end-to-end use of `txn-db`: open a database, write a few keys
//! in one transaction, then read them back from another.
//!
//! Run with: `cargo run --example quick_start`

use txn_db::{Db, TxnError};

fn main() -> Result<(), TxnError> {
    // An in-memory database, ready to use.
    let db = Db::new();

    // Write two keys atomically. Either both land or neither does.
    let mut tx = db.begin();
    tx.put(b"user:1:name".to_vec(), b"ada".to_vec());
    tx.put(b"user:1:role".to_vec(), b"admin".to_vec());
    let commit_ts = tx.commit()?;
    println!("committed at {commit_ts}");

    // A later transaction reads a consistent snapshot of the database.
    let tx = db.begin();
    let name = tx.get(b"user:1:name")?;
    let role = tx.get(b"user:1:role")?;
    println!(
        "name = {}, role = {}",
        as_str(name.as_deref()),
        as_str(role.as_deref())
    );

    // A key that was never written reads as absent.
    println!("missing key present: {}", tx.get(b"user:2:name")?.is_some());

    Ok(())
}

fn as_str(bytes: Option<&[u8]>) -> String {
    bytes.map_or_else(
        || "<absent>".to_string(),
        |b| String::from_utf8_lossy(b).into_owned(),
    )
}
