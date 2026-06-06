//! Move money between two accounts inside one transaction, retrying on
//! conflict. This is the canonical case for a transaction engine: the debit and
//! the credit must both apply or neither must, and concurrent transfers must
//! not lose an update.
//!
//! Run with: `cargo run --example bank_transfer`

use txn_db::{Db, Transaction, TxnError};

fn main() -> Result<(), TxnError> {
    let db = Db::new();

    // Open two accounts.
    let mut tx = db.begin();
    set_balance(&mut tx, b"alice", 100);
    set_balance(&mut tx, b"bob", 0);
    tx.commit()?;

    // Transfer 30 from alice to bob, atomically and with conflict retries.
    transfer(&db, b"alice", b"bob", 30)?;

    let snap = db.snapshot();
    println!("alice = {}", balance(&snap_get(&snap, b"alice")?));
    println!("bob   = {}", balance(&snap_get(&snap, b"bob")?));

    // A transfer that would overdraw is rejected without touching either
    // account.
    match transfer(&db, b"bob", b"alice", 1_000) {
        Err(TxnError::Store { detail, .. }) => println!("rejected: {detail}"),
        other => println!("unexpected: {other:?}"),
    }

    Ok(())
}

/// Move `amount` from `from` to `to`, retrying while the commit conflicts.
fn transfer(db: &Db, from: &[u8], to: &[u8], amount: u64) -> Result<(), TxnError> {
    loop {
        let mut tx = db.begin();
        let from_balance = balance(&tx.get(from)?);
        let to_balance = balance(&tx.get(to)?);

        if from_balance < amount {
            // Surface a domain rejection through the store error channel.
            return Err(TxnError::store("transfer", "insufficient funds"));
        }

        set_balance(&mut tx, from, from_balance - amount);
        set_balance(&mut tx, to, to_balance + amount);

        match tx.commit() {
            Ok(_) => return Ok(()),
            Err(e) if e.is_retryable() => continue,
            Err(e) => return Err(e),
        }
    }
}

fn set_balance(tx: &mut Transaction, account: &[u8], value: u64) {
    tx.put(account.to_vec(), value.to_le_bytes().to_vec());
}

fn balance(value: &Option<std::sync::Arc<[u8]>>) -> u64 {
    value.as_ref().map_or(0, |bytes| {
        let mut buf = [0u8; 8];
        buf.copy_from_slice(&bytes[..8]);
        u64::from_le_bytes(buf)
    })
}

fn snap_get(snap: &txn_db::Snapshot, key: &[u8]) -> Result<Option<std::sync::Arc<[u8]>>, TxnError> {
    snap.get(key)
}
