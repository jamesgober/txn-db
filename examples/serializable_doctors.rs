//! The on-call doctors problem — the textbook case for serializable isolation.
//!
//! Two doctors are on call. The rule is that at least one must always be on
//! call, so a doctor may only go off call after checking that the *other* is
//! still on. Two doctors who check at the same instant each see the other on
//! call and each take themselves off — and now nobody is on call. That is write
//! skew: both transactions read the same rows and write different ones, so plain
//! snapshot isolation lets them both commit even though no serial order would.
//!
//! This example runs the schedule twice: once under snapshot isolation, where
//! the invariant breaks, and once under serializable isolation, where the second
//! transaction is rejected and the invariant holds.
//!
//! Run with: `cargo run --example serializable_doctors --features serializable`

use txn_db::{Db, Transaction, TxnError};

const ALICE: &[u8] = b"on_call:alice";
const BOB: &[u8] = b"on_call:bob";

fn main() -> Result<(), TxnError> {
    println!("under snapshot isolation:");
    let si = run(Db::new(), Isolation::Snapshot)?;
    report(&si);

    println!("\nunder serializable isolation:");
    let ser = run(Db::new(), Isolation::Serializable)?;
    report(&ser);

    Ok(())
}

#[derive(Clone, Copy)]
enum Isolation {
    Snapshot,
    Serializable,
}

/// The outcome of running the write-skew schedule.
struct Outcome {
    alice_committed: bool,
    bob_committed: bool,
    on_call: usize,
}

/// Seed both doctors on call, then run the two "go off call" transactions from
/// the same snapshot under the requested isolation level.
fn run(db: Db, isolation: Isolation) -> Result<Outcome, TxnError> {
    let mut seed = db.begin();
    seed.put(ALICE.to_vec(), vec![1]);
    seed.put(BOB.to_vec(), vec![1]);
    seed.commit()?;

    // Both transactions take their snapshot before either commits.
    let mut alice = begin(&db, isolation);
    let mut bob = begin(&db, isolation);

    let alice_committed = try_go_off_call(&mut alice, ALICE)?;
    let bob_committed = try_go_off_call(&mut bob, BOB)?;

    // Commit in order; the second may be rejected under serializable isolation.
    let alice_committed = alice_committed && alice.commit().is_ok();
    let bob_committed = bob_committed && bob.commit().is_ok();

    // Count who is still on call now.
    let snap = db.snapshot();
    let on_call = [ALICE, BOB]
        .into_iter()
        .filter(|doctor| {
            snap.get(doctor)
                .map(|v| v.as_deref() == Some(&[1]))
                .unwrap_or(false)
        })
        .count();

    Ok(Outcome {
        alice_committed,
        bob_committed,
        on_call,
    })
}

fn begin(db: &Db, isolation: Isolation) -> Transaction {
    match isolation {
        Isolation::Snapshot => db.begin(),
        Isolation::Serializable => db.begin_serializable(),
    }
}

/// Go off call only if the other doctor is still on. Returns whether the
/// transaction decided to write (and so should try to commit).
fn try_go_off_call(tx: &mut Transaction, doctor: &[u8]) -> Result<bool, TxnError> {
    let on_call = [ALICE, BOB]
        .into_iter()
        .filter(|d| matches!(tx.get(d), Ok(Some(v)) if v.as_ref() == [1]))
        .count();

    if on_call >= 2 {
        tx.put(doctor.to_vec(), vec![0]);
        Ok(true)
    } else {
        Ok(false)
    }
}

fn report(outcome: &Outcome) {
    println!(
        "  alice went off call: {}, bob went off call: {}",
        outcome.alice_committed, outcome.bob_committed
    );
    println!("  doctors still on call: {}", outcome.on_call);
    if outcome.on_call == 0 {
        println!("  invariant VIOLATED — nobody is on call");
    } else {
        println!("  invariant held — at least one doctor on call");
    }
}
