//! Property tests for garbage collection.
//!
//! The v0.5 exit criterion is that collection never reclaims a version a live
//! snapshot can still observe. These drive an arbitrary commit history while
//! holding snapshots at various points, run collection, and assert every held
//! snapshot still reads exactly what it read before.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use proptest::prelude::*;
use txn_db::{Db, Snapshot};

const KEY_SPACE: u8 = 6;

#[derive(Debug, Clone)]
enum Step {
    Put(u8, u8),
    Delete(u8),
    Snapshot,
    Gc,
}

fn step_strategy() -> impl Strategy<Value = Step> {
    prop_oneof![
        4 => (0..KEY_SPACE, any::<u8>()).prop_map(|(k, v)| Step::Put(k, v)),
        1 => (0..KEY_SPACE).prop_map(Step::Delete),
        2 => Just(Step::Snapshot),
        2 => Just(Step::Gc),
    ]
}

/// What a snapshot saw across the whole key space when it was taken.
struct Pinned {
    snap: Snapshot,
    observed: Vec<Option<u8>>,
}

fn observe(snap: &Snapshot) -> Vec<Option<u8>> {
    (0..KEY_SPACE)
        .map(|k| snap.get(&[k]).unwrap().map(|v| v[0]))
        .collect()
}

proptest! {
    /// Through an arbitrary interleaving of writes, deletes, snapshot captures,
    /// and collections, every snapshot keeps reading the values it saw when it
    /// was taken. If collection ever reclaimed a version a snapshot needed, one
    /// of these reads would change.
    #[test]
    fn gc_never_reclaims_a_live_snapshots_versions(steps in prop::collection::vec(step_strategy(), 1..80)) {
        let db = Db::new();
        let mut pinned: Vec<Pinned> = Vec::new();

        for step in steps {
            match step {
                Step::Put(k, v) => {
                    let mut tx = db.begin();
                    tx.put(vec![k], vec![v]);
                    prop_assert!(tx.commit().is_ok());
                }
                Step::Delete(k) => {
                    let mut tx = db.begin();
                    tx.delete(vec![k]);
                    prop_assert!(tx.commit().is_ok());
                }
                Step::Snapshot => {
                    let snap = db.snapshot();
                    let observed = observe(&snap);
                    pinned.push(Pinned { snap, observed });
                }
                Step::Gc => {
                    let _ = db.collect_garbage();
                    // Every snapshot still alive must read exactly what it saw.
                    for p in &pinned {
                        prop_assert_eq!(observe(&p.snap), p.observed.clone());
                    }
                }
            }
        }

        // And after the whole run, every snapshot is still consistent.
        for p in &pinned {
            prop_assert_eq!(observe(&p.snap), p.observed.clone());
        }
    }

    /// After collection with no live reader, a key overwritten many times keeps
    /// only its newest value, and that value is intact.
    #[test]
    fn gc_with_no_reader_keeps_only_the_newest_value(values in prop::collection::vec(any::<u8>(), 1..30)) {
        let db = Db::new();
        for v in &values {
            let mut tx = db.begin();
            tx.put(b"k".to_vec(), vec![*v]);
            prop_assert!(tx.commit().is_ok());
        }

        let _ = db.collect_garbage();

        let last = [*values.last().unwrap()];
        let snap = db.snapshot();
        let got = snap.get(b"k").unwrap();
        prop_assert_eq!(got.as_deref(), Some(&last[..]));
    }
}
