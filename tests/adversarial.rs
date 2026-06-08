//! Hardening tests: adversarial schedules and edge cases.
//!
//! These cover the v0.7 goals — long-running readers against aggressive garbage
//! collection, abort storms on a single hot key, very large transactions, and
//! awkward key/value sizes. They assert the engine stays correct and makes
//! progress (no deadlock, no livelock, no lost update) under conditions chosen
//! to break it, rather than the happy path the other suites cover.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;

use txn_db::Db;

fn read_u64(bytes: &Arc<[u8]>) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_le_bytes(buf)
}

/// A snapshot must keep reading its values no matter how hard other threads
/// commit and how aggressively garbage collection runs alongside them. The
/// snapshot is taken after several versions exist, so the collector has older
/// history below the snapshot it is free to reclaim — and must reclaim *only*
/// that, never the version the snapshot still sees.
#[test]
fn long_reader_survives_aggressive_gc() {
    const KEYS: u8 = 32;
    const SEED_ROUNDS: u8 = 6;
    let db = Db::new();

    // Build up several versions of every key (values 0..SEED_ROUNDS-1), then
    // snapshot. The snapshot sees the last seeded value and pins it.
    for round in 0..SEED_ROUNDS {
        let mut tx = db.begin();
        for k in 0..KEYS {
            tx.put(vec![k], vec![round]);
        }
        tx.commit().unwrap();
    }
    let pinned_value = SEED_ROUNDS - 1;
    let snapshot = db.snapshot();

    let stop = Arc::new(AtomicBool::new(false));

    // A garbage-collection thread hammering collect_garbage in a loop.
    let gc = {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut total = 0usize;
            while !stop.load(Ordering::Relaxed) {
                total += db.collect_garbage();
            }
            total
        })
    };

    // Writers overwriting every key many times, racing the collector. They are
    // best-effort: overwrites of the same key conflict, and that is fine here.
    let writers: Vec<_> = (0..4u8)
        .map(|w| {
            let db = db.clone();
            thread::spawn(move || {
                for round in 0..200u16 {
                    for k in 0..KEYS {
                        let mut tx = db.begin();
                        tx.put(
                            vec![k],
                            vec![100u8.wrapping_add(w).wrapping_add(round as u8)],
                        );
                        let _ = tx.commit();
                    }
                }
            })
        })
        .collect();

    for w in writers {
        w.join().unwrap();
    }
    stop.store(true, Ordering::Relaxed);
    let reclaimed = gc.join().unwrap();

    // The snapshot is unmoved: every key still reads the value it pinned.
    for k in 0..KEYS {
        assert_eq!(
            snapshot.get(&[k]).unwrap().as_deref(),
            Some(&[pinned_value][..]),
            "snapshot must keep seeing its pinned value for key {k}"
        );
    }
    // Collection ran and reclaimed the pre-snapshot history it was free to.
    assert!(
        reclaimed > 0,
        "aggressive GC should have reclaimed old versions"
    );

    // Once the snapshot is dropped, a final collection bounds the history.
    drop(snapshot);
    let _ = db.collect_garbage();
    for k in 0..KEYS {
        assert!(db.snapshot().get(&[k]).unwrap().is_some());
    }
}

/// Many threads fight over one key with a read-modify-write retry loop. The
/// engine must lose no update (final value equals the number of increments) and
/// must finish — an abort storm must not deadlock or livelock.
#[test]
fn abort_storm_loses_no_update() {
    const THREADS: u64 = 12;
    const PER_THREAD: u64 = 400;

    let db = Db::new();
    {
        let mut tx = db.begin();
        tx.put(b"hot".to_vec(), 0u64.to_le_bytes().to_vec());
        tx.commit().unwrap();
    }

    let handles: Vec<_> = (0..THREADS)
        .map(|_| {
            let db = db.clone();
            thread::spawn(move || {
                for _ in 0..PER_THREAD {
                    loop {
                        let mut tx = db.begin();
                        let current = tx.get(b"hot").unwrap().map_or(0, |v| read_u64(&v));
                        tx.put(b"hot".to_vec(), (current + 1).to_le_bytes().to_vec());
                        match tx.commit() {
                            Ok(_) => break,
                            Err(e) if e.is_retryable() => continue,
                            Err(e) => panic!("unexpected error: {e}"),
                        }
                    }
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("worker panicked");
    }

    let snap = db.snapshot();
    let total = read_u64(&snap.get(b"hot").unwrap().unwrap());
    assert_eq!(total, THREADS * PER_THREAD);
}

/// A single transaction with a very large write set spanning every shard commits
/// atomically and reads back intact, then a second large transaction overwrites
/// part of it.
#[test]
fn very_large_transaction() {
    const N: u32 = 20_000;
    let db = Db::new();

    let mut tx = db.begin();
    for i in 0..N {
        tx.put(i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec());
    }
    tx.commit().unwrap();

    let snap = db.snapshot();
    for i in [0u32, 1, N / 2, N - 1] {
        let got = snap.get(&i.to_le_bytes()).unwrap().unwrap();
        assert_eq!(u32::from_le_bytes(got[..4].try_into().unwrap()), i);
    }

    // Overwrite the even keys in one more large transaction.
    let mut tx = db.begin();
    for i in (0..N).step_by(2) {
        tx.put(i.to_le_bytes().to_vec(), u32::MAX.to_le_bytes().to_vec());
    }
    tx.commit().unwrap();

    let snap = db.snapshot();
    let even = snap.get(&0u32.to_le_bytes()).unwrap().unwrap();
    let odd = snap.get(&1u32.to_le_bytes()).unwrap().unwrap();
    assert_eq!(u32::from_le_bytes(even[..4].try_into().unwrap()), u32::MAX);
    assert_eq!(u32::from_le_bytes(odd[..4].try_into().unwrap()), 1);
}

/// Awkward key and value sizes — empty and large — round-trip correctly.
#[test]
fn edge_case_key_and_value_sizes() {
    let db = Db::new();
    let big_value = vec![0xABu8; 1 << 20]; // 1 MiB
    let big_key = vec![0x5Au8; 4096];

    let mut tx = db.begin();
    tx.put(Vec::new(), b"empty-key".to_vec()); // empty key
    tx.put(b"empty-value".to_vec(), Vec::new()); // empty value
    tx.put(big_key.clone(), big_value.clone()); // large key and value
    tx.commit().unwrap();

    let snap = db.snapshot();
    assert_eq!(snap.get(b"").unwrap().as_deref(), Some(&b"empty-key"[..]));
    assert_eq!(snap.get(b"empty-value").unwrap().as_deref(), Some(&[][..]));
    assert_eq!(snap.get(&big_key).unwrap().unwrap().len(), big_value.len());
}

/// A mixed adversarial workload: every thread owns a key range and runs an
/// arbitrary sequence of puts, deletes, and snapshot reads while a collector
/// runs. Each thread verifies its own keys end in the state it last wrote — no
/// cross-thread corruption, no panic, no hang.
#[test]
fn mixed_workload_keeps_per_thread_consistency() {
    const THREADS: u8 = 8;
    const KEYS_PER_THREAD: u8 = 16;
    let db = Db::new();

    let stop = Arc::new(AtomicBool::new(false));
    let gc = {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let _ = db.collect_garbage();
            }
        })
    };

    let handles: Vec<_> = (0..THREADS)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                let mut expected = [0u8; KEYS_PER_THREAD as usize];
                for round in 1..=150u8 {
                    for k in 0..KEYS_PER_THREAD {
                        let key = vec![t, k];
                        let deleting = round % 5 == 0;
                        // Even though no other thread touches this key, a commit
                        // can still conflict with this thread's *own* previous
                        // write when the read watermark lags behind it under
                        // concurrent out-of-order commits. That is correct
                        // snapshot-isolation behavior, so retry.
                        loop {
                            let mut tx = db.begin();
                            if deleting {
                                tx.delete(key.clone());
                            } else {
                                tx.put(key.clone(), vec![round]);
                            }
                            match tx.commit() {
                                Ok(_) => break,
                                Err(e) if e.is_retryable() => continue,
                                Err(e) => panic!("unexpected error: {e}"),
                            }
                        }
                        expected[k as usize] = if deleting { 0 } else { round };
                    }
                    // Read a few keys back through a snapshot mid-flight.
                    let snap = db.snapshot();
                    let _ = snap.get(&[t, 0]).unwrap();
                }
                expected
            })
        })
        .collect();

    let mut finals = Vec::new();
    for h in handles {
        finals.push(h.join().expect("worker panicked"));
    }
    stop.store(true, Ordering::Relaxed);
    gc.join().unwrap();

    // Each thread's keys end exactly as that thread last left them.
    let snap = db.snapshot();
    for (t, expected) in finals.iter().enumerate() {
        for k in 0..KEYS_PER_THREAD {
            let got = snap.get(&[t as u8, k]).unwrap();
            match expected[k as usize] {
                0 => assert_eq!(got, None, "thread {t} key {k} should be deleted"),
                v => assert_eq!(got.as_deref(), Some(&[v][..]), "thread {t} key {k}"),
            }
        }
    }
}
