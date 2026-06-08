//! An honest comparison against the baseline a developer reaches for without
//! MVCC: a single `RwLock<HashMap>`.
//!
//! `txn-db` is a transaction layer, not a full storage engine, so the fair
//! comparison is not against `sled` or `redb` but against the obvious
//! hand-rolled alternative — one reader-writer lock over a map. The point is to
//! show, with numbers, what multi-version concurrency control buys and what it
//! costs:
//!
//! - `read_latency` / `write_latency` — single-threaded. The `RwLock<HashMap>`
//!   has no versioning and no commit machinery, so uncontended it is the floor
//!   txn-db pays a small, measured overhead above.
//! - `reads_under_writer` — point reads on many threads while one writer commits
//!   continuously. This is where MVCC earns its keep: a snapshot read never waits
//!   for a writer, while every reader on the `RwLock` baseline blocks whenever
//!   the writer holds the lock.
//!
//! Run with `cargo bench --bench comparison`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use txn_db::Db;

const SEED_KEYS: u64 = 10_000;

fn key(i: u64) -> Vec<u8> {
    i.to_le_bytes().to_vec()
}

/// The naive baseline: one lock over a map.
type Baseline = RwLock<HashMap<Vec<u8>, Vec<u8>>>;

fn seeded_baseline() -> Arc<Baseline> {
    let mut map = HashMap::new();
    for i in 0..SEED_KEYS {
        let _ = map.insert(key(i), i.to_le_bytes().to_vec());
    }
    Arc::new(RwLock::new(map))
}

fn seeded_txn() -> Db {
    let db = Db::new();
    let mut tx = db.begin();
    for i in 0..SEED_KEYS {
        tx.put(key(i), i.to_le_bytes().to_vec());
    }
    tx.commit().expect("seed");
    db
}

fn bench_read_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_latency");
    let k = key(SEED_KEYS / 2);

    let baseline = seeded_baseline();
    group.bench_function("rwlock_hashmap", |b| {
        b.iter(|| {
            let guard = baseline.read().expect("read lock");
            std::hint::black_box(guard.get(&k).cloned())
        });
    });

    let db = seeded_txn();
    group.bench_function("txn_db_snapshot", |b| {
        let snap = db.snapshot();
        b.iter(|| std::hint::black_box(snap.get(&k).expect("read")));
    });
    group.finish();
}

fn bench_write_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_latency");

    let baseline = seeded_baseline();
    group.bench_function("rwlock_hashmap", |b| {
        let mut n = 0u64;
        b.iter(|| {
            let mut guard = baseline.write().expect("write lock");
            let _ = guard.insert(b"hot".to_vec(), n.to_le_bytes().to_vec());
            n = n.wrapping_add(1);
        });
    });

    let db = seeded_txn();
    group.bench_function("txn_db_autocommit", |b| {
        let mut n = 0u64;
        b.iter(|| {
            db.put(b"hot".to_vec(), n.to_le_bytes().to_vec())
                .expect("put");
            n = n.wrapping_add(1);
        });
    });
    group.finish();
}

fn bench_reads_under_writer(c: &mut Criterion) {
    const READS_PER_THREAD: u64 = 2_000;
    let mut group = c.benchmark_group("reads_under_writer");

    for &readers in &[2u64, 8, 32] {
        group.throughput(Throughput::Elements(readers * READS_PER_THREAD));

        group.bench_with_input(
            BenchmarkId::new("rwlock_hashmap", readers),
            &readers,
            |b, &readers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let map = seeded_baseline();
                        total += run_reads_under_writer(
                            readers,
                            || {
                                // Writer: take the exclusive lock and insert.
                                let map = Arc::clone(&map);
                                move |stop: &AtomicBool| {
                                    let mut n = 0u64;
                                    while !stop.load(Ordering::Relaxed) {
                                        let mut g = map.write().expect("w");
                                        let _ = g.insert(b"hot".to_vec(), n.to_le_bytes().to_vec());
                                        n = n.wrapping_add(1);
                                    }
                                }
                            },
                            {
                                let map = Arc::clone(&map);
                                move |t: u64| {
                                    let k = key((t * 7) % SEED_KEYS);
                                    for _ in 0..READS_PER_THREAD {
                                        let g = map.read().expect("r");
                                        std::hint::black_box(g.get(&k).cloned());
                                    }
                                }
                            },
                        );
                    }
                    total
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("txn_db", readers),
            &readers,
            |b, &readers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let db = seeded_txn();
                        total += run_reads_under_writer(
                            readers,
                            || {
                                let db = db.clone();
                                move |stop: &AtomicBool| {
                                    let mut n = 0u64;
                                    while !stop.load(Ordering::Relaxed) {
                                        db.put(b"hot".to_vec(), n.to_le_bytes().to_vec())
                                            .expect("put");
                                        n = n.wrapping_add(1);
                                    }
                                }
                            },
                            {
                                let db = db.clone();
                                move |t: u64| {
                                    let k = key((t * 7) % SEED_KEYS);
                                    let snap = db.snapshot();
                                    for _ in 0..READS_PER_THREAD {
                                        std::hint::black_box(snap.get(&k).expect("read"));
                                    }
                                }
                            },
                        );
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

/// Run `readers` reader threads to completion while one writer runs in the
/// background, and return how long the readers took.
fn run_reads_under_writer<W, WF, R>(readers: u64, make_writer: W, read: R) -> Duration
where
    W: FnOnce() -> WF,
    WF: FnOnce(&AtomicBool) + Send + 'static,
    R: Fn(u64) + Sync,
{
    let stop = Arc::new(AtomicBool::new(false));
    let writer_body = make_writer();
    let writer_stop = Arc::clone(&stop);
    let writer = thread::spawn(move || writer_body(&writer_stop));

    let read = &read;
    let elapsed = thread::scope(|scope| {
        let handles: Vec<_> = (0..readers).map(|t| scope.spawn(move || read(t))).collect();
        let begin = Instant::now();
        for h in handles {
            h.join().expect("reader");
        }
        begin.elapsed()
    });

    stop.store(true, Ordering::Relaxed);
    writer.join().expect("writer");
    elapsed
}

criterion_group!(
    benches,
    bench_read_latency,
    bench_write_latency,
    bench_reads_under_writer
);
criterion_main!(benches);
