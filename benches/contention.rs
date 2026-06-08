//! Contention-scaling benchmarks: transaction throughput as the number of
//! concurrent writers grows.
//!
//! Three workloads at 1, 4, 16, and 64 writers:
//!
//! - `disjoint_commits` — each writer commits to its own keys, so nothing
//!   conflicts. This measures how well the sharded commit path scales when work
//!   is independent.
//! - `contended_commits` — every writer hammers one shared counter with a
//!   read-modify-write retry loop, so they fight over a single key. This
//!   measures the conflict-detection and retry path under worst-case contention.
//! - `begin_drop` — writers only begin and drop transactions. This isolates the
//!   per-transaction bookkeeping (reader registration for garbage collection).
//!
//! Run with `cargo bench --bench contention`. Criterion reports throughput in
//! elements (committed transactions) per second; compare across writer counts to
//! read the scaling curve.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use txn_db::Db;

/// Transactions committed per writer in one workload run.
const PER_WRITER: u64 = 250;
const WRITER_COUNTS: [u64; 4] = [1, 4, 16, 64];

/// Run `writers` threads, each invoking `work(thread_index)` once they are all
/// ready, and return how long the parallel section took.
fn run_parallel(writers: u64, work: impl Fn(u64) + Sync) -> Duration {
    let work = &work;
    let start = Arc::new(AtomicBool::new(false));
    thread::scope(|scope| {
        let handles: Vec<_> = (0..writers)
            .map(|t| {
                let start = Arc::clone(&start);
                scope.spawn(move || {
                    while !start.load(Ordering::Acquire) {
                        std::hint::spin_loop();
                    }
                    work(t);
                })
            })
            .collect();
        let begin = Instant::now();
        start.store(true, Ordering::Release);
        for handle in handles {
            handle.join().expect("writer thread panicked");
        }
        begin.elapsed()
    })
}

fn bench_disjoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("disjoint_commits");
    for &writers in &WRITER_COUNTS {
        group.throughput(Throughput::Elements(writers * PER_WRITER));
        group.bench_with_input(
            BenchmarkId::from_parameter(writers),
            &writers,
            |b, &writers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let db = Db::new();
                        total += run_parallel(writers, |t| {
                            for i in 0..PER_WRITER {
                                let mut tx = db.begin();
                                // Keys are per-writer, so commits never conflict.
                                tx.put(key(t, i), i.to_le_bytes().to_vec());
                                tx.commit().expect("commit");
                            }
                        });
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

fn bench_contended(c: &mut Criterion) {
    let mut group = c.benchmark_group("contended_commits");
    for &writers in &WRITER_COUNTS {
        group.throughput(Throughput::Elements(writers * PER_WRITER));
        group.bench_with_input(
            BenchmarkId::from_parameter(writers),
            &writers,
            |b, &writers| {
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        let db = Db::new();
                        {
                            let mut tx = db.begin();
                            tx.put(b"counter".to_vec(), 0u64.to_le_bytes().to_vec());
                            tx.commit().expect("seed");
                        }
                        total += run_parallel(writers, |_| {
                            for _ in 0..PER_WRITER {
                                loop {
                                    let mut tx = db.begin();
                                    let current =
                                        tx.get(b"counter").expect("read").map_or(0, |v| {
                                            u64::from_le_bytes(v[..8].try_into().unwrap())
                                        });
                                    tx.put(
                                        b"counter".to_vec(),
                                        (current + 1).to_le_bytes().to_vec(),
                                    );
                                    match tx.commit() {
                                        Ok(_) => break,
                                        Err(e) if e.is_retryable() => continue,
                                        Err(e) => panic!("{e}"),
                                    }
                                }
                            }
                        });
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

fn bench_begin_drop(c: &mut Criterion) {
    let mut group = c.benchmark_group("begin_drop");
    for &writers in &WRITER_COUNTS {
        group.throughput(Throughput::Elements(writers * PER_WRITER));
        group.bench_with_input(
            BenchmarkId::from_parameter(writers),
            &writers,
            |b, &writers| {
                let db = Db::new();
                b.iter_custom(|iters| {
                    let mut total = Duration::ZERO;
                    for _ in 0..iters {
                        total += run_parallel(writers, |_| {
                            for _ in 0..PER_WRITER {
                                let tx = db.begin();
                                std::hint::black_box(&tx);
                            }
                        });
                    }
                    total
                });
            },
        );
    }
    group.finish();
}

fn key(writer: u64, i: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(16);
    k.extend_from_slice(&writer.to_le_bytes());
    k.extend_from_slice(&i.to_le_bytes());
    k
}

criterion_group!(benches, bench_disjoint, bench_contended, bench_begin_drop);
criterion_main!(benches);
