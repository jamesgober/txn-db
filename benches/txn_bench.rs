//! Hot-path benchmarks for the transaction engine.
//!
//! These track the operations that dominate a transactional workload: a point
//! read from a snapshot, a single-key commit, a multi-key commit, and the
//! read-modify-write loop that backs an optimistic update. Run with
//! `cargo bench`; criterion records baselines under `target/criterion` so
//! regressions are visible across runs.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use txn_db::Db;

/// Populate a database with `n` sequential keys, each an 8-byte value.
fn seeded_db(n: u64) -> Db {
    let db = Db::new();
    let mut tx = db.begin();
    for i in 0..n {
        tx.put(i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec());
    }
    tx.commit().expect("seed commit");
    db
}

fn bench_point_read(c: &mut Criterion) {
    let db = seeded_db(10_000);
    let mut group = c.benchmark_group("point_read");
    group.throughput(Throughput::Elements(1));
    group.bench_function("get_existing_key", |b| {
        let snap = db.snapshot();
        let key = 4_242u64.to_le_bytes();
        b.iter(|| {
            let value = snap.get(black_box(&key)).expect("read");
            black_box(value)
        });
    });
    group.finish();
}

fn bench_single_key_commit(c: &mut Criterion) {
    let db = seeded_db(1_000);
    let mut group = c.benchmark_group("commit");
    group.throughput(Throughput::Elements(1));
    group.bench_function("single_key", |b| {
        let mut n = 0u64;
        b.iter(|| {
            let mut tx = db.begin();
            tx.put(b"hot".to_vec(), n.to_le_bytes().to_vec());
            n = n.wrapping_add(1);
            tx.commit().expect("commit")
        });
    });
    group.finish();
}

fn bench_batch_commit(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_commit");
    for batch in [1u64, 8, 64, 512] {
        group.throughput(Throughput::Elements(batch));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &batch, |b, &batch| {
            let db = Db::new();
            b.iter(|| {
                let mut tx = db.begin();
                for i in 0..batch {
                    tx.put(i.to_le_bytes().to_vec(), i.to_le_bytes().to_vec());
                }
                tx.commit().expect("commit")
            });
        });
    }
    group.finish();
}

fn bench_read_modify_write(c: &mut Criterion) {
    let db = seeded_db(1);
    {
        let mut tx = db.begin();
        tx.put(b"counter".to_vec(), 0u64.to_le_bytes().to_vec());
        tx.commit().expect("seed counter");
    }
    let mut group = c.benchmark_group("read_modify_write");
    group.throughput(Throughput::Elements(1));
    group.bench_function("increment_uncontended", |b| {
        b.iter(|| {
            let mut tx = db.begin();
            let mut buf = [0u8; 8];
            if let Some(bytes) = tx.get(b"counter").expect("read") {
                buf.copy_from_slice(&bytes[..8]);
            }
            let next = u64::from_le_bytes(buf).wrapping_add(1);
            tx.put(b"counter".to_vec(), next.to_le_bytes().to_vec());
            tx.commit().expect("commit")
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_point_read,
    bench_single_key_commit,
    bench_batch_commit,
    bench_read_modify_write,
);
criterion_main!(benches);
