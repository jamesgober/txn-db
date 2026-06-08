<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br><b>txn-db</b><br>
    <sub><sup>PERFORMANCE</sup></sub>
</h1>
<div align="center">
    <sup>
        <a href="../README.md" title="Project Home"><b>HOME</b></a>
        <span>&nbsp;│&nbsp;</span>
        <a href="./API.md" title="API Reference"><b>API</b></a>
        <span>&nbsp;│&nbsp;</span>
        <span>PERFORMANCE</span>
    </sup>
</div>
<br>

> Hot-path latencies and contention-scaling for `txn-db`, with the methodology to
> reproduce them. Numbers are from one machine and are meant to be read as
> *shape* — the cost of an operation and how throughput scales with concurrency —
> not as absolute guarantees for your hardware.

## Methodology

Two `criterion` benchmark suites under [`benches/`](../benches):

- [`txn_bench`](../benches/txn_bench.rs) — single-threaded hot paths: a point
  read, single-key and batched commits, and the read-modify-write loop. These are
  stable and serve as the regression baseline.
- [`contention`](../benches/contention.rs) — throughput as concurrent writers
  scale across 1, 4, 16, and 64 threads, in three workloads: `disjoint_commits`
  (each writer commits to its own keys), `contended_commits` (all writers retry
  against one shared counter), and `begin_drop` (transactions begun and dropped
  without committing, isolating per-transaction bookkeeping).

Run them with:

```bash
cargo bench --bench txn_bench
cargo bench --bench contention
```

Measurements below were taken on Windows 11 (x86_64), Rust 1.95 stable, release
profile (`lto = "fat"`, `codegen-units = 1`), on an otherwise-busy developer
machine. The multi-threaded `contention` figures carry real run-to-run variance
from OS scheduling and background load; treat them as the scaling *shape*, not
exact rates. The single-threaded `txn_bench` figures are stable to a few percent.

## Single-threaded hot paths

In-memory store, default features, one thread:

| Operation | Time | Notes |
|-----------|------|-------|
| Point read (`Snapshot::get`, existing key) | ~24 ns | One shard read-lock + binary search + `Arc` clone. |
| Single-key commit | ~0.24 µs | `begin` → `put` → `commit` of one key. |
| Read-modify-write (one key) | ~0.31 µs | `get` + `put` + `commit`. |
| Batch commit, 8 keys | ~1.5 µs | One transaction, eight writes. |
| Batch commit, 64 keys | ~11 µs | |
| Batch commit, 512 keys | ~88 µs | |

## Contention scaling

Committed transactions per second, by writer count. Independent (`disjoint`) work
scales with sharding until per-transaction global bookkeeping — the
garbage-collection reader registry and the commit watermark — bounds it past a
few writers. The single-key `contended` workload is the worst case: every writer
fights over one key and most commits retry.

| Writers | `disjoint_commits` | `contended_commits` |
|--------:|-------------------:|--------------------:|
| 1 | ~1.9 M tx/s | ~1.9 M tx/s |
| 4 | ~3.2 M tx/s | ~1.2 M tx/s |
| 16 | ~1.3 M tx/s | ~0.27 M tx/s |
| 64 | ~1.1 M tx/s | ~0.03 M tx/s |

Reading the curve: disjoint throughput roughly doubles from 1 to 4 writers as
independent commits land on different shards in parallel, then flattens as the
global per-transaction operations (registering a reader for GC, advancing the
commit watermark) serialize. The contended workload degrades with writer count by
design — a single hot key cannot be committed in parallel, and added writers only
add retries.

## Optimization log

### v0.6.0 — single-write commit fast path

The commit path's general case locks every shard a transaction touches, in sorted
order, after building per-key shard-index vectors and a guard vector and mapping
keys back to guards with binary search. The overwhelmingly common transaction —
a single write with no read set to validate — needs none of that. v0.6 adds a
fast path that locks the one shard, validates the one key, and applies it, with
no intermediate allocations.

Measured against the v0.5 general path (`txn_bench`, `criterion --baseline`):

| Benchmark | Change |
|-----------|-------:|
| `commit/single_key` | **−45 %** |
| `batch_commit/1` (one key) | **−41 %** |
| `read_modify_write` | **−37 %** |
| `point_read` | ~0 % (unchanged) |
| `batch_commit/512` | ~0 % (unchanged) |

No benchmark regressed. Single-key commits — the bulk of transactional traffic —
got materially cheaper, while reads and large multi-key batches were unaffected.

## Known limits and future work

- **Per-transaction global locks.** Each transaction registers and unregisters a
  reader timestamp (for GC) under one mutex, and each commit advances the read
  watermark under another. These bound `disjoint` scaling past ~4 writers. A
  lock-free watermark fast path for in-order commits, and a cheaper reader
  registry, are the natural next targets.
- **Single hot key.** No design makes concurrent commits to *one* key parallel;
  the answer is application-level sharding of the key, or batching.

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
