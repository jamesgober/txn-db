<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br>
    <b>txn-db</b>
    <br>
    <sub><sup>MVCC TRANSACTION ENGINE</sup></sub>
</h1>

<div align="center">
    <a href="https://crates.io/crates/txn-db"><img alt="Crates.io" src="https://img.shields.io/crates/v/txn-db"></a>
    <a href="https://crates.io/crates/txn-db" alt="Download txn-db"><img alt="Crates.io Downloads" src="https://img.shields.io/crates/d/txn-db?color=%230099ff"></a>
    <a href="https://docs.rs/txn-db" title="txn-db Documentation"><img alt="docs.rs" src="https://img.shields.io/docsrs/txn-db"></a>
    <a href="https://github.com/jamesgober/txn-db/actions"><img alt="GitHub CI" src="https://github.com/jamesgober/txn-db/actions/workflows/ci.yml/badge.svg"></a>
    <a href="https://github.com/rust-lang/rfcs/blob/master/text/2495-min-rust-version.md" title="MSRV"><img alt="MSRV" src="https://img.shields.io/badge/MSRV-1.85%2B-blue"></a>
</div>

<br>

<div align="left">
    <p>
        <strong>txn-db</strong> is a <b>multi-version concurrency control</b> transaction engine: the layer that turns a key-value store into a transactional database. Each write produces a new version tagged with a commit timestamp, so readers get a stable snapshot without ever blocking writers, and writers detect conflicts at commit time rather than holding locks for the duration of a transaction.
    </p>
    <p>
        It is deliberately a <b>layer, not a store</b>: the version store is a trait, so <code>txn-db</code> composes on top of <code>lsm-db</code> or any other backing store, and its durable commit log is <code>wal-db</code>. Snapshot isolation is the default; serializable isolation (SSI) is a feature flag.
    </p>
    <p>
        The common case is <code>begin</code> / <code>get</code> / <code>put</code> / <code>commit</code>, with automatic conflict detection and retry guidance on the error path.
    </p>
    <br>
    <hr>
    <p>
        <strong>MSRV is 1.85+</strong> (Rust 2024 edition). Snapshot isolation by default. Optional serializable (SSI). Durable commits via wal-db.
    </p>
    <blockquote>
        <strong>Status: pre-1.0, in active development.</strong> The Tier-1 API is settled as of <code>0.2</code> and will not change shape before <code>1.0</code>; the commit protocol and any on-disk format are frozen at <code>1.0.0</code>. See <a href="./CHANGELOG.md"><code>CHANGELOG.md</code></a> for detail.
    </blockquote>
</div>

<hr>
<br>

<h2>What it does</h2>

Available now (`0.5`, feature-complete):

- **MVCC** &mdash; each write creates a new version; readers see a consistent snapshot without blocking writers
- **Snapshot isolation** &mdash; a transaction reads the database as of its start timestamp; its own writes are visible to itself before commit
- **Serializable (SSI)** &mdash; opt-in read-set validation under the `serializable` feature, rejecting write skew and the read-only anomaly
- **Durable commit log** &mdash; under the `durability` feature, `Db::open` logs each commit to a `wal-db` write-ahead log and syncs before acknowledging; the log is replayed on restart
- **Garbage collection** &mdash; `Db::collect_garbage` reclaims versions no live transaction or snapshot can observe; an oldest-reader watermark guarantees a held snapshot's versions are never reclaimed
- **Write-write conflict detection** &mdash; first-committer-wins at commit; the later writer is told to retry with a typed, retryable error
- **Sharded commit path** &mdash; lock-free timestamp allocation and per-shard conflict checks, so commits to unrelated keys do not contend (loom-checked)
- **Pluggable backing store** &mdash; the version store is the `VersionStore` trait; an in-memory store ships, and any backend (an LSM tree, a B-tree, a remote store) plugs in unchanged


<br>

## Installation

```toml
[dependencies]
txn-db = "0.6"

# Opt into serializable isolation and/or a durable commit log:
txn-db = { version = "0.6", features = ["serializable", "durability"] }
```

<br>

## Quick start

The whole common case is begin, read and write through the transaction, commit:

```rust
use txn_db::Db;

let db = Db::new();

// Write two keys in one atomic transaction.
let mut tx = db.begin();
tx.put(b"user:1:name".to_vec(), b"ada".to_vec());
tx.put(b"user:1:role".to_vec(), b"admin".to_vec());
tx.commit()?;

// A later transaction reads a consistent snapshot.
let tx = db.begin();
assert_eq!(tx.get(b"user:1:name")?.as_deref(), Some(&b"ada"[..]));
# Ok::<(), txn_db::TxnError>(())
```

When two transactions race to write the same key, the first to commit wins and
the second is told to retry — that is what prevents lost updates:

```rust
use txn_db::Db;

let db = Db::new();
let mut a = db.begin();
let mut b = db.begin();
a.put(b"counter".to_vec(), b"1".to_vec());
b.put(b"counter".to_vec(), b"2".to_vec());

a.commit()?;                          // first committer wins
let err = b.commit().unwrap_err();    // second is rejected
assert!(err.is_retryable());          // retry against the fresh snapshot
# Ok::<(), txn_db::TxnError>(())
```

The retry loop is a few lines; see [`examples/concurrent_counter.rs`](./examples/concurrent_counter.rs)
for the contended read-modify-write pattern, [`examples/bank_transfer.rs`](./examples/bank_transfer.rs)
for an atomic multi-key transfer, and [`examples/custom_store.rs`](./examples/custom_store.rs)
for plugging in your own `VersionStore`.

## Serializable isolation

Snapshot isolation still allows *write skew*: two transactions that read the same
rows and write different ones can both commit, breaking an invariant that ties
those rows together. With the `serializable` feature,
[`begin_serializable`](https://docs.rs/txn-db) validates a transaction's read set
at commit and rejects exactly those cases.

```rust
# #[cfg(feature = "serializable")]
# {
use txn_db::Db;

let db = Db::new();
let mut seed = db.begin();
seed.put(b"on_call:alice".to_vec(), vec![1]);
seed.put(b"on_call:bob".to_vec(), vec![1]);
seed.commit()?;

// Both read the pair, then each takes one row off — classic write skew.
let mut t1 = db.begin_serializable();
let mut t2 = db.begin_serializable();
let _ = (t1.get(b"on_call:alice")?, t1.get(b"on_call:bob")?);
let _ = (t2.get(b"on_call:alice")?, t2.get(b"on_call:bob")?);
t1.put(b"on_call:alice".to_vec(), vec![0]);
t2.put(b"on_call:bob".to_vec(), vec![0]);

t1.commit()?;                          // first commits
assert!(t2.commit().is_err());         // second read a row t1 changed — rejected
# }
# Ok::<(), txn_db::TxnError>(())
```

See [`examples/serializable_doctors.rs`](./examples/serializable_doctors.rs) for the
full on-call-doctors demonstration, side by side under both isolation levels.

## Durability

With the `durability` feature, `Db::open` backs the database with a `wal-db`
write-ahead log. Each commit's record is appended and synced before `commit`
returns, so an acknowledged commit survives a crash; on restart the log is
replayed and uncommitted work leaves no trace.

```rust
# #[cfg(feature = "durability")]
# {
# let dir = tempfile::tempdir().unwrap();
# let path = dir.path().join("txn.wal");
use txn_db::Db;

// First run: commit, then the process exits.
{
    let db = Db::open(&path)?;
    let mut tx = db.begin();
    tx.put(b"k".to_vec(), b"v".to_vec());
    tx.commit()?;
}

// Restart: the log is replayed and the committed write is back.
let db = Db::open(&path)?;
assert_eq!(db.begin().get(b"k")?.as_deref(), Some(&b"v"[..]));
# }
# Ok::<(), txn_db::TxnError>(())
```

See [`examples/durable_store.rs`](./examples/durable_store.rs) for a commit /
drop / reopen walkthrough.

## Garbage collection

Every write keeps the previous version so in-flight readers see a stable
snapshot, so versions accumulate. `Db::collect_garbage` reclaims the versions no
live transaction or snapshot can still observe and returns how many it removed.
A held snapshot pins the versions it can see, so collection never reclaims data
a live reader depends on.

```rust
use txn_db::Db;

let db = Db::new();
for v in 0..100u8 {
    let mut tx = db.begin();
    tx.put(b"k".to_vec(), vec![v]);
    tx.commit()?;
}

// With no snapshot held, only the newest version need be kept.
let reclaimed = db.collect_garbage();
assert!(reclaimed > 0);
assert_eq!(db.begin().get(b"k")?.as_deref(), Some(&[99u8][..]));
# Ok::<(), txn_db::TxnError>(())
```

See [`examples/garbage_collection.rs`](./examples/garbage_collection.rs) for a
demonstration of a held snapshot pinning versions against collection.

<br>

## Examples

| Example | What it shows |
|---------|---------------|
| [`quick_start`](./examples/quick_start.rs) | Shortest end-to-end: open, write, read back. |
| [`bank_transfer`](./examples/bank_transfer.rs) | Atomic multi-key update with conflict retries. |
| [`concurrent_counter`](./examples/concurrent_counter.rs) | Many threads increment one key; no update is lost. |
| [`snapshot_reads`](./examples/snapshot_reads.rs) | A snapshot stays stable as the database moves on. |
| [`custom_store`](./examples/custom_store.rs) | Backing the engine with a custom `VersionStore`. |
| [`serializable_doctors`](./examples/serializable_doctors.rs) | Write skew under SI vs serializable (needs `--features serializable`). |
| [`durable_store`](./examples/durable_store.rs) | Commit, drop, reopen — recovery from the log (needs `--features durability`). |
| [`garbage_collection`](./examples/garbage_collection.rs) | Reclaiming old versions; a held snapshot pins what it can see. |

```bash
cargo run --example quick_start
cargo run --example garbage_collection
cargo run --example serializable_doctors --features serializable
cargo run --example durable_store --features durability
```

<br>

## Status

This is the `0.6` release: the feature-complete engine of `0.5` — MVCC with
snapshot and serializable isolation, a sharded lock-free commit path, a durable
commit log via `wal-db`, and watermark-driven garbage collection — with the
commit hot path profiled and tuned. The single-write commit fast path cuts
single-key commit latency by roughly 40%; see
[`docs/PERFORMANCE.md`](./docs/PERFORMANCE.md) for hot-path numbers and the
contention-scaling curve. The [`docs/API.md`](./docs/API.md) reference documents
the full surface. What remains before `1.0` is adversarial and cross-platform
hardening with the API formally frozen (`0.7`). The Tier-1 API is settled and
will not change before `1.0`.

<hr>
<br>

## Where It Fits

`txn-db` is the transaction layer. It builds on:

- [`wal-db`](https://github.com/jamesgober/wal-db) &mdash; durable transaction commit log
- [`lsm-db`](https://github.com/jamesgober/lsm-db) &mdash; a natural backing version store
- Hive DB &mdash; the transaction orchestration layer (DISTRO) builds on these semantics

It stays foreign-compatible: usable standalone over any version store that implements the trait.

<br>

## Contributing

Before opening a PR, `cargo fmt --all`, `cargo clippy --all-targets --all-features -- -D warnings`, and `cargo test --all-features` must be clean. Hot-path changes require a `criterion` benchmark; correctness-critical paths require property and/or `loom` tests.


<br>

<div id="license">
    <h2>License</h2>
    <p>Licensed under either of</p>
    <ul>
        <li><b>Apache License, Version 2.0</b> &mdash; see <a href="./LICENSE-APACHE">LICENSE-APACHE</a></li>
        <li><b>MIT License</b> &mdash; see <a href="./LICENSE-MIT">LICENSE-MIT</a></li>
    </ul>
    <p>at your option.</p>
</div>

<div align="center">
  <h2></h2>
  <sup>COPYRIGHT <small>&copy;</small> 2026 <strong>JAMES GOBER.</strong></sup>
</div>
