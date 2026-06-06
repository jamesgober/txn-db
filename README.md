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

Available now (`0.2`):

- **MVCC** &mdash; each write creates a new version; readers see a consistent snapshot without blocking writers
- **Snapshot isolation** &mdash; a transaction reads the database as of its start timestamp; its own writes are visible to itself before commit
- **Write-write conflict detection** &mdash; first-committer-wins at commit; the later writer is told to retry with a typed, retryable error
- **Pluggable backing store** &mdash; the version store is the `VersionStore` trait; an in-memory store ships, and any backend plugs in

On the roadmap:

- **Serializable (SSI)** &mdash; optional serializable isolation via read/write conflict tracking
- **Durable txn log** &mdash; commits logged to `wal-db` before acknowledgment (under `durability`)
- **Garbage collection** &mdash; old versions reclaimed once no live snapshot can observe them


<br>

## Installation

```toml
[dependencies]
txn-db = "0.2"
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

<br>

## Examples

| Example | What it shows |
|---------|---------------|
| [`quick_start`](./examples/quick_start.rs) | Shortest end-to-end: open, write, read back. |
| [`bank_transfer`](./examples/bank_transfer.rs) | Atomic multi-key update with conflict retries. |
| [`concurrent_counter`](./examples/concurrent_counter.rs) | Many threads increment one key; no update is lost. |
| [`snapshot_reads`](./examples/snapshot_reads.rs) | A snapshot stays stable as the database moves on. |
| [`custom_store`](./examples/custom_store.rs) | Backing the engine with a custom `VersionStore`. |

```bash
cargo run --example quick_start
```

<br>

## Status

This is the `0.2` foundation: the public surface, the MVCC core, snapshot
isolation, and write-write conflict detection over an in-memory store. The
[`docs/API.md`](./docs/API.md) reference documents the full Tier-1 surface, and
the remaining phases — serializable isolation, durable commits, and garbage
collection — follow per the roadmap. The shape of the Tier-1 API is settled and
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
