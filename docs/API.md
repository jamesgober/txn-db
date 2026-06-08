<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br><b>txn-db</b><br>
    <sub><sup>API REFERENCE</sup></sub>
</h1>
<div align="center">
    <sup>
        <a href="../README.md" title="Project Home"><b>HOME</b></a>
        <span>&nbsp;│&nbsp;</span>
        <span>API</span>
        <span>&nbsp;│&nbsp;</span>
        <a href="../CHANGELOG.md" title="Changelog"><b>CHANGELOG</b></a>
    </sup>
</div>
<br>

> Complete reference for every public item in `txn-db`, with examples.
>
> **Status: pre-1.0.** This document tracks the API surface as it lands across
> the 0.x series. The Tier-1 surface documented here is settled as of `0.2` and
> **API frozen as of `0.7`.** Serializable isolation was added in `0.3`, a
> durable commit log in `0.4`, and garbage collection in `0.5` — at which point
> the engine became feature-complete; `0.6` tuned the hot path (see
> [`PERFORMANCE.md`](./PERFORMANCE.md)) and `0.7` hardened it and froze the public
> surface. `0.8` adds the autocommit `Db::get`/`put`/`delete` convenience (an
> additive, MINOR-compatible change). No existing signature changes before `2.0`.

<h4 id="example-pointers">Example Pointers</h4>

- Quick start: `examples/quick_start.rs` — open, write two keys, read them back.
- Bank transfer: `examples/bank_transfer.rs` — atomic multi-key update with conflict retries.
- Concurrent counter: `examples/concurrent_counter.rs` — many threads increment one key; no update is lost.
- Snapshot reads: `examples/snapshot_reads.rs` — a snapshot stays stable as the database moves on.
- Custom store: `examples/custom_store.rs` — backing the engine with a custom `VersionStore`.

Run any of them with `cargo run --example <name>`.

## Table of Contents

- [Installation](#installation)
- [Overview](#overview)
- [The three tiers](#the-three-tiers)
- [Quick start](#quick-start)
- [Public API](#public-api)
  - [`Db`](#db)
  - [`Transaction`](#transaction)
  - [`Snapshot`](#snapshot)
  - [`Timestamp`](#timestamp)
  - [`TxnError`](#txnerror)
  - [`Result`](#result)
  - [`VersionStore`](#versionstore)
  - [`MemoryStore`](#memorystore)
  - [`WriteEntry`](#writeentry)
  - [`prelude`](#prelude)
- [Isolation model](#isolation-model)
- [Patterns](#patterns)
  - [Retrying on conflict](#retrying-on-conflict)
  - [Atomic multi-key updates](#atomic-multi-key-updates)
  - [Consistent point-in-time reads](#consistent-point-in-time-reads)
  - [Preventing write skew (serializable)](#preventing-write-skew-serializable)
  - [Durability and recovery](#durability-and-recovery)
  - [Reclaiming old versions](#reclaiming-old-versions)
  - [Implementing a custom store](#implementing-a-custom-store)
- [Feature flags](#feature-flags)

---

## Installation

```toml
[dependencies]
txn-db = "0.8"
```

MSRV is Rust 1.85 (the 2024 edition). The crate is `forbid(unsafe_code)`.

---

## Overview

`txn-db` is a multi-version concurrency control (MVCC) transaction engine: the
layer that turns a key-value store into a transactional database. Every write
produces a new version tagged with a commit [`Timestamp`](#timestamp), so
readers get a stable snapshot of the data without ever blocking writers, and
writers detect conflicts at commit time instead of holding locks for the
lifetime of a transaction.

It is deliberately a *layer*, not a store: the version store is the
[`VersionStore`](#versionstore) trait, so the engine composes on top of any
backend that can keep timestamped versions of a key. Keys and values are byte
strings (`[u8]`); the engine assigns no meaning to their contents.

---

## The three tiers

The API is organised in three tiers so the common case stays small and the
power stays reachable.

- **Tier 1 — the common case.** [`Db::new`](#db), [`Db::begin`](#db), and the
  [`Transaction`](#transaction) methods. No builder, no generics to name.
- **Tier 2 — configuration.** A builder for tuning, arriving in a later phase.
- **Tier 3 — the power path.** The [`VersionStore`](#versionstore) trait, the
  seam for custom backends, reached through [`Db::with_store`](#db).

---

## Quick start

```rust
use txn_db::Db;

let db = Db::new();

let mut tx = db.begin();
tx.put(b"k".to_vec(), b"v".to_vec());
tx.commit()?;

let tx = db.begin();
assert_eq!(tx.get(b"k")?.as_deref(), Some(&b"v"[..]));
# Ok::<(), txn_db::TxnError>(())
```

---

## Public API

### `Db`

```rust
pub struct Db<S: VersionStore = MemoryStore> { /* … */ }
```

The database handle and Tier-1 entry point. A `Db` is a cheap, clonable handle
over shared state, like an `Arc`: every clone refers to the same database, so
the idiomatic way to use it across threads is to clone a handle per thread. It
is `Send + Sync` whenever its store is.

The default type parameter is [`MemoryStore`](#memorystore), so the type is
written `Db` with no generics in the common case.

#### Constructors

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `fn new() -> Db<MemoryStore>` | An empty in-memory database. The default configuration. |
| `open` | `fn open(path) -> Result<Db<MemoryStore>>` | A durable database backed by a write-ahead log at `path`, replaying committed transactions on startup. Requires the `durability` feature. |
| `with_store` | `fn with_store(store: S) -> Db<S>` | A database over a custom [`VersionStore`](#versionstore). The Tier-3 seam. |
| `default` | `fn default() -> Db<MemoryStore>` | Equivalent to `Db::new()`. |

#### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `begin` | `fn begin(&self) -> Transaction<S>` | Start a snapshot-isolation transaction over the current snapshot. |
| `begin_serializable` | `fn begin_serializable(&self) -> Transaction<S>` | Start a serializable transaction (read set validated at commit). Requires the `serializable` feature. |
| `snapshot` | `fn snapshot(&self) -> Snapshot<S>` | Take a read-only, point-in-time view. |
| `get` | `fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>>` | Autocommit read of one key (takes a snapshot and reads it). |
| `put` | `fn put(&self, key, value) -> Result<Timestamp>` | Autocommit write of one key, retrying on conflict (last-writer-wins). |
| `delete` | `fn delete(&self, key) -> Result<Timestamp>` | Autocommit delete of one key, retrying on conflict. |
| `last_committed` | `fn last_committed(&self) -> Timestamp` | The timestamp of the most recent commit; `Timestamp::ZERO` if none. |
| `collect_garbage` | `fn collect_garbage(&self) -> usize` | Reclaim versions no live transaction or snapshot can observe; returns the count removed. |
| `clone` | `fn clone(&self) -> Self` | A new handle to the same database. |

The autocommit `get` / `put` / `delete` are the lazy single-operation path: each
runs in its own transaction. `put` and `delete` retry internally on conflict, so
they are last-writer-wins and never return a conflict — for read-then-write
atomicity or explicit conflict handling, use [`begin`](#db).

`begin`, `begin_serializable`, and `snapshot` all capture the current commit
high-water mark as their read timestamp. Commits made after that moment are
invisible to the returned transaction or snapshot. The difference between the two
`begin` variants is at commit: a serializable transaction additionally validates
that nothing it *read* has changed, while a snapshot-isolation transaction
validates only what it *wrote*. See [Isolation model](#isolation-model).

**Examples**

Open, write, read:

```rust
use txn_db::Db;

let db = Db::new();
let mut tx = db.begin();
tx.put(b"greeting".to_vec(), b"hei".to_vec());
tx.commit()?;

assert_eq!(db.begin().get(b"greeting")?.as_deref(), Some(&b"hei"[..]));
# Ok::<(), txn_db::TxnError>(())
```

Share one database across threads — independent keys never conflict:

```rust
use std::thread;
use txn_db::Db;

let db = Db::new();
let handles: Vec<_> = (0..4u8)
    .map(|i| {
        let db = db.clone();
        thread::spawn(move || {
            let mut tx = db.begin();
            tx.put(vec![i], vec![i]);
            tx.commit().expect("commit");
        })
    })
    .collect();
for h in handles {
    h.join().expect("thread");
}
```

Track commit progress:

```rust
use txn_db::{Db, Timestamp};

let db = Db::new();
assert_eq!(db.last_committed(), Timestamp::ZERO);

let mut tx = db.begin();
tx.put(b"k".to_vec(), b"v".to_vec());
let ts = tx.commit()?;
assert_eq!(db.last_committed(), ts);
# Ok::<(), txn_db::TxnError>(())
```

---

### `Transaction`

```rust
#[must_use = "a transaction buffers writes that are discarded unless it is committed"]
pub struct Transaction<S: VersionStore = MemoryStore> { /* … */ }
```

A read-write unit of work over a consistent snapshot. Created by
[`Db::begin`](#db). Reads come from the snapshot captured at `begin` plus the
transaction's own buffered writes; writes are local until
[`commit`](#transaction) succeeds. Dropping a transaction without committing
discards its writes — the same as [`rollback`](#transaction).

#### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>>` | Read `key` as this transaction sees it. |
| `put` | `fn put(&mut self, key: impl Into<Arc<[u8]>>, value: impl Into<Arc<[u8]>>)` | Buffer a write. |
| `delete` | `fn delete(&mut self, key: impl Into<Arc<[u8]>>)` | Buffer a delete (a tombstone at commit). |
| `commit` | `fn commit(self) -> Result<Timestamp>` | Apply all buffered writes atomically; returns the commit timestamp. |
| `rollback` | `fn rollback(self)` | Discard the transaction and its writes. |
| `read_timestamp` | `fn read_timestamp(&self) -> Timestamp` | The snapshot timestamp this transaction reads at. |

**Parameters**

- `get` takes `key: &[u8]` — borrowed, so reads never allocate a key.
- `put` / `delete` take `impl Into<Arc<[u8]>>`. Passing an owned `Vec<u8>`,
  `Box<[u8]>`, or `Arc<[u8]>` moves it in without copying the bytes; passing a
  `&[u8]` copies once into a fresh `Arc`. Byte-string literals (`b"k"`) are
  fixed-size arrays and do not convert directly — use `b"k".to_vec()` or
  `&b"k"[..]`.

**Return values**

- `get` returns `Ok(Some(value))` for a visible value, `Ok(None)` if the key is
  absent (or the transaction has deleted it), and `Err` only if a custom store
  fails the read. The value is an `Arc<[u8]>`, so cloning it is a reference-count
  bump, not a copy.
- `commit` returns the commit `Timestamp` on success. A transaction that wrote
  nothing commits trivially and returns its snapshot timestamp without
  allocating a new one.

**Errors**

- `commit` returns [`TxnError::Conflict`](#txnerror) — retryable — if another
  transaction committed a change to any written key after this transaction's
  snapshot. None of the writes are applied in that case.
- `get` and `commit` return [`TxnError::Store`](#txnerror) if the backing store
  fails. The default in-memory store never fails.

**Examples**

Read-your-own-writes:

```rust
use txn_db::Db;

let db = Db::new();
let mut tx = db.begin();

assert_eq!(tx.get(b"k")?, None);                        // absent
tx.put(b"k".to_vec(), b"v".to_vec());
assert_eq!(tx.get(b"k")?.as_deref(), Some(&b"v"[..]));  // its own write
tx.delete(b"k".to_vec());
assert_eq!(tx.get(b"k")?, None);                        // its own delete
# Ok::<(), txn_db::TxnError>(())
```

Atomic multi-key commit:

```rust
use txn_db::Db;

let db = Db::new();
let mut tx = db.begin();
tx.put(b"account:1".to_vec(), 100u64.to_le_bytes().to_vec());
tx.put(b"account:2".to_vec(), 50u64.to_le_bytes().to_vec());
tx.commit()?;  // both land or neither does
# Ok::<(), txn_db::TxnError>(())
```

Explicit rollback:

```rust
use txn_db::Db;

let db = Db::new();
let mut tx = db.begin();
tx.put(b"k".to_vec(), b"v".to_vec());
tx.rollback();
assert_eq!(db.begin().get(b"k")?, None);
# Ok::<(), txn_db::TxnError>(())
```

---

### `Snapshot`

```rust
pub struct Snapshot<S: VersionStore = MemoryStore> { /* … */ }
```

A read-only, point-in-time view created by [`Db::snapshot`](#db). It reads as of
the moment it was taken and never changes, even as other transactions commit. It
has no write buffer and nothing to commit, so it is cheaper than a transaction
when all you need is to read several keys at one consistent instant.

#### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `get` | `fn get(&self, key: &[u8]) -> Result<Option<Arc<[u8]>>>` | Read `key` as of this snapshot. |
| `read_timestamp` | `fn read_timestamp(&self) -> Timestamp` | The timestamp this snapshot reads at. |

**Examples**

A snapshot is stable across later commits:

```rust
use txn_db::Db;

let db = Db::new();
let mut tx = db.begin();
tx.put(b"k".to_vec(), b"v1".to_vec());
tx.commit()?;

let snap = db.snapshot();          // capture the current state
let mut tx = db.begin();
tx.put(b"k".to_vec(), b"v2".to_vec());
tx.commit()?;                      // move the database forward

assert_eq!(snap.get(b"k")?.as_deref(), Some(&b"v1"[..]));        // unmoved
assert_eq!(db.snapshot().get(b"k")?.as_deref(), Some(&b"v2"[..]));
# Ok::<(), txn_db::TxnError>(())
```

---

### `Timestamp`

```rust
pub struct Timestamp(/* private */);
```

A logical timestamp marking a point in a database's commit history. Timestamps
are issued by the database as a strictly increasing sequence, are totally
ordered, and are `Copy`. They are *logical*, not wall-clock: visibility never
depends on the system clock.

#### Associated items

| Item | Signature | Description |
|------|-----------|-------------|
| `ZERO` | `const ZERO: Timestamp` | The empty database, before any commit. A snapshot at `ZERO` sees nothing. |
| `from_raw` | `fn from_raw(value: u64) -> Timestamp` | Wrap a raw counter value. |
| `get` | `fn get(self) -> u64` | The raw counter value. |

`Display` formats a timestamp as `@N` (for example `@42`).

**Examples**

```rust
use txn_db::Timestamp;

assert_eq!(Timestamp::ZERO.get(), 0);
assert!(Timestamp::ZERO < Timestamp::from_raw(1));
assert_eq!(Timestamp::from_raw(42).to_string(), "@42");
```

---

### `TxnError`

```rust
#[non_exhaustive]
pub enum TxnError {
    Conflict { key_len: usize },
    Store { context: &'static str, detail: String },
    Durability { detail: String },
}
```

The crate error type. It implements `std::error::Error`, `Display`, `Clone`,
`PartialEq`, and `error_forge::ForgeError` (so `kind` / `caption` / `is_fatal`
metadata is available to portfolio tooling). It is `#[non_exhaustive]`: a
`match` over it must include a wildcard arm.

#### Variants

| Variant | Meaning | What to do |
|---------|---------|------------|
| `Conflict { key_len }` | A write-write conflict aborted the commit; another transaction committed a change to a written key after this one's snapshot. Only the key length is carried, never its bytes, so the error is safe to log. | Retry: begin a fresh transaction, re-read, re-apply, commit again. |
| `Store { context, detail }` | The backing [`VersionStore`](#versionstore) failed a read or apply. The in-memory store never produces this. | Store-specific; inspect the variant. |
| `Durability { detail }` | The durable commit log failed, or a record read during recovery did not decode. Produced only with the `durability` feature. An unacknowledged commit is never durable, but the durability guarantee is in doubt — `is_fatal` is `true`. | Treat as unrecoverable; do not retry blindly. |

#### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `is_retryable` | `fn is_retryable(&self) -> bool` | `true` for `Conflict`; the signal to re-run the transaction. |
| `conflict` | `fn conflict(key_len: usize) -> TxnError` | Build a `Conflict` error. A custom store returns this from `try_commit` when validation fails; pass the conflicting key's length. |
| `store` | `fn store(context: &'static str, detail: impl Display) -> TxnError` | Build a `Store` error; for custom store implementations. |

**Examples**

```rust
use txn_db::{Db, TxnError};

let db = Db::new();
let mut a = db.begin();
let mut b = db.begin();
a.put(b"k".to_vec(), b"a".to_vec());
b.put(b"k".to_vec(), b"b".to_vec());

a.commit()?;
let err = b.commit().unwrap_err();
assert!(err.is_retryable());
assert!(matches!(err, TxnError::Conflict { .. }));
# Ok::<(), TxnError>(())
```

---

### `Result`

```rust
pub type Result<T, E = TxnError> = core::result::Result<T, E>;
```

The crate result alias, defaulting its error to [`TxnError`](#txnerror). Most
signatures read `Result<T>`.

---

### `VersionStore`

```rust
pub trait VersionStore: Send + Sync {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>>;
    fn try_commit(
        &self,
        read_ts: Timestamp,
        commit_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<()>;

    // Provided method (default no-op); override to reclaim history.
    fn collect_garbage(&self, low_watermark: Timestamp) -> usize { 0 }
}
```

The Tier-3 seam: the backend a [`Db`](#db) is built on. The transaction layer
supplies the snapshot timestamps and the read and write sets; the store stores
versions and is the serialization point that validates and applies each commit
atomically. Implementations must be `Send + Sync`. Only `get` and `try_commit`
are required; `collect_garbage` defaults to doing nothing.

A custom store signals a conflict from `try_commit` with
[`TxnError::conflict`](#txnerror), and a backend failure with
[`TxnError::store`](#txnerror) — see [Implementing a custom
store](#implementing-a-custom-store).

#### Contract

| Method | Obligation |
|--------|------------|
| `get` | Return the newest version of `key` whose commit timestamp is `<= read_ts`. A tombstone at that position reads as `None`. |
| `try_commit` | As one step, atomic against any other `try_commit` touching an overlapping key: **validate** that no key in `writes` or `reads` has a version newer than `read_ts`, and if all pass, **apply** each write as a new version stamped `commit_ts`. `reads` is empty for snapshot-isolation transactions and carries the read set for serializable ones. The database hands out `commit_ts` uniquely and in increasing order. |
| `collect_garbage` | Reclaim versions no reader at or after `low_watermark` can observe, returning the count removed. Defaults to a no-op, so a store that keeps no history need not implement it. |

**Errors**: `try_commit` returns [`TxnError::Conflict`](#txnerror) if validation
fails (nothing is applied). Any method may return [`TxnError::Store`](#txnerror)
to surface a backend failure through the engine's `Result`.

**Example** — driving the shipped store directly through the trait:

```rust
use std::sync::Arc;
use txn_db::{MemoryStore, Timestamp, VersionStore};

let store = MemoryStore::new();
let key: Arc<[u8]> = Arc::from(&b"k"[..]);
store.try_commit(
    Timestamp::ZERO,
    Timestamp::from_raw(1),
    vec![(key.clone(), Some(Arc::from(&b"v1"[..])))],
    &[],
)?;

assert_eq!(store.get(b"k", Timestamp::from_raw(1))?.as_deref(), Some(&b"v1"[..]));
assert_eq!(store.get(b"k", Timestamp::ZERO)?, None);
# Ok::<(), txn_db::TxnError>(())
```

See [Implementing a custom store](#implementing-a-custom-store) for a wrapper
that adds behavior over an inner store.

---

### `MemoryStore`

```rust
pub struct MemoryStore { /* … */ }
```

An in-memory [`VersionStore`](#versionstore) that shards the keyspace across
independent, separately-locked maps of version chains. Each key hashes to one
shard; within a shard its versions are kept in ascending commit-timestamp order,
so a snapshot read is a binary search. Reads lock one shard and commits lock only
the shards their keys fall in, so commits to unrelated keys run in parallel. This
is the default store of [`Db::new`](#db) and is well suited to caches, tests, and
workloads that fit in memory. Versions accumulate until garbage collection lands
(a later roadmap phase).

#### Methods

| Method | Signature | Description |
|--------|-----------|-------------|
| `new` | `fn new() -> MemoryStore` | An empty store with the default shard count. |
| `with_shards` | `fn with_shards(shards: usize) -> MemoryStore` | An empty store with a chosen shard count, rounded up to a power of two. Tune only with a benchmark in hand. |
| `default` | `fn default() -> MemoryStore` | Equivalent to `new()`. |
| `key_count` | `fn key_count(&self) -> usize` | Number of distinct keys ever written (includes keys whose latest version is a tombstone). |

**Example**

```rust
use txn_db::{Db, MemoryStore};

let db = Db::with_store(MemoryStore::new());  // the explicit form of Db::new()
let mut tx = db.begin();
tx.put(b"hello".to_vec(), b"world".to_vec());
tx.commit()?;
# Ok::<(), txn_db::TxnError>(())
```

---

### `WriteEntry`

```rust
pub type WriteEntry = (Arc<[u8]>, Option<Arc<[u8]>>);
```

One entry in a commit batch handed to [`VersionStore::apply`](#versionstore): a
key paired with the value to write (`Some`) or a tombstone marking a delete
(`None`). You only touch this when implementing a custom store.

---

### `prelude`

```rust
pub mod prelude { /* re-exports */ }
```

The crate's common imports in one `use`: `Db`, `Transaction`, `Snapshot`,
`Timestamp`, `TxnError`, `Result`, `VersionStore`, `MemoryStore`, and
`WriteEntry`.

```rust
use txn_db::prelude::*;

let db = Db::new();
let mut tx = db.begin();
tx.put(b"k".to_vec(), b"v".to_vec());
let _ts: Timestamp = tx.commit()?;
# Ok::<(), TxnError>(())
```

---

## Isolation model

`txn-db` provides **snapshot isolation** by default, with **serializable
isolation** available per transaction under the `serializable` feature.

Common to both:

- A transaction reads the database as of the instant it began. Commits by other
  transactions afterward are invisible to it.
- Within a transaction, reads reflect its own buffered writes
  (read-your-own-writes) before commit.
- At commit, the engine applies **first-committer-wins** on the write set: if
  any key the transaction wrote was changed by another transaction that committed
  after this one's snapshot, the commit is rejected with a retryable
  [`TxnError::Conflict`](#txnerror) and none of its writes are applied. That rule
  prevents lost updates.

Snapshot isolation ([`Db::begin`](#db)) stops there. It permits **write skew**:
two transactions that read an overlapping set and write *different* keys can both
commit, because neither wrote what the other read.

Serializable isolation ([`Db::begin_serializable`](#db)) additionally validates
the **read set** at commit: if any key the transaction read changed after its
snapshot, the commit is rejected. That closes write skew and the read-only
anomaly, making the set of committing (writing) transactions serializable; a
serializable transaction that writes nothing commits trivially, since it observed
a consistent snapshot. This is optimistic read-set validation — it can reject a
transaction that a more permissive scheme would allow, so retry-on-conflict
applies to serializable transactions too. The serialization order is the commit
order.

Because the API exposes only point reads, there are no range predicates and so no
range phantoms to consider; a read of an absent key is validated like any other,
so a later insert of that key is caught.

---

## Patterns

### Retrying on conflict

A write-write conflict is expected under optimistic concurrency; the correct
response is to retry against a fresh snapshot.

```rust
use txn_db::{Db, TxnError};

fn increment(db: &Db, key: &[u8]) -> Result<(), TxnError> {
    loop {
        let mut tx = db.begin();
        let current = tx.get(key)?.map_or(0u64, |v| {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&v[..8]);
            u64::from_le_bytes(buf)
        });
        tx.put(key.to_vec(), (current + 1).to_le_bytes().to_vec());
        match tx.commit() {
            Ok(_) => return Ok(()),
            Err(e) if e.is_retryable() => continue,
            Err(e) => return Err(e),
        }
    }
}

let db = Db::new();
increment(&db, b"counter")?;
# Ok::<(), TxnError>(())
```

### Atomic multi-key updates

All writes in a transaction land together or not at all.

```rust
use txn_db::Db;

let db = Db::new();
let mut tx = db.begin();
tx.put(b"order:1:status".to_vec(), b"paid".to_vec());
tx.put(b"inventory:sku-9".to_vec(), 41u64.to_le_bytes().to_vec());
tx.commit()?;  // both visible at once
# Ok::<(), txn_db::TxnError>(())
```

### Consistent point-in-time reads

Use a [`Snapshot`](#snapshot) to read many keys as of one instant without
blocking writers.

```rust
use txn_db::Db;

let db = Db::new();
let mut tx = db.begin();
tx.put(b"a".to_vec(), b"1".to_vec());
tx.put(b"b".to_vec(), b"2".to_vec());
tx.commit()?;

let snap = db.snapshot();
let a = snap.get(b"a")?;
let b = snap.get(b"b")?;  // a and b are read as of the same instant
assert!(a.is_some() && b.is_some());
# Ok::<(), txn_db::TxnError>(())
```

### Preventing write skew (serializable)

When an invariant ties several rows together, snapshot isolation can let two
transactions break it by each updating a different row. Use
[`begin_serializable`](#db) (the `serializable` feature) so the read set is
validated at commit.

```rust
# #[cfg(feature = "serializable")]
# {
use txn_db::Db;

let db = Db::new();
let mut seed = db.begin();
seed.put(b"x".to_vec(), vec![1]);
seed.put(b"y".to_vec(), vec![1]);
seed.commit()?;

let mut t1 = db.begin_serializable();
let mut t2 = db.begin_serializable();
let _ = (t1.get(b"x")?, t1.get(b"y")?);
let _ = (t2.get(b"x")?, t2.get(b"y")?);
t1.put(b"x".to_vec(), vec![0]);
t2.put(b"y".to_vec(), vec![0]);

t1.commit()?;
assert!(t2.commit().is_err());   // t2 read x, which t1 changed
# }
# Ok::<(), txn_db::TxnError>(())
```

### Durability and recovery

Open the database with [`Db::open`](#db) (the `durability` feature) to back it
with a write-ahead log. Each commit is appended and synced before it is
acknowledged, and the log is replayed on the next open.

```rust
# #[cfg(feature = "durability")]
# {
# let dir = tempfile::tempdir().unwrap();
# let path = dir.path().join("txn.wal");
use txn_db::Db;

{
    let db = Db::open(&path)?;
    let mut tx = db.begin();
    tx.put(b"k".to_vec(), b"v".to_vec());
    tx.commit()?;          // appended + synced before this returns
}

// A new process reopens the same log.
let db = Db::open(&path)?;
assert_eq!(db.begin().get(b"k")?.as_deref(), Some(&b"v"[..]));
# }
# Ok::<(), txn_db::TxnError>(())
```

Only committed transactions are ever logged, so recovery has nothing to undo: a
transaction that aborted, or that the process never managed to make durable, is
simply absent on reopen. A torn record at the tail of the log — a crash
mid-append — is discarded when the log is opened, so recovery always yields a
clean prefix of commits. Commit timestamps resume strictly after the highest
recovered timestamp.

### Reclaiming old versions

Versions accumulate as keys are overwritten. Call [`collect_garbage`](#db)
periodically — or after retiring long-running snapshots — to reclaim the
versions no live reader can observe. A held snapshot pins what it can see, so
collection never removes data a reader still needs.

```rust
use txn_db::Db;

let db = Db::new();
for v in 0..100u8 {
    let mut tx = db.begin();
    tx.put(b"k".to_vec(), vec![v]);
    tx.commit()?;
}

// A held snapshot pins its versions...
let snap = db.snapshot();
let pinned = db.collect_garbage();   // reclaims older history, keeps what `snap` sees
let _ = snap.get(b"k")?;             // still valid

// ...released, the rest becomes reclaimable.
drop(snap);
let _ = db.collect_garbage();
# let _ = pinned;
# Ok::<(), txn_db::TxnError>(())
```

### Implementing a custom store

Wrap or replace the backing store through [`VersionStore`](#versionstore). A
custom store is the seam for backing the engine with an LSM tree, a B-tree, or a
remote store; it returns [`TxnError::conflict`](#txnerror) when `try_commit`
validation fails. This instrumented wrapper counts reads while delegating commit
validation and apply to an inner store:

```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use txn_db::{Db, MemoryStore, Timestamp, TxnError, VersionStore, WriteEntry};

struct Counting {
    inner: MemoryStore,
    reads: AtomicU64,
}

impl VersionStore for Counting {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>, TxnError> {
        let _ = self.reads.fetch_add(1, Ordering::Relaxed);
        self.inner.get(key, read_ts)
    }
    fn try_commit(
        &self,
        read_ts: Timestamp,
        commit_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<(), TxnError> {
        self.inner.try_commit(read_ts, commit_ts, writes, reads)
    }
}

let db = Db::with_store(Counting { inner: MemoryStore::new(), reads: AtomicU64::new(0) });
let mut tx = db.begin();
tx.put(b"k".to_vec(), b"v".to_vec());
tx.commit()?;
# Ok::<(), TxnError>(())
```

---

## Feature flags

| Feature | Default | Description |
|---------|---------|-------------|
| `std` | yes | Standard library. Required by the current implementation. |
| `serializable` | no | Adds [`Db::begin_serializable`](#db): serializable isolation via read-set validation on top of snapshot isolation. Additive — snapshot isolation is unchanged when off. |
| `durability` | no | Adds [`Db::open`](#db): a `wal-db` write-ahead commit log, synced before each commit is acknowledged and replayed on startup. Additive — the in-memory `Db::new` path is unchanged when off. |

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
