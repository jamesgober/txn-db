# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added

### Changed

### Fixed

### Security

---

## [0.5.0] - 2026-06-07

Garbage collection, a proven backing-store seam, and **feature freeze**: the
engine is feature-complete. What remains before 1.0 is hardening, not new
features.

### Added

- `Db::collect_garbage` — reclaim versions no live transaction or snapshot can
  observe, returning the count removed. Driven by an oldest-live-reader
  watermark: a held snapshot pins the versions it can see, so collection never
  reclaims data a reader still needs.
- `VersionStore::collect_garbage` — a provided trait method (default no-op) so a
  store that keeps history can prune it; `MemoryStore` implements it.
- An active-reader registry in the timestamp oracle: every transaction and
  snapshot registers its read timestamp for the lifetime of the handle, so the
  garbage-collection watermark knows the oldest snapshot still in use. The read
  timestamp is taken under the registry lock, closing the race where a
  not-yet-registered reader could be undercut by a concurrent collection.
- `TxnError::conflict` is now public, so a custom `VersionStore` can signal a
  write-write or read-set conflict from `try_commit`.
- Garbage-collection property tests (`tests/gc.rs`): across arbitrary
  interleavings of writes, deletes, snapshots, and collections, every held
  snapshot keeps reading exactly what it saw.
- Backing-store integration test (`tests/backing_store.rs`): a complete,
  independent `VersionStore` (a single-locked `BTreeMap`, sharing no code with
  `MemoryStore`) carries the full transaction semantics — snapshot isolation,
  conflict detection, and serializable read-set validation — proving the trait
  is a real seam.
- `examples/garbage_collection.rs` — version reclamation with a held snapshot
  pinning what it can see.

### Changed

- Feature freeze declared: the public surface is complete. Subsequent 0.x
  releases are optimization (`0.6`) and hardening with the API formally frozen
  (`0.7`), per the roadmap.

---

## [0.4.0] - 2026-06-07

Durability release: a write-ahead commit log via `wal-db`, with log replay on
startup. The in-memory `Db::new` path is unchanged; durability is entirely
opt-in.

### Added

- `Db::open(path)` (behind the new `durability` feature) — a database backed by
  a `wal-db` write-ahead log. Each commit's record is appended and synced before
  `commit` returns, so an acknowledged commit survives a crash; on open, the log
  is replayed and committed transactions are reinstated.
- Commit-record format and decoder with full bounds checking: a corrupt or
  truncated record can never drive an out-of-bounds read or an unbounded
  allocation.
- `TxnError::Durability` — surfaced when the log cannot be written, synced, or
  decoded; reported as fatal.
- Crash-recovery integration tests (`tests/durability.rs`): committed
  transactions survive reopen, uncommitted and rolled-back work does not,
  tombstones persist, timestamps continue after recovery, and a property test
  recovers a clean prefix from a log truncated at an arbitrary point.
- `examples/durable_store.rs` — commit, drop, reopen walkthrough.
- The CI `Doc` job now builds docs with both default and all features, so a doc
  link to a feature-gated item cannot regress unnoticed.

### Changed

- `Cargo.toml`: the `durability` feature now pulls `wal-db` (from crates.io,
  `default-features = false`), matching how the rest of the portfolio wires
  first-party dependencies.

### Fixed

- Documentation comments linked `Db::begin_serializable` (a `serializable`-gated
  method) from always-compiled items, breaking `cargo doc` without that feature.
  The references are now plain code spans, so docs build under every feature
  combination.

---

## [0.3.0] - 2026-06-07

Concurrency-control release: serializable isolation, and a sharded, lock-free
commit path that replaces the foundation's single global commit lock. Snapshot
isolation remains the default and is unchanged.

### Added

- `Db::begin_serializable` (behind the new `serializable` feature) — a
  transaction that tracks its read set and validates it at commit, rejecting
  write skew and the read-only anomaly that snapshot isolation permits. A
  serializable transaction that writes nothing commits trivially.
- `MemoryStore::with_shards` — construct the in-memory store with a chosen shard
  count (rounded up to a power of two) for tuning commit concurrency.
- Timestamp oracle with a lock-free read watermark: `begin` and `snapshot` read
  their timestamp without taking a lock, and commit timestamps are allocated with
  a single atomic increment.
- `loom` concurrency model checks (`tests/loom_txn.rs`) for the concurrent-commit
  path: one-winner conflict detection on a contended key, and consistent
  visibility of disjoint commits. The CI `loom` job now runs them as a gate.
- Serializable property tests (`tests/serializable.rs`): write skew never lets
  both commit, disjoint serializable transactions both commit, uncontended and
  read-only serializable transactions always commit.
- `examples/serializable_doctors.rs` — the on-call-doctors write-skew problem,
  shown under both isolation levels.

### Changed

- **Breaking (Tier-3):** the `VersionStore` trait replaced `latest_commit_ts` and
  `apply` with a single `try_commit(read_ts, commit_ts, writes, reads)` that
  validates the read and write sets and applies the writes atomically. This makes
  the store the serialization point and is what enables sharded, lock-free
  commits. The Tier-1 surface (`Db`, `Transaction`, `Snapshot`) is unchanged.
- `MemoryStore` now shards the keyspace across independently locked maps, so
  commits to unrelated keys no longer contend on one lock. The single global
  commit lock is gone.
- CI: the `loom` job runs `cargo test --test loom_txn --release` as a required
  check; `[lints.rust] unexpected_cfgs` allows the `loom` cfg under
  `deny(warnings)`.

---

## [0.2.0] - 2026-06-06

Foundation release: the public API, the MVCC core, snapshot isolation, and
write-write conflict detection over an in-memory version store. The Tier-1
surface is settled and will not change shape before `1.0`.

### Added

- `Db` &mdash; the database handle and Tier-1 entry point. `Db::new` for an
  in-memory database, `Db::with_store` for a custom backend, `begin`,
  `snapshot`, `last_committed`, and cheap `Clone` for sharing across threads.
- `Transaction` &mdash; read-write unit of work over a consistent snapshot, with
  `get`, `put`, `delete`, `commit`, `rollback`, and `read_timestamp`.
  Read-your-own-writes; writes buffered until commit; drop is an implicit
  rollback.
- `Snapshot` &mdash; read-only, point-in-time view with `get` and
  `read_timestamp`, stable across later commits.
- `Timestamp` &mdash; logical, totally ordered commit timestamp with `ZERO`,
  `from_raw`, and `get`.
- `VersionStore` trait &mdash; the Tier-3 backing-store seam (`get`,
  `latest_commit_ts`, `apply`), with `WriteEntry` as the commit-batch entry type.
- `MemoryStore` &mdash; the default in-memory `VersionStore`, version chains kept
  in commit order with binary-search snapshot reads; `key_count` for diagnostics.
- `TxnError` &mdash; `#[non_exhaustive]` domain error built on `error-forge`, with
  the retryable `Conflict` variant, a `Store` variant for custom backends, and
  `is_retryable`. `Result<T>` alias.
- `prelude` module re-exporting the common surface.
- Snapshot-isolation property tests (reference-model equivalence, snapshot
  stability, write-write conflict, read-your-own-writes) and concurrency tests
  (lost-update prevention under contention, reader isolation from writers).
- `criterion` benchmarks for point reads, single-key and batch commits, and the
  uncontended read-modify-write loop.
- Examples: `quick_start`, `bank_transfer`, `concurrent_counter`,
  `snapshot_reads`, `custom_store`.
- `docs/API.md` &mdash; complete reference for the 0.2 public surface.

### Changed

- `Cargo.toml`: added the `error-forge` dependency. The `durability` and
  `serializable` feature flags are declared as reserved no-ops so the surface is
  stable; their implementations (and the `wal-db` dependency behind
  `durability`) land in the 0.4 and 0.3 phases respectively.
- `README.md`: leads with the Tier-1 surface, a quick start, and an examples
  table; status updated to the 0.2 foundation.

---

## [0.1.0] - 2026-05-30

Initial scaffold and repository bootstrap. No txn-db logic yet &mdash; this release establishes the structure, tooling, and quality gates the implementation will be built on.

### Added

- `Cargo.toml` with full crate metadata, Rust 2024 edition, MSRV 1.85, dual `Apache-2.0 OR MIT` license, `docs.rs` configuration, perf-tuned release profile.
- Feature flags and first-party dependency wiring (see `Cargo.toml`).
- Dev-dependencies for the test stack: `criterion`, `proptest`, and `loom` under `cfg(loom)`.
- `README.md` &mdash; overview, positioning, install, and "where it fits".
- `docs/API.md` reference skeleton.
- `REPS.md` compliance baseline at the repository root.
- `.github/workflows/ci.yml` &mdash; Linux/macOS/Windows CI matrix on stable and MSRV, plus loom and audit/deny jobs.
- `deny.toml`, `clippy.toml`, `rustfmt.toml`, `.gitattributes`, `.gitignore`.
- `.dev/` AI-editor briefing (`PROMPT.md`, `ROADMAP.md`) &mdash; gitignored.

[Unreleased]: https://github.com/jamesgober/txn-db/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/jamesgober/txn-db/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/jamesgober/txn-db/compare/v0.3.0...v0.4.0
[0.3.0]: https://github.com/jamesgober/txn-db/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/jamesgober/txn-db/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/txn-db/releases/tag/v0.1.0
