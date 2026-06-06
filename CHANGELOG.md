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

- `Cargo.toml`: added the `error-forge` dependency; pinned the optional `wal-db`
  dependency (behind the `durability` feature) to the in-repo `1.0` sibling by
  path so the feature surface resolves.
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

[Unreleased]: https://github.com/jamesgober/txn-db/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/jamesgober/txn-db/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/jamesgober/txn-db/releases/tag/v0.1.0
