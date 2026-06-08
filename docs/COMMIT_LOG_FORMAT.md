<h1 align="center">
    <img width="99" alt="Rust logo" src="https://raw.githubusercontent.com/jamesgober/rust-collection/72baabd71f00e14aa9184efcb16fa3deddda3a0a/assets/rust-logo.svg">
    <br><b>txn-db</b><br>
    <sub><sup>COMMIT LOG FORMAT</sup></sub>
</h1>
<div align="center">
    <sup>
        <a href="../README.md" title="Project Home"><b>HOME</b></a>
        <span>&nbsp;│&nbsp;</span>
        <a href="./API.md" title="API Reference"><b>API</b></a>
        <span>&nbsp;│&nbsp;</span>
        <span>COMMIT LOG FORMAT</span>
    </sup>
</div>
<br>

> The normative on-disk format of a `txn-db` durable commit record, **frozen for
> the 1.x series**. A 1.x reader reads any 1.x writer's log, and a 1.x writer's
> log is readable by any 1.x reader. The format is only present with the
> `durability` feature, behind [`Db::open`](./API.md#db).

## Where it lives

`txn-db` does not own a log file format. Durability is delegated to
[`wal-db`](https://github.com/jamesgober/wal-db), which owns the file: it frames
each record with a length prefix and a CRC32C checksum, coalesces fsyncs
(group commit), and on open truncates any torn record at the tail so recovery
always sees a clean prefix of intact records. `txn-db` writes one **commit
record** as the opaque payload of each `wal-db` record — one record per
committed transaction.

This document specifies that payload. Framing, checksums, and torn-tail handling
are `wal-db`'s contract, not repeated here.

## Record layout

One record is one committed transaction. All multi-byte integers are
little-endian. There is no internal checksum — `wal-db`'s CRC32C covers the whole
payload.

```text
┌───────────┬─────────────────────────────────────────────────────────┐
│ version   │ u8     format version; 1 for the 1.x series              │
│ commit_ts │ u64    the transaction's commit timestamp                │
│ count     │ u32    number of writes that follow                      │
├───────────┴─────────────────────────────────────────────────────────┤
│ count repetitions of one write entry:                                │
│   ┌───────────┬───────────────────────────────────────────────────┐ │
│   │ key_len   │ u32    length of the key in bytes                  │ │
│   │ key       │ key_len bytes                                      │ │
│   │ tag       │ u8     1 = a value follows; 0 = tombstone (delete) │ │
│   │ val_len   │ u32    length of the value  (present iff tag == 1) │ │
│   │ value     │ val_len bytes               (present iff tag == 1) │ │
│   └───────────┴───────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────────┘
```

The minimum write entry is 5 bytes: a `u32` `key_len` of 0 and a `tag` of 0
(an empty-key tombstone). The minimum record is 13 bytes: `version` + `commit_ts`
+ a `count` of 0.

## Field semantics

| Field | Meaning |
|-------|---------|
| `version` | Format version. A 1.x reader accepts only `1` and rejects any other value, so a future format can be introduced without being mistaken for this one. |
| `commit_ts` | The logical timestamp the transaction committed at. On recovery, records are replayed in ascending `commit_ts` order (the log order need not match, because commits append after applying and may finish out of order). |
| `count` | The number of write entries. Validated against the bytes remaining before any allocation, so an implausible count cannot force a large allocation. |
| `key_len` / `key` | The written key. May be empty (`key_len == 0`). |
| `tag` | `1` if a value follows (a put); `0` for a tombstone (a delete). Any other value is rejected. |
| `val_len` / `value` | The written value, present only when `tag == 1`. May be empty (`val_len == 0`, an empty value — distinct from a tombstone). |

Only committed transactions are ever written: a transaction that aborts at
conflict detection never reaches the log. Recovery is therefore a pure replay
with nothing to undo.

## Decoding rules (normative)

A conforming reader MUST:

1. Read `version` first and reject the record if it is not `1`.
2. Validate every length against the bytes actually remaining; a length that
   would read past the end of the record is a decode error, never an
   out-of-bounds read.
3. Bound `count` by the remaining byte budget before allocating.
4. Reject a `tag` other than `0` or `1`.
5. Require exact consumption: a record with trailing bytes after the last write
   entry is a decode error.

A decode error surfaces as
[`TxnError::Durability`](./API.md#txnerror). Because `wal-db` removes a torn
record at the tail on open, a decode error during normal recovery indicates
genuine corruption of a complete record, not a partial write.

## Compatibility

- **Frozen for 1.x.** No field is added, removed, reordered, or re-sized within
  the 1.x series. The `version` byte stays `1`.
- A format change would ship under a new `version` value (and a major release),
  so a mixed-version log is detected rather than silently misread.

---

<sub>Copyright &copy; 2026 <strong>James Gober</strong>. All rights reserved.</sub>
