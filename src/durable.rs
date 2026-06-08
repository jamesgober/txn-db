//! The durable commit log: `wal-db` plus a commit-record format.
//!
//! With the `durability` feature, a [`Db`](crate::Db) opened with
//! [`Db::open`](crate::Db::open) appends a record for every committed
//! transaction to a `wal-db` write-ahead log and syncs it before the commit is
//! acknowledged. Only *committed* transactions are ever logged — a transaction
//! that aborts at conflict detection never reaches this point — so recovery is
//! simply replaying the log: there are no uncommitted records to discard.
//!
//! ## Record format
//!
//! Each record is one committed transaction, encoded little-endian:
//!
//! ```text
//! commit_ts : u64
//! count     : u32                       number of writes
//! count × {
//!   key_len : u32
//!   key     : key_len bytes
//!   tag     : u8                        1 = value follows, 0 = tombstone
//!   value   : (u32 len + bytes) if tag == 1
//! }
//! ```
//!
//! `wal-db` already frames each record with its own length and CRC32C and
//! discards a torn record at the tail on recovery, so this format carries no
//! checksum of its own. The decoder still validates every length against the
//! bytes actually present, so a corrupt record can never drive an out-of-bounds
//! read or an unbounded allocation.

use std::sync::Arc;

use wal_db::Wal;

use crate::error::{Result, TxnError};
use crate::store::WriteEntry;
use crate::timestamp::Timestamp;

/// One committed transaction recovered from the log.
pub(crate) struct RecoveredCommit {
    pub(crate) commit_ts: Timestamp,
    pub(crate) writes: Vec<WriteEntry>,
}

/// A `wal-db` log of committed transactions.
pub(crate) struct CommitLog {
    wal: Wal,
}

impl CommitLog {
    /// Open (or create) the log at `path` and decode every record already in it.
    ///
    /// Returns the open log and the recovered commits in log order. The caller
    /// orders them by commit timestamp before installing them.
    pub(crate) fn open(path: impl AsRef<std::path::Path>) -> Result<(Self, Vec<RecoveredCommit>)> {
        let wal = Wal::open(path).map_err(TxnError::durability)?;
        let mut recovered = Vec::new();
        for entry in wal.iter().map_err(TxnError::durability)? {
            let entry = entry.map_err(TxnError::durability)?;
            recovered.push(decode_commit(entry.data())?);
        }
        Ok((CommitLog { wal }, recovered))
    }

    /// Append an already-encoded commit record and make it durable before
    /// returning.
    ///
    /// Uses `wal-db`'s group-commit-aware `append_and_sync`, so concurrent
    /// committers coalesce into a single fsync rather than paying for one each.
    /// The record is encoded by [`encode_for_log`] before the write set is moved
    /// into the version store.
    pub(crate) fn append_committed(&self, record: &[u8]) -> Result<()> {
        self.wal
            .append_and_sync(record)
            .map(|_lsn| ())
            .map_err(TxnError::durability)
    }
}

/// The smallest possible encoded write: a zero-length key and a tombstone tag
/// (`key_len` u32 + `tag` u8). Used to bound the write count before allocating.
const MIN_WRITE_BYTES: usize = 4 + 1;

/// Encode one committed transaction into a record, ready for
/// [`CommitLog::append_committed`].
pub(crate) fn encode_for_log(commit_ts: Timestamp, writes: &[WriteEntry]) -> Vec<u8> {
    let body: usize = writes
        .iter()
        .map(|(key, value)| 4 + key.len() + 1 + value.as_ref().map_or(0, |v| 4 + v.len()))
        .sum();
    let mut buf = Vec::with_capacity(8 + 4 + body);
    buf.extend_from_slice(&commit_ts.get().to_le_bytes());
    buf.extend_from_slice(&(writes.len() as u32).to_le_bytes());
    for (key, value) in writes {
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        match value {
            Some(v) => {
                buf.push(1);
                buf.extend_from_slice(&(v.len() as u32).to_le_bytes());
                buf.extend_from_slice(v);
            }
            None => buf.push(0),
        }
    }
    buf
}

/// Decode one record, validating every length against the bytes present.
fn decode_commit(bytes: &[u8]) -> Result<RecoveredCommit> {
    let mut reader = Reader::new(bytes);
    let commit_ts = Timestamp::from_raw(reader.read_u64()?);
    let count = reader.read_u32()? as usize;

    // A record cannot describe more writes than its remaining bytes could hold,
    // so the claimed count is bounded before a single allocation.
    if count > reader.remaining() / MIN_WRITE_BYTES {
        return Err(corrupt("write count exceeds record size"));
    }

    let mut writes = Vec::with_capacity(count);
    for _ in 0..count {
        let key_len = reader.read_u32()? as usize;
        let key: Arc<[u8]> = Arc::from(reader.read_bytes(key_len)?);
        let value = match reader.read_u8()? {
            0 => None,
            1 => {
                let value_len = reader.read_u32()? as usize;
                Some(Arc::from(reader.read_bytes(value_len)?))
            }
            other => return Err(corrupt_tag(other)),
        };
        writes.push((key, value));
    }

    if reader.remaining() != 0 {
        return Err(corrupt("trailing bytes after commit record"));
    }
    Ok(RecoveredCommit { commit_ts, writes })
}

/// A bounds-checked little-endian cursor over a record's bytes.
struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Reader { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn read_bytes(&mut self, len: usize) -> Result<&'a [u8]> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|&end| end <= self.buf.len())
            .ok_or_else(|| corrupt("record ends mid-field"))?;
        let slice = &self.buf[self.pos..end];
        self.pos = end;
        Ok(slice)
    }

    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_bytes(1)?[0])
    }

    fn read_u32(&mut self) -> Result<u32> {
        let bytes = self.read_bytes(4)?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    fn read_u64(&mut self) -> Result<u64> {
        let b = self.read_bytes(8)?;
        Ok(u64::from_le_bytes([
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
        ]))
    }
}

fn corrupt(reason: &str) -> TxnError {
    TxnError::durability(format!("malformed commit record: {reason}"))
}

fn corrupt_tag(tag: u8) -> TxnError {
    TxnError::durability(format!("malformed commit record: invalid value tag {tag}"))
}

#[cfg(all(test, not(loom)))]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    fn entry(key: &[u8], value: Option<&[u8]>) -> WriteEntry {
        (Arc::from(key), value.map(Arc::from))
    }

    proptest! {
        /// The decoder must treat its input as hostile: any byte string at all
        /// either decodes or errors, but never panics and never tries to
        /// allocate beyond the bytes present.
        #[test]
        fn decode_never_panics_on_arbitrary_bytes(bytes in prop::collection::vec(any::<u8>(), 0..1024)) {
            let _ = decode_commit(&bytes);
        }

        /// Encoding any well-formed commit and decoding it round-trips exactly,
        /// including empty keys, empty values, tombstones, and duplicate keys.
        #[test]
        fn encode_decode_roundtrips_arbitrary(
            ts in any::<u64>(),
            writes in prop::collection::vec(
                (
                    prop::collection::vec(any::<u8>(), 0..24),
                    prop::option::of(prop::collection::vec(any::<u8>(), 0..24)),
                ),
                0..24,
            ),
        ) {
            let entries: Vec<WriteEntry> = writes
                .into_iter()
                .map(|(k, v)| (Arc::from(k.as_slice()), v.map(|v| Arc::from(v.as_slice()))))
                .collect();
            let bytes = encode_for_log(Timestamp::from_raw(ts), &entries);
            let decoded = decode_commit(&bytes).unwrap();
            prop_assert_eq!(decoded.commit_ts, Timestamp::from_raw(ts));
            prop_assert_eq!(decoded.writes, entries);
        }

        /// Any valid record with arbitrary trailing bytes appended must be
        /// rejected — the decoder requires exact consumption.
        #[test]
        fn decode_rejects_arbitrary_trailing_bytes(
            ts in any::<u64>(),
            trailer in prop::collection::vec(any::<u8>(), 1..32),
        ) {
            let mut bytes = encode_for_log(Timestamp::from_raw(ts), &[entry(b"k", Some(b"v"))]);
            bytes.extend_from_slice(&trailer);
            prop_assert!(decode_commit(&bytes).is_err());
        }
    }

    #[test]
    fn test_encode_decode_roundtrip() {
        let writes = vec![
            entry(b"alice", Some(b"100")),
            entry(b"bob", None),
            entry(b"", Some(b"")),
        ];
        let bytes = encode_for_log(Timestamp::from_raw(42), &writes);
        let decoded = decode_commit(&bytes).unwrap();
        assert_eq!(decoded.commit_ts, Timestamp::from_raw(42));
        assert_eq!(decoded.writes, writes);
    }

    #[test]
    fn test_decode_empty_write_set() {
        let bytes = encode_for_log(Timestamp::from_raw(7), &[]);
        let decoded = decode_commit(&bytes).unwrap();
        assert_eq!(decoded.commit_ts, Timestamp::from_raw(7));
        assert!(decoded.writes.is_empty());
    }

    #[test]
    fn test_decode_truncated_record_is_rejected() {
        let bytes = encode_for_log(Timestamp::from_raw(1), &[entry(b"k", Some(b"v"))]);
        for cut in 0..bytes.len() {
            // Any prefix shorter than the whole record must error, never panic.
            assert!(decode_commit(&bytes[..cut]).is_err());
        }
    }

    #[test]
    fn test_decode_rejects_implausible_count() {
        // commit_ts = 0, count = u32::MAX, no body.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(decode_commit(&bytes).is_err());
    }

    #[test]
    fn test_decode_rejects_trailing_bytes() {
        let mut bytes = encode_for_log(Timestamp::from_raw(1), &[]);
        bytes.push(0xff);
        assert!(decode_commit(&bytes).is_err());
    }

    #[test]
    fn test_decode_rejects_bad_value_tag() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes()); // one write
        bytes.extend_from_slice(&1u32.to_le_bytes()); // key_len = 1
        bytes.push(b'k');
        bytes.push(9); // invalid tag
        assert!(decode_commit(&bytes).is_err());
    }
}
