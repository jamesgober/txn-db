//! Commit ordering, visibility, and failure-path regressions.
//!
//! These tests drive interleavings that the plain suites cannot reach on their
//! own: a commit paused inside the version store while another one runs, a
//! store that panics, and a durable commit that is refused. The pauses come
//! from store wrappers that delegate to the real [`MemoryStore`] and only stop
//! at a chosen point; they change nothing about how the store validates or
//! applies.

#![allow(clippy::unwrap_used, clippy::expect_used)]
// Several helpers serve only the `serializable` tests.
#![cfg_attr(not(feature = "serializable"), allow(dead_code))]

use std::collections::HashMap;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use txn_db::{Db, MemoryStore, Result, Timestamp, VersionStore, WriteEntry};

// ------------------------------------------------------------------ harness

/// A one-shot pause point: the paused thread signals `arrived`, then blocks
/// until the test sends on `resume`.
struct Gate {
    arrived: Mutex<Sender<()>>,
    resume: Mutex<Receiver<()>>,
}

struct GateHandle {
    arrived: Receiver<()>,
    resume: Sender<()>,
}

fn gate() -> (Arc<Gate>, GateHandle) {
    let (a_tx, a_rx) = channel();
    let (r_tx, r_rx) = channel();
    (
        Arc::new(Gate {
            arrived: Mutex::new(a_tx),
            resume: Mutex::new(r_rx),
        }),
        GateHandle {
            arrived: a_rx,
            resume: r_tx,
        },
    )
}

impl Gate {
    fn pass(&self) {
        self.arrived.lock().unwrap().send(()).unwrap();
        self.resume.lock().unwrap().recv().unwrap();
    }
}

/// Gates keyed by the first written key that matches.
#[derive(Default)]
struct Gates(Mutex<HashMap<Vec<u8>, Arc<Gate>>>);

impl Gates {
    fn arm(&self, key: &[u8]) -> GateHandle {
        let (g, h) = gate();
        let _ = self.0.lock().unwrap().insert(key.to_vec(), g);
        h
    }

    fn take(&self, keys: &[Vec<u8>]) -> Option<Arc<Gate>> {
        let mut map = self.0.lock().unwrap();
        keys.iter().find_map(|k| map.remove(k))
    }
}

fn keys_of(writes: &[WriteEntry]) -> Vec<Vec<u8>> {
    writes.iter().map(|(k, _)| k.to_vec()).collect()
}

/// A store that implements only the 1.0 trait method, `try_commit`, so the
/// database goes through the default `try_commit_ordered`. It can pause a commit
/// just before or just after the real validate-and-apply, or panic.
#[derive(Default)]
struct PlainGated {
    inner: MemoryStore,
    before: Gates,
    after: Gates,
    panic_on: Mutex<Option<Vec<u8>>>,
}

impl VersionStore for PlainGated {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>> {
        self.inner.get(key, read_ts)
    }

    fn try_commit(
        &self,
        read_ts: Timestamp,
        commit_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<()> {
        let keys = keys_of(&writes);
        let target = self.panic_on.lock().unwrap().clone();
        if let Some(p) = target {
            if keys.contains(&p) {
                panic!("simulated store panic inside try_commit");
            }
        }
        if let Some(g) = self.before.take(&keys) {
            g.pass();
        }
        let out = self.inner.try_commit(read_ts, commit_ts, writes, reads);
        if let Some(g) = self.after.take(&keys) {
            g.pass();
        }
        out
    }

    fn collect_garbage(&self, low_watermark: Timestamp) -> usize {
        self.inner.collect_garbage(low_watermark)
    }
}

/// A store that forwards `try_commit_ordered` to [`MemoryStore`] and can pause
/// a commit inside the timestamp callback, that is while `MemoryStore` holds the
/// commit's shard locks.
#[derive(Default)]
struct LockedGated {
    inner: MemoryStore,
    at_stamp: Gates,
}

impl VersionStore for LockedGated {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>> {
        self.inner.get(key, read_ts)
    }

    fn try_commit(
        &self,
        read_ts: Timestamp,
        commit_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<()> {
        self.inner.try_commit(read_ts, commit_ts, writes, reads)
    }

    fn try_commit_ordered(
        &self,
        read_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
        stamp: &dyn Fn() -> Timestamp,
    ) -> Result<Timestamp> {
        let gate = self.at_stamp.take(&keys_of(&writes));
        self.inner.try_commit_ordered(read_ts, writes, reads, &|| {
            let ts = stamp();
            if let Some(g) = &gate {
                g.pass();
            }
            ts
        })
    }
}

/// Share one store between the test and the database.
struct Shared<T>(Arc<T>);

impl<T: VersionStore> VersionStore for Shared<T> {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Arc<[u8]>>> {
        self.0.get(key, read_ts)
    }
    fn try_commit(
        &self,
        read_ts: Timestamp,
        commit_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
    ) -> Result<()> {
        self.0.try_commit(read_ts, commit_ts, writes, reads)
    }
    fn try_commit_ordered(
        &self,
        read_ts: Timestamp,
        writes: Vec<WriteEntry>,
        reads: &[Arc<[u8]>],
        stamp: &dyn Fn() -> Timestamp,
    ) -> Result<Timestamp> {
        self.0.try_commit_ordered(read_ts, writes, reads, stamp)
    }
    fn collect_garbage(&self, low_watermark: Timestamp) -> usize {
        self.0.collect_garbage(low_watermark)
    }
}

fn shared<T: VersionStore + Default>() -> (Db<Shared<T>>, Arc<T>) {
    let store = Arc::new(T::default());
    (Db::with_store(Shared(Arc::clone(&store))), store)
}

fn int(v: &Option<Arc<[u8]>>) -> i64 {
    let b = v.as_ref().expect("present");
    i64::from_le_bytes(b[..8].try_into().unwrap())
}

fn enc(v: i64) -> Vec<u8> {
    v.to_le_bytes().to_vec()
}

const WAIT: Duration = Duration::from_millis(300);

/// Check one run of the Fekete, O'Neil and O'Neil read-only anomaly schedule:
/// checking `X` and savings `Y` start at 0, D deposits 20 into Y, W withdraws
/// 10 from X with a 1-unit penalty when it sees `X + Y < 10`, and R reads both.
/// Whatever committed, R's view must be a state of the serial order the
/// outcome implies, and that order must be the commit-timestamp order.
fn assert_serializable_outcome(
    r_view: (i64, i64),
    d_ts: Timestamp,
    w_ts: Option<Timestamp>,
    final_xy: (i64, i64),
) {
    match w_ts {
        None => {
            // W aborted: the history is D alone.
            assert_eq!(final_xy, (0, 20));
            assert!(
                r_view == (0, 0) || r_view == (0, 20),
                "R saw {r_view:?} with only D committed"
            );
        }
        Some(w_ts) if final_xy.0 == -11 => {
            // W charged the penalty, so W did not see D: the order is W, D.
            assert!(w_ts < d_ts, "W precedes D but has the larger timestamp");
            assert_eq!(final_xy, (-11, 20));
            assert!(
                [(0, 0), (-11, 0), (-11, 20)].contains(&r_view),
                "R saw {r_view:?}, which no serial order W, D produces"
            );
        }
        Some(w_ts) => {
            // No penalty: W saw D, so the order is D, W.
            assert!(d_ts < w_ts, "D precedes W but has the larger timestamp");
            assert_eq!(final_xy, (-10, 20));
            assert!(
                [(0, 0), (0, 20), (-10, 20)].contains(&r_view),
                "R saw {r_view:?}, which no serial order D, W produces"
            );
        }
    }
}

// ----------------------------------------------- read-only anomaly (ordering)

/// The read-only anomaly through a store that keeps the default
/// `try_commit_ordered`. D is paused inside the store after taking its
/// timestamp; W tries to commit meanwhile. In 1.0 W overtook D, applied with a
/// larger timestamp, and a read-only transaction cut between them saw D's
/// deposit without W's withdrawal although W charged the penalty.
#[cfg(feature = "serializable")]
#[test]
fn read_only_anomaly_is_rejected_through_default_ordering() {
    let (db, store) = shared::<PlainGated>();
    let _ = db.put(b"X".to_vec(), enc(0)).unwrap();
    let _ = db.put(b"Y".to_vec(), enc(0)).unwrap();

    let hd = store.before.arm(b"Y");
    let db_d = db.clone();
    let d = thread::spawn(move || {
        let mut tx = db_d.begin_serializable();
        let y = int(&tx.get(b"Y").unwrap());
        tx.put(b"Y".to_vec(), enc(y + 20));
        tx.commit()
    });
    hd.arrived.recv().unwrap();

    let hw = store.after.arm(b"X");
    let db_w = db.clone();
    let w = thread::spawn(move || {
        let mut tx = db_w.begin_serializable();
        let x = int(&tx.get(b"X").unwrap());
        let y = int(&tx.get(b"Y").unwrap());
        tx.put(b"X".to_vec(), enc(if x + y < 10 { x - 11 } else { x - 10 }));
        tx.commit()
    });

    let read = |db: &Db<Shared<PlainGated>>| {
        let r = db.begin_serializable();
        let view = (int(&r.get(b"X").unwrap()), int(&r.get(b"Y").unwrap()));
        let _ = r.commit().unwrap();
        view
    };

    let (r_view, d_ts, w_res);
    if hw.arrived.recv_timeout(WAIT).is_ok() {
        // W applied while D was in flight (the 1.0 schedule).
        hd.resume.send(()).unwrap();
        d_ts = d.join().unwrap().expect("deposit commits");
        r_view = read(&db);
        hw.resume.send(()).unwrap();
        w_res = w.join().unwrap();
    } else {
        // W could not start its commit while D was in flight.
        hd.resume.send(()).unwrap();
        d_ts = d.join().unwrap().expect("deposit commits");
        hw.arrived.recv().unwrap();
        r_view = read(&db);
        hw.resume.send(()).unwrap();
        w_res = w.join().unwrap();
    }
    let final_xy = (int(&db.get(b"X").unwrap()), int(&db.get(b"Y").unwrap()));
    assert_serializable_outcome(r_view, d_ts, w_res.ok(), final_xy);
}

/// The same anomaly against `MemoryStore`'s own ordering. W is paused while it
/// holds its shard locks, after taking its timestamp. D needs one of those
/// locks, so it can neither take a timestamp nor apply until W finishes, and it
/// must end up with the larger timestamp.
#[cfg(feature = "serializable")]
#[test]
fn read_only_anomaly_is_rejected_by_memory_store_ordering() {
    let (db, store) = shared::<LockedGated>();
    let _ = db.put(b"X".to_vec(), enc(0)).unwrap();
    let _ = db.put(b"Y".to_vec(), enc(0)).unwrap();

    // Both writers take their snapshots before either commits.
    let mut w_tx = db.begin_serializable();
    let x = int(&w_tx.get(b"X").unwrap());
    let y = int(&w_tx.get(b"Y").unwrap());
    w_tx.put(b"X".to_vec(), enc(if x + y < 10 { x - 11 } else { x - 10 }));
    let mut d_tx = db.begin_serializable();
    let dy = int(&d_tx.get(b"Y").unwrap());
    d_tx.put(b"Y".to_vec(), enc(dy + 20));

    let hw = store.at_stamp.arm(b"X");
    let w = thread::spawn(move || w_tx.commit());
    hw.arrived.recv().unwrap(); // W holds the X and Y shard locks

    let (d_done_tx, d_done) = channel();
    let d = thread::spawn(move || {
        let out = d_tx.commit();
        let _ = d_done_tx.send(());
        out
    });
    assert!(
        d_done.recv_timeout(WAIT).is_err(),
        "D must wait for the Y shard W holds"
    );

    // A read-only transaction cut now. Its reads would wait on the shard locks
    // W holds, so it reads after both commits finish; its snapshot is fixed at
    // this point either way.
    let r = db.begin_serializable();

    hw.resume.send(()).unwrap();
    let w_ts = w.join().unwrap().expect("withdrawal commits");
    let d_ts = d.join().unwrap().expect("deposit commits");
    let r_view = (int(&r.get(b"X").unwrap()), int(&r.get(b"Y").unwrap()));
    let _ = r.commit().unwrap();
    let final_xy = (int(&db.get(b"X").unwrap()), int(&db.get(b"Y").unwrap()));
    assert_serializable_outcome(r_view, d_ts, Some(w_ts), final_xy);
    assert_eq!(r_view, (0, 0));
}

// ------------------------------------------------------------- store panics

/// A panic inside the store's commit must cost that one transaction, not the
/// database. In 1.0 it left the watermark stuck forever: later commits were
/// acknowledged but never visible, and `Db::put` of a key it had already
/// written spun forever.
#[test]
fn store_panic_releases_its_timestamp() {
    let (db, store) = shared::<PlainGated>();
    *store.panic_on.lock().unwrap() = Some(b"boom".to_vec());
    let db_p = db.clone();
    let joined = thread::spawn(move || {
        let mut tx = db_p.begin();
        tx.put(b"boom".to_vec(), vec![1]);
        let _ = tx.commit();
    })
    .join();
    assert!(joined.is_err(), "the store panic reaches the caller");
    *store.panic_on.lock().unwrap() = None;

    let before = db.last_committed();
    let ts = db.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
    assert!(ts > before);
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v1"[..]));
    assert!(db.last_committed() >= ts);

    let (done_tx, done) = channel();
    let db_s = db.clone();
    let _second = thread::spawn(move || {
        let _ = done_tx.send(db_s.put(b"k".to_vec(), b"v2".to_vec()).is_ok());
    });
    assert_eq!(done.recv_timeout(Duration::from_secs(10)), Ok(true));
    assert_eq!(db.get(b"k").unwrap().as_deref(), Some(&b"v2"[..]));
    assert_eq!(db.get(b"boom").unwrap(), None);
}

// ------------------------------------------------- visibility after commit

/// `commit` returns only once its write is visible. In 1.0 a commit with a
/// larger timestamp returned while an earlier one was still in flight, and the
/// committing thread could not read its own write back.
#[test]
fn committed_write_is_visible_when_commit_returns() {
    let (db, store) = shared::<PlainGated>();
    let h = store.before.arm(b"slow");
    let db_slow = db.clone();
    let slow = thread::spawn(move || {
        let mut tx = db_slow.begin();
        tx.put(b"slow".to_vec(), vec![1]);
        tx.commit()
    });
    h.arrived.recv().unwrap();

    let (done_tx, done) = channel();
    let db_w = db.clone();
    let writer = thread::spawn(move || {
        let ts = db_w.put(b"k".to_vec(), b"v1".to_vec()).unwrap();
        let seen = db_w.get(b"k").unwrap();
        let snap_seen = db_w.snapshot().get(b"k").unwrap();
        let watermark = db_w.last_committed();
        let _ = done_tx.send(());
        (ts, seen, snap_seen, watermark)
    });
    // Give the writer time to finish if commit does not wait; then release the
    // slow commit either way.
    let _ = done.recv_timeout(WAIT);
    h.resume.send(()).unwrap();
    let _ = slow.join().unwrap().unwrap();
    let (ts, seen, snap_seen, watermark) = writer.join().unwrap();
    assert_eq!(seen.as_deref(), Some(&b"v1"[..]), "Db::get after put");
    assert_eq!(snap_seen.as_deref(), Some(&b"v1"[..]), "snapshot after put");
    assert!(watermark >= ts, "last_committed after put");
}

/// Autocommit writers of one hot key never fail and the last write wins, with
/// slow commits on other keys in flight. Exercises the conflict wait in
/// `Db::put`.
#[test]
fn autocommit_put_makes_progress_under_contention() {
    let db = Db::new();
    let handles: Vec<_> = (0..8u8)
        .map(|t| {
            let db = db.clone();
            thread::spawn(move || {
                for i in 0..200u16 {
                    let _ = db.put(b"hot".to_vec(), vec![t]).unwrap();
                    let _ = db.put(vec![t, (i % 7) as u8], vec![t]).unwrap();
                }
            })
        })
        .collect();
    for h in handles {
        h.join().unwrap();
    }
    let v = db.get(b"hot").unwrap().unwrap();
    assert!(v.len() == 1 && v[0] < 8);
}

// ------------------------------------------------------- durable failures

/// A durable commit over the log's record limit is refused before anything is
/// applied. In 1.0 it returned a durability error yet was visible to readers,
/// and vanished on reopen while a transaction that read it survived.
#[cfg(feature = "durability")]
#[test]
fn oversize_durable_commit_is_refused_and_never_visible() {
    use txn_db::TxnError;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn.wal");
    {
        let db = Db::open(&path).unwrap();
        let _ = db.put(b"before".to_vec(), b"1".to_vec()).unwrap();
        let mut tx = db.begin();
        tx.put(b"blob".to_vec(), vec![7u8; 65 * 1024 * 1024]);
        let err = tx.commit().unwrap_err();
        match &err {
            TxnError::TooLarge { len, limit } => {
                assert_eq!(*limit, 64 * 1024 * 1024);
                assert!(len > limit);
            }
            other => panic!("expected TooLarge, got {other:?}"),
        }
        assert!(!err_is_fatal(&err));
        assert!(!err.is_retryable());
        assert_eq!(db.get(b"blob").unwrap(), None);

        // The database stays usable.
        let _ = db.put(b"after".to_vec(), b"2".to_vec()).unwrap();
        assert_eq!(db.get(b"after").unwrap().as_deref(), Some(&b"2"[..]));
    }
    let db = Db::open(&path).unwrap();
    assert_eq!(db.get(b"blob").unwrap(), None);
    assert_eq!(db.get(b"before").unwrap().as_deref(), Some(&b"1"[..]));
    assert_eq!(db.get(b"after").unwrap().as_deref(), Some(&b"2"[..]));
}

#[cfg(feature = "durability")]
fn err_is_fatal(err: &txn_db::TxnError) -> bool {
    use error_forge::ForgeError;
    err.is_fatal()
}

/// A commit just under the limit still goes through and survives reopening.
#[cfg(feature = "durability")]
#[test]
fn durable_commit_at_the_limit_is_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn.wal");
    // Header 13 bytes, one write: 4 + key + 1 + 4 + value.
    let value_len = 64 * 1024 * 1024 - 13 - 4 - 1 - 1 - 4;
    let ts = {
        let db = Db::open(&path).unwrap();
        let mut tx = db.begin();
        tx.put(b"k".to_vec(), vec![1u8; value_len]);
        tx.commit().unwrap()
    };
    let db = Db::open(&path).unwrap();
    assert_eq!(db.last_committed(), ts);
    assert_eq!(db.get(b"k").unwrap().map(|v| v.len()), Some(value_len));
}

/// Replay keeps only the current version of each key.
#[cfg(feature = "durability")]
#[test]
fn reopen_collects_replayed_history() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn.wal");
    {
        let db = Db::open(&path).unwrap();
        for v in 0..100u8 {
            let _ = db.put(b"hot".to_vec(), vec![v]).unwrap();
        }
    }
    let db = Db::open(&path).unwrap();
    assert_eq!(db.collect_garbage(), 0, "history was collected on open");
    assert_eq!(db.get(b"hot").unwrap().as_deref(), Some(&[99u8][..]));
}
