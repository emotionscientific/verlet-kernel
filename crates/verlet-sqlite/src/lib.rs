//! Engine-owner crate for every first-party verlet SQLite store (ADR 0005).
//!
//! This crate is the only place in the workspace that depends on a SQLite
//! engine. Store crates depend on `verlet-sqlite` and receive configured
//! connections; they never name an engine directly. The crate is named for
//! the file format, not the engine — the current engine is Turso
//! (tursodatabase/turso, pinned exactly), and backing it out is an internal
//! change here, not a store migration.
//!
//! Rules carried by this crate (ADR 0005 plus the EMO-427 transaction policy):
//! - WAL is the default journal mode; `JournalMode::Mvcc` requires a
//!   per-store justification because it makes the file a Turso extension
//!   until checkpointed back.
//! - One engine per database file: never open a file owned by a running
//!   verlet process with stock SQLite tooling (`sqlite3` CLI, rusqlite
//!   builds). Post-migration debugging goes through `tursodb` or daemon RPC.
//! - Production write transactions use [`TransactionBehavior::Immediate`],
//!   allowing competing writers to serialize through the configured busy
//!   timeout. A deferred transaction that reads before writing can instead
//!   receive `BusySnapshot` when a peer commits after its snapshot read; such
//!   callers must roll back the whole transaction and retry with a bound.
//! - The DST seam is [`Db::open_with_io`]: scenario harnesses drive the
//!   engine through simulated, fault-injectable, deterministic IO.

// The two re-export blocks below are the workspace's only exception to the
// no-`pub use` rule. ADR 0005 makes this crate the single engine dependency:
// store crates name `verlet_sqlite`, never `turso`. Spelling the engine at
// every call site would put a second direct engine dependency in three crates
// and end that ownership.
pub use turso::{
    Connection, IntoParams, Row, Rows, Statement, Value, params, transaction::TransactionBehavior,
};

/// Turso IO contracts re-exported by the engine owner for deterministic test
/// harnesses. Callers of [`Db::open_with_io`] should not need a second direct
/// engine dependency merely to implement the seam it exposes.
pub mod io {
    pub use turso_core::io::{FileId, FileSyncType};
    pub use turso_core::{
        Buffer, Clock, Completion, CompletionError, File, IO, LimboError, MemoryIO,
        MonotonicInstant, OpenFlags, WallClockInstant,
    };
}

/// Errors surfaced by the engine-owner layer.
///
/// Store crates map this into their own error vocabulary at the trait
/// boundary (e.g. `HistoryError::Storage`), the same way they map
/// `rusqlite::Error` today.
#[derive(Debug, thiserror::Error)]
pub enum SqliteError {
    #[error("sqlite engine error: {0}")]
    Engine(#[from] turso::Error),
    #[error("sqlite engine error: {0}")]
    Core(#[from] turso_core::LimboError),
    #[error("sqlite io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type SqliteResult<T> = Result<T, SqliteError>;

/// Drive a future to completion with a minimal thread park/unpark poll loop.
///
/// This exists for synchronous store surfaces layered over the async SQLite
/// engine. It is reentrant-safe where `futures_executor::block_on` panics when
/// nested because it has no executor-specific nesting guard. Do not call this
/// from an async context: pending work parks the current thread and can block
/// the executor running it.
pub fn block_on<F: std::future::Future>(future: F) -> F::Output {
    struct ThreadWaker {
        thread: std::thread::Thread,
        notified: std::sync::atomic::AtomicBool,
    }

    impl std::task::Wake for ThreadWaker {
        fn wake(self: std::sync::Arc<Self>) {
            self.wake_by_ref();
        }

        fn wake_by_ref(self: &std::sync::Arc<Self>) {
            self.notified
                .store(true, std::sync::atomic::Ordering::Release);
            self.thread.unpark();
        }
    }

    let mut future = std::pin::pin!(future);
    let thread_waker = std::sync::Arc::new(ThreadWaker {
        thread: std::thread::current(),
        notified: std::sync::atomic::AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(std::sync::Arc::clone(&thread_waker));
    let mut context = std::task::Context::from_waker(&waker);
    loop {
        match future.as_mut().poll(&mut context) {
            std::task::Poll::Ready(output) => return output,
            std::task::Poll::Pending => {
                while !thread_waker
                    .notified
                    .swap(false, std::sync::atomic::Ordering::AcqRel)
                {
                    std::thread::park();
                }
            }
        }
    }
}

/// Journal mode policy for a database file.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum JournalMode {
    /// The workspace default; format-compatible with stock SQLite in both
    /// directions, which is what keeps rollback a dependency flip.
    #[default]
    Wal,
    /// Turso's MVCC mode (`BEGIN CONCURRENT`, concurrent writers). Opt-in
    /// per store with a recorded justification (ADR 0005 §3): a file in
    /// this mode is not freely openable by stock SQLite until checkpointed
    /// back.
    Mvcc,
}

/// Open-time configuration applied to every connection handed out by [`Db`].
///
/// Defaults reproduce what every current store sets by hand: WAL,
/// `foreign_keys = ON`, 5s busy timeout, read-write.
#[derive(Debug, Clone)]
pub struct DbConfig {
    pub journal_mode: JournalMode,
    pub foreign_keys: bool,
    pub busy_timeout: std::time::Duration,
    /// Enforced by Turso's engine-level read-only open flag. Connections also
    /// receive `PRAGMA query_only` as defense in depth.
    pub read_only: bool,
}

impl Default for DbConfig {
    fn default() -> Self {
        Self {
            journal_mode: JournalMode::Wal,
            foreign_keys: true,
            busy_timeout: std::time::Duration::from_secs(5),
            read_only: false,
        }
    }
}

/// A configured database handle. Cheap to clone; a store holds one and
/// acquires connections per operation (Turso connections are inexpensive,
/// and a fresh connection is also what picks up externally applied
/// changes — a held connection reads a stable snapshot).
#[derive(Clone)]
pub struct Db {
    inner: turso::Database,
    config: DbConfig,
    /// Shared Turso file lock acquired before an engine-level read-only
    /// database. Turso's read-only flag deliberately skips its automatic
    /// owner lock, so this guard preserves the verified cross-process refusal
    /// without opening the evidence file for write access.
    _owner_lock: Option<std::sync::Arc<dyn turso_core::File>>,
}

impl Db {
    /// Open the database at `path`, creating parent directories, and apply
    /// `config`. The file is created if missing (unless `read_only`).
    pub async fn open(path: impl AsRef<std::path::Path>, config: DbConfig) -> SqliteResult<Self> {
        let path = path.as_ref();
        let path_str = sqlite_path(path)?;
        prepare_path(path, config.read_only)?;
        let builder = turso::Builder::new_local(path_str).read_only(config.read_only);
        let (builder, owner_lock) = if config.read_only {
            let io: std::sync::Arc<dyn turso_core::IO> =
                std::sync::Arc::new(turso_core::PlatformIO::new()?);
            let owner_lock = acquire_read_lock(path_str, &io)?;
            (builder.with_io_impl(io), Some(owner_lock))
        } else {
            (builder, None)
        };
        let inner = builder.build().await?;
        Self::from_database(inner, config, owner_lock).await
    }

    /// Open with a caller-supplied IO implementation. This is the DST seam:
    /// scenario harnesses pass simulated IO here so fault plans can act on
    /// the engine's reads, writes, and syncs — below the store traits that
    /// `FaultingRuntimeStore` wraps. Path creation and existence checks are
    /// delegated to that IO implementation rather than the host filesystem.
    pub async fn open_with_io(
        path: impl AsRef<std::path::Path>,
        config: DbConfig,
        io: std::sync::Arc<dyn io::IO>,
    ) -> SqliteResult<Self> {
        let path = path.as_ref();
        let path_str = sqlite_path(path)?;
        let owner_lock = if config.read_only {
            Some(acquire_read_lock(path_str, &io)?)
        } else {
            None
        };
        let inner = turso::Builder::new_local(path_str)
            .read_only(config.read_only)
            .with_io_impl(io)
            .build()
            .await?;
        Self::from_database(inner, config, owner_lock).await
    }

    /// In-memory database for tests; same pragma treatment as [`Db::open`].
    pub async fn in_memory(config: DbConfig) -> SqliteResult<Self> {
        let inner = turso::Builder::new_local(":memory:")
            .read_only(config.read_only)
            .build()
            .await?;
        Self::from_database(inner, config, None).await
    }

    /// A new connection with this database's pragmas applied
    /// (`busy_timeout`, `foreign_keys`, and defense-in-depth `query_only` for
    /// an engine-enforced read-only database).
    pub async fn connect(&self) -> SqliteResult<Connection> {
        let conn = self.inner.connect()?;
        apply_connection_config(&conn, &self.config).await?;
        Ok(conn)
    }

    /// The configuration this handle was opened with.
    pub fn config(&self) -> &DbConfig {
        &self.config
    }

    /// Construct a handle and apply file-level journal policy once before
    /// handing out independently configured connections.
    async fn from_database(
        inner: turso::Database,
        config: DbConfig,
        owner_lock: Option<std::sync::Arc<dyn turso_core::File>>,
    ) -> SqliteResult<Self> {
        let db = Self {
            inner,
            config,
            _owner_lock: owner_lock,
        };
        let conn = db.connect().await?;
        if !db.config.read_only {
            let journal_mode = match db.config.journal_mode {
                JournalMode::Wal => "'wal'",
                JournalMode::Mvcc => "'mvcc'",
            };
            conn.pragma_update("journal_mode", journal_mode).await?;
        }
        Ok(db)
    }
}

fn acquire_read_lock(
    path: &str,
    io: &std::sync::Arc<dyn turso_core::IO>,
) -> SqliteResult<std::sync::Arc<dyn turso_core::File>> {
    let file = io.open_file(path, turso_core::OpenFlags::ReadOnly, true)?;
    file.lock_file(false)?;
    Ok(file)
}

fn sqlite_path(path: &std::path::Path) -> SqliteResult<&str> {
    path.to_str().ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "sqlite database path is not valid UTF-8",
        )
        .into()
    })
}

/// Create a writable database's parent directory, or reject a missing
/// read-only database before Turso can create it as a side effect of open.
fn prepare_path(path: &std::path::Path, read_only: bool) -> SqliteResult<()> {
    if read_only {
        if !path.is_file() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!(
                    "read-only sqlite database does not exist: {}",
                    path.display()
                ),
            )
            .into());
        }
    } else if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// Apply connection-local policy to a newly acquired Turso connection.
async fn apply_connection_config(conn: &Connection, config: &DbConfig) -> SqliteResult<()> {
    conn.busy_timeout(config.busy_timeout)?;
    conn.pragma_update(
        "foreign_keys",
        if config.foreign_keys { "ON" } else { "OFF" },
    )
    .await?;
    if config.read_only {
        conn.pragma_update("query_only", 1).await?;
    }
    Ok(())
}

/// Quote a SQLite identifier by doubling embedded quote characters.
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

/// Column names of `table` via `PRAGMA table_info`, in declaration order.
/// Empty when the table does not exist. Replaces the hand-rolled
/// `table_info` probes in the current stores' migration paths.
pub async fn table_columns(conn: &Connection, table: &str) -> SqliteResult<Vec<String>> {
    let mut rows = conn
        .query(
            format!("PRAGMA table_info({})", quote_identifier(table)),
            (),
        )
        .await?;
    let mut columns = Vec::new();
    while let Some(row) = rows.next().await? {
        columns.push(row.get(1)?);
    }
    Ok(columns)
}

/// Add `column` (full `ALTER TABLE ... ADD COLUMN` tail in `ddl`) when it
/// is absent — the additive-migration idiom every store repeats today.
///
/// `table` is quoted as an identifier, but `ddl` is interpolated into the
/// statement verbatim, because it is a DDL fragment and not a value a
/// placeholder could carry. `ddl` must therefore be a compile-time literal;
/// never route caller- or model-supplied text into it.
pub async fn ensure_column(
    conn: &Connection,
    table: &str,
    column: &str,
    ddl: &str,
) -> SqliteResult<()> {
    let result = conn
        .execute(
            format!("ALTER TABLE {} ADD COLUMN {ddl}", quote_identifier(table)),
            (),
        )
        .await;
    if let Err(error) = result {
        if table_columns(conn, table)
            .await?
            .iter()
            .any(|existing| existing == column)
        {
            return Ok(());
        }
        return Err(error.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {

    #[test]
    fn nested_block_on_preserves_the_outer_wake() {
        let (completed_tx, completed_rx) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let outer_polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let result = crate::block_on(std::future::poll_fn({
                let outer_polls = std::sync::Arc::clone(&outer_polls);
                move |outer_context| {
                    if outer_polls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) > 0 {
                        return std::task::Poll::Ready("completed");
                    }

                    let inner_polls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
                    let inner_waker =
                        std::sync::Arc::new(std::sync::Mutex::new(None::<std::task::Waker>));
                    let delayed_wake = {
                        let inner_polls = std::sync::Arc::clone(&inner_polls);
                        let inner_waker = std::sync::Arc::clone(&inner_waker);
                        std::thread::spawn(move || {
                            std::thread::sleep(std::time::Duration::from_millis(50));
                            if inner_polls.load(std::sync::atomic::Ordering::SeqCst) == 1
                                && let Some(waker) = inner_waker.lock().unwrap().take()
                            {
                                waker.wake();
                            }
                        })
                    };
                    crate::block_on(std::future::poll_fn({
                        let inner_polls = std::sync::Arc::clone(&inner_polls);
                        let inner_waker = std::sync::Arc::clone(&inner_waker);
                        let outer_waker = outer_context.waker().clone();
                        move |inner_context| match inner_polls
                            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                        {
                            0 => {
                                *inner_waker.lock().unwrap() = Some(inner_context.waker().clone());
                                outer_waker.wake_by_ref();
                                std::task::Poll::Pending
                            }
                            1 => {
                                inner_context.waker().wake_by_ref();
                                std::task::Poll::Pending
                            }
                            _ => std::task::Poll::Ready(()),
                        }
                    }));
                    delayed_wake.join().unwrap();
                    std::task::Poll::Pending
                }
            }));
            completed_tx.send(result).unwrap();
        });

        assert_eq!(
            completed_rx.recv_timeout(std::time::Duration::from_secs(30)),
            Ok("completed"),
            "nested block_on consumed the outer future's wake"
        );
    }

    async fn pragma_value(conn: &crate::Connection, name: &str) -> crate::Value {
        let mut value = None;
        conn.pragma_query(name, |row| {
            value = Some(row.get_value(0).expect("pragma value"));
            Ok(())
        })
        .await
        .expect("pragma query");
        value.expect("pragma returned one row")
    }

    fn directory_file_bytes(
        path: &std::path::Path,
    ) -> std::collections::BTreeMap<std::ffi::OsString, Vec<u8>> {
        std::fs::read_dir(path)
            .unwrap()
            .map(|entry| {
                let entry = entry.unwrap();
                (entry.file_name(), std::fs::read(entry.path()).unwrap())
            })
            .collect()
    }

    #[tokio::test]
    async fn tempfile_open_creates_parents_and_applies_connection_pragmas() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("nested/state/metadata.turso");
        let config = crate::DbConfig {
            busy_timeout: std::time::Duration::from_millis(1_234),
            ..crate::DbConfig::default()
        };

        let db = crate::Db::open(&path, config).await.unwrap();
        assert!(path.exists());
        let conn = db.connect().await.unwrap();

        assert_eq!(
            pragma_value(&conn, "journal_mode").await,
            crate::Value::Text("wal".into())
        );
        assert_eq!(
            pragma_value(&conn, "foreign_keys").await,
            crate::Value::Integer(1)
        );
        assert_eq!(
            pragma_value(&conn, "busy_timeout").await,
            crate::Value::Integer(1_234)
        );
        assert_eq!(
            pragma_value(&conn, "query_only").await,
            crate::Value::Integer(0)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn invalid_utf8_path_is_rejected_without_creating_parents() {
        use std::os::unix::ffi::OsStringExt as _;

        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("must-not-exist");
        let path = parent.join(std::ffi::OsString::from_vec(vec![0xff]));

        assert!(
            crate::Db::open(path, crate::DbConfig::default())
                .await
                .is_err()
        );
        assert!(!parent.exists());
    }

    #[tokio::test]
    async fn custom_io_open_does_not_touch_the_host_filesystem() {
        let temp = tempfile::tempdir().unwrap();
        let parent = temp.path().join("owned-by-custom-io");
        let path = parent.join("store.sqlite3");
        let io: std::sync::Arc<dyn turso_core::IO> =
            std::sync::Arc::new(turso_core::MemoryIO::new());

        let db = crate::Db::open_with_io(&path, crate::DbConfig::default(), io)
            .await
            .unwrap();
        db.connect()
            .await
            .unwrap()
            .execute("CREATE TABLE records (value TEXT)", ())
            .await
            .unwrap();

        assert!(!parent.exists());
    }

    #[tokio::test]
    async fn in_memory_applies_pragmas_and_migrates_columns_idempotently() {
        let db = crate::Db::in_memory(crate::DbConfig::default())
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        assert_eq!(
            pragma_value(&conn, "foreign_keys").await,
            crate::Value::Integer(1)
        );
        assert_eq!(
            pragma_value(&conn, "busy_timeout").await,
            crate::Value::Integer(5_000)
        );

        conn.execute("CREATE TABLE widgets (id INTEGER PRIMARY KEY)", ())
            .await
            .unwrap();
        assert_eq!(
            crate::table_columns(&conn, "widgets").await.unwrap(),
            vec!["id"]
        );
        assert!(
            crate::table_columns(&conn, "missing_table")
                .await
                .unwrap()
                .is_empty()
        );

        crate::ensure_column(&conn, "widgets", "label", "label TEXT NOT NULL DEFAULT ''")
            .await
            .unwrap();
        crate::ensure_column(&conn, "widgets", "label", "label TEXT NOT NULL DEFAULT ''")
            .await
            .unwrap();
        assert_eq!(
            crate::table_columns(&conn, "widgets").await.unwrap(),
            vec!["id", "label"]
        );
    }

    #[tokio::test]
    async fn tempfile_table_columns_and_ensure_column_survive_reopen() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("store.sqlite3");
        let db = crate::Db::open(&path, crate::DbConfig::default())
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        conn.execute("CREATE TABLE records (key TEXT PRIMARY KEY)", ())
            .await
            .unwrap();
        crate::ensure_column(&conn, "records", "payload", "payload BLOB")
            .await
            .unwrap();
        drop(conn);
        drop(db);

        let reopened = crate::Db::open(&path, crate::DbConfig::default())
            .await
            .unwrap();
        let conn = reopened.connect().await.unwrap();
        assert_eq!(
            crate::table_columns(&conn, "records").await.unwrap(),
            vec!["key", "payload"]
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_ensure_column_calls_are_idempotent() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("store.sqlite3");
        let db = crate::Db::open(&path, crate::DbConfig::default())
            .await
            .unwrap();
        db.connect()
            .await
            .unwrap()
            .execute("CREATE TABLE records (key TEXT PRIMARY KEY)", ())
            .await
            .unwrap();

        let workers = 8;
        let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(workers));
        let mut handles = Vec::new();
        for _ in 0..workers {
            let db = db.clone();
            let barrier = barrier.clone();
            handles.push(tokio::spawn(async move {
                let conn = db.connect().await.unwrap();
                barrier.wait().await;
                crate::ensure_column(&conn, "records", "payload", "payload BLOB").await
            }));
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }
        assert_eq!(
            crate::table_columns(&db.connect().await.unwrap(), "records")
                .await
                .unwrap(),
            vec!["key", "payload"]
        );
    }

    #[tokio::test]
    async fn read_only_connections_can_read_and_reject_writes() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("store.sqlite3");
        let writable = crate::Db::open(&path, crate::DbConfig::default())
            .await
            .unwrap();
        let conn = writable.connect().await.unwrap();
        conn.execute_batch(
            "CREATE TABLE records (value TEXT); INSERT INTO records VALUES ('seed');",
        )
        .await
        .unwrap();
        let mut checkpoint = conn
            .query("PRAGMA wal_checkpoint(TRUNCATE)", ())
            .await
            .unwrap();
        while checkpoint.next().await.unwrap().is_some() {}
        drop(checkpoint);
        drop(conn);
        drop(writable);
        let before = directory_file_bytes(temp.path());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
        }

        let read_only = crate::Db::open(
            &path,
            crate::DbConfig {
                read_only: true,
                ..crate::DbConfig::default()
            },
        )
        .await
        .unwrap();
        let conn = read_only.connect().await.unwrap();
        assert_eq!(
            pragma_value(&conn, "query_only").await,
            crate::Value::Integer(1)
        );
        let mut rows = conn.query("SELECT value FROM records", ()).await.unwrap();
        assert_eq!(
            rows.next()
                .await
                .unwrap()
                .unwrap()
                .get::<String>(0)
                .unwrap(),
            "seed"
        );
        drop(rows);
        conn.pragma_update("query_only", 0).await.unwrap();
        assert!(
            conn.execute("INSERT INTO records VALUES ('denied')", ())
                .await
                .is_err()
        );
        drop(conn);
        drop(read_only);
        assert_eq!(directory_file_bytes(temp.path()), before);
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn concurrent_transaction_commit_regression() {
        // Turso 0.6.1 returned `cannot commit - no transaction is active` from
        // Transaction::commit under concurrent read-then-upsert traffic; fixed
        // in the 0.7 line (verified on 0.7.0-pre.18, EMO-412). Turso 0.7 can
        // legitimately reject a deferred write upgrade with BusySnapshot, so
        // this guard rolls back and retries only that typed error. Every other
        // error, including the 0.6.1 commit failure, remains fatal.
        const MAX_ATTEMPTS_PER_WORKER: usize = 4;
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("transaction-repro.sqlite3");
        let db = crate::Db::open(&path, crate::DbConfig::default())
            .await
            .unwrap();
        let conn = db.connect().await.unwrap();
        conn.execute(
            "CREATE TABLE records (
                id INTEGER PRIMARY KEY,
                committed_workers INTEGER NOT NULL
            )",
            (),
        )
        .await
        .unwrap();
        conn.execute("INSERT INTO records VALUES (1, 0)", ())
            .await
            .unwrap();
        drop(conn);

        let workers = 2;
        let start_barrier = std::sync::Arc::new(tokio::sync::Barrier::new(workers));
        let first_snapshot_barrier = std::sync::Arc::new(tokio::sync::Barrier::new(workers));
        let mut handles = Vec::new();
        for worker in 0..workers {
            let db = db.clone();
            let start_barrier = start_barrier.clone();
            let first_snapshot_barrier = first_snapshot_barrier.clone();
            handles.push(tokio::spawn(async move {
                let mut conn = db.connect().await.unwrap();
                start_barrier.wait().await;
                let mut busy_snapshot_retries = 0;

                for attempt in 1..=MAX_ATTEMPTS_PER_WORKER {
                    let tx = conn
                        .transaction_with_behavior(crate::TransactionBehavior::Deferred)
                        .await
                        .unwrap_or_else(|error| {
                            panic!("worker {worker} attempt {attempt} begin failed: {error:?}")
                        });
                    let mut rows = tx
                        .query("SELECT committed_workers FROM records WHERE id = 1", ())
                        .await
                        .unwrap_or_else(|error| {
                            panic!("worker {worker} attempt {attempt} read failed: {error:?}")
                        });
                    let committed_workers = rows
                        .next()
                        .await
                        .unwrap_or_else(|error| {
                            panic!("worker {worker} attempt {attempt} row read failed: {error:?}")
                        })
                        .expect("records row must exist")
                        .get::<i64>(0)
                        .unwrap();
                    drop(rows);

                    if attempt == 1 {
                        // Both workers now own the same deferred snapshot. One
                        // write upgrade must lose the race and retry.
                        first_snapshot_barrier.wait().await;
                    }

                    let worker_bit = 1_i64 << worker;
                    match tx
                        .execute(
                            "INSERT INTO records VALUES (1, ?1)
                             ON CONFLICT(id) DO UPDATE
                             SET committed_workers = excluded.committed_workers",
                            [committed_workers | worker_bit],
                        )
                        .await
                    {
                        Ok(1) => {}
                        Ok(changed) => panic!(
                            "worker {worker} attempt {attempt} updated {changed} rows, expected one"
                        ),
                        Err(turso::Error::BusySnapshot(_)) => {
                            busy_snapshot_retries += 1;
                            tx.rollback().await.unwrap_or_else(|error| {
                                panic!(
                                    "worker {worker} attempt {attempt} BusySnapshot rollback failed: {error:?}"
                                )
                            });
                            tokio::task::yield_now().await;
                            continue;
                        }
                        Err(error) => {
                            panic!("worker {worker} attempt {attempt} write failed: {error:?}")
                        }
                    }

                    // In pinned Turso, BusySnapshot is produced only while a
                    // statement upgrades a stale read snapshot to a writer.
                    // A successful write already owns the WAL write lock, so
                    // COMMIT has no BusySnapshot path to retry here.
                    tx.commit().await.unwrap_or_else(|error| {
                        panic!("worker {worker} attempt {attempt} commit failed: {error:?}")
                    });
                    return (attempt, busy_snapshot_retries);
                }

                panic!("worker {worker} exhausted {MAX_ATTEMPTS_PER_WORKER} attempts")
            }));
        }

        let mut total_attempts = 0;
        let mut total_busy_snapshot_retries = 0;
        for handle in handles {
            let (attempts, busy_snapshot_retries) = handle.await.unwrap();
            total_attempts += attempts;
            total_busy_snapshot_retries += busy_snapshot_retries;
        }
        assert_eq!(
            total_busy_snapshot_retries, 1,
            "forced same-snapshot race must produce exactly one BusySnapshot"
        );
        assert_eq!(
            total_attempts,
            workers + total_busy_snapshot_retries,
            "each BusySnapshot must account for exactly one whole-transaction retry"
        );
        assert!(
            total_attempts <= workers * MAX_ATTEMPTS_PER_WORKER,
            "deferred retry bound exceeded: {total_attempts} total attempts"
        );

        let conn = db.connect().await.unwrap();
        let mut rows = conn
            .query("SELECT committed_workers FROM records WHERE id = 1", ())
            .await
            .unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            0b11,
            "both workers' effects must survive their successful commits"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn immediate_transactions_serialize_without_busy_snapshot() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("immediate-serialization.sqlite3");
        let db = crate::Db::open(&path, crate::DbConfig::default())
            .await
            .unwrap();
        db.connect()
            .await
            .unwrap()
            .execute("CREATE TABLE counter (value INTEGER NOT NULL)", ())
            .await
            .unwrap();
        db.connect()
            .await
            .unwrap()
            .execute("INSERT INTO counter VALUES (0)", ())
            .await
            .unwrap();

        let workers = 2;
        let begin_barrier = std::sync::Arc::new(tokio::sync::Barrier::new(workers));
        let mut handles = Vec::new();
        for worker in 0..workers {
            let db = db.clone();
            let begin_barrier = begin_barrier.clone();
            handles.push(tokio::spawn(async move {
                let mut conn = db.connect().await.unwrap();
                // Force both workers to contend at BEGIN IMMEDIATE. The loser
                // must busy-wait under DbConfig::default(), not take a stale
                // deferred snapshot.
                begin_barrier.wait().await;
                let tx = conn
                    .transaction_with_behavior(crate::TransactionBehavior::Immediate)
                    .await
                    .unwrap_or_else(|error| {
                        panic!("worker {worker} BEGIN IMMEDIATE failed: {error:?}")
                    });
                let mut rows = tx.query("SELECT value FROM counter", ()).await.unwrap();
                let value = rows
                    .next()
                    .await
                    .unwrap()
                    .expect("counter row must exist")
                    .get::<i64>(0)
                    .unwrap();
                drop(rows);
                tx.execute("UPDATE counter SET value = ?1", [value + 1])
                    .await
                    .unwrap_or_else(|error| {
                        panic!("worker {worker} Immediate write failed: {error:?}")
                    });
                tx.commit().await.unwrap_or_else(|error| {
                    panic!("worker {worker} Immediate commit failed: {error:?}")
                });
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        let conn = db.connect().await.unwrap();
        let mut rows = conn.query("SELECT value FROM counter", ()).await.unwrap();
        assert_eq!(
            rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
            2,
            "Immediate transactions must serialize both increments"
        );
    }
}
