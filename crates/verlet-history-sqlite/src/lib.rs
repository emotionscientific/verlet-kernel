#[derive(Clone)]
pub struct SqliteSessionStore {
    inner: verlet_sqlite::Db,
    writer: std::sync::Arc<tokio::sync::Mutex<()>>,
    /// The placement-lease epoch every journal event append through this
    /// handle presents (EMO-533). 0 is the unplaced/single-instance default.
    /// Cloning copies the value; a daemon must open every handle onto its store
    /// with the one epoch its provisioning gave it — a 0-epoch side handle
    /// against a store already fenced higher fails closed by design.
    lease_epoch: u64,
}

async fn cancellation_safe<T>(
    future: impl std::future::Future<Output = verlet_history::HistoryResult<T>> + Send + 'static,
) -> verlet_history::HistoryResult<T>
where
    T: Send + 'static,
{
    tokio::spawn(future).await.map_err(|error| {
        verlet_history::HistoryError::Storage(format!("sqlite transaction task failed: {error}"))
    })?
}

impl SqliteSessionStore {
    pub async fn open(path: impl AsRef<std::path::Path>) -> verlet_history::HistoryResult<Self> {
        let inner = verlet_sqlite::Db::open(path, verlet_sqlite::DbConfig::default())
            .await
            .map_err(verlet_history::storage_error)?;
        Self::from_db(inner).await
    }

    pub async fn open_read_only(
        path: impl AsRef<std::path::Path>,
    ) -> verlet_history::HistoryResult<Self> {
        let inner = verlet_sqlite::Db::open(
            path,
            verlet_sqlite::DbConfig {
                read_only: true,
                ..verlet_sqlite::DbConfig::default()
            },
        )
        .await
        .map_err(verlet_history::storage_error)?;
        Ok(Self {
            inner,
            writer: std::sync::Arc::new(tokio::sync::Mutex::new(())),
            lease_epoch: 0,
        })
    }

    pub async fn in_memory() -> verlet_history::HistoryResult<Self> {
        let inner = verlet_sqlite::Db::in_memory(verlet_sqlite::DbConfig::default())
            .await
            .map_err(verlet_history::storage_error)?;
        Self::from_db(inner).await
    }

    /// Build the store over an engine-owner handle supplied by the caller.
    ///
    /// The DST scenario lane uses this with a [`verlet_sqlite::Db`] opened through
    /// `Db::open_with_io`; the store remains unaware of the concrete engine IO
    /// trait and applies the same schema initialization as [`Self::open`].
    pub async fn from_db(inner: verlet_sqlite::Db) -> verlet_history::HistoryResult<Self> {
        cancellation_safe(async move {
            let store = Self {
                inner,
                writer: std::sync::Arc::new(tokio::sync::Mutex::new(())),
                lease_epoch: 0,
            };
            {
                let _writer = store.write_guard().await;
                let mut connection = store.connect().await?;
                init_sqlite_schema(&mut connection).await?;
            }
            Ok(store)
        })
        .await
    }

    /// The epoch this handle presents on every journal append (EMO-533).
    /// Fences are per stream and durable (`journal_lease_epochs`); the epoch
    /// is per handle. See [`verlet_history::HistoryError::StaleLeaseEpoch`]
    /// for the rejection contract.
    pub fn with_lease_epoch(mut self, lease_epoch: u64) -> Self {
        self.lease_epoch = lease_epoch;
        self
    }

    async fn connect(&self) -> verlet_history::HistoryResult<verlet_sqlite::Connection> {
        self.inner
            .connect()
            .await
            .map_err(verlet_history::storage_error)
    }

    /// Admit one history mutation at a time across every clone of this store.
    ///
    /// Reads still use independent Turso connections. Writers wait on Tokio's
    /// fair FIFO mutex before opening their per-operation connection and hold
    /// the guard through transaction commit, so daemon bursts cannot turn 200
    /// concurrent RPC tasks into 200 competing WAL writers. Each mutation
    /// keeps its existing immediate transaction and commit boundary; this gate
    /// changes concurrency only, not durability or journal ordering.
    #[doc(hidden)]
    pub async fn daemon_write_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.writer.lock().await
    }

    async fn write_guard(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.daemon_write_guard().await
    }

    /// Clone the engine-owner handle for daemon-owned tables that must share
    /// one transaction with the history stream.
    ///
    /// This is plumbing for store-primary protocols, not an alternate event
    /// append surface. Callers must retain the one-engine-per-file rule from
    /// `verlet-sqlite` and use [`Self::append_events_fenced_in_transaction`]
    /// rather than duplicating the event schema.
    #[doc(hidden)]
    pub fn sqlite_database(&self) -> verlet_sqlite::Db {
        self.inner.clone()
    }

    /// Apply the ordinary expected-tail fence and append inside a transaction
    /// the caller already owns.
    ///
    /// # Correctness
    ///
    /// The transaction must be `Immediate`, opened before any authority read
    /// whose result gates this append. A `Deferred` transaction can lose the
    /// writer race after taking its snapshot, so using one here violates the
    /// read-then-write policy even if the eventual insert happens to succeed.
    /// This seam does not begin or commit the transaction on the caller's
    /// behalf, and deliberately does not reacquire the store's writer gate:
    /// the caller already holds the database write lock. A caller must not
    /// invoke an ordinary store mutation before ending that transaction,
    /// because ordinary mutations acquire the gate before the database lock.
    #[doc(hidden)]
    pub async fn append_events_fenced_in_transaction(
        &self,
        transaction: &verlet_sqlite::Connection,
        stream_id: &verlet_history::EventStreamId,
        expected_next_sequence: verlet_history::EventSequence,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        sqlite_enforce_lease_epoch(transaction, stream_id, self.lease_epoch).await?;
        let actual_next_sequence = sqlite_next_event_sequence(transaction, stream_id).await?;
        if actual_next_sequence != expected_next_sequence.get() {
            return Err(verlet_history::HistoryError::AppendFenceConflict {
                stream_id: stream_id.clone(),
                expected_next_sequence: expected_next_sequence.get(),
                actual_next_sequence,
            });
        }
        let mut appended = Vec::with_capacity(records.len());
        for record in records {
            appended.push(sqlite_insert_event(transaction, stream_id, record).await?);
        }
        Ok(appended)
    }

    pub async fn list_control_stream_coordinates(
        &self,
    ) -> verlet_history::HistoryResult<Vec<verlet_runtime_contracts::ThreadCoordinates>> {
        let connection = self.connect().await?;
        let mut rows = connection
            .query(
                "SELECT DISTINCT tenant_id, user_id, session_id, thread_id
                 FROM event_records
                 WHERE stream_id LIKE 'control:%'
                 ORDER BY tenant_id, user_id, session_id, thread_id",
                (),
            )
            .await
            .map_err(verlet_history::storage_error)?;
        let mut coordinates = Vec::new();
        while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
            coordinates.push(verlet_runtime_contracts::ThreadCoordinates {
                tenant_id: row.get(0).map_err(verlet_history::storage_error)?,
                user_id: row.get(1).map_err(verlet_history::storage_error)?,
                session_id: row.get(2).map_err(verlet_history::storage_error)?,
                thread_id: verlet_history::parse_thread_id(
                    &row.get::<String>(3)
                        .map_err(verlet_history::storage_error)?,
                )?,
            });
        }
        Ok(coordinates)
    }

    /// The `(mandate_event_id, occurrence_index)` pairs already fired on
    /// `stream_id`.
    ///
    /// The clock route needs nothing but these pairs, and it asks on every
    /// poll. Reading the whole stream to find them meant fetching and
    /// deserializing every event record in it — `timer.fired` is a small
    /// fraction of a control stream, and the payload of everything else was
    /// parsed only to be discarded. Filtering on `kind` in SQL and selecting
    /// the one column that is needed keeps the work proportional to the fired
    /// timers instead of the whole stream.
    pub async fn list_fired_timer_occurrences(
        &self,
        stream_id: &verlet_history::EventStreamId,
    ) -> verlet_history::HistoryResult<Vec<(verlet_history::EventRecordId, u64)>> {
        let timer_fired_kind: &str = verlet_history::EventKind::TimerFired.as_ref();
        let connection = self.connect().await?;
        let mut rows = connection
            .query(
                "SELECT payload_json
                 FROM event_records
                 WHERE stream_id = ?1 AND kind = ?2
                 ORDER BY sequence",
                verlet_sqlite::params![stream_id.to_string(), timer_fired_kind],
            )
            .await
            .map_err(verlet_history::storage_error)?;
        let mut fired = Vec::new();
        while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
            let payload_json = row
                .get::<String>(0)
                .map_err(verlet_history::storage_error)?;
            let payload: verlet_history::TimerFiredPayload =
                serde_json::from_str(&payload_json).map_err(verlet_history::codec_error)?;
            fired.push((payload.mandate_event_id, payload.occurrence_index));
        }
        Ok(fired)
    }

    pub async fn list_thread_events(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let connection = self.connect().await?;
        let mut rows = connection
            .query(
                "SELECT event_id, schema, payload_schema, stream_id, sequence, thread_id,
                        tenant_id, user_id, session_id, created_at_ms, kind, origin,
                        provenance_json, payload_json
                 FROM event_records
                 WHERE thread_id = ?1
                 ORDER BY rowid",
                verlet_sqlite::params![thread_id.to_string()],
            )
            .await
            .map_err(verlet_history::storage_error)?;
        let mut events = Vec::new();
        while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
            events.push(sqlite_event_from_row(&row)?);
        }
        Ok(events)
    }

    pub async fn list_event_records(
        &self,
        thread_id: Option<verlet_runtime_contracts::ThreadId>,
        kind: Option<verlet_history::EventKind>,
        from_sequence: Option<verlet_history::EventSequence>,
        to_sequence: Option<verlet_history::EventSequence>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let connection = self.connect().await?;
        let thread_id = thread_id
            .map(|thread_id| thread_id.to_string())
            .unwrap_or_default();
        let kind = kind.map(|kind| kind.to_string()).unwrap_or_default();
        let from_sequence = from_sequence
            .map(verlet_history::EventSequence::get)
            .unwrap_or(i64::MIN);
        let to_sequence = to_sequence
            .map(verlet_history::EventSequence::get)
            .unwrap_or(i64::MAX);
        let mut rows = connection
            .query(
                "SELECT event_id, schema, payload_schema, stream_id, sequence, thread_id,
                        tenant_id, user_id, session_id, created_at_ms, kind, origin,
                        provenance_json, payload_json
                 FROM event_records
                 WHERE (?1 = '' OR thread_id = ?1)
                   AND (?2 = '' OR kind = ?2)
                   AND sequence >= ?3
                   AND sequence <= ?4
                 ORDER BY rowid",
                verlet_sqlite::params![thread_id, kind, from_sequence, to_sequence],
            )
            .await
            .map_err(verlet_history::storage_error)?;
        let mut events = Vec::new();
        while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
            events.push(sqlite_event_from_row(&row)?);
        }
        Ok(events)
    }
}

#[async_trait::async_trait]
impl verlet_history::SessionStore for SqliteSessionStore {
    async fn append(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        self.append_inner(coordinates, parent_entry_id, kind, None)
            .await
    }

    async fn append_with_provenance(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
        provenance: verlet_history::EventProvenance,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        self.append_inner(coordinates, parent_entry_id, kind, Some(provenance))
            .await
    }

    async fn append_turn_input(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: &str,
        kind: verlet_history::SessionEntryKind,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        let store = self.clone();
        let coordinates = coordinates.clone();
        let turn_id = turn_id.to_string();
        cancellation_safe(async move {
            let coordinates = &coordinates;
            let turn_id = turn_id.as_str();
            let _writer = store.write_guard().await;
            let mut connection = store.connect().await?;
            let tx = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(verlet_history::storage_error)?;
            let thread_id = coordinates.thread_id.to_string();
            let existing = sqlite_optional_string(
                &tx,
                "SELECT entry_json FROM session_entries WHERE thread_id = ?1 AND turn_id = ?2",
                verlet_sqlite::params![thread_id.clone(), turn_id],
            )
            .await?
            .map(|json| verlet_history::decode_entry(&json))
            .transpose()?;
            if let Some(existing) = existing {
                verlet_history::validate_entry_coordinates(coordinates, &existing)?;
                if !verlet_history::turn_input_kinds_match(&existing.kind, &kind) {
                    return Err(verlet_history::HistoryError::Storage(format!(
                        "turn {turn_id} input does not match its persisted session entry"
                    )));
                }
                tx.commit().await.map_err(verlet_history::storage_error)?;
                return Ok(existing);
            }
            let parent_entry_id = sqlite_active_leaf_entry(&tx, &thread_id)
                .await?
                .map(|entry| {
                    verlet_history::validate_entry_coordinates(coordinates, &entry)?;
                    Ok(entry.entry_id)
                })
                .transpose()?;
            let entry = verlet_history::SessionEntry::for_turn(
                coordinates.clone(),
                parent_entry_id,
                turn_id,
                kind,
            );
            sqlite_enforce_lease_epoch(
                &tx,
                &verlet_history::EventStreamId::for_thread(coordinates),
                store.lease_epoch,
            )
            .await?;
            sqlite_insert_entry(&tx, &entry).await?;
            tx.execute(
                "INSERT INTO active_leaves (thread_id, entry_id)
             VALUES (?1, ?2)
             ON CONFLICT(thread_id) DO UPDATE SET entry_id = excluded.entry_id",
                verlet_sqlite::params![thread_id, entry.entry_id.to_string()],
            )
            .await
            .map_err(verlet_history::storage_error)?;
            tx.commit().await.map_err(verlet_history::storage_error)?;
            Ok(entry)
        })
        .await
    }

    async fn active_leaf(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_history::HistoryResult<Option<verlet_history::SessionEntryId>> {
        let connection = self.connect().await?;
        let thread_id = coordinates.thread_id.to_string();
        sqlite_active_leaf_entry(&connection, &thread_id)
            .await?
            .map(|entry| {
                verlet_history::validate_entry_coordinates(coordinates, &entry)?;
                Ok(entry.entry_id)
            })
            .transpose()
    }

    async fn select_branch(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        leaf_entry_id: Option<verlet_history::SessionEntryId>,
    ) -> verlet_history::HistoryResult<()> {
        let store = self.clone();
        let coordinates = coordinates.clone();
        cancellation_safe(async move {
            let coordinates = &coordinates;
            let _writer = store.write_guard().await;
            let mut connection = store.connect().await?;
            let tx = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(verlet_history::storage_error)?;
            let thread_id = coordinates.thread_id.to_string();
            let prior_entry_id = sqlite_active_leaf_entry(&tx, &thread_id)
                .await?
                .map(|entry| {
                    verlet_history::validate_entry_coordinates(coordinates, &entry)?;
                    Ok(entry.entry_id)
                })
                .transpose()?;
            if let Some(leaf_entry_id) = leaf_entry_id {
                sqlite_branch_path(&tx, coordinates, leaf_entry_id).await?;
            }
            let payload = serde_json::to_value(verlet_history::ThreadBranchSelectedPayload {
                thread_id: coordinates.thread_id,
                selected_entry_id: leaf_entry_id,
                prior_entry_id,
            })
            .map_err(verlet_history::codec_error)?;
            let stream_id = verlet_history::EventStreamId::for_thread(coordinates);
            sqlite_enforce_lease_epoch(&tx, &stream_id, store.lease_epoch).await?;
            sqlite_insert_event(
                &tx,
                &stream_id,
                verlet_history::NewEventRecord::witnessed(
                    coordinates.clone(),
                    verlet_history::EventKind::ThreadBranchSelected,
                    payload,
                ),
            )
            .await?;
            match leaf_entry_id {
                Some(leaf_entry_id) => {
                    tx.execute(
                        "INSERT INTO active_leaves (thread_id, entry_id)
                     VALUES (?1, ?2)
                     ON CONFLICT(thread_id) DO UPDATE SET entry_id = excluded.entry_id",
                        verlet_sqlite::params![thread_id, leaf_entry_id.to_string()],
                    )
                    .await
                    .map_err(verlet_history::storage_error)?;
                }
                None => {
                    tx.execute(
                        "DELETE FROM active_leaves WHERE thread_id = ?1",
                        verlet_sqlite::params![thread_id],
                    )
                    .await
                    .map_err(verlet_history::storage_error)?;
                }
            }
            tx.commit().await.map_err(verlet_history::storage_error)?;
            Ok(())
        })
        .await
    }

    async fn build_context(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_history::HistoryResult<verlet_history::SessionContext> {
        let connection = self.connect().await?;
        sqlite_build_context(
            &connection,
            coordinates,
            None,
            false,
            &mut std::collections::HashSet::new(),
        )
        .await
    }

    async fn clone_branch(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        source_leaf: Option<verlet_history::SessionEntryId>,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_history::HistoryResult<Option<verlet_history::SessionEntryId>> {
        let store = self.clone();
        let source_coordinates = source_coordinates.clone();
        let target_coordinates = target_coordinates.clone();
        cancellation_safe(async move {
            let source_coordinates = &source_coordinates;
            let target_coordinates = &target_coordinates;
            let _writer = store.write_guard().await;
            let mut connection = store.connect().await?;
            let tx = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(verlet_history::storage_error)?;
            tx.execute(
                "DELETE FROM thread_bases WHERE child_thread_id = ?1",
                verlet_sqlite::params![target_coordinates.thread_id.to_string()],
            )
            .await
            .map_err(verlet_history::storage_error)?;
            let Some(source_leaf) = source_leaf else {
                tx.execute(
                    "DELETE FROM active_leaves WHERE thread_id = ?1",
                    verlet_sqlite::params![target_coordinates.thread_id.to_string()],
                )
                .await
                .map_err(verlet_history::storage_error)?;
                tx.commit().await.map_err(verlet_history::storage_error)?;
                return Ok(None);
            };
            let entries = sqlite_branch_path(&tx, source_coordinates, source_leaf).await?;
            sqlite_enforce_lease_epoch(
                &tx,
                &verlet_history::EventStreamId::for_thread(target_coordinates),
                store.lease_epoch,
            )
            .await?;
            let mut parent_entry_id = None;
            let mut latest_entry_id = None;
            for source_entry in entries {
                let entry = verlet_history::SessionEntry::new(
                    target_coordinates.clone(),
                    parent_entry_id,
                    source_entry.kind.clone(),
                );
                sqlite_insert_entry(&tx, &entry).await?;
                parent_entry_id = Some(entry.entry_id);
                latest_entry_id = Some(entry.entry_id);
            }
            if let Some(entry_id) = latest_entry_id {
                tx.execute(
                    "INSERT INTO active_leaves (thread_id, entry_id)
                 VALUES (?1, ?2)
                 ON CONFLICT(thread_id) DO UPDATE SET entry_id = excluded.entry_id",
                    verlet_sqlite::params![
                        target_coordinates.thread_id.to_string(),
                        entry_id.to_string()
                    ],
                )
                .await
                .map_err(verlet_history::storage_error)?;
            }
            tx.commit().await.map_err(verlet_history::storage_error)?;
            Ok(latest_entry_id)
        })
        .await
    }

    async fn fork_by_reference(
        &self,
        source_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        target_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        base: verlet_history::ThreadBaseRef,
    ) -> verlet_history::HistoryResult<()> {
        verlet_history::validate_thread_base_ref(source_coordinates, target_coordinates, &base)?;
        let store = self.clone();
        let source_coordinates = source_coordinates.clone();
        let target_coordinates = target_coordinates.clone();
        cancellation_safe(async move {
            let source_coordinates = &source_coordinates;
            let target_coordinates = &target_coordinates;
            let _writer = store.write_guard().await;
            let mut connection = store.connect().await?;
            let tx = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(verlet_history::storage_error)?;
            validate_sqlite_base_cycle(
                &tx,
                target_coordinates.thread_id,
                source_coordinates.thread_id,
            )
            .await?;
            if let Some(parent_leaf) = base.parent_leaf_entry_id {
                sqlite_build_context(
                    &tx,
                    source_coordinates,
                    Some(parent_leaf),
                    false,
                    &mut std::collections::HashSet::new(),
                )
                .await?;
            }
            tx.execute(
                "DELETE FROM active_leaves WHERE thread_id = ?1",
                verlet_sqlite::params![target_coordinates.thread_id.to_string()],
            )
            .await
            .map_err(verlet_history::storage_error)?;
            sqlite_insert_thread_base(&tx, &base).await?;
            tx.commit().await.map_err(verlet_history::storage_error)?;
            Ok(())
        })
        .await
    }
}

impl SqliteSessionStore {
    async fn append_inner(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_entry_id: Option<verlet_history::SessionEntryId>,
        kind: verlet_history::SessionEntryKind,
        provenance: Option<verlet_history::EventProvenance>,
    ) -> verlet_history::HistoryResult<verlet_history::SessionEntry> {
        let store = self.clone();
        let coordinates = coordinates.clone();
        cancellation_safe(async move {
            let coordinates = &coordinates;
            let _writer = store.write_guard().await;
            let mut connection = store.connect().await?;
            let tx = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(verlet_history::storage_error)?;
            let thread_id = coordinates.thread_id.to_string();
            let parent_entry_id = match parent_entry_id {
                Some(parent) => {
                    let parent_entry = sqlite_load_entry(&tx, &thread_id, parent)
                        .await?
                        .ok_or(verlet_history::HistoryError::EntryNotFound(parent))?;
                    verlet_history::validate_entry_coordinates(coordinates, &parent_entry)?;
                    Some(parent)
                }
                None => sqlite_active_leaf_entry(&tx, &thread_id)
                    .await?
                    .map(|entry| {
                        verlet_history::validate_entry_coordinates(coordinates, &entry)?;
                        Ok(entry.entry_id)
                    })
                    .transpose()?,
            };

            let entry =
                verlet_history::SessionEntry::new(coordinates.clone(), parent_entry_id, kind);
            sqlite_enforce_lease_epoch(
                &tx,
                &verlet_history::EventStreamId::for_thread(coordinates),
                store.lease_epoch,
            )
            .await?;
            sqlite_insert_entry_with_optional_provenance(&tx, &entry, provenance).await?;
            tx.execute(
                "INSERT INTO active_leaves (thread_id, entry_id)
             VALUES (?1, ?2)
             ON CONFLICT(thread_id) DO UPDATE SET entry_id = excluded.entry_id",
                verlet_sqlite::params![
                    entry.coordinates.thread_id.to_string(),
                    entry.entry_id.to_string()
                ],
            )
            .await
            .map_err(verlet_history::storage_error)?;
            tx.commit().await.map_err(verlet_history::storage_error)?;
            Ok(entry)
        })
        .await
    }
}

#[async_trait::async_trait]
impl verlet_history::EventStore for SqliteSessionStore {
    async fn append_events(
        &self,
        stream_id: &verlet_history::EventStreamId,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let store = self.clone();
        let stream_id = stream_id.clone();
        cancellation_safe(async move {
            let stream_id = &stream_id;
            let _writer = store.write_guard().await;
            let mut connection = store.connect().await?;
            let tx = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(verlet_history::storage_error)?;
            sqlite_enforce_lease_epoch(&tx, stream_id, store.lease_epoch).await?;
            let mut appended = Vec::with_capacity(records.len());
            for record in records {
                appended.push(sqlite_insert_event(&tx, stream_id, record).await?);
            }
            tx.commit().await.map_err(verlet_history::storage_error)?;
            Ok(appended)
        })
        .await
    }

    async fn append_events_fenced(
        &self,
        stream_id: &verlet_history::EventStreamId,
        expected_next_sequence: verlet_history::EventSequence,
        records: Vec<verlet_history::NewEventRecord>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let store = self.clone();
        let stream_id = stream_id.clone();
        cancellation_safe(async move {
            let stream_id = &stream_id;
            let _writer = store.write_guard().await;
            let mut connection = store.connect().await?;
            let tx = connection
                .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
                .await
                .map_err(verlet_history::storage_error)?;
            let appended = store
                .append_events_fenced_in_transaction(
                    &tx,
                    stream_id,
                    expected_next_sequence,
                    records,
                )
                .await?;
            tx.commit().await.map_err(verlet_history::storage_error)?;
            Ok(appended)
        })
        .await
    }

    async fn read_events(
        &self,
        stream_id: &verlet_history::EventStreamId,
        from_sequence: Option<verlet_history::EventSequence>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
        let connection = self.connect().await?;
        sqlite_read_events(&connection, stream_id, from_sequence).await
    }
}

#[async_trait::async_trait]
impl verlet_history::ObservationStore for SqliteSessionStore {
    async fn append_observation(
        &self,
        record: verlet_history::NewObservationRecord,
    ) -> verlet_history::HistoryResult<verlet_history::ObservationRecord> {
        let store = self.clone();
        cancellation_safe(async move {
            let _writer = store.write_guard().await;
            let connection = store.connect().await?;
            sqlite_insert_observation(&connection, record).await
        })
        .await
    }

    async fn list_observations(
        &self,
        scope: &verlet_runtime_contracts::ThreadCoordinates,
        kind: Option<&str>,
    ) -> verlet_history::HistoryResult<Vec<verlet_history::ObservationRecord>> {
        let connection = self.connect().await?;
        sqlite_list_observations(&connection, scope, kind).await
    }
}

async fn init_sqlite_schema(
    connection: &mut verlet_sqlite::Connection,
) -> verlet_history::HistoryResult<()> {
    connection
        .execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS session_entries (
                entry_id TEXT PRIMARY KEY NOT NULL,
                parent_entry_id TEXT REFERENCES session_entries(entry_id),
                turn_id TEXT,
                thread_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                entry_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_session_entries_scope
                ON session_entries(tenant_id, user_id, session_id, thread_id, created_at_ms);

            CREATE INDEX IF NOT EXISTS idx_session_entries_parent
                ON session_entries(thread_id, parent_entry_id);

            CREATE TABLE IF NOT EXISTS active_leaves (
                thread_id TEXT PRIMARY KEY NOT NULL,
                entry_id TEXT NOT NULL REFERENCES session_entries(entry_id)
            );

            CREATE TABLE IF NOT EXISTS thread_bases (
                child_thread_id TEXT PRIMARY KEY NOT NULL,
                parent_thread_id TEXT NOT NULL,
                parent_checkpoint_id TEXT,
                parent_leaf_entry_id TEXT REFERENCES session_entries(entry_id),
                parent_stream_id TEXT NOT NULL,
                parent_stream_to_sequence INTEGER,
                parent_binding_snapshot_id TEXT,
                reason TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_thread_bases_parent
                ON thread_bases(parent_thread_id);

            CREATE TABLE IF NOT EXISTS event_records (
                event_id TEXT PRIMARY KEY NOT NULL,
                schema TEXT NOT NULL DEFAULT 'cooldis.stream.record/1',
                payload_schema TEXT NOT NULL DEFAULT '',
                stream_id TEXT NOT NULL,
                sequence INTEGER NOT NULL,
                thread_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                kind TEXT NOT NULL,
                origin TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                UNIQUE(stream_id, sequence)
            );

            CREATE INDEX IF NOT EXISTS idx_event_records_stream
                ON event_records(stream_id, sequence);

            CREATE INDEX IF NOT EXISTS idx_event_records_scope
                ON event_records(tenant_id, user_id, session_id, thread_id, created_at_ms);

            CREATE TABLE IF NOT EXISTS observation_records (
                observation_id TEXT PRIMARY KEY NOT NULL,
                kind TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                payload_json TEXT NOT NULL,
                provenance_json TEXT NOT NULL,
                supersedes_observation_id TEXT,
                confidence REAL
            );

            CREATE INDEX IF NOT EXISTS idx_observation_records_scope
                ON observation_records(tenant_id, user_id, session_id, thread_id, kind, created_at_ms);

            -- Placement-lease write fence (EMO-533): the highest lease epoch
            -- committed by a successful append transaction per stream. Rows
            -- exist only once a non-zero epoch is presented; single-instance
            -- stores never write here.
            CREATE TABLE IF NOT EXISTS journal_lease_epochs (
                stream_id TEXT PRIMARY KEY NOT NULL,
                minimum_epoch INTEGER NOT NULL
            );
            "#,
        )
        .await
        .map_err(verlet_history::storage_error)?;
    let migration = connection
        .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
        .await
        .map_err(verlet_history::storage_error)?;
    verlet_sqlite::ensure_column(&migration, "session_entries", "turn_id", "turn_id TEXT")
        .await
        .map_err(verlet_history::storage_error)?;
    migration
        .execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_session_entries_turn
             ON session_entries(thread_id, turn_id)
             WHERE turn_id IS NOT NULL",
            (),
        )
        .await
        .map_err(verlet_history::storage_error)?;
    sqlite_migrate_event_records_schema(&migration).await?;
    migration
        .commit()
        .await
        .map_err(verlet_history::storage_error)?;
    sqlite_rebuild_active_leaves_from_events(connection).await
}

async fn sqlite_rebuild_active_leaves_from_events(
    connection: &mut verlet_sqlite::Connection,
) -> verlet_history::HistoryResult<()> {
    let tx = connection
        .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
        .await
        .map_err(verlet_history::storage_error)?;
    let mut selected_threads = std::collections::HashSet::new();
    {
        let mut rows = tx
            .query(
                "SELECT DISTINCT thread_id
                 FROM event_records
                 WHERE kind = ?1",
                verlet_sqlite::params![verlet_history::EventKind::ThreadBranchSelected.as_ref()],
            )
            .await
            .map_err(verlet_history::storage_error)?;
        while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
            selected_threads.insert(
                row.get::<String>(0)
                    .map_err(verlet_history::storage_error)?,
            );
        }
    }
    if selected_threads.is_empty() {
        return tx.commit().await.map_err(verlet_history::storage_error);
    }
    for thread_id in &selected_threads {
        tx.execute(
            "DELETE FROM active_leaves WHERE thread_id = ?1",
            verlet_sqlite::params![thread_id.as_str()],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    }

    let mut journal_entries = Vec::new();
    {
        let mut rows = tx
            .query(
                "SELECT kind, thread_id, payload_json
                 FROM event_records
                 WHERE kind IN (?1, ?2)
                 ORDER BY rowid",
                verlet_sqlite::params![
                    verlet_history::EventKind::SessionEntryAppended.as_ref(),
                    verlet_history::EventKind::ThreadBranchSelected.as_ref(),
                ],
            )
            .await
            .map_err(verlet_history::storage_error)?;
        while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
            journal_entries.push((
                row.get::<String>(0)
                    .map_err(verlet_history::storage_error)?,
                row.get::<String>(1)
                    .map_err(verlet_history::storage_error)?,
                row.get::<String>(2)
                    .map_err(verlet_history::storage_error)?,
            ));
        }
    }

    let branch_selected_kind: &str = verlet_history::EventKind::ThreadBranchSelected.as_ref();
    for (kind, stored_thread_id, payload_json) in journal_entries {
        if !selected_threads.contains(&stored_thread_id) {
            continue;
        }
        let selected_entry_id = if kind == branch_selected_kind {
            let payload: verlet_history::ThreadBranchSelectedPayload =
                serde_json::from_str(&payload_json).map_err(verlet_history::codec_error)?;
            if payload.thread_id.to_string() != stored_thread_id {
                return Err(verlet_history::HistoryError::Codec(format!(
                    "thread.branch.selected payload thread {} does not match event thread {stored_thread_id}",
                    payload.thread_id
                )));
            }
            payload.selected_entry_id
        } else {
            let payload: serde_json::Value =
                serde_json::from_str(&payload_json).map_err(verlet_history::codec_error)?;
            serde_json::from_value(payload["entry_id"].clone())
                .map_err(verlet_history::codec_error)?
        };
        match selected_entry_id {
            Some(entry_id) => {
                if sqlite_load_entry(&tx, &stored_thread_id, entry_id)
                    .await?
                    .is_none()
                {
                    return Err(verlet_history::HistoryError::EntryNotFound(entry_id));
                }
                tx.execute(
                    "INSERT INTO active_leaves (thread_id, entry_id)
                         VALUES (?1, ?2)
                         ON CONFLICT(thread_id) DO UPDATE SET entry_id = excluded.entry_id",
                    verlet_sqlite::params![stored_thread_id, entry_id.to_string()],
                )
                .await
                .map_err(verlet_history::storage_error)?;
            }
            None => {
                tx.execute(
                    "DELETE FROM active_leaves WHERE thread_id = ?1",
                    verlet_sqlite::params![stored_thread_id],
                )
                .await
                .map_err(verlet_history::storage_error)?;
            }
        }
    }
    tx.commit().await.map_err(verlet_history::storage_error)
}

/// Migrates legacy event rows honestly: reconstructed provenance names this
/// migration and does not impersonate the runtime component that may have
/// produced the original unversioned row.
async fn sqlite_migrate_event_records_schema(
    connection: &verlet_sqlite::Connection,
) -> verlet_history::HistoryResult<()> {
    let mut added_identity_column = false;
    let mut added_origin_column = false;
    let had_schema = sqlite_table_has_column(connection, "event_records", "schema").await?;
    verlet_sqlite::ensure_column(
        connection,
        "event_records",
        "schema",
        "schema TEXT NOT NULL DEFAULT 'cooldis.stream.record/1'",
    )
    .await
    .map_err(verlet_history::storage_error)?;
    added_identity_column |= !had_schema;
    let had_payload_schema =
        sqlite_table_has_column(connection, "event_records", "payload_schema").await?;
    verlet_sqlite::ensure_column(
        connection,
        "event_records",
        "payload_schema",
        "payload_schema TEXT NOT NULL DEFAULT ''",
    )
    .await
    .map_err(verlet_history::storage_error)?;
    added_identity_column |= !had_payload_schema;
    let had_origin = sqlite_table_has_column(connection, "event_records", "origin").await?;
    verlet_sqlite::ensure_column(
        connection,
        "event_records",
        "origin",
        "origin TEXT NOT NULL DEFAULT 'witnessed'",
    )
    .await
    .map_err(verlet_history::storage_error)?;
    added_origin_column |= !had_origin;
    let had_provenance =
        sqlite_table_has_column(connection, "event_records", "provenance_json").await?;
    verlet_sqlite::ensure_column(
        connection,
        "event_records",
        "provenance_json",
        "provenance_json TEXT NOT NULL DEFAULT '{}'",
    )
    .await
    .map_err(verlet_history::storage_error)?;
    added_origin_column |= !had_provenance;
    if added_identity_column {
        let mut identity_rows = Vec::new();
        {
            let mut rows = connection
                .query(
                    "SELECT event_id, kind FROM event_records ORDER BY stream_id, sequence",
                    (),
                )
                .await
                .map_err(verlet_history::storage_error)?;
            while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
                identity_rows.push((
                    row.get::<String>(0)
                        .map_err(verlet_history::storage_error)?,
                    row.get::<String>(1)
                        .map_err(verlet_history::storage_error)?,
                ));
            }
        }
        for (event_id, kind) in identity_rows {
            let payload_schema = kind
                .parse::<verlet_history::EventKind>()
                .map(|kind| kind.payload_schema_id())
                .unwrap_or_default();
            connection
                .execute(
                    "UPDATE event_records
                     SET schema = ?2,
                         payload_schema = ?3
                     WHERE event_id = ?1",
                    verlet_sqlite::params![
                        event_id,
                        verlet_history::STREAM_RECORD_SCHEMA_V1,
                        payload_schema
                    ],
                )
                .await
                .map_err(verlet_history::storage_error)?;
        }
    }
    if !added_origin_column {
        return Ok(());
    }

    const ORIGIN_BACKFILL_MIGRATION: &str = "migration:origin-backfill@v1";

    let mut rows_to_backfill = Vec::new();
    {
        let mut rows = connection
            .query(
                "SELECT event_id, stream_id, kind, payload_json
                 FROM event_records
                 WHERE kind IN (?1, ?2)
                 ORDER BY stream_id, sequence",
                verlet_sqlite::params![
                    verlet_history::EventKind::SessionEntryAppended.as_ref(),
                    verlet_history::EventKind::ContextCompileCompleted.as_ref(),
                ],
            )
            .await
            .map_err(verlet_history::storage_error)?;
        while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
            rows_to_backfill.push((
                row.get::<String>(0)
                    .map_err(verlet_history::storage_error)?,
                row.get::<String>(1)
                    .map_err(verlet_history::storage_error)?,
                row.get::<String>(2)
                    .map_err(verlet_history::storage_error)?,
                row.get::<String>(3)
                    .map_err(verlet_history::storage_error)?,
            ));
        }
    }

    let session_entry_appended_kind: &str =
        verlet_history::EventKind::SessionEntryAppended.as_ref();
    for (event_id, _stream_id, kind, payload_json) in rows_to_backfill {
        let (origin, provenance) = if kind == session_entry_appended_kind {
            match serde_json::from_str::<verlet_history::SessionEntry>(&payload_json) {
                Ok(entry) if verlet_history::session_entry_is_user_authored(&entry.kind) => (
                    verlet_history::EventOrigin::Witnessed,
                    verlet_history::EventProvenance::default(),
                ),
                _ => (
                    verlet_history::EventOrigin::Discharged,
                    verlet_history::EventProvenance {
                        discharged_by: Some(ORIGIN_BACKFILL_MIGRATION.to_string()),
                        ..verlet_history::EventProvenance::default()
                    },
                ),
            }
        } else {
            (
                verlet_history::EventOrigin::Discharged,
                verlet_history::EventProvenance {
                    discharged_by: Some(ORIGIN_BACKFILL_MIGRATION.to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )
        };
        let provenance_json =
            serde_json::to_string(&provenance).map_err(verlet_history::codec_error)?;
        connection
            .execute(
                "UPDATE event_records
                 SET origin = ?1, provenance_json = ?2
                 WHERE event_id = ?3",
                verlet_sqlite::params![origin.as_ref(), provenance_json, event_id],
            )
            .await
            .map_err(verlet_history::storage_error)?;
    }
    Ok(())
}

async fn sqlite_table_has_column(
    connection: &verlet_sqlite::Connection,
    table: &str,
    column: &str,
) -> verlet_history::HistoryResult<bool> {
    let mut rows = connection
        .query(format!("PRAGMA table_info({table})"), ())
        .await
        .map_err(verlet_history::storage_error)?;
    while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
        let name: String = row.get(1).map_err(verlet_history::storage_error)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn sqlite_optional_string(
    connection: &verlet_sqlite::Connection,
    sql: &str,
    params: impl verlet_sqlite::IntoParams,
) -> verlet_history::HistoryResult<Option<String>> {
    let mut rows = connection
        .query(sql, params)
        .await
        .map_err(verlet_history::storage_error)?;
    rows.next()
        .await
        .map_err(verlet_history::storage_error)?
        .map(|row| row.get::<String>(0).map_err(verlet_history::storage_error))
        .transpose()
}

async fn sqlite_load_entry(
    connection: &verlet_sqlite::Connection,
    thread_id: &str,
    entry_id: verlet_history::SessionEntryId,
) -> verlet_history::HistoryResult<Option<verlet_history::SessionEntry>> {
    let entry_json = sqlite_optional_string(
        connection,
        "SELECT entry_json FROM session_entries WHERE thread_id = ?1 AND entry_id = ?2",
        verlet_sqlite::params![thread_id, entry_id.to_string()],
    )
    .await?;
    entry_json
        .map(|json| verlet_history::decode_entry(&json))
        .transpose()
}

async fn sqlite_active_leaf_entry(
    connection: &verlet_sqlite::Connection,
    thread_id: &str,
) -> verlet_history::HistoryResult<Option<verlet_history::SessionEntry>> {
    let entry_json = sqlite_optional_string(
        connection,
        "SELECT e.entry_json
             FROM active_leaves a
             JOIN session_entries e ON e.thread_id = a.thread_id AND e.entry_id = a.entry_id
             WHERE a.thread_id = ?1",
        verlet_sqlite::params![thread_id],
    )
    .await?;
    entry_json
        .map(|json| verlet_history::decode_entry(&json))
        .transpose()
}

async fn sqlite_insert_entry(
    connection: &verlet_sqlite::Connection,
    entry: &verlet_history::SessionEntry,
) -> verlet_history::HistoryResult<()> {
    sqlite_insert_entry_with_optional_provenance(connection, entry, None).await
}

async fn sqlite_insert_entry_with_optional_provenance(
    connection: &verlet_sqlite::Connection,
    entry: &verlet_history::SessionEntry,
    provenance: Option<verlet_history::EventProvenance>,
) -> verlet_history::HistoryResult<()> {
    let entry_json = serde_json::to_string(entry).map_err(verlet_history::codec_error)?;
    connection
        .execute(
            "INSERT INTO session_entries (
            entry_id,
            parent_entry_id,
            turn_id,
            thread_id,
            tenant_id,
            user_id,
            session_id,
            created_at_ms,
            entry_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            verlet_sqlite::params![
                entry.entry_id.to_string(),
                entry.parent_entry_id.map(|id| id.to_string()),
                entry.turn_id.as_deref(),
                entry.coordinates.thread_id.to_string(),
                entry.coordinates.tenant_id.as_str(),
                entry.coordinates.user_id.as_str(),
                entry.coordinates.session_id.as_str(),
                entry.created_at_ms,
                entry_json,
            ],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    sqlite_insert_event(
        connection,
        &verlet_history::EventStreamId::for_thread(&entry.coordinates),
        match provenance {
            Some(provenance) => {
                verlet_history::session_entry_event_with_provenance(entry, provenance)
            }
            None => verlet_history::session_entry_event(entry),
        },
    )
    .await?;
    Ok(())
}

/// Enforce the placement-lease write fence for one append transaction
/// (EMO-533). Call once per write transaction, before the first
/// [`sqlite_insert_event`] for the stream — the check and any raise must
/// commit or roll back with the appends they guard.
///
/// Contract: `presented_epoch` below the recorded `minimum_epoch` for the
/// stream returns [`verlet_history::HistoryError::StaleLeaseEpoch`] and the
/// caller appends nothing. Presenting a higher epoch raises the row in the
/// same transaction. Presenting 0 against an absent row is the
/// single-instance fast path: no row is written, the table stays empty.
async fn sqlite_enforce_lease_epoch(
    connection: &verlet_sqlite::Connection,
    stream_id: &verlet_history::EventStreamId,
    presented_epoch: u64,
) -> verlet_history::HistoryResult<()> {
    let mut rows = connection
        .query(
            "SELECT minimum_epoch FROM journal_lease_epochs WHERE stream_id = ?1",
            verlet_sqlite::params![stream_id.as_str()],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    let minimum_epoch = rows
        .next()
        .await
        .map_err(verlet_history::storage_error)?
        .map(|row| row.get::<i64>(0).map_err(verlet_history::storage_error))
        .transpose()?
        .map(|minimum_epoch| {
            u64::try_from(minimum_epoch).map_err(|error| {
                verlet_history::HistoryError::Storage(format!(
                    "journal lease epoch for {stream_id} is invalid: {error}"
                ))
            })
        })
        .transpose()?;
    drop(rows);

    if let Some(minimum_epoch) = minimum_epoch {
        if presented_epoch < minimum_epoch {
            return Err(verlet_history::HistoryError::StaleLeaseEpoch {
                stream_id: stream_id.clone(),
                presented_epoch,
                minimum_epoch,
            });
        }
        if presented_epoch == minimum_epoch {
            return Ok(());
        }
    } else if presented_epoch == 0 {
        return Ok(());
    }

    let presented_epoch = i64::try_from(presented_epoch).map_err(|error| {
        verlet_history::HistoryError::Storage(format!(
            "journal lease epoch for {stream_id} exceeds SQLite INTEGER range: {error}"
        ))
    })?;
    connection
        .execute(
            "INSERT INTO journal_lease_epochs (stream_id, minimum_epoch)
             VALUES (?1, ?2)
             ON CONFLICT(stream_id) DO UPDATE SET minimum_epoch = excluded.minimum_epoch",
            verlet_sqlite::params![stream_id.as_str(), presented_epoch],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    Ok(())
}

async fn sqlite_insert_event(
    connection: &verlet_sqlite::Connection,
    stream_id: &verlet_history::EventStreamId,
    record: verlet_history::NewEventRecord,
) -> verlet_history::HistoryResult<verlet_history::EventRecord> {
    verlet_history::validate_new_event(&record)?;
    let event_id = record.id;
    let mut rows = connection
        .query(
            "SELECT EXISTS(SELECT 1 FROM event_records WHERE event_id = ?1)",
            verlet_sqlite::params![event_id.to_string()],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    let event_id_exists = rows
        .next()
        .await
        .map_err(verlet_history::storage_error)?
        .ok_or_else(|| {
            verlet_history::HistoryError::Storage("SELECT EXISTS returned no row".to_string())
        })?
        .get::<i64>(0)
        .map_err(verlet_history::storage_error)?
        != 0;
    drop(rows);
    if event_id_exists {
        return Err(verlet_history::HistoryError::DuplicateEventId(event_id));
    }
    let next_sequence = sqlite_next_event_sequence(connection, stream_id).await?;
    let event = verlet_history::EventRecord::from_new(
        stream_id.clone(),
        verlet_history::EventSequence::new(next_sequence),
        record,
    );
    event.validate_stream_record_v1()?;
    let payload_json =
        serde_json::to_string(&event.payload).map_err(verlet_history::codec_error)?;
    let provenance_json =
        serde_json::to_string(&event.provenance).map_err(verlet_history::codec_error)?;
    connection
        .execute(
            "INSERT INTO event_records (
            event_id,
            schema,
            payload_schema,
            stream_id,
            sequence,
            thread_id,
            tenant_id,
            user_id,
            session_id,
            created_at_ms,
            kind,
            origin,
            provenance_json,
            payload_json
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
            verlet_sqlite::params![
                event.id.to_string(),
                verlet_history::STREAM_RECORD_SCHEMA_V1,
                event.kind.payload_schema_id(),
                event.stream_id.as_str(),
                event.sequence.get(),
                event.coordinates.thread_id.to_string(),
                event.coordinates.tenant_id.as_str(),
                event.coordinates.user_id.as_str(),
                event.coordinates.session_id.as_str(),
                event.created_at_ms,
                event.kind.as_ref(),
                event.origin.as_ref(),
                provenance_json,
                payload_json,
            ],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    Ok(event)
}

async fn sqlite_next_event_sequence(
    connection: &verlet_sqlite::Connection,
    stream_id: &verlet_history::EventStreamId,
) -> verlet_history::HistoryResult<i64> {
    let mut rows = connection
        .query(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM event_records WHERE stream_id = ?1",
            verlet_sqlite::params![stream_id.as_str()],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    rows.next()
        .await
        .map_err(verlet_history::storage_error)?
        .ok_or_else(|| {
            verlet_history::HistoryError::Storage(
                "next event sequence aggregate returned no row".to_string(),
            )
        })?
        .get::<i64>(0)
        .map_err(verlet_history::storage_error)
}

async fn sqlite_read_events(
    connection: &verlet_sqlite::Connection,
    stream_id: &verlet_history::EventStreamId,
    from_sequence: Option<verlet_history::EventSequence>,
) -> verlet_history::HistoryResult<Vec<verlet_history::EventRecord>> {
    let mut events = Vec::new();
    match from_sequence {
        Some(sequence) => {
            let mut rows = connection
                .query(
                    "SELECT event_id, schema, payload_schema, stream_id, sequence, thread_id,
                            tenant_id, user_id, session_id, created_at_ms, kind, origin,
                            provenance_json, payload_json
                     FROM event_records
                     WHERE stream_id = ?1 AND sequence >= ?2
                     ORDER BY sequence",
                    verlet_sqlite::params![stream_id.as_str(), sequence.get()],
                )
                .await
                .map_err(verlet_history::storage_error)?;
            while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
                events.push(sqlite_event_from_row(&row)?);
            }
        }
        None => {
            let mut rows = connection
                .query(
                    "SELECT event_id, schema, payload_schema, stream_id, sequence, thread_id,
                            tenant_id, user_id, session_id, created_at_ms, kind, origin,
                            provenance_json, payload_json
                     FROM event_records
                     WHERE stream_id = ?1
                     ORDER BY sequence",
                    verlet_sqlite::params![stream_id.as_str()],
                )
                .await
                .map_err(verlet_history::storage_error)?;
            while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
                events.push(sqlite_event_from_row(&row)?);
            }
        }
    }
    Ok(events)
}

fn sqlite_event_from_row(
    row: &verlet_sqlite::Row,
) -> verlet_history::HistoryResult<verlet_history::EventRecord> {
    let event_id: String = row.get(0).map_err(verlet_history::storage_error)?;
    let schema: String = row.get(1).map_err(verlet_history::storage_error)?;
    if schema != verlet_history::STREAM_RECORD_SCHEMA_V1 {
        return Err(verlet_history::codec_error(format!(
            "event record {event_id} has unsupported stream schema {schema:?}"
        )));
    }
    let payload_schema: String = row.get(2).map_err(verlet_history::storage_error)?;
    let kind: String = row.get(10).map_err(verlet_history::storage_error)?;
    let kind = verlet_history::EventKind::try_from(kind)?;
    let expected_payload_schema = kind.payload_schema_id();
    if payload_schema != expected_payload_schema {
        return Err(verlet_history::codec_error(format!(
            "event record {event_id} kind {kind} has payload_schema {payload_schema:?}, expected {expected_payload_schema:?}"
        )));
    }
    let origin_name: String = row.get(11).map_err(verlet_history::storage_error)?;
    let origin: Result<verlet_history::EventOrigin, _> = origin_name.parse();
    let origin = origin.map_err(|_| {
        verlet_history::HistoryError::Codec(format!("unknown event origin: {origin_name}"))
    })?;
    let provenance_json: String = row.get(12).map_err(verlet_history::storage_error)?;
    let payload_json: String = row.get(13).map_err(verlet_history::storage_error)?;
    let event = verlet_history::EventRecord {
        id: verlet_history::EventRecordId::from_uuid(verlet_history::parse_uuid(&event_id)?),
        stream_id: verlet_history::EventStreamId::new(
            row.get::<String>(3)
                .map_err(verlet_history::storage_error)?,
        ),
        sequence: verlet_history::EventSequence::new(
            row.get(4).map_err(verlet_history::storage_error)?,
        ),
        coordinates: verlet_runtime_contracts::ThreadCoordinates {
            thread_id: verlet_history::parse_thread_id(
                &row.get::<String>(5)
                    .map_err(verlet_history::storage_error)?,
            )?,
            tenant_id: row.get(6).map_err(verlet_history::storage_error)?,
            user_id: row.get(7).map_err(verlet_history::storage_error)?,
            session_id: row.get(8).map_err(verlet_history::storage_error)?,
        },
        created_at_ms: row.get(9).map_err(verlet_history::storage_error)?,
        kind,
        origin,
        provenance: serde_json::from_str(&provenance_json).map_err(verlet_history::codec_error)?,
        payload: serde_json::from_str(&payload_json).map_err(verlet_history::codec_error)?,
    };
    event.validate_stream_record_v1()?;
    Ok(event)
}

async fn sqlite_insert_observation(
    connection: &verlet_sqlite::Connection,
    record: verlet_history::NewObservationRecord,
) -> verlet_history::HistoryResult<verlet_history::ObservationRecord> {
    let record = verlet_history::ObservationRecord::from(record);
    let payload_json =
        serde_json::to_string(&record.payload).map_err(verlet_history::codec_error)?;
    let provenance_json =
        serde_json::to_string(&record.provenance).map_err(verlet_history::codec_error)?;
    connection
        .execute(
            "INSERT INTO observation_records (
                observation_id,
                kind,
                thread_id,
                tenant_id,
                user_id,
                session_id,
                created_at_ms,
                payload_json,
                provenance_json,
                supersedes_observation_id,
                confidence
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
            verlet_sqlite::params![
                record.id.to_string(),
                record.kind.as_str(),
                record.scope.thread_id.to_string(),
                record.scope.tenant_id.as_str(),
                record.scope.user_id.as_str(),
                record.scope.session_id.as_str(),
                record.created_at_ms,
                payload_json,
                provenance_json,
                record.supersedes.map(|id| id.to_string()),
                record.confidence,
            ],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    Ok(record)
}

async fn sqlite_list_observations(
    connection: &verlet_sqlite::Connection,
    scope: &verlet_runtime_contracts::ThreadCoordinates,
    kind: Option<&str>,
) -> verlet_history::HistoryResult<Vec<verlet_history::ObservationRecord>> {
    let mut observations = Vec::new();
    match kind {
        Some(kind) => {
            let mut rows = connection
                .query(
                    "SELECT observation_id, kind, thread_id, tenant_id, user_id, session_id,
                            created_at_ms, payload_json, provenance_json,
                            supersedes_observation_id, confidence
                     FROM observation_records
                     WHERE tenant_id = ?1 AND user_id = ?2 AND session_id = ?3
                       AND thread_id = ?4 AND kind = ?5
                     ORDER BY created_at_ms, observation_id",
                    verlet_sqlite::params![
                        scope.tenant_id.as_str(),
                        scope.user_id.as_str(),
                        scope.session_id.as_str(),
                        scope.thread_id.to_string(),
                        kind,
                    ],
                )
                .await
                .map_err(verlet_history::storage_error)?;
            while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
                observations.push(sqlite_observation_from_row(&row)?);
            }
        }
        None => {
            let mut rows = connection
                .query(
                    "SELECT observation_id, kind, thread_id, tenant_id, user_id, session_id,
                            created_at_ms, payload_json, provenance_json,
                            supersedes_observation_id, confidence
                     FROM observation_records
                     WHERE tenant_id = ?1 AND user_id = ?2 AND session_id = ?3
                       AND thread_id = ?4
                     ORDER BY created_at_ms, observation_id",
                    verlet_sqlite::params![
                        scope.tenant_id.as_str(),
                        scope.user_id.as_str(),
                        scope.session_id.as_str(),
                        scope.thread_id.to_string(),
                    ],
                )
                .await
                .map_err(verlet_history::storage_error)?;
            while let Some(row) = rows.next().await.map_err(verlet_history::storage_error)? {
                observations.push(sqlite_observation_from_row(&row)?);
            }
        }
    }
    Ok(observations)
}

fn sqlite_observation_from_row(
    row: &verlet_sqlite::Row,
) -> verlet_history::HistoryResult<verlet_history::ObservationRecord> {
    let observation_id: String = row.get(0).map_err(verlet_history::storage_error)?;
    let payload_json: String = row.get(7).map_err(verlet_history::storage_error)?;
    let provenance_json: String = row.get(8).map_err(verlet_history::storage_error)?;
    let supersedes: Option<String> = row.get(9).map_err(verlet_history::storage_error)?;
    Ok(verlet_history::ObservationRecord {
        id: verlet_history::ObservationId::from_uuid(verlet_history::parse_uuid(&observation_id)?),
        kind: row.get(1).map_err(verlet_history::storage_error)?,
        scope: verlet_runtime_contracts::ThreadCoordinates {
            thread_id: verlet_history::parse_thread_id(
                &row.get::<String>(2)
                    .map_err(verlet_history::storage_error)?,
            )?,
            tenant_id: row.get(3).map_err(verlet_history::storage_error)?,
            user_id: row.get(4).map_err(verlet_history::storage_error)?,
            session_id: row.get(5).map_err(verlet_history::storage_error)?,
        },
        created_at_ms: row.get(6).map_err(verlet_history::storage_error)?,
        payload: serde_json::from_str(&payload_json).map_err(verlet_history::codec_error)?,
        provenance: serde_json::from_str(&provenance_json).map_err(verlet_history::codec_error)?,
        supersedes: supersedes
            .map(|id| verlet_history::parse_uuid(&id).map(verlet_history::ObservationId::from_uuid))
            .transpose()?,
        confidence: row.get(10).map_err(verlet_history::storage_error)?,
    })
}

async fn sqlite_branch_path(
    connection: &verlet_sqlite::Connection,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    leaf_entry_id: verlet_history::SessionEntryId,
) -> verlet_history::HistoryResult<Vec<verlet_history::SessionEntry>> {
    let thread_id = coordinates.thread_id.to_string();
    let mut path = Vec::new();
    let mut cursor = Some(leaf_entry_id);
    while let Some(entry_id) = cursor {
        let entry = sqlite_load_entry(connection, &thread_id, entry_id)
            .await?
            .ok_or(verlet_history::HistoryError::EntryNotFound(entry_id))?;
        verlet_history::validate_entry_coordinates(coordinates, &entry)?;
        cursor = entry.parent_entry_id;
        path.push(entry);
    }
    path.reverse();
    Ok(path)
}

async fn sqlite_build_context(
    connection: &verlet_sqlite::Connection,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    local_leaf_override: Option<verlet_history::SessionEntryId>,
    inherited: bool,
    visiting: &mut std::collections::HashSet<verlet_runtime_contracts::ThreadId>,
) -> verlet_history::HistoryResult<verlet_history::SessionContext> {
    if !visiting.insert(coordinates.thread_id) {
        return Err(verlet_history::HistoryError::ThreadBaseCycle {
            child_thread_id: coordinates.thread_id,
            ancestor_thread_id: coordinates.thread_id,
        });
    }

    let mut entries = Vec::new();
    let mut source_cuts = Vec::new();
    if let Some(base) = sqlite_load_thread_base(connection, coordinates.thread_id).await? {
        if base.child_thread_id != coordinates.thread_id {
            return Err(verlet_history::HistoryError::Storage(format!(
                "thread base child id {} does not match requested thread {}",
                base.child_thread_id, coordinates.thread_id
            )));
        }
        let parent_coordinates =
            verlet_history::coordinates_with_thread_id(coordinates, base.parent_thread_id);
        let parent_context = Box::pin(sqlite_build_context(
            connection,
            &parent_coordinates,
            base.parent_leaf_entry_id,
            true,
            visiting,
        ))
        .await?;
        entries.extend(parent_context.entries);
        source_cuts.extend(parent_context.source_cuts);
    }

    let thread_id = coordinates.thread_id.to_string();
    let local_leaf = match local_leaf_override {
        Some(leaf) => Some(leaf),
        None => sqlite_active_leaf_entry(connection, &thread_id)
            .await?
            .map(|entry| entry.entry_id),
    };
    if let Some(local_leaf) = local_leaf {
        let local_path = sqlite_branch_path(connection, coordinates, local_leaf).await?;
        if !local_path.is_empty() {
            source_cuts.push(verlet_history::SessionContextSourceCut {
                coordinates: coordinates.clone(),
                stream_id: verlet_history::EventStreamId::for_thread(coordinates),
                inherited,
                entry_ids: local_path.iter().map(|entry| entry.entry_id).collect(),
            });
        }
        entries.extend(local_path);
    }

    visiting.remove(&coordinates.thread_id);
    verlet_history::strip_thread_start_identity_entries(&mut entries, &mut source_cuts);
    let mut messages = Vec::new();
    verlet_history::append_model_visible_messages(&entries, &mut messages);
    Ok(verlet_history::SessionContext {
        entries,
        messages,
        source_cuts,
    })
}

async fn sqlite_insert_thread_base(
    connection: &verlet_sqlite::Connection,
    base: &verlet_history::ThreadBaseRef,
) -> verlet_history::HistoryResult<()> {
    connection
        .execute(
            "INSERT INTO thread_bases (
            child_thread_id,
            parent_thread_id,
            parent_checkpoint_id,
            parent_leaf_entry_id,
            parent_stream_id,
            parent_stream_to_sequence,
            parent_binding_snapshot_id,
            reason,
            created_at_ms
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
        ON CONFLICT(child_thread_id) DO UPDATE SET
            parent_thread_id = excluded.parent_thread_id,
            parent_checkpoint_id = excluded.parent_checkpoint_id,
            parent_leaf_entry_id = excluded.parent_leaf_entry_id,
            parent_stream_id = excluded.parent_stream_id,
            parent_stream_to_sequence = excluded.parent_stream_to_sequence,
            parent_binding_snapshot_id = excluded.parent_binding_snapshot_id,
            reason = excluded.reason,
            created_at_ms = excluded.created_at_ms",
            verlet_sqlite::params![
                base.child_thread_id.to_string(),
                base.parent_thread_id.to_string(),
                base.parent_checkpoint_id.map(|id| id.to_string()),
                base.parent_leaf_entry_id.map(|id| id.to_string()),
                base.parent_stream_id.as_str(),
                base.parent_stream_to_sequence
                    .map(|sequence| sequence.get()),
                base.parent_binding_snapshot_id.as_deref(),
                encode_thread_fork_reason(&base.reason)?,
                base.created_at_ms,
            ],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    Ok(())
}

async fn sqlite_load_thread_base(
    connection: &verlet_sqlite::Connection,
    child_thread_id: verlet_runtime_contracts::ThreadId,
) -> verlet_history::HistoryResult<Option<verlet_history::ThreadBaseRef>> {
    let mut rows = connection
        .query(
            "SELECT child_thread_id, parent_thread_id, parent_checkpoint_id,
                    parent_leaf_entry_id, parent_stream_id, parent_stream_to_sequence,
                    parent_binding_snapshot_id, reason, created_at_ms
             FROM thread_bases
             WHERE child_thread_id = ?1",
            verlet_sqlite::params![child_thread_id.to_string()],
        )
        .await
        .map_err(verlet_history::storage_error)?;
    rows.next()
        .await
        .map_err(verlet_history::storage_error)?
        .map(|row| sqlite_thread_base_from_row(&row))
        .transpose()
}

fn sqlite_thread_base_from_row(
    row: &verlet_sqlite::Row,
) -> verlet_history::HistoryResult<verlet_history::ThreadBaseRef> {
    let child_thread_id: String = row.get(0).map_err(verlet_history::storage_error)?;
    let parent_thread_id: String = row.get(1).map_err(verlet_history::storage_error)?;
    let parent_checkpoint_id: Option<String> = row.get(2).map_err(verlet_history::storage_error)?;
    let parent_leaf_entry_id: Option<String> = row.get(3).map_err(verlet_history::storage_error)?;
    let parent_stream_to_sequence: Option<i64> =
        row.get(5).map_err(verlet_history::storage_error)?;
    let reason: String = row.get(7).map_err(verlet_history::storage_error)?;
    Ok(verlet_history::ThreadBaseRef {
        child_thread_id: verlet_history::parse_thread_id(&child_thread_id)?,
        parent_thread_id: verlet_history::parse_thread_id(&parent_thread_id)?,
        parent_checkpoint_id: parent_checkpoint_id
            .map(|id| {
                verlet_runtime_contracts::ThreadCheckpointId::parse_str(&id)
                    .map_err(verlet_history::codec_error)
            })
            .transpose()?,
        parent_leaf_entry_id: parent_leaf_entry_id
            .map(|id| {
                verlet_history::parse_uuid(&id).map(verlet_history::SessionEntryId::from_uuid)
            })
            .transpose()?,
        parent_stream_id: verlet_history::EventStreamId::new(
            row.get::<String>(4)
                .map_err(verlet_history::storage_error)?,
        ),
        parent_stream_to_sequence: parent_stream_to_sequence
            .map(verlet_history::EventSequence::new),
        parent_binding_snapshot_id: row.get(6).map_err(verlet_history::storage_error)?,
        reason: decode_thread_fork_reason(&reason)?,
        created_at_ms: row.get(8).map_err(verlet_history::storage_error)?,
    })
}

async fn validate_sqlite_base_cycle(
    connection: &verlet_sqlite::Connection,
    child_thread_id: verlet_runtime_contracts::ThreadId,
    parent_thread_id: verlet_runtime_contracts::ThreadId,
) -> verlet_history::HistoryResult<()> {
    let mut cursor = Some(parent_thread_id);
    let mut visited = std::collections::HashSet::new();
    while let Some(thread_id) = cursor {
        if thread_id == child_thread_id || !visited.insert(thread_id) {
            return Err(verlet_history::HistoryError::ThreadBaseCycle {
                child_thread_id,
                ancestor_thread_id: thread_id,
            });
        }
        cursor = sqlite_load_thread_base(connection, thread_id)
            .await?
            .map(|base| base.parent_thread_id);
    }
    Ok(())
}

fn encode_thread_fork_reason(
    reason: &verlet_history::ThreadForkReason,
) -> verlet_history::HistoryResult<String> {
    let value = serde_json::to_value(reason).map_err(verlet_history::codec_error)?;
    value.as_str().map(str::to_string).ok_or_else(|| {
        verlet_history::HistoryError::Codec(
            "thread fork reason did not encode as string".to_string(),
        )
    })
}

fn decode_thread_fork_reason(
    value: &str,
) -> verlet_history::HistoryResult<verlet_history::ThreadForkReason> {
    serde_json::from_value(serde_json::Value::String(value.to_string()))
        .map_err(verlet_history::codec_error)
}

#[cfg(test)]
mod tests;
