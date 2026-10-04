//! Process-backed remote placement executor.
//!
//! The parent retains the only handle to each child process. Queue pulls and
//! stream pushes cross the authenticated sync endpoint; neither side opens
//! the other's SQLite file. Child processes admit queue rows through the
//! ordinary durable ingress claim/settle lane, then propagate their local
//! thread stream back under a fenced lease.

use crate::daemon::remote_store::endpoint::SyncIngressQueueAcknowledger as _;
use crate::daemon::remote_store::endpoint::SyncPullSource as _;
use crate::daemon::remote_store::lease::StreamLeaseAuthority as _;
use crate::daemon::remote_store::lease::SyncCredentialAuthority as _;
use crate::daemon::remote_store::propagator::StreamPropagator as _;
use crate::daemon::remote_store::queue::RemoteIngressQueue as _;
use crate::daemon::remote_store::tail::RemoteStreamTail as _;
use sha2::Digest as _;
use std::fmt::Write as _;
use tokio::io::AsyncWriteExt as _;
use verlet_history::EventStore as _;
use verlet_history::SessionStore as _;

const REMOTE_CHILD_COMMAND: &str = "__remote-child";
const REMOTE_TURN_ID_METADATA: &str = "cooldis_remote_turn_id";
const REMOTE_INPUT_METADATA: &str = "cooldis_remote_turn_input";
const CHILD_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);
const CHILD_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(1);
const PARENT_TAIL_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(20);

/// Secret-bearing bootstrap sent once over the child's stdin pipe. It is
/// deliberately not `Debug`: bearer tokens must never reach diagnostics.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct RemoteChildBootstrapV1 {
    pub schema: String,
    pub request: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    pub sync_endpoint: String,
    pub stream_lease: crate::daemon::remote_store::lease::StreamLeaseGrantV1,
    pub stream_bearer_token: String,
    pub queue_bearer_token: String,
    pub daemon_config_path: Option<std::path::PathBuf>,
    pub runtime_home: std::path::PathBuf,
    pub state_home: std::path::PathBuf,
}

const REMOTE_CHILD_BOOTSTRAP_SCHEMA_V1: &str = "cooldis.remote_child.bootstrap/1";

pub(crate) struct ProcessRemoteThreadExecutor {
    inner: std::sync::Arc<ProcessRemoteThreadExecutorInner>,
}

struct ProcessRemoteThreadExecutorInner {
    store: verlet_history_sqlite::SqliteSessionStore,
    queue: crate::daemon::remote_store::queue::SqliteRemoteIngressQueue,
    authority: std::sync::Arc<crate::daemon::remote_store::lease::SqliteStreamLeaseAuthority>,
    sync_endpoint: String,
    daemon_config_path: Option<std::path::PathBuf>,
    child_root: std::path::PathBuf,
    executable: std::path::PathBuf,
    spawn_lock: tokio::sync::Mutex<()>,
    states: std::sync::RwLock<
        std::collections::HashMap<
            verlet_runtime_contracts::ThreadId,
            std::sync::Arc<RemoteChildState>,
        >,
    >,
}

struct RemoteChildState {
    child: verlet_runtime_contracts::ThreadContext,
    status: tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
    _process: AbortOnDrop,
    _tail: AbortOnDrop,
}

struct AbortOnDrop(tokio::task::AbortHandle);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl Clone for ProcessRemoteThreadExecutor {
    fn clone(&self) -> Self {
        Self {
            inner: std::sync::Arc::clone(&self.inner),
        }
    }
}

impl std::fmt::Debug for ProcessRemoteThreadExecutor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ProcessRemoteThreadExecutor")
            .field("sync_endpoint", &self.inner.sync_endpoint)
            .field("child_root", &self.inner.child_root)
            .finish_non_exhaustive()
    }
}

impl ProcessRemoteThreadExecutor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn new(
        store: verlet_history_sqlite::SqliteSessionStore,
        authority: std::sync::Arc<crate::daemon::remote_store::lease::SqliteStreamLeaseAuthority>,
        sync_endpoint: String,
        daemon_config_path: Option<std::path::PathBuf>,
        child_root: std::path::PathBuf,
        executable: std::path::PathBuf,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let queue =
            crate::daemon::remote_store::queue::SqliteRemoteIngressQueue::new(store.clone())
                .await?;
        Ok(Self {
            inner: std::sync::Arc::new(ProcessRemoteThreadExecutorInner {
                store,
                queue,
                authority,
                sync_endpoint,
                daemon_config_path,
                child_root,
                executable,
                spawn_lock: tokio::sync::Mutex::new(()),
                states: std::sync::RwLock::new(std::collections::HashMap::new()),
            }),
        })
    }

    async fn spawn_shielded(
        &self,
        request: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let inner = std::sync::Arc::clone(&self.inner);
        let settlement_store = inner.store.clone();
        let settlement_request = request.clone();
        tokio::spawn(async move {
            let outcome = inner.spawn(request).await;
            if let Err(error) = &outcome {
                let reason = format!("remote child process failed to start: {error}");
                if let Err(settlement_error) = settle_remote_spawn_failure(
                    settlement_store,
                    settlement_request,
                    reason,
                )
                .await
                {
                    eprintln!(
                        "verlet remote child bootstrap failure settlement failed: {settlement_error}"
                    );
                }
            }
            outcome
        })
            .await
            .map_err(|error| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                    "remote child spawn transaction task failed: {error}"
                ))
            })?
    }

    fn state(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<std::sync::Arc<RemoteChildState>> {
        self.inner
            .states
            .read()
            .map_err(|_| remote_error("remote child state lock poisoned"))?
            .get(&thread_id)
            .cloned()
            .ok_or(crate::kernel::runtime_host::VerletError::ThreadNotFound(
                thread_id,
            ))
    }
}

impl ProcessRemoteThreadExecutorInner {
    async fn spawn(
        self: std::sync::Arc<Self>,
        request: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        // A duplicate projector/retry must not race the check below and boot
        // two OS children. The whole bootstrap is already detached from the
        // caller; serializing it also makes state installation the sole
        // visible completion point.
        let _spawn_guard = self.spawn_lock.lock().await;
        let thread_id = request.child.coordinates.thread_id;
        if self
            .states
            .read()
            .map_err(|_| remote_error("remote child state lock poisoned"))?
            .contains_key(&thread_id)
        {
            return Ok(());
        }

        let stream_id = verlet_history::EventStreamId::for_thread(&request.child.coordinates);
        let stream_grant = self
            .authority
            .grant_lease(
                &crate::daemon::remote_store::lease::StreamPrefixScope::new(stream_id.as_str()),
                &request.dispatch_id,
                crate::daemon::remote_store::lease::StreamLeaseLineage::default(),
            )
            .await?;
        let (_, stream_bearer_token) = self.authority.mint_credential(&stream_grant).await?;
        let queue_stream_id =
            crate::daemon::remote_store::queue::remote_ingress_queue_stream_id(thread_id);
        let queue_grant = self
            .authority
            .grant_lease(
                &crate::daemon::remote_store::lease::StreamPrefixScope::new(
                    queue_stream_id.as_str(),
                ),
                &request.dispatch_id,
                crate::daemon::remote_store::lease::StreamLeaseLineage::default(),
            )
            .await?;
        let (_, queue_bearer_token) = self.authority.mint_credential(&queue_grant).await?;

        self.queue
            .enqueue(queue_entry(
                &request.child.coordinates,
                request.turn_id.clone(),
                request.dispatch_id.clone(),
                &request.input,
            )?)
            .await?;

        let runtime_home = self.child_root.join(thread_id.to_string()).join("runtime");
        let state_home = self.child_root.join(thread_id.to_string()).join("state");
        std::fs::create_dir_all(&runtime_home)
            .map_err(|error| remote_error(format!("create remote child runtime home: {error}")))?;
        std::fs::create_dir_all(&state_home)
            .map_err(|error| remote_error(format!("create remote child state home: {error}")))?;
        let bootstrap = RemoteChildBootstrapV1 {
            schema: REMOTE_CHILD_BOOTSTRAP_SCHEMA_V1.to_string(),
            request: request.clone(),
            sync_endpoint: self.sync_endpoint.clone(),
            stream_lease: stream_grant,
            stream_bearer_token,
            queue_bearer_token,
            daemon_config_path: self.daemon_config_path.clone(),
            runtime_home,
            state_home,
        };
        let encoded = serde_json::to_vec(&bootstrap)
            .map_err(|error| remote_error(format!("encode remote child bootstrap: {error}")))?;
        let mut command = tokio::process::Command::new(&self.executable);
        command
            .arg(REMOTE_CHILD_COMMAND)
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::inherit())
            .kill_on_drop(true);
        let mut child = command
            .spawn()
            .map_err(|error| remote_error(format!("spawn remote child process: {error}")))?;
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| remote_error("remote child stdin pipe is unavailable"))?;
        stdin
            .write_all(&encoded)
            .await
            .map_err(|error| remote_error(format!("write remote child bootstrap: {error}")))?;
        stdin
            .shutdown()
            .await
            .map_err(|error| remote_error(format!("close remote child bootstrap: {error}")))?;

        let (status_tx, _) =
            tokio::sync::watch::channel(verlet_runtime_contracts::ThreadStatus::Starting);
        let process_status = status_tx.clone();
        let process_store = self.store.clone();
        let process_request = request.clone();
        let process_task = tokio::spawn(async move {
            let outcome = child.wait().await;
            if !matches!(
                *process_status.borrow(),
                verlet_runtime_contracts::ThreadStatus::Idle
                    | verlet_runtime_contracts::ThreadStatus::Stopped
            ) {
                let _ = process_status.send(verlet_runtime_contracts::ThreadStatus::Failed);
            }
            let reason = match outcome {
                Ok(status) => {
                    format!("remote child process exited before durable terminal state ({status})")
                }
                Err(error) => format!(
                    "remote child process wait failed before durable terminal state ({error})"
                ),
            };
            if let Err(error) =
                settle_remote_process_death(process_store, process_request, reason).await
            {
                eprintln!("verlet remote child process death settlement failed: {error}");
            }
        });
        let tail =
            crate::daemon::remote_store::tail::SqliteRemoteStreamTail::new(self.store.clone());
        let tail_store = self.store.clone();
        let tail_request = request.clone();
        let tail_status = status_tx.clone();
        let tail_task = tokio::spawn(async move {
            run_parent_tail(tail, tail_store, tail_request, tail_status).await;
        });
        let state = std::sync::Arc::new(RemoteChildState {
            child: request.child.clone(),
            status: status_tx,
            _process: AbortOnDrop(process_task.abort_handle()),
            _tail: AbortOnDrop(tail_task.abort_handle()),
        });
        let replaced = self
            .states
            .write()
            .map_err(|_| remote_error("remote child state lock poisoned"))?
            .insert(thread_id, state);
        if replaced.is_some() {
            return Err(remote_error(
                "remote child state was concurrently installed",
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl crate::daemon::remote_store::placement::RemoteThreadExecutor for ProcessRemoteThreadExecutor {
    async fn context(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> Option<verlet_runtime_contracts::ThreadContext> {
        self.inner
            .states
            .read()
            .ok()
            .and_then(|states| states.get(&thread_id).map(|state| state.child.clone()))
    }

    async fn spawn(
        &self,
        request: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        self.spawn_shielded(request).await
    }

    async fn submit(
        &self,
        request: crate::daemon::remote_store::placement::RemoteThreadSubmitRequest,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_runtime_contracts::ThreadStatus> {
        let state = self.state(request.target_thread_id)?;
        self.inner
            .queue
            .enqueue(queue_entry(
                &state.child.coordinates,
                request.turn_id,
                request.dispatch_id,
                &request.input,
            )?)
            .await?;
        let _ = state
            .status
            .send(verlet_runtime_contracts::ThreadStatus::Running);
        Ok(verlet_runtime_contracts::ThreadStatus::Running)
    }

    async fn observe(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::daemon::remote_store::placement::RemoteThreadObservation,
    > {
        let state = self.state(thread_id)?;
        let stream_id = verlet_history::EventStreamId::for_thread(&state.child.coordinates);
        let records = self
            .inner
            .store
            .read_events(&stream_id, None)
            .await
            .map_err(|error| {
                crate::kernel::runtime_host::VerletError::History(error.to_string())
            })?;
        let status = fold_remote_status(&records).unwrap_or(*state.status.borrow());
        let latest_output =
            latest_assistant_output(&self.inner.store, &state.child.coordinates).await?;
        Ok(
            crate::daemon::remote_store::placement::RemoteThreadObservation {
                status,
                latest_output,
            },
        )
    }

    async fn wait(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
        timeout_ms: Option<u64>,
    ) -> crate::kernel::runtime_host::VerletResult<
        crate::daemon::remote_store::placement::RemoteThreadWaitObservation,
    > {
        let state = self.state(thread_id)?;
        let mut status = state.status.subscribe();
        let wait = async {
            loop {
                if matches!(
                    *status.borrow(),
                    verlet_runtime_contracts::ThreadStatus::Idle
                        | verlet_runtime_contracts::ThreadStatus::Stopped
                        | verlet_runtime_contracts::ThreadStatus::Failed
                ) {
                    break;
                }
                if status.changed().await.is_err() {
                    break;
                }
            }
        };
        let timed_out = match timeout_ms {
            Some(timeout_ms) => {
                tokio::time::timeout(std::time::Duration::from_millis(timeout_ms), wait)
                    .await
                    .is_err()
            }
            None => {
                wait.await;
                false
            }
        };
        Ok(
            crate::daemon::remote_store::placement::RemoteThreadWaitObservation {
                observation: self.observe(thread_id).await?,
                timed_out,
            },
        )
    }
}

async fn run_parent_tail(
    tail: crate::daemon::remote_store::tail::SqliteRemoteStreamTail,
    store: verlet_history_sqlite::SqliteSessionStore,
    request: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    status: tokio::sync::watch::Sender<verlet_runtime_contracts::ThreadStatus>,
) {
    let stream_id = verlet_history::EventStreamId::for_thread(&request.child.coordinates);
    let mut cursor = crate::daemon::remote_store::tail::RemoteStreamTailCursor {
        stream_id: stream_id.clone(),
        cursor: None,
    };
    let mut joined = false;
    loop {
        match tail.poll(&cursor).await {
            Ok(page) => {
                let next = page.next;
                let mut page_folded = true;
                for record in page.records {
                    if let Some(folded) = fold_remote_status(std::slice::from_ref(&record)) {
                        let _ = status.send(folded);
                    }
                    if joined {
                        continue;
                    }
                    match settle_remote_terminal_record(store.clone(), request.clone(), record)
                        .await
                    {
                        Ok(true) => joined = true,
                        Ok(false) => {}
                        Err(error) => {
                            eprintln!("verlet remote tail failed to fold terminal record: {error}");
                            page_folded = false;
                            break;
                        }
                    }
                }
                // Advance only after every first-wins fold in this snapshot
                // succeeded. A transient parent-store failure must replay the
                // durable child terminal instead of skipping it forever.
                if page_folded {
                    cursor = next;
                }
            }
            Err(error) => {
                // Isolation is per child task: one malformed/diverged stream
                // reports and retries without stopping any other tail.
                eprintln!("verlet remote tail poll failed for {stream_id}: {error}");
            }
        }
        tokio::time::sleep(PARENT_TAIL_POLL_INTERVAL).await;
    }
}

fn remote_parent_coordinates(
    request: &crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
) -> crate::kernel::runtime_host::VerletResult<verlet_runtime_contracts::ThreadCoordinates> {
    let parent_thread_id = request
        .child
        .parent_thread_id
        .ok_or_else(|| remote_error("remote child bootstrap requires parent thread topology"))?;
    let mut parent = request.child.coordinates.clone();
    parent.thread_id = parent_thread_id;
    Ok(parent)
}

#[allow(clippy::too_many_arguments)]
async fn append_remote_join_shielded(
    store: verlet_history_sqlite::SqliteSessionStore,
    request: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    terminal_state: verlet_history::ThreadTerminalState,
    result_digest: Option<String>,
    reason: Option<String>,
    source_event: Option<(verlet_history::EventStreamId, verlet_history::EventRecordId)>,
    discharged_by: &'static str,
    function: &'static str,
) -> crate::kernel::runtime_host::VerletResult<bool> {
    let parent = remote_parent_coordinates(&request)?;
    tokio::spawn(async move {
        crate::kernel::runtime_host::runtime_services::append_thread_joined_first_wins(
            &store,
            parent,
            request.child.coordinates,
            request.spawned_event_id,
            terminal_state,
            result_digest,
            reason,
            source_event,
            discharged_by,
            function,
        )
        .await
        .map(|joined| joined.appended)
    })
    .await
    .map_err(|error| remote_error(format!("remote child join settlement task failed: {error}")))?
}

/// Fold one durable terminal for the spawn-dispatched turn. The boolean says
/// whether the record named that turn; the shared durable fence may already
/// have been won by process-death settlement or startup recovery.
async fn settle_remote_terminal_record(
    store: verlet_history_sqlite::SqliteSessionStore,
    request: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    record: verlet_history::EventRecord,
) -> crate::kernel::runtime_host::VerletResult<bool> {
    let Some(terminal) =
        crate::daemon::recovery_sweep::project_child_terminal_record(&record, &request.turn_id)
    else {
        return Ok(false);
    };
    append_remote_join_shielded(
        store,
        request,
        terminal.state,
        terminal.outcome_reason.clone(),
        terminal.outcome_reason,
        Some((terminal.stream_id, terminal.event_id)),
        "tail:remote-thread",
        "remote_thread_join/v1",
    )
    .await?;
    Ok(true)
}

async fn settle_remote_process_death(
    store: verlet_history_sqlite::SqliteSessionStore,
    request: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    reason: String,
) -> crate::kernel::runtime_host::VerletResult<bool> {
    append_remote_join_shielded(
        store,
        request,
        verlet_history::ThreadTerminalState::Failed,
        Some(reason.clone()),
        Some(reason),
        None,
        "monitor:remote-child-process",
        "remote_child_process_wait/v1",
    )
    .await
}

async fn settle_remote_spawn_failure(
    store: verlet_history_sqlite::SqliteSessionStore,
    request: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    reason: String,
) -> crate::kernel::runtime_host::VerletResult<bool> {
    append_remote_join_shielded(
        store,
        request,
        verlet_history::ThreadTerminalState::Failed,
        Some(reason.clone()),
        Some(reason),
        None,
        "executor:remote-thread",
        "remote_thread_spawn/v1",
    )
    .await
}

fn queue_entry(
    target: &verlet_runtime_contracts::ThreadCoordinates,
    turn_id: String,
    dispatch_id: verlet_runtime_contracts::handle::DispatchId,
    input: &crate::kernel::runtime_host::turn::TurnInput,
) -> crate::kernel::runtime_host::VerletResult<
    crate::daemon::remote_store::queue::RemoteIngressQueueEntryV1,
> {
    let target_thread_id = target.thread_id;
    let source = verlet_io_core::IoSource::new("cooldis.remote", "ingress");
    let mut envelope = verlet_io_core::IngressEnvelope::new(
        source.clone(),
        verlet_io_core::IoConversation::new(
            target_thread_id.to_string(),
            verlet_io_core::ConversationKind::System,
        ),
        verlet_io_core::IngressContent::text(input.text_projection()),
        0,
    )
    .with_dedupe_key(verlet_io_core::IoDedupeKey::for_source(
        &source,
        dispatch_id.as_str(),
    ))
    .with_delivery(verlet_io_core::IoDelivery::new(dispatch_id.to_string()))
    .with_principal(verlet_io_core::IoPrincipal::new(
        target.tenant_id.clone(),
        target.user_id.clone(),
        format!("remote:{dispatch_id}"),
    ));
    envelope.id = format!("remote-ingress-{}", sha256_hex(dispatch_id.as_str()));
    envelope
        .metadata
        .insert(REMOTE_TURN_ID_METADATA.to_string(), turn_id);
    envelope.metadata.insert(
        REMOTE_INPUT_METADATA.to_string(),
        serde_json::to_string(input)
            .map_err(|error| remote_error(format!("encode remote turn input: {error}")))?,
    );
    Ok(
        crate::daemon::remote_store::queue::RemoteIngressQueueEntryV1 {
            schema: crate::daemon::remote_store::queue::SYNC_INGRESS_QUEUE_ENTRY_SCHEMA_V1
                .to_string(),
            dispatch_id,
            target_thread_id,
            envelope,
            // Queue identity must be byte-stable across retries. The durable row
            // sequence is the authoritative enqueue order; this field therefore
            // carries the stable dispatch epoch rather than a retry-local clock.
            enqueued_at_ms: 0,
        },
    )
}

fn fold_remote_status(
    records: &[verlet_history::EventRecord],
) -> Option<verlet_runtime_contracts::ThreadStatus> {
    records.iter().rev().find_map(|record| match record.kind {
        verlet_history::EventKind::TurnCompleted | verlet_history::EventKind::LoopCompleted => {
            Some(verlet_runtime_contracts::ThreadStatus::Idle)
        }
        verlet_history::EventKind::LoopDenied
        | verlet_history::EventKind::LoopBlocked
        | verlet_history::EventKind::LoopBudgetExhausted
        | verlet_history::EventKind::TurnFailed => {
            Some(verlet_runtime_contracts::ThreadStatus::Failed)
        }
        verlet_history::EventKind::TurnSubmitted => {
            Some(verlet_runtime_contracts::ThreadStatus::Running)
        }
        _ => None,
    })
}

async fn latest_assistant_output(
    store: &verlet_history_sqlite::SqliteSessionStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> crate::kernel::runtime_host::VerletResult<Option<String>> {
    let context = store
        .build_context(coordinates)
        .await
        .map_err(|error| crate::kernel::runtime_host::VerletError::History(error.to_string()))?;
    Ok(context.entries.iter().rev().find_map(|entry| {
        if entry.coordinates.thread_id != coordinates.thread_id {
            return None;
        }
        let (verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::Assistant { content, .. },
        }
        | verlet_history::SessionEntryKind::CustomContextMessage {
            message: verlet_history::CanonicalMessage::Assistant { content, .. },
        }) = &entry.kind
        else {
            return None;
        };
        let text = content
            .iter()
            .filter_map(|content| match content {
                verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
                _ => None,
            })
            .collect::<String>();
        (!text.is_empty()).then_some(text)
    }))
}

pub(crate) async fn run_remote_child(
    mut app_config: crate::adapters::app_server::VerletAppServerConfig,
    bootstrap: RemoteChildBootstrapV1,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if bootstrap.schema != REMOTE_CHILD_BOOTSTRAP_SCHEMA_V1 {
        return Err(remote_error("unsupported remote child bootstrap schema"));
    }
    reject_remote_workspace_binding(bootstrap.request.bind_payload.as_ref())?;
    #[cfg(unix)]
    // SAFETY: getppid has no pointer arguments or caller-side invariants.
    let parent_process_id = unsafe { libc::getppid() };
    app_config.runtime_home = bootstrap.runtime_home.clone();
    app_config.state_home = bootstrap.state_home.clone();
    app_config.listen = crate::adapters::app_server::AppServerListenAddr::Unix(
        crate::adapters::app_server::instance::instance_unix_socket_path(&app_config.state_home)?,
    );
    let app = crate::adapters::app_server::VerletAppServer::new_local(app_config).await?;
    let supervisor = app.supervisor();
    let child = &bootstrap.request.child;
    let parent_thread_id = child
        .parent_thread_id
        .ok_or_else(|| remote_error("remote child bootstrap requires parent thread topology"))?;
    let _parent = match supervisor
        .get_thread(&child.coordinates.tenant_id, parent_thread_id)
        .await
    {
        Ok(parent) => parent,
        Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(_)) => {
            supervisor
                .start_thread_with_id(
                    crate::kernel::supervisor::ThreadStartRequest {
                        tenant_id: child.coordinates.tenant_id.clone(),
                        user_id: child.coordinates.user_id.clone(),
                        session_id: child.coordinates.session_id.clone(),
                        topology: Default::default(),
                        metadata: Default::default(),
                    },
                    parent_thread_id,
                )
                .await?
        }
        Err(error) => return Err(error),
    };
    let child_handle = match supervisor
        .get_thread(&child.coordinates.tenant_id, child.coordinates.thread_id)
        .await
    {
        Ok(child) => child,
        Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(_)) => {
            supervisor
                .start_thread_with_id(
                    crate::kernel::supervisor::ThreadStartRequest {
                        tenant_id: child.coordinates.tenant_id.clone(),
                        user_id: child.coordinates.user_id.clone(),
                        session_id: child.coordinates.session_id.clone(),
                        topology: child.topology.clone(),
                        metadata: child.metadata.clone(),
                    },
                    child.coordinates.thread_id,
                )
                .await?
        }
        Err(error) => return Err(error),
    };
    if let (Some(compile), Some(bind)) = (
        bootstrap.request.compile_payload.clone(),
        bootstrap.request.bind_payload.clone(),
    ) {
        if let Some(principal_id) = bootstrap.request.binding_principal_id.as_deref() {
            child_handle
                .record_remote_manifest_receipts_for_principal(compile, bind, principal_id)
                .await?;
        } else {
            child_handle
                .record_remote_manifest_receipts(compile, bind)
                .await?;
        }
    }

    let local_store = verlet_history_sqlite::SqliteSessionStore::open(app.session_store_path())
        .await
        .map_err(|error| crate::kernel::runtime_host::VerletError::History(error.to_string()))?
        .with_lease_epoch(app.lease_epoch());
    init_child_cursor_schema(local_store.clone()).await?;
    let state_store = std::sync::Arc::new(
        crate::daemon::remote_store::propagator::SqlitePropagationStateStore::new(
            local_store.clone(),
        )
        .await?,
    );
    let stream_id = verlet_history::EventStreamId::for_thread(&child.coordinates);
    let mut propagation_state = match state_store.load(&stream_id).await? {
        Some(state) => state,
        None => {
            let state = crate::daemon::remote_store::propagator::StreamPropagationState {
                stream_id: stream_id.clone(),
                lease: bootstrap.stream_lease.clone(),
                pushed_through: None,
            };
            state_store.persist(&state).await?;
            state
        }
    };
    let http = std::sync::Arc::new(
        crate::daemon::remote_store::endpoint_http::HttpSyncClient::new(bootstrap.sync_endpoint)?,
    );
    let propagator = crate::daemon::remote_store::propagator::LocalFirstStreamPropagator::new(
        local_store.clone(),
        http.clone(),
        http.clone(),
        http.clone(),
        state_store,
        bootstrap.stream_bearer_token,
        std::sync::Arc::new(crate::daemon::clock_route::SystemDaemonClock),
    );
    let bridge = crate::daemon::daemon_io::VerletDaemonIoBridge::from_app_server(&app);
    let queue_stream_id = crate::daemon::remote_store::queue::remote_ingress_queue_stream_id(
        child.coordinates.thread_id,
    );
    let mut queue_cursor = load_child_cursor(&local_store, child.coordinates.thread_id).await?;
    let mut endpoint_backoff = CHILD_POLL_INTERVAL;
    loop {
        #[cfg(unix)]
        if !remote_parent_process_is_alive(parent_process_id) {
            return Err(remote_error(
                "remote child observed its parent daemon process exit",
            ));
        }
        let records = match http
            .pull_after(
                &bootstrap.queue_bearer_token,
                &queue_stream_id,
                queue_cursor.clone(),
            )
            .await
        {
            Ok(records) => records,
            Err(error) if is_transient_sync_error(&error) => {
                tokio::time::sleep(endpoint_backoff).await;
                endpoint_backoff = next_child_retry(endpoint_backoff);
                continue;
            }
            Err(error) => return Err(error),
        };
        let mut retry_queue_delivery = false;
        for record in records {
            let entry = serde_json::from_value::<
                crate::daemon::remote_store::queue::RemoteIngressQueueEntryV1,
            >(record.payload.clone())
            .map_err(|error| remote_error(format!("decode remote child queue entry: {error}")))?;
            if entry.target_thread_id != child.coordinates.thread_id {
                return Err(remote_error(
                    "remote child queue entry escaped its target prefix",
                ));
            }
            let turn_id = entry
                .envelope
                .metadata
                .get(REMOTE_TURN_ID_METADATA)
                .cloned()
                .ok_or_else(|| remote_error("remote child queue entry is missing turn id"))?;
            let turn_input = entry
                .envelope
                .metadata
                .get(REMOTE_INPUT_METADATA)
                .map(|encoded| {
                    serde_json::from_str::<crate::kernel::runtime_host::turn::TurnInput>(encoded)
                })
                .transpose()
                .map_err(|error| remote_error(format!("decode remote turn input: {error}")))?
                .unwrap_or_else(|| {
                    crate::kernel::runtime_host::turn::TurnInput::text(
                        entry.envelope.content.text_projection(),
                    )
                });
            let mut target = verlet_io_core::ResolvedIoTarget::new(
                verlet_io_core::ThreadAddress::new(
                    child.coordinates.tenant_id.clone(),
                    child.coordinates.user_id.clone(),
                    child.coordinates.session_id.clone(),
                )
                .with_thread_id(child.coordinates.thread_id.to_string()),
            );
            target.create_thread_if_missing = false;
            let mut io_input = verlet_io_core::IoTurnInput::text(turn_input.text_projection());
            io_input.metadata = turn_input.metadata.clone();
            match bridge
                .submit_durable_remote_envelope(
                    entry.envelope,
                    target,
                    verlet_io_core::AdmissionDecision::queue(turn_id, io_input),
                    1,
                )
                .await
            {
                Ok(_) => {}
                Err(
                    error @ (verlet_io_core::IoError::InvalidEnvelope(_)
                    | verlet_io_core::IoError::UnknownProtocol(_)
                    | verlet_io_core::IoError::PolicyRejected(_)
                    | verlet_io_core::IoError::PermanentDelivery(_)),
                ) => {
                    return Err(remote_error(format!(
                        "remote child ingress rejected: {error}"
                    )));
                }
                Err(
                    verlet_io_core::IoError::Queue(_)
                    | verlet_io_core::IoError::Delivery(_)
                    | verlet_io_core::IoError::RateLimited { .. }
                    | verlet_io_core::IoError::Bridge(_),
                ) => {
                    // The ordinary durable ingress lane owns dedupe. Retrying the
                    // same queue row is safe whether the failed call committed
                    // nothing or lost its response after committing the claim.
                    retry_queue_delivery = true;
                    break;
                }
            }
            match http
                .acknowledge_ingress(
                    &bootstrap.queue_bearer_token,
                    crate::daemon::remote_store::endpoint::SyncIngressQueueAckRequestV1 {
                        schema:
                            crate::daemon::remote_store::endpoint::SYNC_INGRESS_QUEUE_ACK_SCHEMA_V1
                                .to_string(),
                        target_thread_id: child.coordinates.thread_id,
                        dispatch_id: entry.dispatch_id,
                    },
                )
                .await
            {
                Ok(()) => {}
                Err(error) if is_transient_sync_error(&error) => {
                    retry_queue_delivery = true;
                    break;
                }
                Err(error) => return Err(error),
            }
            queue_cursor = Some(verlet_history::StreamCursorV1::new(
                record.stream_id.clone(),
                record.sequence,
                record.event_id,
            ));
            persist_child_cursor(
                local_store.clone(),
                child.coordinates.thread_id,
                queue_cursor.as_ref().expect("queue cursor was assigned"),
            )
            .await?;
        }
        if retry_queue_delivery {
            tokio::time::sleep(endpoint_backoff).await;
            endpoint_backoff = next_child_retry(endpoint_backoff);
            continue;
        }
        match propagator.propagate_once(&mut propagation_state).await? {
            crate::daemon::remote_store::propagator::PropagationStep::LeaseFenced => {
                return Err(remote_error("remote child stream lease was fenced"));
            }
            crate::daemon::remote_store::propagator::PropagationStep::StreamDiverged { .. } => {
                return Err(remote_error("remote child stream diverged from parent"));
            }
            crate::daemon::remote_store::propagator::PropagationStep::Converged
            | crate::daemon::remote_store::propagator::PropagationStep::Advanced { .. } => {
                endpoint_backoff = CHILD_POLL_INTERVAL;
            }
            crate::daemon::remote_store::propagator::PropagationStep::EndpointUnavailable => {
                tokio::time::sleep(endpoint_backoff).await;
                endpoint_backoff = next_child_retry(endpoint_backoff);
                continue;
            }
        }
        tokio::time::sleep(CHILD_POLL_INTERVAL).await;
    }
}

fn reject_remote_workspace_binding(
    bind_payload: Option<&serde_json::Value>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    if bind_payload
        .and_then(|payload| payload.get("workspace"))
        .is_some_and(|workspace| !workspace.is_null())
    {
        return Err(remote_error(
            "remote child bootstrap cannot carry a local host workspace binding",
        ));
    }
    Ok(())
}

fn is_transient_sync_error(error: &crate::kernel::runtime_host::VerletError) -> bool {
    matches!(
        error,
        crate::kernel::runtime_host::VerletError::RuntimeExecution(_)
    )
}

fn next_child_retry(current: std::time::Duration) -> std::time::Duration {
    current.saturating_mul(2).min(CHILD_RETRY_MAX)
}

#[cfg(unix)]
fn remote_parent_process_is_alive(expected_parent: libc::pid_t) -> bool {
    // Parentage cannot be confused by PID reuse: Unix reparents this process
    // when the daemon dies, so the current PPID must remain the one captured
    // before entering the long-lived child loop. A captured PPID of 1 is
    // ambiguous — the daemon may itself be a container's init — so the
    // monitor is disabled rather than declaring a PID-1 daemon dead.
    expected_parent <= 1 || unsafe { libc::getppid() == expected_parent }
}

async fn init_child_cursor_schema(
    store: verlet_history_sqlite::SqliteSessionStore,
) -> crate::kernel::runtime_host::VerletResult<()> {
    child_cursor_transaction(store, None).await.map(|_| ())
}

async fn load_child_cursor(
    store: &verlet_history_sqlite::SqliteSessionStore,
    thread_id: verlet_runtime_contracts::ThreadId,
) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::StreamCursorV1>> {
    let connection =
        store.sqlite_database().connect().await.map_err(|error| {
            crate::kernel::runtime_host::VerletError::History(error.to_string())
        })?;
    let mut rows = connection
        .query(
            "SELECT cursor_json FROM cooldis_remote_child_queue_cursor WHERE thread_id = ?1",
            verlet_sqlite::params![thread_id.to_string()],
        )
        .await
        .map_err(|error| crate::kernel::runtime_host::VerletError::History(error.to_string()))?;
    rows.next()
        .await
        .map_err(|error| crate::kernel::runtime_host::VerletError::History(error.to_string()))?
        .map(|row| {
            let encoded = row.get::<String>(0).map_err(|error| {
                crate::kernel::runtime_host::VerletError::History(error.to_string())
            })?;
            serde_json::from_str(&encoded).map_err(|error| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "decode child cursor: {error}"
                ))
            })
        })
        .transpose()
}

async fn persist_child_cursor(
    store: verlet_history_sqlite::SqliteSessionStore,
    thread_id: verlet_runtime_contracts::ThreadId,
    cursor: &verlet_history::StreamCursorV1,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let encoded = serde_json::to_string(cursor).map_err(|error| {
        crate::kernel::runtime_host::VerletError::History(format!("encode child cursor: {error}"))
    })?;
    child_cursor_transaction(store, Some((thread_id, encoded)))
        .await
        .map(|_| ())
}

async fn child_cursor_transaction(
    store: verlet_history_sqlite::SqliteSessionStore,
    update: Option<(verlet_runtime_contracts::ThreadId, String)>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    tokio::spawn(async move {
        let database = store.sqlite_database();
        let mut connection = database.connect().await.map_err(|error| {
            crate::kernel::runtime_host::VerletError::History(error.to_string())
        })?;
        let transaction = connection
            .transaction_with_behavior(verlet_sqlite::TransactionBehavior::Immediate)
            .await
            .map_err(|error| {
                crate::kernel::runtime_host::VerletError::History(error.to_string())
            })?;
        transaction
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS cooldis_remote_child_queue_cursor (
                    thread_id TEXT PRIMARY KEY NOT NULL,
                    cursor_json TEXT NOT NULL
                );",
            )
            .await
            .map_err(|error| {
                crate::kernel::runtime_host::VerletError::History(error.to_string())
            })?;
        if let Some((thread_id, encoded)) = update {
            transaction
                .execute(
                    "INSERT INTO cooldis_remote_child_queue_cursor (thread_id, cursor_json)
                     VALUES (?1, ?2)
                     ON CONFLICT(thread_id) DO UPDATE SET cursor_json = excluded.cursor_json",
                    verlet_sqlite::params![thread_id.to_string(), encoded],
                )
                .await
                .map_err(|error| {
                    crate::kernel::runtime_host::VerletError::History(error.to_string())
                })?;
        }
        transaction.commit().await.map_err(|error| {
            crate::kernel::runtime_host::VerletError::History(error.to_string())
        })?;
        Ok(())
    })
    .await
    .map_err(|error| {
        crate::kernel::runtime_host::VerletError::History(format!(
            "remote child cursor transaction task failed: {error}"
        ))
    })?
}

fn sha256_hex(value: &str) -> String {
    sha2::Sha256::digest(value.as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut encoded, byte| {
            let _ = write!(encoded, "{byte:02x}");
            encoded
        },
    )
}

fn remote_error(message: impl Into<String>) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeExecution(message.into())
}

pub(crate) fn is_remote_child_command(command: &std::ffi::OsStr) -> bool {
    command == REMOTE_CHILD_COMMAND
}

#[cfg(test)]
mod tests {
    use crate::daemon::remote_store::placement::RemoteThreadExecutor as _;
    use verlet_history::EventStore as _;

    fn request_fixture() -> crate::daemon::remote_store::placement::RemoteThreadSpawnRequest {
        let parent = verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");
        let child = verlet_runtime_contracts::ThreadContext::with_topology_and_metadata(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::spawned_from(parent.thread_id),
            Default::default(),
        );
        crate::daemon::remote_store::placement::RemoteThreadSpawnRequest {
            child,
            task_name: Some("remote-test".to_string()),
            turn_id: "spawn-turn".to_string(),
            dispatch_id: verlet_runtime_contracts::handle::DispatchId::new("spawn-dispatch"),
            input: crate::kernel::runtime_host::turn::TurnInput::text("run"),
            spawned_event_id: verlet_history::EventRecordId::new(),
            compile_payload: None,
            bind_payload: None,
            binding_principal_id: None,
        }
    }

    #[test]
    fn remote_child_bootstrap_rejects_workspace_before_constructing_the_runtime() {
        let bind = serde_json::json!({
            "placement": {"target": "remote"},
            "workspace": {
                "guest_path": "/work",
                "host_path": "/tmp/remote-workspace",
                "mode": "rw"
            }
        });

        let error = crate::daemon::remote_store::process_executor::reject_remote_workspace_binding(
            Some(&bind),
        )
        .unwrap_err();

        assert!(error.to_string().contains("cannot carry"));
    }

    #[test]
    fn remote_spawn_request_round_trips_the_binding_principal() {
        let mut request = request_fixture();
        request.binding_principal_id = Some("principal:remote-operator".to_string());

        let encoded = serde_json::to_value(&request).unwrap();
        let decoded: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest =
            serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(decoded.binding_principal_id, request.binding_principal_id);

        let mut legacy = encoded;
        legacy
            .as_object_mut()
            .unwrap()
            .remove("binding_principal_id");
        let decoded: crate::daemon::remote_store::placement::RemoteThreadSpawnRequest =
            serde_json::from_value(legacy).unwrap();
        assert_eq!(decoded.binding_principal_id, None);
    }

    fn child_record(
        request: &crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
        kind: verlet_history::EventKind,
        turn_id: &str,
    ) -> verlet_history::EventRecord {
        let stream_id = verlet_history::EventStreamId::for_thread(&request.child.coordinates);
        verlet_history::EventRecord::from_new(
            stream_id,
            verlet_history::EventSequence::new(1),
            verlet_history::NewEventRecord::witnessed(
                request.child.coordinates.clone(),
                kind,
                serde_json::json!({"turn_id": turn_id}),
            ),
        )
    }

    async fn joined_payloads(
        store: &verlet_history_sqlite::SqliteSessionStore,
        request: &crate::daemon::remote_store::placement::RemoteThreadSpawnRequest,
    ) -> Vec<verlet_history::ThreadJoinedPayload> {
        let parent =
            crate::daemon::remote_store::process_executor::remote_parent_coordinates(request)
                .unwrap();
        store
            .read_events(
                &crate::kernel::control_decision::control_stream_id(&parent),
                None,
            )
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.kind == verlet_history::EventKind::ThreadJoined)
            .map(|event| serde_json::from_value(event.payload).unwrap())
            .collect()
    }

    #[test]
    fn loop_completed_folds_remote_status_to_idle() {
        let request = request_fixture();
        let record = child_record(
            &request,
            verlet_history::EventKind::LoopCompleted,
            "spawn-turn",
        );
        assert_eq!(
            crate::daemon::remote_store::process_executor::fold_remote_status(&[record]),
            Some(verlet_runtime_contracts::ThreadStatus::Idle)
        );
    }

    #[test]
    fn turn_failed_folds_remote_status_to_failed() {
        let request = request_fixture();
        let record = child_record(
            &request,
            verlet_history::EventKind::TurnFailed,
            "spawn-turn",
        );
        assert_eq!(
            crate::daemon::remote_store::process_executor::fold_remote_status(&[record]),
            Some(verlet_runtime_contracts::ThreadStatus::Failed)
        );
    }

    #[tokio::test]
    async fn spawn_turn_failure_terminals_settle_with_emo426_state_projection() {
        for (kind, expected) in [
            (
                verlet_history::EventKind::TurnFailed,
                verlet_history::ThreadTerminalState::Failed,
            ),
            (
                verlet_history::EventKind::LoopDenied,
                verlet_history::ThreadTerminalState::Failed,
            ),
            (
                verlet_history::EventKind::LoopBlocked,
                verlet_history::ThreadTerminalState::Failed,
            ),
            (
                verlet_history::EventKind::LoopBudgetExhausted,
                verlet_history::ThreadTerminalState::BudgetExhausted,
            ),
        ] {
            let store = verlet_history_sqlite::SqliteSessionStore::in_memory()
                .await
                .unwrap();
            let request = request_fixture();
            let record = child_record(&request, kind, "spawn-turn");
            assert!(
                crate::daemon::remote_store::process_executor::settle_remote_terminal_record(
                    store.clone(),
                    request.clone(),
                    record
                )
                .await
                .unwrap()
            );
            let joined = joined_payloads(&store, &request).await;
            assert_eq!(joined.len(), 1);
            assert_eq!(joined[0].terminal_state, expected);
        }
    }

    #[tokio::test]
    async fn process_death_late_terminal_duplicate_tail_and_recovery_share_one_join_fence() {
        let store = verlet_history_sqlite::SqliteSessionStore::in_memory()
            .await
            .unwrap();
        let request = request_fixture();
        let terminal = child_record(
            &request,
            verlet_history::EventKind::TurnCompleted,
            "spawn-turn",
        );
        let parent =
            crate::daemon::remote_store::process_executor::remote_parent_coordinates(&request)
                .unwrap();
        let reason = "remote child process died before durable terminal state".to_string();

        let death = crate::daemon::remote_store::process_executor::settle_remote_process_death(
            store.clone(),
            request.clone(),
            reason.clone(),
        );
        let tail = crate::daemon::remote_store::process_executor::settle_remote_terminal_record(
            store.clone(),
            request.clone(),
            terminal.clone(),
        );
        let duplicate_tail =
            crate::daemon::remote_store::process_executor::settle_remote_terminal_record(
                store.clone(),
                request.clone(),
                terminal.clone(),
            );
        let recovery =
            crate::kernel::runtime_host::runtime_services::append_thread_joined_first_wins(
                &store,
                parent,
                request.child.coordinates.clone(),
                request.spawned_event_id,
                verlet_history::ThreadTerminalState::Failed,
                Some(reason.clone()),
                Some(reason),
                Some((terminal.stream_id.clone(), terminal.id)),
                "recovery:startup-sweep",
                "thread_join_recovery/v1",
            );
        let (death, tail, duplicate_tail, recovery) =
            tokio::join!(death, tail, duplicate_tail, recovery);
        death.unwrap();
        tail.unwrap();
        duplicate_tail.unwrap();
        recovery.unwrap();

        assert_eq!(joined_payloads(&store, &request).await.len(), 1);

        let death_only_store = verlet_history_sqlite::SqliteSessionStore::in_memory()
            .await
            .unwrap();
        assert!(
            crate::daemon::remote_store::process_executor::settle_remote_process_death(
                death_only_store.clone(),
                request.clone(),
                "remote child process exited with status 17".to_string(),
            )
            .await
            .unwrap()
        );
        let death_join = joined_payloads(&death_only_store, &request).await;
        assert_eq!(death_join.len(), 1);
        assert_eq!(
            death_join[0].terminal_state,
            verlet_history::ThreadTerminalState::Failed
        );
        assert!(
            death_join[0]
                .result_digest
                .as_deref()
                .is_some_and(|value| value.contains("remote child process"))
        );
    }

    #[tokio::test]
    async fn successful_spawn_turn_settles_once_and_later_submit_turn_cannot_rejoin() {
        let store = verlet_history_sqlite::SqliteSessionStore::in_memory()
            .await
            .unwrap();
        let request = request_fixture();
        let completed = child_record(
            &request,
            verlet_history::EventKind::TurnCompleted,
            "spawn-turn",
        );
        assert!(
            crate::daemon::remote_store::process_executor::settle_remote_terminal_record(
                store.clone(),
                request.clone(),
                completed
            )
            .await
            .unwrap()
        );
        let later = child_record(
            &request,
            verlet_history::EventKind::LoopDenied,
            "later-submit-turn",
        );
        assert!(
            !crate::daemon::remote_store::process_executor::settle_remote_terminal_record(
                store.clone(),
                request.clone(),
                later
            )
            .await
            .unwrap()
        );
        let joined = joined_payloads(&store, &request).await;
        assert_eq!(joined.len(), 1);
        assert_eq!(
            joined[0].terminal_state,
            verlet_history::ThreadTerminalState::Completed
        );
    }

    #[tokio::test]
    async fn detached_bootstrap_failure_settles_after_the_caller_is_cancelled() {
        let store = verlet_history_sqlite::SqliteSessionStore::in_memory()
            .await
            .unwrap();
        let authority = std::sync::Arc::new(
            crate::daemon::remote_store::lease::SqliteStreamLeaseAuthority::new(
                store.clone(),
                crate::daemon::remote_store::endpoint::VerletDaemonSyncConfig::default(),
                std::sync::Arc::new(crate::daemon::clock_route::SystemDaemonClock),
            )
            .await
            .unwrap(),
        );
        let child_root = std::env::temp_dir().join(format!(
            "verlet-remote-cancelled-bootstrap-{}",
            uuid::Uuid::now_v7()
        ));
        let executor =
            crate::daemon::remote_store::process_executor::ProcessRemoteThreadExecutor::new(
                store.clone(),
                authority,
                "http://127.0.0.1:1".to_string(),
                None,
                child_root.clone(),
                child_root.join("missing-verlet-executable"),
            )
            .await
            .unwrap();
        let request = request_fixture();
        let spawn_guard = executor.inner.spawn_lock.lock().await;
        let caller_executor = executor.clone();
        let caller_request = request.clone();
        let caller = tokio::spawn(async move { caller_executor.spawn(caller_request).await });
        tokio::task::yield_now().await;
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        drop(spawn_guard);

        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if !joined_payloads(&store, &request).await.is_empty() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("detached bootstrap failure did not settle the durable spawn");
        let joined = joined_payloads(&store, &request).await;
        assert_eq!(joined.len(), 1);
        assert_eq!(
            joined[0].terminal_state,
            verlet_history::ThreadTerminalState::Failed
        );
        assert!(
            joined[0]
                .result_digest
                .as_deref()
                .is_some_and(|reason| reason.contains("failed to start"))
        );
        let _ = std::fs::remove_dir_all(child_root);
    }
}
