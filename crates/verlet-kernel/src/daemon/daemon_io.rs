use rusqlite::OptionalExtension as _;
use sha2::Digest as _;
use subtle::ConstantTimeEq as _;
use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;
use verlet_history::EventStore as _;

const DEFAULT_QUEUE_BATCH: usize = 16;
const DEFAULT_WORKER_POLL_MS: u64 = 250;
const DEFAULT_EGRESS_PROJECTOR_POLL_MS: u64 = 250;
const MAX_HTTP_HEADER_BYTES: usize = 16 * 1024;
const MAX_HTTP_BODY_BYTES: usize = 4 * 1024 * 1024;
const MAX_TELEGRAM_WEBHOOK_REQUESTS: usize = 128;
const TELEGRAM_WEBHOOK_HEAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const TELEGRAM_WEBHOOK_REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
const HTTP_RESPONSE_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);
const MAX_TYPING_SIMULATION_DELAY: std::time::Duration = std::time::Duration::from_secs(8);
const EGRESS_SQLITE_BUSY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
const IO_EGRESS_PROJECTOR_DISCHARGED_BY: &str = "projector:io-egress";
const IO_EGRESS_PROJECTOR_FUNCTION: &str = "delivery/v1";
const ROUTE_AGENT_REF_METADATA: &str = "cooldis_route_agent_ref";
const INGRESS_MESSAGE_ID_FIELD: &str = "ingress_message_id";
const INGRESS_DEDUPE_SEEN_FIELD: &str = "dedupe_seen";

#[derive(Clone, Debug, Default)]
struct RouteEgressConfig {
    projection_rules: Vec<CompiledEgressProjectionRule>,
    typing_simulation: Option<crate::daemon::daemon_config::VerletTypingSimulationConfig>,
    retry: crate::daemon::daemon_config::VerletEgressRetryConfig,
    threading: Option<String>,
}

impl RouteEgressConfig {
    fn from_route(
        route: &crate::daemon::daemon_config::VerletIoRouteConfig,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let mut projection_rules = Vec::new();
        for (index, rule) in route.egress_projection.iter().enumerate() {
            projection_rules.push(CompiledEgressProjectionRule::compile(
                &route.id, index, rule,
            )?);
        }
        Ok(Self {
            projection_rules,
            typing_simulation: route.typing_simulation.clone(),
            retry: route.egress_retry,
            threading: route.threading.clone(),
        })
    }

    fn restores_per_conversation_bindings(&self) -> bool {
        self.threading.as_deref().unwrap_or("per_conversation") == "per_conversation"
    }

    fn project(
        &self,
        envelope: verlet_io_core::EgressEnvelope,
    ) -> Vec<verlet_io_core::EgressEnvelope> {
        if self.projection_rules.is_empty() {
            return vec![envelope];
        }

        let verlet_io_core::EgressKind::AssistantMessage { text } = &envelope.kind else {
            return vec![envelope];
        };
        let text = text.clone();
        let matches = self.projection_matches(&text);
        if matches.is_empty() {
            return vec![envelope];
        }

        let stripped_text = strip_projection_matches(&text, &matches);
        let has_silence = matches.iter().any(|matched| matched.action == "silence");
        let text_order = first_remaining_text_offset(&text, &matches);
        let mut projected = Vec::new();

        if !has_silence && !stripped_text.trim().is_empty() {
            let mut text_envelope = envelope.clone();
            text_envelope.kind = verlet_io_core::EgressKind::AssistantMessage {
                text: stripped_text,
            };
            projected.push(ProjectedEgress {
                order: text_order.unwrap_or(usize::MAX),
                tie_breaker: usize::MAX,
                envelope: text_envelope,
            });
        }

        for (index, matched) in matches.into_iter().enumerate() {
            let kind = if matched.action == "silence" {
                verlet_io_core::EgressKind::Silence {
                    reason: matched
                        .payload
                        .get("reason")
                        .and_then(serde_json::Value::as_str)
                        .map(ToOwned::to_owned),
                }
            } else {
                verlet_io_core::EgressKind::PlatformAction {
                    action: matched.action,
                    payload: matched.payload,
                }
            };
            projected.push(ProjectedEgress {
                order: matched.start,
                tie_breaker: index,
                envelope: sibling_egress(&envelope, kind),
            });
        }

        projected.sort_by_key(|projected| (projected.order, projected.tie_breaker));
        projected
            .into_iter()
            .map(|projected| projected.envelope)
            .collect()
    }

    fn projection_matches(&self, text: &str) -> Vec<ProjectionMatch> {
        let mut matches = Vec::new();
        for (rule_index, rule) in self.projection_rules.iter().enumerate() {
            for captures in rule.regex.captures_iter(text) {
                let Some(span) = captures.get(0) else {
                    continue;
                };
                matches.push(ProjectionMatch {
                    start: span.start(),
                    end: span.end(),
                    rule_index,
                    action: rule.action.clone(),
                    payload: projection_payload(rule, &captures),
                });
            }
        }

        matches.sort_by_key(|matched| (matched.start, matched.rule_index, matched.end));
        let mut accepted = Vec::new();
        let mut previous_end = 0;
        for matched in matches {
            if matched.start < previous_end {
                continue;
            }
            previous_end = matched.end;
            accepted.push(matched);
        }
        accepted
    }
}

#[derive(Clone, Debug)]
struct CompiledEgressProjectionRule {
    regex: regex::Regex,
    action: String,
}

impl CompiledEgressProjectionRule {
    fn compile(
        route_id: &str,
        index: usize,
        rule: &crate::daemon::daemon_config::VerletEgressProjectionRuleConfig,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        let regex = regex::Regex::new(&rule.pattern).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "io.routes.{route_id}.egress_projection[{index}].pattern invalid regex: {err}"
            ))
        })?;
        Ok(Self {
            regex,
            action: rule.action.trim().to_string(),
        })
    }
}

#[derive(Debug)]
struct ProjectionMatch {
    start: usize,
    end: usize,
    rule_index: usize,
    action: String,
    payload: serde_json::Value,
}

#[derive(Debug)]
struct ProjectedEgress {
    order: usize,
    tie_breaker: usize,
    envelope: verlet_io_core::EgressEnvelope,
}

#[derive(Clone, Debug)]
struct BoundEgressThread {
    route_id: String,
    scope_key: String,
    coordinates: verlet_runtime_contracts::ThreadCoordinates,
}

#[derive(Debug)]
enum ThreadHandleResolutionError {
    Lookup(crate::kernel::runtime_host::VerletError),
    LifecycleLoad(crate::kernel::runtime_host::VerletError),
}

impl ThreadHandleResolutionError {
    fn into_inner(self) -> crate::kernel::runtime_host::VerletError {
        match self {
            Self::Lookup(err) | Self::LifecycleLoad(err) => err,
        }
    }
}

/// Durable route state shared by ingress thread binding and egress projection.
///
/// `cooldis_daemon_egress_threads` keeps its historical name, but its bindings
/// serve both directions and are recovered by ingress during route startup.
#[derive(Clone)]
struct DaemonEgressState {
    connection: std::sync::Arc<std::sync::Mutex<rusqlite::Connection>>,
    dsn: std::sync::Arc<str>,
}

impl DaemonEgressState {
    fn connect(dsn: impl AsRef<str>) -> verlet_io_core::IoResult<Self> {
        let dsn: std::sync::Arc<str> = std::sync::Arc::from(dsn.as_ref());
        let connection = open_egress_state_connection(&dsn)?;
        init_egress_state_schema(&connection)?;
        Ok(Self {
            connection: std::sync::Arc::new(std::sync::Mutex::new(connection)),
            dsn,
        })
    }

    async fn run_blocking<T>(
        &self,
        operation: impl FnOnce(Self) -> verlet_io_core::IoResult<T> + Send + 'static,
    ) -> verlet_io_core::IoResult<T>
    where
        T: Send + 'static,
    {
        let state = self.clone();
        tokio::task::spawn_blocking(move || operation(state))
            .await
            .map_err(|err| {
                verlet_io_core::IoError::Queue(format!("join egress state operation: {err}"))
            })?
    }

    fn bind_thread(
        &self,
        route_id: &str,
        source_scope: &str,
        scope_key: &str,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_io_core::IoResult<()> {
        let connection = self.lock_connection()?;
        connection
            .execute(
                "INSERT INTO cooldis_daemon_egress_threads (
                    route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(route_id, thread_id) DO UPDATE SET
                    source_scope = excluded.source_scope,
                    scope_key = excluded.scope_key,
                    tenant_id = excluded.tenant_id,
                    user_id = excluded.user_id,
                    session_id = excluded.session_id,
                    updated_at_ms = excluded.updated_at_ms",
                rusqlite::params![
                    route_id,
                    source_scope,
                    scope_key,
                    coordinates.tenant_id,
                    coordinates.user_id,
                    coordinates.session_id,
                    coordinates.thread_id.to_string(),
                    now_ms() as i64
                ],
            )
            .map_err(egress_state_error)?;
        Ok(())
    }

    fn claim_ingress_thread_binding(
        &self,
        route_id: &str,
        source_scope: &str,
        scope_key: &str,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_io_core::IoResult<verlet_runtime_contracts::ThreadCoordinates> {
        let mut connection = self.lock_connection()?;
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(egress_state_error)?;
        let existing = tx
            .query_row(
                "SELECT tenant_id, user_id, session_id, thread_id
                 FROM cooldis_daemon_ingress_bindings
                 WHERE route_id = ?1 AND source_scope = ?2 AND scope_key = ?3
                 LIMIT 1",
                rusqlite::params![route_id, source_scope, scope_key],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                    ))
                },
            )
            .optional()
            .map_err(egress_state_error)?;
        let selected = match existing {
            Some((tenant_id, user_id, session_id, thread_id)) => {
                verlet_runtime_contracts::ThreadCoordinates {
                    tenant_id,
                    user_id,
                    session_id,
                    thread_id: verlet_runtime_contracts::ThreadId::parse_str(&thread_id).map_err(
                        |err| {
                            verlet_io_core::IoError::Queue(format!(
                                "invalid ingress binding thread id {thread_id:?}: {err}"
                            ))
                        },
                    )?,
                }
            }
            None => {
                tx.execute(
                    "INSERT INTO cooldis_daemon_ingress_bindings (
                        route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        route_id,
                        source_scope,
                        scope_key,
                        coordinates.tenant_id,
                        coordinates.user_id,
                        coordinates.session_id,
                        coordinates.thread_id.to_string(),
                        now_ms() as i64,
                    ],
                )
                .map_err(egress_state_error)?;
                tx.execute(
                    "INSERT INTO cooldis_daemon_egress_threads (
                        route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                    rusqlite::params![
                        route_id,
                        source_scope,
                        scope_key,
                        coordinates.tenant_id,
                        coordinates.user_id,
                        coordinates.session_id,
                        coordinates.thread_id.to_string(),
                        now_ms() as i64,
                    ],
                )
                .map_err(egress_state_error)?;
                coordinates.clone()
            }
        };
        tx.commit().map_err(egress_state_error)?;
        Ok(selected)
    }

    fn rebind_ingress_thread(
        &self,
        route_id: &str,
        source_scope: &str,
        scope_key: &str,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_io_core::IoResult<()> {
        let mut connection = self.lock_connection()?;
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(egress_state_error)?;
        tx.execute(
            "INSERT INTO cooldis_daemon_egress_threads (
                route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
             ON CONFLICT(route_id, thread_id) DO UPDATE SET
                source_scope = excluded.source_scope,
                scope_key = excluded.scope_key,
                tenant_id = excluded.tenant_id,
                user_id = excluded.user_id,
                session_id = excluded.session_id,
                updated_at_ms = excluded.updated_at_ms",
            rusqlite::params![
                route_id,
                source_scope,
                scope_key,
                coordinates.tenant_id,
                coordinates.user_id,
                coordinates.session_id,
                coordinates.thread_id.to_string(),
                now_ms() as i64,
            ],
        )
        .map_err(egress_state_error)?;
        tx
            .execute(
                "INSERT INTO cooldis_daemon_ingress_bindings (
                    route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
                 ON CONFLICT(route_id, source_scope, scope_key) DO UPDATE SET
                    tenant_id = excluded.tenant_id,
                    user_id = excluded.user_id,
                    session_id = excluded.session_id,
                    thread_id = excluded.thread_id,
                    updated_at_ms = excluded.updated_at_ms",
                rusqlite::params![
                    route_id,
                    source_scope,
                    scope_key,
                    coordinates.tenant_id,
                    coordinates.user_id,
                    coordinates.session_id,
                    coordinates.thread_id.to_string(),
                    now_ms() as i64,
                ],
            )
            .map_err(egress_state_error)?;
        tx.commit().map_err(egress_state_error)
    }

    fn clear_ingress_thread_binding_if_matches(
        &self,
        route_id: &str,
        source_scope: &str,
        scope_key: &str,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> verlet_io_core::IoResult<()> {
        let connection = self.lock_connection()?;
        connection
            .execute(
                "DELETE FROM cooldis_daemon_ingress_bindings
                 WHERE route_id = ?1 AND source_scope = ?2 AND scope_key = ?3 AND thread_id = ?4",
                rusqlite::params![route_id, source_scope, scope_key, thread_id.to_string()],
            )
            .map_err(egress_state_error)?;
        Ok(())
    }

    fn active_ingress_threads(
        &self,
        route_id: &str,
    ) -> verlet_io_core::IoResult<Vec<BoundEgressThread>> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT route_id, scope_key, tenant_id, user_id, session_id, thread_id
                 FROM cooldis_daemon_ingress_bindings
                 WHERE route_id = ?1
                 ORDER BY scope_key",
            )
            .map_err(egress_state_error)?;
        let rows = statement
            .query_map(rusqlite::params![route_id], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                ))
            })
            .map_err(egress_state_error)?;
        let mut bindings = Vec::new();
        for row in rows {
            let (route_id, scope_key, tenant_id, user_id, session_id, thread_id) =
                row.map_err(egress_state_error)?;
            bindings.push(BoundEgressThread {
                route_id,
                scope_key,
                coordinates: verlet_runtime_contracts::ThreadCoordinates {
                    tenant_id,
                    user_id,
                    session_id,
                    thread_id: verlet_runtime_contracts::ThreadId::parse_str(&thread_id).map_err(
                        |err| {
                            verlet_io_core::IoError::Queue(format!(
                                "invalid active ingress thread id {thread_id:?}: {err}"
                            ))
                        },
                    )?,
                },
            });
        }
        Ok(bindings)
    }

    fn bound_threads(&self, route_id: &str) -> verlet_io_core::IoResult<Vec<BoundEgressThread>> {
        let connection = self.lock_connection()?;
        let mut statement = connection
            .prepare(
                "SELECT route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id
                 FROM cooldis_daemon_egress_threads
                 WHERE route_id = ?1
                 ORDER BY updated_at_ms, rowid",
            )
            .map_err(egress_state_error)?;
        let rows = statement
            .query_map(rusqlite::params![route_id], |row| {
                let thread_id: String = row.get(6)?;
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?,
                    row.get::<_, String>(4)?,
                    row.get::<_, String>(5)?,
                    thread_id,
                ))
            })
            .map_err(egress_state_error)?;

        let mut bindings = Vec::new();
        for row in rows {
            let (route_id, scope_key, tenant_id, user_id, session_id, thread_id) =
                row.map_err(egress_state_error)?;
            let thread_id =
                verlet_runtime_contracts::ThreadId::parse_str(&thread_id).map_err(|err| {
                    verlet_io_core::IoError::Queue(format!(
                        "invalid egress thread id {thread_id:?}: {err}"
                    ))
                })?;
            bindings.push(BoundEgressThread {
                route_id,
                scope_key,
                coordinates: verlet_runtime_contracts::ThreadCoordinates {
                    tenant_id,
                    user_id,
                    session_id,
                    thread_id,
                },
            });
        }
        Ok(bindings)
    }

    fn cursor(
        &self,
        route_id: &str,
        thread_id: &str,
    ) -> verlet_io_core::IoResult<Option<verlet_history::StreamCursorV1>> {
        let connection = self.lock_connection()?;
        let cursor_json = connection
            .query_row(
                "SELECT cursor_json
                 FROM cooldis_daemon_egress_cursors
                 WHERE route_id = ?1 AND thread_id = ?2",
                rusqlite::params![route_id, thread_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(egress_state_error)?;
        cursor_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|err| {
                    verlet_io_core::IoError::Queue(format!("decode egress cursor: {err}"))
                })
            })
            .transpose()
    }

    fn store_cursor(
        &self,
        route_id: &str,
        thread_id: &str,
        cursor: &verlet_history::StreamCursorV1,
    ) -> verlet_io_core::IoResult<()> {
        let connection = self.lock_connection()?;
        let current_json = connection
            .query_row(
                "SELECT cursor_json
                 FROM cooldis_daemon_egress_cursors
                 WHERE route_id = ?1 AND thread_id = ?2",
                rusqlite::params![route_id, thread_id],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(egress_state_error)?;
        if let Some(current_json) = current_json {
            let current: verlet_history::StreamCursorV1 = serde_json::from_str(&current_json)
                .map_err(|err| {
                    verlet_io_core::IoError::Queue(format!("decode egress cursor: {err}"))
                })?;
            if current.stream_id == cursor.stream_id
                && current.sequence.get() >= cursor.sequence.get()
            {
                return Ok(());
            }
        }
        let cursor_json = serde_json::to_string(cursor).map_err(|err| {
            verlet_io_core::IoError::Queue(format!("encode egress cursor: {err}"))
        })?;
        connection
            .execute(
                "INSERT INTO cooldis_daemon_egress_cursors (
                    route_id, thread_id, cursor_json, updated_at_ms
                 )
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(route_id, thread_id) DO UPDATE SET
                    cursor_json = excluded.cursor_json,
                    updated_at_ms = excluded.updated_at_ms",
                rusqlite::params![route_id, thread_id, cursor_json, now_ms() as i64],
            )
            .map_err(egress_state_error)?;
        Ok(())
    }

    fn replace_cursor(
        &self,
        route_id: &str,
        thread_id: &str,
        cursor: &verlet_history::StreamCursorV1,
    ) -> verlet_io_core::IoResult<()> {
        let cursor_json = serde_json::to_string(cursor).map_err(|err| {
            verlet_io_core::IoError::Queue(format!("encode egress cursor: {err}"))
        })?;
        self.lock_connection()?
            .execute(
                "INSERT INTO cooldis_daemon_egress_cursors (
                    route_id, thread_id, cursor_json, updated_at_ms
                 )
                 VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(route_id, thread_id) DO UPDATE SET
                    cursor_json = excluded.cursor_json,
                    updated_at_ms = excluded.updated_at_ms",
                rusqlite::params![route_id, thread_id, cursor_json, now_ms() as i64],
            )
            .map_err(egress_state_error)?;
        Ok(())
    }

    fn push_dead_letter(&self, dead_letter: &EgressDeadLetter) -> verlet_io_core::IoResult<()> {
        let envelope_json = serde_json::to_string(&dead_letter.envelope).map_err(|err| {
            verlet_io_core::IoError::Queue(format!("encode dead-letter envelope: {err}"))
        })?;
        let connection = self.lock_connection()?;
        connection
            .execute(
                "INSERT INTO cooldis_daemon_egress_dead_letters (
                    id, route_id, thread_id, source_event_id, envelope_index, dedupe_key,
                    egress_kind, attempts, error, envelope_json, created_at_ms
                 )
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
                rusqlite::params![
                    format!("dead-{}", uuid::Uuid::now_v7()),
                    dead_letter.route_id,
                    dead_letter.thread_id,
                    dead_letter.source_event_id,
                    dead_letter.envelope_index as i64,
                    dead_letter.dedupe_key,
                    dead_letter.egress_kind,
                    dead_letter.attempts as i64,
                    dead_letter.error,
                    envelope_json,
                    now_ms() as i64,
                ],
            )
            .map_err(egress_state_error)?;
        Ok(())
    }

    fn dead_letter_count(&self, route_id: &str) -> verlet_io_core::IoResult<usize> {
        let connection = self.lock_connection()?;
        let count = connection
            .query_row(
                "SELECT COUNT(*)
                 FROM cooldis_daemon_egress_dead_letters
                 WHERE route_id = ?1",
                rusqlite::params![route_id],
                |row| row.get::<_, i64>(0),
            )
            .map_err(egress_state_error)?;
        Ok(count.max(0) as usize)
    }

    fn record_ingress_ownership(
        &self,
        keys: &[IngressOwnershipKey],
        stream_id: &verlet_history::EventStreamId,
        attempt: u32,
    ) -> verlet_io_core::IoResult<String> {
        let ownership_id = uuid::Uuid::now_v7().to_string();
        let mut connection = self.lock_connection()?;
        let tx = connection
            .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)
            .map_err(egress_state_error)?;
        for key in keys {
            tx.execute(
                "INSERT INTO cooldis_daemon_ingress_ownership (
                    dedupe_key, ownership_id, ingress_envelope_id, stream_id, attempt, created_at_ms
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![
                    key.dedupe_key,
                    ownership_id,
                    key.ingress_envelope_id,
                    stream_id.to_string(),
                    i64::from(attempt),
                    now_ms() as i64,
                ],
            )
            .map_err(egress_state_error)?;
        }
        tx.commit().map_err(egress_state_error)?;
        Ok(ownership_id)
    }

    fn ingress_ownership_streams(
        &self,
        keys: &[IngressOwnershipKey],
    ) -> verlet_io_core::IoResult<Vec<verlet_history::EventStreamId>> {
        let connection = self.lock_connection()?;
        ingress_ownership_streams_from(&connection, keys)
    }

    async fn lock_ingress_claim_admission(
        &self,
    ) -> verlet_io_core::IoResult<IngressClaimAdmissionLock> {
        let dsn = std::sync::Arc::clone(&self.dsn);
        tokio::task::spawn_blocking(move || {
            if sqlite_path_from_dsn(&dsn)? == std::path::Path::new(":memory:") {
                return Err(verlet_io_core::IoError::Queue(
                    "durable ingress ownership requires a file-backed sqlite state store"
                        .to_string(),
                ));
            }
            let connection = open_egress_state_connection(&dsn)?;
            connection
                .execute_batch("BEGIN IMMEDIATE")
                .map_err(egress_state_error)?;
            Ok(IngressClaimAdmissionLock {
                connection: Some(connection),
            })
        })
        .await
        .map_err(|err| {
            verlet_io_core::IoError::Queue(format!("join ingress claim admission lock: {err}"))
        })?
    }

    fn lock_connection(
        &self,
    ) -> verlet_io_core::IoResult<std::sync::MutexGuard<'_, rusqlite::Connection>> {
        self.connection.lock().map_err(|err| {
            verlet_io_core::IoError::Queue(format!("egress state lock poisoned: {err}"))
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct IngressOwnershipKey {
    dedupe_key: String,
    ingress_envelope_id: String,
}

#[derive(Clone)]
struct IngressOwnershipReservation {
    state: std::sync::Arc<DaemonEgressState>,
    ownership_id: String,
    keys: Vec<IngressOwnershipKey>,
    stream_id: verlet_history::EventStreamId,
}

struct IngressClaimAdmissionLock {
    connection: Option<rusqlite::Connection>,
}

impl IngressClaimAdmissionLock {
    fn ownership_streams(
        &self,
        keys: &[IngressOwnershipKey],
    ) -> verlet_io_core::IoResult<Vec<verlet_history::EventStreamId>> {
        ingress_ownership_streams_from(
            self.connection
                .as_ref()
                .expect("claim admission lock connection"),
            keys,
        )
    }

    fn reservation_is_current(
        &self,
        reservation: &IngressOwnershipReservation,
    ) -> verlet_io_core::IoResult<bool> {
        let connection = self
            .connection
            .as_ref()
            .expect("claim admission lock connection");
        for key in &reservation.keys {
            let current = connection
                .query_row(
                    "SELECT ownership_id, stream_id
                     FROM cooldis_daemon_ingress_ownership
                     WHERE dedupe_key = ?1
                     ORDER BY attempt DESC, created_at_ms DESC, ownership_id DESC
                     LIMIT 1",
                    rusqlite::params![key.dedupe_key],
                    |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
                )
                .optional()
                .map_err(egress_state_error)?;
            if current.as_ref()
                != Some(&(
                    reservation.ownership_id.clone(),
                    reservation.stream_id.to_string(),
                ))
            {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn prune_to_reservation(
        &self,
        reservation: &IngressOwnershipReservation,
    ) -> verlet_io_core::IoResult<()> {
        let connection = self
            .connection
            .as_ref()
            .expect("claim admission lock connection");
        for key in &reservation.keys {
            connection
                .execute(
                    "DELETE FROM cooldis_daemon_ingress_ownership
                     WHERE dedupe_key = ?1 AND ownership_id <> ?2",
                    rusqlite::params![key.dedupe_key, reservation.ownership_id],
                )
                .map_err(egress_state_error)?;
        }
        Ok(())
    }

    fn prune_to_stream(
        &self,
        keys: &[IngressOwnershipKey],
        stream_id: &verlet_history::EventStreamId,
    ) -> verlet_io_core::IoResult<()> {
        let connection = self
            .connection
            .as_ref()
            .expect("claim admission lock connection");
        for key in keys {
            connection
                .execute(
                    "DELETE FROM cooldis_daemon_ingress_ownership
                     WHERE dedupe_key = ?1
                       AND ownership_id <> (
                           SELECT ownership_id
                           FROM cooldis_daemon_ingress_ownership
                           WHERE dedupe_key = ?1 AND stream_id = ?2
                           ORDER BY attempt ASC, created_at_ms ASC, ownership_id ASC
                           LIMIT 1
                       )",
                    rusqlite::params![key.dedupe_key, stream_id.to_string()],
                )
                .map_err(egress_state_error)?;
        }
        Ok(())
    }

    fn commit(mut self) -> verlet_io_core::IoResult<()> {
        self.connection
            .as_ref()
            .expect("claim admission lock connection")
            .execute_batch("COMMIT")
            .map_err(egress_state_error)?;
        self.connection.take();
        Ok(())
    }
}

impl Drop for IngressClaimAdmissionLock {
    fn drop(&mut self) {
        if let Some(connection) = self.connection.take() {
            let _ = connection.execute_batch("ROLLBACK");
        }
    }
}

fn ingress_ownership_streams_from(
    connection: &rusqlite::Connection,
    keys: &[IngressOwnershipKey],
) -> verlet_io_core::IoResult<Vec<verlet_history::EventStreamId>> {
    let mut streams = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for key in keys {
        let mut statement = connection
            .prepare(
                "SELECT stream_id FROM cooldis_daemon_ingress_ownership
                 WHERE dedupe_key = ?1
                 ORDER BY attempt DESC, created_at_ms DESC, ownership_id DESC",
            )
            .map_err(egress_state_error)?;
        let rows = statement
            .query_map(rusqlite::params![key.dedupe_key], |row| {
                row.get::<_, String>(0)
            })
            .map_err(egress_state_error)?;
        for row in rows {
            let stream = row.map_err(egress_state_error)?;
            if seen.insert(stream.clone()) {
                streams.push(verlet_history::EventStreamId::new(stream));
            }
        }
    }
    Ok(streams)
}

#[derive(Clone, Debug)]
struct EgressDeadLetter {
    route_id: String,
    thread_id: String,
    source_event_id: String,
    envelope_index: usize,
    dedupe_key: String,
    egress_kind: String,
    attempts: u32,
    error: String,
    envelope: verlet_io_core::EgressEnvelope,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
struct IngressReceiptContext {
    target: verlet_io_core::IoTarget,
    metadata: std::collections::BTreeMap<String, String>,
    source_ingress_id: Option<String>,
    turn_id: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct DrainEgressSource {
    id: verlet_history::EventRecordId,
    cursor: verlet_history::StreamCursorV1,
}

impl DrainEgressSource {
    fn from_event(event: &verlet_history::EventRecord) -> Self {
        Self {
            id: event.id,
            cursor: event.cursor_v1(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct RequestedEgressTemplate {
    target: verlet_io_core::IoTarget,
    kind: verlet_io_core::EgressKind,
    source_ingress_id: Option<String>,
    metadata: std::collections::BTreeMap<String, String>,
}

impl RequestedEgressTemplate {
    fn envelope(&self) -> verlet_io_core::EgressEnvelope {
        let mut envelope =
            verlet_io_core::EgressEnvelope::new(self.target.clone(), self.kind.clone(), now_ms());
        envelope.source_ingress_id = self.source_ingress_id.clone();
        envelope.metadata = self.metadata.clone();
        envelope
    }
}

#[derive(Clone, Debug, PartialEq)]
enum DrainEgressWork {
    Advance {
        source: DrainEgressSource,
    },
    Requested {
        source: DrainEgressSource,
        template: RequestedEgressTemplate,
    },
    Assistant {
        source: DrainEgressSource,
        context: IngressReceiptContext,
        text: String,
    },
}

impl DrainEgressWork {
    fn source(&self) -> &DrainEgressSource {
        match self {
            Self::Advance { source }
            | Self::Requested { source, .. }
            | Self::Assistant { source, .. } => source,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum DrainIngressContextEvent {
    Ingress(verlet_history::EventRecordId),
    Session {
        source: DrainEgressSource,
        entry_id: String,
    },
}

/// In-memory projection of the egress-relevant portion of one route/thread stream.
///
/// The view caches its stream fold position, terminal receipt dedupe cursors,
/// ingress contexts and their session-order fold, plus delivery work that has not
/// reached a terminal receipt. It is dropped and rebuilt from one full replay when
/// missing or when either cursor no longer verifies against the stream. At every
/// fold boundary its state must equal folding the same records from sequence one;
/// no field is durable, and no full event record is retained here.
#[derive(Clone, Debug, PartialEq)]
struct DrainEgressView {
    fold_position: Option<verlet_history::StreamCursorV1>,
    observed_delivery_cursor: Option<verlet_history::StreamCursorV1>,
    effective_delivery_cursor: Option<verlet_history::StreamCursorV1>,
    receipt_dedupe_cursors: std::collections::HashMap<String, ReceiptDedupeCursor>,
    ingress_contexts:
        std::collections::HashMap<verlet_history::EventRecordId, IngressReceiptContext>,
    pending_contexts: Vec<IngressReceiptContext>,
    active_context: Option<IngressReceiptContext>,
    context_events: Vec<DrainIngressContextEvent>,
    visible_session_entry_ids: std::collections::HashSet<String>,
    unresolved_session_entry_ids: std::collections::HashSet<String>,
    undelivered_requested_egress: std::collections::VecDeque<DrainEgressWork>,
}

impl DrainEgressView {
    fn new(
        observed_delivery_cursor: Option<verlet_history::StreamCursorV1>,
        effective_delivery_cursor: Option<verlet_history::StreamCursorV1>,
    ) -> Self {
        Self {
            fold_position: None,
            observed_delivery_cursor,
            effective_delivery_cursor,
            receipt_dedupe_cursors: std::collections::HashMap::new(),
            ingress_contexts: std::collections::HashMap::new(),
            pending_contexts: Vec::new(),
            active_context: None,
            context_events: Vec::new(),
            visible_session_entry_ids: std::collections::HashSet::new(),
            unresolved_session_entry_ids: std::collections::HashSet::new(),
            undelivered_requested_egress: std::collections::VecDeque::new(),
        }
    }
}

enum IngressClaimAppend {
    Appended(verlet_history::EventRecord),
    Existing(IngressOutcomeState),
}

#[derive(Clone, Debug)]
enum IngressOutcomeState {
    Missing,
    Claimed {
        claim: verlet_history::EventRecord,
        payload: verlet_history::IoIngressClaimedPayload,
    },
    Settled {
        claim_payload: verlet_history::IoIngressClaimedPayload,
        settle: verlet_history::EventRecord,
    },
}

#[derive(Clone)]
pub struct VerletDaemonIoBridge {
    app_server: Option<crate::adapters::app_server::VerletAppServer>,
    supervisor: crate::kernel::supervisor::VerletSupervisor,
    tenant_id: String,
    user_id: String,
    model: String,
    model_provider: String,
    cwd: std::path::PathBuf,
    session_store_path: Option<std::path::PathBuf>,
    lease_epoch: u64,
    threads: std::sync::Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, verlet_runtime_contracts::ThreadCoordinates>,
        >,
    >,
    thread_scope_locks: std::sync::Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<String, std::sync::Arc<tokio::sync::Mutex<()>>>,
        >,
    >,
    thread_load_locks: std::sync::Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<
                verlet_runtime_contracts::ThreadId,
                std::sync::Arc<tokio::sync::Mutex<()>>,
            >,
        >,
    >,
    active_turns: std::sync::Arc<std::sync::Mutex<std::collections::HashMap<String, String>>>,
    egress_adapters: std::sync::Arc<
        tokio::sync::RwLock<
            std::collections::HashMap<String, std::sync::Arc<dyn verlet_io_core::EgressAdapter>>,
        >,
    >,
    egress_route_configs:
        std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<String, RouteEgressConfig>>>,
    egress_states: std::sync::Arc<
        tokio::sync::RwLock<std::collections::HashMap<String, std::sync::Arc<DaemonEgressState>>>,
    >,
    egress_drain_views: std::sync::Arc<
        tokio::sync::Mutex<
            std::collections::HashMap<
                (String, String),
                std::sync::Arc<tokio::sync::Mutex<Option<DrainEgressView>>>,
            >,
        >,
    >,
    #[cfg(test)]
    pause_after_ingress_claim: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    ingress_claim_paused: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(test)]
    pause_after_ingress_ownership: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    ingress_ownership_paused: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(test)]
    pause_after_fork_creation: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    fork_creation_paused: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(test)]
    pause_after_fork_spawn: std::sync::Arc<std::sync::atomic::AtomicBool>,
    #[cfg(test)]
    fork_spawn_paused: std::sync::Arc<tokio::sync::Notify>,
    #[cfg(test)]
    thread_load_root_barrier:
        std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>>,
    #[cfg(test)]
    ingress_binding_barrier:
        std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>>,
    #[cfg(test)]
    initial_root_candidates:
        std::sync::Arc<std::sync::Mutex<Vec<verlet_runtime_contracts::ThreadCoordinates>>>,
    #[cfg(test)]
    fork_claim_scan_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

impl VerletDaemonIoBridge {
    pub fn new(
        supervisor: crate::kernel::supervisor::VerletSupervisor,
        tenant_id: impl Into<String>,
        user_id: impl Into<String>,
        model_provider: impl Into<String>,
        model: impl Into<String>,
        cwd: impl Into<std::path::PathBuf>,
    ) -> Self {
        Self {
            app_server: None,
            supervisor,
            tenant_id: tenant_id.into(),
            user_id: user_id.into(),
            model: model.into(),
            model_provider: model_provider.into(),
            cwd: cwd.into(),
            session_store_path: None,
            lease_epoch: 0,
            threads: std::sync::Arc::new(tokio::sync::Mutex::new(std::collections::HashMap::new())),
            thread_scope_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            thread_load_locks: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            active_turns: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            egress_adapters: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            egress_route_configs: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            egress_states: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            egress_drain_views: std::sync::Arc::new(tokio::sync::Mutex::new(
                std::collections::HashMap::new(),
            )),
            #[cfg(test)]
            pause_after_ingress_claim: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
            #[cfg(test)]
            ingress_claim_paused: std::sync::Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            pause_after_ingress_ownership: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
            #[cfg(test)]
            ingress_ownership_paused: std::sync::Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            pause_after_fork_creation: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            )),
            #[cfg(test)]
            fork_creation_paused: std::sync::Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            pause_after_fork_spawn: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
            #[cfg(test)]
            fork_spawn_paused: std::sync::Arc::new(tokio::sync::Notify::new()),
            #[cfg(test)]
            thread_load_root_barrier: std::sync::Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            ingress_binding_barrier: std::sync::Arc::new(std::sync::Mutex::new(None)),
            #[cfg(test)]
            initial_root_candidates: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
            #[cfg(test)]
            fork_claim_scan_count: std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }
    }

    pub fn from_app_server(server: &crate::adapters::app_server::VerletAppServer) -> Self {
        let mut bridge = Self::new(
            server.supervisor(),
            server.tenant_id().to_string(),
            server.user_id().to_string(),
            server.model_provider().to_string(),
            server.model().to_string(),
            server.cwd().to_path_buf(),
        );
        bridge.app_server = Some(server.clone());
        bridge.session_store_path = Some(server.session_store_path().to_path_buf());
        bridge.lease_epoch = server.lease_epoch();
        bridge
    }

    #[cfg(test)]
    pub(crate) fn ingress_binding_barrier(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>> {
        std::sync::Arc::clone(&self.ingress_binding_barrier)
    }

    #[cfg(test)]
    pub(crate) fn pause_after_ingress_claim(
        &self,
    ) -> (
        std::sync::Arc<std::sync::atomic::AtomicBool>,
        std::sync::Arc<tokio::sync::Notify>,
    ) {
        (
            std::sync::Arc::clone(&self.pause_after_ingress_claim),
            std::sync::Arc::clone(&self.ingress_claim_paused),
        )
    }

    #[cfg(test)]
    pub(crate) fn thread_load_root_barrier(
        &self,
    ) -> std::sync::Arc<std::sync::Mutex<Option<std::sync::Arc<tokio::sync::Barrier>>>> {
        std::sync::Arc::clone(&self.thread_load_root_barrier)
    }

    pub fn direct_sink(&self) -> std::sync::Arc<dyn verlet_io_core::IngressSink> {
        std::sync::Arc::new(DirectRuntimeIngressSink::new(self.clone()))
    }

    pub(crate) fn route_identity(&self) -> (&str, &str) {
        (&self.tenant_id, &self.user_id)
    }

    pub async fn register_egress_adapter(
        &self,
        protocol: impl Into<String>,
        instance_id: impl Into<String>,
        adapter: std::sync::Arc<dyn verlet_io_core::EgressAdapter>,
    ) {
        let protocol = protocol.into();
        let instance_id = instance_id.into();
        self.egress_adapters
            .write()
            .await
            .insert(source_scope(&protocol, &instance_id), adapter);
    }

    pub async fn register_egress_route_config(
        &self,
        protocol: impl Into<String>,
        instance_id: impl Into<String>,
        route: &crate::daemon::daemon_config::VerletIoRouteConfig,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let protocol = protocol.into();
        let instance_id = instance_id.into();
        let config = RouteEgressConfig::from_route(route)?;
        self.egress_route_configs
            .write()
            .await
            .insert(source_scope(&protocol, &instance_id), config);
        Ok(())
    }

    pub async fn validate_route_agent_ref(
        &self,
        route: &crate::daemon::daemon_config::VerletIoRouteConfig,
    ) -> crate::kernel::runtime_host::VerletResult<()> {
        let Some(agent_ref) = route.agent_ref.as_deref() else {
            return Ok(());
        };
        let app_server = self.app_server.as_ref().ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "io.routes.{}.agent_ref requires daemon IO to be backed by an app-server",
                route.id
            ))
        })?;
        app_server
            .validate_daemon_route_agent_ref(agent_ref)
            .await
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "io.routes.{}.agent_ref {agent_ref:?} did not bind from agent registry root {}: {err}. Publish the agent with `verlet agent publish --registry-root {}` before starting the daemon.",
                    route.id,
                    app_server.agent_registry_root().display(),
                    app_server.agent_registry_root().display()
                ))
            })
    }

    pub async fn register_egress_state_sqlite_dsn(
        &self,
        protocol: impl Into<String>,
        instance_id: impl Into<String>,
        dsn: impl AsRef<str>,
    ) -> verlet_io_core::IoResult<()> {
        let protocol = protocol.into();
        let instance_id = instance_id.into();
        let key = source_scope(&protocol, &instance_id);
        let state = std::sync::Arc::new(DaemonEgressState::connect(dsn)?);
        let restores_per_conversation_bindings = self
            .egress_route_configs
            .read()
            .await
            .get(&key)
            .is_some_and(RouteEgressConfig::restores_per_conversation_bindings);
        let bindings = if restores_per_conversation_bindings {
            self.ingress_bindings(&state, &instance_id)?
        } else {
            Vec::new()
        };

        // Publish the recovered map and its backing state together. A configured
        // per-conversation route cannot create a thread until the state appears,
        // and state readers remain blocked until the map seed is visible.
        let mut states = self.egress_states.write().await;
        if restores_per_conversation_bindings {
            self.threads.lock().await.extend(bindings);
        }
        states.insert(key, state);
        Ok(())
    }

    /// Loads durable ingress bindings for the startup hot-path map seed.
    ///
    /// Runtime handles are loaded lazily on first ingress or egress use.
    fn ingress_bindings(
        &self,
        state: &DaemonEgressState,
        route_id: &str,
    ) -> verlet_io_core::IoResult<Vec<(String, verlet_runtime_contracts::ThreadCoordinates)>> {
        Ok(state
            .active_ingress_threads(route_id)?
            .into_iter()
            .map(|binding| (binding.scope_key, binding.coordinates))
            .collect())
    }

    pub async fn start_egress_projector_sqlite_dsn(
        &self,
        protocol: impl Into<String>,
        instance_id: impl Into<String>,
        dsn: impl AsRef<str>,
    ) -> verlet_io_core::IoResult<tokio::task::JoinHandle<()>> {
        let protocol = protocol.into();
        let instance_id = instance_id.into();
        self.register_egress_state_sqlite_dsn(&protocol, &instance_id, dsn)
            .await?;
        let bridge = self.clone();
        Ok(tokio::spawn(async move {
            bridge.run_egress_projector(protocol, instance_id).await;
        }))
    }

    pub async fn drain_egress_once(
        &self,
        protocol: &str,
        instance_id: &str,
    ) -> verlet_io_core::IoResult<usize> {
        let key = source_scope(protocol, instance_id);
        let state = self.egress_states.read().await.get(&key).cloned();
        let Some(state) = state else {
            self.egress_drain_views
                .lock()
                .await
                .retain(|(route_key, _), slot| {
                    route_key != &key || std::sync::Arc::strong_count(slot) > 1
                });
            return Ok(0);
        };
        let route_config = self
            .egress_route_configs
            .read()
            .await
            .get(&key)
            .cloned()
            .unwrap_or_default();
        let adapter = self.egress_adapters.read().await.get(&key).cloned();
        let bindings = state.bound_threads(instance_id)?;
        let bound_thread_ids = bindings
            .iter()
            .map(|binding| binding.coordinates.thread_id.to_string())
            .collect::<std::collections::HashSet<_>>();
        self.egress_drain_views
            .lock()
            .await
            .retain(|(route_key, thread_id), slot| {
                route_key != &key
                    || bound_thread_ids.contains(thread_id)
                    || std::sync::Arc::strong_count(slot) > 1
            });
        let mut delivered_sources = 0;
        for binding in bindings {
            let handle = match self.bound_thread_handle(&binding).await {
                Ok(handle) => handle,
                Err(_) => continue,
            };
            delivered_sources += self
                .drain_thread_egress(
                    &key,
                    &state,
                    &binding,
                    handle,
                    adapter.as_deref(),
                    &route_config,
                )
                .await?;
        }
        Ok(delivered_sources)
    }

    async fn bound_thread_handle(
        &self,
        binding: &BoundEgressThread,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        self.get_or_load_thread_handle(&binding.coordinates)
            .await
            .map_err(ThreadHandleResolutionError::into_inner)
    }

    /// Resolves a resident runtime thread or lazily rehydrates it from its
    /// durable coordinates and session history after a process restart.
    /// Concurrent ingress and egress callers serialize the load by thread id
    /// so only a fully initialized winner can be observed through this bridge.
    async fn get_or_load_thread_handle(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> Result<crate::kernel::runtime_host::RuntimeThreadHandle, ThreadHandleResolutionError> {
        self.get_or_load_thread_handle_inner(coordinates, std::collections::HashSet::new())
            .await
    }

    fn get_or_load_thread_handle_inner<'a>(
        &'a self,
        coordinates: &'a verlet_runtime_contracts::ThreadCoordinates,
        mut loading: std::collections::HashSet<verlet_runtime_contracts::ThreadId>,
    ) -> futures_util::future::BoxFuture<
        'a,
        Result<crate::kernel::runtime_host::RuntimeThreadHandle, ThreadHandleResolutionError>,
    > {
        Box::pin(async move {
            if !loading.insert(coordinates.thread_id) {
                return Err(ThreadHandleResolutionError::LifecycleLoad(
                    crate::kernel::runtime_host::VerletError::History(format!(
                        "thread lifecycle topology cycle while lazily loading {}",
                        coordinates.thread_id
                    )),
                ));
            }
            loop {
                match self.supervisor.get_thread_at(coordinates).await {
                    Ok(handle) => return Ok(handle),
                    Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(_)) => {
                        let reconstructed = self
                            .reconstruct_thread_lifecycle(coordinates)
                            .await
                            .map_err(ThreadHandleResolutionError::LifecycleLoad)?;
                        if let Some(lifecycle) = &reconstructed {
                            #[cfg(test)]
                            if loading.len() == 1 {
                                let barrier = self
                                    .thread_load_root_barrier
                                    .lock()
                                    .unwrap_or_else(|err| err.into_inner())
                                    .clone();
                                if let Some(barrier) = barrier {
                                    barrier.wait().await;
                                }
                            }
                            let mut seen_related = std::collections::HashSet::new();
                            for related_thread_id in lifecycle
                                .topology
                                .related_thread_ids()
                                .into_iter()
                                .filter(|thread_id| seen_related.insert(*thread_id))
                            {
                                let related_coordinates =
                                    verlet_runtime_contracts::ThreadCoordinates {
                                        tenant_id: coordinates.tenant_id.clone(),
                                        user_id: coordinates.user_id.clone(),
                                        session_id: coordinates.session_id.clone(),
                                        thread_id: related_thread_id,
                                    };
                                self.get_or_load_thread_handle_inner(
                                    &related_coordinates,
                                    loading.clone(),
                                )
                                .await?;
                            }
                        }

                        let load_lock = self.thread_load_lock(coordinates.thread_id).await;
                        let _load_guard = load_lock.lock().await;
                        if let Ok(handle) = self.supervisor.get_thread_at(coordinates).await {
                            return Ok(handle);
                        }
                        let mut lifecycle = match reconstructed {
                            Some(lifecycle) => lifecycle,
                            None => {
                                match self
                                    .reconstruct_thread_lifecycle(coordinates)
                                    .await
                                    .map_err(ThreadHandleResolutionError::LifecycleLoad)?
                                {
                                    Some(_) => continue,
                                    None => {
                                        let now = now_ms();
                                        verlet_runtime_contracts::ThreadLifecycleRecord {
                                            coordinates: coordinates.clone(),
                                            parent_thread_id: None,
                                            topology: verlet_runtime_contracts::ThreadTopology::root(),
                                            status: verlet_runtime_contracts::ThreadLifecycleStatus::Idle,
                                            latest_signal_id: None,
                                            latest_checkpoint_id: None,
                                            created_at_ms: now,
                                            updated_at_ms: now,
                                            metadata: std::collections::BTreeMap::new(),
                                        }
                                    }
                                }
                            }
                        };
                        if let Some(app_server) = &self.app_server {
                            app_server
                                .rehydrate_loaded_manifest_thread_metadata(&mut lifecycle)
                                .await
                                .map_err(ThreadHandleResolutionError::LifecycleLoad)?;
                        } else {
                            crate::adapters::app_server::threads::validate_loaded_manifest_thread_history(
                                &self.supervisor,
                                &mut lifecycle,
                            )
                            .await
                            .map_err(ThreadHandleResolutionError::LifecycleLoad)?;
                        }
                        match self.supervisor.load_thread_from_lifecycle(lifecycle).await {
                            Ok(handle) => return Ok(handle),
                            Err(crate::kernel::runtime_host::VerletError::ThreadAlreadyExists(
                                _,
                            )) => {
                                self.supervisor
                                    .wait_for_thread_start_reservation(
                                        &coordinates.tenant_id,
                                        coordinates.thread_id,
                                    )
                                    .await
                                    .map_err(ThreadHandleResolutionError::Lookup)?;
                            }
                            Err(err) => {
                                return Err(ThreadHandleResolutionError::LifecycleLoad(err));
                            }
                        }
                    }
                    Err(err) => return Err(ThreadHandleResolutionError::Lookup(err)),
                }
            }
        })
    }

    /// EMO-370 seam: reconstruct a lazily loaded thread's lifecycle record
    /// from its own journal: thread-start provenance (topology, parent,
    /// metadata) plus the manifest compile/bind receipts recorded at
    /// creation. The stream is the only durable truth; the binding table
    /// stays a coordinates-only read model, and identity is never
    /// re-resolved from the route's current agent alias (an `@latest` alias
    /// may have moved).
    ///
    /// Returns `Ok(None)` when the journal has no complete start payload. A
    /// route-reserved root currently enters through that path and is rebuilt
    /// in memory without mutating the stream; lazy resume remains a read-only
    /// fold of durable history.
    async fn reconstruct_thread_lifecycle(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> crate::kernel::runtime_host::VerletResult<
        Option<verlet_runtime_contracts::ThreadLifecycleRecord>,
    > {
        let store = self
            .supervisor
            .runtime_store(&coordinates.tenant_id)
            .await?;
        let stream_id = verlet_history::EventStreamId::for_thread(coordinates);
        let events = store
            .read_events(&stream_id, None)
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
        let Some(start) = events.iter().rev().find(|event| {
            event.kind == verlet_history::EventKind::SessionEntryAppended
                && event
                    .payload
                    .get("entry_kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("runtime")
                && event
                    .payload
                    .get("runtime_kind")
                    .and_then(serde_json::Value::as_str)
                    == Some("thread_started")
        }) else {
            return Ok(None);
        };
        let Some(payload) = start
            .payload
            .get("runtime_payload")
            .and_then(serde_json::Value::as_object)
        else {
            return Ok(None);
        };
        if !payload.contains_key("parent_thread_id")
            || !payload.contains_key("topology")
            || !payload.contains_key("metadata")
        {
            return Ok(None);
        }
        let parent_thread_id = serde_json::from_value(payload["parent_thread_id"].clone())
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "thread-start parent codec failed: {err}"
                ))
            })?;
        let topology: verlet_runtime_contracts::ThreadTopology =
            serde_json::from_value(payload["topology"].clone()).map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "thread-start topology codec failed: {err}"
                ))
            })?;
        let metadata: std::collections::BTreeMap<String, String> =
            serde_json::from_value(payload["metadata"].clone()).map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "thread-start metadata codec failed: {err}"
                ))
            })?;
        if parent_thread_id != topology.compatibility_parent_thread_id() {
            return Err(crate::kernel::runtime_host::VerletError::History(format!(
                "thread-start parent does not match topology for {}",
                coordinates.thread_id
            )));
        }
        let created_at_ms = u64::try_from(start.created_at_ms).unwrap_or_default();
        Ok(Some(verlet_runtime_contracts::ThreadLifecycleRecord {
            coordinates: coordinates.clone(),
            parent_thread_id,
            topology,
            status: verlet_runtime_contracts::ThreadLifecycleStatus::Idle,
            latest_signal_id: None,
            latest_checkpoint_id: None,
            created_at_ms,
            updated_at_ms: created_at_ms,
            metadata,
        }))
    }

    async fn thread_load_lock(
        &self,
        thread_id: verlet_runtime_contracts::ThreadId,
    ) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.thread_load_locks.lock().await;
        locks
            .entry(thread_id)
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    pub async fn egress_cursor_for_thread(
        &self,
        protocol: &str,
        instance_id: &str,
        thread_id: &str,
    ) -> verlet_io_core::IoResult<Option<verlet_history::StreamCursorV1>> {
        let key = source_scope(protocol, instance_id);
        let state = self.egress_states.read().await.get(&key).cloned();
        let Some(state) = state else {
            return Ok(None);
        };
        state.cursor(instance_id, thread_id)
    }

    pub async fn egress_dead_letter_count(
        &self,
        protocol: &str,
        instance_id: &str,
    ) -> verlet_io_core::IoResult<usize> {
        let key = source_scope(protocol, instance_id);
        let state = self.egress_states.read().await.get(&key).cloned();
        let Some(state) = state else {
            return Ok(0);
        };
        state.dead_letter_count(instance_id)
    }

    pub async fn submit_envelope(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        if is_clock_tick_envelope(&envelope) {
            return self.submit_clock_tick_envelope(&envelope).await;
        }
        let source_envelopes = [envelope.clone()];
        self.submit_envelope_with_sources(envelope, &source_envelopes, &[], false, None)
            .await
    }

    /// Submits a handle fact through the durable queue-equivalent claim and
    /// settlement path. The returned receipt means the ingress witness is
    /// committed, not merely accepted by an in-memory adapter.
    pub(crate) async fn submit_durable_handle_envelope(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        self.submit_queued_envelope(envelope, 1).await
    }

    async fn submit_queued_envelope(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
        attempt: u32,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        if is_clock_tick_envelope(&envelope) {
            return self.submit_clock_tick_envelope(&envelope).await;
        }
        let source_envelopes = [envelope.clone()];
        let ingress_message_ids = [envelope.id.clone()];
        self.submit_envelope_with_sources(
            envelope,
            &source_envelopes,
            &ingress_message_ids,
            false,
            Some(attempt),
        )
        .await
    }

    /// Child-side remote placement admission. The queue transport has
    /// already authenticated and scoped the entry; this method preserves its
    /// exact target and decision while still entering through the ordinary
    /// durable ingress claim/settle lane (including dedupe ownership).
    pub(crate) async fn submit_durable_remote_envelope(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
        target: verlet_io_core::ResolvedIoTarget,
        decision: verlet_io_core::AdmissionDecision,
        attempt: u32,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        let source_envelopes = [envelope.clone()];
        let ingress_message_ids = [envelope.id.clone()];
        self.submit_envelope_with_sources_at_target(
            envelope,
            &source_envelopes,
            &ingress_message_ids,
            false,
            Some(attempt),
            Some(target),
            Some(decision),
        )
        .await
    }

    async fn queued_message_was_applied(
        &self,
        message: &verlet_io_core::LeasedIngressEnvelope,
    ) -> verlet_io_core::IoResult<bool> {
        let ingress_message_ids = [message.envelope.id.clone()];
        let target = self.resolve_target(&message.envelope).await?;
        Ok(matches!(
            self.ingress_outcome(
                &target,
                std::slice::from_ref(&message.envelope),
                &ingress_message_ids
            )
            .await?,
            IngressOutcomeState::Settled { .. }
        ))
    }

    pub async fn submit_coalesced_envelopes(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
        source_envelopes: &[verlet_io_core::IngressEnvelope],
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        if source_envelopes.is_empty() {
            return Err(verlet_io_core::IoError::Bridge(
                "coalesced ingress submit requires at least one source envelope".to_string(),
            ));
        }
        if is_clock_tick_envelope(&envelope) {
            return Err(verlet_io_core::IoError::Bridge(
                "clock.tick envelopes cannot be coalesced".to_string(),
            ));
        }
        self.submit_envelope_with_sources(envelope, source_envelopes, &[], true, None)
            .await
    }

    async fn submit_coalesced_queued_envelopes(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
        source_envelopes: &[verlet_io_core::IngressEnvelope],
        ingress_message_ids: &[String],
        ingress_attempt: u32,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        if source_envelopes.is_empty() || source_envelopes.len() != ingress_message_ids.len() {
            return Err(verlet_io_core::IoError::Bridge(
                "coalesced queued ingress requires one message id per source envelope".to_string(),
            ));
        }
        if is_clock_tick_envelope(&envelope) {
            return Err(verlet_io_core::IoError::Bridge(
                "clock.tick envelopes cannot be coalesced".to_string(),
            ));
        }
        self.submit_envelope_with_sources(
            envelope,
            source_envelopes,
            ingress_message_ids,
            true,
            Some(ingress_attempt),
        )
        .await
    }

    async fn submit_envelope_with_sources(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
        source_envelopes: &[verlet_io_core::IngressEnvelope],
        ingress_message_ids: &[String],
        coalesced: bool,
        ingress_attempt: Option<u32>,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        self.submit_envelope_with_sources_at_target(
            envelope,
            source_envelopes,
            ingress_message_ids,
            coalesced,
            ingress_attempt,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn submit_envelope_with_sources_at_target(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
        source_envelopes: &[verlet_io_core::IngressEnvelope],
        ingress_message_ids: &[String],
        coalesced: bool,
        ingress_attempt: Option<u32>,
        target_override: Option<verlet_io_core::ResolvedIoTarget>,
        decision_override: Option<verlet_io_core::AdmissionDecision>,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        if !ingress_message_ids.is_empty() && source_envelopes.len() != ingress_message_ids.len() {
            return Err(verlet_io_core::IoError::Bridge(
                "durable ingress requires one message id per source envelope".to_string(),
            ));
        }
        let mut target = match target_override {
            Some(target) => target,
            None => self.resolve_target(&envelope).await?,
        };
        let attribution_error = envelope.require_attributed(&target).err().or_else(|| {
            source_envelopes
                .iter()
                .find_map(|source_envelope| source_envelope.require_attributed(&target).err())
        });
        if !ingress_message_ids.is_empty() {
            match self
                .ingress_outcome(&target, source_envelopes, ingress_message_ids)
                .await?
            {
                IngressOutcomeState::Missing => {}
                state @ IngressOutcomeState::Claimed { .. } => {
                    if ingress_attempt == Some(1) && ingress_outcome_is_fork(&state) {
                        return Ok(fork_claim_loser_receipt(&envelope, target, &state));
                    }
                    return self
                        .recover_ingress_outcome(&envelope, &target, state)
                        .await;
                }
                state @ IngressOutcomeState::Settled { .. } => {
                    return Ok(deduplicated_ingress_receipt(&envelope, target, &state));
                }
            }
        }
        let (coordinates, _handle) = self.ensure_thread(&target, &envelope).await?;
        target.address.thread_id = Some(coordinates.thread_id.to_string());
        let state = self.ingress_state(&target).await?;
        let policy_hash = self
            .ensure_route_policy_bound(&coordinates, &envelope)
            .await?;
        let mut ingress_event_ids = Vec::new();
        for (index, source_envelope) in source_envelopes.iter().enumerate() {
            let ingress_event = self
                .record_ingress_received(
                    &coordinates,
                    source_envelope,
                    ingress_message_ids.get(index).map(String::as_str),
                )
                .await?;
            ingress_event_ids.push(ingress_event.id);
        }
        let decision = match attribution_error {
            Some(err) => verlet_io_core::AdmissionDecision::reject(err.to_string()),
            None => match decision_override {
                Some(decision) => decision,
                None => self.decide(&envelope, &target, &state).await?,
            },
        };
        let ingress_source_stream =
            crate::kernel::control_decision::control_stream_id(&coordinates);
        let admission_event = self
            .record_admission_decided(
                &coordinates,
                &envelope,
                &decision,
                &policy_hash,
                ingress_event_ids.clone(),
                coalesced,
                !ingress_message_ids.is_empty(),
            )
            .await?;
        let ingress_ownership = if ingress_message_ids.is_empty() {
            None
        } else {
            self.record_ingress_ownership(
                &envelope,
                source_envelopes,
                &ingress_source_stream,
                ingress_attempt.unwrap_or(1),
            )
            .await?
        };
        #[cfg(test)]
        if ingress_ownership.is_some()
            && self
                .pause_after_ingress_ownership
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            self.ingress_ownership_paused.notify_waiters();
            std::future::pending::<()>().await;
        }
        let (receipt, _) = self
            .apply_with_ingress_outcomes(
                &envelope,
                &target,
                &decision,
                ingress_message_ids,
                Some(&ingress_source_stream),
                &ingress_event_ids,
                Some(admission_event.id),
                ingress_ownership.as_ref(),
            )
            .await?;
        Ok(receipt)
    }

    async fn submit_clock_tick_envelope(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        let store_path = self.session_store_path.as_ref().ok_or_else(|| {
            verlet_io_core::IoError::Bridge(
                "clock.tick requires a daemon session store path".to_string(),
            )
        })?;
        let coordinates = clock_tick_coordinates(envelope)?;
        let target = verlet_io_core::ResolvedIoTarget::new(
            verlet_io_core::ThreadAddress::new(
                coordinates.tenant_id.clone(),
                coordinates.user_id.clone(),
                coordinates.session_id.clone(),
            )
            .with_thread_id(coordinates.thread_id.to_string()),
        );
        envelope.require_attributed(&target)?;
        let decision = verlet_io_core::AdmissionDecision::ObserveOnly {
            reason: "clock tick admitted as timer.fired".to_string(),
        };
        let mut receipt = verlet_io_core::KernelIoReceipt::new(envelope, target, &decision);
        receipt.thread_id = Some(coordinates.thread_id.to_string());

        let timer = clock_tick_payload(envelope)?;
        let store = verlet_history_sqlite::SqliteSessionStore::open(store_path)
            .await
            .map_err(verlet_history_error)?
            .with_lease_epoch(self.lease_epoch);
        let mandate_is_live =
            crate::kernel::mandate_lifecycle::list_active_mandates(&store, &coordinates)
                .await
                .map_err(verlet_bridge_error)?
                .iter()
                .any(|mandate| mandate.event.id == timer.mandate_event_id);
        if !mandate_is_live {
            return Ok(receipt);
        }
        let stream_id = crate::kernel::control_decision::control_stream_id(&coordinates);
        let events = store
            .read_events(&stream_id, None)
            .await
            .map_err(verlet_history_error)?;
        for event in &events {
            if event.kind != verlet_history::EventKind::TimerFired {
                continue;
            }
            let payload =
                serde_json::from_value::<verlet_history::TimerFiredPayload>(event.payload.clone())
                    .map_err(|err| {
                        verlet_io_core::IoError::Bridge(format!(
                            "invalid timer.fired payload: {err}"
                        ))
                    })?;
            if payload.mandate_event_id == timer.mandate_event_id
                && payload.occurrence_index == timer.occurrence_index
            {
                return Ok(receipt);
            }
        }

        let mandate_event_id = timer.mandate_event_id;
        let mut record = verlet_history::NewEventRecord::witnessed(
            coordinates,
            verlet_history::EventKind::TimerFired,
            serde_json::to_value(timer).map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("encode timer.fired payload: {err}"))
            })?,
        );
        record.provenance = verlet_history::EventProvenance {
            source_streams: vec![stream_id.clone()],
            source_event_ids: vec![mandate_event_id],
            ..verlet_history::EventProvenance::default()
        };
        store
            .append_events(&stream_id, vec![record])
            .await
            .map_err(verlet_history_error)?;
        Ok(receipt)
    }

    async fn resolve_target(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::ResolvedIoTarget> {
        if matches!(
            &envelope.content,
            verlet_io_core::IngressContent::Event { kind, .. } if kind == verlet_runtime_contracts::handle::HANDLE_DISPATCH_CONTENT_KIND
        ) {
            return self.resolve_handle_dispatch_target(envelope);
        }
        if matches!(
            &envelope.content,
            verlet_io_core::IngressContent::Event { kind, .. } if kind == verlet_runtime_contracts::handle::HANDLE_OUTCOME_CONTENT_KIND
        ) {
            return self.resolve_handle_outcome_target(envelope).await;
        }
        let threading = envelope
            .metadata
            .get("cooldis_route_threading")
            .map(String::as_str)
            .unwrap_or("per_conversation");
        let session_id = match threading {
            "single_thread" | "route_single_thread" => {
                format!("io:{}", envelope.source.stable_scope())
            }
            "per_actor" => format!(
                "io:{}:{}",
                envelope.source.stable_scope(),
                envelope
                    .actor
                    .as_ref()
                    .map(|actor| actor.external_actor_id.as_str())
                    .unwrap_or("anonymous")
            ),
            _ => format!(
                "io:{}:{}",
                envelope.source.stable_scope(),
                envelope.conversation.stable_key()
            ),
        };

        let mut target = verlet_io_core::ResolvedIoTarget::new(verlet_io_core::ThreadAddress::new(
            self.tenant_id.clone(),
            self.user_id.clone(),
            session_id,
        ))
        .with_provider_policy(verlet_io_core::ProviderPolicy::new(
            self.model_provider.clone(),
            self.model.clone(),
        ));
        target.metadata.insert(
            "verlet_source_scope".to_string(),
            envelope.source.stable_scope(),
        );
        if let Some(agent_ref) = envelope.metadata.get(ROUTE_AGENT_REF_METADATA) {
            target
                .metadata
                .insert(ROUTE_AGENT_REF_METADATA.to_string(), agent_ref.clone());
        }
        Ok(target)
    }

    fn resolve_handle_dispatch_target(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::ResolvedIoTarget> {
        let verlet_io_core::IngressContent::Event { payload, .. } = &envelope.content else {
            unreachable!("handle dispatch kind was checked above");
        };
        let dispatch = serde_json::from_value::<
            verlet_runtime_contracts::handle::HandleDispatchEnvelope,
        >(payload.clone())
        .map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("invalid handle dispatch payload: {err}"))
        })?;
        if dispatch.handle.kind != verlet_runtime_contracts::handle::HandleKind::Process {
            return Err(verlet_io_core::IoError::Bridge(
                "handle dispatch ingress currently requires kind process".to_string(),
            ));
        }
        if dispatch.consumer.tenant_id != self.tenant_id
            || dispatch.consumer.user_id != self.user_id
        {
            return Err(verlet_io_core::IoError::Bridge(format!(
                "handle dispatch {} consumer is outside this daemon scope",
                dispatch.dispatch_id
            )));
        }
        let mut target = verlet_io_core::ResolvedIoTarget::new(
            verlet_io_core::ThreadAddress::new(
                dispatch.consumer.tenant_id.clone(),
                dispatch.consumer.user_id.clone(),
                dispatch.consumer.session_id.clone(),
            )
            .with_thread_id(dispatch.consumer.thread_id.to_string()),
        )
        .with_provider_policy(verlet_io_core::ProviderPolicy::new(
            self.model_provider.clone(),
            self.model.clone(),
        ));
        target.create_thread_if_missing = false;
        target.metadata.insert(
            "verlet_source_scope".to_string(),
            envelope.source.stable_scope(),
        );
        Ok(target)
    }

    /// Resolves settlement ingress from the durable spawn-time handle
    /// binding. Envelope conversation metadata is deliberately not authority:
    /// a restart re-folds the original request/spawn records and targets that
    /// consumer thread.
    async fn resolve_handle_outcome_target(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::ResolvedIoTarget> {
        let verlet_io_core::IngressContent::Event { payload, .. } = &envelope.content else {
            unreachable!("handle outcome kind was checked above");
        };
        let terminal = serde_json::from_value::<
            verlet_runtime_contracts::handle::HandleTerminalEnvelope,
        >(payload.clone())
        .map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("invalid handle outcome payload: {err}"))
        })?;
        let store = self.ingress_event_store().await?;
        let mut resolved = None;
        for coordinates in store
            .list_control_stream_coordinates()
            .await
            .map_err(verlet_history_error)?
        {
            if coordinates.tenant_id != self.tenant_id || coordinates.user_id != self.user_id {
                continue;
            }
            let events = store
                .read_events(
                    &crate::kernel::control_decision::control_stream_id(&coordinates),
                    None,
                )
                .await
                .map_err(verlet_history_error)?;
            match terminal.handle.kind {
                verlet_runtime_contracts::handle::HandleKind::Thread => {
                    if !events.iter().any(|event| {
                        event.kind == verlet_history::EventKind::ThreadSpawned
                            && event
                                .payload
                                .get("correlation_id")
                                .and_then(serde_json::Value::as_str)
                                == Some(terminal.dispatch_id.as_str())
                    }) {
                        continue;
                    }
                    for binding in
                        crate::kernel::thread_spawn_projector::fold_thread_handle_bindings(&events)
                            .map_err(verlet_bridge_error)?
                    {
                        if binding.dispatch_id != terminal.dispatch_id {
                            continue;
                        }
                        if binding.handle != terminal.handle {
                            return Err(verlet_io_core::IoError::Bridge(format!(
                                "handle outcome {} does not match its durable spawn binding",
                                terminal.dispatch_id
                            )));
                        }
                        if let Some(existing) = &resolved
                            && existing != &binding.consumer
                        {
                            return Err(verlet_io_core::IoError::Bridge(format!(
                                "handle outcome {} resolves to multiple consumer threads",
                                terminal.dispatch_id
                            )));
                        }
                        resolved = Some(binding.consumer);
                    }
                }
                verlet_runtime_contracts::handle::HandleKind::Process => {
                    for event in events
                        .iter()
                        .filter(|event| event.kind == verlet_history::EventKind::IoIngressReceived)
                    {
                        if event
                            .payload
                            .pointer("/content/payload/dispatch_id")
                            .and_then(serde_json::Value::as_str)
                            != Some(terminal.dispatch_id.as_str())
                        {
                            continue;
                        }
                        let witness = serde_json::from_value::<
                            verlet_history::IoIngressReceivedPayload,
                        >(event.payload.clone())
                        .map_err(|err| {
                            verlet_io_core::IoError::Bridge(format!(
                                "invalid ingress witness payload: {err}"
                            ))
                        })?;
                        let Some(content) = witness.content else {
                            continue;
                        };
                        let content =
                            serde_json::from_value::<verlet_io_core::IngressContent>(content)
                                .map_err(|err| {
                                    verlet_io_core::IoError::Bridge(format!(
                                        "invalid witnessed content: {err}"
                                    ))
                                })?;
                        let verlet_io_core::IngressContent::Event { kind, payload } = content
                        else {
                            continue;
                        };
                        if kind != verlet_runtime_contracts::handle::HANDLE_DISPATCH_CONTENT_KIND {
                            continue;
                        }
                        let binding = serde_json::from_value::<
                            verlet_runtime_contracts::handle::HandleDispatchEnvelope,
                        >(payload)
                        .map_err(|err| {
                            verlet_io_core::IoError::Bridge(format!(
                                "invalid process dispatch witness payload: {err}"
                            ))
                        })?;
                        if binding.dispatch_id != terminal.dispatch_id {
                            continue;
                        }
                        if binding.handle != terminal.handle {
                            return Err(verlet_io_core::IoError::Bridge(format!(
                                "process outcome {} does not match its durable dispatch binding",
                                terminal.dispatch_id
                            )));
                        }
                        if let Some(existing) = &resolved
                            && existing != &binding.consumer
                        {
                            return Err(verlet_io_core::IoError::Bridge(format!(
                                "process outcome {} resolves to multiple consumer threads",
                                terminal.dispatch_id
                            )));
                        }
                        resolved = Some(binding.consumer);
                    }
                }
            }
        }
        let coordinates = resolved.ok_or_else(|| {
            verlet_io_core::IoError::Bridge(format!(
                "handle outcome {} has no durable spawn binding",
                terminal.dispatch_id
            ))
        })?;
        let mut target = verlet_io_core::ResolvedIoTarget::new(
            verlet_io_core::ThreadAddress::new(
                coordinates.tenant_id.clone(),
                coordinates.user_id.clone(),
                coordinates.session_id.clone(),
            )
            .with_thread_id(coordinates.thread_id.to_string()),
        )
        .with_provider_policy(verlet_io_core::ProviderPolicy::new(
            self.model_provider.clone(),
            self.model.clone(),
        ));
        target.create_thread_if_missing = false;
        target.metadata.insert(
            "verlet_source_scope".to_string(),
            envelope.source.stable_scope(),
        );
        Ok(target)
    }

    async fn ingress_event_store(
        &self,
    ) -> verlet_io_core::IoResult<verlet_history_sqlite::SqliteSessionStore> {
        let store_path = self.session_store_path.as_ref().ok_or_else(|| {
            verlet_io_core::IoError::Bridge(
                "durable ingress outcomes require a daemon session store".to_string(),
            )
        })?;
        verlet_history_sqlite::SqliteSessionStore::open(store_path)
            .await
            .map(|store| store.with_lease_epoch(self.lease_epoch))
            .map_err(verlet_history_error)
    }

    async fn resolved_target_coordinates(
        &self,
        target: &verlet_io_core::ResolvedIoTarget,
    ) -> verlet_io_core::IoResult<Option<verlet_runtime_contracts::ThreadCoordinates>> {
        if let Some(thread_id) = target.address.thread_id.as_deref() {
            return Ok(Some(verlet_runtime_contracts::ThreadCoordinates {
                tenant_id: target.address.tenant_id.clone(),
                user_id: target.address.user_id.clone(),
                session_id: target.address.session_id.clone(),
                thread_id: verlet_runtime_contracts::ThreadId::parse_str(thread_id).map_err(
                    |err| {
                        verlet_io_core::IoError::Bridge(format!(
                            "invalid resolved ingress thread id: {err}"
                        ))
                    },
                )?,
            }));
        }
        Ok(self
            .threads
            .lock()
            .await
            .get(&target.address.scope_key())
            .cloned())
    }

    async fn ingress_outcome(
        &self,
        target: &verlet_io_core::ResolvedIoTarget,
        source_envelopes: &[verlet_io_core::IngressEnvelope],
        ingress_envelope_ids: &[String],
    ) -> verlet_io_core::IoResult<IngressOutcomeState> {
        if ingress_envelope_ids.is_empty() {
            return Ok(IngressOutcomeState::Missing);
        }
        let ownership_keys = ingress_ownership_keys(source_envelopes)?;
        if !ownership_keys.is_empty()
            && let Some(state) = self.ingress_route_state(&source_envelopes[0]).await
        {
            let ownership_streams = state.ingress_ownership_streams(&ownership_keys)?;
            let owned = self
                .ingress_outcome_on_streams(&ownership_streams, ingress_envelope_ids)
                .await?;
            if !matches!(owned, IngressOutcomeState::Missing) {
                return Ok(owned);
            }
        }
        let Some(mut coordinates) = self.resolved_target_coordinates(target).await? else {
            return Ok(IngressOutcomeState::Missing);
        };
        let store = self.ingress_event_store().await?;
        let mut resolved_thread = true;
        loop {
            let events = store
                .read_events(
                    &crate::kernel::control_decision::control_stream_id(&coordinates),
                    None,
                )
                .await
                .map_err(verlet_history_error)?;
            let state = ingress_outcome_fold(&events, ingress_envelope_ids)?;
            if !matches!(state, IngressOutcomeState::Missing) {
                if resolved_thread || ingress_outcome_is_fork(&state) {
                    return Ok(state);
                }
                return Ok(IngressOutcomeState::Missing);
            }
            let handle = self
                .get_or_load_thread_handle(&coordinates)
                .await
                .map_err(|err| verlet_bridge_error(err.into_inner()))?;
            let Some(parent_thread_id) = handle.context().parent_thread_id else {
                return Ok(IngressOutcomeState::Missing);
            };
            coordinates.thread_id = parent_thread_id;
            resolved_thread = false;
        }
    }

    async fn ingress_outcome_on_streams(
        &self,
        streams: &[verlet_history::EventStreamId],
        ingress_envelope_ids: &[String],
    ) -> verlet_io_core::IoResult<IngressOutcomeState> {
        if streams.is_empty() {
            return Ok(IngressOutcomeState::Missing);
        }
        let store = self.ingress_event_store().await?;
        let mut events = Vec::new();
        for stream in streams {
            events.extend(
                store
                    .read_events(stream, None)
                    .await
                    .map_err(verlet_history_error)?,
            );
        }
        ingress_outcome_fold(&events, ingress_envelope_ids)
    }

    async fn await_ingress_outcome_on_streams(
        &self,
        streams: &[verlet_history::EventStreamId],
        ingress_envelope_ids: &[String],
    ) -> verlet_io_core::IoResult<IngressOutcomeState> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let store = self.ingress_event_store().await?;
            ingress_outcome_on_store(&store, streams, ingress_envelope_ids).await
        })
        .await
        .map_err(|_| ingress_outcome_timeout_error())?
    }

    async fn ingress_route_state(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
    ) -> Option<std::sync::Arc<DaemonEgressState>> {
        let route_id = route_id_for_ingress(envelope);
        let key = source_scope(&envelope.source.protocol, &route_id);
        self.egress_states.read().await.get(&key).cloned()
    }

    async fn record_ingress_ownership(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        source_envelopes: &[verlet_io_core::IngressEnvelope],
        stream_id: &verlet_history::EventStreamId,
        attempt: u32,
    ) -> verlet_io_core::IoResult<Option<IngressOwnershipReservation>> {
        let keys = ingress_ownership_keys(source_envelopes)?;
        if keys.is_empty() {
            return Ok(None);
        }
        let Some(state) = self.ingress_route_state(envelope).await else {
            let route_id = route_id_for_ingress(envelope);
            let source_scope = source_scope(&envelope.source.protocol, &route_id);
            if self
                .egress_route_configs
                .read()
                .await
                .contains_key(&source_scope)
            {
                return Err(verlet_io_core::IoError::Bridge(
                    "durable ingress ownership requires its route state before claim admission"
                        .to_string(),
                ));
            }
            return Ok(None);
        };
        let owned_keys = keys.clone();
        let owned_stream_id = stream_id.clone();
        let ownership_id = state
            .run_blocking(move |state| {
                state.record_ingress_ownership(&owned_keys, &owned_stream_id, attempt)
            })
            .await?;
        Ok(Some(IngressOwnershipReservation {
            state,
            ownership_id,
            keys,
            stream_id: stream_id.clone(),
        }))
    }

    async fn append_ingress_claim(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        ingress_envelope_ids: &[String],
        ingress_witness_event_ids: &[verlet_history::EventRecordId],
        admission_event_id: verlet_history::EventRecordId,
        intent: verlet_history::IngressOutcomeIntent,
        ownership: Option<&IngressOwnershipReservation>,
    ) -> verlet_io_core::IoResult<IngressClaimAppend> {
        let store = self.ingress_event_store().await?;
        let stream_id = crate::kernel::control_decision::control_stream_id(coordinates);
        let mut ownership_lock = match ownership {
            Some(ownership) => {
                let lock = ownership.state.lock_ingress_claim_admission().await?;
                let streams = lock.ownership_streams(&ownership.keys)?;
                match self
                    .ingress_outcome_on_streams(&streams, ingress_envelope_ids)
                    .await?
                {
                    IngressOutcomeState::Missing => {}
                    state => {
                        lock.prune_to_stream(&ownership.keys, &ingress_outcome_stream_id(&state))?;
                        lock.commit()?;
                        return Ok(IngressClaimAppend::Existing(state));
                    }
                }
                if !lock.reservation_is_current(ownership)? {
                    lock.commit()?;
                    let state = self
                        .await_ingress_outcome_on_streams(&streams, ingress_envelope_ids)
                        .await?;
                    return Ok(IngressClaimAppend::Existing(state));
                }
                lock.prune_to_reservation(ownership)?;
                Some(lock)
            }
            None => None,
        };
        loop {
            let events = store
                .read_events(&stream_id, None)
                .await
                .map_err(verlet_history_error)?;
            match ingress_outcome_fold(&events, ingress_envelope_ids)? {
                IngressOutcomeState::Missing => {}
                state => {
                    if let Some(lock) = ownership_lock.take() {
                        lock.commit()?;
                    }
                    return Ok(IngressClaimAppend::Existing(state));
                }
            }
            let expected_next_sequence = events
                .last()
                .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| verlet_history::EventSequence::new(1));
            let payload = verlet_history::IoIngressClaimedPayload {
                ingress_envelope_ids: ingress_envelope_ids.to_vec(),
                ingress_witness_event_ids: ingress_witness_event_ids.to_vec(),
                admission_event_id,
                intent: intent.clone(),
            };
            let claim = verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::IoIngressClaimed,
                serde_json::to_value(payload).map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!(
                        "encode io.ingress.claimed payload: {err}"
                    ))
                })?,
                ingress_claim_provenance(&stream_id, ingress_witness_event_ids, admission_event_id),
            );
            match store
                .append_events_fenced(&stream_id, expected_next_sequence, vec![claim])
                .await
            {
                Ok(mut appended) => {
                    let claim = appended.pop().ok_or_else(|| {
                        verlet_io_core::IoError::Bridge(
                            "ingress claim append returned no record".to_string(),
                        )
                    })?;
                    if let Some(lock) = ownership_lock.take() {
                        lock.commit()?;
                    }
                    #[cfg(test)]
                    if self
                        .pause_after_ingress_claim
                        .load(std::sync::atomic::Ordering::SeqCst)
                    {
                        self.ingress_claim_paused.notify_waiters();
                        std::future::pending::<()>().await;
                    }
                    return Ok(IngressClaimAppend::Appended(claim));
                }
                Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => return Err(verlet_history_error(err)),
            }
        }
    }

    async fn append_effect_free_ingress_outcome(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        ingress_envelope_ids: &[String],
        ingress_witness_event_ids: &[verlet_history::EventRecordId],
        admission_event_id: verlet_history::EventRecordId,
        intent: verlet_history::IngressOutcomeIntent,
        ownership: Option<&IngressOwnershipReservation>,
    ) -> verlet_io_core::IoResult<IngressClaimAppend> {
        let store = self.ingress_event_store().await?;
        let stream_id = crate::kernel::control_decision::control_stream_id(coordinates);
        let mut ownership_lock = match ownership {
            Some(ownership) => {
                let lock = ownership.state.lock_ingress_claim_admission().await?;
                let streams = lock.ownership_streams(&ownership.keys)?;
                match self
                    .ingress_outcome_on_streams(&streams, ingress_envelope_ids)
                    .await?
                {
                    IngressOutcomeState::Missing => {}
                    state => {
                        lock.prune_to_stream(&ownership.keys, &ingress_outcome_stream_id(&state))?;
                        lock.commit()?;
                        return Ok(IngressClaimAppend::Existing(state));
                    }
                }
                if !lock.reservation_is_current(ownership)? {
                    lock.commit()?;
                    let state = self
                        .await_ingress_outcome_on_streams(&streams, ingress_envelope_ids)
                        .await?;
                    return Ok(IngressClaimAppend::Existing(state));
                }
                lock.prune_to_reservation(ownership)?;
                Some(lock)
            }
            None => None,
        };
        loop {
            let events = store
                .read_events(&stream_id, None)
                .await
                .map_err(verlet_history_error)?;
            match ingress_outcome_fold(&events, ingress_envelope_ids)? {
                IngressOutcomeState::Missing => {}
                state => {
                    if let Some(lock) = ownership_lock.take() {
                        lock.commit()?;
                    }
                    return Ok(IngressClaimAppend::Existing(state));
                }
            }
            let expected_next_sequence = events
                .last()
                .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| verlet_history::EventSequence::new(1));
            let claim_payload = verlet_history::IoIngressClaimedPayload {
                ingress_envelope_ids: ingress_envelope_ids.to_vec(),
                ingress_witness_event_ids: ingress_witness_event_ids.to_vec(),
                admission_event_id,
                intent: intent.clone(),
            };
            let claim = verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::IoIngressClaimed,
                serde_json::to_value(claim_payload).map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!(
                        "encode io.ingress.claimed payload: {err}"
                    ))
                })?,
                ingress_claim_provenance(&stream_id, ingress_witness_event_ids, admission_event_id),
            );
            let settle_payload = verlet_history::IoIngressSettledPayload {
                claim_event_id: claim.id,
                ingress_envelope_ids: ingress_envelope_ids.to_vec(),
                evidence_event_id: None,
                settled_by: verlet_history::IngressSettledBy::Execution,
            };
            let settle = verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::IoIngressSettled,
                serde_json::to_value(settle_payload).map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!(
                        "encode io.ingress.settled payload: {err}"
                    ))
                })?,
                ingress_settle_provenance(&stream_id, coordinates, claim.id, None),
            );
            match store
                .append_events_fenced(&stream_id, expected_next_sequence, vec![claim, settle])
                .await
            {
                Ok(mut appended) => {
                    if appended.len() != 2 {
                        return Err(verlet_io_core::IoError::Bridge(
                            "effect-free ingress outcome append returned an incomplete batch"
                                .to_string(),
                        ));
                    }
                    if let Some(lock) = ownership_lock.take() {
                        lock.commit()?;
                    }
                    return Ok(IngressClaimAppend::Appended(appended.remove(0)));
                }
                Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => return Err(verlet_history_error(err)),
            }
        }
    }

    async fn append_ingress_settle(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        claim: &verlet_history::EventRecord,
        claim_payload: &verlet_history::IoIngressClaimedPayload,
        evidence_event_id: Option<verlet_history::EventRecordId>,
        settled_by: verlet_history::IngressSettledBy,
    ) -> verlet_io_core::IoResult<verlet_history::EventRecord> {
        let store = self.ingress_event_store().await?;
        let stream_id = crate::kernel::control_decision::control_stream_id(coordinates);
        loop {
            let events = store
                .read_events(&stream_id, None)
                .await
                .map_err(verlet_history_error)?;
            match ingress_outcome_fold(&events, &claim_payload.ingress_envelope_ids)? {
                IngressOutcomeState::Settled { settle, .. } => return Ok(settle),
                IngressOutcomeState::Claimed {
                    claim: existing, ..
                } if existing.id == claim.id => {}
                IngressOutcomeState::Claimed { .. } | IngressOutcomeState::Missing => {
                    return Err(verlet_io_core::IoError::Bridge(
                        "ingress settle no longer matches the active claim".to_string(),
                    ));
                }
            }
            let expected_next_sequence = events
                .last()
                .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| verlet_history::EventSequence::new(1));
            let payload = verlet_history::IoIngressSettledPayload {
                claim_event_id: claim.id,
                ingress_envelope_ids: claim_payload.ingress_envelope_ids.clone(),
                evidence_event_id,
                settled_by,
            };
            let settle = verlet_history::NewEventRecord::discharged(
                coordinates.clone(),
                verlet_history::EventKind::IoIngressSettled,
                serde_json::to_value(payload).map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!(
                        "encode io.ingress.settled payload: {err}"
                    ))
                })?,
                ingress_settle_provenance(&stream_id, coordinates, claim.id, evidence_event_id),
            );
            match store
                .append_events_fenced(&stream_id, expected_next_sequence, vec![settle])
                .await
            {
                Ok(mut appended) => {
                    return appended.pop().ok_or_else(|| {
                        verlet_io_core::IoError::Bridge(
                            "ingress settle append returned no record".to_string(),
                        )
                    });
                }
                Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => return Err(verlet_history_error(err)),
            }
        }
    }

    async fn wait_for_turn_execution_evidence(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        turn_id: &str,
        submission_mode: verlet_runtime_contracts::TurnSubmissionMode,
    ) -> verlet_io_core::IoResult<verlet_history::EventRecord> {
        let store = self.ingress_event_store().await?;
        let stream_id = verlet_history::EventStreamId::for_thread(coordinates);
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let mut events = store
                .read_events(&stream_id, None)
                .await
                .map_err(verlet_history_error)?;
            let mut cursor = events.last().map(verlet_history::EventRecord::cursor_v1);
            loop {
                if let Some(evidence) = turn_execution_evidence(&events, turn_id, submission_mode) {
                    return Ok(evidence);
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                events = match &cursor {
                    Some(cursor) => store
                        .read_events_after_cursor(&stream_id, cursor)
                        .await
                        .map_err(verlet_history_error)?,
                    None => store
                        .read_events(&stream_id, Some(verlet_history::EventSequence::new(1)))
                        .await
                        .map_err(verlet_history_error)?,
                };
                if let Some(last) = events.last() {
                    cursor = Some(last.cursor_v1());
                }
            }
        })
        .await
        .map_err(|_| {
            verlet_io_core::IoError::Bridge(format!(
                "timed out waiting for execution evidence for ingress turn {turn_id}"
            ))
        })?
    }

    async fn ingress_state(
        &self,
        target: &verlet_io_core::ResolvedIoTarget,
    ) -> verlet_io_core::IoResult<verlet_io_core::IngressState> {
        let active_turn_id = self
            .lock_active_turns()
            .get(&target.address.scope_key())
            .cloned();
        Ok(verlet_io_core::IngressState {
            active_turn_id,
            pending_count: 0,
            dedupe_seen: false,
            metadata: target.metadata.clone(),
        })
    }

    async fn decide(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        state: &verlet_io_core::IngressState,
    ) -> verlet_io_core::IoResult<verlet_io_core::AdmissionDecision> {
        let input = verlet_io_core::IoTurnInput::from_envelope(envelope, target);
        let turn_id = format!("turn-{}", uuid::Uuid::now_v7());
        let policy = envelope
            .metadata
            .get("cooldis_route_policy")
            .map(String::as_str)
            .unwrap_or("queue_per_conversation");
        match policy {
            "queue_per_conversation" | "coalesce_bursts" => {
                Ok(verlet_io_core::AdmissionDecision::queue(turn_id, input))
            }
            "observe_only" => Ok(verlet_io_core::AdmissionDecision::ObserveOnly {
                reason: "route policy observe_only".to_string(),
            }),
            "reject" => Ok(verlet_io_core::AdmissionDecision::reject(
                "route policy reject",
            )),
            "steer" | "steer_when_active" => {
                if let Some(active_turn_id) = &state.active_turn_id {
                    Ok(verlet_io_core::AdmissionDecision::steer(
                        turn_id,
                        Some(active_turn_id.clone()),
                        input,
                    ))
                } else {
                    Ok(verlet_io_core::AdmissionDecision::queue(turn_id, input))
                }
            }
            "interrupt" | "interrupt_on_new_dm" => {
                Ok(verlet_io_core::AdmissionDecision::Interrupt {
                    reason: "route policy interrupt".to_string(),
                    replacement_turn_id: Some(turn_id),
                    replacement: Some(input),
                })
            }
            "fork" | "fork_on_new_dm" => Ok(verlet_io_core::AdmissionDecision::Fork {
                child_key: turn_id,
                input,
            }),
            other => Err(verlet_io_core::IoError::Bridge(format!(
                "unknown route policy {other:?}"
            ))),
        }
    }

    async fn ensure_route_policy_bound(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        envelope: &verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<String> {
        let policy_id = admission_route_policy_id(envelope);
        let content_hash = crate::agent::manifest_bind::canonical_json_hash(
            &admission_route_policy_config(envelope),
        )
        .map_err(verlet_bridge_error)?;
        let handle = self
            .supervisor
            .get_thread_at(coordinates)
            .await
            .map_err(verlet_bridge_error)?;
        let control_events = handle
            .read_control_events()
            .await
            .map_err(verlet_bridge_error)?;
        let latest = control_events
            .iter()
            .filter(|event| event.kind == verlet_history::EventKind::PolicyBound)
            .filter(|event| {
                event
                    .payload
                    .get("policy_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(policy_id.as_str())
            })
            .max_by_key(|event| event.sequence.get());
        if latest.and_then(|event| {
            event
                .payload
                .get("content_hash")
                .and_then(serde_json::Value::as_str)
        }) == Some(content_hash.as_str())
        {
            return Ok(content_hash);
        }
        let payload = verlet_history::PolicyBoundPayload {
            policy_kind: verlet_history::PolicyKind::AdmissionRoute,
            policy_id,
            content_hash: content_hash.clone(),
            valid_from_note: "valid until next policy.bound of same policy_id".to_string(),
        };
        let mut value = serde_json::to_value(payload).map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("policy.bound payload codec failed: {err}"))
        })?;
        if let Some(object) = value.as_object_mut() {
            object.insert(
                "schema".to_string(),
                serde_json::json!(verlet_history::EventKind::PolicyBound.payload_schema_id()),
            );
        }
        handle
            .append_control_event(verlet_history::NewEventRecord::witnessed(
                coordinates.clone(),
                verlet_history::EventKind::PolicyBound,
                value,
            ))
            .await
            .map_err(verlet_bridge_error)?;
        Ok(content_hash)
    }

    async fn record_ingress_received(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        envelope: &verlet_io_core::IngressEnvelope,
        ingress_message_id: Option<&str>,
    ) -> verlet_io_core::IoResult<verlet_history::EventRecord> {
        let record = ingress_received_control_record(coordinates, envelope, ingress_message_id)?;
        let Some(ingress_message_id) = ingress_message_id else {
            let handle = self
                .supervisor
                .get_thread_at(coordinates)
                .await
                .map_err(verlet_bridge_error)?;
            return handle
                .append_control_event(record)
                .await
                .map_err(verlet_bridge_error);
        };
        let store = self.ingress_event_store().await?;
        let stream_id = crate::kernel::control_decision::control_stream_id(coordinates);
        loop {
            let events = store
                .read_events(&stream_id, None)
                .await
                .map_err(verlet_history_error)?;
            if let Some(existing) = events.iter().find(|event| {
                event.kind == verlet_history::EventKind::IoIngressReceived
                    && event
                        .payload
                        .get(INGRESS_MESSAGE_ID_FIELD)
                        .and_then(serde_json::Value::as_str)
                        == Some(ingress_message_id)
            }) {
                if existing.payload.get("envelope_digest") != record.payload.get("envelope_digest")
                {
                    return Err(verlet_io_core::IoError::Bridge(format!(
                        "durable ingress message {ingress_message_id:?} changed envelope digest"
                    )));
                }
                return Ok(existing.clone());
            }
            let expected_next_sequence = events
                .last()
                .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| verlet_history::EventSequence::new(1));
            match store
                .append_events_fenced(&stream_id, expected_next_sequence, vec![record.clone()])
                .await
            {
                Ok(mut appended) => {
                    return appended.pop().ok_or_else(|| {
                        verlet_io_core::IoError::Bridge(
                            "ingress witness append returned no record".to_string(),
                        )
                    });
                }
                Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => return Err(verlet_history_error(err)),
            }
        }
    }

    async fn record_admission_decided(
        &self,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        envelope: &verlet_io_core::IngressEnvelope,
        decision: &verlet_io_core::AdmissionDecision,
        policy_hash: &str,
        ingress_event_ids: Vec<verlet_history::EventRecordId>,
        coalesced: bool,
        durable: bool,
    ) -> verlet_io_core::IoResult<verlet_history::EventRecord> {
        let handle = self
            .supervisor
            .get_thread_at(coordinates)
            .await
            .map_err(verlet_bridge_error)?;
        let route_id = route_id_for_envelope(envelope);
        let context = crate::kernel::admission::AdmissionGateContext::route_policy(
            route_id,
            policy_hash.to_string(),
            if coalesced {
                verlet_history::AdmissionDecision::Coalesce
            } else {
                event_admission_decision(decision)
            },
            admissible_decisions_for_envelope(envelope),
            ingress_event_ids,
        );
        if !durable {
            return crate::kernel::admission::append_admission_decided(&handle, context)
                .await
                .map_err(verlet_bridge_error);
        }
        let record =
            crate::kernel::admission::admission_decided_record(coordinates.clone(), context)
                .map_err(verlet_bridge_error)?;
        let desired: verlet_history::AdmissionDecidedPayload =
            serde_json::from_value(record.payload.clone()).map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("decode admission decision payload: {err}"))
            })?;
        let store = self.ingress_event_store().await?;
        let stream_id = crate::kernel::control_decision::control_stream_id(coordinates);
        loop {
            let events = store
                .read_events(&stream_id, None)
                .await
                .map_err(verlet_history_error)?;
            if let Some(existing) = events.iter().find(|event| {
                event.kind == verlet_history::EventKind::AdmissionDecided
                    && serde_json::from_value::<verlet_history::AdmissionDecidedPayload>(
                        event.payload.clone(),
                    )
                    .is_ok_and(|payload| {
                        payload.source_ingress_event_ids == desired.source_ingress_event_ids
                    })
            }) {
                let existing_payload: verlet_history::AdmissionDecidedPayload =
                    serde_json::from_value(existing.payload.clone()).map_err(|err| {
                        verlet_io_core::IoError::Bridge(format!(
                            "decode existing admission decision: {err}"
                        ))
                    })?;
                if existing_payload != desired {
                    return Err(verlet_io_core::IoError::Bridge(
                        "durable ingress admission decision changed under redelivery".to_string(),
                    ));
                }
                return Ok(existing.clone());
            }
            let expected_next_sequence = events
                .last()
                .map(|event| verlet_history::EventSequence::new(event.sequence.get() + 1))
                .unwrap_or_else(|| verlet_history::EventSequence::new(1));
            match store
                .append_events_fenced(&stream_id, expected_next_sequence, vec![record.clone()])
                .await
            {
                Ok(mut appended) => {
                    return appended.pop().ok_or_else(|| {
                        verlet_io_core::IoError::Bridge(
                            "admission decision append returned no record".to_string(),
                        )
                    });
                }
                Err(verlet_history::HistoryError::AppendFenceConflict { .. }) => continue,
                Err(err) => return Err(verlet_history_error(err)),
            }
        }
    }

    async fn ensure_thread(
        &self,
        target: &verlet_io_core::ResolvedIoTarget,
        envelope: &verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<(
        verlet_runtime_contracts::ThreadCoordinates,
        crate::kernel::runtime_host::RuntimeThreadHandle,
    )> {
        if let Some(thread_id) = &target.address.thread_id {
            let thread_id =
                verlet_runtime_contracts::ThreadId::parse_str(thread_id).map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!("invalid target thread id: {err}"))
                })?;
            let coordinates = verlet_runtime_contracts::ThreadCoordinates {
                tenant_id: target.address.tenant_id.clone(),
                user_id: target.address.user_id.clone(),
                session_id: target.address.session_id.clone(),
                thread_id,
            };
            let handle = self
                .supervisor
                .get_thread_at(&coordinates)
                .await
                .map_err(verlet_bridge_error)?;
            return Ok((coordinates, handle));
        }

        let scope_key = target.address.scope_key();
        let durable_binding = self.durable_ingress_binding(envelope).await?;
        let scope_lock = self.thread_scope_lock(&scope_key).await;
        let _scope_guard = scope_lock.lock().await;

        let mut reserved_coordinates = {
            let threads = self.threads.lock().await;
            threads.get(&scope_key).cloned()
        };
        if let Some(coordinates) = &reserved_coordinates {
            let store = self
                .supervisor
                .runtime_store(&coordinates.tenant_id)
                .await
                .map_err(verlet_bridge_error)?;
            let events = store
                .read_events(
                    &verlet_history::EventStreamId::for_thread(coordinates),
                    None,
                )
                .await
                .map_err(verlet_history_error)?;
            if !events.is_empty() {
                match self.get_or_load_thread_handle(coordinates).await {
                    Ok(handle) => return Ok((coordinates.clone(), handle)),
                    Err(ThreadHandleResolutionError::LifecycleLoad(_)) => {
                        let mut threads = self.threads.lock().await;
                        if threads.get(&scope_key) == Some(coordinates) {
                            threads.remove(&scope_key);
                        }
                        drop(threads);
                        if let Some((route_id, source_scope, state)) = &durable_binding {
                            let route_id = route_id.clone();
                            let source_scope = source_scope.clone();
                            let scope_key = scope_key.clone();
                            let thread_id = coordinates.thread_id;
                            state
                                .run_blocking(move |state| {
                                    state.clear_ingress_thread_binding_if_matches(
                                        &route_id,
                                        &source_scope,
                                        &scope_key,
                                        thread_id,
                                    )
                                })
                                .await?;
                        }
                        reserved_coordinates = None;
                    }
                    Err(ThreadHandleResolutionError::Lookup(err)) => {
                        return Err(verlet_bridge_error(err));
                    }
                }
            }
        }

        let topology = target
            .parent_thread_id
            .as_deref()
            .map(verlet_runtime_contracts::ThreadId::parse_str)
            .transpose()
            .map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("invalid parent thread id: {err}"))
            })?
            .map(verlet_runtime_contracts::ThreadTopology::spawned_from)
            .unwrap_or_else(verlet_runtime_contracts::ThreadTopology::root);
        let agent_binding = self.route_agent_binding(target).await?;
        let metadata = agent_binding
            .as_ref()
            .map(|binding| binding.metadata.clone())
            .unwrap_or_default();

        let request = || crate::kernel::supervisor::ThreadStartRequest {
            tenant_id: target.address.tenant_id.clone(),
            user_id: target.address.user_id.clone(),
            session_id: target.address.session_id.clone(),
            topology: topology.clone(),
            metadata: metadata.clone(),
        };
        let (coordinates, handle) = if let Some((route_id, source_scope, state)) = durable_binding {
            let candidate = verlet_runtime_contracts::ThreadCoordinates {
                tenant_id: target.address.tenant_id.clone(),
                user_id: target.address.user_id.clone(),
                session_id: target.address.session_id.clone(),
                thread_id: verlet_runtime_contracts::ThreadId::new(),
            };
            #[cfg(test)]
            self.initial_root_candidates
                .lock()
                .unwrap_or_else(|err| err.into_inner())
                .push(candidate.clone());
            #[cfg(test)]
            {
                let barrier = self
                    .ingress_binding_barrier
                    .lock()
                    .unwrap_or_else(|err| err.into_inner())
                    .clone();
                if let Some(barrier) = barrier {
                    barrier.wait().await;
                }
            }
            let scope_key = target.address.scope_key();
            let requested_coordinates = reserved_coordinates.as_ref().unwrap_or(&candidate).clone();
            let selected = match state
                .run_blocking(move |state| {
                    state.claim_ingress_thread_binding(
                        &route_id,
                        &source_scope,
                        &scope_key,
                        &requested_coordinates,
                    )
                })
                .await
            {
                Ok(selected) => selected,
                Err(err) => return Err(err),
            };
            let handle = self
                .start_or_adopt_reserved_root(request(), &selected, agent_binding.as_ref())
                .await?;
            pause_after_ingress_binding_for_restart_smoke().await?;
            (selected, handle)
        } else {
            let handle = self
                .start_thread_with_manifest_witness(request(), None, agent_binding)
                .await
                .map_err(verlet_bridge_error)?;
            (handle.context().coordinates.clone(), handle)
        };
        self.threads
            .lock()
            .await
            .insert(scope_key, coordinates.clone());
        Ok((coordinates, handle))
    }

    async fn start_thread_with_manifest_witness(
        &self,
        request: crate::kernel::supervisor::ThreadStartRequest,
        reserved_thread_id: Option<verlet_runtime_contracts::ThreadId>,
        agent_binding: Option<crate::agent::agent_process::KernelThreadSpawnAgentBinding>,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        let supervisor = self.supervisor.clone();
        tokio::spawn(async move {
            let handle = match reserved_thread_id {
                Some(thread_id) => supervisor.start_thread_with_id(request, thread_id).await?,
                None => supervisor.start_thread(request).await?,
            };
            if let Some(binding) = agent_binding
                && let Err(err) = handle
                    .record_manifest_receipts_for_principal(
                        binding.compile_receipt,
                        binding.bind_receipt,
                        &binding.principal_id,
                    )
                    .await
            {
                let _ = supervisor
                    .shutdown_thread_at(&handle.context().coordinates)
                    .await;
                return Err(err);
            }
            Ok(handle)
        })
        .await
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "daemon thread manifest witness task failed: {err}"
            ))
        })?
    }

    async fn start_or_adopt_reserved_root(
        &self,
        request: crate::kernel::supervisor::ThreadStartRequest,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        agent_binding: Option<&crate::agent::agent_process::KernelThreadSpawnAgentBinding>,
    ) -> verlet_io_core::IoResult<crate::kernel::runtime_host::RuntimeThreadHandle> {
        loop {
            let store = self
                .supervisor
                .runtime_store(&coordinates.tenant_id)
                .await
                .map_err(verlet_bridge_error)?;
            let events = store
                .read_events(
                    &verlet_history::EventStreamId::for_thread(coordinates),
                    None,
                )
                .await
                .map_err(verlet_history_error)?;
            if !events.is_empty() {
                return self
                    .get_or_load_thread_handle(coordinates)
                    .await
                    .map_err(|err| verlet_bridge_error(err.into_inner()));
            }
            match self
                .start_thread_with_manifest_witness(
                    request.clone(),
                    Some(coordinates.thread_id),
                    agent_binding.cloned(),
                )
                .await
            {
                Ok(handle) => return Ok(handle),
                Err(crate::kernel::runtime_host::VerletError::ThreadAlreadyExists(existing))
                    if existing == coordinates.thread_id =>
                {
                    self.supervisor
                        .wait_for_thread_start_reservation(
                            &coordinates.tenant_id,
                            coordinates.thread_id,
                        )
                        .await
                        .map_err(verlet_bridge_error)?;
                    match self.supervisor.get_thread_at(coordinates).await {
                        Ok(handle) => return Ok(handle),
                        Err(crate::kernel::runtime_host::VerletError::ThreadNotFound(_)) => {
                            continue;
                        }
                        Err(err) => return Err(verlet_bridge_error(err)),
                    }
                }
                Err(err) => return Err(verlet_bridge_error(err)),
            }
        }
    }

    async fn thread_scope_lock(&self, scope_key: &str) -> std::sync::Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.thread_scope_locks.lock().await;
        locks
            .entry(scope_key.to_string())
            .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn lock_active_turns(
        &self,
    ) -> std::sync::MutexGuard<'_, std::collections::HashMap<String, String>> {
        self.active_turns
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn clear_active_turn_if_matches(&self, scope_key: &str, completed_turn_id: &str) {
        let mut active_turns = self.lock_active_turns();
        if active_turns
            .get(scope_key)
            .is_some_and(|active_turn_id| active_turn_id == completed_turn_id)
        {
            active_turns.remove(scope_key);
        }
    }

    async fn durable_ingress_binding(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<Option<(String, String, std::sync::Arc<DaemonEgressState>)>> {
        let threading = envelope
            .metadata
            .get("cooldis_route_threading")
            .map(String::as_str)
            .unwrap_or("per_conversation");
        if threading != "per_conversation" {
            return Ok(None);
        }

        let route_id = route_id_for_ingress(envelope);
        let source_scope = source_scope(&envelope.source.protocol, &route_id);
        let route_requires_state = self
            .egress_route_configs
            .read()
            .await
            .get(&source_scope)
            .is_some_and(RouteEgressConfig::restores_per_conversation_bindings);
        let state = self.egress_states.read().await.get(&source_scope).cloned();
        match state {
            Some(state) => Ok(Some((route_id, source_scope, state))),
            None if route_requires_state => Err(verlet_io_core::IoError::Bridge(format!(
                "durable route state for {source_scope:?} is not ready"
            ))),
            None => Ok(None),
        }
    }

    async fn route_agent_binding(
        &self,
        target: &verlet_io_core::ResolvedIoTarget,
    ) -> verlet_io_core::IoResult<Option<crate::agent::agent_process::KernelThreadSpawnAgentBinding>>
    {
        let Some(agent_ref) = target
            .metadata
            .get(ROUTE_AGENT_REF_METADATA)
            .filter(|agent_ref| !agent_ref.trim().is_empty())
        else {
            return Ok(None);
        };
        let app_server = self.app_server.as_ref().ok_or_else(|| {
            verlet_io_core::IoError::Bridge(
                "daemon route agent_ref requires daemon IO to be backed by an app-server"
                    .to_string(),
            )
        })?;
        app_server
            .bind_daemon_route_agent(agent_ref)
            .await
            .map(Some)
            .map_err(verlet_bridge_error)
    }

    fn runtime_input(
        &self,
        input: &verlet_io_core::IoTurnInput,
    ) -> crate::kernel::runtime_host::turn::TurnInput {
        let policy = input.provider_policy.clone().unwrap_or_else(|| {
            verlet_io_core::ProviderPolicy::new(self.model_provider.clone(), self.model.clone())
        });
        let mut turn = crate::kernel::runtime_host::turn::TurnInput::text(input.text.clone())
            .with_provider(policy.provider)
            .with_model(policy.model)
            .with_cwd(self.cwd.clone());
        for (key, value) in &input.metadata {
            turn = turn.with_metadata(key.clone(), value.clone());
        }
        for attachment in &input.attachments {
            turn = turn.with_metadata(
                format!("attachment:{}", attachment.id),
                attachment
                    .name
                    .clone()
                    .unwrap_or_else(|| attachment.media_type.clone()),
            );
        }
        turn
    }

    async fn apply_fork_admission(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        child_key: &str,
        input: &verlet_io_core::IoTurnInput,
        ingress_message_ids: &[String],
        ingress_source_stream: Option<&verlet_history::EventStreamId>,
        source_ingress_event_ids: &[verlet_history::EventRecordId],
        admission_event_id: Option<verlet_history::EventRecordId>,
        ingress_ownership: Option<&IngressOwnershipReservation>,
    ) -> verlet_io_core::IoResult<(verlet_io_core::KernelIoReceipt, Option<String>)> {
        let (parent_coordinates, parent_handle) = self.ensure_thread(target, envelope).await?;
        let scope_key = target.address.scope_key();
        let scope_lock = self.thread_scope_lock(&scope_key).await;
        let _scope_guard = scope_lock.lock().await;
        if ingress_message_ids.is_empty() {
            let (receipt, _) = self
                .run_fork_effects(
                    envelope,
                    target,
                    &parent_coordinates,
                    &parent_handle,
                    child_key,
                    input,
                    ingress_source_stream,
                    source_ingress_event_ids,
                    None,
                    verlet_runtime_contracts::ThreadId::new(),
                    false,
                )
                .await?;
            return Ok((receipt, None));
        }
        let admission_event_id = admission_event_id.ok_or_else(|| {
            verlet_io_core::IoError::Bridge(
                "durable ingress claim requires admission evidence".to_string(),
            )
        })?;
        let source_stream = ingress_source_stream.ok_or_else(|| {
            verlet_io_core::IoError::Bridge(
                "durable ingress claim requires its control stream".to_string(),
            )
        })?;
        let reserved_child_thread_id = verlet_runtime_contracts::ThreadId::new();
        let claim = self
            .append_ingress_claim(
                &parent_coordinates,
                ingress_message_ids,
                source_ingress_event_ids,
                admission_event_id,
                Self::ingress_claim_intent(
                    &verlet_io_core::AdmissionDecision::Fork {
                        child_key: child_key.to_string(),
                        input: input.clone(),
                    },
                    Some(reserved_child_thread_id),
                )?,
                ingress_ownership,
            )
            .await?;
        let claim = match claim {
            IngressClaimAppend::Appended(claim) => claim,
            IngressClaimAppend::Existing(state @ IngressOutcomeState::Claimed { .. }) => {
                let receipt = fork_claim_loser_receipt(envelope, target.clone(), &state);
                return Ok((
                    receipt,
                    ingress_outcome_turn_id(&state).map(ToOwned::to_owned),
                ));
            }
            IngressClaimAppend::Existing(state) => {
                let receipt = deduplicated_ingress_receipt(envelope, target.clone(), &state);
                return Ok((receipt, Some(child_key.to_string())));
            }
        };
        let claim_payload = serde_json::from_value::<verlet_history::IoIngressClaimedPayload>(
            claim.payload.clone(),
        )
        .map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("decode appended ingress claim: {err}"))
        })?;
        let reserved_child_thread_id = match &claim_payload.intent {
            verlet_history::IngressOutcomeIntent::Fork {
                child_thread_id: Some(child_thread_id),
                ..
            } => *child_thread_id,
            verlet_history::IngressOutcomeIntent::Fork {
                child_thread_id: None,
                ..
            } => {
                return Err(verlet_io_core::IoError::Bridge(format!(
                    "newly appended fork claim {} is missing its reserved child thread id",
                    claim.id
                )));
            }
            _ => {
                return Err(verlet_io_core::IoError::Bridge(
                    "appended fork claim carried a non-fork intent".to_string(),
                ));
            }
        };
        let (receipt, spawned) = self
            .run_fork_effects(
                envelope,
                target,
                &parent_coordinates,
                &parent_handle,
                child_key,
                input,
                Some(source_stream),
                &claim_payload.ingress_witness_event_ids,
                Some(claim.id),
                reserved_child_thread_id,
                false,
            )
            .await?;
        self.append_ingress_settle(
            &parent_coordinates,
            &claim,
            &claim_payload,
            Some(spawned.id),
            verlet_history::IngressSettledBy::Execution,
        )
        .await?;
        Ok((receipt, None))
    }

    async fn run_fork_effects(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        parent_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        parent_handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        child_key: &str,
        input: &verlet_io_core::IoTurnInput,
        ingress_source_stream: Option<&verlet_history::EventStreamId>,
        source_ingress_event_ids: &[verlet_history::EventRecordId],
        claim_event_id: Option<verlet_history::EventRecordId>,
        reserved_child_thread_id: verlet_runtime_contracts::ThreadId,
        scan_for_existing_spawn: bool,
    ) -> verlet_io_core::IoResult<(verlet_io_core::KernelIoReceipt, verlet_history::EventRecord)>
    {
        let existing_spawn = match (claim_event_id, scan_for_existing_spawn) {
            (Some(claim_event_id), true) => {
                self.fork_spawned_for_claim(parent_coordinates, claim_event_id)
                    .await?
            }
            _ => None,
        };
        let (child_handle, spawned, recovering_spawned_child) = match existing_spawn {
            Some((spawned, payload)) => {
                if payload.child_thread_id != reserved_child_thread_id {
                    return Err(verlet_io_core::IoError::Bridge(format!(
                        "fork claim reserved child {reserved_child_thread_id}, but thread.spawned names {}",
                        payload.child_thread_id
                    )));
                }
                let child_coordinates = verlet_runtime_contracts::ThreadCoordinates {
                    tenant_id: parent_coordinates.tenant_id.clone(),
                    user_id: parent_coordinates.user_id.clone(),
                    session_id: parent_coordinates.session_id.clone(),
                    thread_id: payload.child_thread_id,
                };
                let child_handle = self
                    .get_or_load_thread_handle(&child_coordinates)
                    .await
                    .map_err(|err| verlet_bridge_error(err.into_inner()))?;
                (child_handle, spawned, true)
            }
            None => {
                let inherited_manifest_receipts = self
                    .inherited_workspace_manifest_receipts(parent_handle)
                    .await?;
                let checkpoint = self
                    .supervisor
                    .create_checkpoint_at(
                        parent_coordinates,
                        None,
                        Some("daemon-io-fork".to_string()),
                        parent_handle.context().metadata.clone(),
                    )
                    .await
                    .map_err(verlet_bridge_error)?;
                let child_handle = self
                    .fork_thread_with_manifest_witness(
                        checkpoint.clone(),
                        reserved_child_thread_id,
                        inherited_manifest_receipts,
                    )
                    .await
                    .map_err(verlet_bridge_error)?;
                #[cfg(test)]
                if self
                    .pause_after_fork_creation
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    self.fork_creation_paused.notify_waiters();
                    std::future::pending::<()>().await;
                }
                let source_cut = self
                    .fork_source_cut_for_child(parent_handle, &child_handle, &checkpoint)
                    .await?;
                let spawned = self
                    .append_fork_thread_spawned_event(
                        parent_handle,
                        parent_coordinates,
                        &child_handle,
                        &source_cut,
                        claim_event_id,
                    )
                    .await?;
                #[cfg(test)]
                if self
                    .pause_after_fork_spawn
                    .load(std::sync::atomic::Ordering::SeqCst)
                {
                    self.fork_spawn_paused.notify_waiters();
                    std::future::pending::<()>().await;
                }
                (child_handle, spawned, false)
            }
        };
        let child_coordinates = child_handle.context().coordinates.clone();
        self.rebind_ingress_thread(envelope, target, &child_coordinates)
            .await?;
        self.threads
            .lock()
            .await
            .insert(target.address.scope_key(), child_coordinates.clone());
        let child_events = child_handle
            .read_thread_events(None)
            .await
            .map_err(verlet_bridge_error)?;
        if !child_events.iter().any(|event| {
            event.kind == verlet_history::EventKind::TurnSubmitted
                && event
                    .payload
                    .get("turn_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(child_key)
        }) {
            self.append_ingress_turn_submitted_event(
                &child_handle,
                envelope,
                target,
                child_key,
                ingress_source_stream,
                source_ingress_event_ids,
            )
            .await?;
        }
        if !recovering_spawned_child
            || turn_execution_evidence(
                &child_events,
                child_key,
                verlet_runtime_contracts::TurnSubmissionMode::Queue,
            )
            .is_none()
        {
            self.lock_active_turns()
                .insert(target.address.scope_key(), child_key.to_string());
            let reserved = match self
                .supervisor
                .reserve_admitted_turn_to(
                    &child_coordinates,
                    child_key.to_string(),
                    self.runtime_input(input),
                    verlet_runtime_contracts::TurnSubmissionMode::Queue,
                    None,
                )
                .await
            {
                Ok(reserved) => reserved,
                Err(err) => {
                    self.clear_active_turn_if_matches(&target.address.scope_key(), child_key);
                    return Err(verlet_bridge_error(err));
                }
            };
            crate::kernel::admission::submit_reserved(reserved).await;
        }

        let mut receipt_target = target.clone();
        receipt_target.address.thread_id = Some(child_coordinates.thread_id.to_string());
        let mut receipt = verlet_io_core::KernelIoReceipt::new(
            envelope,
            receipt_target,
            &verlet_io_core::AdmissionDecision::Fork {
                child_key: child_key.to_string(),
                input: input.clone(),
            },
        );
        receipt.thread_id = Some(child_coordinates.thread_id.to_string());
        Ok((receipt, spawned))
    }

    async fn inherited_workspace_manifest_receipts(
        &self,
        parent: &crate::kernel::runtime_host::RuntimeThreadHandle,
    ) -> verlet_io_core::IoResult<Option<(serde_json::Value, serde_json::Value)>> {
        let Some(raw) = parent
            .context()
            .metadata
            .get(crate::adapters::app_server::THREAD_AGENT_WORKSPACE_METADATA)
        else {
            return Ok(None);
        };
        let stored = serde_json::from_str::<
            crate::agent::manifest_bind::AgentManifestResolvedWorkspaceMount,
        >(raw)
        .map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("parent workspace metadata is invalid: {err}"))
        })?;
        let (compile_payload, bind_payload) =
            crate::adapters::app_server::threads::active_manifest_receipt_payloads(parent)
                .await
                .map_err(verlet_bridge_error)?
                .ok_or_else(|| {
                    verlet_io_core::IoError::Bridge(
                        "parent workspace metadata has no durable manifest bind witness"
                            .to_string(),
                    )
                })?;
        let witnessed = bind_payload
            .get("workspace")
            .cloned()
            .map(
                serde_json::from_value::<
                    crate::agent::manifest_bind::AgentManifestResolvedWorkspaceMount,
                >,
            )
            .transpose()
            .map_err(|err| {
                verlet_io_core::IoError::Bridge(format!(
                    "parent workspace bind witness is invalid: {err}"
                ))
            })?;
        if witnessed.as_ref() != Some(&stored) {
            return Err(verlet_io_core::IoError::Bridge(
                "parent workspace metadata disagrees with its durable manifest bind witness"
                    .to_string(),
            ));
        }
        Ok(Some((compile_payload, bind_payload)))
    }

    async fn fork_thread_with_manifest_witness(
        &self,
        checkpoint: crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
        child_thread_id: verlet_runtime_contracts::ThreadId,
        manifest_receipts: Option<(serde_json::Value, serde_json::Value)>,
    ) -> crate::kernel::runtime_host::VerletResult<crate::kernel::runtime_host::RuntimeThreadHandle>
    {
        let supervisor = self.supervisor.clone();
        tokio::spawn(async move {
            let child = supervisor
                .fork_thread_from_checkpoint_with_id_at(checkpoint, child_thread_id)
                .await?;
            if let Some((compile_payload, bind_payload)) = manifest_receipts
                && let Err(err) = child
                    .record_manifest_receipts(compile_payload, bind_payload)
                    .await
            {
                let _ = supervisor
                    .shutdown_thread_at(&child.context().coordinates)
                    .await;
                return Err(err);
            }
            Ok(child)
        })
        .await
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "daemon fork manifest witness task failed: {err}"
            ))
        })?
    }

    async fn fork_spawned_for_claim(
        &self,
        parent_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        claim_event_id: verlet_history::EventRecordId,
    ) -> verlet_io_core::IoResult<
        Option<(
            verlet_history::EventRecord,
            verlet_history::ThreadSpawnedPayload,
        )>,
    > {
        #[cfg(test)]
        self.fork_claim_scan_count
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let store = self.ingress_event_store().await?;
        let events = store
            .read_events(
                &crate::kernel::control_decision::control_stream_id(parent_coordinates),
                None,
            )
            .await
            .map_err(verlet_history_error)?;
        for event in events {
            if event.kind != verlet_history::EventKind::ThreadSpawned {
                continue;
            }
            let payload = serde_json::from_value::<verlet_history::ThreadSpawnedPayload>(
                event.payload.clone(),
            )
            .map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("invalid thread.spawned payload: {err}"))
            })?;
            if payload.fork.as_ref().and_then(|fork| fork.claim_event_id) == Some(claim_event_id) {
                return Ok(Some((event, payload)));
            }
        }
        Ok(None)
    }

    async fn fork_source_cut_for_child(
        &self,
        parent_handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        child_handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        attempted_checkpoint: &crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
    ) -> verlet_io_core::IoResult<verlet_history::ThreadSpawnedForkSourceCutPayload> {
        let child_context = child_handle.context();
        let verlet_runtime_contracts::ThreadLineage::Branch {
            parent_thread_id,
            checkpoint_id: Some(checkpoint_id),
        } = child_context.topology.lineage
        else {
            return Err(verlet_io_core::IoError::Bridge(format!(
                "reserved fork child {} has no checkpoint lineage",
                child_context.coordinates.thread_id
            )));
        };
        if parent_thread_id != parent_handle.context().coordinates.thread_id {
            return Err(verlet_io_core::IoError::Bridge(format!(
                "reserved fork child {} names parent {parent_thread_id}, expected {}",
                child_context.coordinates.thread_id,
                parent_handle.context().coordinates.thread_id
            )));
        }
        if checkpoint_id == attempted_checkpoint.id {
            return Ok(fork_source_cut_payload(
                &parent_handle.context().coordinates,
                attempted_checkpoint,
                None,
            ));
        }

        let parent_context = parent_handle
            .session_context()
            .await
            .map_err(verlet_bridge_error)?;
        let checkpoint_id_text = checkpoint_id.to_string();
        let checkpoint_entry = parent_context.entries.iter().rev().find(|entry| {
            matches!(
                &entry.kind,
                verlet_history::SessionEntryKind::Runtime { kind, payload }
                    if kind == "thread_checkpoint"
                        && payload.get("checkpoint_id").and_then(serde_json::Value::as_str)
                            == Some(checkpoint_id_text.as_str())
            )
        });
        let checkpoint_entry = checkpoint_entry.ok_or_else(|| {
            verlet_io_core::IoError::Bridge(format!(
                "reserved fork child {} cites checkpoint {checkpoint_id}, but the parent has no matching durable checkpoint",
                child_context.coordinates.thread_id
            ))
        })?;
        Ok(verlet_history::ThreadSpawnedForkSourceCutPayload {
            thread_id: parent_thread_id,
            checkpoint_id,
            leaf_entry_id: Some(checkpoint_entry.entry_id),
            stream_id: verlet_history::EventStreamId::for_thread(
                &parent_handle.context().coordinates,
            ),
            stream_to_sequence: None,
        })
    }

    async fn append_fork_thread_spawned_event(
        &self,
        parent_handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        parent_coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        child_handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        source_cut: &verlet_history::ThreadSpawnedForkSourceCutPayload,
        claim_event_id: Option<verlet_history::EventRecordId>,
    ) -> verlet_io_core::IoResult<verlet_history::EventRecord> {
        let child_context = child_handle.context();
        let metadata = &child_context.metadata;
        let child_manifest_hash = metadata
            .get(crate::kernel::runtime_host::THREAD_AGENT_MANIFEST_HASH_METADATA)
            .cloned()
            .unwrap_or_else(|| "unbound".to_string());
        let fork = verlet_history::ThreadSpawnedForkPayload {
            mode: "clone".to_string(),
            claim_event_id,
            source_cut: source_cut.clone(),
        };
        let inputs_context = serde_json::json!({
            "operation": "thread/fork",
            "fork": &fork,
        });
        let inputs_hash = crate::agent::manifest_bind::canonical_json_hash(&inputs_context)
            .map_err(verlet_bridge_error)?;
        let payload = verlet_history::ThreadSpawnedPayload {
            parent_thread_id: parent_coordinates.thread_id,
            parent_turn_id: None,
            child_thread_id: child_context.coordinates.thread_id,
            child_manifest_hash,
            granted: Vec::new(),
            child_policy_hash: None,
            inputs_hash,
            fork: Some(fork),
        };
        let mut value = serde_json::to_value(payload).map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("thread.spawned payload codec failed: {err}"))
        })?;
        let object = value.as_object_mut().ok_or_else(|| {
            verlet_io_core::IoError::Bridge(
                "thread.spawned payload did not encode as object".to_string(),
            )
        })?;
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::ThreadSpawned.payload_schema_id()),
        );
        parent_handle
            .append_control_event(verlet_history::NewEventRecord::witnessed(
                parent_coordinates.clone(),
                verlet_history::EventKind::ThreadSpawned,
                value,
            ))
            .await
            .map_err(verlet_bridge_error)
    }

    async fn bind_egress_thread(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_io_core::IoResult<()> {
        let route_id = route_id_for_ingress(envelope);
        let key = source_scope(&envelope.source.protocol, &route_id);
        let state = self.egress_states.read().await.get(&key).cloned();
        if let Some(state) = state {
            let scope_key = target.address.scope_key();
            let coordinates = coordinates.clone();
            state
                .run_blocking(move |state| {
                    state.bind_thread(&route_id, &key, &scope_key, &coordinates)
                })
                .await?;
        }
        Ok(())
    }

    async fn rebind_ingress_thread(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    ) -> verlet_io_core::IoResult<()> {
        let route_id = route_id_for_ingress(envelope);
        let key = source_scope(&envelope.source.protocol, &route_id);
        let state = self.egress_states.read().await.get(&key).cloned();
        if let Some(state) = state {
            let scope_key = target.address.scope_key();
            let coordinates = coordinates.clone();
            state
                .run_blocking(move |state| {
                    state.rebind_ingress_thread(&route_id, &key, &scope_key, &coordinates)
                })
                .await?;
        }
        Ok(())
    }

    async fn append_ingress_turn_submitted_event(
        &self,
        handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        envelope: &verlet_io_core::IngressEnvelope,
        _target: &verlet_io_core::ResolvedIoTarget,
        turn_id: &str,
        ingress_source_stream: Option<&verlet_history::EventStreamId>,
        source_ingress_event_ids: &[verlet_history::EventRecordId],
    ) -> verlet_io_core::IoResult<verlet_history::EventRecord> {
        if let Some(existing) = handle
            .read_thread_events(None)
            .await
            .map_err(verlet_bridge_error)?
            .into_iter()
            .find(|event| {
                event.kind == verlet_history::EventKind::TurnSubmitted
                    && event
                        .payload
                        .get("turn_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(turn_id)
                    && event
                        .payload
                        .get("ingress_envelope_id")
                        .and_then(serde_json::Value::as_str)
                        == Some(envelope.id.as_str())
            })
        {
            return Ok(existing);
        }
        let route_id = route_id_for_ingress(envelope);
        let mut payload = serde_json::to_value(verlet_history::IoIngressReceivedPayload {
            route_id: Some(route_id.clone()),
            dedupe_key: envelope
                .effective_dedupe_key()
                .as_ref()
                .map(|key| key.stable_key()),
            external_conversation_id: Some(envelope.conversation.external_conversation_id.clone()),
            external_actor_id: envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.clone()),
            external_message_id: envelope.metadata.get("telegram_message_id").cloned(),
            content: witnessed_ingress_content(envelope)?,
            envelope_digest: ingress_envelope_digest(envelope)?,
        })
        .map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("encode ingress receipt payload: {err}"))
        })?;
        let object = payload.as_object_mut().ok_or_else(|| {
            verlet_io_core::IoError::Bridge(
                "ingress receipt payload did not encode as object".to_string(),
            )
        })?;
        object.insert(
            "schema".to_string(),
            serde_json::json!(verlet_history::EventKind::TurnSubmitted.payload_schema_id()),
        );
        object.insert(
            "turn_id".to_string(),
            serde_json::Value::String(turn_id.to_string()),
        );
        object.insert(
            "source_scope".to_string(),
            serde_json::Value::String(envelope.source.stable_scope()),
        );
        object.insert(
            "ingress_envelope_id".to_string(),
            serde_json::Value::String(envelope.id.clone()),
        );
        object.insert(
            "target".to_string(),
            serde_json::to_value(verlet_io_core::IoTarget::reply_to(envelope)).map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("encode ingress target: {err}"))
            })?,
        );
        object.insert(
            "ingress_metadata".to_string(),
            serde_json::to_value(&envelope.metadata).map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("encode ingress metadata: {err}"))
            })?,
        );
        if !source_ingress_event_ids.is_empty() && ingress_source_stream.is_none() {
            return Err(verlet_io_core::IoError::Bridge(
                "derived ingress turn submission requires its control source stream".to_string(),
            ));
        }

        let record = || {
            if source_ingress_event_ids.is_empty() {
                return verlet_history::NewEventRecord::witnessed(
                    handle.context().coordinates.clone(),
                    verlet_history::EventKind::TurnSubmitted,
                    payload.clone(),
                );
            }
            verlet_history::NewEventRecord::discharged(
                handle.context().coordinates.clone(),
                verlet_history::EventKind::TurnSubmitted,
                payload.clone(),
                verlet_history::EventProvenance {
                    source_streams: ingress_source_stream.cloned().into_iter().collect(),
                    source_event_ids: source_ingress_event_ids.to_vec(),
                    discharged_by: Some("projector:io-ingress-apply".to_string()),
                    function: Some("ingress_turn_submit/v1".to_string()),
                    ..verlet_history::EventProvenance::default()
                },
            )
        };
        handle
            .append_thread_event_record(record())
            .await
            .map_err(verlet_bridge_error)
    }

    async fn run_egress_projector(self, protocol: String, instance_id: String) {
        let poll_interval = std::time::Duration::from_millis(DEFAULT_EGRESS_PROJECTOR_POLL_MS);
        loop {
            let _ = self.drain_egress_once(&protocol, &instance_id).await;
            tokio::time::sleep(poll_interval).await;
        }
    }

    #[cfg(test)]
    async fn deliver_egress(&self, envelope: verlet_io_core::EgressEnvelope) {
        let key = envelope.target.source.stable_scope();
        let adapter = self.egress_adapters.read().await.get(&key).cloned();
        let Some(adapter) = adapter else {
            return;
        };
        let route_config = self
            .egress_route_configs
            .read()
            .await
            .get(&key)
            .cloned()
            .unwrap_or_default();
        for envelope in route_config.project(envelope) {
            if let Some(typing) = &route_config.typing_simulation
                && let verlet_io_core::EgressKind::AssistantMessage { text } = &envelope.kind
                && !text.is_empty()
            {
                let typing_envelope = sibling_egress(
                    &envelope,
                    verlet_io_core::EgressKind::PlatformAction {
                        action: "typing".to_string(),
                        payload: serde_json::Value::Object(serde_json::Map::new()),
                    },
                );
                let _ = adapter.deliver(typing_envelope).await;
                let delay = typing_delay_for_text(text, typing.chars_per_second);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }
            let _ = adapter.deliver(envelope).await;
        }
    }

    async fn drain_thread_egress(
        &self,
        route_key: &str,
        state: &DaemonEgressState,
        binding: &BoundEgressThread,
        handle: crate::kernel::runtime_host::RuntimeThreadHandle,
        adapter: Option<&dyn verlet_io_core::EgressAdapter>,
        route_config: &RouteEgressConfig,
    ) -> verlet_io_core::IoResult<usize> {
        let thread_id = binding.coordinates.thread_id.to_string();
        let view_key = (route_key.to_string(), thread_id.clone());
        let view_slot = {
            let mut views = self.egress_drain_views.lock().await;
            views
                .entry(view_key)
                .or_insert_with(|| std::sync::Arc::new(tokio::sync::Mutex::new(None)))
                .clone()
        };
        let mut view_slot = view_slot.lock().await;
        let persisted_cursor = state.cursor(&binding.route_id, &thread_id)?;
        let mut rebuild = view_slot
            .as_ref()
            .is_none_or(|view| view.observed_delivery_cursor.as_ref() != persisted_cursor.as_ref());
        let events = if rebuild {
            handle
                .read_thread_events(None)
                .await
                .map_err(verlet_bridge_error)?
        } else {
            let view = view_slot.as_ref().expect("checked drain view");
            match &view.fold_position {
                Some(cursor) => match handle.read_thread_events_after_cursor(cursor).await {
                    Ok(events) => events,
                    Err(_) => {
                        rebuild = true;
                        handle
                            .read_thread_events(None)
                            .await
                            .map_err(verlet_bridge_error)?
                    }
                },
                None => handle
                    .read_thread_events(Some(verlet_history::EventSequence::new(1)))
                    .await
                    .map_err(verlet_bridge_error)?,
            }
        };
        let context = handle
            .session_context()
            .await
            .map_err(verlet_bridge_error)?;
        if rebuild {
            let effective_cursor = verified_delivery_cursor(
                &events,
                &verlet_history::EventStreamId::for_thread(&binding.coordinates),
                persisted_cursor.as_ref(),
            );
            *view_slot = Some(DrainEgressView::new(
                persisted_cursor.clone(),
                effective_cursor,
            ));
        }
        let view = view_slot.as_mut().expect("drain view initialized");
        fold_drain_egress_events(view, &events, &context.entries, route_config)?;

        let mut delivered_sources = 0;
        while let Some(work) = view.undelivered_requested_egress.front().cloned() {
            match work {
                DrainEgressWork::Advance { source } => {
                    store_drain_delivery_cursor(state, binding, view, &source.cursor)?;
                    view.undelivered_requested_egress.pop_front();
                }
                DrainEgressWork::Requested { source, template } => {
                    let outcome = self
                        .deliver_projected_envelope(
                            state,
                            binding,
                            &handle,
                            adapter,
                            &source,
                            0,
                            template.envelope(),
                            route_config.retry,
                            &mut view.receipt_dedupe_cursors,
                        )
                        .await?;
                    match outcome {
                        EnvelopeDeliveryOutcome::Delivered(cursor) => {
                            store_drain_delivery_cursor(state, binding, view, &cursor)?;
                            view.undelivered_requested_egress.pop_front();
                            delivered_sources += 1;
                        }
                        EnvelopeDeliveryOutcome::Blocked => break,
                    }
                }
                DrainEgressWork::Assistant {
                    source,
                    context,
                    text,
                } => {
                    let completed_turn_id = context.turn_id.clone();
                    match self
                        .deliver_assistant_source(
                            state,
                            binding,
                            &handle,
                            adapter,
                            route_config,
                            &source,
                            context,
                            text,
                            &mut view.receipt_dedupe_cursors,
                        )
                        .await?
                    {
                        SourceDeliveryOutcome::Completed(cursor) => {
                            store_drain_delivery_cursor(state, binding, view, &cursor)?;
                            view.undelivered_requested_egress.pop_front();
                            delivered_sources += 1;
                            if let Some(completed_turn_id) = completed_turn_id {
                                self.clear_active_turn_if_matches(
                                    &binding.scope_key,
                                    &completed_turn_id,
                                );
                            }
                        }
                        SourceDeliveryOutcome::Blocked => break,
                    }
                }
            }
        }
        Ok(delivered_sources)
    }

    async fn deliver_assistant_source(
        &self,
        state: &DaemonEgressState,
        binding: &BoundEgressThread,
        handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        adapter: Option<&dyn verlet_io_core::EgressAdapter>,
        route_config: &RouteEgressConfig,
        source_event: &DrainEgressSource,
        source_context: IngressReceiptContext,
        text: String,
        receipt_cursors: &mut std::collections::HashMap<String, ReceiptDedupeCursor>,
    ) -> verlet_io_core::IoResult<SourceDeliveryOutcome> {
        let mut envelope = verlet_io_core::EgressEnvelope::new(
            source_context.target,
            verlet_io_core::EgressKind::AssistantMessage { text },
            now_ms(),
        );
        envelope.source_ingress_id = source_context.source_ingress_id;
        envelope.metadata = source_context.metadata;

        let mut envelope_index = 0;
        let mut latest_receipt_cursor = None;
        for projected in route_config.project(envelope) {
            if let Some(typing) = &route_config.typing_simulation
                && let verlet_io_core::EgressKind::AssistantMessage { text } = &projected.kind
                && !text.is_empty()
            {
                let typing_envelope = sibling_egress(
                    &projected,
                    verlet_io_core::EgressKind::PlatformAction {
                        action: "typing".to_string(),
                        payload: serde_json::Value::Object(serde_json::Map::new()),
                    },
                );
                let outcome = self
                    .deliver_projected_envelope(
                        state,
                        binding,
                        handle,
                        adapter,
                        source_event,
                        envelope_index,
                        typing_envelope,
                        route_config.retry,
                        receipt_cursors,
                    )
                    .await?;
                match outcome {
                    EnvelopeDeliveryOutcome::Delivered(cursor) => {
                        retain_newest_cursor(&mut latest_receipt_cursor, cursor);
                    }
                    EnvelopeDeliveryOutcome::Blocked => return Ok(SourceDeliveryOutcome::Blocked),
                }
                envelope_index += 1;
                let delay = typing_delay_for_text(text, typing.chars_per_second);
                if !delay.is_zero() {
                    tokio::time::sleep(delay).await;
                }
            }

            let outcome = self
                .deliver_projected_envelope(
                    state,
                    binding,
                    handle,
                    adapter,
                    source_event,
                    envelope_index,
                    projected,
                    route_config.retry,
                    receipt_cursors,
                )
                .await?;
            match outcome {
                EnvelopeDeliveryOutcome::Delivered(cursor) => {
                    retain_newest_cursor(&mut latest_receipt_cursor, cursor);
                }
                EnvelopeDeliveryOutcome::Blocked => return Ok(SourceDeliveryOutcome::Blocked),
            }
            envelope_index += 1;
        }

        let cursor = latest_receipt_cursor.unwrap_or_else(|| source_event.cursor.clone());
        Ok(SourceDeliveryOutcome::Completed(cursor))
    }

    async fn deliver_projected_envelope(
        &self,
        state: &DaemonEgressState,
        binding: &BoundEgressThread,
        handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        adapter: Option<&dyn verlet_io_core::EgressAdapter>,
        source_event: &DrainEgressSource,
        envelope_index: usize,
        envelope: verlet_io_core::EgressEnvelope,
        retry: crate::daemon::daemon_config::VerletEgressRetryConfig,
        receipt_cursors: &mut std::collections::HashMap<String, ReceiptDedupeCursor>,
    ) -> verlet_io_core::IoResult<EnvelopeDeliveryOutcome> {
        let dedupe_key = egress_dedupe_key(source_event.id, envelope_index);
        if let Some(receipt) = matching_receipt_cursor(receipt_cursors, &dedupe_key, &envelope.kind)
        {
            return Ok(EnvelopeDeliveryOutcome::Delivered(receipt.cursor.clone()));
        }

        if matches!(envelope.kind, verlet_io_core::EgressKind::Silence { .. }) {
            let receipt = verlet_io_core::DeliveryReceipt {
                egress_id: envelope.id.clone(),
                delivered: true,
                external_message_id: None,
                error: None,
                metadata: std::collections::BTreeMap::new(),
            };
            let event = append_egress_delivered_receipt(
                handle,
                binding,
                source_event,
                envelope_index,
                &dedupe_key,
                &envelope,
                &receipt,
                1,
            )
            .await?;
            let cursor = event.cursor_v1();
            receipt_cursors.insert(
                dedupe_key,
                ReceiptDedupeCursor {
                    cursor: cursor.clone(),
                    egress_kind: egress_kind_name(&envelope.kind),
                },
            );
            return Ok(EnvelopeDeliveryOutcome::Delivered(cursor));
        }

        let Some(adapter) = adapter else {
            return Ok(EnvelopeDeliveryOutcome::Blocked);
        };
        let max_attempts = retry.max_attempts.max(1);
        let mut last_error = String::new();
        // Tracks what the loop actually spent, which is below max_attempts when a
        // permanent failure cuts it short; the receipt and dead letter record
        // the real count rather than the budget.
        let mut attempts_used = max_attempts;
        for attempt in 1..=max_attempts {
            match adapter.deliver(envelope.clone()).await {
                Ok(receipt) => {
                    let event = append_egress_delivered_receipt(
                        handle,
                        binding,
                        source_event,
                        envelope_index,
                        &dedupe_key,
                        &envelope,
                        &receipt,
                        attempt,
                    )
                    .await?;
                    let cursor = event.cursor_v1();
                    receipt_cursors.insert(
                        dedupe_key,
                        ReceiptDedupeCursor {
                            cursor: cursor.clone(),
                            egress_kind: egress_kind_name(&envelope.kind),
                        },
                    );
                    return Ok(EnvelopeDeliveryOutcome::Delivered(cursor));
                }
                Err(err) => {
                    last_error = err.to_string();
                    // A permanent failure fails the same way on every attempt,
                    // so spending the rest of the budget on it only delays the
                    // dead letter it is already headed for.
                    if matches!(err, verlet_io_core::IoError::PermanentDelivery(_)) {
                        attempts_used = attempt;
                        break;
                    }
                    if attempt < max_attempts {
                        // Prefer the remote's own figure over the backoff
                        // curve: retrying a rate limit early typically extends
                        // it rather than getting the message through sooner.
                        let delay = match err {
                            verlet_io_core::IoError::RateLimited {
                                retry_after_ms: Some(retry_after_ms),
                                ..
                            } => std::time::Duration::from_millis(retry_after_ms),
                            _ => egress_backoff_delay(retry.base_backoff_ms, attempt),
                        };
                        if !delay.is_zero() {
                            tokio::time::sleep(delay).await;
                        }
                    }
                }
            }
        }

        let event = append_egress_failed_receipt(
            handle,
            binding,
            source_event,
            envelope_index,
            &dedupe_key,
            &envelope,
            attempts_used,
            &last_error,
        )
        .await?;
        let egress_kind = egress_kind_name(&envelope.kind);
        state.push_dead_letter(&EgressDeadLetter {
            route_id: binding.route_id.clone(),
            thread_id: binding.coordinates.thread_id.to_string(),
            source_event_id: source_event.id.to_string(),
            envelope_index,
            dedupe_key: dedupe_key.clone(),
            egress_kind: egress_kind.clone(),
            attempts: attempts_used,
            error: last_error,
            envelope,
        })?;
        let cursor = event.cursor_v1();
        receipt_cursors.insert(
            dedupe_key,
            ReceiptDedupeCursor {
                cursor: cursor.clone(),
                egress_kind,
            },
        );
        Ok(EnvelopeDeliveryOutcome::Delivered(cursor))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum SourceDeliveryOutcome {
    Completed(verlet_history::StreamCursorV1),
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum EnvelopeDeliveryOutcome {
    Delivered(verlet_history::StreamCursorV1),
    Blocked,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ReceiptDedupeCursor {
    cursor: verlet_history::StreamCursorV1,
    egress_kind: String,
}

fn verified_delivery_cursor(
    events: &[verlet_history::EventRecord],
    stream_id: &verlet_history::EventStreamId,
    cursor: Option<&verlet_history::StreamCursorV1>,
) -> Option<verlet_history::StreamCursorV1> {
    let cursor = cursor?;
    if cursor.validate_stream_cursor_v1().is_err() || &cursor.stream_id != stream_id {
        return None;
    }
    events
        .iter()
        .find(|event| event.sequence == cursor.sequence && event.id == cursor.event_id)
        .map(|_| cursor.clone())
}

fn store_drain_delivery_cursor(
    state: &DaemonEgressState,
    binding: &BoundEgressThread,
    view: &mut DrainEgressView,
    cursor: &verlet_history::StreamCursorV1,
) -> verlet_io_core::IoResult<()> {
    let thread_id = binding.coordinates.thread_id.to_string();
    if view.observed_delivery_cursor.is_some() && view.effective_delivery_cursor.is_none() {
        state.replace_cursor(&binding.route_id, &thread_id, cursor)?;
    } else {
        state.store_cursor(&binding.route_id, &thread_id, cursor)?;
    }
    let retained = match &view.effective_delivery_cursor {
        Some(current)
            if current.stream_id == cursor.stream_id
                && current.sequence.get() >= cursor.sequence.get() =>
        {
            current.clone()
        }
        _ => cursor.clone(),
    };
    view.observed_delivery_cursor = Some(retained.clone());
    view.effective_delivery_cursor = Some(retained);
    Ok(())
}

fn fold_drain_egress_events(
    view: &mut DrainEgressView,
    events: &[verlet_history::EventRecord],
    entries: &[verlet_history::SessionEntry],
    route_config: &RouteEgressConfig,
) -> verlet_io_core::IoResult<()> {
    for event in events {
        if let Some((dedupe_key, receipt)) = receipt_dedupe_cursor_from_event(event) {
            view.receipt_dedupe_cursors.insert(dedupe_key, receipt);
        }
    }
    refresh_drain_session_context(view, entries, route_config);

    for event in events {
        let source = DrainEgressSource::from_event(event);
        let after_cursor =
            drain_source_is_after_delivery_cursor(&source, view.effective_delivery_cursor.as_ref());
        let mut assistant = None;

        if let Some(context) = ingress_context_from_event(event) {
            view.ingress_contexts.insert(event.id, context.clone());
            view.context_events
                .push(DrainIngressContextEvent::Ingress(event.id));
            view.pending_contexts.push(context);
        }
        if event.kind == verlet_history::EventKind::SessionEntryAppended
            && let Some(entry_id) = event
                .payload
                .get("entry_id")
                .and_then(serde_json::Value::as_str)
        {
            view.context_events.push(DrainIngressContextEvent::Session {
                source: source.clone(),
                entry_id: entry_id.to_string(),
            });
            if let Some(entry) = entries
                .iter()
                .find(|entry| entry.entry_id.to_string() == entry_id)
            {
                if session_entry_is_user_authored(entry) && !view.pending_contexts.is_empty() {
                    view.active_context = Some(view.pending_contexts.remove(0));
                }
                if let Some(text) = assistant_text_from_entry(entry)
                    && let Some(context) = view.active_context.clone()
                {
                    assistant = Some((context, text));
                }
            } else {
                view.unresolved_session_entry_ids
                    .insert(entry_id.to_string());
            }
        }

        let work = if event.kind == verlet_history::EventKind::IoEgressRequested && after_cursor {
            match requested_egress_template_from_event(event, &view.ingress_contexts) {
                Ok(Some(template)) => DrainEgressWork::Requested {
                    source: source.clone(),
                    template,
                },
                Ok(None) => DrainEgressWork::Advance {
                    source: source.clone(),
                },
                Err(err) => {
                    eprintln!(
                        "verlet egress projector skipped invalid io.egress.requested event {}: {err}",
                        event.id
                    );
                    DrainEgressWork::Advance {
                        source: source.clone(),
                    }
                }
            }
        } else if let Some((context, text)) = assistant {
            let partial = source_has_partial_projected_receipts(
                route_config,
                source.id,
                &context,
                &text,
                &view.receipt_dedupe_cursors,
            );
            if after_cursor || partial {
                DrainEgressWork::Assistant {
                    source: source.clone(),
                    context,
                    text,
                }
            } else {
                view.fold_position = Some(source.cursor);
                continue;
            }
        } else if after_cursor {
            DrainEgressWork::Advance {
                source: source.clone(),
            }
        } else {
            view.fold_position = Some(source.cursor);
            continue;
        };
        enqueue_drain_egress_work(&mut view.undelivered_requested_egress, work);
        view.fold_position = Some(source.cursor);
    }
    Ok(())
}

fn refresh_drain_session_context(
    view: &mut DrainEgressView,
    entries: &[verlet_history::SessionEntry],
    route_config: &RouteEgressConfig,
) {
    let visible_entry_ids = entries
        .iter()
        .map(|entry| entry.entry_id.to_string())
        .collect::<std::collections::HashSet<_>>();
    if view.visible_session_entry_ids == visible_entry_ids {
        return;
    }
    view.visible_session_entry_ids = visible_entry_ids;
    view.pending_contexts.clear();
    view.active_context = None;
    view.unresolved_session_entry_ids.clear();
    let mut assistants = std::collections::HashMap::<
        verlet_history::EventRecordId,
        (IngressReceiptContext, String),
    >::new();
    for event in &view.context_events {
        match event {
            DrainIngressContextEvent::Ingress(event_id) => {
                if let Some(context) = view.ingress_contexts.get(event_id) {
                    view.pending_contexts.push(context.clone());
                }
            }
            DrainIngressContextEvent::Session { source, entry_id } => {
                let Some(entry) = entries
                    .iter()
                    .find(|entry| entry.entry_id.to_string() == *entry_id)
                else {
                    view.unresolved_session_entry_ids.insert(entry_id.clone());
                    continue;
                };
                if session_entry_is_user_authored(entry) && !view.pending_contexts.is_empty() {
                    view.active_context = Some(view.pending_contexts.remove(0));
                }
                if let Some(text) = assistant_text_from_entry(entry)
                    && let Some(context) = view.active_context.clone()
                {
                    assistants.insert(source.id, (context, text));
                }
            }
        }
    }

    let session_sources = view
        .context_events
        .iter()
        .filter_map(|event| match event {
            DrainIngressContextEvent::Session { source, .. } => Some((source.id, source.clone())),
            DrainIngressContextEvent::Ingress(_) => None,
        })
        .collect::<std::collections::HashMap<_, _>>();
    let mut reconciled = std::collections::VecDeque::new();
    while let Some(work) = view.undelivered_requested_egress.pop_front() {
        let source = work.source().clone();
        if !session_sources.contains_key(&source.id) {
            reconciled.push_back(work);
            continue;
        }
        let after_cursor =
            drain_source_is_after_delivery_cursor(&source, view.effective_delivery_cursor.as_ref());
        if let Some((context, text)) = assistants.get(&source.id) {
            let partial = source_has_partial_projected_receipts(
                route_config,
                source.id,
                context,
                text,
                &view.receipt_dedupe_cursors,
            );
            if after_cursor || partial {
                reconciled.push_back(DrainEgressWork::Assistant {
                    source,
                    context: context.clone(),
                    text: text.clone(),
                });
            }
        } else if after_cursor {
            reconciled.push_back(DrainEgressWork::Advance { source });
        }
    }
    for (event_id, (context, text)) in assistants {
        if reconciled.iter().any(|work| work.source().id == event_id) {
            continue;
        }
        let Some(source) = session_sources.get(&event_id).cloned() else {
            continue;
        };
        let after_cursor =
            drain_source_is_after_delivery_cursor(&source, view.effective_delivery_cursor.as_ref());
        let partial = source_has_partial_projected_receipts(
            route_config,
            source.id,
            &context,
            &text,
            &view.receipt_dedupe_cursors,
        );
        if after_cursor || partial {
            reconciled.push_back(DrainEgressWork::Assistant {
                source,
                context,
                text,
            });
        }
    }
    reconciled
        .make_contiguous()
        .sort_by_key(|work| work.source().cursor.sequence.get());
    view.undelivered_requested_egress.clear();
    for work in reconciled {
        enqueue_drain_egress_work(&mut view.undelivered_requested_egress, work);
    }
}

fn enqueue_drain_egress_work(
    queue: &mut std::collections::VecDeque<DrainEgressWork>,
    work: DrainEgressWork,
) {
    if queue
        .iter()
        .any(|queued| queued.source().id == work.source().id)
    {
        return;
    }
    if matches!(queue.back(), Some(DrainEgressWork::Advance { .. }))
        && matches!(&work, DrainEgressWork::Advance { .. })
    {
        queue.pop_back();
    }
    queue.push_back(work);
}

fn drain_source_is_after_delivery_cursor(
    source: &DrainEgressSource,
    cursor: Option<&verlet_history::StreamCursorV1>,
) -> bool {
    cursor.is_none_or(|cursor| {
        cursor.stream_id != source.cursor.stream_id
            || source.cursor.sequence.get() > cursor.sequence.get()
    })
}

fn receipt_dedupe_cursor_from_event(
    event: &verlet_history::EventRecord,
) -> Option<(String, ReceiptDedupeCursor)> {
    if !matches!(
        event.kind,
        verlet_history::EventKind::IoEgressDelivered | verlet_history::EventKind::IoEgressFailed
    ) {
        return None;
    }
    let dedupe_key = event
        .payload
        .get("dedupe_key")
        .and_then(serde_json::Value::as_str)?
        .to_string();
    let egress_kind = event
        .payload
        .get("egress_kind")
        .and_then(serde_json::Value::as_str)?
        .to_string();
    Some((
        dedupe_key,
        ReceiptDedupeCursor {
            cursor: event.cursor_v1(),
            egress_kind,
        },
    ))
}

impl VerletDaemonIoBridge {
    fn ingress_claim_intent(
        decision: &verlet_io_core::AdmissionDecision,
        reserved_child_thread_id: Option<verlet_runtime_contracts::ThreadId>,
    ) -> verlet_io_core::IoResult<verlet_history::IngressOutcomeIntent> {
        let input_digest = |input: &verlet_io_core::IoTurnInput| {
            serde_json::to_value(input)
                .map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!("encode ingress turn input: {err}"))
                })
                .and_then(|value| {
                    crate::agent::manifest_bind::canonical_json_hash(&value)
                        .map_err(verlet_bridge_error)
                })
        };
        match decision {
            verlet_io_core::AdmissionDecision::Queue { turn_id, input } => {
                Ok(verlet_history::IngressOutcomeIntent::Turn {
                    turn_id: turn_id.clone(),
                    submission_mode: "queue".to_string(),
                    input_digest: input_digest(input)?,
                })
            }
            verlet_io_core::AdmissionDecision::Steer { turn_id, input, .. } => {
                Ok(verlet_history::IngressOutcomeIntent::Turn {
                    turn_id: turn_id.clone(),
                    submission_mode: "steer".to_string(),
                    input_digest: input_digest(input)?,
                })
            }
            verlet_io_core::AdmissionDecision::Interrupt {
                reason,
                replacement_turn_id,
                replacement,
            } => Ok(verlet_history::IngressOutcomeIntent::Interrupt {
                replacement_turn_id: replacement_turn_id.clone(),
                cancel_reason: reason.clone(),
                input_digest: match replacement {
                    Some(input) => input_digest(input)?,
                    None => {
                        crate::agent::manifest_bind::canonical_json_hash(&serde_json::Value::Null)
                            .map_err(verlet_bridge_error)?
                    }
                },
            }),
            verlet_io_core::AdmissionDecision::Fork { child_key, input } => {
                let child_thread_id = reserved_child_thread_id.ok_or_else(|| {
                    verlet_io_core::IoError::Bridge(
                        "fork ingress claim requires a reserved child thread id".to_string(),
                    )
                })?;
                Ok(verlet_history::IngressOutcomeIntent::Fork {
                    child_key: child_key.clone(),
                    child_thread_id: Some(child_thread_id),
                    input_digest: input_digest(input)?,
                })
            }
            verlet_io_core::AdmissionDecision::ObserveOnly { reason } => {
                Ok(verlet_history::IngressOutcomeIntent::Observe {
                    reason: reason.clone(),
                })
            }
            verlet_io_core::AdmissionDecision::Reject { reason, .. } => {
                Ok(verlet_history::IngressOutcomeIntent::Reject {
                    reason: reason.clone(),
                })
            }
        }
    }

    fn claimed_decision(
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        intent: &verlet_history::IngressOutcomeIntent,
    ) -> verlet_io_core::AdmissionDecision {
        let input = || verlet_io_core::IoTurnInput::from_envelope(envelope, target);
        match intent {
            verlet_history::IngressOutcomeIntent::Turn {
                turn_id,
                submission_mode,
                ..
            } if submission_mode == "steer" => {
                verlet_io_core::AdmissionDecision::steer(turn_id.clone(), None, input())
            }
            verlet_history::IngressOutcomeIntent::Turn { turn_id, .. } => {
                verlet_io_core::AdmissionDecision::queue(turn_id.clone(), input())
            }
            verlet_history::IngressOutcomeIntent::Interrupt {
                replacement_turn_id,
                cancel_reason,
                ..
            } => verlet_io_core::AdmissionDecision::Interrupt {
                reason: cancel_reason.clone(),
                replacement_turn_id: replacement_turn_id.clone(),
                replacement: replacement_turn_id.as_ref().map(|_| input()),
            },
            verlet_history::IngressOutcomeIntent::Fork { child_key, .. } => {
                verlet_io_core::AdmissionDecision::Fork {
                    child_key: child_key.clone(),
                    input: input(),
                }
            }
            verlet_history::IngressOutcomeIntent::Observe { reason } => {
                verlet_io_core::AdmissionDecision::ObserveOnly {
                    reason: reason.clone(),
                }
            }
            verlet_history::IngressOutcomeIntent::Reject { reason } => {
                verlet_io_core::AdmissionDecision::reject(reason.clone())
            }
        }
    }

    async fn complete_claimed_turn(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        coordinates: &verlet_runtime_contracts::ThreadCoordinates,
        handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
        decision: &verlet_io_core::AdmissionDecision,
        claim: &verlet_history::EventRecord,
        claim_payload: &verlet_history::IoIngressClaimedPayload,
        turn_id: &str,
        submission_mode: verlet_runtime_contracts::TurnSubmissionMode,
        reserved: crate::kernel::runtime_host::ReservedTurnSubmission,
        ingress_source_stream: &verlet_history::EventStreamId,
        settled_by: verlet_history::IngressSettledBy,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        self.bind_egress_thread(envelope, target, coordinates)
            .await?;
        self.append_ingress_turn_submitted_event(
            handle,
            envelope,
            target,
            turn_id,
            Some(ingress_source_stream),
            &claim_payload.ingress_witness_event_ids,
        )
        .await?;
        self.lock_active_turns()
            .insert(target.address.scope_key(), turn_id.to_string());
        crate::kernel::admission::submit_reserved(reserved).await;
        let evidence = self
            .wait_for_turn_execution_evidence(coordinates, turn_id, submission_mode)
            .await?;
        self.append_ingress_settle(
            coordinates,
            claim,
            claim_payload,
            Some(evidence.id),
            settled_by,
        )
        .await?;
        let mut receipt = verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), decision);
        receipt.thread_id = Some(coordinates.thread_id.to_string());
        Ok(receipt)
    }

    async fn recover_claimed_fork(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        claim: &verlet_history::EventRecord,
        claim_payload: &verlet_history::IoIngressClaimedPayload,
        ingress_source_stream: &verlet_history::EventStreamId,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        let verlet_history::IngressOutcomeIntent::Fork {
            child_key,
            child_thread_id,
            input_digest,
        } = &claim_payload.intent
        else {
            return Err(verlet_io_core::IoError::Bridge(
                "fork recovery received a non-fork claim".to_string(),
            ));
        };
        let Some(child_thread_id) = child_thread_id else {
            return Err(verlet_io_core::IoError::Bridge(format!(
                "legacy fork claim {} predates reservation-before-creation and cannot be recovered; settle requires operator action",
                claim.id
            )));
        };
        let input = verlet_io_core::IoTurnInput::from_envelope(envelope, target);
        let actual_digest = crate::agent::manifest_bind::canonical_json_hash(
            &serde_json::to_value(&input).map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("encode recovered fork input: {err}"))
            })?,
        )
        .map_err(verlet_bridge_error)?;
        if &actual_digest != input_digest {
            return Err(verlet_io_core::IoError::Bridge(
                "recovered fork input does not match the claimed digest".to_string(),
            ));
        }
        let parent_coordinates = &claim.coordinates;
        let parent_handle = self
            .get_or_load_thread_handle(parent_coordinates)
            .await
            .map_err(|err| verlet_bridge_error(err.into_inner()))?;
        let (receipt, spawned) = self
            .run_fork_effects(
                envelope,
                target,
                parent_coordinates,
                &parent_handle,
                child_key,
                &input,
                Some(ingress_source_stream),
                &claim_payload.ingress_witness_event_ids,
                Some(claim.id),
                *child_thread_id,
                true,
            )
            .await?;
        self.append_ingress_settle(
            parent_coordinates,
            claim,
            claim_payload,
            Some(spawned.id),
            verlet_history::IngressSettledBy::Recovery,
        )
        .await?;
        Ok(receipt)
    }

    async fn recover_ingress_outcome(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        state: IngressOutcomeState,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        let IngressOutcomeState::Claimed { claim, payload } = state else {
            return Ok(deduplicated_ingress_receipt(
                envelope,
                target.clone(),
                &state,
            ));
        };
        let coordinates = claim.coordinates.clone();
        let mut handle = self
            .get_or_load_thread_handle(&coordinates)
            .await
            .map_err(|err| verlet_bridge_error(err.into_inner()))?;
        if handle.status() == verlet_runtime_contracts::ThreadStatus::Failed {
            self.supervisor
                .shutdown_thread_at(&coordinates)
                .await
                .map_err(verlet_bridge_error)?;
            handle = self
                .get_or_load_thread_handle(&coordinates)
                .await
                .map_err(|err| verlet_bridge_error(err.into_inner()))?;
        }
        let decision = Self::claimed_decision(envelope, target, &payload.intent);
        let source_stream = crate::kernel::control_decision::control_stream_id(&coordinates);
        match &payload.intent {
            verlet_history::IngressOutcomeIntent::Turn {
                turn_id,
                submission_mode,
                input_digest,
            } => {
                let input = verlet_io_core::IoTurnInput::from_envelope(envelope, target);
                let actual_digest = crate::agent::manifest_bind::canonical_json_hash(
                    &serde_json::to_value(&input).map_err(|err| {
                        verlet_io_core::IoError::Bridge(format!(
                            "encode recovered ingress input: {err}"
                        ))
                    })?,
                )
                .map_err(verlet_bridge_error)?;
                if &actual_digest != input_digest {
                    return Err(verlet_io_core::IoError::Bridge(
                        "recovered ingress input does not match the claimed digest".to_string(),
                    ));
                }
                let mode = match submission_mode.as_str() {
                    "queue" => verlet_runtime_contracts::TurnSubmissionMode::Queue,
                    "steer" => verlet_runtime_contracts::TurnSubmissionMode::Steer,
                    other => {
                        return Err(verlet_io_core::IoError::Bridge(format!(
                            "claimed ingress turn has unknown submission mode {other:?}"
                        )));
                    }
                };
                let thread_events = handle
                    .read_thread_events(None)
                    .await
                    .map_err(verlet_bridge_error)?;
                if let Some(evidence) = turn_execution_evidence(&thread_events, turn_id, mode) {
                    self.append_ingress_settle(
                        &coordinates,
                        &claim,
                        &payload,
                        Some(evidence.id),
                        verlet_history::IngressSettledBy::Recovery,
                    )
                    .await?;
                    let mut receipt =
                        verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), &decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok(receipt);
                }
                let reserved = self
                    .supervisor
                    .reserve_admitted_turn_to(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(&input),
                        mode,
                        None,
                    )
                    .await
                    .map_err(verlet_bridge_error)?;
                self.complete_claimed_turn(
                    envelope,
                    target,
                    &coordinates,
                    &handle,
                    &decision,
                    &claim,
                    &payload,
                    turn_id,
                    mode,
                    reserved,
                    &source_stream,
                    verlet_history::IngressSettledBy::Recovery,
                )
                .await
            }
            verlet_history::IngressOutcomeIntent::Interrupt {
                replacement_turn_id,
                cancel_reason,
                input_digest,
            } => {
                self.supervisor
                    .cancel_at(&coordinates, cancel_reason.clone())
                    .await
                    .map_err(verlet_bridge_error)?;
                let Some(turn_id) = replacement_turn_id else {
                    self.append_ingress_settle(
                        &coordinates,
                        &claim,
                        &payload,
                        None,
                        verlet_history::IngressSettledBy::Recovery,
                    )
                    .await?;
                    let mut receipt =
                        verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), &decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok(receipt);
                };
                let input = verlet_io_core::IoTurnInput::from_envelope(envelope, target);
                let actual_digest = crate::agent::manifest_bind::canonical_json_hash(
                    &serde_json::to_value(&input).map_err(|err| {
                        verlet_io_core::IoError::Bridge(format!(
                            "encode recovered interrupt input: {err}"
                        ))
                    })?,
                )
                .map_err(verlet_bridge_error)?;
                if &actual_digest != input_digest {
                    return Err(verlet_io_core::IoError::Bridge(
                        "recovered interrupt input does not match the claimed digest".to_string(),
                    ));
                }
                let thread_events = handle
                    .read_thread_events(None)
                    .await
                    .map_err(verlet_bridge_error)?;
                if let Some(evidence) = turn_execution_evidence(
                    &thread_events,
                    turn_id,
                    verlet_runtime_contracts::TurnSubmissionMode::Interrupt,
                ) {
                    self.append_ingress_settle(
                        &coordinates,
                        &claim,
                        &payload,
                        Some(evidence.id),
                        verlet_history::IngressSettledBy::Recovery,
                    )
                    .await?;
                    let mut receipt =
                        verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), &decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok(receipt);
                }
                let reserved = self
                    .supervisor
                    .reserve_admitted_turn_to(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(&input),
                        verlet_runtime_contracts::TurnSubmissionMode::Interrupt,
                        None,
                    )
                    .await
                    .map_err(verlet_bridge_error)?;
                self.complete_claimed_turn(
                    envelope,
                    target,
                    &coordinates,
                    &handle,
                    &decision,
                    &claim,
                    &payload,
                    turn_id,
                    verlet_runtime_contracts::TurnSubmissionMode::Interrupt,
                    reserved,
                    &source_stream,
                    verlet_history::IngressSettledBy::Recovery,
                )
                .await
            }
            verlet_history::IngressOutcomeIntent::Observe { .. }
            | verlet_history::IngressOutcomeIntent::Reject { .. } => {
                Err(verlet_io_core::IoError::Bridge(
                    "effect-free ingress claim is missing its atomic settle".to_string(),
                ))
            }
            verlet_history::IngressOutcomeIntent::Fork { .. } => {
                let scope_lock = self.thread_scope_lock(&target.address.scope_key()).await;
                let _scope_guard = scope_lock.lock().await;
                let store = self.ingress_event_store().await?;
                let events = store
                    .read_events(
                        &crate::kernel::control_decision::control_stream_id(&claim.coordinates),
                        None,
                    )
                    .await
                    .map_err(verlet_history_error)?;
                match ingress_outcome_fold(&events, &payload.ingress_envelope_ids)? {
                    IngressOutcomeState::Claimed {
                        claim: active_claim,
                        payload: active_payload,
                    } if active_claim.id == claim.id => {
                        self.recover_claimed_fork(
                            envelope,
                            target,
                            &active_claim,
                            &active_payload,
                            &crate::kernel::control_decision::control_stream_id(&claim.coordinates),
                        )
                        .await
                    }
                    state @ IngressOutcomeState::Settled { .. } => Ok(
                        deduplicated_ingress_receipt(envelope, target.clone(), &state),
                    ),
                    IngressOutcomeState::Claimed { .. } | IngressOutcomeState::Missing => {
                        Err(verlet_io_core::IoError::Bridge(
                            "fork recovery no longer matches the active claim".to_string(),
                        ))
                    }
                }
            }
        }
    }

    async fn apply_with_ingress_outcomes(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        decision: &verlet_io_core::AdmissionDecision,
        ingress_message_ids: &[String],
        ingress_source_stream: Option<&verlet_history::EventStreamId>,
        source_ingress_event_ids: &[verlet_history::EventRecordId],
        admission_event_id: Option<verlet_history::EventRecordId>,
        ingress_ownership: Option<&IngressOwnershipReservation>,
    ) -> verlet_io_core::IoResult<(verlet_io_core::KernelIoReceipt, Option<String>)> {
        match decision {
            verlet_io_core::AdmissionDecision::Queue { turn_id, input } => {
                let (coordinates, handle) = self.ensure_thread(target, envelope).await?;
                let reserved = self
                    .supervisor
                    .reserve_admitted_turn_to(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(input),
                        verlet_runtime_contracts::TurnSubmissionMode::Queue,
                        None,
                    )
                    .await
                    .map_err(verlet_bridge_error)?;
                if ingress_message_ids.is_empty() {
                    self.bind_egress_thread(envelope, target, &coordinates)
                        .await?;
                    self.append_ingress_turn_submitted_event(
                        &handle,
                        envelope,
                        target,
                        turn_id,
                        ingress_source_stream,
                        source_ingress_event_ids,
                    )
                    .await?;
                    self.lock_active_turns()
                        .insert(target.address.scope_key(), turn_id.clone());
                    crate::kernel::admission::submit_reserved(reserved).await;
                    let mut receipt =
                        verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok((receipt, None));
                }
                let admission_event_id = admission_event_id.ok_or_else(|| {
                    verlet_io_core::IoError::Bridge(
                        "durable ingress claim requires admission evidence".to_string(),
                    )
                })?;
                let source_stream = ingress_source_stream.ok_or_else(|| {
                    verlet_io_core::IoError::Bridge(
                        "durable ingress claim requires its control stream".to_string(),
                    )
                })?;
                let claim = self
                    .append_ingress_claim(
                        &coordinates,
                        ingress_message_ids,
                        source_ingress_event_ids,
                        admission_event_id,
                        Self::ingress_claim_intent(decision, None)?,
                        ingress_ownership,
                    )
                    .await?;
                let IngressClaimAppend::Appended(claim) = claim else {
                    let IngressClaimAppend::Existing(state) = claim else {
                        unreachable!()
                    };
                    let settled_turn_id = ingress_outcome_turn_id(&state).map(ToOwned::to_owned);
                    let receipt = match state {
                        state @ IngressOutcomeState::Claimed { .. } => {
                            self.recover_ingress_outcome(envelope, target, state)
                                .await?
                        }
                        state => deduplicated_ingress_receipt(envelope, target.clone(), &state),
                    };
                    return Ok((receipt, settled_turn_id));
                };
                let claim_payload =
                    serde_json::from_value::<verlet_history::IoIngressClaimedPayload>(
                        claim.payload.clone(),
                    )
                    .map_err(|err| {
                        verlet_io_core::IoError::Bridge(format!(
                            "decode appended ingress claim: {err}"
                        ))
                    })?;
                let receipt = self
                    .complete_claimed_turn(
                        envelope,
                        target,
                        &coordinates,
                        &handle,
                        decision,
                        &claim,
                        &claim_payload,
                        turn_id,
                        verlet_runtime_contracts::TurnSubmissionMode::Queue,
                        reserved,
                        source_stream,
                        verlet_history::IngressSettledBy::Execution,
                    )
                    .await?;
                Ok((receipt, None))
            }
            verlet_io_core::AdmissionDecision::Steer { turn_id, input, .. } => {
                let (coordinates, handle) = self.ensure_thread(target, envelope).await?;
                let reserved = self
                    .supervisor
                    .reserve_admitted_turn_to(
                        &coordinates,
                        turn_id.clone(),
                        self.runtime_input(input),
                        verlet_runtime_contracts::TurnSubmissionMode::Steer,
                        None,
                    )
                    .await
                    .map_err(verlet_bridge_error)?;
                if ingress_message_ids.is_empty() {
                    self.bind_egress_thread(envelope, target, &coordinates)
                        .await?;
                    self.append_ingress_turn_submitted_event(
                        &handle,
                        envelope,
                        target,
                        turn_id,
                        ingress_source_stream,
                        source_ingress_event_ids,
                    )
                    .await?;
                    self.lock_active_turns()
                        .insert(target.address.scope_key(), turn_id.clone());
                    crate::kernel::admission::submit_reserved(reserved).await;
                    let mut receipt =
                        verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok((receipt, None));
                }
                let admission_event_id = admission_event_id.ok_or_else(|| {
                    verlet_io_core::IoError::Bridge(
                        "durable ingress claim requires admission evidence".to_string(),
                    )
                })?;
                let source_stream = ingress_source_stream.ok_or_else(|| {
                    verlet_io_core::IoError::Bridge(
                        "durable ingress claim requires its control stream".to_string(),
                    )
                })?;
                let claim = self
                    .append_ingress_claim(
                        &coordinates,
                        ingress_message_ids,
                        source_ingress_event_ids,
                        admission_event_id,
                        Self::ingress_claim_intent(decision, None)?,
                        ingress_ownership,
                    )
                    .await?;
                let IngressClaimAppend::Appended(claim) = claim else {
                    let IngressClaimAppend::Existing(state) = claim else {
                        unreachable!()
                    };
                    let settled_turn_id = ingress_outcome_turn_id(&state).map(ToOwned::to_owned);
                    let receipt = match state {
                        state @ IngressOutcomeState::Claimed { .. } => {
                            self.recover_ingress_outcome(envelope, target, state)
                                .await?
                        }
                        state => deduplicated_ingress_receipt(envelope, target.clone(), &state),
                    };
                    return Ok((receipt, settled_turn_id));
                };
                let claim_payload =
                    serde_json::from_value::<verlet_history::IoIngressClaimedPayload>(
                        claim.payload.clone(),
                    )
                    .map_err(|err| {
                        verlet_io_core::IoError::Bridge(format!(
                            "decode appended ingress claim: {err}"
                        ))
                    })?;
                let receipt = self
                    .complete_claimed_turn(
                        envelope,
                        target,
                        &coordinates,
                        &handle,
                        decision,
                        &claim,
                        &claim_payload,
                        turn_id,
                        verlet_runtime_contracts::TurnSubmissionMode::Steer,
                        reserved,
                        source_stream,
                        verlet_history::IngressSettledBy::Execution,
                    )
                    .await?;
                Ok((receipt, None))
            }
            verlet_io_core::AdmissionDecision::Interrupt {
                reason,
                replacement_turn_id,
                replacement,
            } => {
                let (coordinates, handle) = self.ensure_thread(target, envelope).await?;
                let reserved =
                    if let (Some(turn_id), Some(input)) = (replacement_turn_id, replacement) {
                        Some(
                            self.supervisor
                                .reserve_admitted_turn_to(
                                    &coordinates,
                                    turn_id.clone(),
                                    self.runtime_input(input),
                                    verlet_runtime_contracts::TurnSubmissionMode::Interrupt,
                                    None,
                                )
                                .await
                                .map_err(verlet_bridge_error)?,
                        )
                    } else {
                        None
                    };
                if ingress_message_ids.is_empty() {
                    self.supervisor
                        .cancel_at(&coordinates, reason.clone())
                        .await
                        .map_err(verlet_bridge_error)?;
                    if let (Some(turn_id), Some(reserved)) = (replacement_turn_id, reserved) {
                        self.bind_egress_thread(envelope, target, &coordinates)
                            .await?;
                        self.append_ingress_turn_submitted_event(
                            &handle,
                            envelope,
                            target,
                            turn_id,
                            ingress_source_stream,
                            source_ingress_event_ids,
                        )
                        .await?;
                        self.lock_active_turns()
                            .insert(target.address.scope_key(), turn_id.clone());
                        crate::kernel::admission::submit_reserved(reserved).await;
                    }
                    let mut receipt =
                        verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    return Ok((receipt, None));
                }
                let admission_event_id = admission_event_id.ok_or_else(|| {
                    verlet_io_core::IoError::Bridge(
                        "durable ingress claim requires admission evidence".to_string(),
                    )
                })?;
                let source_stream = ingress_source_stream.ok_or_else(|| {
                    verlet_io_core::IoError::Bridge(
                        "durable ingress claim requires its control stream".to_string(),
                    )
                })?;
                let claim = self
                    .append_ingress_claim(
                        &coordinates,
                        ingress_message_ids,
                        source_ingress_event_ids,
                        admission_event_id,
                        Self::ingress_claim_intent(decision, None)?,
                        ingress_ownership,
                    )
                    .await?;
                let IngressClaimAppend::Appended(claim) = claim else {
                    let IngressClaimAppend::Existing(state) = claim else {
                        unreachable!()
                    };
                    let settled_turn_id = ingress_outcome_turn_id(&state).map(ToOwned::to_owned);
                    let receipt = match state {
                        state @ IngressOutcomeState::Claimed { .. } => {
                            self.recover_ingress_outcome(envelope, target, state)
                                .await?
                        }
                        state => deduplicated_ingress_receipt(envelope, target.clone(), &state),
                    };
                    return Ok((receipt, settled_turn_id));
                };
                let claim_payload =
                    serde_json::from_value::<verlet_history::IoIngressClaimedPayload>(
                        claim.payload.clone(),
                    )
                    .map_err(|err| {
                        verlet_io_core::IoError::Bridge(format!(
                            "decode appended ingress claim: {err}"
                        ))
                    })?;
                self.supervisor
                    .cancel_at(&coordinates, reason.clone())
                    .await
                    .map_err(verlet_bridge_error)?;
                if let (Some(turn_id), Some(reserved)) = (replacement_turn_id, reserved) {
                    let receipt = self
                        .complete_claimed_turn(
                            envelope,
                            target,
                            &coordinates,
                            &handle,
                            decision,
                            &claim,
                            &claim_payload,
                            turn_id,
                            verlet_runtime_contracts::TurnSubmissionMode::Interrupt,
                            reserved,
                            source_stream,
                            verlet_history::IngressSettledBy::Execution,
                        )
                        .await?;
                    Ok((receipt, None))
                } else {
                    self.append_ingress_settle(
                        &coordinates,
                        &claim,
                        &claim_payload,
                        None,
                        verlet_history::IngressSettledBy::Execution,
                    )
                    .await?;
                    let mut receipt =
                        verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), decision);
                    receipt.thread_id = Some(coordinates.thread_id.to_string());
                    Ok((receipt, None))
                }
            }
            verlet_io_core::AdmissionDecision::ObserveOnly { .. }
            | verlet_io_core::AdmissionDecision::Reject { .. }
                if !ingress_message_ids.is_empty() =>
            {
                let (coordinates, _handle) = self.ensure_thread(target, envelope).await?;
                let admission_event_id = admission_event_id.ok_or_else(|| {
                    verlet_io_core::IoError::Bridge(
                        "durable ingress claim requires admission evidence".to_string(),
                    )
                })?;
                let outcome = self
                    .append_effect_free_ingress_outcome(
                        &coordinates,
                        ingress_message_ids,
                        source_ingress_event_ids,
                        admission_event_id,
                        Self::ingress_claim_intent(decision, None)?,
                        ingress_ownership,
                    )
                    .await?;
                let receipt = match outcome {
                    IngressClaimAppend::Appended(_) => {
                        let mut receipt = verlet_io_core::KernelIoReceipt::new(
                            envelope,
                            target.clone(),
                            decision,
                        );
                        receipt.thread_id = Some(coordinates.thread_id.to_string());
                        receipt
                    }
                    IngressClaimAppend::Existing(state @ IngressOutcomeState::Claimed { .. }) => {
                        self.recover_ingress_outcome(envelope, target, state)
                            .await?
                    }
                    IngressClaimAppend::Existing(state) => {
                        deduplicated_ingress_receipt(envelope, target.clone(), &state)
                    }
                };
                Ok((receipt, None))
            }
            verlet_io_core::AdmissionDecision::ObserveOnly { .. } => Ok((
                verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), decision),
                None,
            )),
            verlet_io_core::AdmissionDecision::Reject { reason, .. } => {
                Err(verlet_io_core::IoError::PolicyRejected(reason.clone()))
            }
            verlet_io_core::AdmissionDecision::Fork { child_key, input } => {
                self.apply_fork_admission(
                    envelope,
                    target,
                    child_key,
                    input,
                    ingress_message_ids,
                    ingress_source_stream,
                    source_ingress_event_ids,
                    admission_event_id,
                    ingress_ownership,
                )
                .await
            }
        }
    }
}

#[async_trait::async_trait]
impl verlet_io_core::KernelIoBridge for VerletDaemonIoBridge {
    async fn apply(
        &self,
        envelope: &verlet_io_core::IngressEnvelope,
        target: &verlet_io_core::ResolvedIoTarget,
        decision: &verlet_io_core::AdmissionDecision,
    ) -> verlet_io_core::IoResult<verlet_io_core::KernelIoReceipt> {
        let source_envelopes = [envelope.clone()];
        self.submit_envelope_with_sources_at_target(
            envelope.clone(),
            &source_envelopes,
            &[],
            false,
            None,
            Some(target.clone()),
            Some(decision.clone()),
        )
        .await
    }
}

#[derive(Clone)]
pub struct DirectRuntimeIngressSink {
    bridge: VerletDaemonIoBridge,
}

impl DirectRuntimeIngressSink {
    pub fn new(bridge: VerletDaemonIoBridge) -> Self {
        Self { bridge }
    }
}

#[async_trait::async_trait]
impl verlet_io_core::IngressSink for DirectRuntimeIngressSink {
    async fn submit(
        &self,
        envelope: verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::IngressAck> {
        envelope.require_witnessed()?;
        let ack = verlet_io_core::IngressAck::accepted(&envelope);
        self.bridge.submit_envelope(envelope).await?;
        Ok(ack)
    }
}

pub struct RouteIngressSink {
    inner: std::sync::Arc<dyn verlet_io_core::IngressSink>,
    route_id: String,
    policy: Option<String>,
    content_policies: Option<std::collections::BTreeMap<String, String>>,
    threading: Option<String>,
    agent_ref: Option<String>,
    coalesce_bursts: Option<crate::daemon::daemon_config::VerletCoalesceBurstsConfig>,
    principal: Option<verlet_io_core::IoPrincipal>,
}

impl RouteIngressSink {
    pub fn new(
        inner: std::sync::Arc<dyn verlet_io_core::IngressSink>,
        route: &crate::daemon::daemon_config::VerletIoRouteConfig,
    ) -> Self {
        Self {
            inner,
            route_id: route.id.clone(),
            policy: route.policy.clone(),
            content_policies: route.content_policies.clone(),
            threading: route.threading.clone(),
            agent_ref: route.agent_ref.clone(),
            coalesce_bursts: route.coalesce_bursts,
            principal: None,
        }
    }

    pub fn with_route_identity(
        inner: std::sync::Arc<dyn verlet_io_core::IngressSink>,
        route: &crate::daemon::daemon_config::VerletIoRouteConfig,
        tenant_id: impl Into<String>,
        principal_id: impl Into<String>,
    ) -> Self {
        let mut sink = Self::new(inner, route);
        sink.principal = Some(verlet_io_core::IoPrincipal::new(
            tenant_id,
            principal_id,
            format!("route:{}", route.id),
        ));
        sink
    }
}

#[async_trait::async_trait]
impl verlet_io_core::IngressSink for RouteIngressSink {
    async fn submit(
        &self,
        mut envelope: verlet_io_core::IngressEnvelope,
    ) -> verlet_io_core::IoResult<verlet_io_core::IngressAck> {
        if envelope.principal.is_none() {
            envelope.principal = self.principal.clone();
        }
        envelope
            .metadata
            .insert("cooldis_route_id".to_string(), self.route_id.clone());
        let content_policy = match &envelope.content {
            verlet_io_core::IngressContent::Event { kind, .. } => self
                .content_policies
                .as_ref()
                .and_then(|policies| policies.get(kind))
                .map(String::as_str),
            _ => None,
        };
        let effective_policy = content_policy.or(self.policy.as_deref());
        if let Some(policy) = effective_policy {
            envelope
                .metadata
                .insert("cooldis_route_policy".to_string(), policy.to_string());
        }
        if let Some(threading) = &self.threading {
            envelope
                .metadata
                .insert("cooldis_route_threading".to_string(), threading.clone());
        }
        if let Some(agent_ref) = &self.agent_ref {
            envelope
                .metadata
                .insert(ROUTE_AGENT_REF_METADATA.to_string(), agent_ref.clone());
        }
        if route_coalesce_applies(effective_policy)
            && let Some(coalesce) = self.coalesce_bursts
        {
            envelope
                .metadata
                .insert("cooldis_coalesce_bursts".to_string(), "true".to_string());
            envelope.metadata.insert(
                "cooldis_coalesce_window_ms".to_string(),
                coalesce.window_ms.to_string(),
            );
            envelope.metadata.insert(
                "cooldis_coalesce_max_batch".to_string(),
                coalesce.max_batch.to_string(),
            );
        }
        envelope.require_witnessed()?;
        if envelope.principal.is_none() {
            return Err(verlet_io_core::IoError::InvalidEnvelope(format!(
                "principal is required: route {:?} has no identity binding",
                self.route_id
            )));
        }
        self.inner.submit(envelope).await
    }
}

fn route_coalesce_applies(policy: Option<&str>) -> bool {
    !matches!(policy, Some("observe_only" | "reject"))
}

pub struct VerletDaemonQueueWorker {
    queue: std::sync::Arc<dyn verlet_io_core::IngressQueueStore>,
    bridge: VerletDaemonIoBridge,
    worker_id: String,
    max_messages: usize,
    poll_interval: std::time::Duration,
    visibility_timeout_secs: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct QueueDrainOutcome {
    count: usize,
    held_until_ms: Option<u64>,
}

impl VerletDaemonQueueWorker {
    pub fn new(
        queue: std::sync::Arc<dyn verlet_io_core::IngressQueueStore>,
        bridge: VerletDaemonIoBridge,
        worker_id: impl Into<String>,
        visibility_timeout_secs: u32,
    ) -> Self {
        Self {
            queue,
            bridge,
            worker_id: worker_id.into(),
            max_messages: DEFAULT_QUEUE_BATCH,
            poll_interval: std::time::Duration::from_millis(DEFAULT_WORKER_POLL_MS),
            visibility_timeout_secs,
        }
    }

    pub fn with_poll_interval(mut self, poll_interval: std::time::Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub fn with_max_messages(mut self, max_messages: usize) -> Self {
        self.max_messages = max_messages;
        self
    }

    pub async fn drain_once(&self) -> verlet_io_core::IoResult<usize> {
        Ok(self.drain_once_inner().await?.count)
    }

    async fn drain_once_inner(&self) -> verlet_io_core::IoResult<QueueDrainOutcome> {
        let leased = self
            .queue
            .lease_ingress(
                &self.worker_id,
                self.max_messages,
                self.visibility_timeout_secs,
            )
            .await?;
        let count = leased.len();
        let mut held_until_ms: Option<u64> = None;
        let mut coalesce_groups: std::collections::BTreeMap<
            CoalesceGroupKey,
            Vec<verlet_io_core::LeasedIngressEnvelope>,
        > = std::collections::BTreeMap::new();
        for mut message in leased {
            self.attach_route_principal(&mut message.envelope);
            if message.envelope.require_witnessed().is_err() {
                self.submit_leased_message(message).await?;
                continue;
            }
            match coalesce_policy_for_envelope(&message.envelope) {
                Ok(Some(_)) => {
                    coalesce_groups
                        .entry(coalesce_group_key(&message.envelope))
                        .or_default()
                        .push(message);
                }
                Ok(None) => self.submit_leased_message(message).await?,
                Err(err) => {
                    let reason = err.to_string();
                    self.queue
                        .retry_ingress(&message.message_id, &reason)
                        .await?;
                    return Err(err);
                }
            }
        }
        for (_key, messages) in coalesce_groups {
            if let Some(visible_at_ms) = self.process_coalesce_group(messages).await? {
                held_until_ms = Some(
                    held_until_ms
                        .map(|existing| existing.min(visible_at_ms))
                        .unwrap_or(visible_at_ms),
                );
            }
        }
        Ok(QueueDrainOutcome {
            count,
            held_until_ms,
        })
    }

    fn attach_route_principal(&self, envelope: &mut verlet_io_core::IngressEnvelope) {
        if envelope.principal.is_none()
            && let Some(route_id) = envelope
                .metadata
                .get("cooldis_route_id")
                .filter(|route_id| !route_id.is_empty())
        {
            envelope.principal = Some(verlet_io_core::IoPrincipal::new(
                self.bridge.tenant_id.clone(),
                self.bridge.user_id.clone(),
                format!("route:{route_id}"),
            ));
        }
    }

    async fn submit_leased_message(
        &self,
        message: verlet_io_core::LeasedIngressEnvelope,
    ) -> verlet_io_core::IoResult<()> {
        match self
            .bridge
            .submit_queued_envelope(message.envelope, message.attempt)
            .await
        {
            Ok(_) => self.queue.complete_ingress(&message.message_id).await,
            Err(err) => {
                let reason = err.to_string();
                self.queue
                    .retry_ingress(&message.message_id, &reason)
                    .await?;
                Err(err)
            }
        }
    }

    async fn process_coalesce_group(
        &self,
        mut messages: Vec<verlet_io_core::LeasedIngressEnvelope>,
    ) -> verlet_io_core::IoResult<Option<u64>> {
        sort_coalesce_messages(&mut messages);
        while !messages.is_empty() {
            let policy = coalesce_policy_for_envelope(&messages[0].envelope)?.ok_or_else(|| {
                verlet_io_core::IoError::Queue(
                    "coalesce group is missing coalesce policy".to_string(),
                )
            })?;
            let batch_len = messages.len().min(policy.max_batch);
            let ready = coalesce_batch_is_ready(&messages[..batch_len], policy);
            if !ready {
                let visible_at_ms = coalesce_visible_at_ms(&messages[0].envelope, policy);
                for message in messages {
                    self.queue
                        .hold_ingress_until(&message.message_id, visible_at_ms)
                        .await?;
                }
                return Ok(Some(visible_at_ms));
            }

            let remainder = messages.split_off(batch_len);
            let batch = messages;
            let mut fresh_batch = Vec::with_capacity(batch.len());
            for message in batch {
                if self.bridge.queued_message_was_applied(&message).await? {
                    self.queue.complete_ingress(&message.message_id).await?;
                } else {
                    fresh_batch.push(message);
                }
            }
            if fresh_batch.len() < batch_len {
                fresh_batch.extend(remainder);
                messages = fresh_batch;
                sort_coalesce_messages(&mut messages);
                continue;
            }
            let batch = fresh_batch;
            let merged = merged_coalesce_envelope(&batch)?;
            let source_envelopes = batch
                .iter()
                .map(|message| message.envelope.clone())
                .collect::<Vec<_>>();
            let ingress_message_ids = batch
                .iter()
                .map(|message| message.envelope.id.clone())
                .collect::<Vec<_>>();
            let ingress_attempt = batch
                .iter()
                .map(|message| message.attempt)
                .max()
                .unwrap_or(1);
            match self
                .bridge
                .submit_coalesced_queued_envelopes(
                    merged,
                    &source_envelopes,
                    &ingress_message_ids,
                    ingress_attempt,
                )
                .await
            {
                Ok(_) => {
                    for message in &batch {
                        self.queue.complete_ingress(&message.message_id).await?;
                    }
                }
                Err(err) => {
                    let reason = err.to_string();
                    for message in &batch {
                        self.queue
                            .retry_ingress(&message.message_id, &reason)
                            .await?;
                    }
                    return Err(err);
                }
            }
            messages = remainder;
        }
        Ok(None)
    }

    pub async fn run(self) {
        let mut retry_state = crate::daemon::retry::RetryState::new(self.poll_interval);
        loop {
            match self.drain_once_inner().await {
                Ok(QueueDrainOutcome { count: 0, .. }) => {
                    let decision = retry_state.on_success(self.poll_interval);
                    if let Some(log) = decision.log {
                        eprintln!(
                            "{}",
                            log.message("verlet daemon ingress worker", decision.delay)
                        );
                    }
                    tokio::time::sleep(decision.delay).await
                }
                Ok(QueueDrainOutcome {
                    held_until_ms: Some(held_until_ms),
                    ..
                }) => {
                    let decision = retry_state.on_success(self.poll_interval);
                    if let Some(log) = decision.log {
                        eprintln!(
                            "{}",
                            log.message("verlet daemon ingress worker", decision.delay)
                        );
                    }
                    let delay_ms = held_until_ms.saturating_sub(now_ms());
                    if delay_ms > 0 {
                        tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
                    }
                }
                Ok(_) => {
                    let decision = retry_state.on_success(self.poll_interval);
                    if let Some(log) = decision.log {
                        eprintln!(
                            "{}",
                            log.message("verlet daemon ingress worker", decision.delay)
                        );
                    }
                }
                Err(err) => {
                    let decision = retry_state.on_failure(&err.to_string(), self.poll_interval);
                    if let Some(log) = decision.log {
                        eprintln!(
                            "{}",
                            log.message("verlet daemon ingress worker", decision.delay)
                        );
                    }
                    tokio::time::sleep(decision.delay).await;
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct TelegramWebhookServerConfig {
    pub route_id: String,
    pub listen: String,
    pub path: String,
    pub secret_token: String,
}

pub struct TelegramWebhookServer {
    route_id: String,
    path: String,
    secret_token_hash: [u8; 32],
    listener: tokio::net::TcpListener,
    sink: std::sync::Arc<dyn verlet_io_core::IngressSink>,
}

impl TelegramWebhookServer {
    pub async fn bind(
        config: TelegramWebhookServerConfig,
        sink: std::sync::Arc<dyn verlet_io_core::IngressSink>,
    ) -> crate::kernel::runtime_host::VerletResult<Self> {
        if config.secret_token.trim().is_empty() {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                format!(
                    "Telegram webhook route {} requires a non-empty secret_token",
                    config.route_id
                ),
            ));
        }
        let listener = tokio::net::TcpListener::bind(&config.listen)
            .await
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "failed to bind Telegram webhook route {} on {}: {err}",
                    config.route_id, config.listen
                ))
            })?;
        Ok(Self {
            route_id: config.route_id,
            path: config.path,
            secret_token_hash: sha2::Sha256::digest(config.secret_token.as_bytes()).into(),
            listener,
            sink,
        })
    }

    pub fn local_addr(&self) -> crate::kernel::runtime_host::VerletResult<std::net::SocketAddr> {
        self.listener.local_addr().map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read Telegram webhook address: {err}"
            ))
        })
    }

    pub async fn serve(self) -> crate::kernel::runtime_host::VerletResult<()> {
        let adapter = std::sync::Arc::new(verlet_io_telegram::TelegramWebhookAdapter::new(
            self.route_id,
        ));
        let mut requests = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                accepted = self.listener.accept(), if requests.len() < MAX_TELEGRAM_WEBHOOK_REQUESTS => {
                    let (stream, _) = accepted.map_err(|err| {
                        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                            "failed to accept Telegram webhook connection: {err}"
                        ))
                    })?;
                    let path = self.path.clone();
                    let secret_token_hash = self.secret_token_hash;
                    let sink = self.sink.clone();
                    let adapter = adapter.clone();
                    requests.spawn(async move {
                        telegram_webhook_request_with_timeout(handle_telegram_webhook_connection(
                            stream,
                            path,
                            secret_token_hash,
                            adapter,
                            sink,
                        ))
                        .await
                    });
                }
                completed = requests.join_next(), if !requests.is_empty() => {
                    report_telegram_webhook_request_completion(completed);
                }
            }
        }
    }
}

async fn telegram_webhook_request_with_timeout(
    request: impl std::future::Future<Output = crate::kernel::runtime_host::VerletResult<()>>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    tokio::time::timeout(TELEGRAM_WEBHOOK_REQUEST_TIMEOUT, request)
        .await
        .map_err(|_| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "Telegram webhook request timed out".to_string(),
            )
        })?
}

fn report_telegram_webhook_request_completion(
    completed: Option<
        Result<crate::kernel::runtime_host::VerletResult<()>, tokio::task::JoinError>,
    >,
) {
    match completed {
        Some(Ok(Err(err))) => eprintln!("verlet Telegram webhook request failed: {err}"),
        Some(Err(err)) => eprintln!("verlet Telegram webhook request task failed: {err}"),
        Some(Ok(Ok(()))) | None => {}
    }
}

async fn handle_telegram_webhook_connection(
    mut stream: tokio::net::TcpStream,
    path: String,
    secret_token_hash: [u8; 32],
    adapter: std::sync::Arc<verlet_io_telegram::TelegramWebhookAdapter>,
    sink: std::sync::Arc<dyn verlet_io_core::IngressSink>,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let request_head = match read_http_request_head(&mut stream).await {
        Ok(request) => request,
        Err(_) => {
            write_telegram_auth_failure(&mut stream).await?;
            return Ok(());
        }
    };

    let actual_token_hash = sha2::Sha256::digest(
        request_head
            .headers
            .get("x-telegram-bot-api-secret-token")
            .map_or(b"".as_slice(), |value| value.as_bytes()),
    );
    let token_present = subtle::Choice::from(u8::from(
        request_head
            .headers
            .contains_key("x-telegram-bot-api-secret-token"),
    ));
    let token_matches = actual_token_hash.as_slice().ct_eq(&secret_token_hash);
    if !bool::from(token_present & token_matches) {
        write_telegram_auth_failure(&mut stream).await?;
        return Ok(());
    }

    if request_head.method != "POST" {
        write_json_response(
            &mut stream,
            405,
            serde_json::json!({ "ok": false, "error": "method_not_allowed" }),
        )
        .await?;
        return Ok(());
    }
    if request_head.path != path {
        write_json_response(
            &mut stream,
            404,
            serde_json::json!({ "ok": false, "error": "not_found" }),
        )
        .await?;
        return Ok(());
    }
    let body = match read_http_request_body(&mut stream, request_head).await {
        Ok(request) => request,
        Err(err) => {
            write_json_response(
                &mut stream,
                400,
                serde_json::json!({ "ok": false, "error": err.to_string() }),
            )
            .await?;
            return Ok(());
        }
    };

    let update: verlet_io_telegram::TelegramUpdate =
        serde_json::from_slice(&body).map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to decode Telegram update JSON: {err}"
            ))
        })?;
    match adapter
        .submit_update(sink.as_ref(), &update, now_ms())
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string()))?
    {
        Some(ack) => {
            write_json_response(
                &mut stream,
                200,
                serde_json::json!({ "ok": true, "accepted": ack.accepted, "envelopeId": ack.envelope_id }),
            )
            .await?;
        }
        None => {
            write_json_response(
                &mut stream,
                200,
                serde_json::json!({ "ok": true, "accepted": false, "reason": "unsupported_update" }),
            )
            .await?;
        }
    }
    Ok(())
}

struct HttpRequestHead {
    method: String,
    path: String,
    headers: std::collections::HashMap<String, String>,
    buffered_body: Vec<u8>,
}

async fn read_http_request_head<R>(
    stream: &mut R,
) -> crate::kernel::runtime_host::VerletResult<HttpRequestHead>
where
    R: tokio::io::AsyncRead + Unpin,
{
    tokio::time::timeout(
        TELEGRAM_WEBHOOK_HEAD_TIMEOUT,
        read_http_request_head_inner(stream),
    )
    .await
    .map_err(|_| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "Telegram webhook request head timed out".to_string(),
        )
    })?
}

async fn read_http_request_head_inner<R>(
    stream: &mut R,
) -> crate::kernel::runtime_host::VerletResult<HttpRequestHead>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut buffer = Vec::new();
    let header_end;
    loop {
        let remaining = MAX_HTTP_HEADER_BYTES.saturating_sub(buffer.len());
        if remaining == 0 {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "HTTP headers are too large".to_string(),
            ));
        }
        let mut chunk = [0_u8; 1024];
        let read_len = remaining.min(chunk.len());
        let read = stream.read(&mut chunk[..read_len]).await.map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read HTTP request: {err}"
            ))
        })?;
        if read == 0 {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "connection closed before HTTP headers".to_string(),
            ));
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(index) = find_header_end(&buffer) {
            header_end = index;
            break;
        }
    }

    let headers_text = std::str::from_utf8(&buffer[..header_end]).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "HTTP headers are not UTF-8: {err}"
        ))
    })?;
    let mut lines = headers_text.split("\r\n");
    let request_line = lines.next().ok_or_else(|| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "missing HTTP request line".to_string(),
        )
    })?;
    let mut request_parts = request_line.split_whitespace();
    let method = request_parts
        .next()
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "missing HTTP method".to_string(),
            )
        })?
        .to_string();
    let path = request_parts
        .next()
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "missing HTTP path".to_string(),
            )
        })?
        .to_string();

    let mut headers = std::collections::HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    Ok(HttpRequestHead {
        method,
        path,
        headers,
        buffered_body: buffer[header_end + 4..].to_vec(),
    })
}

async fn read_http_request_body(
    stream: &mut tokio::net::TcpStream,
    request: HttpRequestHead,
) -> crate::kernel::runtime_host::VerletResult<Vec<u8>> {
    let content_length = request
        .headers
        .get("content-length")
        .map(|value| {
            value.parse::<usize>().map_err(|err| {
                crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                    "invalid content-length {value:?}: {err}"
                ))
            })
        })
        .transpose()?
        .unwrap_or(0);
    if content_length > MAX_HTTP_BODY_BYTES {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
            "HTTP body is too large".to_string(),
        ));
    }

    let mut body = request.buffered_body;
    while body.len() < content_length {
        let mut chunk = vec![0_u8; content_length - body.len()];
        let read = stream.read(&mut chunk).await.map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
                "failed to read HTTP body: {err}"
            ))
        })?;
        if read == 0 {
            return Err(crate::kernel::runtime_host::VerletError::RuntimeFactory(
                "connection closed before HTTP body".to_string(),
            ));
        }
        body.extend_from_slice(&chunk[..read]);
    }
    body.truncate(content_length);

    Ok(body)
}

async fn write_telegram_auth_failure(
    stream: &mut tokio::net::TcpStream,
) -> crate::kernel::runtime_host::VerletResult<()> {
    write_json_response(
        stream,
        401,
        serde_json::json!({ "ok": false, "error": "unauthorized" }),
    )
    .await
}

async fn write_json_response(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    body: serde_json::Value,
) -> crate::kernel::runtime_host::VerletResult<()> {
    let body = body.to_string();
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        _ => "Internal Server Error",
    };
    let response = format!(
        "HTTP/1.1 {status} {reason}\r\n\
Content-Type: application/json\r\n\
Content-Length: {}\r\n\
Connection: close\r\n\
\r\n\
{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await.map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to write HTTP response: {err}"
        ))
    })?;
    stream.shutdown().await.map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeFactory(format!(
            "failed to close HTTP response: {err}"
        ))
    })?;
    let mut discard = [0_u8; 1024];
    let _ = tokio::time::timeout(HTTP_RESPONSE_DRAIN_TIMEOUT, async {
        loop {
            match stream.read(&mut discard).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    })
    .await;
    Ok(())
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn projection_payload(
    rule: &CompiledEgressProjectionRule,
    captures: &regex::Captures<'_>,
) -> serde_json::Value {
    let mut payload = serde_json::Map::new();
    for name in rule.regex.capture_names().flatten() {
        if let Some(value) = captures.name(name) {
            payload.insert(
                name.to_string(),
                serde_json::Value::String(value.as_str().to_string()),
            );
        }
    }
    serde_json::Value::Object(payload)
}

fn strip_projection_matches(text: &str, matches: &[ProjectionMatch]) -> String {
    let mut stripped = String::with_capacity(text.len());
    let mut cursor = 0;
    for matched in matches {
        if cursor < matched.start {
            stripped.push_str(&text[cursor..matched.start]);
        }
        cursor = matched.end;
    }
    if cursor < text.len() {
        stripped.push_str(&text[cursor..]);
    }
    stripped
}

fn first_remaining_text_offset(text: &str, matches: &[ProjectionMatch]) -> Option<usize> {
    let mut cursor = 0;
    for matched in matches {
        if cursor < matched.start {
            return Some(cursor);
        }
        cursor = matched.end;
    }
    (cursor < text.len()).then_some(cursor)
}

fn sibling_egress(
    source: &verlet_io_core::EgressEnvelope,
    kind: verlet_io_core::EgressKind,
) -> verlet_io_core::EgressEnvelope {
    let mut envelope = verlet_io_core::EgressEnvelope::new(source.target.clone(), kind, now_ms());
    envelope.source_ingress_id = source.source_ingress_id.clone();
    envelope.metadata = source.metadata.clone();
    envelope
}

fn source_has_partial_projected_receipts(
    route_config: &RouteEgressConfig,
    source_event_id: verlet_history::EventRecordId,
    source_context: &IngressReceiptContext,
    text: &str,
    receipt_cursors: &std::collections::HashMap<String, ReceiptDedupeCursor>,
) -> bool {
    let mut envelope = verlet_io_core::EgressEnvelope::new(
        source_context.target.clone(),
        verlet_io_core::EgressKind::AssistantMessage {
            text: text.to_string(),
        },
        now_ms(),
    );
    envelope.source_ingress_id = source_context.source_ingress_id.clone();
    envelope.metadata = source_context.metadata.clone();

    let mut envelope_index = 0;
    let mut saw_receipt = false;
    let mut saw_missing = false;
    for projected in route_config.project(envelope) {
        if let Some(_typing) = &route_config.typing_simulation
            && let verlet_io_core::EgressKind::AssistantMessage { text } = &projected.kind
            && !text.is_empty()
        {
            let typing_envelope = sibling_egress(
                &projected,
                verlet_io_core::EgressKind::PlatformAction {
                    action: "typing".to_string(),
                    payload: serde_json::Value::Object(serde_json::Map::new()),
                },
            );
            note_projection_receipt_presence(
                source_event_id,
                envelope_index,
                &typing_envelope.kind,
                receipt_cursors,
                &mut saw_receipt,
                &mut saw_missing,
            );
            envelope_index += 1;
        }

        note_projection_receipt_presence(
            source_event_id,
            envelope_index,
            &projected.kind,
            receipt_cursors,
            &mut saw_receipt,
            &mut saw_missing,
        );
        envelope_index += 1;
    }
    saw_receipt && saw_missing
}

fn note_projection_receipt_presence(
    source_event_id: verlet_history::EventRecordId,
    envelope_index: usize,
    kind: &verlet_io_core::EgressKind,
    receipt_cursors: &std::collections::HashMap<String, ReceiptDedupeCursor>,
    saw_receipt: &mut bool,
    saw_missing: &mut bool,
) {
    let dedupe_key = egress_dedupe_key(source_event_id, envelope_index);
    if matching_receipt_cursor(receipt_cursors, &dedupe_key, kind).is_some() {
        *saw_receipt = true;
    } else {
        *saw_missing = true;
    }
}

fn matching_receipt_cursor<'a>(
    receipt_cursors: &'a std::collections::HashMap<String, ReceiptDedupeCursor>,
    dedupe_key: &str,
    kind: &verlet_io_core::EgressKind,
) -> Option<&'a ReceiptDedupeCursor> {
    let egress_kind = egress_kind_name(kind);
    receipt_cursors
        .get(dedupe_key)
        .filter(|receipt| receipt.egress_kind == egress_kind)
}

fn retain_newest_cursor(
    slot: &mut Option<verlet_history::StreamCursorV1>,
    candidate: verlet_history::StreamCursorV1,
) {
    if slot
        .as_ref()
        .is_none_or(|current| candidate.sequence.get() > current.sequence.get())
    {
        *slot = Some(candidate);
    }
}

async fn append_egress_delivered_receipt(
    handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
    binding: &BoundEgressThread,
    source_event: &DrainEgressSource,
    envelope_index: usize,
    dedupe_key: &str,
    envelope: &verlet_io_core::EgressEnvelope,
    receipt: &verlet_io_core::DeliveryReceipt,
    attempts: u32,
) -> verlet_io_core::IoResult<verlet_history::EventRecord> {
    let payload = egress_delivered_payload(binding, envelope, receipt, attempts)?;
    append_egress_receipt_event(
        handle,
        source_event,
        verlet_history::EventKind::IoEgressDelivered,
        add_egress_receipt_metadata(
            payload,
            source_event.id,
            envelope_index,
            dedupe_key,
            &envelope.id,
        )?,
    )
    .await
}

async fn append_egress_failed_receipt(
    handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
    binding: &BoundEgressThread,
    source_event: &DrainEgressSource,
    envelope_index: usize,
    dedupe_key: &str,
    envelope: &verlet_io_core::EgressEnvelope,
    attempts: u32,
    error: &str,
) -> verlet_io_core::IoResult<verlet_history::EventRecord> {
    let payload = egress_failed_payload(binding, envelope, attempts, error)?;
    append_egress_receipt_event(
        handle,
        source_event,
        verlet_history::EventKind::IoEgressFailed,
        add_egress_receipt_metadata(
            payload,
            source_event.id,
            envelope_index,
            dedupe_key,
            &envelope.id,
        )?,
    )
    .await
}

async fn append_egress_receipt_event(
    handle: &crate::kernel::runtime_host::RuntimeThreadHandle,
    source_event: &DrainEgressSource,
    kind: verlet_history::EventKind,
    payload: serde_json::Value,
) -> verlet_io_core::IoResult<verlet_history::EventRecord> {
    let stream_id = verlet_history::EventStreamId::for_thread(&handle.context().coordinates);
    handle
        .append_thread_event_record(verlet_history::NewEventRecord::discharged(
            handle.context().coordinates.clone(),
            kind,
            payload,
            verlet_history::EventProvenance {
                source_streams: vec![stream_id],
                source_event_ids: vec![source_event.id],
                discharged_by: Some(IO_EGRESS_PROJECTOR_DISCHARGED_BY.to_string()),
                function: Some(IO_EGRESS_PROJECTOR_FUNCTION.to_string()),
                ..verlet_history::EventProvenance::default()
            },
        ))
        .await
        .map_err(verlet_bridge_error)
}

fn egress_delivered_payload(
    binding: &BoundEgressThread,
    envelope: &verlet_io_core::EgressEnvelope,
    receipt: &verlet_io_core::DeliveryReceipt,
    attempts: u32,
) -> verlet_io_core::IoResult<serde_json::Value> {
    serde_json::to_value(verlet_history::IoEgressDeliveredPayload {
        route_id: binding.route_id.clone(),
        egress_kind: egress_kind_name(&envelope.kind),
        external_message_id: receipt.external_message_id.clone(),
        attempts,
    })
    .map_err(|err| {
        verlet_io_core::IoError::Bridge(format!("encode egress delivered payload: {err}"))
    })
}

fn egress_failed_payload(
    binding: &BoundEgressThread,
    envelope: &verlet_io_core::EgressEnvelope,
    attempts: u32,
    error: &str,
) -> verlet_io_core::IoResult<serde_json::Value> {
    let mut payload = serde_json::to_value(verlet_history::IoEgressFailedPayload {
        route_id: binding.route_id.clone(),
        egress_kind: egress_kind_name(&envelope.kind),
        attempts,
        error_class: "delivery_failed".to_string(),
        dead_lettered: true,
    })
    .map_err(|err| {
        verlet_io_core::IoError::Bridge(format!("encode egress failed payload: {err}"))
    })?;
    payload_object_mut(&mut payload)?.insert(
        "error".to_string(),
        serde_json::Value::String(error.to_string()),
    );
    Ok(payload)
}

fn add_egress_receipt_metadata(
    mut payload: serde_json::Value,
    source_event_id: verlet_history::EventRecordId,
    envelope_index: usize,
    dedupe_key: &str,
    egress_id: &str,
) -> verlet_io_core::IoResult<serde_json::Value> {
    let object = payload_object_mut(&mut payload)?;
    object.insert(
        "source_event_id".to_string(),
        serde_json::Value::String(source_event_id.to_string()),
    );
    object.insert(
        "envelope_index".to_string(),
        serde_json::Value::Number(serde_json::Number::from(envelope_index as u64)),
    );
    object.insert(
        "dedupe_key".to_string(),
        serde_json::Value::String(dedupe_key.to_string()),
    );
    object.insert(
        "egress_id".to_string(),
        serde_json::Value::String(egress_id.to_string()),
    );
    Ok(payload)
}

fn payload_object_mut(
    payload: &mut serde_json::Value,
) -> verlet_io_core::IoResult<&mut serde_json::Map<String, serde_json::Value>> {
    payload.as_object_mut().ok_or_else(|| {
        verlet_io_core::IoError::Bridge("receipt payload did not encode as object".to_string())
    })
}

fn ingress_context_from_event(
    event: &verlet_history::EventRecord,
) -> Option<IngressReceiptContext> {
    if event.kind != verlet_history::EventKind::TurnSubmitted {
        return None;
    }
    let target = serde_json::from_value(event.payload.get("target")?.clone()).ok()?;
    let metadata = event
        .payload
        .get("ingress_metadata")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .ok()
        .flatten()
        .unwrap_or_default();
    let source_ingress_id = event
        .payload
        .get("ingress_envelope_id")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    let turn_id = event
        .payload
        .get("turn_id")
        .and_then(serde_json::Value::as_str)
        .map(ToOwned::to_owned);
    Some(IngressReceiptContext {
        target,
        metadata,
        source_ingress_id,
        turn_id,
    })
}

#[cfg(test)]
async fn await_ingress_outcome_on_store<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    streams: &[verlet_history::EventStreamId],
    ingress_envelope_ids: &[String],
) -> verlet_io_core::IoResult<IngressOutcomeState> {
    tokio::time::timeout(
        std::time::Duration::from_secs(30),
        ingress_outcome_on_store(store, streams, ingress_envelope_ids),
    )
    .await
    .map_err(|_| ingress_outcome_timeout_error())?
}

async fn ingress_outcome_on_store<S: verlet_history::EventStore + ?Sized>(
    store: &S,
    streams: &[verlet_history::EventStreamId],
    ingress_envelope_ids: &[String],
) -> verlet_io_core::IoResult<IngressOutcomeState> {
    let mut events = Vec::new();
    let mut cursors = Vec::with_capacity(streams.len());
    for stream in streams {
        let stream_events = store
            .read_events(stream, None)
            .await
            .map_err(verlet_history_error)?;
        cursors.push(
            stream_events
                .last()
                .map(verlet_history::EventRecord::cursor_v1),
        );
        events.extend(stream_events);
    }
    loop {
        let state = ingress_outcome_fold(&events, ingress_envelope_ids)?;
        if !matches!(state, IngressOutcomeState::Missing) {
            return Ok(state);
        }
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        for (index, stream) in streams.iter().enumerate() {
            let new_events = match &cursors[index] {
                Some(cursor) => store
                    .read_events_after_cursor(stream, cursor)
                    .await
                    .map_err(verlet_history_error)?,
                None => store
                    .read_events(stream, Some(verlet_history::EventSequence::new(1)))
                    .await
                    .map_err(verlet_history_error)?,
            };
            if let Some(last) = new_events.last() {
                cursors[index] = Some(last.cursor_v1());
            }
            events.extend(new_events);
        }
    }
}

fn ingress_outcome_timeout_error() -> verlet_io_core::IoError {
    verlet_io_core::IoError::Bridge(
        "timed out waiting for superseding durable ingress ownership".to_string(),
    )
}

fn ingress_outcome_fold(
    events: &[verlet_history::EventRecord],
    ingress_envelope_ids: &[String],
) -> verlet_io_core::IoResult<IngressOutcomeState> {
    if ingress_envelope_ids.is_empty() {
        return Ok(IngressOutcomeState::Missing);
    }
    let requested = ingress_envelope_ids
        .iter()
        .collect::<std::collections::HashSet<_>>();
    let mut owner_by_envelope = std::collections::HashMap::<
        String,
        (
            verlet_history::EventRecord,
            verlet_history::IoIngressClaimedPayload,
        ),
    >::new();
    let mut settles = std::collections::HashMap::<
        verlet_history::EventRecordId,
        (
            verlet_history::EventRecord,
            verlet_history::IoIngressSettledPayload,
        ),
    >::new();
    for event in events {
        match event.kind {
            verlet_history::EventKind::IoIngressClaimed => {
                let payload = serde_json::from_value::<verlet_history::IoIngressClaimedPayload>(
                    event.payload.clone(),
                )
                .map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!(
                        "invalid io.ingress.claimed payload: {err}"
                    ))
                })?;
                for envelope_id in &payload.ingress_envelope_ids {
                    if owner_by_envelope.contains_key(envelope_id) {
                        return Err(verlet_io_core::IoError::Bridge(format!(
                            "ingress envelope {envelope_id:?} has more than one claim"
                        )));
                    }
                    owner_by_envelope.insert(envelope_id.clone(), (event.clone(), payload.clone()));
                }
            }
            verlet_history::EventKind::IoIngressSettled => {
                let payload = serde_json::from_value::<verlet_history::IoIngressSettledPayload>(
                    event.payload.clone(),
                )
                .map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!(
                        "invalid io.ingress.settled payload: {err}"
                    ))
                })?;
                if settles
                    .insert(payload.claim_event_id, (event.clone(), payload))
                    .is_some()
                {
                    return Err(verlet_io_core::IoError::Bridge(
                        "ingress claim has more than one settle".to_string(),
                    ));
                }
            }
            _ => {}
        }
    }
    let mut owners = requested
        .iter()
        .filter_map(|id| owner_by_envelope.get(id.as_str()))
        .collect::<Vec<_>>();
    if owners.is_empty() {
        return Ok(IngressOutcomeState::Missing);
    }
    if owners.len() != requested.len() {
        return Err(verlet_io_core::IoError::Bridge(
            "durable ingress batch partially overlaps claimed envelopes".to_string(),
        ));
    }
    let (claim, claim_payload) = owners.pop().expect("non-empty claim owner set");
    if owners.iter().any(|(event, _)| event.id != claim.id) {
        return Err(verlet_io_core::IoError::Bridge(
            "durable ingress batch maps to different claims".to_string(),
        ));
    }
    match settles.remove(&claim.id) {
        Some((settle, settle_payload)) => {
            if settle_payload.ingress_envelope_ids != claim_payload.ingress_envelope_ids {
                return Err(verlet_io_core::IoError::Bridge(
                    "ingress settle envelope set does not match its claim".to_string(),
                ));
            }
            Ok(IngressOutcomeState::Settled {
                claim_payload: claim_payload.clone(),
                settle,
            })
        }
        None => Ok(IngressOutcomeState::Claimed {
            claim: claim.clone(),
            payload: claim_payload.clone(),
        }),
    }
}

fn ingress_claim_provenance(
    control_stream: &verlet_history::EventStreamId,
    ingress_witness_event_ids: &[verlet_history::EventRecordId],
    admission_event_id: verlet_history::EventRecordId,
) -> verlet_history::EventProvenance {
    verlet_history::EventProvenance {
        source_streams: vec![control_stream.clone()],
        source_event_ids: ingress_witness_event_ids
            .iter()
            .copied()
            .chain(std::iter::once(admission_event_id))
            .collect(),
        discharged_by: Some("controller:ingress-outcome".to_string()),
        function: Some("claim/v1".to_string()),
        ..verlet_history::EventProvenance::default()
    }
}

fn ingress_settle_provenance(
    control_stream: &verlet_history::EventStreamId,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    claim_event_id: verlet_history::EventRecordId,
    evidence_event_id: Option<verlet_history::EventRecordId>,
) -> verlet_history::EventProvenance {
    let mut source_streams = vec![control_stream.clone()];
    if evidence_event_id.is_some() {
        source_streams.push(verlet_history::EventStreamId::for_thread(coordinates));
    }
    verlet_history::EventProvenance {
        source_streams,
        source_event_ids: std::iter::once(claim_event_id)
            .chain(evidence_event_id)
            .collect(),
        discharged_by: Some("controller:ingress-outcome".to_string()),
        function: Some("settle/v1".to_string()),
        ..verlet_history::EventProvenance::default()
    }
}

fn deduplicated_ingress_receipt(
    envelope: &verlet_io_core::IngressEnvelope,
    target: verlet_io_core::ResolvedIoTarget,
    state: &IngressOutcomeState,
) -> verlet_io_core::KernelIoReceipt {
    let turn_id = ingress_outcome_turn_id(state);
    let reason = match turn_id {
        Some(turn_id) => format!("durable ingress claim settled for turn {turn_id}"),
        None => "durable ingress claim already settled".to_string(),
    };
    eprintln!(
        "verlet daemon ingress {} deduplicated: {reason}",
        envelope.id
    );
    let decision = verlet_io_core::AdmissionDecision::ObserveOnly { reason };
    let mut receipt = verlet_io_core::KernelIoReceipt::new(envelope, target.clone(), &decision);
    receipt.thread_id = target.address.thread_id;
    receipt
}

fn fork_claim_loser_receipt(
    envelope: &verlet_io_core::IngressEnvelope,
    target: verlet_io_core::ResolvedIoTarget,
    state: &IngressOutcomeState,
) -> verlet_io_core::KernelIoReceipt {
    eprintln!(
        "verlet daemon ingress {} deduplicated: durable fork claim is owned by another first apply",
        envelope.id
    );
    let decision = verlet_io_core::AdmissionDecision::ObserveOnly {
        reason: "durable fork claim is owned by another first apply".to_string(),
    };
    let mut receipt = verlet_io_core::KernelIoReceipt::new(envelope, target, &decision);
    if let IngressOutcomeState::Claimed { claim, .. } = state {
        receipt.thread_id = Some(claim.coordinates.thread_id.to_string());
    }
    receipt
}

fn ingress_outcome_turn_id(state: &IngressOutcomeState) -> Option<&str> {
    let intent = match state {
        IngressOutcomeState::Missing => return None,
        IngressOutcomeState::Claimed { payload, .. } => &payload.intent,
        IngressOutcomeState::Settled { claim_payload, .. } => &claim_payload.intent,
    };
    match intent {
        verlet_history::IngressOutcomeIntent::Turn { turn_id, .. } => Some(turn_id),
        verlet_history::IngressOutcomeIntent::Interrupt {
            replacement_turn_id,
            ..
        } => replacement_turn_id.as_deref(),
        verlet_history::IngressOutcomeIntent::Fork { child_key, .. } => Some(child_key),
        verlet_history::IngressOutcomeIntent::Observe { .. }
        | verlet_history::IngressOutcomeIntent::Reject { .. } => None,
    }
}

fn ingress_outcome_stream_id(state: &IngressOutcomeState) -> verlet_history::EventStreamId {
    let coordinates = match state {
        IngressOutcomeState::Claimed { claim, .. } => &claim.coordinates,
        IngressOutcomeState::Settled { settle, .. } => &settle.coordinates,
        IngressOutcomeState::Missing => unreachable!("missing ingress outcome has no stream"),
    };
    crate::kernel::control_decision::control_stream_id(coordinates)
}

fn ingress_outcome_is_fork(state: &IngressOutcomeState) -> bool {
    let intent = match state {
        IngressOutcomeState::Missing => return false,
        IngressOutcomeState::Claimed { payload, .. } => &payload.intent,
        IngressOutcomeState::Settled { claim_payload, .. } => &claim_payload.intent,
    };
    matches!(intent, verlet_history::IngressOutcomeIntent::Fork { .. })
}

fn turn_execution_evidence(
    events: &[verlet_history::EventRecord],
    turn_id: &str,
    submission_mode: verlet_runtime_contracts::TurnSubmissionMode,
) -> Option<verlet_history::EventRecord> {
    events
        .iter()
        .find(|event| {
            event.kind != verlet_history::EventKind::TurnSubmitted
                && (submission_mode == verlet_runtime_contracts::TurnSubmissionMode::Steer
                    || event.kind != verlet_history::EventKind::SessionEntryAppended)
                && (event
                    .payload
                    .get("turn_id")
                    .and_then(serde_json::Value::as_str)
                    == Some(turn_id)
                    || event
                        .payload
                        .get("subject")
                        .and_then(|subject| subject.get("turn_id"))
                        .and_then(serde_json::Value::as_str)
                        == Some(turn_id))
        })
        .cloned()
}

fn requested_egress_template_from_event(
    event: &verlet_history::EventRecord,
    ingress_contexts: &std::collections::HashMap<
        verlet_history::EventRecordId,
        IngressReceiptContext,
    >,
) -> verlet_io_core::IoResult<Option<RequestedEgressTemplate>> {
    let request =
        serde_json::from_value::<verlet_history::IoEgressRequestedPayload>(event.payload.clone())
            .map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("invalid io.egress.requested payload: {err}"))
        })?;
    let kind = serde_json::from_value::<verlet_io_core::EgressKind>(request.egress_kind).map_err(
        |err| verlet_io_core::IoError::Bridge(format!("invalid requested egress kind: {err}")),
    )?;
    let matched_context = request
        .match_event_id
        .and_then(|match_event_id| ingress_contexts.get(&match_event_id));
    let target = if let Some(context) = &matched_context {
        context.target.clone()
    } else if let Some(target) = request.resolved_target {
        serde_json::from_value::<verlet_io_core::IoTarget>(target).map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("invalid requested egress target: {err}"))
        })?
    } else {
        return Ok(None);
    };
    let (source_ingress_id, metadata) = if let Some(context) = matched_context {
        (context.source_ingress_id.clone(), context.metadata.clone())
    } else {
        (None, target.metadata.clone())
    };
    Ok(Some(RequestedEgressTemplate {
        target,
        kind,
        source_ingress_id,
        metadata,
    }))
}

#[cfg(test)]
fn assistant_text_from_session_event(
    event: &verlet_history::EventRecord,
    entries: &[verlet_history::SessionEntry],
) -> Option<String> {
    let entry = session_entry_for_event(event, entries)?;
    assistant_text_from_entry(entry)
}

#[cfg(test)]
fn session_entry_for_event<'a>(
    event: &verlet_history::EventRecord,
    entries: &'a [verlet_history::SessionEntry],
) -> Option<&'a verlet_history::SessionEntry> {
    if event.kind != verlet_history::EventKind::SessionEntryAppended {
        return None;
    }
    let entry_id = event
        .payload
        .get("entry_id")
        .and_then(serde_json::Value::as_str)?;
    entries
        .iter()
        .find(|entry| entry.entry_id.to_string() == entry_id)
}

fn session_entry_is_user_authored(entry: &verlet_history::SessionEntry) -> bool {
    matches!(
        entry.kind,
        verlet_history::SessionEntryKind::Message {
            message: verlet_history::CanonicalMessage::User { .. },
        } | verlet_history::SessionEntryKind::CustomContextMessage {
            message: verlet_history::CanonicalMessage::User { .. },
        }
    )
}

fn assistant_text_from_entry(entry: &verlet_history::SessionEntry) -> Option<String> {
    let (verlet_history::SessionEntryKind::Message {
        message: verlet_history::CanonicalMessage::Assistant { content, .. },
    }
    | verlet_history::SessionEntryKind::CustomContextMessage {
        message: verlet_history::CanonicalMessage::Assistant { content, .. },
    }) = &entry.kind
    else {
        return None;
    };
    let text = text_from_canonical_content(content);
    (!text.is_empty()).then_some(text)
}

fn text_from_canonical_content(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .filter_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.as_str()),
            verlet_history::CanonicalContent::Image { .. }
            | verlet_history::CanonicalContent::Thinking { .. }
            | verlet_history::CanonicalContent::ToolCall { .. } => None,
        })
        .collect::<Vec<_>>()
        .join("")
}

fn route_id_for_ingress(envelope: &verlet_io_core::IngressEnvelope) -> String {
    envelope
        .metadata
        .get("cooldis_route_id")
        .cloned()
        .unwrap_or_else(|| envelope.source.instance_id.clone())
}

fn ingress_ownership_keys(
    source_envelopes: &[verlet_io_core::IngressEnvelope],
) -> verlet_io_core::IoResult<Vec<IngressOwnershipKey>> {
    let mut keys = std::collections::BTreeMap::<String, String>::new();
    for envelope in source_envelopes {
        let Some(dedupe_key) = envelope.effective_dedupe_key() else {
            // Queue envelopes without an effective dedupe key retain the ADR
            // 0003 envelope-id fold. There is no dedupe row to own or age.
            return Ok(Vec::new());
        };
        let dedupe_key = dedupe_key.stable_key();
        if let Some(existing) = keys.insert(dedupe_key.clone(), envelope.id.clone())
            && existing != envelope.id
        {
            return Err(verlet_io_core::IoError::Bridge(format!(
                "durable ingress batch reuses dedupe key {dedupe_key:?} for different envelopes"
            )));
        }
    }
    Ok(keys
        .into_iter()
        .map(|(dedupe_key, ingress_envelope_id)| IngressOwnershipKey {
            dedupe_key,
            ingress_envelope_id,
        })
        .collect())
}

fn ingress_envelope_digest(
    envelope: &verlet_io_core::IngressEnvelope,
) -> verlet_io_core::IoResult<String> {
    let bytes = serde_json::to_vec(envelope).map_err(|err| {
        verlet_io_core::IoError::Bridge(format!("encode ingress envelope digest: {err}"))
    })?;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    Ok(format!("sha256:{:x}", hasher.finalize()))
}

fn egress_dedupe_key(
    source_event_id: verlet_history::EventRecordId,
    envelope_index: usize,
) -> String {
    format!("{source_event_id}:{envelope_index}")
}

fn egress_kind_name(kind: &verlet_io_core::EgressKind) -> String {
    match kind {
        verlet_io_core::EgressKind::PlatformAction { action, .. } => {
            format!("platform_action:{action}")
        }
        other => other.as_ref().to_string(),
    }
}

fn egress_backoff_delay(base_backoff_ms: u64, failed_attempt: u32) -> std::time::Duration {
    if base_backoff_ms == 0 {
        return std::time::Duration::ZERO;
    }
    let exponent = failed_attempt.saturating_sub(1).min(31);
    let factor = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    std::time::Duration::from_millis(base_backoff_ms.saturating_mul(factor))
}

fn typing_delay_for_text(text: &str, chars_per_second: u32) -> std::time::Duration {
    if chars_per_second == 0 {
        return std::time::Duration::ZERO;
    }
    let chars = text.chars().count();
    if chars == 0 {
        return std::time::Duration::ZERO;
    }
    let seconds =
        (chars as f64 / chars_per_second as f64).min(MAX_TYPING_SIMULATION_DELAY.as_secs_f64());
    std::time::Duration::from_secs_f64(seconds)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CoalescePolicy {
    window_ms: u64,
    max_batch: usize,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct CoalesceGroupKey {
    route_id: String,
    source_scope: String,
    conversation_key: String,
    threading: String,
    actor_key: Option<String>,
}

fn coalesce_policy_for_envelope(
    envelope: &verlet_io_core::IngressEnvelope,
) -> verlet_io_core::IoResult<Option<CoalescePolicy>> {
    let policy = envelope
        .metadata
        .get("cooldis_route_policy")
        .map(String::as_str)
        .unwrap_or("queue_per_conversation");
    let enabled = policy == "coalesce_bursts"
        || envelope
            .metadata
            .get("cooldis_coalesce_bursts")
            .is_some_and(|value| value == "true")
        || envelope.metadata.contains_key("cooldis_coalesce_window_ms")
        || envelope.metadata.contains_key("cooldis_coalesce_max_batch");
    if !enabled {
        return Ok(None);
    }
    let window_ms = envelope
        .metadata
        .get("cooldis_coalesce_window_ms")
        .ok_or_else(|| {
            verlet_io_core::IoError::Queue("coalesce_bursts requires window_ms".to_string())
        })?
        .parse::<u64>()
        .map_err(|err| {
            verlet_io_core::IoError::Queue(format!("invalid coalesce_bursts window_ms: {err}"))
        })?;
    let max_batch = envelope
        .metadata
        .get("cooldis_coalesce_max_batch")
        .ok_or_else(|| {
            verlet_io_core::IoError::Queue("coalesce_bursts requires max_batch".to_string())
        })?
        .parse::<usize>()
        .map_err(|err| {
            verlet_io_core::IoError::Queue(format!("invalid coalesce_bursts max_batch: {err}"))
        })?;
    if window_ms == 0 {
        return Err(verlet_io_core::IoError::Queue(
            "coalesce_bursts window_ms must be greater than zero".to_string(),
        ));
    }
    if max_batch == 0 {
        return Err(verlet_io_core::IoError::Queue(
            "coalesce_bursts max_batch must be greater than zero".to_string(),
        ));
    }
    Ok(Some(CoalescePolicy {
        window_ms,
        max_batch,
    }))
}

fn coalesce_group_key(envelope: &verlet_io_core::IngressEnvelope) -> CoalesceGroupKey {
    let threading = envelope
        .metadata
        .get("cooldis_route_threading")
        .map(String::as_str)
        .unwrap_or("per_conversation");
    let actor_key = if threading == "per_actor" {
        Some(
            envelope
                .actor
                .as_ref()
                .map(|actor| actor.external_actor_id.clone())
                .unwrap_or_else(|| "anonymous".to_string()),
        )
    } else {
        None
    };
    CoalesceGroupKey {
        route_id: route_id_for_envelope(envelope),
        source_scope: envelope.source.stable_scope(),
        conversation_key: envelope.conversation.stable_key(),
        threading: threading.to_string(),
        actor_key,
    }
}

fn sort_coalesce_messages(messages: &mut [verlet_io_core::LeasedIngressEnvelope]) {
    messages.sort_by(|left, right| {
        left.envelope
            .received_at_ms
            .cmp(&right.envelope.received_at_ms)
            .then_with(|| left.message_id.cmp(&right.message_id))
    });
}

fn coalesce_batch_is_ready(
    messages: &[verlet_io_core::LeasedIngressEnvelope],
    policy: CoalescePolicy,
) -> bool {
    messages.len() >= policy.max_batch
        || messages.iter().any(|message| message.attempt > 1)
        || messages
            .first()
            .is_some_and(|message| now_ms() >= coalesce_visible_at_ms(&message.envelope, policy))
}

fn coalesce_visible_at_ms(
    envelope: &verlet_io_core::IngressEnvelope,
    policy: CoalescePolicy,
) -> u64 {
    envelope.received_at_ms.saturating_add(policy.window_ms)
}

fn merged_coalesce_envelope(
    messages: &[verlet_io_core::LeasedIngressEnvelope],
) -> verlet_io_core::IoResult<verlet_io_core::IngressEnvelope> {
    let first = messages.first().ok_or_else(|| {
        verlet_io_core::IoError::Queue("cannot coalesce an empty ingress batch".to_string())
    })?;
    let mut merged = first.envelope.clone();
    merged.content = verlet_io_core::IngressContent::text(
        messages
            .iter()
            .map(|message| message.envelope.content.text_projection())
            .collect::<Vec<_>>()
            .join("\n"),
    );
    merged.attachments = messages
        .iter()
        .flat_map(|message| message.envelope.attachments.clone())
        .collect();
    merged.dedupe_key = None;
    merged.received_at_ms = first.envelope.received_at_ms;
    merged
        .metadata
        .insert("verlet_coalesced".to_string(), "true".to_string());
    merged.metadata.insert(
        "cooldis_coalesced_batch_size".to_string(),
        messages.len().to_string(),
    );
    merged.metadata.insert(
        "cooldis_coalesced_source_envelope_ids".to_string(),
        messages
            .iter()
            .map(|message| message.envelope.id.as_str())
            .collect::<Vec<_>>()
            .join(","),
    );
    Ok(merged)
}

fn fork_source_cut_payload(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    checkpoint: &crate::kernel::runtime_host::runtime_api::ThreadCheckpoint,
    stream_to_sequence: Option<verlet_history::EventSequence>,
) -> verlet_history::ThreadSpawnedForkSourceCutPayload {
    let stream_id = verlet_history::EventStreamId::for_thread(coordinates);
    verlet_history::ThreadSpawnedForkSourceCutPayload {
        thread_id: coordinates.thread_id,
        checkpoint_id: checkpoint.id,
        leaf_entry_id: checkpoint.active_entry_id,
        stream_id,
        stream_to_sequence,
    }
}

fn source_scope(protocol: &str, instance_id: &str) -> String {
    format!("{protocol}:{instance_id}")
}

fn route_id_for_envelope(envelope: &verlet_io_core::IngressEnvelope) -> String {
    envelope
        .metadata
        .get("cooldis_route_id")
        .cloned()
        .unwrap_or_else(|| envelope.source.stable_scope())
}

fn admission_route_policy_id(envelope: &verlet_io_core::IngressEnvelope) -> String {
    format!("admission_route:{}", route_id_for_envelope(envelope))
}

fn admission_route_policy_config(envelope: &verlet_io_core::IngressEnvelope) -> serde_json::Value {
    let mut config = serde_json::json!({
        "route_id": route_id_for_envelope(envelope),
        "policy": envelope
            .metadata
            .get("cooldis_route_policy")
            .map(String::as_str)
            .unwrap_or("queue_per_conversation"),
        "threading": envelope
            .metadata
            .get("cooldis_route_threading")
            .map(String::as_str)
            .unwrap_or("per_conversation"),
    });
    if envelope_declares_coalesce(envelope)
        && let Some(object) = config.as_object_mut()
    {
        let window_ms = envelope
            .metadata
            .get("cooldis_coalesce_window_ms")
            .and_then(|value| value.parse::<u64>().ok());
        let max_batch = envelope
            .metadata
            .get("cooldis_coalesce_max_batch")
            .and_then(|value| value.parse::<usize>().ok());
        object.insert(
            "coalesce_bursts".to_string(),
            serde_json::json!({
                "window_ms": window_ms,
                "max_batch": max_batch,
            }),
        );
    }
    config
}

fn external_message_id(envelope: &verlet_io_core::IngressEnvelope) -> Option<String> {
    envelope
        .metadata
        .get("external_message_id")
        .or_else(|| envelope.metadata.get("telegram_message_id"))
        .cloned()
}

fn ingress_received_control_record(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    envelope: &verlet_io_core::IngressEnvelope,
    ingress_message_id: Option<&str>,
) -> verlet_io_core::IoResult<verlet_history::NewEventRecord> {
    let envelope_value = serde_json::to_value(envelope).map_err(|err| {
        verlet_io_core::IoError::Bridge(format!("ingress envelope codec failed: {err}"))
    })?;
    let payload = verlet_history::IoIngressReceivedPayload {
        route_id: Some(route_id_for_envelope(envelope)),
        dedupe_key: envelope
            .effective_dedupe_key()
            .as_ref()
            .map(|key| key.stable_key()),
        external_conversation_id: Some(envelope.conversation.external_conversation_id.clone()),
        external_actor_id: envelope
            .actor
            .as_ref()
            .map(|actor| actor.external_actor_id.clone()),
        external_message_id: external_message_id(envelope),
        content: witnessed_ingress_content(envelope)?,
        envelope_digest: crate::agent::manifest_bind::canonical_json_hash(&envelope_value)
            .map_err(verlet_bridge_error)?,
    };
    let mut value = serde_json::to_value(payload).map_err(|err| {
        verlet_io_core::IoError::Bridge(format!("io.ingress.received payload codec failed: {err}"))
    })?;
    let object = value.as_object_mut().ok_or_else(|| {
        verlet_io_core::IoError::Bridge(
            "io.ingress.received payload did not encode as object".to_string(),
        )
    })?;
    object.insert(
        "schema".to_string(),
        serde_json::json!(verlet_history::EventKind::IoIngressReceived.payload_schema_id()),
    );
    if let Some(message_id) = ingress_message_id {
        object.insert(
            INGRESS_MESSAGE_ID_FIELD.to_string(),
            serde_json::Value::String(message_id.to_string()),
        );
        object.insert(
            INGRESS_DEDUPE_SEEN_FIELD.to_string(),
            serde_json::Value::Bool(false),
        );
    }
    Ok(verlet_history::NewEventRecord::witnessed(
        coordinates.clone(),
        verlet_history::EventKind::IoIngressReceived,
        value,
    ))
}

fn witnessed_ingress_content(
    envelope: &verlet_io_core::IngressEnvelope,
) -> verlet_io_core::IoResult<Option<serde_json::Value>> {
    match &envelope.content {
        verlet_io_core::IngressContent::Event { kind, .. }
            if kind == verlet_runtime_contracts::handle::HANDLE_DISPATCH_CONTENT_KIND =>
        {
            serde_json::to_value(&envelope.content)
                .map(Some)
                .map_err(|err| {
                    verlet_io_core::IoError::Bridge(format!(
                        "encode witnessed ingress content: {err}"
                    ))
                })
        }
        _ => Ok(None),
    }
}

fn event_admission_decision(
    decision: &verlet_io_core::AdmissionDecision,
) -> verlet_history::AdmissionDecision {
    match decision {
        verlet_io_core::AdmissionDecision::Queue { .. } => verlet_history::AdmissionDecision::Queue,
        verlet_io_core::AdmissionDecision::Steer { .. } => verlet_history::AdmissionDecision::Steer,
        verlet_io_core::AdmissionDecision::Interrupt { .. } => {
            verlet_history::AdmissionDecision::Interrupt
        }
        verlet_io_core::AdmissionDecision::Fork { .. } => verlet_history::AdmissionDecision::Fork,
        verlet_io_core::AdmissionDecision::ObserveOnly { .. } => {
            verlet_history::AdmissionDecision::Observe
        }
        verlet_io_core::AdmissionDecision::Reject { .. } => {
            verlet_history::AdmissionDecision::Reject
        }
    }
}

fn admissible_decisions_for_envelope(
    envelope: &verlet_io_core::IngressEnvelope,
) -> Vec<verlet_history::AdmissionDecision> {
    let mut admissible = match envelope
        .metadata
        .get("cooldis_route_policy")
        .map(String::as_str)
        .unwrap_or("queue_per_conversation")
    {
        "observe_only" => vec![verlet_history::AdmissionDecision::Observe],
        "reject" => vec![verlet_history::AdmissionDecision::Reject],
        "steer" | "steer_when_active" => {
            vec![
                verlet_history::AdmissionDecision::Queue,
                verlet_history::AdmissionDecision::Steer,
            ]
        }
        "interrupt" | "interrupt_on_new_dm" => vec![verlet_history::AdmissionDecision::Interrupt],
        "fork" | "fork_on_new_dm" => vec![verlet_history::AdmissionDecision::Fork],
        "coalesce_bursts" => vec![
            verlet_history::AdmissionDecision::Queue,
            verlet_history::AdmissionDecision::Coalesce,
        ],
        _ => vec![verlet_history::AdmissionDecision::Queue],
    };
    if envelope_declares_coalesce(envelope)
        && !admissible.contains(&verlet_history::AdmissionDecision::Coalesce)
    {
        admissible.push(verlet_history::AdmissionDecision::Coalesce);
    }
    admissible
}

fn envelope_declares_coalesce(envelope: &verlet_io_core::IngressEnvelope) -> bool {
    envelope
        .metadata
        .get("cooldis_route_policy")
        .is_some_and(|policy| policy == "coalesce_bursts")
        || envelope
            .metadata
            .get("cooldis_coalesce_bursts")
            .is_some_and(|value| value == "true")
        || envelope.metadata.contains_key("cooldis_coalesce_window_ms")
        || envelope.metadata.contains_key("cooldis_coalesce_max_batch")
}

fn is_clock_tick_envelope(envelope: &verlet_io_core::IngressEnvelope) -> bool {
    envelope.source.protocol == crate::daemon::clock_route::CLOCK_TICK_ROUTE_KIND
        && matches!(
            &envelope.content,
            verlet_io_core::IngressContent::Event { kind, .. } if kind == crate::daemon::clock_route::TIMER_FIRED_ENVELOPE_KIND
        )
}

fn clock_tick_coordinates(
    envelope: &verlet_io_core::IngressEnvelope,
) -> verlet_io_core::IoResult<verlet_runtime_contracts::ThreadCoordinates> {
    let principal = envelope.principal.as_ref().ok_or_else(|| {
        verlet_io_core::IoError::InvalidEnvelope("principal is required".to_string())
    })?;
    Ok(verlet_runtime_contracts::ThreadCoordinates {
        tenant_id: principal.tenant_id.clone(),
        user_id: principal.principal_id.clone(),
        session_id: required_metadata(envelope, "cooldis_session_id")?.to_string(),
        thread_id: verlet_runtime_contracts::ThreadId::parse_str(required_metadata(
            envelope,
            "verlet_thread_id",
        )?)
        .map_err(|err| {
            verlet_io_core::IoError::Bridge(format!("invalid clock.tick thread id: {err}"))
        })?,
    })
}

fn clock_tick_payload(
    envelope: &verlet_io_core::IngressEnvelope,
) -> verlet_io_core::IoResult<verlet_history::TimerFiredPayload> {
    if let verlet_io_core::IngressContent::Event { kind, payload } = &envelope.content
        && kind == crate::daemon::clock_route::TIMER_FIRED_ENVELOPE_KIND
    {
        return serde_json::from_value::<verlet_history::TimerFiredPayload>(payload.clone())
            .map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("invalid clock.tick payload: {err}"))
            });
    }

    Ok(verlet_history::TimerFiredPayload {
        mandate_event_id: crate::kernel::mandate_lifecycle::parse_mandate_event_id(
            required_metadata(envelope, "cooldis_mandate_event_id")?,
        )
        .map_err(verlet_bridge_error)?,
        scheduled_for: required_metadata(envelope, "cooldis_scheduled_for")?.to_string(),
        occurrence_index: required_metadata(envelope, "verlet_occurrence_index")?
            .parse::<u64>()
            .map_err(|err| {
                verlet_io_core::IoError::Bridge(format!(
                    "invalid clock.tick occurrence index: {err}"
                ))
            })?,
        catch_up: required_metadata(envelope, "verlet_catch_up")?
            .parse::<bool>()
            .map_err(|err| {
                verlet_io_core::IoError::Bridge(format!("invalid clock.tick catch_up flag: {err}"))
            })?,
    })
}

fn required_metadata<'a>(
    envelope: &'a verlet_io_core::IngressEnvelope,
    key: &str,
) -> verlet_io_core::IoResult<&'a str> {
    envelope
        .metadata
        .get(key)
        .map(String::as_str)
        .ok_or_else(|| {
            verlet_io_core::IoError::Bridge(format!("clock.tick missing metadata {key:?}"))
        })
}

fn open_egress_state_connection(dsn: &str) -> verlet_io_core::IoResult<rusqlite::Connection> {
    let path = sqlite_path_from_dsn(dsn)?;
    if path == std::path::Path::new(":memory:") {
        return rusqlite::Connection::open_in_memory().map_err(egress_state_error);
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            verlet_io_core::IoError::Queue(format!(
                "create egress sqlite directory {}: {err}",
                parent.display()
            ))
        })?;
    }
    if !path.exists() {
        std::fs::File::create(&path).map_err(|err| {
            verlet_io_core::IoError::Queue(format!(
                "create egress sqlite file {}: {err}",
                path.display()
            ))
        })?;
    }
    let connection = rusqlite::Connection::open(path).map_err(egress_state_error)?;
    connection
        .busy_timeout(EGRESS_SQLITE_BUSY_TIMEOUT)
        .map_err(egress_state_error)?;
    Ok(connection)
}

fn sqlite_path_from_dsn(dsn: &str) -> verlet_io_core::IoResult<std::path::PathBuf> {
    let Some(path) = dsn.strip_prefix("sqlite://") else {
        return Err(verlet_io_core::IoError::Queue(format!(
            "egress projector requires a sqlite:// DSN, got {dsn:?}"
        )));
    };
    Ok(std::path::PathBuf::from(path))
}

fn init_egress_state_schema(connection: &rusqlite::Connection) -> verlet_io_core::IoResult<()> {
    connection
        .execute_batch(
            "
            CREATE TABLE IF NOT EXISTS cooldis_daemon_egress_threads (
                route_id TEXT NOT NULL,
                source_scope TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (route_id, thread_id)
            );
            CREATE INDEX IF NOT EXISTS idx_cooldis_daemon_egress_threads_route
                ON cooldis_daemon_egress_threads (route_id, updated_at_ms);
            CREATE TABLE IF NOT EXISTS cooldis_ingress_dedupe (
                queue_name TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                envelope_id TEXT NOT NULL,
                inserted_at_ms INTEGER NOT NULL,
                PRIMARY KEY (queue_name, dedupe_key)
            );
            CREATE TABLE IF NOT EXISTS cooldis_daemon_ingress_bindings (
                route_id TEXT NOT NULL,
                source_scope TEXT NOT NULL,
                scope_key TEXT NOT NULL,
                tenant_id TEXT NOT NULL,
                user_id TEXT NOT NULL,
                session_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (route_id, source_scope, scope_key)
            );
            INSERT OR IGNORE INTO cooldis_daemon_ingress_bindings (
                route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
            )
            SELECT route_id, source_scope, scope_key, tenant_id, user_id, session_id, thread_id, updated_at_ms
            FROM cooldis_daemon_egress_threads AS candidate
            WHERE candidate.rowid = (
                SELECT latest.rowid
                FROM cooldis_daemon_egress_threads AS latest
                WHERE latest.route_id = candidate.route_id
                  AND latest.source_scope = candidate.source_scope
                  AND latest.scope_key = candidate.scope_key
                ORDER BY latest.updated_at_ms DESC, latest.rowid DESC
                LIMIT 1
            );
            CREATE TABLE IF NOT EXISTS cooldis_daemon_ingress_ownership (
                dedupe_key TEXT NOT NULL,
                ownership_id TEXT NOT NULL,
                ingress_envelope_id TEXT NOT NULL,
                stream_id TEXT NOT NULL,
                attempt INTEGER NOT NULL,
                created_at_ms INTEGER NOT NULL,
                PRIMARY KEY (dedupe_key, ownership_id)
            );
            CREATE INDEX IF NOT EXISTS idx_cooldis_daemon_ingress_ownership_key
                ON cooldis_daemon_ingress_ownership
                    (dedupe_key, attempt DESC, created_at_ms DESC, ownership_id DESC);
            CREATE TRIGGER IF NOT EXISTS cooldis_ingress_dedupe_delete_ownership
            AFTER DELETE ON cooldis_ingress_dedupe
            BEGIN
                DELETE FROM cooldis_daemon_ingress_ownership
                WHERE dedupe_key = OLD.dedupe_key;
            END;
            CREATE TABLE IF NOT EXISTS cooldis_daemon_egress_cursors (
                route_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                cursor_json TEXT NOT NULL,
                updated_at_ms INTEGER NOT NULL,
                PRIMARY KEY (route_id, thread_id)
            );
            CREATE TABLE IF NOT EXISTS cooldis_daemon_egress_dead_letters (
                id TEXT PRIMARY KEY,
                route_id TEXT NOT NULL,
                thread_id TEXT NOT NULL,
                source_event_id TEXT NOT NULL,
                envelope_index INTEGER NOT NULL,
                dedupe_key TEXT NOT NULL,
                egress_kind TEXT NOT NULL,
                attempts INTEGER NOT NULL,
                error TEXT NOT NULL,
                envelope_json TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_cooldis_daemon_egress_dead_letters_route
                ON cooldis_daemon_egress_dead_letters (route_id, created_at_ms);
            ",
        )
        .map_err(egress_state_error)
}

/// Deterministic crash cut for `verlet-restart-smoke`.
///
/// When the test-only variable names a marker path, the daemon creates it
/// after the durable route binding commits and parks before publishing the
/// binding in memory or submitting the first turn. The smoke then SIGKILLs the
/// process. Normal daemon runs do not set the variable and return immediately.
async fn pause_after_ingress_binding_for_restart_smoke() -> verlet_io_core::IoResult<()> {
    let Some(marker) = std::env::var_os("VERLET_TEST_PAUSE_AFTER_INGRESS_BINDING") else {
        return Ok(());
    };
    let marker = std::path::PathBuf::from(marker);
    std::fs::write(&marker, b"binding persisted\n").map_err(|err| {
        verlet_io_core::IoError::Bridge(format!(
            "write restart smoke binding marker {}: {err}",
            marker.display()
        ))
    })?;
    std::future::pending::<()>().await;
    Ok(())
}

fn verlet_bridge_error(err: crate::kernel::runtime_host::VerletError) -> verlet_io_core::IoError {
    verlet_io_core::IoError::Bridge(err.to_string())
}

fn verlet_history_error(err: impl std::fmt::Display) -> verlet_io_core::IoError {
    verlet_io_core::IoError::Bridge(err.to_string())
}

fn egress_state_error(err: rusqlite::Error) -> verlet_io_core::IoError {
    verlet_io_core::IoError::Queue(format!("egress state sqlite: {err}"))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests;
