use chrono::TimeZone as _;
use std::str::FromStr as _;
use verlet_history::EventStore as _;

pub const CLOCK_TICK_ROUTE_KIND: &str = "clock.tick";
pub const TIMER_FIRED_ENVELOPE_KIND: &str = "timer.fired";
const DEFAULT_CLOCK_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

pub trait DaemonClock: Send + Sync {
    fn now(&self) -> chrono::DateTime<chrono::Utc>;
}

#[derive(Clone, Debug)]
pub struct SystemDaemonClock;

impl DaemonClock for SystemDaemonClock {
    fn now(&self) -> chrono::DateTime<chrono::Utc> {
        chrono::Utc::now()
    }
}

pub struct VerletDaemonClockRoute {
    route_id: String,
    store: verlet_history_sqlite::SqliteSessionStore,
    sink: std::sync::Arc<dyn verlet_io_core::IngressSink>,
    clock: std::sync::Arc<dyn DaemonClock>,
    started_at: chrono::DateTime<chrono::Utc>,
    poll_interval: std::time::Duration,
}

impl VerletDaemonClockRoute {
    pub fn new(
        route_id: impl Into<String>,
        store: verlet_history_sqlite::SqliteSessionStore,
        sink: std::sync::Arc<dyn verlet_io_core::IngressSink>,
        clock: std::sync::Arc<dyn DaemonClock>,
    ) -> Self {
        let started_at = clock.now();
        Self {
            route_id: route_id.into(),
            store,
            sink,
            clock,
            started_at,
            poll_interval: DEFAULT_CLOCK_POLL_INTERVAL,
        }
    }

    #[cfg(test)]
    pub fn with_started_at(mut self, started_at: chrono::DateTime<chrono::Utc>) -> Self {
        self.started_at = started_at;
        self
    }

    pub fn with_poll_interval(mut self, poll_interval: std::time::Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    pub async fn enqueue_due_once(&self) -> crate::kernel::runtime_host::VerletResult<usize> {
        let now = self.clock.now();
        let mut schedule = self.build_due_schedule(now).await?;
        let mut enqueued = 0;
        while let Some(std::cmp::Reverse(tick)) = schedule.peek().cloned() {
            if tick.scheduled_for > now {
                break;
            }
            schedule.pop();
            let ack = self
                .sink
                .submit(tick.envelope(&self.route_id, now)?)
                .await
                .map_err(|err| {
                    crate::kernel::runtime_host::VerletError::RuntimeFactory(err.to_string())
                })?;
            if ack.accepted {
                enqueued += 1;
            }
        }
        Ok(enqueued)
    }

    pub async fn run(self) {
        let mut retry_state = crate::daemon::retry::RetryState::new(self.poll_interval);
        loop {
            let decision = match self.enqueue_due_once().await {
                Ok(_) => retry_state.on_success(self.poll_interval),
                Err(err) => retry_state.on_failure(&err.to_string(), self.poll_interval),
            };
            if let Some(log) = decision.log {
                eprintln!(
                    "{}",
                    log.message(
                        &format!("verlet clock route {}", self.route_id),
                        decision.delay
                    )
                );
            }
            tokio::time::sleep(decision.delay).await;
        }
    }

    async fn build_due_schedule(
        &self,
        now: chrono::DateTime<chrono::Utc>,
    ) -> crate::kernel::runtime_host::VerletResult<
        std::collections::BinaryHeap<std::cmp::Reverse<ScheduledTick>>,
    > {
        let mut heap = std::collections::BinaryHeap::new();
        for coordinates in self
            .store
            .list_control_stream_coordinates()
            .await
            .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?
        {
            let active =
                crate::kernel::mandate_lifecycle::list_active_mandates(&self.store, &coordinates)
                    .await?;
            let fired = fired_occurrence_indices(&self.store, &coordinates).await?;
            for mandate in active {
                if let Some(tick) =
                    next_tick_for_mandate(&coordinates, &mandate, &fired, self.started_at, now)?
                {
                    heap.push(std::cmp::Reverse(tick));
                }
            }
        }
        Ok(heap)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct ScheduledTick {
    coordinates: verlet_runtime_contracts::ThreadCoordinates,
    mandate_event_id: verlet_history::EventRecordId,
    scheduled_for: chrono::DateTime<chrono::Utc>,
    occurrence_index: u64,
    catch_up: bool,
}

impl ScheduledTick {
    fn envelope(
        &self,
        route_id: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> crate::kernel::runtime_host::VerletResult<verlet_io_core::IngressEnvelope> {
        let payload = verlet_history::TimerFiredPayload {
            mandate_event_id: self.mandate_event_id,
            scheduled_for: self
                .scheduled_for
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            occurrence_index: self.occurrence_index,
            catch_up: self.catch_up,
        };
        let source = verlet_io_core::IoSource::new(CLOCK_TICK_ROUTE_KIND, route_id);
        let delivery_id = format!("{}:{}", self.mandate_event_id, self.occurrence_index);
        let dedupe_key = verlet_io_core::IoDedupeKey::for_source(&source, delivery_id.clone());
        Ok(verlet_io_core::IngressEnvelope::new(
            source,
            verlet_io_core::IoConversation::new(
                format!("thread:{}", self.coordinates.thread_id),
                verlet_io_core::ConversationKind::System,
            ),
            verlet_io_core::IngressContent::Event {
                kind: TIMER_FIRED_ENVELOPE_KIND.to_string(),
                payload: serde_json::to_value(&payload).map_err(json_error)?,
            },
            now.timestamp_millis().max(0) as u64,
        )
        .with_dedupe_key(dedupe_key)
        .with_delivery(verlet_io_core::IoDelivery::new(delivery_id))
        .with_principal(verlet_io_core::IoPrincipal::new(
            self.coordinates.tenant_id.clone(),
            self.coordinates.user_id.clone(),
            format!("mandate:{}", self.mandate_event_id),
        ))
        .with_metadata("cooldis_route_id", route_id.to_string())
        .with_metadata("cooldis_session_id", self.coordinates.session_id.clone())
        .with_metadata("verlet_thread_id", self.coordinates.thread_id.to_string())
        .with_metadata(
            "cooldis_mandate_event_id",
            self.mandate_event_id.to_string(),
        )
        .with_metadata(
            "cooldis_scheduled_for",
            self.scheduled_for
                .to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
        )
        .with_metadata("verlet_occurrence_index", self.occurrence_index.to_string())
        .with_metadata("verlet_catch_up", self.catch_up.to_string()))
    }
}

impl Ord for ScheduledTick {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.scheduled_for
            .cmp(&other.scheduled_for)
            .then_with(|| self.occurrence_index.cmp(&other.occurrence_index))
            .then_with(|| {
                self.mandate_event_id
                    .to_string()
                    .cmp(&other.mandate_event_id.to_string())
            })
    }
}

impl PartialOrd for ScheduledTick {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

async fn fired_occurrence_indices(
    store: &verlet_history_sqlite::SqliteSessionStore,
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
) -> crate::kernel::runtime_host::VerletResult<
    std::collections::HashSet<(verlet_history::EventRecordId, u64)>,
> {
    let events = store
        .read_events(
            &crate::kernel::control_decision::control_stream_id(coordinates),
            None,
        )
        .await
        .map_err(|err| crate::kernel::runtime_host::VerletError::History(err.to_string()))?;
    let mut fired = std::collections::HashSet::new();
    for event in events {
        if event.kind != verlet_history::EventKind::TimerFired {
            continue;
        }
        let payload = serde_json::from_value::<verlet_history::TimerFiredPayload>(event.payload)
            .map_err(|err| {
                crate::kernel::runtime_host::VerletError::History(format!(
                    "timer.fired payload is invalid: {err}"
                ))
            })?;
        fired.insert((payload.mandate_event_id, payload.occurrence_index));
    }
    Ok(fired)
}

fn next_tick_for_mandate(
    coordinates: &verlet_runtime_contracts::ThreadCoordinates,
    mandate: &crate::kernel::mandate_lifecycle::ActiveMandate,
    fired: &std::collections::HashSet<(verlet_history::EventRecordId, u64)>,
    route_started_at: chrono::DateTime<chrono::Utc>,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::kernel::runtime_host::VerletResult<Option<ScheduledTick>> {
    let Some(schedule) = mandate.payload.schedule.as_ref() else {
        return Ok(None);
    };
    let catch_up_policy = mandate.payload.catch_up.unwrap_or_default();
    let mandate_event_id = mandate.event.id;
    let max_occurrences = mandate.payload.max_occurrences.map(u64::from);
    let start = datetime_from_millis(mandate.event.created_at_ms)?;
    let last_fired = fired
        .iter()
        .filter_map(|(event_id, index)| (*event_id == mandate_event_id).then_some(*index))
        .max();
    let mut occurrence_index = last_fired.map(|index| index + 1).unwrap_or(0);
    if max_occurrences.is_some_and(|max| occurrence_index >= max) {
        return Ok(None);
    }

    let mut scheduled_for = occurrence_at_index(schedule, start, occurrence_index)?;
    match catch_up_policy {
        crate::kernel::control_decision::MandateCatchUpPolicy::SkipMissed
            if scheduled_for < route_started_at =>
        {
            let Some(index) = first_occurrence_index_at_or_after(
                schedule,
                start,
                route_started_at,
                max_occurrences,
            )?
            else {
                return Ok(None);
            };
            occurrence_index = index;
            scheduled_for = occurrence_at_index(schedule, start, occurrence_index)?;
        }
        crate::kernel::control_decision::MandateCatchUpPolicy::CoalesceMissed
            if scheduled_for < route_started_at && scheduled_for <= now =>
        {
            if let Some(index) =
                latest_occurrence_index_at_or_before(schedule, start, now, max_occurrences)?
                && index >= occurrence_index
            {
                occurrence_index = index;
                scheduled_for = occurrence_at_index(schedule, start, occurrence_index)?;
            }
        }
        _ => {}
    }

    if max_occurrences.is_some_and(|max| occurrence_index >= max) {
        return Ok(None);
    }
    if fired.contains(&(mandate_event_id, occurrence_index)) {
        return Ok(None);
    }
    Ok(Some(ScheduledTick {
        coordinates: coordinates.clone(),
        mandate_event_id,
        scheduled_for,
        occurrence_index,
        catch_up: catch_up_policy
            == crate::kernel::control_decision::MandateCatchUpPolicy::CoalesceMissed
            && scheduled_for < route_started_at,
    }))
}

/// Rejects a zero interval before it reaches the occurrence arithmetic.
///
/// `mandate_lifecycle::validate_schedule` enforces `every_ms >=
/// MIN_MANDATE_INTERVAL_MS` on the write path, but this route replays mandate
/// payloads it read back out of the event store and never validated itself. A
/// zero that reached here would divide by zero in the two index helpers below,
/// panicking the route; `occurrence_at_index` would instead quietly place every
/// occurrence at `start` and fire forever. Both are worse than a refusal.
fn reject_zero_interval(every_ms: u64) -> crate::kernel::runtime_host::VerletResult<u64> {
    if every_ms == 0 {
        return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
            "interval schedule every_ms must be greater than zero".to_string(),
        ));
    }
    Ok(every_ms)
}

fn occurrence_at_index(
    schedule: &crate::kernel::control_decision::MandateSchedulePayload,
    start: chrono::DateTime<chrono::Utc>,
    index: u64,
) -> crate::kernel::runtime_host::VerletResult<chrono::DateTime<chrono::Utc>> {
    match schedule {
        crate::kernel::control_decision::MandateSchedulePayload::Interval { every_ms } => {
            let multiplier = index.checked_add(1).ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "interval occurrence index overflowed".to_string(),
                )
            })?;
            let every_ms = reject_zero_interval(*every_ms)?;
            let offset_ms = every_ms.checked_mul(multiplier).ok_or_else(|| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "interval occurrence offset overflowed".to_string(),
                )
            })?;
            let offset_ms = i64::try_from(offset_ms).map_err(|_| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "interval occurrence offset overflowed".to_string(),
                )
            })?;
            start
                .checked_add_signed(chrono::Duration::milliseconds(offset_ms))
                .ok_or_else(|| {
                    crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        "interval occurrence timestamp overflowed".to_string(),
                    )
                })
        }
        crate::kernel::control_decision::MandateSchedulePayload::At { when } => {
            if index > 0 {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "at schedule has only one occurrence".to_string(),
                ));
            }
            parse_utc(when)
        }
        crate::kernel::control_decision::MandateSchedulePayload::Cron { expr, tz } => {
            let (cron, timezone) = cron_schedule(expr, tz)?;
            cron.iter_after(start.with_timezone(&timezone))
                .nth(index as usize)
                .map(|dt| dt.with_timezone(&chrono::Utc))
                .ok_or_else(|| {
                    crate::kernel::runtime_host::VerletError::RuntimeExecution(
                        "cron schedule did not produce an occurrence".to_string(),
                    )
                })
        }
    }
}

fn first_occurrence_index_at_or_after(
    schedule: &crate::kernel::control_decision::MandateSchedulePayload,
    start: chrono::DateTime<chrono::Utc>,
    threshold: chrono::DateTime<chrono::Utc>,
    max_occurrences: Option<u64>,
) -> crate::kernel::runtime_host::VerletResult<Option<u64>> {
    match schedule {
        crate::kernel::control_decision::MandateSchedulePayload::Interval { every_ms } => {
            let every_ms = reject_zero_interval(*every_ms)?;
            let every_ms_i64 = i64::try_from(every_ms).map_err(|_| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "interval duration overflowed".to_string(),
                )
            })?;
            let Some(first) =
                start.checked_add_signed(chrono::Duration::milliseconds(every_ms_i64))
            else {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "interval occurrence timestamp overflowed".to_string(),
                ));
            };
            if threshold <= first {
                return Ok(Some(0));
            }
            let diff_ms = threshold.signed_duration_since(first).num_milliseconds() as u64;
            let index = diff_ms.div_ceil(every_ms);
            if max_occurrences.is_some_and(|max| index >= max) {
                Ok(None)
            } else {
                Ok(Some(index))
            }
        }
        crate::kernel::control_decision::MandateSchedulePayload::At { when } => {
            let when = parse_utc(when)?;
            Ok((when >= threshold)
                .then_some(0)
                .filter(|index| !max_occurrences.is_some_and(|max| *index >= max)))
        }
        crate::kernel::control_decision::MandateSchedulePayload::Cron { expr, tz } => {
            let (cron, timezone) = cron_schedule(expr, tz)?;
            for (index, occurrence) in cron.iter_after(start.with_timezone(&timezone)).enumerate() {
                let index = index as u64;
                if max_occurrences.is_some_and(|max| index >= max) {
                    return Ok(None);
                }
                if occurrence.with_timezone(&chrono::Utc) >= threshold {
                    return Ok(Some(index));
                }
            }
            Ok(None)
        }
    }
}

fn latest_occurrence_index_at_or_before(
    schedule: &crate::kernel::control_decision::MandateSchedulePayload,
    start: chrono::DateTime<chrono::Utc>,
    threshold: chrono::DateTime<chrono::Utc>,
    max_occurrences: Option<u64>,
) -> crate::kernel::runtime_host::VerletResult<Option<u64>> {
    match schedule {
        crate::kernel::control_decision::MandateSchedulePayload::Interval { every_ms } => {
            let every_ms = reject_zero_interval(*every_ms)?;
            let every_ms_i64 = i64::try_from(every_ms).map_err(|_| {
                crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "interval duration overflowed".to_string(),
                )
            })?;
            let Some(first) =
                start.checked_add_signed(chrono::Duration::milliseconds(every_ms_i64))
            else {
                return Err(crate::kernel::runtime_host::VerletError::RuntimeExecution(
                    "interval occurrence timestamp overflowed".to_string(),
                ));
            };
            if threshold < first {
                return Ok(None);
            }
            let diff_ms = threshold.signed_duration_since(first).num_milliseconds() as u64;
            let mut index = diff_ms / every_ms;
            if let Some(max) = max_occurrences {
                if max == 0 {
                    return Ok(None);
                }
                index = index.min(max - 1);
            }
            Ok(Some(index))
        }
        crate::kernel::control_decision::MandateSchedulePayload::At { when } => {
            let when = parse_utc(when)?;
            Ok((when <= threshold)
                .then_some(0)
                .filter(|index| !max_occurrences.is_some_and(|max| *index >= max)))
        }
        crate::kernel::control_decision::MandateSchedulePayload::Cron { expr, tz } => {
            let (cron, timezone) = cron_schedule(expr, tz)?;
            let mut latest = None;
            for (index, occurrence) in cron.iter_after(start.with_timezone(&timezone)).enumerate() {
                let index = index as u64;
                if max_occurrences.is_some_and(|max| index >= max) {
                    break;
                }
                if occurrence.with_timezone(&chrono::Utc) > threshold {
                    break;
                }
                latest = Some(index);
            }
            Ok(latest)
        }
    }
}

fn cron_schedule(
    expr: &str,
    tz: &str,
) -> crate::kernel::runtime_host::VerletResult<(croner::Cron, chrono_tz::Tz)> {
    let cron = croner::Cron::from_str(expr).map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "malformed cron expression: {err}"
        ))
    })?;
    let timezone = tz.parse::<chrono_tz::Tz>().map_err(|err| {
        crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
            "unknown IANA timezone {tz:?}: {err}"
        ))
    })?;
    Ok((cron, timezone))
}

fn datetime_from_millis(
    value: i64,
) -> crate::kernel::runtime_host::VerletResult<chrono::DateTime<chrono::Utc>> {
    chrono::Utc
        .timestamp_millis_opt(value)
        .single()
        .ok_or_else(|| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "invalid mandate event timestamp {value}"
            ))
        })
}

fn parse_utc(
    value: &str,
) -> crate::kernel::runtime_host::VerletResult<chrono::DateTime<chrono::Utc>> {
    chrono::DateTime::parse_from_rfc3339(value)
        .map(|value| value.with_timezone(&chrono::Utc))
        .map_err(|err| {
            crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
                "invalid RFC3339 instant: {err}"
            ))
        })
}

fn json_error(err: serde_json::Error) -> crate::kernel::runtime_host::VerletError {
    crate::kernel::runtime_host::VerletError::RuntimeExecution(format!(
        "failed to encode clock tick payload: {err}"
    ))
}

#[cfg(test)]
mod tests {
    use verlet_history::EventStore as _;

    #[derive(Clone)]
    struct FakeClock {
        now: std::sync::Arc<std::sync::Mutex<chrono::DateTime<chrono::Utc>>>,
    }

    impl FakeClock {
        fn new(now: chrono::DateTime<chrono::Utc>) -> Self {
            Self {
                now: std::sync::Arc::new(std::sync::Mutex::new(now)),
            }
        }
    }

    impl crate::daemon::clock_route::DaemonClock for FakeClock {
        fn now(&self) -> chrono::DateTime<chrono::Utc> {
            *self.now.lock().unwrap()
        }
    }

    struct CaptureSink {
        envelopes: std::sync::Arc<std::sync::Mutex<Vec<verlet_io_core::IngressEnvelope>>>,
    }

    #[async_trait::async_trait]
    impl verlet_io_core::IngressSink for CaptureSink {
        async fn submit(
            &self,
            envelope: verlet_io_core::IngressEnvelope,
        ) -> verlet_io_core::IoResult<verlet_io_core::IngressAck> {
            let ack = verlet_io_core::IngressAck::accepted(&envelope);
            self.envelopes.lock().unwrap().push(envelope);
            Ok(ack)
        }
    }

    fn dt(value: &str) -> chrono::DateTime<chrono::Utc> {
        crate::daemon::clock_route::parse_utc(value).unwrap()
    }

    fn coordinates() -> verlet_runtime_contracts::ThreadCoordinates {
        verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session")
    }

    fn active_mandate(
        schedule: crate::kernel::control_decision::MandateSchedulePayload,
        catch_up: crate::kernel::control_decision::MandateCatchUpPolicy,
        started_at: chrono::DateTime<chrono::Utc>,
        max_occurrences: Option<u32>,
    ) -> crate::kernel::mandate_lifecycle::ActiveMandate {
        let coordinates = coordinates();
        let event = verlet_history::EventRecord {
            id: verlet_history::EventRecordId::new(),
            stream_id: verlet_history::EventStreamId::new(format!(
                "control:{}",
                coordinates.thread_id
            )),
            sequence: verlet_history::EventSequence::new(1),
            coordinates: coordinates.clone(),
            created_at_ms: started_at.timestamp_millis(),
            kind: verlet_history::EventKind::MandateStarted,
            origin: verlet_history::EventOrigin::Witnessed,
            provenance: Default::default(),
            payload: serde_json::json!({}),
        };
        crate::kernel::mandate_lifecycle::ActiveMandate {
            event,
            payload: crate::kernel::control_decision::MandateStartedPayload {
                subject: crate::kernel::control_decision::MandateSubject {
                    thread_id: Some(coordinates.thread_id.to_string()),
                    loop_id: None,
                },
                mandate_id: "mandate-test".to_string(),
                snapshot_id: "schedule.v1".to_string(),
                thread_id: Some(coordinates.thread_id.to_string()),
                max_continuations: None,
                expires_at_ms: None,
                schedule: Some(schedule),
                max_occurrences,
                catch_up: Some(catch_up),
                input_template: None,
            },
        }
    }

    #[test]
    fn coalesce_missed_interval_fires_once_at_latest_missed_index() {
        let mandate = active_mandate(
            crate::kernel::control_decision::MandateSchedulePayload::Interval { every_ms: 60_000 },
            crate::kernel::control_decision::MandateCatchUpPolicy::CoalesceMissed,
            dt("2026-01-01T00:00:00Z"),
            None,
        );
        let tick = crate::daemon::clock_route::next_tick_for_mandate(
            &coordinates(),
            &mandate,
            &std::collections::HashSet::new(),
            dt("2026-01-01T00:02:30Z"),
            dt("2026-01-01T00:02:30Z"),
        )
        .unwrap()
        .unwrap();

        assert_eq!(tick.occurrence_index, 1);
        assert_eq!(tick.scheduled_for, dt("2026-01-01T00:02:00Z"));
        assert!(tick.catch_up);
    }

    #[test]
    fn skip_missed_interval_waits_until_next_on_time_occurrence() {
        let mandate = active_mandate(
            crate::kernel::control_decision::MandateSchedulePayload::Interval { every_ms: 60_000 },
            crate::kernel::control_decision::MandateCatchUpPolicy::SkipMissed,
            dt("2026-01-01T00:00:00Z"),
            None,
        );
        let tick = crate::daemon::clock_route::next_tick_for_mandate(
            &coordinates(),
            &mandate,
            &std::collections::HashSet::new(),
            dt("2026-01-01T00:01:30Z"),
            dt("2026-01-01T00:01:30Z"),
        )
        .unwrap()
        .unwrap();

        assert_eq!(tick.occurrence_index, 1);
        assert_eq!(tick.scheduled_for, dt("2026-01-01T00:02:00Z"));
        assert!(!tick.catch_up);
    }

    #[test]
    fn a_zero_interval_is_refused_instead_of_dividing_by_zero() {
        // The write path (mandate_lifecycle::validate_schedule) enforces
        // every_ms >= MIN_MANDATE_INTERVAL_MS, but this route replays mandate
        // payloads read back out of the event store, which it never validated.
        // A zero reaching the index helpers used to panic the whole route.
        let schedule =
            crate::kernel::control_decision::MandateSchedulePayload::Interval { every_ms: 0 };
        let start = dt("2026-01-01T00:00:00Z");
        let threshold = dt("2026-01-01T00:05:00Z");

        assert!(
            crate::daemon::clock_route::occurrence_at_index(&schedule, start, 0)
                .unwrap_err()
                .to_string()
                .contains("every_ms must be greater than zero")
        );
        assert!(
            crate::daemon::clock_route::first_occurrence_index_at_or_after(
                &schedule, start, threshold, None,
            )
            .unwrap_err()
            .to_string()
            .contains("every_ms must be greater than zero")
        );
        assert!(
            crate::daemon::clock_route::latest_occurrence_index_at_or_before(
                &schedule, start, threshold, None,
            )
            .unwrap_err()
            .to_string()
            .contains("every_ms must be greater than zero")
        );
    }

    #[test]
    fn at_schedule_fires_once_and_respects_max_occurrences() {
        let schedule = crate::kernel::control_decision::MandateSchedulePayload::At {
            when: "2026-01-01T00:01:00Z".to_string(),
        };
        assert_eq!(
            crate::daemon::clock_route::occurrence_at_index(
                &schedule,
                dt("2026-01-01T00:00:00Z"),
                0
            )
            .unwrap(),
            dt("2026-01-01T00:01:00Z")
        );
        assert!(
            crate::daemon::clock_route::first_occurrence_index_at_or_after(
                &schedule,
                dt("2026-01-01T00:00:00Z"),
                dt("2026-01-01T00:02:00Z"),
                None,
            )
            .unwrap()
            .is_none()
        );
        assert!(
            crate::daemon::clock_route::latest_occurrence_index_at_or_before(
                &schedule,
                dt("2026-01-01T00:00:00Z"),
                dt("2026-01-01T00:02:00Z"),
                Some(0),
            )
            .unwrap()
            .is_none()
        );
    }

    #[test]
    fn cron_schedule_crosses_dst_boundary_with_stable_indices() {
        let schedule = crate::kernel::control_decision::MandateSchedulePayload::Cron {
            expr: "0 30 1 * * *".to_string(),
            tz: "America/New_York".to_string(),
        };
        let start = dt("2026-10-31T04:00:00Z");
        let first = crate::daemon::clock_route::occurrence_at_index(&schedule, start, 0).unwrap();
        let second = crate::daemon::clock_route::occurrence_at_index(&schedule, start, 1).unwrap();
        let third = crate::daemon::clock_route::occurrence_at_index(&schedule, start, 2).unwrap();

        assert_eq!(first, dt("2026-10-31T05:30:00Z"));
        assert_eq!(second, dt("2026-11-01T05:30:00Z"));
        assert_eq!(third, dt("2026-11-02T06:30:00Z"));
        assert_eq!(
            crate::daemon::clock_route::latest_occurrence_index_at_or_before(
                &schedule,
                start,
                dt("2026-11-01T06:45:00Z"),
                None,
            )
            .unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn route_uses_injected_clock_when_building_tick_envelope() {
        let root = std::env::temp_dir()
            .join("verlet-clock-route-tests")
            .join(uuid::Uuid::now_v7().to_string());
        let store = verlet_history_sqlite::SqliteSessionStore::open(root.join("history.sqlite3"))
            .await
            .unwrap();
        let coordinates = coordinates();
        let mandate_event_id = verlet_history::EventRecordId::from_uuid(uuid::Uuid::from_u128(1));
        let mandate = verlet_history::NewEventRecord {
            id: mandate_event_id,
            coordinates: coordinates.clone(),
            created_at_ms: dt("2026-01-01T00:00:00Z").timestamp_millis(),
            kind: verlet_history::EventKind::MandateStarted,
            origin: verlet_history::EventOrigin::Witnessed,
            provenance: Default::default(),
            payload: serde_json::to_value(crate::kernel::control_decision::MandateStartedPayload {
                subject: crate::kernel::control_decision::MandateSubject {
                    thread_id: Some(coordinates.thread_id.to_string()),
                    loop_id: None,
                },
                mandate_id: "mandate-clock".to_string(),
                snapshot_id: "schedule.v1".to_string(),
                thread_id: Some(coordinates.thread_id.to_string()),
                max_continuations: None,
                expires_at_ms: None,
                schedule: Some(
                    crate::kernel::control_decision::MandateSchedulePayload::At {
                        when: "2026-01-01T00:01:00Z".to_string(),
                    },
                ),
                max_occurrences: None,
                catch_up: Some(
                    crate::kernel::control_decision::MandateCatchUpPolicy::CoalesceMissed,
                ),
                input_template: None,
            })
            .unwrap(),
        };
        store
            .append_events(
                &crate::kernel::control_decision::control_stream_id(&coordinates),
                vec![mandate],
            )
            .await
            .unwrap();
        let envelopes = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = std::sync::Arc::new(CaptureSink {
            envelopes: envelopes.clone(),
        });
        let clock = std::sync::Arc::new(FakeClock::new(dt("2026-01-01T00:02:00Z")));
        let route = crate::daemon::clock_route::VerletDaemonClockRoute::new(
            "clock-main",
            store,
            sink,
            clock,
        )
        .with_started_at(dt("2026-01-01T00:02:00Z"));

        assert_eq!(route.enqueue_due_once().await.unwrap(), 1);
        let captured = envelopes.lock().unwrap();
        assert_eq!(captured.len(), 1);
        assert_eq!(
            captured[0].source.protocol,
            crate::daemon::clock_route::CLOCK_TICK_ROUTE_KIND
        );
        let delivery_id = "00000000-0000-0000-0000-000000000001:0";
        assert_eq!(
            captured[0]
                .effective_dedupe_key()
                .as_ref()
                .map(verlet_io_core::IoDedupeKey::stable_key),
            Some("clock.tick:clock-main:00000000-0000-0000-0000-000000000001:0".to_string()),
            "the adapter-envelope migration must preserve the literal pre-contract key"
        );
        assert_eq!(
            captured[0]
                .delivery
                .as_ref()
                .map(|delivery| delivery.delivery_id.as_str()),
            Some(delivery_id)
        );
        assert_eq!(
            captured[0].principal,
            Some(verlet_io_core::IoPrincipal::new(
                coordinates.tenant_id.clone(),
                coordinates.user_id.clone(),
                format!("mandate:{mandate_event_id}"),
            ))
        );
        assert!(!captured[0].metadata.contains_key("cooldis_tenant_id"));
        assert!(!captured[0].metadata.contains_key("cooldis_user_id"));
        assert_eq!(
            captured[0]
                .metadata
                .get("verlet_catch_up")
                .map(String::as_str),
            Some("true")
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
