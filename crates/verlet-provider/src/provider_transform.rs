//! Replay-fidelity normalization of canonical history at the provider
//! boundary.
//!
//! Canonical history is provider-neutral and durable; wire requests are
//! per-target. When a thread's history carries content the target API
//! cannot represent — thinking from another provider, assistant images,
//! cache controls, tool calls whose results were never recorded, assistant
//! messages that ended in an error — replaying it verbatim either silently
//! loses content inside the wire builders or fails request validation and
//! bricks the thread. This module normalizes the compiled history for the
//! target deterministically, and counts every action so the caller can put
//! the accounting on the compiled-context receipt.
//!
//! The transform only selects and arranges; it never creates content.
//! Dangling tool calls are dropped, not completed with synthetic results:
//! a synthesized result would put created content into model-visible
//! context without having been discharged to the thread's stream first.
//! Foreign thinking text is re-presented verbatim inside `<thinking>` tags
//! so the model can tell quoted reasoning from assistant prose; thinking
//! with no visible text (redacted or encrypted-only) is dropped. Native
//! thinking — provenance matching the target api and provider — passes
//! untouched so the wire builders can replay it faithfully (e.g. OpenAI
//! encrypted reasoning items).
//!
//! The latest user message is exempt from content drops: current-turn
//! input the target cannot represent must fail closed at request
//! validation, not be silently eaten. Cache-control markers are metadata,
//! not content, and are stripped (and counted) everywhere the target lacks
//! cache support, including the latest user message.

/// Accounting for one [`normalize_history_for_target`] run. All counts are
/// zero when the history is already representable on the target; the
/// caller embeds the counts in the compiled-context receipt.
#[derive(Clone, Debug, Default, Eq, PartialEq, serde::Serialize)]
pub struct ReplayTransformCounts {
    pub thinking_converted: usize,
    pub thinking_dropped: usize,
    pub images_dropped: usize,
    pub cache_controls_stripped: usize,
    pub dangling_tool_calls_dropped: usize,
    pub unpaired_tool_results_dropped: usize,
    pub duplicate_tool_results_dropped: usize,
    pub errored_assistants_dropped: usize,
    pub empty_assistants_dropped: usize,
}

impl ReplayTransformCounts {
    pub fn is_noop(&self) -> bool {
        *self == Self::default()
    }

    pub fn add_assign(&mut self, other: Self) {
        self.thinking_converted += other.thinking_converted;
        self.thinking_dropped += other.thinking_dropped;
        self.images_dropped += other.images_dropped;
        self.cache_controls_stripped += other.cache_controls_stripped;
        self.dangling_tool_calls_dropped += other.dangling_tool_calls_dropped;
        self.unpaired_tool_results_dropped += other.unpaired_tool_results_dropped;
        self.duplicate_tool_results_dropped += other.duplicate_tool_results_dropped;
        self.errored_assistants_dropped += other.errored_assistants_dropped;
        self.empty_assistants_dropped += other.empty_assistants_dropped;
    }
}

/// The normalized history plus the accounting of what changed.
#[derive(Clone, Debug)]
pub struct ReplayTransform {
    pub messages: Vec<verlet_history::CanonicalMessage>,
    pub counts: ReplayTransformCounts,
}

/// Normalizes compiled canonical history for the target api/provider.
///
/// Rules, in order of application:
/// - assistants whose turn ended in `Error` or `Cancelled` are dropped,
///   together with the tool results answering their tool calls;
/// - tool calls with no recorded tool result are dropped; tool results
///   with no surviving issuing tool call are dropped, and a tool call keeps
///   only the first result answering it;
/// - thinking from a different api/provider converts to a `<thinking>`
///   text block when it has visible text and is dropped otherwise; native
///   thinking passes verbatim;
/// - assistant images are dropped (no wire builder represents them);
///   historical user images are dropped only when the target lacks image
///   support; the latest user message keeps its images so unsupported
///   current-turn input fails closed at validation;
/// - cache controls are stripped when the target lacks cache support;
/// - messages left without content are dropped.
pub fn normalize_history_for_target(
    messages: Vec<verlet_history::CanonicalMessage>,
    target_api: &verlet_history::ProviderApi,
    target_provider: &str,
) -> ReplayTransform {
    let capabilities = crate::ProviderCapabilityRecord::for_api(target_api.clone());
    let mut counts = ReplayTransformCounts::default();

    let mut errored = vec![false; messages.len()];
    let mut issuer_by_call_id: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut duplicate_call_ids: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut result_call_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    for (index, message) in messages.iter().enumerate() {
        match message {
            verlet_history::CanonicalMessage::Assistant {
                content,
                stop_reason,
                ..
            } => {
                if matches!(
                    stop_reason,
                    verlet_history::CanonicalStopReason::Error
                        | verlet_history::CanonicalStopReason::Cancelled
                ) {
                    errored[index] = true;
                }
                for block in content {
                    if let verlet_history::CanonicalContent::ToolCall { id, .. } = block
                        && issuer_by_call_id.insert(id.clone(), index).is_some()
                    {
                        duplicate_call_ids.insert(id.clone());
                    }
                }
            }
            verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. } => {
                result_call_ids.insert(tool_call_id.clone());
            }
            verlet_history::CanonicalMessage::User { .. } => {}
        }
    }
    let latest_user_index = match messages.last() {
        Some(verlet_history::CanonicalMessage::User { .. }) => Some(messages.len() - 1),
        _ => None,
    };

    // A tool call keeps at most one answering result: the wire shape pairs them
    // one-to-one, and a second result for an id already emitted is rejected by
    // the target the same way a duplicate tool call would be.
    let mut emitted_result_call_ids: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    let mut output = Vec::with_capacity(messages.len());
    for (index, message) in messages.into_iter().enumerate() {
        match message {
            verlet_history::CanonicalMessage::User {
                content,
                timestamp_ms,
            } => {
                let is_latest_user = Some(index) == latest_user_index;
                let mut kept = Vec::with_capacity(content.len());
                for block in content {
                    match block {
                        verlet_history::CanonicalContent::Image { .. }
                            if !capabilities.supports_images && !is_latest_user =>
                        {
                            counts.images_dropped += 1;
                        }
                        block => {
                            kept.push(strip_text_cache_control(block, &capabilities, &mut counts))
                        }
                    }
                }
                if kept.is_empty() && !is_latest_user {
                    continue;
                }
                output.push(verlet_history::CanonicalMessage::User {
                    content: kept,
                    timestamp_ms,
                });
            }
            verlet_history::CanonicalMessage::Assistant {
                content,
                api,
                provider,
                model,
                usage,
                stop_reason,
                error_message,
                timestamp_ms,
            } => {
                if errored[index] {
                    counts.errored_assistants_dropped += 1;
                    continue;
                }
                let native = api == *target_api && provider == target_provider;
                let mut kept = Vec::with_capacity(content.len());
                for block in content {
                    match block {
                        verlet_history::CanonicalContent::Thinking { .. } if native => {
                            kept.push(block)
                        }
                        verlet_history::CanonicalContent::Thinking { text, .. } => {
                            if text.trim().is_empty() {
                                counts.thinking_dropped += 1;
                            } else {
                                counts.thinking_converted += 1;
                                kept.push(verlet_history::CanonicalContent::Text {
                                    text: format!("<thinking>\n{text}\n</thinking>"),
                                    cache_control: None,
                                });
                            }
                        }
                        verlet_history::CanonicalContent::Image { .. } => {
                            counts.images_dropped += 1;
                        }
                        verlet_history::CanonicalContent::ToolCall {
                            id,
                            name,
                            arguments,
                        } => {
                            if !duplicate_call_ids.contains(&id) && result_call_ids.contains(&id) {
                                kept.push(verlet_history::CanonicalContent::ToolCall {
                                    id,
                                    name,
                                    arguments,
                                });
                            } else {
                                counts.dangling_tool_calls_dropped += 1;
                            }
                        }
                        block => {
                            kept.push(strip_text_cache_control(block, &capabilities, &mut counts))
                        }
                    }
                }
                if kept.is_empty() {
                    counts.empty_assistants_dropped += 1;
                    continue;
                }
                output.push(verlet_history::CanonicalMessage::Assistant {
                    content: kept,
                    api,
                    provider,
                    model,
                    usage,
                    stop_reason,
                    error_message,
                    timestamp_ms,
                });
            }
            verlet_history::CanonicalMessage::ToolResult {
                tool_call_id,
                tool_name,
                content,
                is_error,
                cache_control,
                timestamp_ms,
            } => {
                let issuer_alive = issuer_by_call_id
                    .get(&tool_call_id)
                    .filter(|_| !duplicate_call_ids.contains(&tool_call_id))
                    .is_some_and(|&issuer| !errored[issuer]);
                if !issuer_alive {
                    counts.unpaired_tool_results_dropped += 1;
                    continue;
                }
                if !emitted_result_call_ids.insert(tool_call_id.clone()) {
                    counts.duplicate_tool_results_dropped += 1;
                    continue;
                }
                let cache_control =
                    if cache_control.is_some() && !capabilities.supports_cache_control {
                        counts.cache_controls_stripped += 1;
                        None
                    } else {
                        cache_control
                    };
                let kept = content
                    .into_iter()
                    .map(|block| strip_text_cache_control(block, &capabilities, &mut counts))
                    .collect();
                output.push(verlet_history::CanonicalMessage::ToolResult {
                    tool_call_id,
                    tool_name,
                    content: kept,
                    is_error,
                    cache_control,
                    timestamp_ms,
                });
            }
        }
    }

    ReplayTransform {
        messages: output,
        counts,
    }
}

fn strip_text_cache_control(
    block: verlet_history::CanonicalContent,
    capabilities: &crate::ProviderCapabilityRecord,
    counts: &mut ReplayTransformCounts,
) -> verlet_history::CanonicalContent {
    match block {
        verlet_history::CanonicalContent::Text {
            text,
            cache_control: Some(_),
        } if !capabilities.supports_cache_control => {
            counts.cache_controls_stripped += 1;
            verlet_history::CanonicalContent::Text {
                text,
                cache_control: None,
            }
        }
        block => block,
    }
}

#[cfg(test)]
mod tests;
