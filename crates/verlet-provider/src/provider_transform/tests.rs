use crate::ProviderWireAdapter as _;

const ANTHROPIC: (&str, verlet_history::ProviderApi) =
    ("anthropic", verlet_history::ProviderApi::AnthropicMessages);
const OPENAI: (&str, verlet_history::ProviderApi) =
    ("openai", verlet_history::ProviderApi::OpenAIResponses);
const OPENAI_COMPATIBLE: (&str, verlet_history::ProviderApi) = (
    "openai_compatible",
    verlet_history::ProviderApi::OpenAIChatCompletions,
);

fn assistant(
    source: (&str, verlet_history::ProviderApi),
    content: Vec<verlet_history::CanonicalContent>,
    stop_reason: verlet_history::CanonicalStopReason,
) -> verlet_history::CanonicalMessage {
    verlet_history::CanonicalMessage::assistant(
        source.0,
        source.1,
        "test-model",
        content,
        stop_reason,
    )
}

fn anthropic_thinking(text: &str) -> verlet_history::CanonicalContent {
    verlet_history::CanonicalContent::Thinking {
        text: text.to_string(),
        provider: verlet_history::ThinkingProvider::Anthropic,
        metadata: verlet_history::ThinkingMetadata::Anthropic {
            signature: Some("sig".to_string()),
        },
    }
}

fn openai_thinking(text: &str) -> verlet_history::CanonicalContent {
    verlet_history::CanonicalContent::Thinking {
        text: text.to_string(),
        provider: verlet_history::ThinkingProvider::OpenAIResponses,
        metadata: verlet_history::ThinkingMetadata::OpenAIResponses {
            item_id: Some("rs_1".to_string()),
            output_index: Some(0),
            summary_index: 0,
            encrypted_content: Some("enc".to_string()),
        },
    }
}

fn image() -> verlet_history::CanonicalContent {
    verlet_history::CanonicalContent::Image {
        data: "aGk=".to_string(),
        mime_type: "image/png".to_string(),
    }
}

fn normalize(
    messages: Vec<verlet_history::CanonicalMessage>,
    target: (&str, verlet_history::ProviderApi),
) -> crate::provider_transform::ReplayTransform {
    crate::provider_transform::normalize_history_for_target(messages, &target.1, target.0)
}

#[test]
fn matching_provenance_history_is_a_zero_count_pass_through() {
    let messages = vec![
        verlet_history::CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![
                anthropic_thinking("hmm"),
                verlet_history::CanonicalContent::text("hello"),
                verlet_history::CanonicalContent::tool_call(
                    "toolu_1",
                    "lookup",
                    serde_json::json!({"q": 1}),
                ),
            ],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result("toolu_1", "lookup", "found", false),
        assistant(
            ANTHROPIC,
            vec![verlet_history::CanonicalContent::text("done")],
            verlet_history::CanonicalStopReason::EndTurn,
        ),
    ];
    let transformed = normalize(messages.clone(), ANTHROPIC);
    assert!(transformed.counts.is_noop(), "{:?}", transformed.counts);
    assert_eq!(transformed.messages, messages);
}

#[test]
fn foreign_thinking_converts_to_tagged_text_and_native_thinking_passes() {
    let messages = vec![
        verlet_history::CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![anthropic_thinking("foreign reasoning")],
            verlet_history::CanonicalStopReason::EndTurn,
        ),
        assistant(
            OPENAI,
            vec![openai_thinking("native reasoning")],
            verlet_history::CanonicalStopReason::EndTurn,
        ),
        verlet_history::CanonicalMessage::user_text("again"),
    ];
    let transformed = normalize(messages, OPENAI);
    assert_eq!(transformed.counts.thinking_converted, 1);
    assert_eq!(transformed.counts.thinking_dropped, 0);
    let verlet_history::CanonicalMessage::Assistant { content, .. } = &transformed.messages[1]
    else {
        panic!("expected assistant");
    };
    assert_eq!(
        content[0],
        verlet_history::CanonicalContent::Text {
            text: "<thinking>\nforeign reasoning\n</thinking>".to_string(),
            cache_control: None,
        }
    );
    let verlet_history::CanonicalMessage::Assistant { content, .. } = &transformed.messages[2]
    else {
        panic!("expected assistant");
    };
    assert_eq!(content[0], openai_thinking("native reasoning"));
}

#[test]
fn foreign_thinking_without_visible_text_is_dropped() {
    let messages = vec![
        verlet_history::CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![
                verlet_history::CanonicalContent::Thinking {
                    text: String::new(),
                    provider: verlet_history::ThinkingProvider::Anthropic,
                    metadata: verlet_history::ThinkingMetadata::AnthropicRedacted {
                        data: "opaque".to_string(),
                    },
                },
                verlet_history::CanonicalContent::text("visible answer"),
            ],
            verlet_history::CanonicalStopReason::EndTurn,
        ),
    ];
    let transformed = normalize(messages, OPENAI);
    assert_eq!(transformed.counts.thinking_dropped, 1);
    assert_eq!(transformed.counts.thinking_converted, 0);
}

#[test]
fn errored_assistants_drop_with_their_tool_results() {
    let messages = vec![
        verlet_history::CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![verlet_history::CanonicalContent::tool_call(
                "toolu_err",
                "lookup",
                serde_json::json!({}),
            )],
            verlet_history::CanonicalStopReason::Error,
        ),
        verlet_history::CanonicalMessage::tool_result("toolu_err", "lookup", "late result", false),
        verlet_history::CanonicalMessage::user_text("retry"),
    ];
    let transformed = normalize(messages, ANTHROPIC);
    assert_eq!(transformed.counts.errored_assistants_dropped, 1);
    assert_eq!(transformed.counts.unpaired_tool_results_dropped, 1);
    assert_eq!(transformed.messages.len(), 2);
}

#[test]
fn dangling_tool_calls_and_unpaired_tool_results_are_dropped() {
    let messages = vec![
        verlet_history::CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![
                verlet_history::CanonicalContent::tool_call(
                    "toolu_paired",
                    "lookup",
                    serde_json::json!({}),
                ),
                verlet_history::CanonicalContent::tool_call(
                    "toolu_dangling",
                    "lookup",
                    serde_json::json!({}),
                ),
            ],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result("toolu_paired", "lookup", "ok", false),
        verlet_history::CanonicalMessage::tool_result("toolu_unknown", "lookup", "orphan", false),
    ];
    let transformed = normalize(messages, ANTHROPIC);
    assert_eq!(transformed.counts.dangling_tool_calls_dropped, 1);
    assert_eq!(transformed.counts.unpaired_tool_results_dropped, 1);
    let verlet_history::CanonicalMessage::Assistant { content, .. } = &transformed.messages[1]
    else {
        panic!("expected assistant");
    };
    assert_eq!(content.len(), 1);
    assert!(matches!(
        &content[0],
        verlet_history::CanonicalContent::ToolCall { id, .. } if id == "toolu_paired"
    ));
    assert_eq!(transformed.messages.len(), 3);

    let messages = vec![
        verlet_history::CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![
                verlet_history::CanonicalContent::tool_call(
                    "toolu_dup",
                    "lookup",
                    serde_json::json!({"q": 1}),
                ),
                verlet_history::CanonicalContent::tool_call(
                    "toolu_dup",
                    "lookup",
                    serde_json::json!({"q": 2}),
                ),
            ],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result("toolu_dup", "lookup", "ambiguous", false),
        verlet_history::CanonicalMessage::user_text("continue"),
    ];
    let transformed = normalize(messages, ANTHROPIC);
    assert_eq!(transformed.counts.dangling_tool_calls_dropped, 2);
    assert_eq!(transformed.counts.unpaired_tool_results_dropped, 1);
    assert_eq!(transformed.counts.empty_assistants_dropped, 1);
    assert_eq!(
        transformed
            .messages
            .iter()
            .filter(|message| matches!(message, verlet_history::CanonicalMessage::User { .. }))
            .count(),
        2
    );
}

#[test]
fn assistants_left_empty_by_drops_are_removed() {
    let messages = vec![
        verlet_history::CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![verlet_history::CanonicalContent::tool_call(
                "toolu_dangling",
                "lookup",
                serde_json::json!({}),
            )],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::user_text("again"),
    ];
    let transformed = normalize(messages, ANTHROPIC);
    assert_eq!(transformed.counts.dangling_tool_calls_dropped, 1);
    assert_eq!(transformed.counts.empty_assistants_dropped, 1);
    assert_eq!(transformed.messages.len(), 2);
}

#[test]
fn historical_user_images_drop_for_non_image_targets_but_latest_user_keeps_them() {
    let messages = vec![
        verlet_history::CanonicalMessage::User {
            content: vec![
                verlet_history::CanonicalContent::text("look at this"),
                image(),
            ],
            timestamp_ms: 1,
        },
        assistant(
            OPENAI_COMPATIBLE,
            vec![verlet_history::CanonicalContent::text("seen")],
            verlet_history::CanonicalStopReason::EndTurn,
        ),
        verlet_history::CanonicalMessage::User {
            content: vec![verlet_history::CanonicalContent::text("and this"), image()],
            timestamp_ms: 2,
        },
    ];
    let transformed = normalize(messages, OPENAI_COMPATIBLE);
    assert_eq!(transformed.counts.images_dropped, 1);
    let verlet_history::CanonicalMessage::User { content, .. } = &transformed.messages[0] else {
        panic!("expected user");
    };
    assert_eq!(content.len(), 1);
    let verlet_history::CanonicalMessage::User { content, .. } = &transformed.messages[2] else {
        panic!("expected user");
    };
    assert_eq!(content.len(), 2, "latest user message keeps its image");

    let messages = vec![
        verlet_history::CanonicalMessage::User {
            content: vec![
                verlet_history::CanonicalContent::text("look at this"),
                image(),
            ],
            timestamp_ms: 1,
        },
        assistant(
            OPENAI_COMPATIBLE,
            vec![verlet_history::CanonicalContent::tool_call(
                "call_1",
                "lookup",
                serde_json::json!({}),
            )],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result("call_1", "lookup", "found", false),
    ];
    let transformed = normalize(messages, OPENAI_COMPATIBLE);
    assert_eq!(transformed.counts.images_dropped, 1);
    assert_eq!(transformed.messages.len(), 3);
    let verlet_history::CanonicalMessage::User { content, .. } = &transformed.messages[0] else {
        panic!("expected user");
    };
    assert_eq!(
        content,
        &[verlet_history::CanonicalContent::text("look at this")]
    );
}

#[test]
fn cache_controls_strip_for_non_cache_targets_and_survive_for_anthropic() {
    let messages = vec![
        verlet_history::CanonicalMessage::User {
            content: vec![verlet_history::CanonicalContent::cached_text(
                "cached prompt",
                verlet_history::CacheControl::ephemeral(),
            )],
            timestamp_ms: 1,
        },
        assistant(
            ANTHROPIC,
            vec![verlet_history::CanonicalContent::tool_call(
                "toolu_1",
                "lookup",
                serde_json::json!({}),
            )],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::ToolResult {
            tool_call_id: "toolu_1".to_string(),
            tool_name: "lookup".to_string(),
            content: vec![verlet_history::CanonicalContent::text("ok")],
            is_error: false,
            cache_control: Some(verlet_history::CacheControl::ephemeral()),
            timestamp_ms: 2,
        },
        verlet_history::CanonicalMessage::user_text("next"),
    ];
    let kept = normalize(messages.clone(), ANTHROPIC);
    assert!(kept.counts.is_noop());
    let stripped = normalize(messages, OPENAI);
    assert_eq!(stripped.counts.cache_controls_stripped, 2);
}

#[test]
fn normalized_anthropic_history_passes_openai_validation_and_builds_wire_bodies() {
    let messages = vec![
        verlet_history::CanonicalMessage::User {
            content: vec![
                verlet_history::CanonicalContent::cached_text(
                    "cached",
                    verlet_history::CacheControl::ephemeral(),
                ),
                image(),
            ],
            timestamp_ms: 1,
        },
        assistant(
            ANTHROPIC,
            vec![
                anthropic_thinking("let me look"),
                verlet_history::CanonicalContent::tool_call(
                    "toolu_1",
                    "lookup",
                    serde_json::json!({"q": 1}),
                ),
            ],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::ToolResult {
            tool_call_id: "toolu_1".to_string(),
            tool_name: "lookup".to_string(),
            content: vec![verlet_history::CanonicalContent::text("found")],
            is_error: false,
            cache_control: Some(verlet_history::CacheControl::ephemeral()),
            timestamp_ms: 2,
        },
        assistant(
            ANTHROPIC,
            vec![verlet_history::CanonicalContent::text("answer")],
            verlet_history::CanonicalStopReason::EndTurn,
        ),
        verlet_history::CanonicalMessage::user_text("continue"),
    ];

    for (target, body_must_validate) in [(OPENAI, true), (OPENAI_COMPATIBLE, true)] {
        let transformed = normalize(messages.clone(), target.clone());
        let request = crate::ProviderRequest {
            api: target.1.clone(),
            provider: target.0.to_string(),
            model: "test-model".to_string(),
            system: Vec::new(),
            messages: transformed.messages,
            tools: Vec::new(),
            max_tokens: 256,
            temperature: None,
            thinking: None,
        };
        let body = match target.1 {
            verlet_history::ProviderApi::OpenAIResponses => {
                crate::OpenAIResponsesAdapter::default().build_request_body(&request)
            }
            verlet_history::ProviderApi::OpenAIChatCompletions => {
                crate::OpenAIChatCompletionsAdapter.build_request_body(&request)
            }
            _ => unreachable!(),
        };
        assert_eq!(body.is_ok(), body_must_validate, "{target:?}: {body:?}");
    }
}

#[test]
fn unnormalized_anthropic_history_fails_openai_validation() {
    let request = crate::ProviderRequest {
        api: verlet_history::ProviderApi::OpenAIResponses,
        provider: "openai".to_string(),
        model: "test-model".to_string(),
        system: Vec::new(),
        messages: vec![verlet_history::CanonicalMessage::User {
            content: vec![verlet_history::CanonicalContent::cached_text(
                "cached",
                verlet_history::CacheControl::ephemeral(),
            )],
            timestamp_ms: 1,
        }],
        tools: Vec::new(),
        max_tokens: 256,
        temperature: None,
        thinking: None,
    };
    assert!(
        crate::OpenAIResponsesAdapter::default()
            .build_request_body(&request)
            .is_err()
    );
}

#[test]
fn usage_and_provenance_survive_the_rebuild() {
    let usage = verlet_history::CanonicalUsage {
        input_tokens: 7,
        ..verlet_history::CanonicalUsage::default()
    };
    let messages = vec![
        verlet_history::CanonicalMessage::user_text("hi"),
        verlet_history::CanonicalMessage::assistant_with_usage(
            "anthropic",
            verlet_history::ProviderApi::AnthropicMessages,
            "test-model",
            vec![
                anthropic_thinking("t"),
                verlet_history::CanonicalContent::text("a"),
            ],
            usage.clone(),
            verlet_history::CanonicalStopReason::EndTurn,
        ),
    ];
    let transformed = normalize(messages, OPENAI);
    let verlet_history::CanonicalMessage::Assistant {
        usage: kept_usage,
        provider,
        api,
        ..
    } = &transformed.messages[1]
    else {
        panic!("expected assistant");
    };
    assert_eq!(kept_usage, &usage);
    assert_eq!(provider, "anthropic");
    assert_eq!(api, &verlet_history::ProviderApi::AnthropicMessages);
}

#[test]
fn duplicate_tool_results_for_one_call_are_collapsed_to_one() {
    // The transform exists to hand the wire builders a shape the target can
    // represent. Anthropic rejects two tool_result blocks carrying the same
    // tool_use_id, and duplicate tool *calls* are already deduped via
    // duplicate_call_ids, so the mirror case has to hold too.
    let messages = vec![
        verlet_history::CanonicalMessage::user_text("hi"),
        assistant(
            ANTHROPIC,
            vec![verlet_history::CanonicalContent::tool_call(
                "toolu_dup",
                "lookup",
                serde_json::json!({}),
            )],
            verlet_history::CanonicalStopReason::ToolUse,
        ),
        verlet_history::CanonicalMessage::tool_result("toolu_dup", "lookup", "first", false),
        verlet_history::CanonicalMessage::tool_result("toolu_dup", "lookup", "second", false),
    ];

    let transformed = normalize(messages, ANTHROPIC);

    let results = transformed
        .messages
        .iter()
        .filter(|message| {
            matches!(
                message,
                verlet_history::CanonicalMessage::ToolResult { tool_call_id, .. }
                    if tool_call_id == "toolu_dup"
            )
        })
        .count();
    assert_eq!(
        results, 1,
        "exactly one tool result may survive per tool call, got {results}"
    );
    assert_eq!(transformed.counts.duplicate_tool_results_dropped, 1);
}
