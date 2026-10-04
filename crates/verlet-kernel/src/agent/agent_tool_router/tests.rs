#[tokio::test]
async fn router_projects_registry_operations_as_tool_definitions() {
    let router = router_with_echo_operation("echo").await;

    let definitions = router.tool_definitions().await;

    assert_eq!(definitions.len(), 1);
    assert_eq!(definitions[0].name, "echo_search");
    assert_eq!(definitions[0].input_schema["required"][0], "input");
}

#[tokio::test]
async fn router_projects_kernel_tool_definitions_with_registry_operations() {
    let router = router_with_echo_operation("echo")
        .await
        .with_kernel_tool_provider(std::sync::Arc::new(FakeKernelToolProvider));

    let definitions = router.tool_definitions().await;
    let names = definitions
        .iter()
        .map(|definition| definition.name.as_str())
        .collect::<Vec<_>>();

    assert_eq!(names, vec!["echo_search", "record_context"]);
}

#[tokio::test]
async fn router_invokes_registry_operation_and_returns_tool_result() {
    let router = router_with_echo_operation("echo").await;

    let result = router
        .invoke_tool_call(
            "call_1",
            "echo_search",
            serde_json::json!({
                "input": "verlet"
            }),
        )
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if content == vec![verlet_history::CanonicalContent::text("echo:verlet")]
    ));
}

#[tokio::test]
async fn routers_with_shared_registry_isolate_concurrent_kernel_dispatch_overlays() {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    registry
        .register_kernel(
            verlet_operations::operation_registry::KernelOperationRegistration::new(
                "thread-identity",
                verlet_abi::WasmOperationManifest {
                    abi: verlet_wasm::runner::OPERATION_ABI.to_string(),
                    operations: vec![verlet_abi::WasmOperationDefinition {
                        id: 1,
                        name: "identify-thread".to_string(),
                        input: verlet_abi::WasmOperationValueKind::Bytes,
                        output: verlet_abi::WasmOperationValueKind::Bytes,
                        events: verlet_abi::WasmOperationEventKind::None,
                        mode: verlet_abi::WasmOperationMode::Sync,
                        required_capabilities: Vec::new(),
                    }],
                },
            ),
        )
        .await
        .unwrap();

    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(2));
    let router_a = kernel_identity_router(
        std::sync::Arc::clone(&registry),
        "thread-a",
        std::sync::Arc::clone(&barrier),
    );
    let router_b = kernel_identity_router(registry, "thread-b", barrier);

    let (thread_a, thread_b) = tokio::join!(
        router_a.invoke_tool_call(
            "call-a",
            "identify-thread",
            serde_json::json!({"input": ""}),
        ),
        router_b.invoke_tool_call(
            "call-b",
            "identify-thread",
            serde_json::json!({"input": ""}),
        ),
    );

    assert!(matches!(
        thread_a,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if tool_result_text(&content) == "thread-a"
    ));
    assert!(matches!(
        thread_b,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if tool_result_text(&content) == "thread-b"
    ));
}

#[tokio::test]
async fn router_invokes_kernel_tool_provider() {
    let router = crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
        verlet_operations::operation_registry::OperationRegistry::new(),
    ))
    .with_kernel_tool_provider(std::sync::Arc::new(FakeKernelToolProvider));

    let result = router
        .invoke_tool_call("call_1", "record_context", serde_json::json!({}))
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if tool_result_text(&content) == r#"{"status":"idle"}"#
    ));
}

#[tokio::test]
async fn router_outcome_adapter_wraps_synchronous_kernel_tools_as_completed() {
    let router = crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
        verlet_operations::operation_registry::OperationRegistry::new(),
    ))
    .with_kernel_tool_provider(std::sync::Arc::new(FakeKernelToolProvider));

    let result = router
        .invoke_tool_call_outcome("call_1", "record_context", serde_json::json!({}))
        .await
        .unwrap();

    assert!(matches!(
        result,
        crate::agent::agent_tool_router::AgentKernelToolOutcome::Completed(Some(verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        })) if tool_result_text(&content) == r#"{"status":"idle"}"#
    ));
}

#[tokio::test]
async fn router_passes_turn_context_to_kernel_tool_provider() {
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::<
        Option<crate::kernel::runtime_host::turn::TurnContextSnapshot>,
    >::new()));
    let router = crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
        verlet_operations::operation_registry::OperationRegistry::new(),
    ))
    .with_kernel_tool_provider(std::sync::Arc::new(RecordingKernelToolProvider {
        seen: std::sync::Arc::clone(&seen),
    }));
    let input = crate::kernel::runtime_host::turn::TurnInput::text("hello")
        .with_cwd("/tmp/verlet-turn")
        .with_model("gpt-test")
        .with_provider("openai")
        .with_permission_profile("workspace-write")
        .with_metadata("source", "test");
    let coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant_a", "user_1", "session_1");
    let turn_context = crate::kernel::runtime_host::turn::TurnContext::new(
        verlet_runtime_contracts::ThreadContext::root(coordinates.clone()),
        "turn-1",
        &input,
        tokio_util::sync::CancellationToken::new(),
    );

    let result = router
        .invoke_tool_call_for_turn(
            &turn_context,
            "call_1",
            "record_context",
            serde_json::json!({}),
        )
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            ..
        }
    ));
    let snapshots = seen.lock().unwrap();
    let snapshot = snapshots[0].as_ref().expect("turn context snapshot");
    assert_eq!(snapshot.turn_id, "turn-1");
    assert_eq!(snapshot.coordinates, coordinates);
    assert_eq!(snapshot.model.as_deref(), Some("gpt-test"));
    assert_eq!(snapshot.provider.as_deref(), Some("openai"));
    assert_eq!(
        snapshot.permission_profile.as_deref(),
        Some("workspace-write")
    );
    assert_eq!(
        snapshot.metadata.get("source").map(String::as_str),
        Some("test")
    );
    assert!(!snapshot.cancellation_requested);
}

#[tokio::test]
async fn router_returns_error_tool_result_for_bad_input_shape() {
    let router = router_with_echo_operation("echo").await;

    let result = router
        .invoke_tool_call(
            "call_1",
            "echo_search",
            serde_json::json!({"other": "verlet"}),
        )
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: true,
            content,
            ..
        } if tool_result_text(&content).contains("requires a string input field")
    ));
}

#[tokio::test]
async fn router_rejects_non_object_json_tool_arguments() {
    let router = router_with_operation("json-echo", "echo", "json", Vec::new()).await;

    let result = router
        .invoke_tool_call(
            "call_1",
            "json_echo_search",
            serde_json::json!("not an object"),
        )
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: true,
            content,
            ..
        } if tool_result_text(&content).contains("requires object arguments")
    ));
}

#[test]
fn surface_envelope_mounts_model_arguments_and_bound_values() {
    let surface = write_tool_surface();

    let envelope = crate::agent::agent_tool_router::assemble_surface_envelope(
        "call_1",
        "write",
        &surface,
        serde_json::json!({"path": "notes.txt", "content": "hello"}),
    )
    .unwrap();

    assert_eq!(
        envelope,
        serde_json::json!({
            "root": "/workspace",
            "args": {"path": "notes.txt", "content": "hello"}
        })
    );
}

#[test]
fn surface_envelope_validates_the_assembled_host_input() {
    let mut surface = write_tool_surface();
    surface
        .bound_values
        .insert("root".to_string(), serde_json::json!(42));

    let error = crate::agent::agent_tool_router::assemble_surface_envelope(
        "call_1",
        "write",
        &surface,
        serde_json::json!({"path": "notes.txt", "content": "hello"}),
    )
    .unwrap_err()
    .to_string();

    assert!(error.contains("assembled envelope"), "{error}");
    assert!(error.contains("root"), "{error}");
}

#[tokio::test]
async fn surface_router_projects_derived_schema_and_invokes_with_host_envelope() {
    let router = router_with_operation("surface", "echo", "json", Vec::new())
        .await
        .with_tool_aliases([crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: "write".to_string(),
            registered_name: "surface".to_string(),
            operation_name: "search".to_string(),
            attach_event_id: None,
            surface: Some(write_tool_surface()),
        }]);

    let definitions = router.tool_definitions().await;
    assert_eq!(definitions[0].name, "write");
    assert_eq!(definitions[0].input_schema, write_model_input_schema());

    let result = router
        .invoke_tool_call(
            "call_1",
            "write",
            serde_json::json!({"path": "notes.txt", "content": "hello"}),
        )
        .await;
    let verlet_history::CanonicalMessage::ToolResult {
        is_error: false,
        content,
        ..
    } = result
    else {
        panic!("expected successful surface tool result");
    };
    let envelope = tool_result_text(&content)
        .strip_prefix("echo:")
        .map(|value| serde_json::from_str::<serde_json::Value>(value).unwrap())
        .unwrap();
    assert_eq!(
        envelope,
        serde_json::json!({
            "root": "/workspace",
            "args": {"path": "notes.txt", "content": "hello"}
        })
    );
}

#[tokio::test]
async fn surface_router_returns_tool_error_text_for_bound_parameter_collision() {
    let router = router_with_operation("surface", "echo", "json", Vec::new())
        .await
        .with_tool_aliases([crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: "write".to_string(),
            registered_name: "surface".to_string(),
            operation_name: "search".to_string(),
            attach_event_id: None,
            surface: Some(write_tool_surface()),
        }]);

    let result = router
        .invoke_tool_call(
            "call_1",
            "write",
            serde_json::json!({
                "root": "/model-owned",
                "path": "notes.txt",
                "content": "hello"
            }),
        )
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: true,
            content,
            ..
        } if {
            let text = tool_result_text(&content);
            text.contains("bound parameter") && text.contains("root")
        }
    ));
}

#[tokio::test]
async fn surface_router_returns_schema_error_text_for_model_arguments() {
    let router = router_with_operation("surface", "echo", "json", Vec::new())
        .await
        .with_tool_aliases([crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: "write".to_string(),
            registered_name: "surface".to_string(),
            operation_name: "search".to_string(),
            attach_event_id: None,
            surface: Some(write_tool_surface()),
        }]);

    let result = router
        .invoke_tool_call("call_1", "write", serde_json::json!({"path": "notes.txt"}))
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: true,
            content,
            ..
        } if {
            let text = tool_result_text(&content);
            text.contains("failed schema validation") && text.contains("content")
        }
    ));
}

#[tokio::test]
async fn router_invokes_attached_operation_without_manifest_capability_check() {
    let router = router_with_operation(
        "secret-echo",
        "echo",
        "bytes",
        vec!["secret:EXAMPLE_API_KEY"],
    )
    .await;

    let result = router
        .invoke_tool_call(
            "call_1",
            "secret_echo_search",
            serde_json::json!({"input": "verlet"}),
        )
        .await;

    assert!(matches!(
        result,
        verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } if tool_result_text(&content) == "echo:verlet"
    ));
}

#[tokio::test]
async fn router_invokes_kernel_process_operation_alias() {
    let cwd = temp_cwd("router");
    let router = router_with_kernel_process_operation(cwd.clone()).await;

    let result = router
        .invoke_tool_call(
            "call_1",
            crate::operations::kernel_packages::PROCESS_EXEC_OPERATION,
            serde_json::json!({
                "command": ["pwd"],
                "yield_time_ms": 1_000,
                "timeout_ms": 2_000
            }),
        )
        .await;

    let verlet_history::CanonicalMessage::ToolResult {
        is_error: false,
        content,
        ..
    } = result
    else {
        panic!("expected successful tool result");
    };
    let receipt = serde_json::from_str::<serde_json::Value>(&tool_result_text(&content)).unwrap();
    assert_eq!(receipt["operation"], "cooldis.process_exec");
    assert_eq!(receipt["status"], "completed");
    assert_eq!(receipt["backend"], "host_bash");
    assert_eq!(
        receipt["stdout"].as_str().unwrap().trim(),
        cwd.display().to_string()
    );
    assert!(receipt["process_id"].as_str().is_some());
    assert_eq!(receipt["dispatch_id"], "call_1");
}

#[tokio::test]
async fn router_invokes_kernel_notify_operation_alias_without_delivery_claim() {
    let router = router_with_kernel_notify_operation().await;

    let result = router
        .invoke_tool_call(
            "call_1",
            crate::operations::kernel_packages::CHANNEL_EMIT_OPERATION,
            serde_json::json!({
                "channel": "slack",
                "message": "Ready for review",
                "thread_id": "thread-1"
            }),
        )
        .await;

    let verlet_history::CanonicalMessage::ToolResult {
        is_error: false,
        content,
        ..
    } = result
    else {
        panic!("expected successful tool result");
    };
    let receipt = serde_json::from_str::<serde_json::Value>(&tool_result_text(&content)).unwrap();
    assert_eq!(receipt["operation"], "cooldis.channel_emit");
    assert_eq!(receipt["status"], "recorded");
    assert_eq!(receipt["delivery"], "not_sent");
    assert_eq!(receipt["channel_decision_required"], true);
    assert_eq!(receipt["channel"], "slack");
    assert_eq!(receipt["message"], "Ready for review");
}

#[tokio::test]
async fn router_addresses_child_threads_by_task_name_without_exposing_raw_ids() {
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
    ));
    let root = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let router = router_with_kernel_thread_operations(&host, root.context().clone()).await;

    let spawn = router
        .invoke_tool_call(
            "spawn-call-1",
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
            serde_json::json!({"task_name": "worker", "message": "echo first"}),
        )
        .await;
    let retry = router
        .invoke_tool_call(
            "spawn-call-1",
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
            serde_json::json!({"task_name": "worker", "message": "echo retry"}),
        )
        .await;
    let duplicate = router
        .invoke_tool_call(
            "spawn-call-2",
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
            serde_json::json!({"task_name": "worker", "message": "echo duplicate"}),
        )
        .await;
    let submit = router
        .invoke_tool_call(
            "submit-call-1",
            crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION,
            serde_json::json!({"task_name": "worker", "message": "echo steered"}),
        )
        .await;
    let status = router
        .invoke_tool_call(
            "status-call-1",
            crate::operations::kernel_packages::THREAD_STATUS_OPERATION,
            serde_json::json!({"task_name": "worker"}),
        )
        .await;
    let wait = router
        .invoke_tool_call(
            "wait-call-1",
            crate::operations::kernel_packages::THREAD_WAIT_OPERATION,
            serde_json::json!({"task_name": "worker", "timeout_ms": 1_000}),
        )
        .await;

    let children = host.children_of(root.context().coordinates.thread_id).await;
    assert_eq!(children.len(), 1);
    let child_id = children[0].context().coordinates.thread_id.to_string();
    let parent_id = root.context().coordinates.thread_id.to_string();

    for (message, operation) in [
        (&spawn, "cooldis.thread_spawn"),
        (&retry, "cooldis.thread_spawn"),
        (&submit, "cooldis.thread_submit"),
        (&wait, "cooldis.thread_wait"),
        (&status, "cooldis.thread_status"),
    ] {
        let verlet_history::CanonicalMessage::ToolResult {
            is_error: false,
            content,
            ..
        } = message
        else {
            panic!("expected successful alias-only tool result: {message:?}");
        };
        let text = tool_result_text(content);
        assert!(!text.contains(&child_id), "child id leaked in {text}");
        assert!(!text.contains(&parent_id), "parent id leaked in {text}");
        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
        assert_eq!(value["operation"], operation);
        assert_eq!(value["task_name"], "worker");
        assert!(value["status"].is_string());
        assert_eq!(value.as_object().unwrap().len(), 3);
    }

    let verlet_history::CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = duplicate
    else {
        panic!("expected duplicate task_name tool error");
    };
    let duplicate_error = tool_result_text(&content);
    assert!(duplicate_error.contains("task_name \"worker\" is already bound"));
    assert!(!duplicate_error.contains(&child_id));
    assert!(!duplicate_error.contains(&parent_id));

    let missing = router
        .invoke_tool_call(
            "status-missing",
            crate::operations::kernel_packages::THREAD_STATUS_OPERATION,
            serde_json::json!({"task_name": "missing"}),
        )
        .await;
    let verlet_history::CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = missing
    else {
        panic!("expected missing task_name tool error");
    };
    let missing_error = tool_result_text(&content);
    assert!(missing_error.contains("task_name \"missing\" was not found"));
    assert!(!missing_error.contains(&child_id));
    assert!(!missing_error.contains(&parent_id));

    let missing_wait = router
        .invoke_tool_call(
            "wait-missing",
            crate::operations::kernel_packages::THREAD_WAIT_OPERATION,
            serde_json::json!({"task_name": "missing", "timeout_ms": 1}),
        )
        .await;
    let verlet_history::CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = missing_wait
    else {
        panic!("expected missing wait task_name tool error");
    };
    let missing_wait_error = tool_result_text(&content);
    assert!(missing_wait_error.contains("task_name \"missing\" was not found"));
    assert!(!missing_wait_error.contains(&child_id));
    assert!(!missing_wait_error.contains(&parent_id));

    let rejected_raw_wait = router
        .invoke_tool_call(
            "wait-raw-id",
            crate::operations::kernel_packages::THREAD_WAIT_OPERATION,
            serde_json::json!({"target_thread_id": child_id, "timeout_ms": 1}),
        )
        .await;
    let verlet_history::CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = rejected_raw_wait
    else {
        panic!("expected raw-id wait input to fail decode");
    };
    let rejected_raw_wait_error = tool_result_text(&content);
    assert!(rejected_raw_wait_error.contains("unknown field `target_thread_id`"));
    assert!(!rejected_raw_wait_error.contains(&child_id));
    assert!(!rejected_raw_wait_error.contains(&parent_id));

    let cancel = router
        .invoke_tool_call(
            "cancel-call-1",
            crate::operations::kernel_packages::THREAD_CANCEL_OPERATION,
            serde_json::json!({"task_name": "worker"}),
        )
        .await;
    let verlet_history::CanonicalMessage::ToolResult {
        is_error: false,
        content,
        ..
    } = cancel
    else {
        panic!("expected successful task_name cancellation");
    };
    let cancel_text = tool_result_text(&content);
    assert!(!cancel_text.contains(&child_id));
    assert!(!cancel_text.contains(&parent_id));
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&cancel_text).unwrap(),
        serde_json::json!({
            "operation": "cooldis.thread_cancel",
            "status": "stopped",
            "task_name": "worker"
        })
    );

    let unavailable = router
        .invoke_tool_call(
            "status-call-2",
            crate::operations::kernel_packages::THREAD_STATUS_OPERATION,
            serde_json::json!({"task_name": "worker"}),
        )
        .await;
    let verlet_history::CanonicalMessage::ToolResult {
        is_error: true,
        content,
        ..
    } = unavailable
    else {
        panic!("expected unavailable task_name tool error");
    };
    let unavailable_error = tool_result_text(&content);
    assert!(
        unavailable_error.ends_with("thread_status task_name \"worker\" target is not available")
    );
    assert!(!unavailable_error.contains(&child_id));
    assert!(!unavailable_error.contains(&parent_id));

    host.shutdown_all().await.unwrap();
}

async fn router_with_echo_operation(
    name: &str,
) -> crate::agent::agent_tool_router::AgentToolRouter {
    router_with_operation(name, "echo", "bytes", Vec::new()).await
}

async fn router_with_operation(
    name: &str,
    prefix: &str,
    input: &str,
    required_capabilities: Vec<&str>,
) -> crate::agent::agent_tool_router::AgentToolRouter {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let wasm = wat::parse_str(echo_operation_guest(prefix, input, &required_capabilities))
        .expect("echo operation fixture should compile");
    let mut registration = verlet_operations::operation_registry::OperationRegistration::new(
        name,
        verlet_wasm::WasmRuntimeArtifact::bytes(wasm),
    );
    for capability in required_capabilities {
        registration = registration.with_capability_grant(capability);
    }
    registry.register(registration).await.unwrap();
    crate::agent::agent_tool_router::AgentToolRouter::new(registry)
}

async fn router_with_kernel_process_operation(
    cwd: std::path::PathBuf,
) -> crate::agent::agent_tool_router::AgentToolRouter {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let package = crate::operations::kernel_packages::verlet_process_kernel_package();
    let context = verlet_runtime_contracts::ThreadContext::root(
        verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
    );
    let store: std::sync::Arc<dyn verlet_history::RuntimeStore> =
        std::sync::Arc::new(verlet_history::InMemorySessionStore::new());
    let process_dispatcher = crate::kernel::process_handle_dispatch::test_process_dispatcher(
        store,
        context.coordinates.clone(),
    );
    let dispatcher: std::sync::Arc<
        dyn verlet_operations::operation_registry::KernelOperationDispatcher,
    > = std::sync::Arc::new(
        crate::agent::agent_process::KernelProcessOperationProvider::new(context, cwd)
            .with_process_dispatcher(process_dispatcher),
    );
    let mut registration = verlet_operations::operation_registry::KernelOperationRegistration::new(
        crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE,
        package.manifest.clone(),
    )
    .with_capability_grants(package.capability_grants.clone())
    .with_dispatcher(dispatcher);
    registration.metadata.insert(
        crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND.to_string(),
        serde_json::json!(crate::operations::kernel_packages::KERNEL_RUNTIME_KIND),
    );
    registry.register_kernel(registration).await.unwrap();
    crate::agent::agent_tool_router::AgentToolRouter::new(registry).with_tool_aliases(vec![
        crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: crate::operations::kernel_packages::PROCESS_EXEC_OPERATION.to_string(),
            registered_name: crate::operations::kernel_packages::VERLET_PROCESS_PACKAGE.to_string(),
            operation_name: crate::operations::kernel_packages::PROCESS_EXEC_OPERATION.to_string(),
            attach_event_id: None,
            surface: None,
        },
    ])
}

#[test]
fn router_attach_provenance_is_only_present_on_fold_derived_aliases() {
    let attach_event_id = verlet_history::EventRecordId::from_uuid(uuid::Uuid::from_u128(42));
    let router = crate::agent::agent_tool_router::AgentToolRouter::new(std::sync::Arc::new(
        verlet_operations::operation_registry::OperationRegistry::new(),
    ))
    .with_tool_aliases([
        crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: "bound_operation".to_string(),
            registered_name: "bound".to_string(),
            operation_name: "run".to_string(),
            attach_event_id: Some(attach_event_id),
            surface: None,
        },
        crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: "kernel_operation".to_string(),
            registered_name: "kernel".to_string(),
            operation_name: "run".to_string(),
            attach_event_id: None,
            surface: None,
        },
    ]);

    assert_eq!(
        router.attach_event_id_for_tool_name("bound_operation"),
        Some(attach_event_id)
    );
    assert_eq!(
        router.attach_event_id_for_tool_name("kernel_operation"),
        None
    );
    assert_eq!(router.attach_event_id_for_tool_name("static_tool"), None);
}

async fn router_with_kernel_notify_operation() -> crate::agent::agent_tool_router::AgentToolRouter {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let package = crate::operations::kernel_packages::verlet_notify_kernel_package();
    let dispatcher: std::sync::Arc<
        dyn verlet_operations::operation_registry::KernelOperationDispatcher,
    > = std::sync::Arc::new(crate::agent::agent_process::KernelNotifyOperationProvider);
    let mut registration = verlet_operations::operation_registry::KernelOperationRegistration::new(
        crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE,
        package.manifest.clone(),
    )
    .with_capability_grants(package.capability_grants.clone())
    .with_dispatcher(dispatcher);
    registration.metadata.insert(
        crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND.to_string(),
        serde_json::json!(crate::operations::kernel_packages::KERNEL_RUNTIME_KIND),
    );
    registry.register_kernel(registration).await.unwrap();
    crate::agent::agent_tool_router::AgentToolRouter::new(registry).with_tool_aliases(vec![
        crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: crate::operations::kernel_packages::NOTIFY_PREVIEW_OPERATION.to_string(),
            registered_name: crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE.to_string(),
            operation_name: crate::operations::kernel_packages::NOTIFY_PREVIEW_OPERATION
                .to_string(),
            attach_event_id: None,
            surface: None,
        },
        crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: crate::operations::kernel_packages::CHANNEL_EMIT_OPERATION.to_string(),
            registered_name: crate::operations::kernel_packages::VERLET_NOTIFY_PACKAGE.to_string(),
            operation_name: crate::operations::kernel_packages::CHANNEL_EMIT_OPERATION.to_string(),
            attach_event_id: None,
            surface: None,
        },
    ])
}

async fn router_with_kernel_thread_operations(
    host: &crate::kernel::runtime_host::RuntimeHost,
    context: verlet_runtime_contracts::ThreadContext,
) -> crate::agent::agent_tool_router::AgentToolRouter {
    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let package = crate::operations::kernel_packages::verlet_threads_kernel_package();
    let dispatcher: std::sync::Arc<
        dyn verlet_operations::operation_registry::KernelOperationDispatcher,
    > = std::sync::Arc::new(
        crate::agent::agent_process::KernelThreadOperationProvider::new(
            host.kernel_control(),
            context,
        ),
    );
    let mut registration = verlet_operations::operation_registry::KernelOperationRegistration::new(
        crate::operations::kernel_packages::VERLET_THREADS_PACKAGE,
        package.manifest.clone(),
    )
    .with_capability_grants(package.capability_grants.clone())
    .with_dispatcher(dispatcher);
    registration.metadata.insert(
        crate::operations::kernel_packages::OPERATION_METADATA_RUNTIME_KIND.to_string(),
        serde_json::json!(crate::operations::kernel_packages::KERNEL_RUNTIME_KIND),
    );
    registry.register_kernel(registration).await.unwrap();
    crate::agent::agent_tool_router::AgentToolRouter::new(registry).with_tool_aliases(
        [
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
            crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION,
            crate::operations::kernel_packages::THREAD_WAIT_OPERATION,
            crate::operations::kernel_packages::THREAD_STATUS_OPERATION,
            crate::operations::kernel_packages::THREAD_CANCEL_OPERATION,
        ]
        .into_iter()
        .map(
            |operation| crate::agent::agent_tool_router::OperationToolAlias {
                tool_name: operation.to_string(),
                registered_name: crate::operations::kernel_packages::VERLET_THREADS_PACKAGE
                    .to_string(),
                operation_name: operation.to_string(),
                attach_event_id: None,
                surface: None,
            },
        ),
    )
}

fn temp_cwd(name: &str) -> std::path::PathBuf {
    let cwd = std::env::temp_dir().join(format!(
        "verlet-agent-tool-router-{name}-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&cwd).unwrap();
    std::fs::canonicalize(cwd).unwrap()
}

fn tool_result_text(content: &[verlet_history::CanonicalContent]) -> String {
    content
        .iter()
        .find_map(|content| match content {
            verlet_history::CanonicalContent::Text { text, .. } => Some(text.clone()),
            _ => None,
        })
        .unwrap_or_default()
}

fn write_model_input_schema() -> serde_json::Value {
    serde_json::json!({
        "type": "object",
        "properties": {
            "path": {"type": "string"},
            "content": {"type": "string"}
        },
        "required": ["path", "content"],
        "additionalProperties": false
    })
}

fn write_tool_surface() -> crate::agent::agent_tool_router::ToolCallSurface {
    crate::agent::agent_tool_router::ToolCallSurface {
        model_input_schema: write_model_input_schema(),
        envelope_input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "root": {"type": "string"},
                "args": write_model_input_schema()
            },
            "required": ["root", "args"],
            "additionalProperties": false
        }),
        args_field: "args".to_string(),
        bound_values: std::collections::BTreeMap::from([(
            "root".to_string(),
            serde_json::json!("/workspace"),
        )]),
    }
}

fn echo_operation_guest(prefix: &str, input: &str, required_capabilities: &[&str]) -> String {
    let manifest = serde_json::json!({
        "abi": "cooldis.operation/0.1",
        "operations": [{
            "id": 1,
            "name": "search",
            "input": input,
            "output": "bytes",
            "events": "none",
            "mode": "sync",
            "required_capabilities": required_capabilities
        }]
    })
    .to_string();
    let prefix = format!("{prefix}:");
    format!(
        r#"
            (module
              (import "cooldis_0.1" "source_read" (func $source_read (param i32 i32 i32) (result i32)))
              (import "cooldis_0.1" "sink_write" (func $sink_write (param i32 i32 i32) (result i32)))
              (memory (export "memory") 1)
              (data (i32.const 4096) "{manifest}")
              (data (i32.const 8192) "{prefix}")
              (func (export "__verlet_describe_module__") (param $sink i32) (result i32)
                i32.const 0
                i32.const {manifest_len}
                i32.store
                local.get $sink
                i32.const 4096
                i32.const 0
                call $sink_write)
              (func (export "__verlet_call_operation__")
                (param $op i32)
                (param $invocation i32)
                (param $source i32)
                (param $output i32)
                (param $events i32)
                (result i32)
                (local $n i32)
                local.get $op
                i32.const 1
                i32.ne
                if
                  i32.const 2
                  return
                end
                i32.const 0
                i32.const 1024
                i32.store
                local.get $source
                i32.const 1024
                i32.const 0
                call $source_read
                drop
                i32.const 0
                i32.load
                local.set $n
                i32.const 0
                i32.const {prefix_len}
                i32.store
                local.get $output
                i32.const 8192
                i32.const 0
                call $sink_write
                drop
                i32.const 0
                local.get $n
                i32.store
                local.get $output
                i32.const 1024
                i32.const 0
                call $sink_write
                drop
                i32.const 0))
            "#,
        manifest = wat_bytes(manifest.as_bytes()),
        manifest_len = manifest.len(),
        prefix = wat_bytes(prefix.as_bytes()),
        prefix_len = prefix.len(),
    )
}

fn wat_bytes(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|byte| match byte {
            b'\n' => "\\0a".to_string(),
            b'\r' => "\\0d".to_string(),
            b'\t' => "\\09".to_string(),
            b'"' => "\\22".to_string(),
            b'\\' => "\\5c".to_string(),
            0x20..=0x7e => (*byte as char).to_string(),
            _ => format!("\\{byte:02x}"),
        })
        .collect()
}

fn kernel_identity_router(
    registry: std::sync::Arc<verlet_operations::operation_registry::OperationRegistry>,
    thread_id: &'static str,
    barrier: std::sync::Arc<tokio::sync::Barrier>,
) -> crate::agent::agent_tool_router::AgentToolRouter {
    let dispatcher: std::sync::Arc<
        dyn verlet_operations::operation_registry::KernelOperationDispatcher,
    > = std::sync::Arc::new(ThreadIdentityKernelDispatcher { thread_id, barrier });
    crate::agent::agent_tool_router::AgentToolRouter::new(registry)
        .with_kernel_dispatch_overlay(
            verlet_operations::operation_registry::KernelDispatchOverlay::new()
                .with_dispatcher("thread-identity", dispatcher),
        )
        .with_tool_aliases(vec![crate::agent::agent_tool_router::OperationToolAlias {
            tool_name: "identify-thread".to_string(),
            registered_name: "thread-identity".to_string(),
            operation_name: "identify-thread".to_string(),
            attach_event_id: None,
            surface: None,
        }])
}

struct ThreadIdentityKernelDispatcher {
    thread_id: &'static str,
    barrier: std::sync::Arc<tokio::sync::Barrier>,
}

#[async_trait::async_trait]
impl verlet_operations::operation_registry::KernelOperationDispatcher
    for ThreadIdentityKernelDispatcher
{
    async fn invoke_kernel_operation(
        &self,
        _operation_name: &str,
        _input: Vec<u8>,
    ) -> verlet_operations::VerletResult<Vec<u8>> {
        self.barrier.wait().await;
        Ok(self.thread_id.as_bytes().to_vec())
    }
}

struct FakeKernelToolProvider;

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for FakeKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            "record_context",
            "Return the current Verlet thread status.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        if call.tool_name != "record_context" {
            return Ok(None);
        }
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            r#"{"status":"idle"}"#,
            false,
        )))
    }
}

struct RecordingKernelToolProvider {
    seen: std::sync::Arc<
        std::sync::Mutex<Vec<Option<crate::kernel::runtime_host::turn::TurnContextSnapshot>>>,
    >,
}

#[async_trait::async_trait]
impl crate::agent::agent_tool_router::AgentKernelToolProvider for RecordingKernelToolProvider {
    async fn tool_definitions(&self) -> Vec<verlet_provider::ToolDefinition> {
        vec![verlet_provider::ToolDefinition::new(
            "record_context",
            "Record the current Verlet turn context.",
            serde_json::json!({
                "type": "object",
                "additionalProperties": false
            }),
        )]
    }

    async fn invoke_tool_call(
        &self,
        call: crate::agent::agent_tool_router::AgentKernelToolCall,
    ) -> crate::kernel::runtime_host::VerletResult<Option<verlet_history::CanonicalMessage>> {
        self.seen.lock().unwrap().push(call.turn_context.clone());
        Ok(Some(verlet_history::CanonicalMessage::tool_result(
            call.call_id,
            call.tool_name,
            "recorded",
            false,
        )))
    }
}
