#[tokio::test]
async fn kernel_thread_operations_spawn_and_control_child_by_task_name() {
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
    let provider = crate::agent::agent_process::KernelThreadOperationProvider::new(
        host.kernel_control(),
        root.context().clone(),
    );

    let spawn = provider
        .invoke_json(
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
            serde_json::json!({
                "task_name": "worker",
                "message": "echo tool-child",
            }),
        )
        .await
        .unwrap();
    assert_eq!(spawn["operation"], "cooldis.thread_spawn");
    assert_eq!(spawn["task_name"], "worker");
    assert_eq!(spawn.as_object().unwrap().len(), 3);
    let children = host.children_of(root.context().coordinates.thread_id).await;
    assert_eq!(children.len(), 1);

    let wait = provider
        .invoke_json(
            crate::operations::kernel_packages::THREAD_WAIT_OPERATION,
            serde_json::json!({
                "task_name": "worker",
                "timeout_ms": 1_000,
            }),
        )
        .await
        .unwrap();
    assert_eq!(wait["operation"], "cooldis.thread_wait");
    assert_eq!(wait["task_name"], "worker");
    assert_eq!(wait.as_object().unwrap().len(), 3);

    let status = provider
        .invoke_json(
            crate::operations::kernel_packages::THREAD_STATUS_OPERATION,
            serde_json::json!({"task_name": "worker"}),
        )
        .await
        .unwrap();
    assert_eq!(status["operation"], "cooldis.thread_status");
    assert_eq!(status["task_name"], "worker");
    assert_eq!(status.as_object().unwrap().len(), 3);

    provider
        .invoke_json(
            crate::operations::kernel_packages::THREAD_CANCEL_OPERATION,
            serde_json::json!({
                "task_name": "worker",
            }),
        )
        .await
        .unwrap();
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn kernel_thread_operations_decode_errors_do_not_echo_raw_thread_id_values() {
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
    ));
    let root = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session-a"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let provider = crate::agent::agent_process::KernelThreadOperationProvider::new(
        host.kernel_control(),
        root.context().clone(),
    );
    let raw_id = verlet_runtime_contracts::ThreadId::new().to_string();

    for (operation, arguments) in [
        (
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
            serde_json::json!({
                "task_name": "worker",
                "message": "work",
                "target_thread_id": raw_id,
            }),
        ),
        (
            crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION,
            serde_json::json!({
                "task_name": "worker",
                "message": "work",
                "target_thread_id": raw_id,
            }),
        ),
        (
            crate::operations::kernel_packages::THREAD_WAIT_OPERATION,
            serde_json::json!({
                "task_name": "worker",
                "target_thread_id": raw_id,
            }),
        ),
        (
            crate::operations::kernel_packages::THREAD_STATUS_OPERATION,
            serde_json::json!({
                "task_name": "worker",
                "target_thread_id": raw_id,
            }),
        ),
        (
            crate::operations::kernel_packages::THREAD_CANCEL_OPERATION,
            serde_json::json!({
                "task_name": "worker",
                "target_thread_id": raw_id,
            }),
        ),
    ] {
        let err = provider
            .invoke_json(operation, arguments)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("unknown field `target_thread_id`"));
        assert!(!err.to_string().contains(&raw_id));
    }
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn retried_thread_submit_dispatch_enqueues_one_turn() {
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
    let provider = crate::agent::agent_process::KernelThreadOperationProvider::new(
        host.kernel_control(),
        root.context().clone(),
    );
    provider
        .invoke_json(
            crate::operations::kernel_packages::THREAD_SPAWN_OPERATION,
            serde_json::json!({"task_name": "worker", "message": "echo initial"}),
        )
        .await
        .unwrap();
    let arguments = serde_json::json!({
        "task_name": "worker",
        "message": "echo submit-once",
        "dispatch_id": "submit-dispatch-1",
    });

    let first = provider
        .invoke_json(
            crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION,
            arguments.clone(),
        )
        .await
        .unwrap();
    let retry = provider
        .invoke_json(
            crate::operations::kernel_packages::THREAD_SUBMIT_OPERATION,
            arguments,
        )
        .await
        .unwrap();

    assert_eq!(first, retry);
    assert_eq!(first["operation"], "cooldis.thread_submit");
    assert_eq!(first["task_name"], "worker");
    assert_eq!(first.as_object().unwrap().len(), 3);
    let child = host
        .children_of(root.context().coordinates.thread_id)
        .await
        .pop()
        .unwrap();
    let submitted_count = tokio::time::timeout(std::time::Duration::from_secs(30), async {
        loop {
            let session = child.session_context().await.unwrap();
            let count = session
                .messages
                .iter()
                .filter(|message| {
                    matches!(
                        message,
                        verlet_history::CanonicalMessage::User { content, .. }
                            if content.iter().any(|content| matches!(
                                content,
                                verlet_history::CanonicalContent::Text { text, .. }
                                    if text == "echo submit-once"
                            ))
                    )
                })
                .count();
            if count > 0 {
                break count;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(submitted_count, 1);

    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn thread_submit_dispatch_fold_is_scoped_to_the_target_thread() {
    let host = crate::kernel::runtime_host::RuntimeHost::new(std::sync::Arc::new(
        crate::capabilities::execution::VirtualBashRuntimeFactory::default(),
    ));
    let caller = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let first_target = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let second_target = host
        .start_thread(
            verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session"),
            verlet_runtime_contracts::ThreadTopology::root(),
        )
        .await
        .unwrap();
    let control = host.kernel_control();
    let dispatch_id = verlet_runtime_contracts::handle::DispatchId::new("shared-submit-dispatch");

    let first = control
        .submit_to_thread_with_dispatch(
            caller.context(),
            first_target.context().coordinates.thread_id,
            dispatch_id.clone(),
            crate::kernel::runtime_host::turn::TurnInput::text("echo first target"),
        )
        .await
        .unwrap();
    let retry = control
        .submit_to_thread_with_dispatch(
            caller.context(),
            first_target.context().coordinates.thread_id,
            dispatch_id.clone(),
            crate::kernel::runtime_host::turn::TurnInput::text("echo folded retry"),
        )
        .await
        .unwrap();
    let second = control
        .submit_to_thread_with_dispatch(
            caller.context(),
            second_target.context().coordinates.thread_id,
            dispatch_id,
            crate::kernel::runtime_host::turn::TurnInput::text("echo second target"),
        )
        .await
        .unwrap();

    assert_eq!(first, retry);
    assert_eq!(first.turn_id, second.turn_id);
    assert_ne!(first.interaction_id, second.interaction_id);
    for interaction_id in [first.interaction_id, second.interaction_id] {
        let interaction_id = uuid::Uuid::parse_str(&interaction_id.to_string()).unwrap();
        assert_eq!(interaction_id.get_version_num(), 8);
    }
    tokio::task::yield_now().await;
    let first_session = first_target.session_context().await.unwrap();
    let second_session = second_target.session_context().await.unwrap();
    assert!(first_session.messages.iter().any(|message| matches!(
        message,
        verlet_history::CanonicalMessage::User { content, .. }
            if content.iter().any(|content| matches!(
                content,
                verlet_history::CanonicalContent::Text { text, .. } if text == "echo first target"
            ))
    )));
    assert!(first_session.messages.iter().all(|message| !matches!(
        message,
        verlet_history::CanonicalMessage::User { content, .. }
            if content.iter().any(|content| matches!(
                content,
                verlet_history::CanonicalContent::Text { text, .. } if text == "echo folded retry"
            ))
    )));
    assert!(second_session.messages.iter().any(|message| matches!(
        message,
        verlet_history::CanonicalMessage::User { content, .. }
            if content.iter().any(|content| matches!(
                content,
                verlet_history::CanonicalContent::Text { text, .. } if text == "echo second target"
            ))
    )));

    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn compatibility_submit_without_dispatch_keeps_legacy_turn_identity() {
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

    let receipt = host
        .kernel_control()
        .submit_to_thread(
            root.context(),
            root.context().coordinates.thread_id,
            None,
            crate::kernel::runtime_host::turn::TurnInput::text("echo compatibility submit"),
        )
        .await
        .unwrap();

    assert!(receipt.turn_id.starts_with("agent-process-v1-"));
    let interaction_id = uuid::Uuid::parse_str(&receipt.interaction_id.to_string()).unwrap();
    assert_eq!(interaction_id.get_version_num(), 7);
    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn kernel_process_operations_exec_and_poll_host_command() {
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
    let provider = crate::agent::agent_process::KernelProcessOperationProvider::new(
        root.context().clone(),
        temp_cwd("exec"),
    )
    .with_process_dispatcher(
        crate::kernel::process_handle_dispatch::test_process_dispatcher(
            host.runtime_store(),
            root.context().coordinates.clone(),
        ),
    );

    let started = provider
        .invoke_json(
            crate::operations::kernel_packages::PROCESS_EXEC_OPERATION,
            serde_json::json!({
                "command": ["/bin/sh", "-c", "sleep 0.1; printf done"],
                "yield_time_ms": 1,
                "timeout_ms": 2_000,
                "output_bytes_cap": 4_096
            }),
        )
        .await
        .unwrap();
    assert_eq!(started["operation"], "cooldis.process_exec");
    assert_eq!(started["backend"], "host_bash");
    assert_eq!(started["status"], "running");
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let polled = provider
        .invoke_json(
            crate::operations::kernel_packages::PROCESS_POLL_OPERATION,
            serde_json::json!({
                "process_id": process_id.clone(),
                "yield_time_ms": 1_000,
                "output_bytes_cap": 4_096
            }),
        )
        .await
        .unwrap();
    assert_eq!(polled["operation"], "cooldis.process_poll");
    assert_eq!(polled["backend"], "host_bash");
    assert_eq!(polled["status"], "completed");
    assert_eq!(polled["exit_code"], 0);
    assert_eq!(polled["stdout"], "done");
    assert_eq!(polled["process_id"], process_id);
    assert!(polled["event_count"].as_u64().unwrap() >= 2);

    host.shutdown_all().await.unwrap();
}

#[tokio::test]
async fn kernel_process_operations_write_and_terminate_host_process() {
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
    let provider = crate::agent::agent_process::KernelProcessOperationProvider::new(
        root.context().clone(),
        temp_cwd("stdin"),
    )
    .with_process_dispatcher(
        crate::kernel::process_handle_dispatch::test_process_dispatcher(
            host.runtime_store(),
            root.context().coordinates.clone(),
        ),
    );

    let started = provider
        .invoke_json(
            crate::operations::kernel_packages::PROCESS_EXEC_OPERATION,
            serde_json::json!({
                "command": ["cat"],
                "stream_stdin": true,
                "yield_time_ms": 1,
                "timeout_ms": 5_000,
                "output_bytes_cap": 4_096
            }),
        )
        .await
        .unwrap();
    assert_eq!(started["status"], "running");
    let process_id = started["process_id"].as_str().unwrap().to_string();

    let wrote = provider
        .invoke_json(
            crate::operations::kernel_packages::PROCESS_WRITE_OPERATION,
            serde_json::json!({
                "process_id": process_id.clone(),
                "delta_base64": base64::Engine::encode(&base64::engine::general_purpose::STANDARD, "hello\n"),
                "yield_time_ms": 500,
                "output_bytes_cap": 4_096
            }),
        )
        .await
        .unwrap();
    assert_eq!(wrote["operation"], "cooldis.process_write");
    assert_eq!(wrote["status"], "running");
    assert_eq!(wrote["stdout"], "hello\n");

    let process_id = wrote["process_id"].as_str().unwrap().to_string();
    let terminated = provider
        .invoke_json(
            crate::operations::kernel_packages::PROCESS_TERMINATE_OPERATION,
            serde_json::json!({
                "process_id": process_id,
                "reason": "test cleanup",
                "yield_time_ms": 1_000
            }),
        )
        .await
        .unwrap();
    assert_eq!(terminated["operation"], "cooldis.process_terminate");
    assert_eq!(terminated["status"], "cancelled");
    assert_eq!(terminated["process_id"], process_id);

    host.shutdown_all().await.unwrap();
}

fn temp_cwd(name: &str) -> std::path::PathBuf {
    let cwd = std::env::temp_dir().join(format!(
        "verlet-kernel-process-{name}-{}",
        uuid::Uuid::now_v7()
    ));
    std::fs::create_dir_all(&cwd).unwrap();
    cwd
}
