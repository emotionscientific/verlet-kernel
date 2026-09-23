fn repo_root() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(std::path::Path::parent)
        .expect("kernel crate should live two levels below repo root")
        .to_path_buf()
}

fn read(path: impl AsRef<std::path::Path>) -> String {
    std::fs::read_to_string(path.as_ref())
        .unwrap_or_else(|err| panic!("read {}: {err}", path.as_ref().display()))
}

#[test]
fn workspace_layout_contract_matches_current_repository_shape() {
    let root = repo_root();
    let root_manifest = read(root.join("Cargo.toml"));

    assert!(root_manifest.contains("[workspace]"));
    assert!(!root_manifest.contains("[package]"));
    assert!(root_manifest.contains(r#""crates/verlet-kernel""#));
    assert!(root_manifest.contains(r#"default-members = ["crates/verlet-kernel"]"#));
    assert!(!root_manifest.contains("crates/codex-inprocess-adapter"));

    for removed_path in ["crates/codex-inprocess-adapter", "vendor/codex"] {
        assert!(
            !root.join(removed_path).exists(),
            "{removed_path} should not reappear in the runtime workspace"
        );
    }

    for removed_root in ["src", "tests", "packages", "services"] {
        assert!(
            !root.join(removed_root).exists(),
            "{removed_root}/ should not reappear at the workspace root"
        );
    }

    for required in [
        "crates/verlet-kernel/src/kernel/runtime_host.rs",
        "crates/verlet-kernel/src/agent/agent_tool_router.rs",
        "crates/verlet-kernel/src/adapters/app_server.rs",
        "crates/verlet-kernel/src/capabilities/wasm_runner.rs",
        "crates/verlet-kernel/src/operations/operation_registry.rs",
        "crates/verlet-kernel/src/daemon/daemon_config.rs",
        "crates/verlet-kernel/src/cli.rs",
        "crates/verlet-kernel/src/bin/verlet.rs",
        "crates/verlet-kernel/src/bin/verlet-mcp-server.rs",
        "crates/verlet-kernel/tests/runtime_loop_scenarios.rs",
        "crates/verlet-kernel/tests/support_bins/verlet-mcp-echo-server.rs",
        "apps/console/package.json",
        "apps/console/src/App.svelte",
        "docs/README.md",
        "docs/index.md",
        "docs/getting-started.md",
        "docs/kernel-invariants.md",
        "docs/public-api-coverage.md",
        "docs/repository-map.md",
        "docs/developers/documentation-system.md",
        "mkdocs.yml",
        "scripts/ax-blind-test.sh",
    ] {
        assert!(root.join(required).exists(), "{required} should exist");
    }
}

#[test]
fn no_crate_mounts_a_module_through_mod_rs() {
    const DIRECTORY_VISIT_MAX: usize = 10_000;

    let crates = repo_root().join("crates");
    let mut pending: Vec<std::path::PathBuf> = vec![crates.clone()];
    let mut offenders: Vec<String> = Vec::new();

    for _ in 0..DIRECTORY_VISIT_MAX {
        let Some(directory) = pending.pop() else {
            break;
        };
        for entry in std::fs::read_dir(&directory)
            .unwrap_or_else(|err| panic!("read dir {}: {err}", directory.display()))
        {
            let path = entry.expect("directory entry should be readable").path();
            let name = path.file_name().unwrap_or_default();
            if path.is_dir() {
                if name != "target" {
                    pending.push(path);
                }
            } else if name == "mod.rs" {
                offenders.push(
                    path.strip_prefix(&crates)
                        .unwrap_or(&path)
                        .display()
                        .to_string(),
                );
            }
        }
    }

    assert!(
        pending.is_empty(),
        "crates/ has more than {DIRECTORY_VISIT_MAX} directories; raise the bound"
    );
    offenders.sort();
    assert!(
        offenders.is_empty(),
        "modules mount through foo.rs + foo/bar.rs, never mod.rs; found {offenders:?}"
    );
}

#[test]
fn github_workflow_stays_a_light_remote_sentinel() {
    let workflow = read(repo_root().join(".github/workflows/verify.yml"));
    let remote_ci = read(repo_root().join("scripts/check-remote-ci.sh"));
    let pre_push = read(repo_root().join("scripts/check-pre-push.sh"));

    assert!(workflow.contains("remote-sentinel:"));
    assert!(workflow.contains("run: scripts/check-remote-ci.sh"));
    assert!(!workflow.contains("scripts/check-ci.sh"));
    assert!(!workflow.contains("cargo run --locked --bin verlet-inprocess-smoke"));

    assert!(remote_ci.contains("scripts/guard-rails.sh\" tracked"));
    assert!(remote_ci.contains("cargo fmt --all -- --check"));
    assert!(remote_ci.contains("cargo metadata --locked --format-version 1 --no-deps"));
    assert!(!remote_ci.contains("crates/codex-inprocess-adapter"));
    assert!(!remote_ci.contains("cargo check"));
    assert!(!remote_ci.contains("cargo test"));
    assert!(!remote_ci.contains("cargo run --locked --bin verlet-inprocess-smoke"));

    assert!(pre_push.contains("scripts/verify.sh"));
    assert!(pre_push.contains("cargo clippy --workspace --all-targets --locked"));
    assert!(!pre_push.contains("verlet-inprocess-smoke"));

    let verify = read(repo_root().join("scripts/verify.sh"));
    assert!(!verify.contains("VERLET_VERIFY_SKIP_INPROCESS"));
    assert!(!verify.contains("crates/codex-inprocess-adapter"));
    assert!(!verify.contains("verlet-inprocess-smoke"));

    let kernel_manifest = read(repo_root().join("crates/verlet-kernel/Cargo.toml"));
    assert!(kernel_manifest.contains("autobins = false"));
}

#[test]
fn public_domain_namespaces_and_flat_exports_stay_usable() {
    let _flat_supervisor = verlet::kernel::supervisor::VerletSupervisor::new();
    let _namespaced_supervisor = verlet::kernel::supervisor::VerletSupervisor::new();
    let _tenant_context = verlet::kernel::supervisor::TenantRuntimeContext::local(
        "tenant",
        "/tmp/runtime",
        "/tmp/state",
    );

    let registry =
        std::sync::Arc::new(verlet_operations::operation_registry::OperationRegistry::new());
    let _router = verlet::agent::agent_tool_router::AgentToolRouter::new(registry);

    let daemon_config = verlet::daemon::daemon_config::VerletDaemonConfig::default();
    daemon_config
        .validate()
        .expect("default daemon config should validate");

    let _artifact = verlet_wasm::WasmRuntimeArtifact::bytes(Vec::new());
    let _coordinates =
        verlet_runtime_contracts::ThreadCoordinates::new("tenant", "user", "session");

    assert!(!verlet::adapters::mcp_server::MCP_PROTOCOL_VERSION.is_empty());
    assert_eq!(verlet_process::bridge::UNIX_NAMESPACE, "unix");
}

#[test]
fn hosted_docs_markdown_links_resolve_inside_docs_tree() {
    let docs = repo_root().join("docs");
    let mut checked = 0usize;
    let mut missing = Vec::new();

    for entry in std::fs::read_dir(&docs).expect("read docs directory") {
        let path = entry.expect("read docs entry").path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("md") {
            continue;
        }

        let text = read(&path);
        for link in markdown_links(&text) {
            if link.starts_with("http")
                || link.starts_with('#')
                || link.starts_with("mailto:")
                || !link.contains(".md")
            {
                continue;
            }

            let target = link.split('#').next().unwrap_or(&link);
            let resolved = path.parent().unwrap_or(&docs).join(target);
            checked += 1;
            if !resolved.exists() {
                missing.push(format!("{} -> {link}", path.display()));
            }
        }
    }

    assert!(checked > 0, "docs link check should exercise local links");
    assert!(
        missing.is_empty(),
        "missing docs links:\n{}",
        missing.join("\n")
    );
}

#[test]
fn public_docs_pin_repo_positioning_and_workflow() {
    let docs_index = read(repo_root().join("docs/README.md"));
    let public_docs = read(repo_root().join("docs/index.md"));
    let docs_system = read(repo_root().join("docs/developers/documentation-system.md"));
    let agents = read(repo_root().join("AGENTS.md"));
    let blind_test = read(repo_root().join("scripts/ax-blind-test.sh"));

    for required in [
        "agent manifests",
        "operation publishing",
        "ABI contracts",
        "local runtime execution",
        "Public API Coverage",
    ] {
        assert!(
            public_docs.contains(required)
                || docs_index.contains(required)
                || docs_system.contains(required),
            "public docs should mention {required:?}"
        );
    }

    assert!(docs_index.contains("Public API Coverage](public-api-coverage.md)"));
    assert!(docs_index.contains("Kernel Invariants](kernel-invariants.md)"));
    assert!(docs_system.contains("docs/"));
    assert!(docs_system.contains("internal planning notes"));
    assert!(agents.contains("Agent Experience (AX)"));
    assert!(agents.contains("docs/public-api-coverage.md"));
    assert!(blind_test.contains("Verlet AX Blind-Test Care Test"));
    assert!(blind_test.contains("docs/"));
    assert!(blind_test.contains("Why should I give a shit about Verlet?"));
    assert!(blind_test.contains("VERLET_AX_AGENT_COMMAND"));
    assert!(blind_test.contains("codex exec"));
}

#[test]
fn public_api_coverage_tracks_docs_and_man_page_gaps() {
    let coverage = read(repo_root().join("docs/public-api-coverage.md"));
    let cli = read(repo_root().join("crates/verlet-kernel/src/cli.rs"));

    for surface in [
        "verlet",
        "verlet agent init",
        "verlet agent plan",
        "verlet agent publish",
        "verlet agent list",
        "verlet agent versions",
        "verlet agent diff",
        "verlet agent show",
        "verlet agent run",
        "verlet blob publish",
        "verlet tool build",
        "verlet tool list",
        "verlet tool publish",
        "verlet tool run",
        "verlet tool manual",
        "verlet auth",
        "verlet rpc",
        "verlet chat",
        "verlet debug rpc",
        "verlet serve",
        "verlet daemon config validate",
        "verlet daemon service print",
        "verlet daemon service install",
        "verlet daemon service uninstall",
    ] {
        assert!(
            coverage.contains(surface),
            "coverage ledger should track {surface}"
        );

        assert!(
            cli.contains(surface) || surface == "verlet",
            "CLI source should still expose {surface}"
        );
    }

    for required in [
        "Help/man projection",
        "`verlet(1)`",
        "Command contracts",
        "issue [#104]",
        "Virtual-bash commands",
        "MCP compatibility ingress",
        "RPC thread methods",
        "Agent manifests",
        "Daemon config",
        "Secret references",
        "Identity/RBAC adapters",
    ] {
        assert!(
            coverage.contains(required),
            "coverage ledger should mention {required:?}"
        );
    }
}

#[test]
fn app_server_docs_track_v1_approval_control_surface() {
    let app_server = read(repo_root().join("docs/app-server.md"));
    let stdlib = read(repo_root().join("docs/standard-operations.md"));
    let tracker = read(repo_root().join("docs/v1-release-candidate.md"));
    let coverage = read(repo_root().join("docs/public-api-coverage.md"));

    for (name, doc) in [
        ("app-server", app_server.as_str()),
        ("standard-operations", stdlib.as_str()),
        ("public-api-coverage", coverage.as_str()),
    ] {
        assert!(
            doc.contains("approval/resolve"),
            "{name} should name the V1 abstract approval write surface"
        );
    }

    assert!(
        stdlib.contains("`std::permission.approval_gate`")
            && stdlib.contains("paired `approval.requested` plus `tool.call.suspended`")
            && stdlib.contains("reference executable template for the")
            && tracker.contains("`std::permission.approval_gate`")
            && tracker.contains("catalog marks it runtime-executable/reference-only"),
        "approval gate docs should describe the abstract executable gate without claiming channel HITL"
    );

    for stale in [
        "A future app-server method would look like",
        "approval resolution remain behind a concrete interface decision",
        "resolution remain behind a concrete interface decision",
        "Approval and waiting inspection can ship first",
        "still V1 work: process/command",
        "`std::permission.approval_gate` remains interface-only",
        "approval_gate` remains interface-only",
    ] {
        assert!(
            !stdlib.contains(stale) && !app_server.contains(stale) && !tracker.contains(stale),
            "stale approval/control-plane language should not reappear: {stale:?}"
        );
    }
}

#[test]
fn release_candidate_gate_keeps_v1_live_matrix_explicit() {
    let script = read(repo_root().join("scripts/release-v1-candidate.sh"));
    let tracker = read(repo_root().join("docs/v1-release-candidate.md"));

    for required in [
        "--live-provider-protocols",
        "--live-openai-responses",
        "--live-anthropic-messages",
        "--live-openai-compatible",
        "--live-search",
        "--live-telegram",
        "verlet-bifrost-smoke",
        "legacy private provider lane; unavailable",
        "legacy private search lane; unavailable",
        "public checkout",
        "Telegram bot IO live lane is paused for V1",
    ] {
        assert!(
            script.contains(required),
            "release script should mention {required:?}"
        );
    }

    assert!(
        script.contains("legacy private provider lane; unavailable here"),
        "legacy VERLET_RELEASE_LIVE_PROVIDER should fail closed in public checkout"
    );
    assert!(
        script.contains("--live                    run public live provider-protocol lanes except paused Telegram"),
        "--live should not silently claim paused Telegram coverage"
    );

    for required in [
        "Default Gate",
        "Optional Public Lanes",
        "Maintainer-Private Lanes",
        "legacy private lane flag or environment variable",
    ] {
        assert!(
            tracker.contains(required),
            "release tracker should mention {required:?}"
        );
    }
}

#[test]
fn non_test_request_paths_do_not_reintroduce_panic_shortcuts() {
    let root = repo_root();
    let request_paths = [
        "crates/verlet-kernel/src/adapters/app_server/connection.rs",
        "crates/verlet-kernel/src/adapters/app_server/default_manifest.rs",
        "crates/verlet-kernel/src/adapters/app_server.rs",
        "crates/verlet-kernel/src/adapters/app_server/subscriptions.rs",
        "crates/verlet-kernel/src/adapters/app_server/threads.rs",
        "crates/verlet-kernel/src/adapters/mcp_server.rs",
        "crates/verlet-kernel/src/adapters/agent_loop.rs",
        "crates/verlet-kernel/src/capabilities/execution.rs",
        "crates/verlet-kernel/src/capabilities/wasm_runner.rs",
        "crates/verlet-kernel/src/kernel/coupling_scheduler.rs",
        "crates/verlet-kernel/src/kernel/runtime_host.rs",
        "crates/verlet-kernel/src/kernel/supervisor.rs",
    ];
    let forbidden = [
        ".unwrap(",
        ".expect(",
        "panic!(",
        "todo!(",
        "unimplemented!(",
    ];
    let mut hits = Vec::new();

    for relative in request_paths {
        let path = root.join(relative);
        let source = strip_cfg_test_modules(&read(&path));
        for (idx, line) in source.lines().enumerate() {
            if forbidden.iter().any(|pattern| line.contains(pattern)) {
                hits.push(format!("{relative}:{}:{}", idx + 1, line.trim()));
            }
        }
    }

    assert!(
        hits.is_empty(),
        "non-test request-serving paths should return typed errors instead of panic shortcuts:\n{}",
        hits.join("\n")
    );
}

fn markdown_links(text: &str) -> Vec<String> {
    let mut links = Vec::new();
    let mut rest = text;

    while let Some(label_end) = rest.find("](") {
        let after_open = &rest[label_end + 2..];
        let Some(link_end) = after_open.find(')') else {
            break;
        };
        links.push(after_open[..link_end].to_string());
        rest = &after_open[link_end + 1..];
    }

    links
}

fn strip_cfg_test_modules(source: &str) -> String {
    let mut stripped = String::new();
    let mut pending_cfg_test = false;
    let mut test_module_depth = 0i32;

    for line in source.lines() {
        let trimmed = line.trim();
        if test_module_depth > 0 {
            test_module_depth += brace_delta(line);
            continue;
        }
        if pending_cfg_test && trimmed.starts_with("mod tests") {
            test_module_depth = brace_delta(line).max(1);
            pending_cfg_test = false;
            continue;
        }
        if trimmed == "#[cfg(test)]" {
            pending_cfg_test = true;
            continue;
        }
        if pending_cfg_test && !trimmed.starts_with("#[") && !trimmed.is_empty() {
            pending_cfg_test = false;
        }
        stripped.push_str(line);
        stripped.push('\n');
    }

    stripped
}

fn brace_delta(line: &str) -> i32 {
    line.chars().fold(0, |depth, ch| match ch {
        '{' => depth + 1,
        '}' => depth - 1,
        _ => depth,
    })
}
