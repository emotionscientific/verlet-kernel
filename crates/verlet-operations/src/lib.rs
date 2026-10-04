pub mod blob_store;
pub mod import_package;
pub mod kit_package;
pub mod openapi_plan;
pub mod operation_registry;
pub mod operation_store;
pub mod skill_import;
pub mod skill_package;
pub mod tool_package;

pub type VerletResult<T> = Result<T, VerletOperationsError>;

/// Commit a content-addressed artifact by renaming `tmp_path` over `path`.
///
/// One rename failure is benign: a concurrent writer installing the *same* hash
/// got there first, and because the destination is content-addressed it holds
/// the same bytes either way. That race is the only reason a failure is
/// tolerated, so the destination still has to be a regular file.
///
/// Any other failure — the destination is a directory, a leftover truncated
/// file from an interrupted earlier write, `EACCES`, `ENOSPC` — is reported.
/// Returning success there would claim an install that never happened, and the
/// lie would only surface much later as a hash mismatch from the `get` paths.
pub(crate) fn commit_content_addressed_artifact(
    tmp_path: &std::path::Path,
    path: &std::path::Path,
    label: &str,
) -> VerletResult<()> {
    match std::fs::rename(tmp_path, path) {
        Ok(()) => Ok(()),
        Err(_) if path.is_file() => {
            let _ = std::fs::remove_file(tmp_path);
            Ok(())
        }
        Err(err) => Err(VerletOperationsError::RuntimeFactory(format!(
            "failed to install {label} {}: {err}",
            path.display()
        ))),
    }
}

#[derive(Debug, thiserror::Error)]
pub enum VerletOperationsError {
    #[error("runtime factory failed: {0}")]
    RuntimeFactory(String),
    #[error("runtime execution failed: {0}")]
    RuntimeExecution(String),
}

impl From<verlet_wasm::VerletWasmError> for VerletOperationsError {
    fn from(error: verlet_wasm::VerletWasmError) -> Self {
        match error {
            verlet_wasm::VerletWasmError::RuntimeFactory(message) => Self::RuntimeFactory(message),
            verlet_wasm::VerletWasmError::RuntimeExecution(message) => {
                Self::RuntimeExecution(message)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RegisteredOperation {
    pub name: String,
    pub manifest: verlet_abi::WasmOperationManifest,
    pub capability_grants: std::collections::BTreeSet<String>,
    pub metadata: std::collections::BTreeMap<String, serde_json::Value>,
}

impl RegisteredOperation {
    pub fn projections(&self) -> OperationProjectionSet {
        OperationProjectionSet::from_registered(self)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OperationProjectionSet {
    pub registered_name: String,
    pub operations: Vec<OperationProjection>,
}

impl OperationProjectionSet {
    pub fn from_registered(record: &RegisteredOperation) -> Self {
        Self {
            registered_name: record.name.clone(),
            operations: record
                .manifest
                .operations
                .iter()
                .map(|operation| OperationProjection::from_operation(&record.name, operation))
                .collect(),
        }
    }

    pub fn with_tool_interface(
        mut self,
        interface: Option<&crate::tool_package::ToolInterfaceContract>,
    ) -> Self {
        let Some(interface) = interface else {
            return self;
        };
        for operation in &interface.operations {
            let Some(mcp) = &operation.mcp else {
                continue;
            };
            if let Some(projection) = self
                .operations
                .iter_mut()
                .find(|projection| projection.operation_name == operation.name)
            {
                projection.mcp.tool_name = mcp.tool_name.clone();
            }
        }
        self
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct OperationProjection {
    pub registered_name: String,
    pub operation_name: String,
    pub operation_id: u32,
    pub input: verlet_abi::WasmOperationValueKind,
    pub output: verlet_abi::WasmOperationValueKind,
    pub events: verlet_abi::WasmOperationEventKind,
    pub mode: verlet_abi::WasmOperationMode,
    pub cli: OperationCliProjection,
    pub process: OperationProcessProjection,
    pub http: OperationHttpProjection,
    pub llm_tool: OperationLlmToolProjection,
    pub mcp: OperationMcpProjection,
    pub abi: verlet_abi::AbiOperationContract,
}

impl OperationProjection {
    fn from_operation(
        registered_name: &str,
        operation: &verlet_abi::WasmOperationDefinition,
    ) -> Self {
        let tool_name = projection_tool_name(registered_name, &operation.name);
        Self {
            registered_name: registered_name.to_string(),
            operation_name: operation.name.clone(),
            operation_id: operation.id,
            input: operation.input.clone(),
            output: operation.output.clone(),
            events: operation.events.clone(),
            mode: operation.mode.clone(),
            cli: OperationCliProjection {
                command: format!("verlet tool run {registered_name} {}", operation.name),
                stdin: operation.input.clone(),
                stdout: operation.output.clone(),
            },
            process: OperationProcessProjection {
                command: format!("verlet run {registered_name} {}", operation.name),
                stdin: operation.input.clone(),
                stdout: operation.output.clone(),
                stderr: operation.events.clone(),
            },
            http: OperationHttpProjection {
                method: "POST".to_string(),
                path: format!("/operations/{registered_name}/{}", operation.name),
                request_body: operation.input.clone(),
                response_body: operation.output.clone(),
                event_stream: operation.events.clone(),
            },
            llm_tool: OperationLlmToolProjection {
                name: tool_name.clone(),
                input: operation.input.clone(),
                output: operation.output.clone(),
            },
            mcp: OperationMcpProjection {
                tool_name,
                input: operation.input.clone(),
                output: operation.output.clone(),
            },
            abi: verlet_abi::AbiOperationContract::from_operation(registered_name, operation),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperationCliProjection {
    pub command: String,
    pub stdin: verlet_abi::WasmOperationValueKind,
    pub stdout: verlet_abi::WasmOperationValueKind,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperationProcessProjection {
    pub command: String,
    pub stdin: verlet_abi::WasmOperationValueKind,
    pub stdout: verlet_abi::WasmOperationValueKind,
    pub stderr: verlet_abi::WasmOperationEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperationHttpProjection {
    pub method: String,
    pub path: String,
    pub request_body: verlet_abi::WasmOperationValueKind,
    pub response_body: verlet_abi::WasmOperationValueKind,
    pub event_stream: verlet_abi::WasmOperationEventKind,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperationLlmToolProjection {
    pub name: String,
    pub input: verlet_abi::WasmOperationValueKind,
    pub output: verlet_abi::WasmOperationValueKind,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct OperationMcpProjection {
    pub tool_name: String,
    pub input: verlet_abi::WasmOperationValueKind,
    pub output: verlet_abi::WasmOperationValueKind,
}

pub fn projection_tool_name(registered_name: &str, operation_name: &str) -> String {
    let registered = projection_tool_name_part(registered_name);
    let operation = projection_tool_name_part(operation_name);
    if registered == operation {
        operation
    } else {
        format!("{registered}_{operation}")
    }
}

fn projection_tool_name_part(raw: &str) -> String {
    let mut name = String::with_capacity(raw.len());
    let mut last_was_separator = false;
    for ch in raw.chars() {
        let normalized = if ch.is_ascii_alphanumeric() {
            last_was_separator = false;
            ch.to_ascii_lowercase()
        } else {
            if last_was_separator {
                continue;
            }
            last_was_separator = true;
            '_'
        };
        name.push(normalized);
    }
    name.trim_matches('_').to_string()
}

#[cfg(test)]
mod tests {
    #[test]
    fn committing_an_artifact_refuses_a_destination_that_cannot_hold_it() {
        let root = std::env::temp_dir().join(format!("verlet-commit-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let tmp_path = root.join("artifact.tmp");
        std::fs::write(&tmp_path, b"payload").unwrap();

        // A directory already sitting at the destination cannot hold the
        // artifact, so the failed rename has to be reported. The arm this
        // replaced returned Ok for any destination that merely existed, which
        // claimed an install that never happened.
        let destination = root.join("occupied");
        std::fs::create_dir(&destination).unwrap();

        let error =
            crate::commit_content_addressed_artifact(&tmp_path, &destination, "blob artifact")
                .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("failed to install blob artifact"),
            "unexpected error: {error}"
        );

        std::fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn registered_operation_derives_all_projection_surfaces() {
        let operation = crate::RegisteredOperation {
            name: "Example Search".to_string(),
            manifest: verlet_abi::WasmOperationManifest {
                abi: "cooldis.operation/0.1".to_string(),
                operations: vec![verlet_abi::WasmOperationDefinition {
                    id: 1,
                    name: "search".to_string(),
                    input: verlet_abi::WasmOperationValueKind::Bytes,
                    output: verlet_abi::WasmOperationValueKind::Bytes,
                    events: verlet_abi::WasmOperationEventKind::None,
                    mode: verlet_abi::WasmOperationMode::Sync,
                    required_capabilities: Vec::new(),
                }],
            },
            capability_grants: std::collections::BTreeSet::new(),
            metadata: std::collections::BTreeMap::new(),
        };

        let projections = operation.projections();

        assert_eq!(projections.registered_name, "Example Search");
        assert_eq!(projections.operations.len(), 1);
        let projection = &projections.operations[0];
        assert_eq!(
            projection.cli.command,
            "verlet tool run Example Search search"
        );
        assert_eq!(
            projection.process.command,
            "verlet run Example Search search"
        );
        assert_eq!(
            projection.process.stderr,
            verlet_abi::WasmOperationEventKind::None
        );
        assert_eq!(projection.http.method, "POST");
        assert_eq!(projection.http.path, "/operations/Example Search/search");
        assert_eq!(projection.llm_tool.name, "example_search_search");
        assert_eq!(projection.mcp.tool_name, "example_search_search");
        assert_eq!(
            crate::projection_tool_name("http-fetch", "http_fetch"),
            "http_fetch"
        );
        assert_eq!(
            crate::projection_tool_name("document", "extract_text"),
            "document_extract_text"
        );
        assert_eq!(projection.input, verlet_abi::WasmOperationValueKind::Bytes);
        assert_eq!(projection.output, verlet_abi::WasmOperationValueKind::Bytes);
        assert_eq!(projection.abi.registered_name, "Example Search");
        assert_eq!(projection.abi.operation_name, "search");
        assert_eq!(projection.abi.source_ports[0].name, "input");
        assert_eq!(projection.abi.sink_ports[0].name, "output");
        assert!(!projection.abi.has_hidden_durable_sink());
    }
}
