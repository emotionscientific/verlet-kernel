use sha2::Digest as _;
use std::io::Write as _;

const RECORD_SCHEMA_VERSION: u32 = 1;
const BINDING_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct OperationBlobStore {
    root: std::path::PathBuf,
}

impl OperationBlobStore {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn put(&self, bytes: &[u8]) -> crate::VerletResult<String> {
        let hash = wasm_sha256(bytes);
        let path = self.artifact_path(&hash)?;
        if path.exists() {
            let existing = std::fs::read(&path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read existing blob {}: {err}",
                    path.display()
                ))
            })?;
            if wasm_sha256(&existing) == hash {
                return Ok(hash);
            }
            std::fs::remove_file(&path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to replace corrupt existing blob {}: {err}",
                    path.display()
                ))
            })?;
        }
        let Some(parent) = path.parent() else {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "blob path {} has no parent directory",
                path.display()
            )));
        };
        std::fs::create_dir_all(parent).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to create blob directory {}: {err}",
                parent.display()
            ))
        })?;
        let tmp_path = parent.join(format!(".{hash}.tmp.{}", uuid::Uuid::now_v7()));
        {
            let mut file = std::fs::File::create(&tmp_path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to create temp blob {}: {err}",
                    tmp_path.display()
                ))
            })?;
            file.write_all(bytes).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to write temp blob {}: {err}",
                    tmp_path.display()
                ))
            })?;
            file.sync_all().map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to sync temp blob {}: {err}",
                    tmp_path.display()
                ))
            })?;
        }
        crate::commit_content_addressed_artifact(&tmp_path, &path, "blob")?;
        Ok(hash)
    }

    pub fn get(&self, hash: &str) -> crate::VerletResult<Option<Vec<u8>>> {
        validate_hash(hash)?;
        let path = self.artifact_path(hash)?;
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read blob {}: {err}",
                path.display()
            ))
        })?;
        let actual = wasm_sha256(&bytes);
        if actual != hash {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "blob {} hash mismatch: expected {hash}, got {actual}",
                path.display()
            )));
        }
        Ok(Some(bytes))
    }

    pub fn artifact_path(&self, hash: &str) -> crate::VerletResult<std::path::PathBuf> {
        validate_hash(hash)?;
        Ok(self.root.join(&hash[..2]).join(format!("{hash}.wasm")))
    }
}

#[derive(Clone, Debug)]
pub struct LocalOperationRegistry {
    root: std::path::PathBuf,
    blobs: OperationBlobStore,
}

impl LocalOperationRegistry {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        let root = root.into();
        Self {
            blobs: OperationBlobStore::new(root.join("blobs")),
            root,
        }
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn blobs(&self) -> &OperationBlobStore {
        &self.blobs
    }

    pub async fn publish_artifact(
        &self,
        request: PublishOperationRequest,
    ) -> crate::VerletResult<PublishedOperationRecord> {
        let name = validate_record_name(&request.name)?;
        let bytes = std::fs::read(&request.artifact_path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read operation artifact {}: {err}",
                request.artifact_path.display()
            ))
        })?;
        let validation_config = verlet_wasm::WasmRuntimeConfig::new(
            verlet_wasm::WasmRuntimeArtifact::bytes(bytes.clone()),
        )
        .with_capability_grants(request.capability_grants.clone());
        let validation = verlet_wasm::runner::WasmRuntimeFactory::new(validation_config)?
            .validate_operation_artifact()
            .await?;
        validate_required_grants(&name, &validation, &request.capability_grants)?;

        let hash = self.blobs.put(&bytes)?;
        let registered = crate::RegisteredOperation {
            name: name.clone(),
            manifest: validation.clone(),
            capability_grants: request.capability_grants.clone(),
            metadata: request.metadata.clone(),
        };
        let projections = registered
            .projections()
            .with_tool_interface(request.interface.as_ref());
        if let Some(interface) = &request.interface {
            interface.validate_against_operation_record(&name, &validation, &projections)?;
        }
        let record = PublishedOperationRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            name,
            active_artifact_hash: hash,
            manifest: validation,
            projections,
            interface: request.interface,
            capability_grants: request.capability_grants,
            metadata: request.metadata,
            source: request.source,
            build: PublishedOperationBuild {
                artifact_path: request.artifact_path,
                published_at_ms: now_ms(),
            },
        };
        record.validate()?;
        self.write_version_record_atomically(&record)?;
        self.write_record_atomically(&record)?;
        Ok(record)
    }

    pub fn publish_interface_record(
        &self,
        request: PublishInterfaceOperationRequest,
    ) -> crate::VerletResult<PublishedOperationRecord> {
        let name = validate_record_name(&request.name)?;
        validate_manifest_shape(&request.manifest)?;
        validate_required_grants(&name, &request.manifest, &request.capability_grants)?;
        let interface = request.interface;
        let artifact_bytes = serde_json::to_vec(&interface).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to encode kernel operation contract for {name:?}: {err}"
            ))
        })?;
        let hash = self.blobs.put(&artifact_bytes)?;
        let registered = crate::RegisteredOperation {
            name: name.clone(),
            manifest: request.manifest.clone(),
            capability_grants: request.capability_grants.clone(),
            metadata: request.metadata.clone(),
        };
        let projections = registered
            .projections()
            .with_tool_interface(Some(&interface));
        interface.validate_against_operation_record(&name, &request.manifest, &projections)?;
        let record = PublishedOperationRecord {
            schema_version: RECORD_SCHEMA_VERSION,
            name,
            active_artifact_hash: hash,
            manifest: request.manifest,
            projections,
            interface: Some(interface),
            capability_grants: request.capability_grants,
            metadata: request.metadata,
            source: request.source,
            build: PublishedOperationBuild {
                artifact_path: std::path::PathBuf::from("<interface-contract>"),
                published_at_ms: now_ms(),
            },
        };
        record.validate()?;
        self.write_version_record_atomically(&record)?;
        self.write_record_atomically(&record)?;
        Ok(record)
    }

    pub fn load_record(&self, name: &str) -> crate::VerletResult<PublishedOperationRecord> {
        let name = validate_record_name(name)?;
        let path = self.record_path(&name)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read operation record {}: {err}",
                path.display()
            ))
        })?;
        let record: PublishedOperationRecord = serde_json::from_slice(&bytes).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to decode operation record {}: {err}",
                path.display()
            ))
        })?;
        record.validate()?;
        if record.name != name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "operation record {} names {:?}, expected {:?}",
                path.display(),
                record.name,
                name
            )));
        }
        Ok(record)
    }

    pub fn list_records(&self) -> crate::VerletResult<Vec<PublishedOperationRecord>> {
        let records_dir = self.root.join("records");
        if !records_dir.exists() {
            return Ok(Vec::new());
        }
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&records_dir).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read operation records directory {}: {err}",
                records_dir.display()
            ))
        })? {
            let entry = entry.map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read operation record entry in {}: {err}",
                    records_dir.display()
                ))
            })?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            names.push(validate_record_name(name)?);
        }
        names.sort();
        names
            .into_iter()
            .map(|name| self.load_record(&name))
            .collect()
    }

    pub fn load_version_record(
        &self,
        name: &str,
        artifact_hash: &str,
    ) -> crate::VerletResult<PublishedOperationRecord> {
        let name = validate_record_name(name)?;
        validate_hash(artifact_hash)?;
        let path = self.version_record_path(&name, artifact_hash)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read operation version record {}: {err}",
                path.display()
            ))
        })?;
        let record: PublishedOperationRecord = serde_json::from_slice(&bytes).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to decode operation version record {}: {err}",
                path.display()
            ))
        })?;
        record.validate()?;
        if record.name != name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "operation version record {} names {:?}, expected {:?}",
                path.display(),
                record.name,
                name
            )));
        }
        if record.active_artifact_hash != artifact_hash {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "operation version record {} uses artifact hash {}, expected {}",
                path.display(),
                record.active_artifact_hash,
                artifact_hash
            )));
        }
        Ok(record)
    }

    pub fn bind_capsule_operation(
        &self,
        scope: CapsuleBindingScope,
        operation_name: impl AsRef<str>,
        artifact_hash: impl AsRef<str>,
    ) -> crate::VerletResult<CapsuleBindingRecord> {
        let operation_name = validate_record_name(operation_name.as_ref())?;
        let artifact_hash = artifact_hash.as_ref();
        self.load_version_record(&operation_name, artifact_hash)
            .map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "cannot bind capsule operation {operation_name:?} to version {artifact_hash}: {err}"
                ))
            })?;
        let record = CapsuleBindingRecord {
            schema_version: BINDING_SCHEMA_VERSION,
            scope,
            operation_name,
            target: CapsuleBindingTarget::Version {
                artifact_hash: artifact_hash.to_string(),
            },
            updated_at_ms: now_ms(),
        };
        record.validate()?;
        self.write_binding_record_atomically(&record)?;
        Ok(record)
    }

    pub fn unbind_capsule_operation(
        &self,
        scope: CapsuleBindingScope,
        operation_name: impl AsRef<str>,
    ) -> crate::VerletResult<CapsuleBindingRecord> {
        let record = CapsuleBindingRecord {
            schema_version: BINDING_SCHEMA_VERSION,
            scope,
            operation_name: validate_record_name(operation_name.as_ref())?,
            target: CapsuleBindingTarget::Tombstone,
            updated_at_ms: now_ms(),
        };
        record.validate()?;
        self.write_binding_record_atomically(&record)?;
        Ok(record)
    }

    pub fn list_capsule_bindings(
        &self,
        scope: CapsuleBindingScope,
    ) -> crate::VerletResult<Vec<CapsuleBindingRecord>> {
        let dir = self.binding_scope_dir(&scope)?;
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut names = Vec::new();
        for entry in std::fs::read_dir(&dir).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read capsule binding directory {}: {err}",
                dir.display()
            ))
        })? {
            let entry = entry.map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read capsule binding entry in {}: {err}",
                    dir.display()
                ))
            })?;
            let path = entry.path();
            if path.extension().and_then(|extension| extension.to_str()) != Some("json") {
                continue;
            }
            let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            names.push(validate_record_name(name)?);
        }
        names.sort();
        names
            .into_iter()
            .map(|name| self.load_capsule_binding(scope.clone(), &name))
            .collect()
    }

    pub fn load_capsule_binding(
        &self,
        scope: CapsuleBindingScope,
        operation_name: &str,
    ) -> crate::VerletResult<CapsuleBindingRecord> {
        let operation_name = validate_record_name(operation_name)?;
        let path = self.binding_record_path(&scope, &operation_name)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read capsule binding record {}: {err}",
                path.display()
            ))
        })?;
        let record: CapsuleBindingRecord = serde_json::from_slice(&bytes).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to decode capsule binding record {}: {err}",
                path.display()
            ))
        })?;
        record.validate()?;
        if record.scope != scope {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "capsule binding record {} has scope {:?}, expected {:?}",
                path.display(),
                record.scope,
                scope
            )));
        }
        if record.operation_name != operation_name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "capsule binding record {} names {:?}, expected {:?}",
                path.display(),
                record.operation_name,
                operation_name
            )));
        }
        Ok(record)
    }

    pub fn resolve_capsule_binding_snapshot(
        &self,
        request: CapsuleBindingResolutionRequest,
    ) -> crate::VerletResult<CapsuleBindingSnapshot> {
        let mut resolved = std::collections::BTreeMap::<String, PublishedOperationRecord>::new();
        let mut binding_records = Vec::new();
        let scopes = request.scopes()?;
        for scope in scopes {
            for binding in self.list_capsule_bindings(scope)? {
                match &binding.target {
                    CapsuleBindingTarget::Version { artifact_hash } => {
                        let record =
                            self.load_version_record(&binding.operation_name, artifact_hash)?;
                        resolved.insert(binding.operation_name.clone(), record);
                    }
                    CapsuleBindingTarget::Tombstone => {
                        resolved.remove(&binding.operation_name);
                    }
                }
                binding_records.push(binding);
            }
        }

        if request.load_all_active_when_unbound
            && resolved.is_empty()
            && binding_records.is_empty()
            && request.active_operation_names.is_empty()
        {
            for record in self.list_records()? {
                resolved.insert(record.name.clone(), record);
            }
        }

        for operation_name in request.active_operation_names {
            let record = self.load_record(&operation_name)?;
            resolved.insert(operation_name, record);
        }

        Ok(CapsuleBindingSnapshot {
            records: resolved.into_values().collect(),
            bindings: binding_records,
        })
    }

    pub async fn load_runtime_config_for_record(
        &self,
        record: &PublishedOperationRecord,
    ) -> crate::VerletResult<verlet_wasm::WasmRuntimeConfig> {
        let config = self.load_runtime_config_for_published_record(record)?;
        let manifest = verlet_wasm::runner::WasmRuntimeFactory::new(config.clone())?
            .validate_operation_artifact()
            .await?;
        if manifest != record.manifest {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "operation {:?} manifest mismatch for artifact {}",
                record.name, record.active_artifact_hash
            )));
        }
        Ok(config)
    }

    pub fn load_runtime_config_for_published_record(
        &self,
        record: &PublishedOperationRecord,
    ) -> crate::VerletResult<verlet_wasm::WasmRuntimeConfig> {
        record.validate()?;
        if matches!(record.source, PublishedOperationSource::Kernel { .. }) {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "operation {:?} is kernel-native and can only run through the in-process kernel dispatcher",
                record.name
            )));
        }
        let bytes = self
            .blobs
            .get(&record.active_artifact_hash)?
            .ok_or_else(|| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "operation blob {} for {:?} was not found",
                    record.active_artifact_hash, record.name
                ))
            })?;
        Ok(
            verlet_wasm::WasmRuntimeConfig::new(verlet_wasm::WasmRuntimeArtifact::bytes(bytes))
                .with_capability_grants(record.capability_grants.clone()),
        )
    }

    pub fn record_path(&self, name: &str) -> crate::VerletResult<std::path::PathBuf> {
        let name = validate_record_name(name)?;
        Ok(self.root.join("records").join(format!("{name}.json")))
    }

    pub fn version_record_path(
        &self,
        name: &str,
        artifact_hash: &str,
    ) -> crate::VerletResult<std::path::PathBuf> {
        let name = validate_record_name(name)?;
        validate_hash(artifact_hash)?;
        Ok(self
            .root
            .join("versions")
            .join(name)
            .join(format!("{artifact_hash}.json")))
    }

    pub fn binding_record_path(
        &self,
        scope: &CapsuleBindingScope,
        operation_name: &str,
    ) -> crate::VerletResult<std::path::PathBuf> {
        let operation_name = validate_record_name(operation_name)?;
        Ok(self
            .binding_scope_dir(scope)?
            .join(format!("{operation_name}.json")))
    }

    fn write_record_atomically(
        &self,
        record: &PublishedOperationRecord,
    ) -> crate::VerletResult<()> {
        let path = self.record_path(&record.name)?;
        write_json_atomically(&path, format!("operation record {:?}", record.name), record)
    }

    fn write_version_record_atomically(
        &self,
        record: &PublishedOperationRecord,
    ) -> crate::VerletResult<()> {
        record.validate()?;
        let path = self.version_record_path(&record.name, &record.active_artifact_hash)?;
        if path.exists() {
            self.load_version_record(&record.name, &record.active_artifact_hash)?;
            return Ok(());
        }
        write_json_atomically(
            &path,
            format!(
                "operation version record {:?}@{}",
                record.name, record.active_artifact_hash
            ),
            record,
        )
    }

    fn write_binding_record_atomically(
        &self,
        record: &CapsuleBindingRecord,
    ) -> crate::VerletResult<()> {
        record.validate()?;
        let path = self.binding_record_path(&record.scope, &record.operation_name)?;
        write_json_atomically(
            &path,
            format!(
                "capsule binding record {:?} in {:?}",
                record.operation_name, record.scope
            ),
            record,
        )
    }

    fn binding_scope_dir(
        &self,
        scope: &CapsuleBindingScope,
    ) -> crate::VerletResult<std::path::PathBuf> {
        scope.validate()?;
        Ok(match scope {
            CapsuleBindingScope::Global => self.root.join("bindings").join("global"),
            CapsuleBindingScope::Tenant { tenant_id } => self
                .root
                .join("bindings")
                .join("tenant")
                .join(validate_scope_segment("tenant_id", tenant_id)?),
            CapsuleBindingScope::Thread {
                tenant_id,
                thread_id,
            } => self
                .root
                .join("bindings")
                .join("thread")
                .join(validate_scope_segment("tenant_id", tenant_id)?)
                .join(validate_scope_segment("thread_id", thread_id)?),
        })
    }
}

struct TempFileCleanup {
    path: std::path::PathBuf,
}

impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(crate) fn write_json_atomically<T: serde::Serialize>(
    path: &std::path::Path,
    label: String,
    value: &T,
) -> crate::VerletResult<()> {
    let Some(parent) = path.parent() else {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "{label} path {} has no parent directory",
            path.display()
        )));
    };
    std::fs::create_dir_all(parent).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to create {label} directory {}: {err}",
            parent.display()
        ))
    })?;
    let tmp_path = parent.join(format!(".verlet.tmp.{}", uuid::Uuid::now_v7()));
    let cleanup = TempFileCleanup {
        path: tmp_path.clone(),
    };
    let bytes = serde_json::to_vec_pretty(value).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!("failed to encode {label}: {err}"))
    })?;
    {
        let mut file = std::fs::File::create(&tmp_path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to create temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.write_all(&bytes).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to write temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
        file.sync_all().map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to sync temp {label} {}: {err}",
                tmp_path.display()
            ))
        })?;
    }
    std::fs::rename(&cleanup.path, &path).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to atomically install {label} {}: {err}",
            path.display()
        ))
    })
}

#[derive(Clone, Debug)]
pub struct PublishOperationRequest {
    pub name: String,
    pub artifact_path: std::path::PathBuf,
    pub source: PublishedOperationSource,
    pub interface: Option<crate::tool_package::ToolInterfaceContract>,
    pub capability_grants: std::collections::BTreeSet<String>,
    pub metadata: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug)]
pub struct PublishInterfaceOperationRequest {
    pub name: String,
    pub source: PublishedOperationSource,
    pub manifest: verlet_abi::WasmOperationManifest,
    pub interface: crate::tool_package::ToolInterfaceContract,
    pub capability_grants: std::collections::BTreeSet<String>,
    pub metadata: std::collections::BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub enum PublishedOperationSource {
    Rust {
        module_path: std::path::PathBuf,
        release: bool,
    },
    Wasm {
        bin_path: std::path::PathBuf,
    },
    Import {
        manifest_path: std::path::PathBuf,
        spec_sha256: String,
    },
    Kernel {
        package: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PublishedOperationBuild {
    pub artifact_path: std::path::PathBuf,
    pub published_at_ms: u64,
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PublishedOperationRecord {
    pub schema_version: u32,
    pub name: String,
    pub active_artifact_hash: String,
    pub manifest: verlet_abi::WasmOperationManifest,
    pub projections: crate::OperationProjectionSet,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interface: Option<crate::tool_package::ToolInterfaceContract>,
    pub capability_grants: std::collections::BTreeSet<String>,
    pub metadata: std::collections::BTreeMap<String, serde_json::Value>,
    pub source: PublishedOperationSource,
    pub build: PublishedOperationBuild,
}

impl PublishedOperationRecord {
    pub fn validate(&self) -> crate::VerletResult<()> {
        if self.schema_version != RECORD_SCHEMA_VERSION {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "unsupported operation record schema version {}",
                self.schema_version
            )));
        }
        let name = validate_record_name(&self.name)?;
        if name != self.name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "operation record name {:?} did not normalize to itself",
                self.name
            )));
        }
        validate_hash(&self.active_artifact_hash)?;
        if let PublishedOperationSource::Import {
            manifest_path,
            spec_sha256,
        } = &self.source
        {
            if manifest_path.as_os_str().is_empty() {
                return Err(crate::VerletOperationsError::RuntimeFactory(
                    "operation import manifest path cannot be empty".to_string(),
                ));
            }
            validate_sha256("operation import spec sha256", spec_sha256)?;
        }
        validate_manifest_shape(&self.manifest)?;
        validate_required_grants(&self.name, &self.manifest, &self.capability_grants)?;
        let registered = crate::RegisteredOperation {
            name: self.name.clone(),
            manifest: self.manifest.clone(),
            capability_grants: self.capability_grants.clone(),
            metadata: self.metadata.clone(),
        };
        let expected = registered
            .projections()
            .with_tool_interface(self.interface.as_ref());
        if self.projections != expected {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "operation record {:?} projections are stale",
                self.name
            )));
        }
        if let Some(interface) = &self.interface {
            interface.validate_against_operation_record(
                &self.name,
                &self.manifest,
                &self.projections,
            )?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum CapsuleBindingScope {
    Global,
    Tenant {
        #[serde(rename = "tenantId", alias = "tenant_id")]
        tenant_id: String,
    },
    Thread {
        #[serde(rename = "tenantId", alias = "tenant_id")]
        tenant_id: String,
        #[serde(rename = "threadId", alias = "thread_id")]
        thread_id: String,
    },
}

impl CapsuleBindingScope {
    pub fn global() -> Self {
        Self::Global
    }

    pub fn tenant(tenant_id: impl Into<String>) -> Self {
        Self::Tenant {
            tenant_id: tenant_id.into(),
        }
    }

    pub fn thread(tenant_id: impl Into<String>, thread_id: impl Into<String>) -> Self {
        Self::Thread {
            tenant_id: tenant_id.into(),
            thread_id: thread_id.into(),
        }
    }

    fn validate(&self) -> crate::VerletResult<()> {
        match self {
            Self::Global => Ok(()),
            Self::Tenant { tenant_id } => {
                validate_scope_segment("tenant_id", tenant_id)?;
                Ok(())
            }
            Self::Thread {
                tenant_id,
                thread_id,
            } => {
                validate_scope_segment("tenant_id", tenant_id)?;
                validate_scope_segment("thread_id", thread_id)?;
                Ok(())
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum CapsuleBindingTarget {
    Version {
        #[serde(rename = "artifactHash", alias = "artifact_hash")]
        artifact_hash: String,
    },
    Tombstone,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapsuleBindingRecord {
    pub schema_version: u32,
    pub scope: CapsuleBindingScope,
    pub operation_name: String,
    pub target: CapsuleBindingTarget,
    pub updated_at_ms: u64,
}

impl CapsuleBindingRecord {
    pub fn validate(&self) -> crate::VerletResult<()> {
        if self.schema_version != BINDING_SCHEMA_VERSION {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "unsupported capsule binding schema version {}",
                self.schema_version
            )));
        }
        self.scope.validate()?;
        let operation_name = validate_record_name(&self.operation_name)?;
        if operation_name != self.operation_name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "capsule binding operation name {:?} did not normalize to itself",
                self.operation_name
            )));
        }
        match &self.target {
            CapsuleBindingTarget::Version { artifact_hash } => validate_hash(artifact_hash)?,
            CapsuleBindingTarget::Tombstone => {}
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapsuleBindingResolutionRequest {
    pub tenant_id: String,
    pub thread_id: Option<String>,
    pub active_operation_names: std::collections::BTreeSet<String>,
    pub load_all_active_when_unbound: bool,
}

impl CapsuleBindingResolutionRequest {
    pub fn for_tenant(tenant_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            thread_id: None,
            active_operation_names: std::collections::BTreeSet::new(),
            load_all_active_when_unbound: false,
        }
    }

    pub fn for_thread(tenant_id: impl Into<String>, thread_id: impl Into<String>) -> Self {
        Self {
            tenant_id: tenant_id.into(),
            thread_id: Some(thread_id.into()),
            active_operation_names: std::collections::BTreeSet::new(),
            load_all_active_when_unbound: false,
        }
    }

    pub fn with_active_operation_name(mut self, name: impl Into<String>) -> Self {
        self.active_operation_names.insert(name.into());
        self
    }

    pub fn with_active_operation_names<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.active_operation_names
            .extend(names.into_iter().map(Into::into));
        self
    }

    pub fn load_all_active_when_unbound(mut self, value: bool) -> Self {
        self.load_all_active_when_unbound = value;
        self
    }

    fn scopes(&self) -> crate::VerletResult<Vec<CapsuleBindingScope>> {
        validate_scope_segment("tenant_id", &self.tenant_id)?;
        let mut scopes = vec![
            CapsuleBindingScope::Global,
            CapsuleBindingScope::tenant(self.tenant_id.clone()),
        ];
        if let Some(thread_id) = &self.thread_id {
            validate_scope_segment("thread_id", thread_id)?;
            scopes.push(CapsuleBindingScope::thread(
                self.tenant_id.clone(),
                thread_id.clone(),
            ));
        }
        Ok(scopes)
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CapsuleBindingSnapshot {
    pub records: Vec<PublishedOperationRecord>,
    pub bindings: Vec<CapsuleBindingRecord>,
}

impl CapsuleBindingSnapshot {
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn operation_names(&self) -> Vec<String> {
        self.records
            .iter()
            .map(|record| record.name.clone())
            .collect()
    }
}

pub fn validate_record_name(name: &str) -> crate::VerletResult<String> {
    let name = name.trim();
    if name.is_empty() {
        return Err(crate::VerletOperationsError::RuntimeFactory(
            "operation record name cannot be empty".to_string(),
        ));
    }
    if name == "." || name == ".." || name.len() > 128 {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "operation record name {name:?} is not path-safe"
        )));
    }
    if !name
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "operation record name {name:?} must use ASCII letters, numbers, '.', '_' or '-'"
        )));
    }
    Ok(name.to_string())
}

fn validate_scope_segment(label: &str, value: &str) -> crate::VerletResult<String> {
    let value = value.trim();
    if value.is_empty() {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "capsule binding scope {label} cannot be empty"
        )));
    }
    if value == "." || value == ".." || value.len() > 128 {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "capsule binding scope {label} {value:?} is not path-safe"
        )));
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "capsule binding scope {label} {value:?} must use ASCII letters, numbers, '.', '_' or '-'"
        )));
    }
    Ok(value.to_string())
}

pub fn wasm_sha256(bytes: &[u8]) -> String {
    let digest = sha2::Sha256::digest(bytes);
    let mut out = String::with_capacity(64);
    for byte in digest {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn validate_hash(hash: &str) -> crate::VerletResult<()> {
    validate_sha256("operation artifact hash", hash)
}

fn validate_sha256(label: &str, hash: &str) -> crate::VerletResult<()> {
    if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "{label} {hash:?} is not a sha256 hex digest"
        )))
    }
}

fn validate_required_grants(
    name: &str,
    manifest: &verlet_abi::WasmOperationManifest,
    grants: &std::collections::BTreeSet<String>,
) -> crate::VerletResult<()> {
    let missing: Vec<_> = manifest
        .operations
        .iter()
        .flat_map(|operation| {
            operation
                .required_capabilities
                .iter()
                .filter(|capability| !grants.contains(capability.as_str()))
                .map(|capability| format!("{}:{capability}", operation.name))
        })
        .collect();
    if missing.is_empty() {
        Ok(())
    } else {
        Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "operation publish {name:?} requires ungranted capabilities: {}",
            missing.join(", ")
        )))
    }
}

fn validate_manifest_shape(
    manifest: &verlet_abi::WasmOperationManifest,
) -> crate::VerletResult<()> {
    if manifest.abi != "cooldis.operation/0.1" {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "unsupported operation record manifest abi {:?}",
            manifest.abi
        )));
    }
    if manifest.operations.is_empty() {
        return Err(crate::VerletOperationsError::RuntimeFactory(
            "operation record manifest has no operations".to_string(),
        ));
    }
    let mut ids = std::collections::BTreeSet::new();
    let mut names = std::collections::BTreeSet::new();
    for operation in &manifest.operations {
        if operation.id == 0 {
            return Err(crate::VerletOperationsError::RuntimeFactory(
                "operation record manifest uses reserved operation id 0".to_string(),
            ));
        }
        if operation.name.trim().is_empty() {
            return Err(crate::VerletOperationsError::RuntimeFactory(
                "operation record manifest has an empty operation name".to_string(),
            ));
        }
        if !ids.insert(operation.id) {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "operation record manifest has duplicate operation id {}",
                operation.id
            )));
        }
        if !names.insert(operation.name.clone()) {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "operation record manifest has duplicate operation name {:?}",
                operation.name
            )));
        }
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests;
