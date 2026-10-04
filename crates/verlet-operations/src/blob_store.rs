use std::io::Write as _;

const BLOB_RECORD_SCHEMA_VERSION: u32 = 1;
const BLOB_RECORD_KIND: &str = "cooldis.blob";
const BLOB_REF_PREFIX: &str = "resource://artifact/sha256:";

#[derive(Clone, Debug)]
pub struct LocalBlobRegistry {
    root: std::path::PathBuf,
    artifacts: BlobArtifactStore,
}

impl LocalBlobRegistry {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        let root = root.into();
        Self {
            artifacts: BlobArtifactStore::new(root.join("artifacts")),
            root,
        }
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn publish_file(
        &self,
        path: impl AsRef<std::path::Path>,
        name: Option<&str>,
    ) -> crate::VerletResult<PublishedBlobRecord> {
        let path = path.as_ref();
        let bytes = std::fs::read(path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read blob source {}: {err}",
                path.display()
            ))
        })?;
        self.publish_bytes(bytes, name, Some(path.to_path_buf()))
    }

    pub fn publish_bytes(
        &self,
        bytes: Vec<u8>,
        name: Option<&str>,
        source_path: Option<std::path::PathBuf>,
    ) -> crate::VerletResult<PublishedBlobRecord> {
        let name = name
            .map(crate::operation_store::validate_record_name)
            .transpose()?;
        let hash = self.artifacts.put(&bytes)?;
        let record = PublishedBlobRecord {
            schema_version: BLOB_RECORD_SCHEMA_VERSION,
            kind: BLOB_RECORD_KIND.to_string(),
            name,
            ref_uri: blob_ref_uri(&hash),
            artifact_hash: hash.clone(),
            content_sha256: format!("sha256:{hash}"),
            size_bytes: bytes.len() as u64,
            source_path,
            published_at_ms: now_ms(),
        };
        record.validate()?;
        self.write_version_record_atomically(&record)?;
        if record.name.is_some() {
            self.write_named_record_atomically(&record)?;
        }
        Ok(record)
    }

    pub fn load_ref(&self, ref_uri: &str) -> crate::VerletResult<PublishedBlobRecord> {
        let hash = blob_hash_from_ref(ref_uri)?;
        let path = self.version_record_path(&hash)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read blob record {}: {err}",
                path.display()
            ))
        })?;
        let record: PublishedBlobRecord = serde_json::from_slice(&bytes).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to decode blob record {}: {err}",
                path.display()
            ))
        })?;
        record.validate()?;
        if record.artifact_hash != hash {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "blob record {} uses artifact hash {}, expected {}",
                path.display(),
                record.artifact_hash,
                hash
            )));
        }
        Ok(record)
    }

    pub fn load_text_ref(
        &self,
        ref_uri: &str,
    ) -> crate::VerletResult<(PublishedBlobRecord, String)> {
        let record = self.load_ref(ref_uri)?;
        let bytes = self.artifacts.get(&record.artifact_hash)?.ok_or_else(|| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "blob artifact {:?} is missing from {}; run `verlet blob publish <file>` to publish it",
                record.ref_uri,
                self.root.display()
            ))
        })?;
        let text = String::from_utf8(bytes).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "blob artifact {:?} is not valid UTF-8 text: {err}",
                record.ref_uri
            ))
        })?;
        Ok((record, text))
    }

    pub fn version_record_path(
        &self,
        artifact_hash: &str,
    ) -> crate::VerletResult<std::path::PathBuf> {
        validate_blob_hash(artifact_hash)?;
        Ok(self
            .root
            .join("records")
            .join("artifact")
            .join(format!("sha256-{artifact_hash}.json")))
    }

    pub fn named_record_path(&self, name: &str) -> crate::VerletResult<std::path::PathBuf> {
        let name = crate::operation_store::validate_record_name(name)?;
        Ok(self.root.join("names").join(format!("{name}.json")))
    }

    fn write_version_record_atomically(
        &self,
        record: &PublishedBlobRecord,
    ) -> crate::VerletResult<()> {
        let path = self.version_record_path(&record.artifact_hash)?;
        if path.exists() {
            self.load_ref(&record.ref_uri)?;
            return Ok(());
        }
        write_json_atomically(
            &path,
            format!("blob record {}", record.content_sha256),
            record,
        )
    }

    fn write_named_record_atomically(
        &self,
        record: &PublishedBlobRecord,
    ) -> crate::VerletResult<()> {
        let Some(name) = record.name.as_deref() else {
            return Ok(());
        };
        write_json_atomically(
            &self.named_record_path(name)?,
            format!("blob name record {name:?}"),
            record,
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct PublishedBlobRecord {
    pub schema_version: u32,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub ref_uri: String,
    pub artifact_hash: String,
    pub content_sha256: String,
    pub size_bytes: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_path: Option<std::path::PathBuf>,
    pub published_at_ms: u64,
}

impl PublishedBlobRecord {
    pub fn validate(&self) -> crate::VerletResult<()> {
        if self.schema_version != BLOB_RECORD_SCHEMA_VERSION {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "unsupported blob record schema_version {}",
                self.schema_version
            )));
        }
        if self.kind != BLOB_RECORD_KIND {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "blob record kind must be {BLOB_RECORD_KIND:?}, got {:?}",
                self.kind
            )));
        }
        if let Some(name) = &self.name {
            crate::operation_store::validate_record_name(name)?;
        }
        validate_blob_hash(&self.artifact_hash)?;
        let expected_ref = blob_ref_uri(&self.artifact_hash);
        if self.ref_uri != expected_ref {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "blob record ref_uri {:?} does not match expected {:?}",
                self.ref_uri, expected_ref
            )));
        }
        let expected_sha256 = format!("sha256:{}", self.artifact_hash);
        if self.content_sha256 != expected_sha256 {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "blob record content_sha256 {:?} does not match expected {:?}",
                self.content_sha256, expected_sha256
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct BlobArtifactStore {
    root: std::path::PathBuf,
}

impl BlobArtifactStore {
    fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn put(&self, bytes: &[u8]) -> crate::VerletResult<String> {
        let hash = crate::operation_store::wasm_sha256(bytes);
        let path = self.artifact_path(&hash)?;
        if path.exists() {
            let existing = std::fs::read(&path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read existing blob artifact {}: {err}",
                    path.display()
                ))
            })?;
            if crate::operation_store::wasm_sha256(&existing) == hash {
                return Ok(hash);
            }
            std::fs::remove_file(&path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to replace corrupt blob artifact {}: {err}",
                    path.display()
                ))
            })?;
        }
        let Some(parent) = path.parent() else {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "blob artifact path {} has no parent directory",
                path.display()
            )));
        };
        std::fs::create_dir_all(parent).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to create blob artifact directory {}: {err}",
                parent.display()
            ))
        })?;
        let tmp_path = parent.join(format!(".{hash}.tmp.{}", uuid::Uuid::now_v7()));
        {
            let mut file = std::fs::File::create(&tmp_path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to create temp blob artifact {}: {err}",
                    tmp_path.display()
                ))
            })?;
            file.write_all(bytes).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to write temp blob artifact {}: {err}",
                    tmp_path.display()
                ))
            })?;
            file.sync_all().map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to sync temp blob artifact {}: {err}",
                    tmp_path.display()
                ))
            })?;
        }
        crate::commit_content_addressed_artifact(&tmp_path, &path, "blob artifact")?;
        Ok(hash)
    }

    fn get(&self, hash: &str) -> crate::VerletResult<Option<Vec<u8>>> {
        validate_blob_hash(hash)?;
        let path = self.artifact_path(hash)?;
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read blob artifact {}: {err}",
                path.display()
            ))
        })?;
        let actual = crate::operation_store::wasm_sha256(&bytes);
        if actual != hash {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "blob artifact {} hash mismatch: expected {hash}, got {actual}",
                path.display()
            )));
        }
        Ok(Some(bytes))
    }

    fn artifact_path(&self, hash: &str) -> crate::VerletResult<std::path::PathBuf> {
        validate_blob_hash(hash)?;
        Ok(self.root.join(&hash[..2]).join(format!("{hash}.blob")))
    }
}

pub fn blob_ref_uri(hash: &str) -> String {
    format!("{BLOB_REF_PREFIX}{hash}")
}

pub fn blob_hash_from_ref(ref_uri: &str) -> crate::VerletResult<String> {
    let hash = ref_uri.strip_prefix(BLOB_REF_PREFIX).ok_or_else(|| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "blob resource ref {ref_uri:?} must be resource://artifact/sha256:<hash>"
        ))
    })?;
    validate_blob_hash(hash)?;
    Ok(hash.to_string())
}

fn validate_blob_hash(hash: &str) -> crate::VerletResult<()> {
    if hash.len() != 64 || !hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "blob artifact hash {hash:?} must be 64 hex characters"
        )));
    }
    Ok(())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn write_json_atomically<T: serde::Serialize>(
    path: &std::path::Path,
    label: impl AsRef<str>,
    value: &T,
) -> crate::VerletResult<()> {
    let label = label.as_ref();
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
    let tmp_path = parent.join(format!(".tmp-{}.json", uuid::Uuid::now_v7()));
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
        file.write_all(b"\n").map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to finish temp {label} {}: {err}",
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
    std::fs::rename(&tmp_path, path).map_err(|err| {
        let _ = std::fs::remove_file(&tmp_path);
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to install {label} {}: {err}",
            path.display()
        ))
    })
}
