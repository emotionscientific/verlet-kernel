use sha2::Digest as _;
use std::io::Write as _;

const SKILL_PACKAGE_SCHEMA_VERSION: u32 = 1;
const SKILL_RECORD_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug)]
pub struct LocalSkillRegistry {
    root: std::path::PathBuf,
    blobs: SkillPackageBlobStore,
}

impl LocalSkillRegistry {
    pub fn new(root: impl Into<std::path::PathBuf>) -> Self {
        let root = root.into();
        Self {
            blobs: SkillPackageBlobStore::new(root.join("blobs")),
            root,
        }
    }

    pub fn root(&self) -> &std::path::Path {
        &self.root
    }

    pub fn publish_directory(
        &self,
        request: PublishSkillPackageRequest,
    ) -> crate::VerletResult<PublishedSkillPackageRecord> {
        let package = SkillPackage::from_directory(&request.package_dir, request.name.as_deref())?;
        self.publish_package(package)
    }

    pub fn publish_package(
        &self,
        package: SkillPackage,
    ) -> crate::VerletResult<PublishedSkillPackageRecord> {
        let bytes = package.to_artifact_bytes()?;
        let hash = self.blobs.put(&bytes)?;
        let record = PublishedSkillPackageRecord {
            schema_version: SKILL_RECORD_SCHEMA_VERSION,
            name: package.name.clone(),
            active_artifact_hash: hash,
            package,
        };
        record.validate()?;
        self.write_version_record_atomically(&record)?;
        self.write_record_atomically(&record)?;
        Ok(record)
    }

    pub fn load_record(&self, name: &str) -> crate::VerletResult<PublishedSkillPackageRecord> {
        let name = crate::operation_store::validate_record_name(name)?;
        let path = self.record_path(&name)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read skill package record {}: {err}",
                path.display()
            ))
        })?;
        let record: PublishedSkillPackageRecord =
            serde_json::from_slice(&bytes).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to decode skill package record {}: {err}",
                    path.display()
                ))
            })?;
        record.validate()?;
        if record.name != name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package record {} names {:?}, expected {:?}",
                path.display(),
                record.name,
                name
            )));
        }
        Ok(record)
    }

    pub fn load_version_record(
        &self,
        name: &str,
        artifact_hash: &str,
    ) -> crate::VerletResult<PublishedSkillPackageRecord> {
        let name = crate::operation_store::validate_record_name(name)?;
        validate_skill_hash(artifact_hash)?;
        let path = self.version_record_path(&name, artifact_hash)?;
        let bytes = std::fs::read(&path).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read skill package version record {}: {err}",
                path.display()
            ))
        })?;
        let record: PublishedSkillPackageRecord =
            serde_json::from_slice(&bytes).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to decode skill package version record {}: {err}",
                    path.display()
                ))
            })?;
        record.validate()?;
        if record.name != name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package version record {} names {:?}, expected {:?}",
                path.display(),
                record.name,
                name
            )));
        }
        if record.active_artifact_hash != artifact_hash {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package version record {} uses artifact hash {}, expected {}",
                path.display(),
                record.active_artifact_hash,
                artifact_hash
            )));
        }
        Ok(record)
    }

    pub fn record_path(&self, name: &str) -> crate::VerletResult<std::path::PathBuf> {
        let name = crate::operation_store::validate_record_name(name)?;
        Ok(self.root.join("records").join(format!("{name}.json")))
    }

    pub fn version_record_path(
        &self,
        name: &str,
        artifact_hash: &str,
    ) -> crate::VerletResult<std::path::PathBuf> {
        let name = crate::operation_store::validate_record_name(name)?;
        validate_skill_hash(artifact_hash)?;
        Ok(self
            .root
            .join("versions")
            .join(name)
            .join(format!("{artifact_hash}.json")))
    }

    fn write_record_atomically(
        &self,
        record: &PublishedSkillPackageRecord,
    ) -> crate::VerletResult<()> {
        let path = self.record_path(&record.name)?;
        write_json_atomically(
            &path,
            format!("skill package record {:?}", record.name),
            record,
        )
    }

    fn write_version_record_atomically(
        &self,
        record: &PublishedSkillPackageRecord,
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
                "skill package version record {:?}@{}",
                record.name, record.active_artifact_hash
            ),
            record,
        )
    }
}

#[derive(Clone, Debug)]
pub struct PublishSkillPackageRequest {
    pub package_dir: std::path::PathBuf,
    pub name: Option<String>,
}

#[derive(Clone, Debug)]
struct SkillPackageBlobStore {
    root: std::path::PathBuf,
}

impl SkillPackageBlobStore {
    fn new(root: impl Into<std::path::PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn put(&self, bytes: &[u8]) -> crate::VerletResult<String> {
        let hash = crate::operation_store::wasm_sha256(bytes);
        let path = self.artifact_path(&hash)?;
        if path.exists() {
            let existing = std::fs::read(&path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read existing skill package blob {}: {err}",
                    path.display()
                ))
            })?;
            if crate::operation_store::wasm_sha256(&existing) == hash {
                return Ok(hash);
            }
            std::fs::remove_file(&path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to replace corrupt skill package blob {}: {err}",
                    path.display()
                ))
            })?;
        }
        let Some(parent) = path.parent() else {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package blob path {} has no parent directory",
                path.display()
            )));
        };
        std::fs::create_dir_all(parent).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to create skill package blob directory {}: {err}",
                parent.display()
            ))
        })?;
        let tmp_path = parent.join(format!(".{hash}.tmp.{}", uuid::Uuid::now_v7()));
        {
            let mut file = std::fs::File::create(&tmp_path).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to create temp skill package blob {}: {err}",
                    tmp_path.display()
                ))
            })?;
            file.write_all(bytes).map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to write temp skill package blob {}: {err}",
                    tmp_path.display()
                ))
            })?;
            file.sync_all().map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to sync temp skill package blob {}: {err}",
                    tmp_path.display()
                ))
            })?;
        }
        crate::commit_content_addressed_artifact(&tmp_path, &path, "skill package blob")?;
        Ok(hash)
    }

    fn artifact_path(&self, hash: &str) -> crate::VerletResult<std::path::PathBuf> {
        validate_skill_hash(hash)?;
        Ok(self
            .root
            .join(&hash[..2])
            .join(format!("{hash}.skills.json")))
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct PublishedSkillPackageRecord {
    pub schema_version: u32,
    pub name: String,
    pub active_artifact_hash: String,
    pub package: SkillPackage,
}

impl PublishedSkillPackageRecord {
    pub fn ref_uri(&self) -> String {
        format!("skill://{}@sha256:{}", self.name, self.active_artifact_hash)
    }

    pub fn validate(&self) -> crate::VerletResult<()> {
        if self.schema_version != SKILL_RECORD_SCHEMA_VERSION {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "unsupported skill package record schema version {}",
                self.schema_version
            )));
        }
        let name = crate::operation_store::validate_record_name(&self.name)?;
        if name != self.name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package record name {:?} did not normalize to itself",
                self.name
            )));
        }
        validate_skill_hash(&self.active_artifact_hash)?;
        self.package.validate()?;
        if self.package.name != self.name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package record name {:?} does not match package name {:?}",
                self.name, self.package.name
            )));
        }
        let bytes = self.package.to_artifact_bytes()?;
        let expected = crate::operation_store::wasm_sha256(&bytes);
        if expected != self.active_artifact_hash {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package record {:?} artifact hash mismatch: expected {}, got {}",
                self.name, expected, self.active_artifact_hash
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SkillPackage {
    pub schema_version: u32,
    pub name: String,
    pub skills: Vec<SkillPackageEntry>,
}

impl SkillPackage {
    pub(crate) fn from_entries(
        name: &str,
        mut skills: Vec<SkillPackageEntry>,
    ) -> crate::VerletResult<Self> {
        skills.sort_by(|left, right| left.name.cmp(&right.name));
        let package = Self {
            schema_version: SKILL_PACKAGE_SCHEMA_VERSION,
            name: crate::operation_store::validate_record_name(name)?,
            skills,
        };
        package.validate()?;
        Ok(package)
    }

    pub fn from_directory(
        package_dir: &std::path::Path,
        name: Option<&str>,
    ) -> crate::VerletResult<Self> {
        let metadata = std::fs::metadata(package_dir).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read skill package directory {}: {err}",
                package_dir.display()
            ))
        })?;
        if !metadata.is_dir() {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill publish input {} is not a directory",
                package_dir.display()
            )));
        }
        let package_name = match name {
            Some(name) => crate::operation_store::validate_record_name(name)?,
            None => {
                let inferred = package_dir
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        crate::VerletOperationsError::RuntimeFactory(format!(
                            "skill package directory {} has no package name; pass --name",
                            package_dir.display()
                        ))
                    })?;
                crate::operation_store::validate_record_name(inferred)?
            }
        };
        let mut skill_dirs = Vec::new();
        for entry in std::fs::read_dir(package_dir).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read skill package directory {}: {err}",
                package_dir.display()
            ))
        })? {
            let entry = entry.map_err(|err| {
                crate::VerletOperationsError::RuntimeFactory(format!(
                    "failed to read skill package directory entry in {}: {err}",
                    package_dir.display()
                ))
            })?;
            let path = entry.path();
            if path.is_dir() && path.join("SKILL.md").is_file() {
                skill_dirs.push(path);
            }
        }
        skill_dirs.sort_by(|left, right| {
            left.file_name()
                .and_then(|name| name.to_str())
                .cmp(&right.file_name().and_then(|name| name.to_str()))
        });
        let mut skills = Vec::new();
        for skill_dir in skill_dirs {
            skills.push(SkillPackageEntry::from_skill_dir(&skill_dir)?);
        }
        Self::from_entries(&package_name, skills)
    }

    pub fn to_artifact_bytes(&self) -> crate::VerletResult<Vec<u8>> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to encode skill package artifact {:?}: {err}",
                self.name
            ))
        })
    }

    pub fn render_index(&self) -> String {
        let mut out = String::new();
        for skill in &self.skills {
            out.push_str(&skill.name);
            out.push_str(" — ");
            out.push_str(&skill.description);
            out.push('\n');
        }
        out
    }

    pub fn validate(&self) -> crate::VerletResult<()> {
        if self.schema_version != SKILL_PACKAGE_SCHEMA_VERSION {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "unsupported skill package schema version {}",
                self.schema_version
            )));
        }
        let name = crate::operation_store::validate_record_name(&self.name)?;
        if name != self.name {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package name {:?} did not normalize to itself",
                self.name
            )));
        }
        if self.skills.is_empty() {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package {:?} contains no <name>/SKILL.md entries",
                self.name
            )));
        }
        let mut names = std::collections::BTreeSet::new();
        let mut previous = None;
        for skill in &self.skills {
            skill.validate()?;
            if !names.insert(skill.name.clone()) {
                return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "skill package {:?} contains duplicate skill name {:?}",
                    self.name, skill.name
                )));
            }
            if let Some(previous) = previous.replace(skill.name.clone())
                && previous > skill.name
            {
                return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "skill package {:?} skills are not sorted by name",
                    self.name
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct SkillPackageEntry {
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trigger_hint: Option<String>,
    pub body_sha256: String,
    pub body: String,
}

impl SkillPackageEntry {
    fn from_skill_dir(skill_dir: &std::path::Path) -> crate::VerletResult<Self> {
        let file = skill_dir.join("SKILL.md");
        let body = std::fs::read_to_string(&file).map_err(|err| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "failed to read skill file {}: {err}",
                file.display()
            ))
        })?;
        Self::from_skill_body(skill_dir, body)
    }

    /// Parse already-read `SKILL.md` contents without reopening the path.
    /// Host binders use this after they have confined and pinned the file.
    pub fn from_skill_body(skill_dir: &std::path::Path, body: String) -> crate::VerletResult<Self> {
        let file = skill_dir.join("SKILL.md");
        let dirname = skill_dir.file_name().and_then(|name| name.to_str());
        let metadata = parse_skill_metadata(&file, dirname, &body)?;
        let entry = Self {
            name: metadata.name,
            description: metadata.description,
            trigger_hint: metadata.trigger_hint,
            body_sha256: sha256_hex(body.as_bytes()),
            body,
        };
        entry.validate()?;
        Ok(entry)
    }

    pub fn validate(&self) -> crate::VerletResult<()> {
        if self.name.trim().is_empty() {
            return Err(crate::VerletOperationsError::RuntimeFactory(
                "skill package entry name cannot be empty".to_string(),
            ));
        }
        if self.name.contains('/')
            || self.name.contains('\0')
            || self.name == "."
            || self.name == ".."
        {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package entry name {:?} is not a safe /skills filename",
                self.name
            )));
        }
        if self.description.trim().is_empty() {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package entry {:?} description cannot be empty",
                self.name
            )));
        }
        validate_skill_hash(self.body_sha256.trim_start_matches("sha256:"))?;
        let expected = sha256_hex(self.body.as_bytes());
        if self.body_sha256 != expected {
            return Err(crate::VerletOperationsError::RuntimeFactory(format!(
                "skill package entry {:?} body_sha256 mismatch: expected {}, got {}",
                self.name, expected, self.body_sha256
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SkillPackageRef {
    pub name: String,
    pub artifact_hash: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DeclaredSkillPackageRef {
    Floating { name: String },
    Pinned(SkillPackageRef),
}

impl DeclaredSkillPackageRef {
    pub fn parse(reference: &str) -> crate::VerletResult<Self> {
        let body = reference.strip_prefix("skill://").ok_or_else(|| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "skill ref {reference:?} must start with skill://"
            ))
        })?;
        let Some((name, hash)) = body.split_once("@sha256:") else {
            return Ok(Self::Floating {
                name: crate::operation_store::validate_record_name(body)?,
            });
        };
        let name = crate::operation_store::validate_record_name(name)?;
        validate_skill_hash(hash)?;
        Ok(Self::Pinned(SkillPackageRef {
            name,
            artifact_hash: hash.to_string(),
        }))
    }
}

impl SkillPackageRef {
    pub fn parse(reference: &str) -> crate::VerletResult<Self> {
        match DeclaredSkillPackageRef::parse(reference)? {
            DeclaredSkillPackageRef::Pinned(reference) => Ok(reference),
            DeclaredSkillPackageRef::Floating { .. } => {
                Err(crate::VerletOperationsError::RuntimeFactory(format!(
                    "skill ref {reference:?} must be content-addressed as skill://<package>@sha256:<hash>"
                )))
            }
        }
    }
}

struct ParsedSkillMetadata {
    name: String,
    description: String,
    trigger_hint: Option<String>,
}

fn parse_skill_metadata(
    file: &std::path::Path,
    dirname: Option<&str>,
    body: &str,
) -> crate::VerletResult<ParsedSkillMetadata> {
    if body.trim().is_empty() {
        return Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "skill file {} is empty",
            file.display()
        )));
    }
    if body.starts_with("---\n") || body == "---" {
        return parse_frontmatter(file, dirname, body);
    }
    Ok(ParsedSkillMetadata {
        name: fallback_skill_name(file, dirname)?,
        description: first_non_heading_line(file, body)?,
        trigger_hint: None,
    })
}

fn parse_frontmatter(
    file: &std::path::Path,
    dirname: Option<&str>,
    body: &str,
) -> crate::VerletResult<ParsedSkillMetadata> {
    let rest = body
        .strip_prefix("---\n")
        .ok_or_else(|| malformed_frontmatter(file, "missing frontmatter body"))?;
    let Some((frontmatter, _markdown)) = rest.split_once("\n---") else {
        return Err(malformed_frontmatter(file, "missing closing ---"));
    };
    let mut name = None;
    let mut description = None;
    let mut trigger_hint = None;
    for (index, raw_line) in frontmatter.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            return Err(malformed_frontmatter(
                file,
                &format!("line {} is not key: value", index + 1),
            ));
        };
        let value = parse_frontmatter_value(file, key.trim(), value.trim())?;
        match key.trim() {
            "name" => name = Some(value),
            "description" => description = Some(value),
            "trigger_hint" => trigger_hint = Some(value),
            other => {
                return Err(malformed_frontmatter(
                    file,
                    &format!("unsupported key {other:?}"),
                ));
            }
        }
    }
    let description = description
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            malformed_frontmatter(file, "description is required when frontmatter is present")
        })?;
    Ok(ParsedSkillMetadata {
        name: match name.filter(|value| !value.trim().is_empty()) {
            Some(name) => name,
            None => fallback_skill_name(file, dirname)?,
        },
        description,
        trigger_hint: trigger_hint.filter(|value| !value.trim().is_empty()),
    })
}

fn fallback_skill_name(
    file: &std::path::Path,
    dirname: Option<&str>,
) -> crate::VerletResult<String> {
    dirname.map(str::to_string).ok_or_else(|| {
        let skill_dir = file.parent().unwrap_or(file);
        crate::VerletOperationsError::RuntimeFactory(format!(
            "skill directory {} has no unicode name",
            skill_dir.display()
        ))
    })
}

fn parse_frontmatter_value(
    file: &std::path::Path,
    key: &str,
    raw: &str,
) -> crate::VerletResult<String> {
    if raw.is_empty() {
        return Err(malformed_frontmatter(
            file,
            &format!("key {key:?} has an empty value"),
        ));
    }
    if raw.starts_with('"') || raw.starts_with('\'') {
        let quote = raw.as_bytes()[0] as char;
        if !raw.ends_with(quote) || raw.len() < 2 {
            return Err(malformed_frontmatter(
                file,
                &format!("key {key:?} has an unterminated quoted value"),
            ));
        }
        return Ok(raw[1..raw.len() - 1].to_string());
    }
    Ok(raw.to_string())
}

fn first_non_heading_line(file: &std::path::Path, body: &str) -> crate::VerletResult<String> {
    body.lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with('#'))
        .map(str::to_string)
        .ok_or_else(|| {
            crate::VerletOperationsError::RuntimeFactory(format!(
                "skill file {} has no non-heading description line",
                file.display()
            ))
        })
}

fn malformed_frontmatter(file: &std::path::Path, reason: &str) -> crate::VerletOperationsError {
    crate::VerletOperationsError::RuntimeFactory(format!(
        "malformed frontmatter in {}: {reason}",
        file.display()
    ))
}

fn write_json_atomically<T: serde::Serialize>(
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
    std::fs::rename(&tmp_path, path).map_err(|err| {
        crate::VerletOperationsError::RuntimeFactory(format!(
            "failed to atomically install {label} {}: {err}",
            path.display()
        ))
    })
}

fn validate_skill_hash(hash: &str) -> crate::VerletResult<()> {
    if hash.len() == 64 && hash.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(crate::VerletOperationsError::RuntimeFactory(format!(
            "skill package artifact hash {hash:?} is not a sha256 hex digest"
        )))
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    let digest = sha2::Sha256::digest(bytes);
    format!("sha256:{digest:x}")
}

#[cfg(test)]
mod tests;
