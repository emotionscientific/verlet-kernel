//! Shared surface for the standalone agent file tools.
//!
//! Every tool in `agent-tools/` is a pure function over a filesystem it
//! reaches only through [`ToolFs`]. The embedder picks the backend:
//!
//! - native: [`StdFs`] (feature `std-fs`) over the real filesystem,
//! - kernel: an adapter over the thread's verlet-vfs,
//! - wasm: an adapter over the guest ABI's fs imports.
//!
//! Tool crates must not depend on `std::fs`, tokio, or any host detail.
//! The trait is synchronous on purpose: wasm guests block on host calls,
//! and async hosts wrap tool runs in `spawn_blocking` at the boundary.

/// Hard backstop on a single structured tool result, in bytes. Pi-compatible
/// read, grep, and find output is truncated at its smaller tool-specific
/// limits first. This cap protects the record and wire from unexpectedly large
/// receipts and edit details.
pub const MAX_RESULT_BYTES: usize = 4 * 1024 * 1024;

/// Pi's automatic head-truncation line limit.
pub const DEFAULT_MAX_LINES: usize = 2000;

/// Pi's automatic head-truncation byte limit (50 KiB).
pub const DEFAULT_MAX_BYTES: usize = 50 * 1024;

/// Maximum bytes one bounded file tool reads from a single file (8 MiB).
pub const MAX_FILE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Clone, Copy, Debug, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TruncatedBy {
    Lines,
    Bytes,
}

/// Structured receipt for Pi-compatible complete-line head truncation.
#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TruncationResult {
    pub content: String,
    pub truncated: bool,
    pub truncated_by: Option<TruncatedBy>,
    pub total_lines: usize,
    pub total_bytes: usize,
    pub output_lines: usize,
    pub output_bytes: usize,
    pub last_line_partial: bool,
    pub first_line_exceeds_limit: bool,
    pub max_lines: usize,
    pub max_bytes: usize,
}

/// Port of Pi `core/tools/truncate.ts::truncateHead`.
pub fn truncate_head(content: &str, max_lines: usize, max_bytes: usize) -> TruncationResult {
    let total_bytes = content.len();
    let total_lines = if content.is_empty() {
        0
    } else {
        content
            .split('\n')
            .count()
            .saturating_sub(usize::from(content.ends_with('\n')))
    };

    if total_lines <= max_lines && total_bytes <= max_bytes {
        return TruncationResult {
            content: content.to_owned(),
            truncated: false,
            truncated_by: None,
            total_lines,
            total_bytes,
            output_lines: total_lines,
            output_bytes: total_bytes,
            last_line_partial: false,
            first_line_exceeds_limit: false,
            max_lines,
            max_bytes,
        };
    }

    let first_line = content.split_once('\n').map_or(content, |(line, _)| line);
    if !content.is_empty() && first_line.len() > max_bytes {
        return TruncationResult {
            content: String::new(),
            truncated: true,
            truncated_by: Some(TruncatedBy::Bytes),
            total_lines,
            total_bytes,
            output_lines: 0,
            output_bytes: 0,
            last_line_partial: false,
            first_line_exceeds_limit: true,
            max_lines,
            max_bytes,
        };
    }

    let mut output = String::new();
    let mut output_lines = 0_usize;
    let mut output_bytes = 0_usize;
    let mut truncated_by = TruncatedBy::Lines;
    for line in content.split_terminator('\n').take(max_lines) {
        let line_bytes = line.len().saturating_add(usize::from(output_lines > 0));
        if output_bytes.saturating_add(line_bytes) > max_bytes {
            truncated_by = TruncatedBy::Bytes;
            break;
        }
        if output_lines > 0 {
            output.push('\n');
        }
        output.push_str(line);
        output_lines = output_lines.saturating_add(1);
        output_bytes = output_bytes.saturating_add(line_bytes);
    }
    if output_lines >= max_lines && output_bytes <= max_bytes {
        truncated_by = TruncatedBy::Lines;
    }

    TruncationResult {
        output_bytes: output.len(),
        content: output,
        truncated: true,
        truncated_by: Some(truncated_by),
        total_lines,
        total_bytes,
        output_lines,
        last_line_partial: false,
        first_line_exceeds_limit: false,
        max_lines,
        max_bytes,
    }
}

/// Format a byte count exactly like Pi's `formatSize` helper.
pub fn format_size(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes}B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1}KB", bytes as f64 / 1024.0)
    } else {
        format!("{:.1}MB", bytes as f64 / (1024.0 * 1024.0))
    }
}

/// JavaScript string length: UTF-16 code units, not UTF-8 bytes or Unicode
/// scalar values.
pub fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

/// Port of JavaScript `slice(0, maxUnits)` for model-facing text. A split
/// surrogate is decoded lossily because Rust strings cannot contain an
/// unpaired surrogate and UTF-8 encoders render one as U+FFFD.
pub fn truncate_utf16(text: &str, max_units: usize) -> (String, bool) {
    let units = text.encode_utf16().collect::<Vec<_>>();
    if units.len() <= max_units {
        return (text.to_owned(), false);
    }
    (String::from_utf16_lossy(&units[..max_units]), true)
}

/// Apply the authority-preserving subset of Pi's path normalization: strip one
/// leading `@` and map Unicode space variants to ASCII space. Tilde and file
/// URLs deliberately remain literal so normalization cannot introduce ambient
/// host authority outside the confinement root.
pub fn normalize_tool_path(path: &std::path::Path) -> std::path::PathBuf {
    let Some(path) = path.to_str() else {
        return path.to_path_buf();
    };
    let mut normalized = path
        .chars()
        .map(|character| match character {
            '\u{00a0}' | '\u{2000}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            character => character,
        })
        .collect::<String>();
    if normalized.starts_with('@') {
        normalized.remove(0);
    }
    std::path::PathBuf::from(normalized)
}

/// Minimal synchronous filesystem access for tool cores.
///
/// Backends enforce path confinement (workspace root, allowed roots):
/// a path outside the granted scope returns [`ToolFsError::Denied`].
/// Tool cores never re-check confinement.
pub trait ToolFs {
    fn read_file(&self, path: &std::path::Path) -> Result<Vec<u8>, ToolFsError>;
    /// Read one complete file only when it is at most `max_bytes`; an error
    /// never exposes a partial buffer.
    fn read_file_bounded(
        &self,
        path: &std::path::Path,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ToolFsError>;
    fn write_file(&self, path: &std::path::Path, content: &[u8]) -> Result<(), ToolFsError>;
    /// Create a directory. `recursive` = `mkdir -p`.
    fn mkdir(&self, path: &std::path::Path, recursive: bool) -> Result<(), ToolFsError>;
    fn stat(&self, path: &std::path::Path) -> Result<FileStat, ToolFsError>;
    fn read_dir(&self, path: &std::path::Path) -> Result<Vec<DirEntry>, ToolFsError>;
    fn exists(&self, path: &std::path::Path) -> Result<bool, ToolFsError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FileStat {
    pub is_dir: bool,
    pub is_file: bool,
    pub size: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirEntry {
    pub name: String,
    pub is_dir: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum ToolFsError {
    #[error("not found: {0}")]
    NotFound(std::path::PathBuf),
    #[error("access denied: {0}")]
    Denied(std::path::PathBuf),
    #[error("file exceeds {max_bytes} byte read limit: {path}")]
    FileTooLarge {
        path: std::path::PathBuf,
        max_bytes: usize,
    },
    #[error("{0}")]
    Io(String),
}

/// Errors a tool run can produce. The embedder renders these as the
/// tool-result error text the model sees; messages are written for the
/// model (actionable, no host paths beyond what the model supplied).
#[derive(Debug, thiserror::Error)]
pub enum ToolError {
    #[error(transparent)]
    Fs(#[from] ToolFsError),
    #[error("invalid arguments: {0}")]
    InvalidArgs(String),
    #[error("{0}")]
    Failed(String),
    #[error("result exceeds {MAX_RESULT_BYTES} bytes; narrow the request")]
    ResultTooLarge,
}

/// One file discovered by [`walk_files`].
///
/// `path` is the path to pass back to [`ToolFs`]. `relative_path` is the
/// deterministic, `/`-separated path exposed by model-facing tools.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalkFile {
    pub path: std::path::PathBuf,
    pub relative_path: String,
    pub is_dir: bool,
    pub size: u64,
    skip_read_errors: bool,
}

/// Walk a file or directory using only [`ToolFs`].
///
/// Directory walks include hidden files, skip every `.git` directory, apply
/// root `.git/info/exclude` plus nested `.gitignore` files, prune ignored
/// directories, skip listed entries that disappear before they can be
/// resolved, and return files and directories sorted by root-relative path.
/// Directory relative paths carry a trailing `/`. Stat errors other than
/// [`ToolFsError::NotFound`] still fail the walk.
pub fn walk_files(root: &std::path::Path, fs: &dyn ToolFs) -> Result<Vec<WalkFile>, ToolError> {
    let stat = fs.stat(root)?;
    if stat.is_file {
        let relative_path = root
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| root.to_string_lossy().into_owned());
        return Ok(vec![WalkFile {
            path: root.to_path_buf(),
            relative_path,
            is_dir: false,
            size: stat.size,
            skip_read_errors: false,
        }]);
    }
    if !stat.is_dir {
        return Err(ToolError::Failed(format!(
            "path {} is not a file or directory",
            root.display()
        )));
    }

    let mut ignore_matchers = Vec::new();
    add_ignore_file(
        fs,
        root,
        &root.join(".git/info/exclude"),
        &mut ignore_matchers,
    )?;
    add_ignore_file(fs, root, &root.join(".gitignore"), &mut ignore_matchers)?;

    let mut files = Vec::new();
    walk_directory(
        fs,
        root,
        std::path::Path::new(""),
        &mut ignore_matchers,
        &mut files,
    )?;
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(files)
}

fn walk_directory(
    fs: &dyn ToolFs,
    directory: &std::path::Path,
    relative_directory: &std::path::Path,
    ignore_matchers: &mut Vec<ignore::gitignore::Gitignore>,
    files: &mut Vec<WalkFile>,
) -> Result<(), ToolError> {
    let mut entries = fs.read_dir(directory)?;
    entries.sort_by(|left, right| left.name.cmp(&right.name));

    for entry in entries {
        let path = directory.join(&entry.name);
        let relative = relative_directory.join(&entry.name);
        if entry.is_dir {
            if entry.name == ".git" || is_ignored(ignore_matchers, &path, true) {
                continue;
            }

            files.push(WalkFile {
                path: path.clone(),
                relative_path: format!("{}/", relative_path_string(&relative)),
                is_dir: true,
                size: 0,
                skip_read_errors: true,
            });
            let matcher_count = ignore_matchers.len();
            add_ignore_file(fs, &path, &path.join(".gitignore"), ignore_matchers)?;
            walk_directory(fs, &path, &relative, ignore_matchers, files)?;
            ignore_matchers.truncate(matcher_count);
        } else if !is_ignored(ignore_matchers, &path, false) {
            let stat = match fs.stat(&path) {
                Ok(stat) => stat,
                Err(ToolFsError::NotFound(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            if stat.is_file {
                files.push(WalkFile {
                    path,
                    relative_path: relative_path_string(&relative),
                    is_dir: false,
                    size: stat.size,
                    skip_read_errors: true,
                });
            }
        }
    }
    Ok(())
}

/// Read a file selected by [`walk_files`] without letting one stale or
/// unreadable directory entry abort the rest of a walk.
///
/// Files named directly as the walk root still surface read errors. Directory
/// entries that fail after a successful stat are skipped, as are entries that
/// exceed `max_bytes` either at stat time or while being read.
pub fn read_walk_file_bounded(
    file: &WalkFile,
    fs: &dyn ToolFs,
    max_bytes: usize,
) -> Result<Option<Vec<u8>>, ToolError> {
    if file.is_dir || file.size > u64::try_from(max_bytes).unwrap_or(u64::MAX) {
        return Ok(None);
    }
    read_walk_entry_bounded(fs, &file.path, max_bytes, file.skip_read_errors)
}

fn read_walk_entry_bounded(
    fs: &dyn ToolFs,
    path: &std::path::Path,
    max_bytes: usize,
    skip_read_errors: bool,
) -> Result<Option<Vec<u8>>, ToolError> {
    match fs.read_file_bounded(path, max_bytes) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(ToolFsError::FileTooLarge { .. }) => Ok(None),
        Err(_) if skip_read_errors => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn add_ignore_file(
    fs: &dyn ToolFs,
    match_root: &std::path::Path,
    ignore_path: &std::path::Path,
    ignore_matchers: &mut Vec<ignore::gitignore::Gitignore>,
) -> Result<(), ToolError> {
    let stat = match fs.stat(ignore_path) {
        Ok(stat) => stat,
        Err(ToolFsError::NotFound(_)) => return Ok(()),
        Err(error) => return Err(error.into()),
    };
    if !stat.is_file || stat.size > u64::try_from(MAX_FILE_BYTES).unwrap_or(u64::MAX) {
        return Ok(());
    }

    let bytes = match read_walk_entry_bounded(fs, ignore_path, MAX_FILE_BYTES, true)? {
        Some(bytes) => bytes,
        None => return Ok(()),
    };
    let content = String::from_utf8_lossy(&bytes);
    let mut builder = ignore::gitignore::GitignoreBuilder::new(match_root);
    for (index, line) in content.lines().enumerate() {
        let line = if index == 0 {
            line.trim_start_matches('\u{feff}')
        } else {
            line
        };
        let _ = builder.add_line(Some(ignore_path.to_path_buf()), line);
    }
    let matcher = builder
        .build()
        .map_err(|error| ToolError::Failed(format!("failed to parse ignore rules: {error}")))?;
    ignore_matchers.push(matcher);
    Ok(())
}

fn is_ignored(
    ignore_matchers: &[ignore::gitignore::Gitignore],
    path: &std::path::Path,
    is_dir: bool,
) -> bool {
    let mut ignored = false;
    for matcher in ignore_matchers {
        match matcher.matched(path, is_dir) {
            ignore::Match::None => {}
            ignore::Match::Ignore(_) => ignored = true,
            ignore::Match::Whitelist(_) => ignored = false,
        }
    }
    ignored
}

fn relative_path_string(path: &std::path::Path) -> String {
    path.components()
        .filter_map(|component| match component {
            std::path::Component::Normal(part) => Some(part.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// How a tool call composes with retries and replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EffectClass {
    /// No observable side effect. Safe to re-run any number of times.
    Pure,
    /// Re-running with identical arguments against the resulting state is
    /// a no-op (write with same content, edit already applied).
    Idempotent,
}

/// The model-facing contract of one tool. This is the single source of the
/// surface the model sees; packaging (native op, wasm op, remote executor)
/// carries it along unchanged.
#[derive(Clone, Debug)]
pub struct ToolContract {
    /// Default model-facing name. Manifests may attach under another name.
    pub name: &'static str,
    /// Model-facing description, verbatim.
    pub description: &'static str,
    /// JSON Schema for the arguments object.
    pub input_schema: serde_json::Value,
    pub effect_class: EffectClass,
}

/// Native backend over the real filesystem, for standalone CLI bins and tests.
///
/// Relative paths resolve beneath the configured root. Absolute paths are
/// accepted only when they name the root or one of its descendants. The root
/// and every existing path component are canonicalized before access, so both
/// lexical `..` escapes and symlinks that leave the root return
/// [`ToolFsError::Denied`]. Missing descendants are resolved only after their
/// nearest existing ancestor has passed the same check.
#[cfg(feature = "std-fs")]
pub struct StdFs {
    pub root: std::path::PathBuf,
}

#[cfg(feature = "std-fs")]
impl StdFs {
    pub fn new(root: impl AsRef<std::path::Path>) -> Result<Self, ToolFsError> {
        let root = root.as_ref().to_path_buf();
        let fs = Self {
            root: lexical_normalize(&root),
        };
        fs.canonical_root()?;
        Ok(fs)
    }

    fn canonical_root(&self) -> Result<std::path::PathBuf, ToolFsError> {
        if !self.root.is_absolute() {
            return Err(ToolFsError::Denied(self.root.clone()));
        }

        let canonical_root =
            std::fs::canonicalize(&self.root).map_err(|error| map_io_error(&self.root, error))?;
        let metadata =
            std::fs::metadata(&canonical_root).map_err(|error| map_io_error(&self.root, error))?;
        if !metadata.is_dir() {
            return Err(ToolFsError::Io(
                "filesystem root is not a directory".to_owned(),
            ));
        }
        Ok(canonical_root)
    }

    fn resolve(&self, path: &std::path::Path) -> Result<std::path::PathBuf, ToolFsError> {
        let root = lexical_normalize(&self.root);
        let canonical_root = self.canonical_root()?;
        let relative = if path.is_absolute() {
            path.strip_prefix(&root)
                .or_else(|_| path.strip_prefix(&canonical_root))
                .map_err(|_| ToolFsError::Denied(path.to_path_buf()))?
        } else {
            path
        };
        let mut resolved = canonical_root.clone();
        for component in relative.components() {
            match component {
                std::path::Component::CurDir => {}
                std::path::Component::ParentDir => {
                    if resolved == canonical_root
                        || !resolved.pop()
                        || !resolved.starts_with(&canonical_root)
                    {
                        return Err(ToolFsError::Denied(path.to_path_buf()));
                    }
                }
                std::path::Component::Normal(part) => {
                    resolved.push(part);
                    match std::fs::symlink_metadata(&resolved) {
                        Ok(_) => {
                            resolved = std::fs::canonicalize(&resolved)
                                .map_err(|error| map_io_error(path, error))?;
                            if !resolved.starts_with(&canonical_root) {
                                return Err(ToolFsError::Denied(path.to_path_buf()));
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(map_io_error(path, error)),
                    }
                }
                std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                    return Err(ToolFsError::Denied(path.to_path_buf()));
                }
            }
        }

        Ok(resolved)
    }
}

#[cfg(feature = "std-fs")]
impl ToolFs for StdFs {
    fn read_file(&self, path: &std::path::Path) -> Result<Vec<u8>, ToolFsError> {
        let resolved = self.resolve(path)?;
        reject_special_file_open(path, &resolved, false)?;
        std::fs::read(resolved).map_err(|error| map_io_error(path, error))
    }

    fn read_file_bounded(
        &self,
        path: &std::path::Path,
        max_bytes: usize,
    ) -> Result<Vec<u8>, ToolFsError> {
        let resolved = self.resolve(path)?;
        reject_special_file_open(path, &resolved, false)?;
        let file = std::fs::File::open(resolved).map_err(|error| map_io_error(path, error))?;
        let read_limit = u64::try_from(max_bytes.saturating_add(1)).unwrap_or(u64::MAX);
        let mut reader = std::io::Read::take(file, read_limit);
        let mut bytes = Vec::with_capacity(max_bytes.min(64 * 1024));
        std::io::Read::read_to_end(&mut reader, &mut bytes)
            .map_err(|error| map_io_error(path, error))?;
        if bytes.len() > max_bytes {
            return Err(ToolFsError::FileTooLarge {
                path: path.to_path_buf(),
                max_bytes,
            });
        }
        Ok(bytes)
    }

    fn write_file(&self, path: &std::path::Path, content: &[u8]) -> Result<(), ToolFsError> {
        let resolved = self.resolve(path)?;
        reject_special_file_open(path, &resolved, true)?;
        std::fs::write(resolved, content).map_err(|error| map_io_error(path, error))
    }

    fn mkdir(&self, path: &std::path::Path, recursive: bool) -> Result<(), ToolFsError> {
        let resolved = self.resolve(path)?;
        let result = if recursive {
            std::fs::create_dir_all(&resolved)
        } else {
            std::fs::create_dir(&resolved)
        };
        result.map_err(|error| map_io_error(path, error))?;

        let canonical =
            std::fs::canonicalize(&resolved).map_err(|error| map_io_error(path, error))?;
        if !canonical.starts_with(self.canonical_root()?) {
            return Err(ToolFsError::Denied(path.to_path_buf()));
        }
        Ok(())
    }

    fn stat(&self, path: &std::path::Path) -> Result<FileStat, ToolFsError> {
        let resolved = self.resolve(path)?;
        let metadata = std::fs::metadata(resolved).map_err(|error| map_io_error(path, error))?;
        Ok(FileStat {
            is_dir: metadata.is_dir(),
            is_file: metadata.is_file(),
            size: metadata.len(),
        })
    }

    fn read_dir(&self, path: &std::path::Path) -> Result<Vec<DirEntry>, ToolFsError> {
        let resolved = self.resolve(path)?;
        let entries = std::fs::read_dir(resolved).map_err(|error| map_io_error(path, error))?;
        let mut result = Vec::new();
        for entry in entries {
            let entry = entry.map_err(|error| map_io_error(path, error))?;
            let file_type = entry
                .file_type()
                .map_err(|error| map_io_error(path, error))?;
            result.push(DirEntry {
                name: entry.file_name().to_string_lossy().into_owned(),
                is_dir: file_type.is_dir(),
            });
        }
        result.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(result)
    }

    fn exists(&self, path: &std::path::Path) -> Result<bool, ToolFsError> {
        let resolved = self.resolve(path)?;
        match std::fs::metadata(resolved) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(map_io_error(path, error)),
        }
    }
}

#[cfg(feature = "std-fs")]
fn reject_special_file_open(
    path: &std::path::Path,
    resolved: &std::path::Path,
    allow_missing: bool,
) -> Result<(), ToolFsError> {
    match std::fs::metadata(resolved) {
        Ok(metadata) if !metadata.is_file() && !metadata.is_dir() => Err(ToolFsError::Io(format!(
            "path is not a regular file: {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if allow_missing && error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(map_io_error(path, error)),
    }
}

#[cfg(feature = "std-fs")]
fn lexical_normalize(path: &std::path::Path) -> std::path::PathBuf {
    let mut normalized = std::path::PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            std::path::Component::RootDir => normalized.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => {
                normalized.pop();
            }
            std::path::Component::Normal(part) => normalized.push(part),
        }
    }
    normalized
}

#[cfg(feature = "std-fs")]
fn map_io_error(path: &std::path::Path, error: std::io::Error) -> ToolFsError {
    match error.kind() {
        std::io::ErrorKind::NotFound => ToolFsError::NotFound(path.to_path_buf()),
        std::io::ErrorKind::PermissionDenied => ToolFsError::Denied(path.to_path_buf()),
        _ => ToolFsError::Io(error.to_string()),
    }
}

/// Run one tool's native JSON-over-stdin CLI surface over [`StdFs`].
///
/// Input has the shape `{"root":"/absolute/path","args":{...}}`. Exactly one
/// JSON object is written to stdout: `{"ok":...}` on success or
/// `{"error":"..."}` on failure. The returned process exit status is zero
/// for success and one for every input, filesystem, tool, or output error.
#[cfg(feature = "std-fs")]
pub fn run_cli<Args, Output>(
    run: fn(Args, &dyn ToolFs) -> Result<Output, ToolError>,
) -> std::process::ExitCode
where
    Args: serde::de::DeserializeOwned,
    Output: serde::Serialize,
{
    #[derive(serde::Deserialize)]
    struct CliInput<Args> {
        root: std::path::PathBuf,
        args: Args,
    }

    let mut input = String::new();
    if let Err(error) = std::io::Read::read_to_string(&mut std::io::stdin(), &mut input) {
        return write_cli_error(format!("failed to read stdin: {error}"));
    }
    let input = match serde_json::from_str::<CliInput<Args>>(&input) {
        Ok(input) => input,
        Err(error) => return write_cli_error(format!("invalid input JSON: {error}")),
    };
    let fs = match StdFs::new(&input.root) {
        Ok(fs) => fs,
        Err(error) => return write_cli_error(error.to_string()),
    };

    match run(input.args, &fs) {
        Ok(output) => match serde_json::to_value(output) {
            Ok(output) => {
                let mut result = serde_json::Map::new();
                result.insert("ok".to_owned(), output);
                write_cli_json(serde_json::Value::Object(result), true)
            }
            Err(error) => write_cli_error(format!("failed to serialize result: {error}")),
        },
        Err(error) => write_cli_error(error.to_string()),
    }
}

/// Run a native CLI with a tool-specific prepare/coerce/validate step.
///
/// Pi's edit tool prepares several legacy argument shapes before schema
/// validation. Keeping this hook at the JSON boundary lets that tool return
/// Pi's validation envelope without weakening the typed [`run_cli`] path used
/// by the other tools.
#[cfg(feature = "std-fs")]
pub fn run_cli_with_parser<Args, Output>(
    parse_args: fn(serde_json::Value) -> Result<Args, String>,
    run: fn(Args, &dyn ToolFs) -> Result<Output, ToolError>,
) -> std::process::ExitCode
where
    Output: serde::Serialize,
{
    #[derive(serde::Deserialize)]
    struct CliInput {
        root: std::path::PathBuf,
        args: serde_json::Value,
    }

    let mut input = String::new();
    if let Err(error) = std::io::Read::read_to_string(&mut std::io::stdin(), &mut input) {
        return write_cli_error(format!("failed to read stdin: {error}"));
    }
    let input = match serde_json::from_str::<CliInput>(&input) {
        Ok(input) => input,
        Err(error) => return write_cli_error(format!("invalid input JSON: {error}")),
    };
    let args = match parse_args(input.args) {
        Ok(args) => args,
        Err(error) => return write_cli_error(error),
    };
    let fs = match StdFs::new(&input.root) {
        Ok(fs) => fs,
        Err(error) => return write_cli_error(error.to_string()),
    };

    match run(args, &fs) {
        Ok(output) => match serde_json::to_value(output) {
            Ok(output) => {
                let mut result = serde_json::Map::new();
                result.insert("ok".to_owned(), output);
                write_cli_json(serde_json::Value::Object(result), true)
            }
            Err(error) => write_cli_error(format!("failed to serialize result: {error}")),
        },
        Err(error) => write_cli_error(error.to_string()),
    }
}

#[cfg(feature = "std-fs")]
fn write_cli_error(error: String) -> std::process::ExitCode {
    write_cli_json(serde_json::json!({"error": error}), false)
}

#[cfg(feature = "std-fs")]
fn write_cli_json(value: serde_json::Value, success: bool) -> std::process::ExitCode {
    let mut bytes = match serde_json::to_vec(&value) {
        Ok(bytes) => bytes,
        Err(_) => return std::process::ExitCode::FAILURE,
    };
    bytes.push(b'\n');
    if std::io::Write::write_all(&mut std::io::stdout(), &bytes).is_err() {
        return std::process::ExitCode::FAILURE;
    }
    if success {
        std::process::ExitCode::SUCCESS
    } else {
        std::process::ExitCode::FAILURE
    }
}

#[cfg(all(test, feature = "std-fs"))]
mod tests {
    #[test]
    fn parent_escape_is_denied() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(outer.path().join("outside.txt"), "secret").unwrap();
        let fs = crate::StdFs { root };

        let error =
            crate::ToolFs::read_file(&fs, std::path::Path::new("../outside.txt")).unwrap_err();
        assert!(matches!(error, crate::ToolFsError::Denied(_)));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_chain_escape_is_denied() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        let outside = outer.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(outside.join("secret.txt"), "secret").unwrap();
        std::os::unix::fs::symlink("second-link", root.join("first-link")).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("second-link")).unwrap();
        let fs = crate::StdFs::new(&root).unwrap();

        let error = crate::ToolFs::read_file(&fs, std::path::Path::new("first-link/secret.txt"))
            .unwrap_err();
        assert!(matches!(error, crate::ToolFsError::Denied(_)));
    }

    #[cfg(unix)]
    #[test]
    fn parent_after_an_escaping_symlink_is_denied_before_lexical_collapse() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        let outside = outer.path().join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&outside).unwrap();
        std::fs::write(root.join("secret.txt"), "inside").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        let fs = crate::StdFs::new(&root).unwrap();

        let error =
            crate::ToolFs::read_file(&fs, std::path::Path::new("link/../secret.txt")).unwrap_err();

        assert!(matches!(error, crate::ToolFsError::Denied(_)));
    }

    #[test]
    fn absolute_paths_use_component_prefixes_and_missing_parents_cannot_escape() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        let colliding_root = outer.path().join("rootx");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&colliding_root).unwrap();
        std::fs::write(root.join("inside.txt"), "inside").unwrap();
        std::fs::write(colliding_root.join("outside.txt"), "outside").unwrap();
        let fs = crate::StdFs::new(&root).unwrap();

        assert_eq!(
            crate::ToolFs::read_file(&fs, &root.join("inside.txt")).unwrap(),
            b"inside"
        );
        let prefix_collision =
            crate::ToolFs::read_file(&fs, &colliding_root.join("outside.txt")).unwrap_err();
        let missing_tail = crate::ToolFs::write_file(
            &fs,
            std::path::Path::new("missing/../../outside.txt"),
            b"escape",
        )
        .unwrap_err();

        assert!(matches!(prefix_collision, crate::ToolFsError::Denied(_)));
        assert!(matches!(missing_tail, crate::ToolFsError::Denied(_)));
    }

    #[test]
    fn bounded_reads_accept_the_exact_limit_and_return_no_partial_buffer() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("large.bin"),
            vec![b'x'; crate::MAX_FILE_BYTES],
        )
        .unwrap();
        let fs = crate::StdFs::new(root.path()).unwrap();

        let exact = crate::ToolFs::read_file_bounded(
            &fs,
            std::path::Path::new("large.bin"),
            crate::MAX_FILE_BYTES,
        )
        .unwrap();
        std::fs::write(
            root.path().join("large.bin"),
            vec![b'x'; crate::MAX_FILE_BYTES.saturating_add(1)],
        )
        .unwrap();
        let error = crate::ToolFs::read_file_bounded(
            &fs,
            std::path::Path::new("large.bin"),
            crate::MAX_FILE_BYTES,
        )
        .unwrap_err();

        assert_eq!(exact.len(), crate::MAX_FILE_BYTES);
        assert!(matches!(
            error,
            crate::ToolFsError::FileTooLarge {
                max_bytes: crate::MAX_FILE_BYTES,
                ..
            }
        ));
    }

    #[cfg(unix)]
    #[test]
    fn bounded_reads_reject_a_fifo_before_waiting_for_a_writer() {
        let root = tempfile::tempdir().unwrap();
        let fifo = root.path().join("named.pipe");
        assert!(std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success());
        let writer_path = fifo.clone();
        let writer = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(500));
            std::fs::write(writer_path, b"late writer").unwrap();
        });
        let fs = crate::StdFs::new(root.path()).unwrap();

        let started = std::time::Instant::now();
        let result = crate::ToolFs::read_file_bounded(
            &fs,
            std::path::Path::new("named.pipe"),
            crate::MAX_FILE_BYTES,
        );
        let elapsed = started.elapsed();

        let mut cleanup_reader = std::fs::File::open(&fifo).unwrap();
        let mut cleanup_bytes = Vec::new();
        std::io::Read::read_to_end(&mut cleanup_reader, &mut cleanup_bytes).unwrap();
        writer.join().unwrap();
        assert!(result.is_err());
        assert!(
            elapsed < std::time::Duration::from_millis(250),
            "{elapsed:?}"
        );
    }

    #[test]
    fn walk_skips_oversized_ignore_rules_without_reading_unbounded_content() {
        let root = tempfile::tempdir().unwrap();
        let mut ignore = vec![b'#'; crate::MAX_FILE_BYTES.saturating_add(1)];
        ignore.push(b'\n');
        ignore.extend_from_slice(b"hidden.txt\n");
        std::fs::write(root.path().join(".gitignore"), ignore).unwrap();
        std::fs::write(root.path().join("hidden.txt"), "visible").unwrap();
        let fs = crate::StdFs::new(root.path()).unwrap();

        let files = crate::walk_files(std::path::Path::new("."), &fs).unwrap();

        assert!(files.iter().any(|file| file.relative_path == "hidden.txt"));
    }

    #[test]
    fn walk_applies_ignore_rules_at_the_exact_read_limit() {
        let root = tempfile::tempdir().unwrap();
        let mut ignore = b"hidden.txt\n".to_vec();
        ignore.resize(crate::MAX_FILE_BYTES, b'#');
        std::fs::write(root.path().join(".gitignore"), ignore).unwrap();
        std::fs::write(root.path().join("hidden.txt"), "hidden").unwrap();
        std::fs::write(root.path().join("visible.txt"), "visible").unwrap();
        let fs = crate::StdFs::new(root.path()).unwrap();

        let files = crate::walk_files(std::path::Path::new("."), &fs).unwrap();

        assert!(!files.iter().any(|file| file.relative_path == "hidden.txt"));
        assert!(files.iter().any(|file| file.relative_path == "visible.txt"));
    }

    #[test]
    fn gitignore_matches_git_for_anchoring_directory_rules_and_pruned_negation() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("nested/dir")).unwrap();
        std::fs::create_dir(root.path().join("pruned")).unwrap();
        std::fs::write(
            root.path().join(".gitignore"),
            "/foo\ndir/\npruned/\n!pruned/keep.txt\n",
        )
        .unwrap();
        std::fs::write(root.path().join("foo"), "ignored").unwrap();
        std::fs::write(root.path().join("nested/foo"), "visible").unwrap();
        std::fs::write(root.path().join("dir"), "visible file").unwrap();
        std::fs::write(root.path().join("nested/dir/child"), "ignored").unwrap();
        std::fs::write(root.path().join("pruned/keep.txt"), "still ignored").unwrap();
        let fs = crate::StdFs::new(root.path()).unwrap();

        let files = crate::walk_files(std::path::Path::new("."), &fs).unwrap();

        assert_eq!(
            files
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec![".gitignore", "dir", "nested/", "nested/foo"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn walk_skips_a_dangling_symlink_and_returns_real_files() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("real.txt"), "real").unwrap();
        std::os::unix::fs::symlink(
            root.path().join("missing-target"),
            root.path().join("broken-link"),
        )
        .unwrap();
        let fs = crate::StdFs::new(root.path()).unwrap();

        let files = crate::walk_files(std::path::Path::new("."), &fs).unwrap();

        assert_eq!(
            files
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["real.txt"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn walk_skips_a_unix_socket_and_returns_regular_files() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("real.txt"), "real").unwrap();
        let _listener =
            std::os::unix::net::UnixListener::bind(root.path().join("live.sock")).unwrap();
        let fs = crate::StdFs::new(root.path()).unwrap();

        let files = crate::walk_files(std::path::Path::new("."), &fs).unwrap();

        assert_eq!(
            files
                .iter()
                .map(|file| file.relative_path.as_str())
                .collect::<Vec<_>>(),
            vec!["real.txt"]
        );
    }

    #[cfg(unix)]
    #[test]
    fn walk_still_propagates_denied_entry_stats() {
        let outer = tempfile::tempdir().unwrap();
        let root = outer.path().join("root");
        let outside = outer.path().join("outside.txt");
        std::fs::create_dir(&root).unwrap();
        std::fs::write(&outside, "outside").unwrap();
        std::os::unix::fs::symlink(&outside, root.join("escaped-link")).unwrap();
        let fs = crate::StdFs::new(&root).unwrap();

        let error = crate::walk_files(std::path::Path::new("."), &fs).unwrap_err();

        assert!(matches!(
            error,
            crate::ToolError::Fs(crate::ToolFsError::Denied(_))
        ));
    }

    #[test]
    fn pi_helpers_pin_truncation_utf16_and_path_normalization() {
        // Pi source: core/tools/truncate.ts:47-160,264-275 and utils/paths.ts:7,75-99.
        let truncated = crate::truncate_head("one\ntwo\nthree", 2, 50 * 1024);
        assert_eq!(truncated.content, "one\ntwo");
        assert_eq!(truncated.truncated_by, Some(crate::TruncatedBy::Lines));
        assert_eq!(crate::format_size(50 * 1024), "50.0KB");
        assert_eq!(crate::utf16_len("é🙂"), 3);
        assert_eq!(
            crate::truncate_utf16(&"🙂".repeat(251), 500),
            ("🙂".repeat(250), true)
        );
        assert_eq!(
            crate::normalize_tool_path(std::path::Path::new("@a\u{00a0}b\u{2000}c\u{3000}d")),
            std::path::PathBuf::from("a b c d")
        );
        assert_eq!(
            crate::normalize_tool_path(std::path::Path::new("~/literal")),
            std::path::PathBuf::from("~/literal")
        );
        assert_eq!(
            crate::normalize_tool_path(std::path::Path::new("file:///literal")),
            std::path::PathBuf::from("file:///literal")
        );
    }

    #[test]
    fn truncate_head_pins_exact_line_and_byte_boundaries() {
        // Pi source: core/tools/truncate.ts:78-160.
        let exactly_two_thousand = std::iter::repeat_n("x", crate::DEFAULT_MAX_LINES)
            .collect::<Vec<_>>()
            .join("\n");
        let over_two_thousand = format!("{exactly_two_thousand}\nx");
        let exact_line_boundary = crate::truncate_head(
            &exactly_two_thousand,
            crate::DEFAULT_MAX_LINES,
            crate::DEFAULT_MAX_BYTES,
        );
        let over_line_boundary = crate::truncate_head(
            &over_two_thousand,
            crate::DEFAULT_MAX_LINES,
            crate::DEFAULT_MAX_BYTES,
        );

        assert!(!exact_line_boundary.truncated);
        assert!(over_line_boundary.truncated);
        assert_eq!(
            over_line_boundary.truncated_by,
            Some(crate::TruncatedBy::Lines)
        );
        assert_eq!(over_line_boundary.output_lines, crate::DEFAULT_MAX_LINES);

        let exactly_fifty_kib = "🙂".repeat(crate::DEFAULT_MAX_BYTES / 4);
        let exact_byte_boundary = crate::truncate_head(
            &exactly_fifty_kib,
            crate::DEFAULT_MAX_LINES,
            crate::DEFAULT_MAX_BYTES,
        );
        let complete_first_line = crate::truncate_head(
            &format!("{exactly_fifty_kib}\nx"),
            crate::DEFAULT_MAX_LINES,
            crate::DEFAULT_MAX_BYTES,
        );
        let oversized_first_line = crate::truncate_head(
            &format!("{exactly_fifty_kib}x\ny"),
            crate::DEFAULT_MAX_LINES,
            crate::DEFAULT_MAX_BYTES,
        );

        assert!(!exact_byte_boundary.truncated);
        assert_eq!(exact_byte_boundary.output_bytes, crate::DEFAULT_MAX_BYTES);
        assert!(complete_first_line.truncated);
        assert_eq!(complete_first_line.content, exactly_fifty_kib);
        assert_eq!(complete_first_line.output_lines, 1);
        assert!(!complete_first_line.first_line_exceeds_limit);
        assert!(oversized_first_line.truncated);
        assert!(oversized_first_line.content.is_empty());
        assert!(oversized_first_line.first_line_exceeds_limit);
    }

    #[test]
    fn truncate_utf16_pins_surrogate_pair_boundaries() {
        // JavaScript slice(0, 500) keeps a split high surrogate. Rust strings
        // represent that unpaired surrogate as U+FFFD in model-facing UTF-8.
        let exact_pair_boundary = format!("{}🙂tail", "x".repeat(498));
        let split_pair_boundary = format!("{}🙂tail", "x".repeat(499));

        assert_eq!(
            crate::truncate_utf16(&exact_pair_boundary, 500),
            (format!("{}🙂", "x".repeat(498)), true)
        );
        assert_eq!(
            crate::truncate_utf16(&split_pair_boundary, 500),
            (format!("{}\u{fffd}", "x".repeat(499)), true)
        );
    }
}
