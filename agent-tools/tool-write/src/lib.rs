//! `write` — create or overwrite a file with Pi-compatible behavior.
//!
//! Pinned semantics:
//! - Parent directories are created as needed (`mkdir -p`).
//! - Existing files are overwritten unconditionally; there is no prior-read
//!   or stale-file guard in Pi.
//! - Content is written exactly as given (no trailing-newline fixups).
//! - Model-facing text reports JavaScript UTF-16 code units as "bytes", while
//!   the structured receipt retains the true UTF-8 byte count.

#[derive(Clone, Debug, serde::Deserialize)]
pub struct WriteArgs {
    /// Path of the file to write (relative to the workspace root or
    /// absolute within the granted scope).
    pub path: std::path::PathBuf,
    /// Full file content.
    pub content: String,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct WriteOutput {
    /// Pi-compatible model-facing primary output.
    pub text: String,
    /// True UTF-8 byte count retained as a structured receipt.
    pub bytes_written: u64,
    /// Whether the target existed immediately before the unconditional write.
    pub replaced: bool,
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "write",
        description: "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. Automatically creates parent directories.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file to write (relative or absolute)"},
                "content": {"type": "string", "description": "Content to write to the file"}
            },
            "required": ["path", "content"]
        }),
        effect_class: verlet_tool_core::EffectClass::Idempotent,
    }
}

pub fn run(
    args: WriteArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<WriteOutput, verlet_tool_core::ToolError> {
    let input_path = args.path;
    let path = verlet_tool_core::normalize_tool_path(&input_path);
    let replaced = fs.exists(&path).unwrap_or(false);
    if fs
        .stat(&path)
        .is_ok_and(|stat| !stat.is_file && !stat.is_dir)
    {
        return Err(verlet_tool_core::ToolError::Failed(format!(
            "Path is not a file: {}",
            path.display()
        )));
    }

    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs.mkdir(parent, true)?;
        }
    }
    fs.write_file(&path, args.content.as_bytes())?;

    Ok(WriteOutput {
        text: format!(
            "Successfully wrote {} bytes to {}",
            verlet_tool_core::utf16_len(&args.content),
            input_path.display()
        ),
        bytes_written: u64::try_from(args.content.len()).unwrap_or(u64::MAX),
        replaced,
    })
}

#[cfg(test)]
mod tests {
    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    fn args(path: &str, content: &str) -> crate::WriteArgs {
        crate::WriteArgs {
            path: std::path::PathBuf::from(path),
            content: content.to_owned(),
        }
    }

    #[test]
    fn creates_a_file_and_nested_parents() {
        let root = tempfile::tempdir().unwrap();

        let output = crate::run(
            args("nested/deeper/file.txt", "exact content"),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            output,
            crate::WriteOutput {
                text: "Successfully wrote 13 bytes to nested/deeper/file.txt".to_owned(),
                bytes_written: 13,
                replaced: false,
            }
        );
        assert_eq!(
            std::fs::read(root.path().join("nested/deeper/file.txt")).unwrap(),
            b"exact content"
        );
    }

    #[test]
    fn overwrites_an_existing_file_unconditionally() {
        // Pi behavior sheet item 12; source: core/tools/write.ts:208-231.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "old").unwrap();

        let output = crate::run(args("file.txt", "new"), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::WriteOutput {
                text: "Successfully wrote 3 bytes to file.txt".to_owned(),
                bytes_written: 3,
                replaced: true,
            }
        );
        assert_eq!(std::fs::read(root.path().join("file.txt")).unwrap(), b"new");
    }

    #[test]
    fn reports_utf16_units_in_text_and_utf8_bytes_in_the_receipt() {
        // Pi behavior sheet item 13; source: core/tools/write.ts:224-231.
        let root = tempfile::tempdir().unwrap();

        let output = crate::run(args("unicode.txt", "é🙂"), &fs(root.path())).unwrap();

        assert_eq!(output.text, "Successfully wrote 3 bytes to unicode.txt");
        assert_eq!(output.bytes_written, 6);
        assert!(!output.replaced);
    }

    #[test]
    fn an_existing_directory_reaches_the_backend_write_error() {
        let root = tempfile::tempdir().unwrap();

        let error = crate::run(args("", "content"), &fs(root.path())).unwrap_err();

        assert!(!error.to_string().contains("is not a file"));
    }

    #[cfg(unix)]
    #[test]
    fn rejects_a_fifo_before_opening_it() {
        let root = tempfile::tempdir().unwrap();
        assert!(std::process::Command::new("mkfifo")
            .arg(root.path().join("named.pipe"))
            .status()
            .unwrap()
            .success());

        let error = crate::run(args("named.pipe", "blocked"), &fs(root.path())).unwrap_err();

        assert_eq!(error.to_string(), "Path is not a file: named.pipe");
    }

    #[test]
    fn contract_and_path_normalization_match_pi() {
        // Pi behavior sheets items 1, 4, and 12; source: write.ts:15-18,187-231.
        let root = tempfile::tempdir().unwrap();
        let contract = crate::contract();
        assert_eq!(
            contract.description,
            "Write content to a file. Creates the file if it doesn't exist, overwrites if it does. Automatically creates parent directories."
        );
        assert_eq!(
            contract.input_schema,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to the file to write (relative or absolute)"},
                    "content": {"type": "string", "description": "Content to write to the file"}
                },
                "required": ["path", "content"]
            })
        );

        crate::run(args("@unicode\u{2009}space.txt", "new"), &fs(root.path())).unwrap();

        assert_eq!(
            std::fs::read(root.path().join("unicode space.txt")).unwrap(),
            b"new"
        );
    }
}
