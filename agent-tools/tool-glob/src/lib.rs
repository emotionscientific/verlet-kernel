//! `find` — find files and directories by glob pattern.
//!
//! Pi's version shells out to `fd`; ours walks through [`ToolFs`] with
//! `globset` matching so every backend (native, vfs, wasm) traverses
//! identically. Gitignore awareness comes from parsing `.gitignore`
//! files with the `ignore` crate's buffer-based gitignore module during
//! the walk — not from the `ignore` crate's walker, which is welded to
//! `std::fs`.
//!
//! Pinned semantics:
//! - Pi's pattern preprocessing is preserved: basename patterns match at any
//!   depth; slash-containing patterns use full paths and usually gain `**/`.
//! - Respects `.gitignore` (per directory, nested, plus root
//!   `.git/info/exclude`); hidden files are included; `.git/` is always
//!   skipped.
//! - Results include directories with trailing `/` and are root-relative,
//!   `/`-separated, and sorted
//!   lexicographically (Pi preserves backend process order; we pin deterministic
//!   order instead — recorded deviation).
//! - Ignore-rule files over the shared 8 MiB file limit are skipped.
//! - `limit` (default 1000) and the 50 KiB complete-line head limit emit Pi's
//!   actionable notices.

pub const DEFAULT_LIMIT: i64 = 1000;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct GlobArgs {
    /// Glob pattern, e.g. `*.ts`, `**/*.json`, `src/**/*.spec.ts`.
    pub pattern: String,
    /// Directory to search in (default: workspace root).
    pub path: Option<std::path::PathBuf>,
    /// Maximum number of results (default 1000).
    pub limit: Option<i64>,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct GlobOutput {
    /// Pi-compatible model-facing primary output.
    pub text: String,
    /// Root-relative `/`-separated paths, sorted.
    pub paths: Vec<String>,
    pub limit_reached: bool,
    pub truncated: bool,
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "find",
        description: "Search for files by glob pattern. Returns matching file paths relative to the search directory. Respects .gitignore. Output is truncated to 1000 results or 50KB (whichever is hit first).",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Glob pattern to match files, e.g. '*.ts', '**/*.json', or 'src/**/*.spec.ts'"},
                "path": {"type": "string", "description": "Directory to search in (default: current directory)"},
                "limit": {"type": "number", "description": "Maximum number of results (default: 1000)"}
            },
            "required": ["pattern"]
        }),
        effect_class: verlet_tool_core::EffectClass::Pure,
    }
}

pub fn run(
    args: GlobArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<GlobOutput, verlet_tool_core::ToolError> {
    let effective_limit = args.limit.unwrap_or(DEFAULT_LIMIT).max(1);
    let full_path = args.pattern.contains('/');
    let effective_pattern = if full_path
        && !args.pattern.starts_with('/')
        && !args.pattern.starts_with("**/")
        && args.pattern != "**"
    {
        format!("**/{}", args.pattern)
    } else {
        args.pattern.clone()
    };
    let matcher = globset::Glob::new(&effective_pattern)
        .map_err(|error| {
            verlet_tool_core::ToolError::InvalidArgs(format!(
                "invalid glob pattern {:?}: {error}",
                args.pattern
            ))
        })?
        .compile_matcher();
    let input_root = args.path.unwrap_or_else(|| std::path::PathBuf::from("."));
    let root = verlet_tool_core::normalize_tool_path(&input_root);
    let root_stat = fs.stat(&root).map_err(|error| match error {
        verlet_tool_core::ToolFsError::NotFound(_) => {
            verlet_tool_core::ToolError::Failed(format!("Path not found: {}", root.display()))
        }
        error => error.into(),
    })?;
    if !root_stat.is_dir {
        return Err(verlet_tool_core::ToolError::Failed(format!(
            "Not a directory: {}",
            root.display()
        )));
    }
    let files = verlet_tool_core::walk_files(&root, fs).map_err(|error| match error {
        verlet_tool_core::ToolError::Fs(verlet_tool_core::ToolFsError::NotFound(_)) => {
            verlet_tool_core::ToolError::Failed(format!("Path not found: {}", root.display()))
        }
        error => error,
    })?;
    let limit = usize::try_from(effective_limit).unwrap_or(usize::MAX);
    let mut paths = Vec::new();

    for file in files {
        let matches = if full_path {
            matcher.is_match(&file.path)
        } else {
            let relative = file.relative_path.trim_end_matches('/');
            std::path::Path::new(relative)
                .file_name()
                .is_some_and(|name| matcher.is_match(std::path::Path::new(name)))
        };
        if !matches {
            continue;
        }
        paths.push(file.relative_path);
        if paths.len() >= limit {
            break;
        }
    }
    let limit_reached = paths.len() >= limit;

    let rendered_bytes = paths
        .iter()
        .map(String::len)
        .sum::<usize>()
        .saturating_add(paths.len().saturating_sub(1));
    if rendered_bytes > verlet_tool_core::MAX_RESULT_BYTES {
        return Err(verlet_tool_core::ToolError::ResultTooLarge);
    }

    if paths.is_empty() {
        return Ok(GlobOutput {
            text: "No files found matching pattern".to_owned(),
            paths,
            limit_reached: false,
            truncated: false,
        });
    }

    let raw_output = paths.join("\n");
    let truncation = verlet_tool_core::truncate_head(
        &raw_output,
        usize::MAX,
        verlet_tool_core::DEFAULT_MAX_BYTES,
    );
    let mut text = truncation.content.clone();
    let mut notices = Vec::new();
    if limit_reached {
        notices.push(format!(
            "{effective_limit} results limit reached. Use limit={} for more, or refine pattern",
            effective_limit.saturating_mul(2),
        ));
    }
    if truncation.truncated {
        notices.push(format!(
            "{} limit reached",
            verlet_tool_core::format_size(verlet_tool_core::DEFAULT_MAX_BYTES)
        ));
    }
    if !notices.is_empty() {
        text.push_str(&format!("\n\n[{}]", notices.join(". ")));
    }

    Ok(GlobOutput {
        text,
        paths,
        limit_reached,
        truncated: truncation.truncated,
    })
}

#[cfg(test)]
mod tests {
    fn args(pattern: &str, limit: Option<i64>) -> crate::GlobArgs {
        crate::GlobArgs {
            pattern: pattern.to_owned(),
            path: None,
            limit,
        }
    }

    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    #[test]
    fn double_star_matches_hidden_files_with_relative_sorted_paths() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("src")).unwrap();
        std::fs::create_dir_all(root.path().join(".git")).unwrap();
        std::fs::write(root.path().join("src/b.json"), "b").unwrap();
        std::fs::write(root.path().join("src/a.json"), "a").unwrap();
        std::fs::write(root.path().join(".hidden.json"), "hidden").unwrap();
        std::fs::write(root.path().join(".git/secret.json"), "secret").unwrap();

        let output = crate::run(args("**/*.json", None), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::GlobOutput {
                text: ".hidden.json\nsrc/a.json\nsrc/b.json".to_owned(),
                paths: vec![
                    ".hidden.json".to_owned(),
                    "src/a.json".to_owned(),
                    "src/b.json".to_owned(),
                ],
                limit_reached: false,
                truncated: false,
            }
        );
    }

    #[test]
    fn model_facing_contract_is_named_find() {
        // Pi behavior sheet item 34; source: core/tools/find.ts:123-133.
        assert_eq!(crate::contract().name, "find");
    }

    #[test]
    fn a_file_search_root_is_a_structured_error() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "content").unwrap();
        let mut search = args("**", None);
        search.path = Some(std::path::PathBuf::from("file.txt"));

        let error = crate::run(search, &fs(root.path())).unwrap_err();

        assert_eq!(error.to_string(), "Not a directory: file.txt");
    }

    #[test]
    fn nested_gitignore_negation_and_info_exclude_are_respected() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("nested")).unwrap();
        std::fs::create_dir_all(root.path().join("ignored")).unwrap();
        std::fs::create_dir_all(root.path().join(".git/info")).unwrap();
        std::fs::write(
            root.path().join(".gitignore"),
            "ignored/\n*.tmp\n!keep.tmp\nnested/*.log\n",
        )
        .unwrap();
        std::fs::write(root.path().join("nested/.gitignore"), "!important.log\n").unwrap();
        std::fs::write(root.path().join(".git/info/exclude"), "excluded.txt\n").unwrap();
        std::fs::write(root.path().join("ignored/unseen.txt"), "unseen").unwrap();
        std::fs::write(root.path().join("drop.tmp"), "drop").unwrap();
        std::fs::write(root.path().join("keep.tmp"), "keep").unwrap();
        std::fs::write(root.path().join("excluded.txt"), "excluded").unwrap();
        std::fs::write(root.path().join("nested/drop.log"), "drop").unwrap();
        std::fs::write(root.path().join("nested/important.log"), "keep").unwrap();
        std::fs::write(root.path().join("nested/keep.txt"), "keep").unwrap();

        let output = crate::run(args("**", None), &fs(root.path())).unwrap();

        assert_eq!(
            output.paths,
            vec![
                ".gitignore".to_owned(),
                "keep.tmp".to_owned(),
                "nested/".to_owned(),
                "nested/.gitignore".to_owned(),
                "nested/important.log".to_owned(),
                "nested/keep.txt".to_owned(),
            ]
        );
    }

    #[test]
    fn ignored_directories_are_pruned_without_being_read() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("ignored")).unwrap();
        std::fs::write(root.path().join(".gitignore"), "ignored/\n").unwrap();
        std::fs::write(root.path().join("ignored/file.txt"), "hidden").unwrap();
        let recording = RecordingFs {
            inner: fs(root.path()),
            read_dirs: std::cell::RefCell::new(Vec::new()),
        };

        crate::run(args("**", None), &recording).unwrap();

        assert!(!recording
            .read_dirs
            .borrow()
            .iter()
            .any(|path| path.ends_with("ignored")));
    }

    #[test]
    fn limit_caps_sorted_results_and_reports_a_partial_listing() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("c.txt"), "c").unwrap();
        std::fs::write(root.path().join("a.txt"), "a").unwrap();
        std::fs::write(root.path().join("b.txt"), "b").unwrap();

        let output = crate::run(args("**", Some(2)), &fs(root.path())).unwrap();

        assert_eq!(output.paths, vec!["a.txt".to_owned(), "b.txt".to_owned()]);
        assert!(output.limit_reached);
        assert_eq!(
            output.text,
            "a.txt\nb.txt\n\n[2 results limit reached. Use limit=4 for more, or refine pattern]"
        );
    }

    #[test]
    fn invalid_patterns_are_argument_errors() {
        let root = tempfile::tempdir().unwrap();

        let error = crate::run(args("[", None), &fs(root.path())).unwrap_err();

        assert!(error
            .to_string()
            .starts_with("invalid arguments: invalid glob pattern"));
    }

    #[test]
    fn nonpositive_limit_floors_to_one_and_maximum_i64_limit_is_safe() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "content").unwrap();

        let zero = crate::run(args("**", Some(0)), &fs(root.path())).unwrap();
        let negative = crate::run(args("**", Some(-10)), &fs(root.path())).unwrap();
        let maximum = crate::run(args("**", Some(i64::MAX)), &fs(root.path())).unwrap();

        assert_eq!(
            zero.text,
            "file.txt\n\n[1 results limit reached. Use limit=2 for more, or refine pattern]"
        );
        assert_eq!(negative.text, zero.text);
        assert_eq!(maximum.paths, vec!["file.txt"]);
        assert!(!maximum.limit_reached);
    }

    #[test]
    fn no_matches_directories_and_slash_patterns_match_pi() {
        // Pi behavior sheet items 35, 36, and 39; source: core/tools/find.ts:254-267,303-352.
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("src/foo/bar")).unwrap();
        std::fs::write(root.path().join("src/foo/bar/example.spec.ts"), "").unwrap();

        let no_match = crate::run(args("*.nope", None), &fs(root.path())).unwrap();
        let directories = crate::run(args("bar", None), &fs(root.path())).unwrap();
        let path_pattern = crate::run(args("src/**/*.spec.ts", None), &fs(root.path())).unwrap();
        let leading_double_star =
            crate::run(args("**/src/**/*.spec.ts", None), &fs(root.path())).unwrap();
        let root_pattern = crate::run(args("/", None), &fs(root.path())).unwrap();

        assert_eq!(no_match.text, "No files found matching pattern");
        assert_eq!(directories.paths, vec!["src/foo/bar/".to_owned()]);
        assert_eq!(directories.text, "src/foo/bar/");
        assert_eq!(path_pattern.paths, vec!["src/foo/bar/example.spec.ts"]);
        assert_eq!(leading_double_star.paths, path_pattern.paths);
        assert_eq!(root_pattern.text, "No files found matching pattern");
    }

    #[test]
    fn absolute_search_root_stays_relative_and_preserves_directory_separators() {
        // Pi regression 6104: root search paths must not drop the first segment.
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(root.path().join("home/user/project")).unwrap();
        std::fs::write(root.path().join("home/user/project/file.txt"), "").unwrap();
        let mut search = args("**", None);
        search.path = Some(root.path().to_path_buf());

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(
            output.paths,
            vec![
                "home/".to_owned(),
                "home/user/".to_owned(),
                "home/user/project/".to_owned(),
                "home/user/project/file.txt".to_owned(),
            ]
        );
    }

    #[test]
    fn byte_truncation_and_contract_strings_match_pi() {
        // Pi behavior sheet items 1, 39, and 42; source: core/tools/find.ts:123-133,328-352.
        let root = tempfile::tempdir().unwrap();
        for index in 0..240 {
            std::fs::write(
                root.path()
                    .join(format!("{}-{index:03}.txt", "x".repeat(220))),
                "",
            )
            .unwrap();
        }

        let output = crate::run(args("*.txt", None), &fs(root.path())).unwrap();
        let combined = crate::run(args("*.txt", Some(240)), &fs(root.path())).unwrap();

        assert!(output.truncated);
        assert!(output.text.ends_with("\n\n[50.0KB limit reached]"));
        assert!(combined.text.ends_with(
            "\n\n[240 results limit reached. Use limit=480 for more, or refine pattern. 50.0KB limit reached]"
        ));
        assert_eq!(
            crate::contract().description,
            "Search for files by glob pattern. Returns matching file paths relative to the search directory. Respects .gitignore. Output is truncated to 1000 results or 50KB (whichever is hit first)."
        );
        assert_eq!(
            crate::contract().input_schema,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "pattern": {"type": "string", "description": "Glob pattern to match files, e.g. '*.ts', '**/*.json', or 'src/**/*.spec.ts'"},
                    "path": {"type": "string", "description": "Directory to search in (default: current directory)"},
                    "limit": {"type": "number", "description": "Maximum number of results (default: 1000)"}
                },
                "required": ["pattern"]
            })
        );
    }

    struct RecordingFs {
        inner: verlet_tool_core::StdFs,
        read_dirs: std::cell::RefCell<Vec<std::path::PathBuf>>,
    }

    impl verlet_tool_core::ToolFs for RecordingFs {
        fn read_file(
            &self,
            path: &std::path::Path,
        ) -> Result<Vec<u8>, verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::read_file(&self.inner, path)
        }

        fn read_file_bounded(
            &self,
            path: &std::path::Path,
            max_bytes: usize,
        ) -> Result<Vec<u8>, verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::read_file_bounded(&self.inner, path, max_bytes)
        }

        fn write_file(
            &self,
            path: &std::path::Path,
            content: &[u8],
        ) -> Result<(), verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::write_file(&self.inner, path, content)
        }

        fn mkdir(
            &self,
            path: &std::path::Path,
            recursive: bool,
        ) -> Result<(), verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::mkdir(&self.inner, path, recursive)
        }

        fn stat(
            &self,
            path: &std::path::Path,
        ) -> Result<verlet_tool_core::FileStat, verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::stat(&self.inner, path)
        }

        fn read_dir(
            &self,
            path: &std::path::Path,
        ) -> Result<Vec<verlet_tool_core::DirEntry>, verlet_tool_core::ToolFsError> {
            self.read_dirs.borrow_mut().push(path.to_path_buf());
            verlet_tool_core::ToolFs::read_dir(&self.inner, path)
        }

        fn exists(&self, path: &std::path::Path) -> Result<bool, verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::exists(&self.inner, path)
        }
    }
}
