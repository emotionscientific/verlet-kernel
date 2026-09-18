//! `grep` — search file contents for a pattern.
//!
//! Pi's version spawns `rg --json` (auto-downloading ripgrep when
//! missing). Ours runs ripgrep's engine in-process — the `grep-regex` and
//! `grep-searcher` crates search over any reader — fed by the same
//! `ToolFs` walk as `tool-glob`. No spawned binaries, no downloads, and
//! the whole reason this exists as a function tool is preserved: the
//! pattern arrives as a JSON string, no shell quoting, and the output is
//! bounded `file:line` rows.
//!
//! Pinned semantics (mirroring Pi's flags):
//! - Regex by default; `literal: true` = fixed-string. Public argument
//!   `ignoreCase` matches Pi. Same walk rules as `tool-glob` (gitignore, hidden files
//!   included, `.git/` skipped), with optional `glob` file filter.
//! - Output rows: `path:line: text` with root-relative paths; `context`
//!   emits one independent block per match and formats non-match rows as
//!   `path-line- text`, with no block separators.
//! - Lines longer than 500 UTF-16 code units are clipped with Pi's suffix.
//! - `limit` (default 100) caps match count; search stops early when
//!   reached and emits Pi's actionable notice.
//! - Final text is complete-line head-truncated at 50 KiB.
//! - Non-UTF-8 files, files containing NUL, and files over 8 MiB are skipped.
//! - Deterministic file order (same as glob's sort), so identical state
//!   yields identical output on every backend.

pub const DEFAULT_LIMIT: i64 = 100;
pub const MAX_LINE_CHARS: usize = 500;

#[derive(Clone, Debug, serde::Deserialize)]
pub struct GrepArgs {
    /// Search pattern (regex, or literal string with `literal: true`).
    pub pattern: String,
    /// Directory or file to search (default: workspace root).
    pub path: Option<std::path::PathBuf>,
    /// Filter files by glob pattern, e.g. `*.ts`.
    pub glob: Option<String>,
    #[serde(default, rename = "ignoreCase")]
    pub ignore_case: bool,
    #[serde(default)]
    pub literal: bool,
    /// Lines of context before and after each match (default 0).
    pub context: Option<i64>,
    /// Maximum number of matches (default 100).
    pub limit: Option<i64>,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct GrepOutput {
    /// Rendered match rows (`path:line: text`).
    pub text: String,
    pub match_count: u64,
    pub limit_reached: bool,
    pub truncated: bool,
    pub lines_truncated: bool,
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "grep",
        description: "Search file contents for a pattern. Returns matching lines with file paths and line numbers. Respects .gitignore. Output is truncated to 100 matches or 50KB (whichever is hit first). Long lines are truncated to 500 chars.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "pattern": {"type": "string", "description": "Search pattern (regex or literal string)"},
                "path": {"type": "string", "description": "Directory or file to search (default: current directory)"},
                "glob": {"type": "string", "description": "Filter files by glob pattern, e.g. '*.ts' or '**/*.spec.ts'"},
                "ignoreCase": {"type": "boolean", "description": "Case-insensitive search (default: false)"},
                "literal": {"type": "boolean", "description": "Treat pattern as literal string instead of regex (default: false)"},
                "context": {"type": "number", "description": "Number of lines to show before and after each match (default: 0)"},
                "limit": {"type": "number", "description": "Maximum number of matches to return (default: 100)"}
            },
            "required": ["pattern"]
        }),
        effect_class: verlet_tool_core::EffectClass::Pure,
    }
}

pub fn run(
    args: GrepArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<GrepOutput, verlet_tool_core::ToolError> {
    let effective_limit = args.limit.unwrap_or(DEFAULT_LIMIT).max(1);
    let limit = u64::try_from(effective_limit).unwrap_or(u64::MAX);

    let mut matcher_builder = grep_regex::RegexMatcherBuilder::new();
    matcher_builder
        .case_insensitive(args.ignore_case)
        .fixed_strings(args.literal);
    let matcher = matcher_builder.build(&args.pattern).map_err(|error| {
        verlet_tool_core::ToolError::InvalidArgs(format!(
            "invalid search pattern {:?}: {error}",
            args.pattern
        ))
    })?;
    let glob_matcher = args
        .glob
        .as_deref()
        .map(|pattern| {
            globset::Glob::new(pattern)
                .map(|glob| glob.compile_matcher())
                .map_err(|error| {
                    verlet_tool_core::ToolError::InvalidArgs(format!(
                        "invalid glob pattern {pattern:?}: {error}"
                    ))
                })
        })
        .transpose()?;
    let input_root = args.path.unwrap_or_else(|| std::path::PathBuf::from("."));
    let root = verlet_tool_core::normalize_tool_path(&input_root);
    let files = verlet_tool_core::walk_files(&root, fs).map_err(|error| match error {
        verlet_tool_core::ToolError::Fs(verlet_tool_core::ToolFsError::NotFound(_)) => {
            verlet_tool_core::ToolError::Failed(format!("Path not found: {}", root.display()))
        }
        error => error,
    })?;
    let context = usize::try_from(args.context.unwrap_or(0).max(0)).unwrap_or(usize::MAX);
    let mut blocks = Vec::new();
    let mut rows = Vec::new();
    let mut match_count = 0_u64;
    let mut limit_reached = false;
    let mut lines_truncated = false;

    for file in files {
        if file.is_dir {
            continue;
        }
        if glob_matcher
            .as_ref()
            .is_some_and(|glob| !glob.is_match(std::path::Path::new(&file.relative_path)))
        {
            continue;
        }

        let bytes = match verlet_tool_core::read_walk_file_bounded(
            &file,
            fs,
            verlet_tool_core::MAX_FILE_BYTES,
        )? {
            Some(bytes) => bytes,
            None => continue,
        };
        if std::str::from_utf8(&bytes).is_err() || bytes.contains(&0) {
            continue;
        }

        let remaining = limit.saturating_sub(match_count);
        let mut searcher_builder = grep_searcher::SearcherBuilder::new();
        searcher_builder
            .line_number(true)
            .bom_sniffing(false)
            .max_matches(Some(remaining));
        let mut searcher = searcher_builder.build();
        let mut sink = MatchLineSink {
            matches: Vec::new(),
        };
        searcher
            .search_slice(&matcher, &bytes, &mut sink)
            .map_err(|error| {
                verlet_tool_core::ToolError::Failed(format!(
                    "failed to search {}: {error}",
                    file.relative_path
                ))
            })?;

        match_count =
            match_count.saturating_add(u64::try_from(sink.matches.len()).unwrap_or(u64::MAX));
        if context == 0 {
            for matched in sink.matches {
                let (row, truncated) = render_line(
                    &file.relative_path,
                    matched.line_number,
                    true,
                    &matched.bytes,
                );
                rows.push(row);
                lines_truncated |= truncated;
            }
        } else {
            let (file_blocks, truncated) =
                render_context_blocks(&file.relative_path, &bytes, &sink.matches, context);
            blocks.extend(file_blocks);
            lines_truncated |= truncated;
        }

        if match_count >= limit {
            limit_reached = true;
            break;
        }
    }

    if match_count == 0 {
        return Ok(GrepOutput {
            text: "No matches found".to_owned(),
            match_count,
            limit_reached: false,
            truncated: false,
            lines_truncated: false,
        });
    }

    let raw_output = if context == 0 {
        rows.join("\n")
    } else {
        blocks
            .into_iter()
            .map(|block| block.join("\n"))
            .collect::<Vec<_>>()
            .join("\n")
    };
    let truncation = verlet_tool_core::truncate_head(
        &raw_output,
        usize::MAX,
        verlet_tool_core::DEFAULT_MAX_BYTES,
    );
    let mut text = truncation.content.clone();
    let mut notices = Vec::new();
    if limit_reached {
        notices.push(format!(
            "{effective_limit} matches limit reached. Use limit={} for more, or refine pattern",
            effective_limit.saturating_mul(2),
        ));
    }
    if truncation.truncated {
        notices.push(format!(
            "{} limit reached",
            verlet_tool_core::format_size(verlet_tool_core::DEFAULT_MAX_BYTES)
        ));
    }
    if lines_truncated {
        notices.push(format!(
            "Some lines truncated to {MAX_LINE_CHARS} chars. Use read tool to see full lines"
        ));
    }
    if !notices.is_empty() {
        text.push_str(&format!("\n\n[{}]", notices.join(". ")));
    }
    if text.len() > verlet_tool_core::MAX_RESULT_BYTES {
        return Err(verlet_tool_core::ToolError::ResultTooLarge);
    }

    Ok(GrepOutput {
        text,
        match_count,
        limit_reached,
        truncated: truncation.truncated,
        lines_truncated,
    })
}

struct MatchLineSink {
    matches: Vec<MatchLine>,
}

struct MatchLine {
    line_number: u64,
    bytes: Vec<u8>,
}

impl grep_searcher::Sink for MatchLineSink {
    type Error = std::io::Error;

    fn matched(
        &mut self,
        _searcher: &grep_searcher::Searcher,
        matched: &grep_searcher::SinkMatch<'_>,
    ) -> Result<bool, Self::Error> {
        if let Some(line_number) = matched.line_number() {
            let bytes = matched
                .bytes()
                .strip_suffix(b"\n")
                .unwrap_or_else(|| matched.bytes())
                .to_vec();
            self.matches.push(MatchLine { line_number, bytes });
        }
        Ok(true)
    }
}

fn render_context_blocks(
    path: &str,
    bytes: &[u8],
    matches: &[MatchLine],
    context: usize,
) -> (Vec<Vec<String>>, bool) {
    if matches.is_empty() {
        return (Vec::new(), false);
    }
    let line_count = file_line_count(bytes);
    let context = u64::try_from(context).unwrap_or(u64::MAX);
    let mut blocks = Vec::new();
    let mut any_truncated = false;
    for matched in matches {
        let line_number = matched.line_number;
        let start = line_number.saturating_sub(context).max(1);
        let end = line_number.saturating_add(context).min(line_count);
        let mut block = Vec::new();
        for current in start..=end {
            let (row, truncated) = render_line(
                path,
                current,
                current == line_number,
                line_at(bytes, current),
            );
            block.push(row);
            any_truncated |= truncated;
        }
        blocks.push(block);
    }
    (blocks, any_truncated)
}

fn render_line(path: &str, line_number: u64, is_match: bool, bytes: &[u8]) -> (String, bool) {
    let text = String::from_utf8_lossy(bytes);
    let text = text.strip_suffix('\r').unwrap_or(&text);
    let (mut clipped, was_truncated) = verlet_tool_core::truncate_utf16(text, MAX_LINE_CHARS);
    if was_truncated {
        clipped.push_str("... [truncated]");
    }
    let row = if is_match {
        format!("{path}:{line_number}: {clipped}")
    } else {
        format!("{path}-{line_number}- {clipped}")
    };
    (row, was_truncated)
}

fn line_at(bytes: &[u8], line_number: u64) -> &[u8] {
    let index = usize::try_from(line_number.saturating_sub(1)).unwrap_or(usize::MAX);
    bytes
        .split(|byte| *byte == b'\n')
        .nth(index)
        .unwrap_or_default()
}

fn file_line_count(bytes: &[u8]) -> u64 {
    let count = bytes
        .iter()
        .filter(|byte| **byte == b'\n')
        .count()
        .saturating_add(usize::from(!bytes.ends_with(b"\n")));
    u64::try_from(count).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    fn args(pattern: &str) -> crate::GrepArgs {
        crate::GrepArgs {
            pattern: pattern.to_owned(),
            path: None,
            glob: None,
            ignore_case: false,
            literal: false,
            context: None,
            limit: None,
        }
    }

    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    #[test]
    fn renders_plain_rows_in_deterministic_file_order() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("b.txt"), "needle b\n").unwrap();
        std::fs::write(root.path().join("a.txt"), "zero\nNeedle a\n").unwrap();
        let mut search = args("needle");
        search.ignore_case = true;

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(output.text, "a.txt:2: Needle a\nb.txt:1: needle b");
        assert_eq!(output.match_count, 2);
        assert!(!output.limit_reached);
    }

    #[test]
    fn renders_independent_context_blocks_without_separators() {
        // Pi behavior sheet items 26 and 27; source: core/tools/grep.ts:255-273,321-338.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "before a\nhit one\nafter a\ngap one\ngap two\nbefore b\nhit two\nafter b\n",
        )
        .unwrap();
        let mut search = args("hit");
        search.context = Some(1);

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(
            output.text,
            "file.txt-1- before a\n\
             file.txt:2: hit one\n\
             file.txt-3- after a\n\
             file.txt-6- before b\n\
             file.txt:7: hit two\n\
             file.txt-8- after b"
        );
    }

    #[test]
    fn overlapping_context_is_repeated_for_each_match() {
        // Pi behavior sheet item 27; source: core/tools/grep.ts:321-338.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "before\nhit one\nhit two\nafter\n",
        )
        .unwrap();
        let mut search = args("hit");
        search.context = Some(1);

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(
            output.text,
            "file.txt-1- before\nfile.txt:2: hit one\nfile.txt-3- hit two\nfile.txt-2- hit one\nfile.txt:3: hit two\nfile.txt-4- after"
        );
    }

    #[test]
    fn clips_long_rendered_lines_at_five_hundred_utf16_units() {
        // Pi behavior sheet item 28; source: core/tools/truncate.ts:264-275.
        let root = tempfile::tempdir().unwrap();
        let line = format!("{}needle", "x".repeat(crate::MAX_LINE_CHARS));
        std::fs::write(root.path().join("long.txt"), line).unwrap();

        let output = crate::run(args("needle"), &fs(root.path())).unwrap();

        assert_eq!(
            output.text,
            format!(
                "long.txt:1: {}... [truncated]\n\n[Some lines truncated to 500 chars. Use read tool to see full lines]",
                "x".repeat(crate::MAX_LINE_CHARS)
            )
        );
    }

    #[test]
    fn clips_emoji_on_exact_and_split_utf16_boundaries() {
        // Pi source: core/tools/truncate.ts:264-275. A split surrogate is
        // represented lossily because Rust model-facing text must remain UTF-8.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("a.txt"),
            format!("{}🙂needle", "x".repeat(498)),
        )
        .unwrap();
        std::fs::write(
            root.path().join("b.txt"),
            format!("{}🙂needle", "x".repeat(499)),
        )
        .unwrap();

        let output = crate::run(args("needle"), &fs(root.path())).unwrap();

        assert_eq!(
            output.text,
            format!(
                "a.txt:1: {}🙂... [truncated]\nb.txt:1: {}\u{fffd}... [truncated]\n\n[Some lines truncated to 500 chars. Use read tool to see full lines]",
                "x".repeat(498),
                "x".repeat(499),
            )
        );
    }

    #[test]
    fn literal_mode_matches_regex_metacharacters_exactly() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("literal.txt"), "value a+b [x] done\n").unwrap();
        let mut search = args("a+b [x]");
        search.literal = true;

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(output.text, "literal.txt:1: value a+b [x] done");
        assert_eq!(output.match_count, 1);
    }

    #[test]
    fn no_matches_uses_pi_text() {
        // Pi behavior sheet item 25; source: core/tools/grep.ts:303-318.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "haystack\n").unwrap();

        let output = crate::run(args("needle"), &fs(root.path())).unwrap();

        assert_eq!(output.text, "No matches found");
    }

    #[test]
    fn match_limit_stops_before_reading_the_next_file() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("a.txt"), "hit\n").unwrap();
        std::fs::write(root.path().join("b.txt"), "hit\n").unwrap();
        let recording = RecordingFs {
            inner: fs(root.path()),
            reads: std::cell::RefCell::new(Vec::new()),
            replacement_on_bounded_read: std::cell::RefCell::new(None),
            failing_bounded_read: None,
        };
        let mut search = args("hit");
        search.limit = Some(1);

        let output = crate::run(search, &recording).unwrap();

        assert_eq!(
            output.text,
            "a.txt:1: hit\n\n[1 matches limit reached. Use limit=2 for more, or refine pattern]"
        );
        assert_eq!(output.match_count, 1);
        assert!(output.limit_reached);
        assert!(!recording
            .reads
            .borrow()
            .iter()
            .any(|path| path.ends_with("b.txt")));
    }

    #[test]
    fn skips_binary_files_containing_nul() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("binary.dat"), b"hit\0more").unwrap();
        std::fs::write(root.path().join("text.txt"), b"hit\n").unwrap();

        let output = crate::run(args("hit"), &fs(root.path())).unwrap();

        assert_eq!(output.text, "text.txt:1: hit");
        assert_eq!(output.match_count, 1);
    }

    #[test]
    fn skips_nul_anywhere_in_a_file() {
        let root = tempfile::tempdir().unwrap();
        let mut content = vec![b'x'; 8 * 1024];
        content.extend_from_slice(b"\0hit\n");
        std::fs::write(root.path().join("late-nul.dat"), content).unwrap();
        std::fs::write(root.path().join("text.txt"), b"hit\n").unwrap();

        let output = crate::run(args("hit"), &fs(root.path())).unwrap();

        assert_eq!(output.text, "text.txt:1: hit");
        assert_eq!(output.match_count, 1);
    }

    #[test]
    fn skips_non_utf8_files() {
        let root = tempfile::tempdir().unwrap();
        let mut invalid = vec![b'x'; 4 * 1024];
        invalid.extend_from_slice(b"\xffhit\n");
        std::fs::write(root.path().join("invalid.txt"), invalid).unwrap();
        std::fs::write(root.path().join("text.txt"), b"hit\n").unwrap();

        let output = crate::run(args("hit"), &fs(root.path())).unwrap();

        assert_eq!(output.text, "text.txt:1: hit");
        assert_eq!(output.match_count, 1);
    }

    #[test]
    fn skips_oversized_files_without_reading_them() {
        let root = tempfile::tempdir().unwrap();
        let oversized = vec![b'x'; verlet_tool_core::MAX_FILE_BYTES.saturating_add(1)];
        std::fs::write(root.path().join("oversized.txt"), oversized).unwrap();
        std::fs::write(root.path().join("text.txt"), b"hit\n").unwrap();
        let recording = RecordingFs {
            inner: fs(root.path()),
            reads: std::cell::RefCell::new(Vec::new()),
            replacement_on_bounded_read: std::cell::RefCell::new(None),
            failing_bounded_read: None,
        };

        let output = crate::run(args("hit"), &recording).unwrap();

        assert_eq!(output.text, "text.txt:1: hit");
        assert!(!recording
            .reads
            .borrow()
            .iter()
            .any(|path| path.ends_with("oversized.txt")));
    }

    #[test]
    fn a_file_that_grows_after_stat_is_skipped_without_searching_a_partial_buffer() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("changing.txt"), b"hit\n").unwrap();
        let recording = RecordingFs {
            inner: fs(root.path()),
            reads: std::cell::RefCell::new(Vec::new()),
            replacement_on_bounded_read: std::cell::RefCell::new(Some(vec![
                b'x';
                verlet_tool_core::MAX_FILE_BYTES
                    .saturating_add(1)
            ])),
            failing_bounded_read: None,
        };

        let output = crate::run(args("hit"), &recording).unwrap();

        assert_eq!(output.text, "No matches found");
        assert_eq!(output.match_count, 0);
        assert_eq!(
            recording.reads.borrow().as_slice(),
            &[std::path::PathBuf::from("./changing.txt")]
        );
    }

    #[test]
    fn optional_glob_filters_root_relative_file_paths() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("src")).unwrap();
        std::fs::write(root.path().join("src/code.rs"), "hit\n").unwrap();
        std::fs::write(root.path().join("src/code.txt"), "hit\n").unwrap();
        let mut search = args("hit");
        search.glob = Some("**/*.rs".to_owned());

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(output.text, "src/code.rs:1: hit");
    }

    #[test]
    fn a_direct_file_search_uses_the_file_name_as_its_relative_path() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("one.txt"), "hit\n").unwrap();
        let mut search = args("hit");
        search.path = Some(std::path::PathBuf::from("one.txt"));

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert_eq!(output.text, "one.txt:1: hit");
    }

    #[test]
    fn a_directory_search_skips_a_file_that_fails_during_read() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("a-unreadable.txt"), "hit\n").unwrap();
        std::fs::write(root.path().join("visible.txt"), "hit\n").unwrap();
        let recording = RecordingFs {
            inner: fs(root.path()),
            reads: std::cell::RefCell::new(Vec::new()),
            replacement_on_bounded_read: std::cell::RefCell::new(None),
            failing_bounded_read: Some(std::path::PathBuf::from("./a-unreadable.txt")),
        };

        let output = crate::run(args("hit"), &recording).unwrap();

        assert_eq!(output.text, "visible.txt:1: hit");
        assert_eq!(output.match_count, 1);
    }

    #[test]
    fn a_direct_file_search_surfaces_a_read_failure() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("unreadable.txt"), "hit\n").unwrap();
        let recording = RecordingFs {
            inner: fs(root.path()),
            reads: std::cell::RefCell::new(Vec::new()),
            replacement_on_bounded_read: std::cell::RefCell::new(None),
            failing_bounded_read: Some(std::path::PathBuf::from("unreadable.txt")),
        };
        let mut search = args("hit");
        search.path = Some(std::path::PathBuf::from("unreadable.txt"));

        let error = crate::run(search, &recording).unwrap_err();

        assert_eq!(error.to_string(), "fixture read failure: unreadable.txt");
    }

    #[test]
    fn invalid_regex_and_glob_patterns_are_argument_errors() {
        let root = tempfile::tempdir().unwrap();
        let regex_error = crate::run(args("["), &fs(root.path())).unwrap_err();
        let mut invalid_glob = args("hit");
        invalid_glob.glob = Some("[".to_owned());
        let glob_error = crate::run(invalid_glob, &fs(root.path())).unwrap_err();

        assert!(regex_error
            .to_string()
            .starts_with("invalid arguments: invalid search pattern"));
        assert!(glob_error
            .to_string()
            .starts_with("invalid arguments: invalid glob pattern"));
    }

    #[test]
    fn numeric_edges_clamp_like_pi() {
        // Pi behavior sheet item 32; source: core/tools/grep.ts:193-195.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "before\nhit\nafter\n").unwrap();

        let mut zero_search = args("hit");
        zero_search.limit = Some(0);
        zero_search.context = Some(-10);
        let zero = crate::run(zero_search, &fs(root.path())).unwrap();
        let mut maximum_search = args("hit");
        maximum_search.limit = Some(i64::MAX);
        maximum_search.context = Some(i64::MAX);
        let maximum = crate::run(maximum_search, &fs(root.path())).unwrap();

        assert_eq!(
            zero.text,
            "file.txt:2: hit\n\n[1 matches limit reached. Use limit=2 for more, or refine pattern]"
        );
        assert_eq!(
            maximum.text,
            "file.txt-1- before\nfile.txt:2: hit\nfile.txt-3- after"
        );
        assert!(!maximum.limit_reached);
    }

    #[test]
    fn result_is_head_truncated_at_fifty_kibibytes() {
        // Pi behavior sheet item 25; source: core/tools/grep.ts:338-366.
        let root = tempfile::tempdir().unwrap();
        let line = format!("{}hit\n", "🙂".repeat(crate::MAX_LINE_CHARS + 1));
        let line_count = 2200;
        std::fs::write(root.path().join("large.txt"), line.repeat(line_count)).unwrap();
        let mut search = args("hit");
        search.limit = Some(line_count as i64);

        let output = crate::run(search, &fs(root.path())).unwrap();

        assert!(output.truncated);
        assert!(output.text.ends_with(
            "\n\n[2200 matches limit reached. Use limit=4400 for more, or refine pattern. 50.0KB limit reached. Some lines truncated to 500 chars. Use read tool to see full lines]"
        ));
    }

    #[test]
    fn glob_and_grep_observe_the_same_shared_walk_file_set() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("nested")).unwrap();
        std::fs::create_dir(root.path().join("ignored")).unwrap();
        std::fs::create_dir(root.path().join(".git")).unwrap();
        std::fs::write(root.path().join(".gitignore"), "# needle\nignored/\n").unwrap();
        std::fs::write(
            root.path().join("nested/.gitignore"),
            "# needle\n*.skip\n!keep.skip\n",
        )
        .unwrap();
        std::fs::write(root.path().join("visible.txt"), "needle\n").unwrap();
        std::fs::write(root.path().join(".hidden"), "needle\n").unwrap();
        std::fs::write(root.path().join("nested/visible.txt"), "needle\n").unwrap();
        std::fs::write(root.path().join("nested/drop.skip"), "needle\n").unwrap();
        std::fs::write(root.path().join("nested/keep.skip"), "needle\n").unwrap();
        std::fs::write(root.path().join("ignored/unseen.txt"), "needle\n").unwrap();
        std::fs::write(root.path().join(".git/unseen.txt"), "needle\n").unwrap();
        let filesystem = fs(root.path());

        let glob = verlet_tool_glob::run(
            verlet_tool_glob::GlobArgs {
                pattern: "**".to_owned(),
                path: None,
                limit: None,
            },
            &filesystem,
        )
        .unwrap();
        let grep = crate::run(args("needle"), &filesystem).unwrap();
        let grep_paths = grep
            .text
            .lines()
            .map(|line| line.split(':').next().unwrap().to_owned())
            .collect::<Vec<_>>();

        assert_eq!(
            glob.paths
                .into_iter()
                .filter(|path| !path.ends_with('/'))
                .collect::<Vec<_>>(),
            grep_paths
        );
    }

    #[test]
    fn contract_and_camel_case_schema_match_pi() {
        // Pi behavior sheet items 1 and 24; source: core/tools/grep.ts:24-36,128-138.
        let contract = crate::contract();
        assert_eq!(
            contract.description,
            "Search file contents for a pattern. Returns matching lines with file paths and line numbers. Respects .gitignore. Output is truncated to 100 matches or 50KB (whichever is hit first). Long lines are truncated to 500 chars."
        );
        assert_eq!(
            contract.input_schema,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "pattern": {"type": "string", "description": "Search pattern (regex or literal string)"},
                    "path": {"type": "string", "description": "Directory or file to search (default: current directory)"},
                    "glob": {"type": "string", "description": "Filter files by glob pattern, e.g. '*.ts' or '**/*.spec.ts'"},
                    "ignoreCase": {"type": "boolean", "description": "Case-insensitive search (default: false)"},
                    "literal": {"type": "boolean", "description": "Treat pattern as literal string instead of regex (default: false)"},
                    "context": {"type": "number", "description": "Number of lines to show before and after each match (default: 0)"},
                    "limit": {"type": "number", "description": "Maximum number of matches to return (default: 100)"}
                },
                "required": ["pattern"]
            })
        );
        let parsed: crate::GrepArgs = serde_json::from_value(serde_json::json!({
            "pattern": "needle",
            "ignoreCase": true
        }))
        .unwrap();
        assert!(parsed.ignore_case);
    }

    struct RecordingFs {
        inner: verlet_tool_core::StdFs,
        reads: std::cell::RefCell<Vec<std::path::PathBuf>>,
        replacement_on_bounded_read: std::cell::RefCell<Option<Vec<u8>>>,
        failing_bounded_read: Option<std::path::PathBuf>,
    }

    impl verlet_tool_core::ToolFs for RecordingFs {
        fn read_file(
            &self,
            path: &std::path::Path,
        ) -> Result<Vec<u8>, verlet_tool_core::ToolFsError> {
            self.reads.borrow_mut().push(path.to_path_buf());
            verlet_tool_core::ToolFs::read_file(&self.inner, path)
        }

        fn read_file_bounded(
            &self,
            path: &std::path::Path,
            max_bytes: usize,
        ) -> Result<Vec<u8>, verlet_tool_core::ToolFsError> {
            self.reads.borrow_mut().push(path.to_path_buf());
            if self.failing_bounded_read.as_deref() == Some(path) {
                return Err(verlet_tool_core::ToolFsError::Io(format!(
                    "fixture read failure: {}",
                    path.display()
                )));
            }
            if let Some(replacement) = self.replacement_on_bounded_read.borrow_mut().take() {
                std::fs::write(self.inner.root.join(path), replacement).unwrap();
            }
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
            verlet_tool_core::ToolFs::read_dir(&self.inner, path)
        }

        fn exists(&self, path: &std::path::Path) -> Result<bool, verlet_tool_core::ToolFsError> {
            verlet_tool_core::ToolFs::exists(&self.inner, path)
        }
    }
}
