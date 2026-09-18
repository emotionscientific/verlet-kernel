//! `read` — Pi-compatible line-range file access.
//!
//! Image viewing remains a separate optional package. Text behavior follows
//! Pi, including automatic complete-line head truncation at 2,000 lines or
//! 50 KiB and addressable continuation notices.
//!
//! Pinned semantics:
//! - `offset` is 1-indexed; `offset` past end of file is an error naming
//!   the file's total line count.
//! - No `limit` → to end of file before Pi's automatic truncation.
//! - Content is returned verbatim (no line numbering, no tab expansion);
//!   UTF-8 with lossy replacement for invalid bytes.
//! - Caller limits and automatic truncation use Pi's exact continuation text.
//! - Line accounting follows Pi's newline split: an empty file is one empty
//!   logical line, and a terminal newline creates a final empty line.
//! - Files over the shared 8 MiB limit return a structured tool error; the
//!   4 MiB result cap remains a final structured-result backstop.

#[derive(Clone, Debug, serde::Deserialize)]
pub struct ReadArgs {
    /// Path to the file to read (relative to the workspace root or absolute
    /// within the granted scope).
    pub path: std::path::PathBuf,
    /// Line number to start reading from (1-indexed).
    pub offset: Option<i64>,
    /// Maximum number of lines to read.
    pub limit: Option<i64>,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct ReadOutput {
    /// File content for the selected range, verbatim, plus the range
    /// trailer when partial.
    pub text: String,
    /// 1-indexed first line returned.
    pub start_line: u64,
    /// 1-indexed last line returned.
    pub end_line: u64,
    /// Total lines in the file.
    pub total_lines: u64,
    /// Present only when automatic 2,000-line/50 KiB truncation occurred.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub truncation: Option<verlet_tool_core::TruncationResult>,
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "read",
        description: "Read the contents of a file. For text files, output is truncated to 2000 lines or 50KB (whichever is hit first). Use offset/limit for large files. When you need the full file, continue with offset until complete.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file to read (relative or absolute)"},
                "offset": {"type": "number", "description": "Line number to start reading from (1-indexed)"},
                "limit": {"type": "number", "description": "Maximum number of lines to read"}
            },
            "required": ["path"]
        }),
        effect_class: verlet_tool_core::EffectClass::Pure,
    }
}

pub fn run(
    args: ReadArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<ReadOutput, verlet_tool_core::ToolError> {
    let input_path = args.path;
    let path = verlet_tool_core::normalize_tool_path(&input_path);
    let stat = fs.stat(&path)?;
    if stat.is_dir {
        return Err(verlet_tool_core::ToolError::Failed(format!(
            "Path is a directory: {}",
            path.display()
        )));
    }
    if !stat.is_file {
        return Err(verlet_tool_core::ToolError::Failed(format!(
            "Path is not a file: {}",
            path.display()
        )));
    }
    if stat.size > u64::try_from(verlet_tool_core::MAX_FILE_BYTES).unwrap_or(u64::MAX) {
        return Err(oversized_file_error(&path));
    }
    let bytes = fs
        .read_file_bounded(&path, verlet_tool_core::MAX_FILE_BYTES)
        .map_err(|error| match error {
            verlet_tool_core::ToolFsError::FileTooLarge { path, .. } => oversized_file_error(&path),
            error => error.into(),
        })?;
    let total_line_count = file_line_count(&bytes);
    let total_lines = u64::try_from(total_line_count).unwrap_or(u64::MAX);
    let requested_offset = args.offset;
    let start_index_i64 = match requested_offset {
        Some(offset) if offset != 0 => offset.saturating_sub(1).max(0),
        _ => 0,
    };

    if i128::from(start_index_i64) >= total_line_count as i128 {
        return Err(offset_past_end(requested_offset.unwrap_or(1), total_lines));
    }

    let start_index = usize::try_from(start_index_i64)
        .map_err(|_| verlet_tool_core::ToolError::InvalidArgs("offset is too large".to_owned()))?;
    let (end_index, user_limit_end) = match args.limit {
        Some(limit) => {
            let numeric_end =
                (start_index_i64 as i128 + i128::from(limit)).min(total_line_count as i128);
            let slice_end = if numeric_end < 0 {
                (total_line_count as i128 + numeric_end).max(0)
            } else {
                numeric_end
            }
            .clamp(0, total_line_count as i128) as usize;
            (slice_end.max(start_index), Some(numeric_end))
        }
        None => (total_line_count, None),
    };
    let selected_bytes = line_range(&bytes, start_index, end_index, total_line_count);
    let selected_content = String::from_utf8_lossy(selected_bytes);
    let truncation = verlet_tool_core::truncate_head(
        &selected_content,
        verlet_tool_core::DEFAULT_MAX_LINES,
        verlet_tool_core::DEFAULT_MAX_BYTES,
    );
    let start_line = u64::try_from(start_index.saturating_add(1)).unwrap_or(u64::MAX);
    let mut end_line = u64::try_from(end_index).unwrap_or(u64::MAX);
    let mut text;
    let mut truncation_details = None;

    if truncation.first_line_exceeds_limit {
        let first_line_size = selected_content
            .split_once('\n')
            .map_or(selected_content.len(), |(line, _)| line.len());
        text = format!(
            "[Line {start_line} is {}, exceeds {} limit. Use bash: sed -n '{start_line}p' {} | head -c {}]",
            verlet_tool_core::format_size(first_line_size),
            verlet_tool_core::format_size(verlet_tool_core::DEFAULT_MAX_BYTES),
            input_path.display(),
            verlet_tool_core::DEFAULT_MAX_BYTES,
        );
        end_line = start_line.saturating_sub(1);
        truncation_details = Some(truncation);
    } else if truncation.truncated {
        let output_lines = u64::try_from(truncation.output_lines).unwrap_or(u64::MAX);
        end_line = start_line.saturating_add(output_lines).saturating_sub(1);
        let next_offset = end_line.saturating_add(1);
        text = truncation.content.clone();
        if truncation.truncated_by == Some(verlet_tool_core::TruncatedBy::Lines) {
            text.push_str(&format!(
                "\n\n[Showing lines {start_line}-{end_line} of {total_lines}. Use offset={next_offset} to continue.]"
            ));
        } else {
            text.push_str(&format!(
                "\n\n[Showing lines {start_line}-{end_line} of {total_lines} ({} limit). Use offset={next_offset} to continue.]",
                verlet_tool_core::format_size(verlet_tool_core::DEFAULT_MAX_BYTES),
            ));
        }
        truncation_details = Some(truncation);
    } else if let Some(numeric_end) = user_limit_end {
        text = truncation.content;
        if numeric_end < total_line_count as i128 {
            let remaining = total_line_count as i128 - numeric_end;
            let next_offset = numeric_end + 1;
            text.push_str(&format!(
                "\n\n[{remaining} more lines in file. Use offset={next_offset} to continue.]"
            ));
        }
    } else {
        text = truncation.content;
    }
    if text.len() > verlet_tool_core::MAX_RESULT_BYTES {
        return Err(verlet_tool_core::ToolError::ResultTooLarge);
    }

    Ok(ReadOutput {
        text,
        start_line,
        end_line,
        total_lines,
        truncation: truncation_details,
    })
}

fn file_line_count(bytes: &[u8]) -> usize {
    bytes
        .iter()
        .filter(|byte| **byte == b'\n')
        .count()
        .saturating_add(1)
}

fn line_range(bytes: &[u8], start_index: usize, end_index: usize, total_lines: usize) -> &[u8] {
    let start = line_start_byte(bytes, start_index);
    if end_index <= start_index {
        return &bytes[start..start];
    }
    let end = if end_index >= total_lines {
        bytes.len()
    } else {
        line_start_byte(bytes, end_index).saturating_sub(1)
    };
    &bytes[start..end.max(start)]
}

fn line_start_byte(bytes: &[u8], line_index: usize) -> usize {
    if line_index == 0 {
        return 0;
    }
    bytes
        .iter()
        .enumerate()
        .filter(|(_, byte)| **byte == b'\n')
        .nth(line_index.saturating_sub(1))
        .map_or(bytes.len(), |(index, _)| index.saturating_add(1))
}

fn offset_past_end(offset: i64, total_lines: u64) -> verlet_tool_core::ToolError {
    verlet_tool_core::ToolError::Failed(format!(
        "Offset {offset} is beyond end of file ({total_lines} lines total)"
    ))
}

fn oversized_file_error(path: &std::path::Path) -> verlet_tool_core::ToolError {
    verlet_tool_core::ToolError::Failed(format!(
        "file exceeds {} byte read limit: {}; offset/limit cannot bypass this limit, use bash to inspect a bounded range or split the file",
        verlet_tool_core::MAX_FILE_BYTES,
        path.display()
    ))
}

#[cfg(test)]
mod tests {
    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    fn args(path: &str, offset: Option<i64>, limit: Option<i64>) -> crate::ReadArgs {
        crate::ReadArgs {
            path: std::path::PathBuf::from(path),
            offset,
            limit,
        }
    }

    #[test]
    fn reads_a_whole_file_verbatim() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "first\nsecond\n").unwrap();

        let output = crate::run(args("file.txt", None, None), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::ReadOutput {
                text: "first\nsecond\n".to_owned(),
                start_line: 1,
                end_line: 3,
                total_lines: 3,
                truncation: None,
            }
        );
    }

    #[test]
    fn reads_an_offset_and_limit_window_with_a_trailer() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "one\ntwo\nthree\nfour").unwrap();

        let output = crate::run(args("file.txt", Some(2), Some(2)), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::ReadOutput {
                text: "two\nthree\n\n[1 more lines in file. Use offset=4 to continue.]".to_owned(),
                start_line: 2,
                end_line: 3,
                total_lines: 4,
                truncation: None,
            }
        );
    }

    #[test]
    fn rejects_an_offset_past_the_end_and_names_the_total() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "one\ntwo").unwrap();

        let error = crate::run(args("file.txt", Some(3), None), &fs(root.path())).unwrap_err();

        assert_eq!(
            error.to_string(),
            "Offset 3 is beyond end of file (2 lines total)"
        );
    }

    #[test]
    fn reads_an_empty_file() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("empty.txt"), "").unwrap();

        let output = crate::run(args("empty.txt", None, None), &fs(root.path())).unwrap();

        assert_eq!(
            output,
            crate::ReadOutput {
                text: String::new(),
                start_line: 1,
                end_line: 1,
                total_lines: 1,
                truncation: None,
            }
        );
    }

    #[test]
    fn a_directory_is_a_structured_tool_error() {
        let root = tempfile::tempdir().unwrap();

        let error = crate::run(args(".", None, None), &fs(root.path())).unwrap_err();

        assert_eq!(error.to_string(), "Path is a directory: .");
    }

    #[test]
    fn an_oversized_file_is_an_actionable_structured_error_for_every_window() {
        let root = tempfile::tempdir().unwrap();
        let content = vec![b'x'; verlet_tool_core::MAX_FILE_BYTES.saturating_add(1)];
        std::fs::write(root.path().join("oversized.txt"), content).unwrap();

        let filesystem = fs(root.path());
        let error = crate::run(args("oversized.txt", None, None), &filesystem).unwrap_err();
        let window_error =
            crate::run(args("oversized.txt", Some(1), Some(1)), &filesystem).unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "file exceeds {} byte read limit: oversized.txt; offset/limit cannot bypass this limit, use bash to inspect a bounded range or split the file",
                verlet_tool_core::MAX_FILE_BYTES
            )
        );
        assert_eq!(window_error.to_string(), error.to_string());
    }

    #[test]
    fn reports_an_oversized_first_line_with_pi_text() {
        // Pi behavior sheet item 9; source: core/tools/read.ts:297-301.
        let root = tempfile::tempdir().unwrap();
        let content = vec![b'x'; verlet_tool_core::DEFAULT_MAX_BYTES + 1];
        std::fs::write(root.path().join("large.txt"), content).unwrap();

        let output = crate::run(args("large.txt", None, None), &fs(root.path())).unwrap();

        assert_eq!(
            output.text,
            "[Line 1 is 50.0KB, exceeds 50.0KB limit. Use bash: sed -n '1p' large.txt | head -c 51200]"
        );
        assert!(output.truncation.unwrap().first_line_exceeds_limit);
    }

    #[test]
    fn replaces_invalid_utf8_lossily() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("invalid.txt"), [b'a', 0xff, b'\n']).unwrap();

        let output = crate::run(args("invalid.txt", None, None), &fs(root.path())).unwrap();

        assert_eq!(output.text, "a\u{fffd}\n");
    }

    #[test]
    fn byte_truncation_uses_complete_lines_and_pi_notice() {
        // Pi behavior sheet item 9; source: core/tools/read.ts:294-312.
        let root = tempfile::tempdir().unwrap();
        let line = format!("{}\n", "x".repeat(1024));
        std::fs::write(root.path().join("large.txt"), line.repeat(60)).unwrap();

        let output = crate::run(args("large.txt", None, None), &fs(root.path())).unwrap();

        assert!(output.text.ends_with(
            "\n\n[Showing lines 1-49 of 61 (50.0KB limit). Use offset=50 to continue.]"
        ));
    }

    #[test]
    fn zero_and_negative_offsets_start_at_the_first_line_and_zero_limit_is_empty() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "content").unwrap();
        let fs = fs(root.path());

        let zero_offset = crate::run(args("file.txt", Some(0), None), &fs).unwrap();
        let negative_offset = crate::run(args("file.txt", Some(-5), None), &fs).unwrap();
        let zero_limit = crate::run(args("file.txt", None, Some(0)), &fs).unwrap();

        assert_eq!(zero_offset.text, "content");
        assert_eq!(negative_offset.text, "content");
        assert_eq!(
            zero_limit.text,
            "\n\n[1 more lines in file. Use offset=1 to continue.]"
        );
    }

    #[test]
    fn maximum_i64_limit_is_bounded_by_the_available_lines() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "one\ntwo").unwrap();

        let output =
            crate::run(args("file.txt", Some(1), Some(i64::MAX)), &fs(root.path())).unwrap();
        let offset_error = crate::run(
            args("file.txt", Some(i64::MAX), Some(i64::MAX)),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(output.text, "one\ntwo");
        assert_eq!(
            offset_error.to_string(),
            format!("Offset {} is beyond end of file (2 lines total)", i64::MAX)
        );
    }

    #[test]
    fn automatically_truncates_at_two_thousand_lines_with_pi_notice() {
        // Pi behavior sheet item 9; source: core/tools/read.ts:294-312.
        let root = tempfile::tempdir().unwrap();
        let content = (1..=2001)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(root.path().join("large.txt"), content).unwrap();

        let output = crate::run(args("large.txt", None, None), &fs(root.path())).unwrap();

        assert!(output.text.ends_with(
            "line 2000\n\n[Showing lines 1-2000 of 2001. Use offset=2001 to continue.]"
        ));
    }

    #[test]
    fn caller_limit_precedes_automatic_line_truncation_at_the_exact_boundary() {
        // Pi source: core/tools/read.ts:284-317 and truncate.ts:78-160.
        let root = tempfile::tempdir().unwrap();
        let content = std::iter::repeat_n("x", 2002)
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(root.path().join("large.txt"), content).unwrap();
        let filesystem = fs(root.path());

        let exact = crate::run(args("large.txt", Some(2), Some(2000)), &filesystem).unwrap();
        let over = crate::run(args("large.txt", Some(2), Some(2001)), &filesystem).unwrap();

        assert!(exact.truncation.is_none());
        assert!(exact
            .text
            .ends_with("\n\n[1 more lines in file. Use offset=2002 to continue.]"));
        assert_eq!(exact.start_line, 2);
        assert_eq!(exact.end_line, 2001);
        assert!(over.truncation.is_some());
        assert!(over
            .text
            .ends_with("\n\n[Showing lines 2-2001 of 2002. Use offset=2002 to continue.]"));
    }

    #[test]
    fn contract_and_benign_path_normalization_match_pi() {
        // Pi behavior sheet items 1 and 4; source: read.ts:209-222 and path-utils.ts:40-49.
        let contract = crate::contract();
        assert_eq!(
            contract.description,
            "Read the contents of a file. For text files, output is truncated to 2000 lines or 50KB (whichever is hit first). Use offset/limit for large files. When you need the full file, continue with offset until complete."
        );
        assert_eq!(
            contract.input_schema,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to the file to read (relative or absolute)"},
                    "offset": {"type": "number", "description": "Line number to start reading from (1-indexed)"},
                    "limit": {"type": "number", "description": "Maximum number of lines to read"}
                },
                "required": ["path"]
            })
        );
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("unicode space.txt"), "ok").unwrap();

        let output = crate::run(
            args("@unicode\u{202f}space.txt", None, None),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(output.text, "ok");
    }
}
