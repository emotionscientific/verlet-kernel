//! `edit` — replace exact text spans in one file.
//!
//! Ported from Pi's edit tool (`core/tools/edit.ts`, `edit-diff.ts`).
//! This is the tool with the tricky logic; the implementation ticket ports
//! Pi's test fixtures alongside it.
//!
//! Pinned semantics:
//! - `edits` apply as one atomic batch: all succeed or the file is
//!   untouched.
//! - The public schema uses Pi's `oldText`/`newText` names and accepts Pi's
//!   stringified, single-object, and legacy top-level preparer forms.
//! - Each `oldText` must match exactly once. Match strategy per Pi:
//!   exact match first, then a normalized pass (NFKC, smart quotes to
//!   ASCII, Pi's complete dash and special-space sets, trailing whitespace
//!   stripped per line) that must still be unique.
//! - Zero matches, multiple matches, or overlapping edit spans are
//!   reported with Pi's exact singular/plural errors.
//! - Model-facing text is Pi's success sentence; display diff and unified
//!   patch are structured details.
//! - Line endings: file's existing endings are preserved; `old_text`
//!   matching normalizes CRLF to LF for comparison.
//! - Invalid UTF-8 decodes lossily for matching; untouched lines are spliced
//!   back from their original bytes.
//! - Edit keeps its whole-file atomic behavior under the shared 8 MiB ceiling.
//!   Oversized targets return a structured error and are never silently
//!   skipped or partially changed.

use unicode_normalization::UnicodeNormalization as _;

#[derive(Clone, Debug)]
pub struct EditArgs {
    /// Path of the file to edit (relative to the workspace root or
    /// absolute within the granted scope).
    pub path: std::path::PathBuf,
    /// Replacements to apply as one atomic batch.
    pub edits: Vec<Edit>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Edit {
    /// Text to find. Must match exactly one location in the file.
    pub old_text: String,
    /// Replacement text.
    pub new_text: String,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
pub struct EditOutput {
    /// Pi-compatible model-facing primary output.
    pub text: String,
    pub details: EditDetails,
    pub edits_applied: u32,
}

#[derive(Clone, Debug, serde::Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct EditDetails {
    /// Display-oriented diff with +/- markers and padded line numbers.
    pub diff: String,
    /// Standard unified patch.
    pub patch: String,
    /// First changed line in the new file.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_changed_line: Option<u64>,
}

impl<'de> serde::Deserialize<'de> for EditArgs {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = <serde_json::Value as serde::Deserialize>::deserialize(deserializer)?;
        parse_edit_args(value).map_err(serde::de::Error::custom)
    }
}

/// Pi-compatible prepare/coerce/validate parser for native and embedded JSON
/// boundaries. Errors use Pi's validation envelope and validator messages.
pub fn parse_cli_args(value: serde_json::Value) -> Result<EditArgs, String> {
    parse_edit_args(value)
}

fn parse_edit_args(value: serde_json::Value) -> Result<EditArgs, String> {
    let prepared = prepare_edit_arguments(value);
    let mut converted = prepared.clone();
    coerce_edit_arguments(&mut converted);
    let validation_errors = validate_edit_arguments(&converted);
    if !validation_errors.is_empty() {
        let errors = validation_errors
            .into_iter()
            .map(|(path, message)| format!("  - {path}: {message}"))
            .collect::<Vec<_>>()
            .join("\n");
        let received = serde_json::to_string_pretty(&prepared)
            .unwrap_or_else(|_| "<unserializable arguments>".to_owned());
        return Err(format!(
            "Validation failed for tool \"edit\":\n{errors}\n\nReceived arguments:\n{received}"
        ));
    }

    let mut object = converted
        .as_object()
        .cloned()
        .ok_or_else(|| "validated edit arguments were not an object".to_owned())?;
    let path = object
        .remove("path")
        .and_then(|value| value.as_str().map(std::path::PathBuf::from))
        .ok_or_else(|| "validated edit path was not a string".to_owned())?;
    let edits = object
        .remove("edits")
        .and_then(|value| value.as_array().cloned())
        .ok_or_else(|| "validated edits were not an array".to_owned())?
        .into_iter()
        .map(|value| {
            let edit = value
                .as_object()
                .ok_or_else(|| "validated edit was not an object".to_owned())?;
            let old_text = edit
                .get("oldText")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "validated oldText was not a string".to_owned())?;
            let new_text = edit
                .get("newText")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| "validated newText was not a string".to_owned())?;
            Ok(Edit {
                old_text: old_text.to_owned(),
                new_text: new_text.to_owned(),
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(EditArgs { path, edits })
}

fn prepare_edit_arguments(mut value: serde_json::Value) -> serde_json::Value {
    let Some(object) = value.as_object_mut() else {
        return value;
    };

    if let Some(serde_json::Value::String(encoded)) = object.get("edits") {
        if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(encoded) {
            if parsed.is_array() || is_single_edit_value(&parsed) {
                object.insert("edits".to_owned(), parsed);
            }
        }
    } else if object.get("edits").is_some_and(is_single_edit_value) {
        if let Some(edit) = object.remove("edits") {
            object.insert("edits".to_owned(), serde_json::Value::Array(vec![edit]));
        }
    }

    let legacy = match (object.get("oldText"), object.get("newText")) {
        (Some(serde_json::Value::String(old_text)), Some(serde_json::Value::String(new_text))) => {
            Some(serde_json::json!({"oldText": old_text, "newText": new_text}))
        }
        _ => None,
    };
    if let Some(legacy) = legacy {
        let mut edits = object
            .remove("edits")
            .and_then(|value| value.as_array().cloned())
            .unwrap_or_default();
        edits.push(legacy);
        object.remove("oldText");
        object.remove("newText");
        object.insert("edits".to_owned(), serde_json::Value::Array(edits));
    }
    value
}

fn is_single_edit_value(value: &serde_json::Value) -> bool {
    value.as_object().is_some_and(|edit| {
        edit.get("oldText")
            .is_some_and(serde_json::Value::is_string)
            && edit
                .get("newText")
                .is_some_and(serde_json::Value::is_string)
    })
}

fn coerce_edit_arguments(value: &mut serde_json::Value) {
    let Some(object) = value.as_object_mut() else {
        return;
    };
    if let Some(path) = object.get_mut("path") {
        coerce_string(path);
    }
    let Some(edits) = object.get_mut("edits") else {
        return;
    };
    if !edits.is_array() {
        let value = std::mem::take(edits);
        *edits = serde_json::Value::Array(vec![value]);
    }
    let Some(edits) = edits.as_array_mut() else {
        return;
    };
    for edit in edits {
        let Some(edit) = edit.as_object_mut() else {
            continue;
        };
        if let Some(old_text) = edit.get_mut("oldText") {
            coerce_string(old_text);
        }
        if let Some(new_text) = edit.get_mut("newText") {
            coerce_string(new_text);
        }
    }
}

fn coerce_string(value: &mut serde_json::Value) {
    let converted = match value {
        serde_json::Value::Null => Some("null".to_owned()),
        serde_json::Value::Bool(value) => Some(value.to_string()),
        serde_json::Value::Number(value) => value
            .as_i64()
            .map(|value| value.to_string())
            .or_else(|| value.as_u64().map(|value| value.to_string()))
            .or_else(|| value.as_f64().map(|value| value.to_string())),
        serde_json::Value::String(_)
        | serde_json::Value::Array(_)
        | serde_json::Value::Object(_) => None,
    };
    if let Some(converted) = converted {
        *value = serde_json::Value::String(converted);
    }
}

fn validate_edit_arguments(value: &serde_json::Value) -> Vec<(String, String)> {
    let Some(object) = value.as_object() else {
        return vec![("root".to_owned(), "must be object".to_owned())];
    };
    let missing = ["path", "edits"]
        .into_iter()
        .filter(|property| !object.contains_key(*property))
        .collect::<Vec<_>>();
    let mut errors = Vec::new();
    if let Some(first) = missing.first() {
        errors.push((
            (*first).to_owned(),
            format!("must have required properties {}", missing.join(", ")),
        ));
    }
    if object.get("path").is_some_and(|path| !path.is_string()) {
        errors.push(("path".to_owned(), "must be string".to_owned()));
    }
    if let Some(serde_json::Value::Array(edits)) = object.get("edits") {
        for (index, edit) in edits.iter().enumerate() {
            let Some(edit) = edit.as_object() else {
                errors.push((format!("edits.{index}"), "must be object".to_owned()));
                continue;
            };
            let missing = ["oldText", "newText"]
                .into_iter()
                .filter(|property| !edit.contains_key(*property))
                .collect::<Vec<_>>();
            if let Some(first) = missing.first() {
                errors.push((
                    format!("edits.{index}.{first}"),
                    format!("must have required properties {}", missing.join(", ")),
                ));
            }
            if edit
                .get("oldText")
                .is_some_and(|old_text| !old_text.is_string())
            {
                errors.push((
                    format!("edits.{index}.oldText"),
                    "must be string".to_owned(),
                ));
            }
            if edit
                .get("newText")
                .is_some_and(|new_text| !new_text.is_string())
            {
                errors.push((
                    format!("edits.{index}.newText"),
                    "must be string".to_owned(),
                ));
            }
        }
    }
    errors
}

pub fn contract() -> verlet_tool_core::ToolContract {
    verlet_tool_core::ToolContract {
        name: "edit",
        description: "Edit a single file using exact text replacement. Every edits[].oldText must match a unique, non-overlapping region of the original file. If two changes affect the same block or nearby lines, merge them into one edit instead of emitting overlapping edits. Do not include large unchanged regions just to connect distant changes.",
        input_schema: serde_json::json!({
            "type": "object",
            "properties": {
                "path": {"type": "string", "description": "Path to the file to edit (relative or absolute)"},
                "edits": {
                    "type": "array",
                    "description": "One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "oldText": {"type": "string", "description": "Exact text for one targeted replacement. It must be unique in the original file and must not overlap with any other edits[].oldText in the same call."},
                            "newText": {"type": "string", "description": "Replacement text for this targeted edit."}
                        },
                        "required": ["oldText", "newText"]
                    }
                }
            },
            "required": ["path", "edits"]
        }),
        effect_class: verlet_tool_core::EffectClass::Idempotent,
    }
}

pub fn run(
    args: EditArgs,
    fs: &dyn verlet_tool_core::ToolFs,
) -> Result<EditOutput, verlet_tool_core::ToolError> {
    if args.edits.is_empty() {
        return Err(verlet_tool_core::ToolError::Failed(
            "Edit tool input is invalid. edits must contain at least one replacement.".to_owned(),
        ));
    }

    let edits = args
        .edits
        .iter()
        .map(|edit| NormalizedEdit {
            old_text: normalize_to_lf(&edit.old_text),
            new_text: normalize_to_lf(&edit.new_text),
        })
        .collect::<Vec<_>>();
    let input_path = args.path;
    let path_display = input_path.display().to_string();
    let path = verlet_tool_core::normalize_tool_path(&input_path);

    let stat = fs
        .stat(&path)
        .map_err(|error| edit_access_error(&path_display, error))?;
    if !stat.is_file && !stat.is_dir {
        return Err(edit_access_error(
            &path_display,
            verlet_tool_core::ToolFsError::Io("path is not a regular file".to_owned()),
        ));
    }
    if stat.size > u64::try_from(verlet_tool_core::MAX_FILE_BYTES).unwrap_or(u64::MAX) {
        return Err(edit_access_error(
            &path_display,
            verlet_tool_core::ToolFsError::FileTooLarge {
                path,
                max_bytes: verlet_tool_core::MAX_FILE_BYTES,
            },
        ));
    }
    let bytes = fs
        .read_file_bounded(&path, verlet_tool_core::MAX_FILE_BYTES)
        .map_err(|error| edit_access_error(&path_display, error))?;
    let (bom, text_bytes) = if bytes.starts_with(&[0xef, 0xbb, 0xbf]) {
        (&bytes[..3], &bytes[3..])
    } else {
        (&bytes[..0], bytes.as_slice())
    };
    let content = String::from_utf8_lossy(text_bytes);
    let line_ending = detect_line_ending(&content);
    let base_content = normalize_to_lf(&content);

    for (index, edit) in edits.iter().enumerate() {
        if edit.old_text.is_empty() {
            return Err(empty_old_text_error(&path_display, index, edits.len()));
        }
    }

    let uses_normalized_match = edits
        .iter()
        .any(|edit| find_text(&base_content, &edit.old_text).used_normalized_match);
    let replacement_base = if uses_normalized_match {
        normalize_for_match(&base_content)
    } else {
        base_content.clone()
    };

    let mut matched_edits = Vec::with_capacity(edits.len());
    for (index, edit) in edits.iter().enumerate() {
        let match_result = find_text(&replacement_base, &edit.old_text);
        if !match_result.found {
            return Err(not_found_error(&path_display, index, edits.len()));
        }

        let occurrences = count_occurrences(&replacement_base, &edit.old_text);
        if occurrences > 1 {
            return Err(duplicate_error(
                &path_display,
                index,
                edits.len(),
                occurrences,
            ));
        }

        matched_edits.push(MatchedEdit {
            edit_index: index,
            match_index: match_result.index,
            match_length: match_result.match_length,
            new_text: edit.new_text.clone(),
        });
    }

    matched_edits.sort_by_key(|edit| edit.match_index);
    for pair in matched_edits.windows(2) {
        let previous = &pair[0];
        let current = &pair[1];
        if previous.match_index + previous.match_length > current.match_index {
            return Err(verlet_tool_core::ToolError::Failed(format!(
                "edits[{}] and edits[{}] overlap in {}. Merge them into one edit or target disjoint regions.",
                previous.edit_index, current.edit_index, path_display,
            )));
        }
    }

    let new_content = if uses_normalized_match {
        apply_replacements_preserving_unchanged_lines(
            &base_content,
            &replacement_base,
            &matched_edits,
        )?
    } else {
        apply_replacements(&replacement_base, &matched_edits, 0)
    };
    if new_content == base_content {
        return Err(no_change_error(&path_display, edits.len()));
    }

    let text_diff = similar::TextDiff::from_lines(&base_content, &new_content);
    let patch = text_diff
        .unified_diff()
        .context_radius(4)
        .header(&path_display, &path_display)
        .to_string();
    let (diff, first_changed_line) = generate_display_diff(&base_content, &new_content, 4);
    let text = format!(
        "Successfully replaced {} block(s) in {}.",
        edits.len(),
        path_display
    );
    if text
        .len()
        .saturating_add(diff.len())
        .saturating_add(patch.len())
        > verlet_tool_core::MAX_RESULT_BYTES
    {
        return Err(verlet_tool_core::ToolError::ResultTooLarge);
    }

    let edited_bytes =
        splice_unchanged_original_lines(text_bytes, &base_content, &new_content, line_ending)?;
    let mut output_bytes = Vec::with_capacity(bom.len() + edited_bytes.len());
    output_bytes.extend_from_slice(bom);
    output_bytes.extend_from_slice(&edited_bytes);
    fs.write_file(&path, &output_bytes)?;

    Ok(EditOutput {
        text,
        details: EditDetails {
            diff,
            patch,
            first_changed_line,
        },
        edits_applied: u32::try_from(edits.len()).unwrap_or(u32::MAX),
    })
}

#[derive(Clone, Copy)]
enum LineEnding {
    Lf,
    CrLf,
}

struct NormalizedEdit {
    old_text: String,
    new_text: String,
}

struct MatchResult {
    found: bool,
    index: usize,
    match_length: usize,
    used_normalized_match: bool,
}

#[derive(Clone)]
struct MatchedEdit {
    edit_index: usize,
    match_index: usize,
    match_length: usize,
    new_text: String,
}

#[derive(Clone, Copy)]
struct LineSpan {
    start: usize,
    end: usize,
}

struct ReplacementGroup {
    start_line: usize,
    end_line: usize,
    replacements: Vec<MatchedEdit>,
}

fn edit_access_error(
    path: &str,
    error: verlet_tool_core::ToolFsError,
) -> verlet_tool_core::ToolError {
    let message = match error {
        verlet_tool_core::ToolFsError::NotFound(_) => "Error code: ENOENT".to_owned(),
        verlet_tool_core::ToolFsError::Denied(_) => "Error code: EACCES".to_owned(),
        verlet_tool_core::ToolFsError::FileTooLarge { path, max_bytes } => format!(
            "file exceeds {max_bytes} byte edit limit: {}; split the file or use bash to apply a bounded change",
            path.display()
        ),
        verlet_tool_core::ToolFsError::Io(message) => message,
    };
    verlet_tool_core::ToolError::Failed(format!("Could not edit file: {path}. {message}."))
}

fn not_found_error(
    path: &str,
    edit_index: usize,
    total_edits: usize,
) -> verlet_tool_core::ToolError {
    if total_edits == 1 {
        verlet_tool_core::ToolError::Failed(format!(
            "Could not find the exact text in {path}. The old text must match exactly including all whitespace and newlines."
        ))
    } else {
        verlet_tool_core::ToolError::Failed(format!(
            "Could not find edits[{edit_index}] in {path}. The oldText must match exactly including all whitespace and newlines."
        ))
    }
}

fn duplicate_error(
    path: &str,
    edit_index: usize,
    total_edits: usize,
    occurrences: usize,
) -> verlet_tool_core::ToolError {
    if total_edits == 1 {
        verlet_tool_core::ToolError::Failed(format!(
            "Found {occurrences} occurrences of the text in {path}. The text must be unique. Please provide more context to make it unique."
        ))
    } else {
        verlet_tool_core::ToolError::Failed(format!(
            "Found {occurrences} occurrences of edits[{edit_index}] in {path}. Each oldText must be unique. Please provide more context to make it unique."
        ))
    }
}

fn empty_old_text_error(
    path: &str,
    edit_index: usize,
    total_edits: usize,
) -> verlet_tool_core::ToolError {
    if total_edits == 1 {
        verlet_tool_core::ToolError::Failed(format!("oldText must not be empty in {path}."))
    } else {
        verlet_tool_core::ToolError::Failed(format!(
            "edits[{edit_index}].oldText must not be empty in {path}."
        ))
    }
}

fn no_change_error(path: &str, total_edits: usize) -> verlet_tool_core::ToolError {
    if total_edits == 1 {
        verlet_tool_core::ToolError::Failed(format!(
            "No changes made to {path}. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected."
        ))
    } else {
        verlet_tool_core::ToolError::Failed(format!(
            "No changes made to {path}. The replacements produced identical content."
        ))
    }
}

fn detect_line_ending(content: &str) -> LineEnding {
    match (content.find("\r\n"), content.find('\n')) {
        (Some(crlf), Some(lf)) if crlf == lf.saturating_sub(1) => LineEnding::CrLf,
        _ => LineEnding::Lf,
    }
}

fn normalize_to_lf(text: &str) -> String {
    text.replace("\r\n", "\n").replace('\r', "\n")
}

fn restore_line_endings(text: &str, ending: LineEnding) -> String {
    match ending {
        LineEnding::Lf => text.to_owned(),
        LineEnding::CrLf => text.replace('\n', "\r\n"),
    }
}

fn generate_display_diff(
    old_content: &str,
    new_content: &str,
    context_lines: usize,
) -> (String, Option<u64>) {
    #[derive(Clone, Copy)]
    enum RowKind {
        Equal,
        Delete,
        Insert,
    }

    struct Row {
        kind: RowKind,
        line_number: usize,
        text: String,
    }

    let line_number_width = old_content
        .split('\n')
        .count()
        .max(new_content.split('\n').count())
        .to_string()
        .len();
    let text_diff = similar::TextDiff::from_lines(old_content, new_content);
    let mut old_line_number = 1_usize;
    let mut new_line_number = 1_usize;
    let mut first_changed_line = None;
    let mut rows = Vec::new();

    for change in text_diff.iter_all_changes() {
        let text = change.value().strip_suffix('\n').unwrap_or(change.value());
        match change.tag() {
            similar::ChangeTag::Equal => {
                rows.push(Row {
                    kind: RowKind::Equal,
                    line_number: old_line_number,
                    text: text.to_owned(),
                });
                old_line_number = old_line_number.saturating_add(1);
                new_line_number = new_line_number.saturating_add(1);
            }
            similar::ChangeTag::Delete => {
                first_changed_line.get_or_insert(new_line_number);
                rows.push(Row {
                    kind: RowKind::Delete,
                    line_number: old_line_number,
                    text: text.to_owned(),
                });
                old_line_number = old_line_number.saturating_add(1);
            }
            similar::ChangeTag::Insert => {
                first_changed_line.get_or_insert(new_line_number);
                rows.push(Row {
                    kind: RowKind::Insert,
                    line_number: new_line_number,
                    text: text.to_owned(),
                });
                new_line_number = new_line_number.saturating_add(1);
            }
        }
    }

    let mut visible = vec![false; rows.len()];
    for (index, row) in rows.iter().enumerate() {
        if !matches!(row.kind, RowKind::Equal) {
            let start = index.saturating_sub(context_lines);
            let end = index
                .saturating_add(context_lines)
                .min(rows.len().saturating_sub(1));
            for item in &mut visible[start..=end] {
                *item = true;
            }
        }
    }

    let gap = format!(" {} ...", " ".repeat(line_number_width));
    let mut output = Vec::new();
    let mut previous_visible = None;
    for (index, row) in rows.iter().enumerate() {
        if !visible[index] {
            continue;
        }
        if previous_visible.map_or(index > 0, |previous| index > previous + 1) {
            output.push(gap.clone());
        }
        let marker = match row.kind {
            RowKind::Equal => ' ',
            RowKind::Delete => '-',
            RowKind::Insert => '+',
        };
        output.push(format!(
            "{marker}{:>width$} {}",
            row.line_number,
            row.text,
            width = line_number_width,
        ));
        previous_visible = Some(index);
    }
    if previous_visible.is_some_and(|previous| previous + 1 < rows.len()) {
        output.push(gap);
    }

    (
        output.join("\n"),
        first_changed_line.map(|line| u64::try_from(line).unwrap_or(u64::MAX)),
    )
}

fn split_byte_lines_with_endings(content: &[u8]) -> Vec<&[u8]> {
    let mut lines = Vec::new();
    let mut start = 0_usize;
    for (index, byte) in content.iter().enumerate() {
        if *byte == b'\n' {
            lines.push(&content[start..=index]);
            start = index.saturating_add(1);
        }
    }
    if start < content.len() {
        lines.push(&content[start..]);
    }
    lines
}

fn splice_unchanged_original_lines(
    original_bytes: &[u8],
    old_content: &str,
    new_content: &str,
    line_ending: LineEnding,
) -> Result<Vec<u8>, verlet_tool_core::ToolError> {
    let original_lines = split_byte_lines_with_endings(original_bytes);
    let text_diff = similar::TextDiff::from_lines(old_content, new_content);
    let mut old_line_index = 0_usize;
    let mut output = Vec::new();

    for change in text_diff.iter_all_changes() {
        match change.tag() {
            similar::ChangeTag::Equal => {
                let line = original_lines.get(old_line_index).ok_or_else(|| {
                    verlet_tool_core::ToolError::Failed(
                        "Cannot preserve unchanged lines because the base content has a different line count."
                            .to_owned(),
                    )
                })?;
                output.extend_from_slice(line);
                old_line_index = old_line_index.saturating_add(1);
            }
            similar::ChangeTag::Delete => {
                old_line_index = old_line_index.saturating_add(1);
            }
            similar::ChangeTag::Insert => {
                let restored = restore_line_endings(change.value(), line_ending);
                output.extend_from_slice(restored.as_bytes());
            }
        }
    }

    Ok(output)
}

fn normalize_for_match(text: &str) -> String {
    text.nfkc()
        .map(|character| match character {
            '\u{2018}' | '\u{2019}' | '\u{201a}' | '\u{201b}' => '\'',
            '\u{201c}' | '\u{201d}' | '\u{201e}' | '\u{201f}' => '"',
            '\u{2010}' | '\u{2011}' | '\u{2012}' | '\u{2013}' | '\u{2014}' | '\u{2015}'
            | '\u{2212}' => '-',
            '\u{00a0}' | '\u{2002}'..='\u{200a}' | '\u{202f}' | '\u{205f}' | '\u{3000}' => ' ',
            character => character,
        })
        .collect::<String>()
        .split('\n')
        .map(str::trim_end)
        .collect::<Vec<_>>()
        .join("\n")
}

fn find_text(content: &str, old_text: &str) -> MatchResult {
    if let Some(index) = content.find(old_text) {
        return MatchResult {
            found: true,
            index,
            match_length: old_text.len(),
            used_normalized_match: false,
        };
    }

    let normalized_content = normalize_for_match(content);
    let normalized_old_text = normalize_for_match(old_text);
    if normalized_old_text.is_empty() {
        return MatchResult {
            found: false,
            index: 0,
            match_length: 0,
            used_normalized_match: false,
        };
    }
    match normalized_content.find(&normalized_old_text) {
        Some(index) => MatchResult {
            found: true,
            index,
            match_length: normalized_old_text.len(),
            used_normalized_match: true,
        },
        None => MatchResult {
            found: false,
            index: 0,
            match_length: 0,
            used_normalized_match: false,
        },
    }
}

fn count_occurrences(content: &str, old_text: &str) -> usize {
    let normalized_content = normalize_for_match(content);
    let normalized_old_text = normalize_for_match(old_text);
    if normalized_old_text.is_empty() {
        return 0;
    }
    normalized_content
        .match_indices(&normalized_old_text)
        .count()
}

fn split_lines_with_endings(content: &str) -> Vec<&str> {
    let mut lines = Vec::new();
    let mut start = 0;
    for (index, character) in content.char_indices() {
        if character == '\n' {
            lines.push(&content[start..index + 1]);
            start = index + 1;
        }
    }
    if start < content.len() {
        lines.push(&content[start..]);
    }
    lines
}

fn line_spans(content: &str) -> Vec<LineSpan> {
    let mut offset = 0;
    split_lines_with_endings(content)
        .into_iter()
        .map(|line| {
            let span = LineSpan {
                start: offset,
                end: offset + line.len(),
            };
            offset = span.end;
            span
        })
        .collect()
}

fn replacement_line_range(
    lines: &[LineSpan],
    replacement: &MatchedEdit,
) -> Result<(usize, usize), verlet_tool_core::ToolError> {
    let replacement_start = replacement.match_index;
    let replacement_end = replacement.match_index + replacement.match_length;
    let start_line = lines
        .iter()
        .position(|line| replacement_start >= line.start && replacement_start < line.end)
        .ok_or_else(|| {
            verlet_tool_core::ToolError::Failed(
                "Replacement range is outside the base content.".to_owned(),
            )
        })?;
    let mut end_line = start_line;
    while end_line < lines.len() && lines[end_line].end < replacement_end {
        end_line += 1;
    }
    if end_line >= lines.len() {
        return Err(verlet_tool_core::ToolError::Failed(
            "Replacement range is outside the base content.".to_owned(),
        ));
    }
    Ok((start_line, end_line + 1))
}

fn apply_replacements(content: &str, replacements: &[MatchedEdit], offset: usize) -> String {
    let mut result = content.to_owned();
    for replacement in replacements.iter().rev() {
        let start = replacement.match_index - offset;
        result.replace_range(
            start..start + replacement.match_length,
            &replacement.new_text,
        );
    }
    result
}

fn apply_replacements_preserving_unchanged_lines(
    original_content: &str,
    normalized_content: &str,
    replacements: &[MatchedEdit],
) -> Result<String, verlet_tool_core::ToolError> {
    let original_lines = split_lines_with_endings(original_content);
    let normalized_lines = line_spans(normalized_content);
    if original_lines.len() != normalized_lines.len() {
        return Err(verlet_tool_core::ToolError::Failed(
            "Cannot preserve unchanged lines because the base content has a different line count."
                .to_owned(),
        ));
    }

    let mut groups = Vec::<ReplacementGroup>::new();
    for replacement in replacements {
        let (start_line, end_line) = replacement_line_range(&normalized_lines, replacement)?;
        if let Some(group) = groups.last_mut() {
            if start_line < group.end_line {
                group.end_line = group.end_line.max(end_line);
                group.replacements.push(replacement.clone());
                continue;
            }
        }
        groups.push(ReplacementGroup {
            start_line,
            end_line,
            replacements: vec![replacement.clone()],
        });
    }

    let mut original_line_index = 0;
    let mut result = String::new();
    for group in groups {
        result.push_str(&original_lines[original_line_index..group.start_line].concat());
        let group_start = normalized_lines[group.start_line].start;
        let group_end = normalized_lines[group.end_line - 1].end;
        result.push_str(&apply_replacements(
            &normalized_content[group_start..group_end],
            &group.replacements,
            group_start,
        ));
        original_line_index = group.end_line;
    }
    result.push_str(&original_lines[original_line_index..].concat());
    Ok(result)
}

#[cfg(test)]
mod tests {
    fn fs(root: &std::path::Path) -> verlet_tool_core::StdFs {
        verlet_tool_core::StdFs::new(root).unwrap()
    }

    fn args(path: &str, edits: &[(&str, &str)]) -> crate::EditArgs {
        crate::EditArgs {
            path: std::path::PathBuf::from(path),
            edits: edits
                .iter()
                .map(|(old_text, new_text)| crate::Edit {
                    old_text: (*old_text).to_owned(),
                    new_text: (*new_text).to_owned(),
                })
                .collect(),
        }
    }

    #[test]
    fn replaces_one_exact_unique_match_and_returns_a_unified_diff() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should replace text in file".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "Hello, world!\n").unwrap();

        let output =
            crate::run(args("file.txt", &[("world", "testing")]), &fs(root.path())).unwrap();

        assert_eq!(output.edits_applied, 1);
        assert_eq!(output.text, "Successfully replaced 1 block(s) in file.txt.");
        assert_eq!(output.details.diff, "-1 Hello, world!\n+1 Hello, testing!");
        assert!(
            output
                .details
                .patch
                .starts_with("--- file.txt\n+++ file.txt\n@@")
        );
        assert_eq!(output.details.first_changed_line, Some(1));
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"Hello, testing!\n"
        );
    }

    #[test]
    fn details_use_pi_display_diff_gap_and_four_line_context() {
        // Pi behavior sheet item 21; source: core/tools/edit-diff.ts:364-499.
        let root = tempfile::tempdir().unwrap();
        let content = (1..=15)
            .map(|line| format!("line {line}"))
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(root.path().join("file.txt"), content).unwrap();

        let output = crate::run(
            args("file.txt", &[("line 2", "LINE 2"), ("line 14", "LINE 14")]),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            output.details.diff,
            "  1 line 1\n- 2 line 2\n+ 2 LINE 2\n  3 line 3\n  4 line 4\n  5 line 5\n  6 line 6\n    ...\n 10 line 10\n 11 line 11\n 12 line 12\n 13 line 13\n-14 line 14\n+14 LINE 14\n 15 line 15"
        );
        assert_eq!(output.details.first_changed_line, Some(2));
        assert!(output.details.patch.contains("@@ -1,6 +1,6 @@"));
    }

    #[test]
    fn replaces_a_normalized_only_smart_quote_match() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should match smart single quotes to ASCII quotes".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "console.log(‘hello’);\nkeep trailing   \n",
        )
        .unwrap();

        crate::run(
            args(
                "file.txt",
                &[("console.log('hello');", "console.log('world');")],
            ),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "console.log('world');\nkeep trailing   \n"
        );
    }

    #[test]
    fn strips_trailing_whitespace_only_from_lines_touched_by_a_normalized_match() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should match text with trailing whitespace stripped".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "line one   \nline two  \nline three   \n",
        )
        .unwrap();

        crate::run(
            args("file.txt", &[("line one\nline two\n", "replacement\n")]),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "replacement\nline three   \n"
        );
    }

    #[test]
    fn replaces_compatibility_equivalent_unicode_content() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should match compatibility-equivalent Unicode forms".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "ＡＢＣ１２３\ncafe\u{301}\n").unwrap();

        crate::run(
            args("file.txt", &[("ABC123\ncafé\n", "XYZ789\ncoffee\n")]),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "XYZ789\ncoffee\n"
        );
    }

    #[test]
    fn normalized_spans_remain_byte_aligned_after_multibyte_prefixes() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "🙂 préface\nconsole.log(‘hello’);\n終わり\n",
        )
        .unwrap();

        crate::run(
            args(
                "file.txt",
                &[("console.log('hello');", "console.log('world');")],
            ),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "🙂 préface\nconsole.log('world');\n終わり\n"
        );
    }

    #[test]
    fn invalid_utf8_decodes_lossily_and_untouched_lines_remain_byte_exact() {
        // Pi behavior sheet item 19; source: core/tools/edit.ts:358-370.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("invalid.txt"), b"\xff\ntarget\n").unwrap();
        std::fs::write(root.path().join("empty.txt"), b"").unwrap();
        let fs = fs(root.path());

        crate::run(args("invalid.txt", &[("target", "done")]), &fs).unwrap();
        let empty = crate::run(args("empty.txt", &[("x", "y")]), &fs).unwrap_err();

        assert_eq!(
            std::fs::read(root.path().join("invalid.txt")).unwrap(),
            b"\xff\ndone\n"
        );
        assert_eq!(
            empty.to_string(),
            "Could not find the exact text in empty.txt. The old text must match exactly including all whitespace and newlines."
        );
    }

    #[test]
    fn rejects_targets_over_the_ceiling_without_silently_skipping_or_changing_them() {
        let root = tempfile::tempdir().unwrap();
        let padding_line = format!("{}\n", "x".repeat(1024));
        let padding = padding_line.repeat(
            verlet_tool_core::MAX_FILE_BYTES
                .saturating_div(padding_line.len())
                .saturating_add(1),
        );
        let content = format!("target\n{padding}");
        assert!(content.len() > verlet_tool_core::MAX_FILE_BYTES);
        std::fs::write(root.path().join("oversized.txt"), &content).unwrap();

        let error = crate::run(
            args("oversized.txt", &[("target", "edited")]),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "Could not edit file: oversized.txt. file exceeds {} byte edit limit: oversized.txt; split the file or use bash to apply a bounded change.",
                verlet_tool_core::MAX_FILE_BYTES
            )
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("oversized.txt")).unwrap(),
            content
        );
    }

    #[test]
    fn access_and_directory_errors_follow_pi_compatibility_shape() {
        // Pi behavior sheet item 20; source: core/tools/edit.ts:347-360.
        let root = tempfile::tempdir().unwrap();

        let missing = crate::run(args("missing.txt", &[("x", "y")]), &fs(root.path())).unwrap_err();
        let directory = crate::run(args(".", &[("x", "y")]), &fs(root.path())).unwrap_err();

        assert_eq!(
            missing.to_string(),
            "Could not edit file: missing.txt. Error code: ENOENT."
        );
        assert!(!directory.to_string().contains("is not a file"));
    }

    #[cfg(unix)]
    #[test]
    fn a_fifo_is_rejected_before_edit_opens_it() {
        let root = tempfile::tempdir().unwrap();
        assert!(
            std::process::Command::new("mkfifo")
                .arg(root.path().join("named.pipe"))
                .status()
                .unwrap()
                .success()
        );

        let error = crate::run(
            args("named.pipe", &[("hello", "goodbye")]),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Could not edit file: named.pipe. path is not a regular file."
        );
    }

    #[test]
    fn replaces_pi_full_dash_and_special_space_normalization_sets() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should match Unicode dashes to ASCII hyphen"; behavior sheet item 18,
        // source: core/tools/edit-diff.ts:27-54.
        let root = tempfile::tempdir().unwrap();
        let dashes = [
            '\u{2010}', '\u{2011}', '\u{2012}', '\u{2013}', '\u{2014}', '\u{2015}', '\u{2212}',
        ];
        let spaces = [
            '\u{00a0}', '\u{2002}', '\u{2003}', '\u{2004}', '\u{2005}', '\u{2006}', '\u{2007}',
            '\u{2008}', '\u{2009}', '\u{200a}', '\u{202f}', '\u{205f}', '\u{3000}',
        ];
        let content = format!(
            "{}\n{}\n",
            dashes
                .iter()
                .map(|dash| format!("a{dash}b"))
                .collect::<Vec<_>>()
                .join(" "),
            spaces
                .iter()
                .map(|space| format!("a{space}b"))
                .collect::<Vec<_>>()
                .join(" "),
        );
        let old_text = format!(
            "{}\n{}",
            std::iter::repeat_n("a-b", dashes.len())
                .collect::<Vec<_>>()
                .join(" "),
            std::iter::repeat_n("a b", spaces.len())
                .collect::<Vec<_>>()
                .join(" "),
        );
        std::fs::write(root.path().join("file.txt"), content).unwrap();

        crate::run(
            args("file.txt", &[(&old_text, "normalized")]),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "normalized\n"
        );
    }

    #[test]
    fn rejects_an_ambiguous_match_and_names_the_old_text() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should fail if text appears multiple times".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "foo foo foo").unwrap();

        let error = crate::run(args("file.txt", &[("foo", "bar")]), &fs(root.path())).unwrap_err();

        assert_eq!(
            error.to_string(),
            "Found 3 occurrences of the text in file.txt. The text must be unique. Please provide more context to make it unique."
        );
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"foo foo foo"
        );
    }

    #[test]
    fn rejects_ambiguity_after_normalization() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should detect duplicates after fuzzy normalization".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "‘hello’\n’hello‘\n").unwrap();

        let error = crate::run(
            args("file.txt", &[("'hello'", "replacement")]),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Found 2 occurrences of the text in file.txt. The text must be unique. Please provide more context to make it unique."
        );
    }

    #[test]
    fn rejects_a_zero_match_and_names_the_old_text() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should fail if text not found".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "Hello, world!").unwrap();

        let error = crate::run(
            args("file.txt", &[("nonexistent", "testing")]),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Could not find the exact text in file.txt. The old text must match exactly including all whitespace and newlines."
        );
    }

    #[test]
    fn multi_edit_duplicate_empty_and_no_change_errors_are_exact() {
        // Pi behavior sheet item 17; source: core/tools/edit-diff.ts:253-289,320-359.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "foo foo\nbar\n").unwrap();
        let filesystem = fs(root.path());

        let duplicate = crate::run(
            args("file.txt", &[("bar", "BAR"), ("foo", "FOO")]),
            &filesystem,
        )
        .unwrap_err();
        let empty = crate::run(
            args("file.txt", &[("bar", "BAR"), ("", "empty")]),
            &filesystem,
        )
        .unwrap_err();
        let no_change = crate::run(
            args("file.txt", &[("foo foo", "foo foo"), ("bar", "bar")]),
            &filesystem,
        )
        .unwrap_err();

        assert_eq!(
            duplicate.to_string(),
            "Found 2 occurrences of edits[1] in file.txt. Each oldText must be unique. Please provide more context to make it unique."
        );
        assert_eq!(
            empty.to_string(),
            "edits[1].oldText must not be empty in file.txt."
        );
        assert_eq!(
            no_change.to_string(),
            "No changes made to file.txt. The replacements produced identical content."
        );
    }

    #[test]
    fn rejects_overlapping_edits_and_names_both_old_texts() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should fail when multi-edit regions overlap".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "one\ntwo\nthree\n").unwrap();

        let error = crate::run(
            args(
                "file.txt",
                &[
                    ("one\ntwo\n", "ONE\nTWO\n"),
                    ("two\nthree\n", "TWO\nTHREE\n"),
                ],
            ),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "edits[0] and edits[1] overlap in file.txt. Merge them into one edit or target disjoint regions."
        );
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"one\ntwo\nthree\n"
        );
    }

    #[test]
    fn applies_a_reverse_ordered_multi_edit_batch_against_the_original() {
        // Pi source cases: packages/coding-agent/test/tools.test.ts,
        // "should replace multiple disjoint regions in one call" and
        // "should match edits against the original file, not incrementally".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "foo\nbar\nbaz\n").unwrap();

        let output = crate::run(
            args("file.txt", &[("bar\n", "BAR\n"), ("foo\n", "foo bar\n")]),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(output.edits_applied, 2);
        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "foo bar\nBAR\nbaz\n"
        );
    }

    #[test]
    fn applies_exact_and_normalized_matches_in_one_batch() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should support fuzzy matching in multi-edit mode".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            "exact target\nconsole.log(‘hello’);\n",
        )
        .unwrap();

        crate::run(
            args(
                "file.txt",
                &[
                    ("console.log('hello');\n", "console.log('world');\n"),
                    ("exact target\n", "EXACT\n"),
                ],
            ),
            &fs(root.path()),
        )
        .unwrap();

        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "EXACT\nconsole.log('world');\n"
        );
    }

    #[test]
    fn matches_lf_text_and_preserves_a_crlf_file_and_bom() {
        // Pi source cases: packages/coding-agent/test/tools.test.ts,
        // "should preserve CRLF line endings after edit" and
        // "should preserve UTF-8 BOM after edit".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(
            root.path().join("file.txt"),
            b"\xef\xbb\xbffirst\r\nsecond\r\nthird\r\n",
        )
        .unwrap();

        let output = crate::run(
            args("file.txt", &[("second\n", "REPLACED\n")]),
            &fs(root.path()),
        )
        .unwrap();

        assert!(!output.details.diff.contains('\r'));
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"\xef\xbb\xbffirst\r\nREPLACED\r\nthird\r\n"
        );
    }

    #[test]
    fn leaves_the_file_byte_identical_when_the_second_edit_fails() {
        // Pi source case: packages/coding-agent/test/tools.test.ts,
        // "should not partially apply edits when one edit fails".
        let root = tempfile::tempdir().unwrap();
        let original = b"alpha\r\nbeta\r\ngamma\r\n";
        std::fs::write(root.path().join("file.txt"), original).unwrap();

        let error = crate::run(
            args(
                "file.txt",
                &[("alpha\n", "ALPHA\n"), ("missing\n", "MISSING\n")],
            ),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "Could not find edits[1] in file.txt. The oldText must match exactly including all whitespace and newlines."
        );
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            original
        );
    }

    #[test]
    fn rejects_an_empty_batch_empty_old_text_and_no_op_edit() {
        // Pi source case for the empty batch:
        // packages/coding-agent/test/tools.test.ts,
        // "should fail when edits is empty".
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "content").unwrap();
        let fs = fs(root.path());

        let empty_batch = crate::run(args("file.txt", &[]), &fs).unwrap_err();
        let empty_old = crate::run(args("file.txt", &[("", "new")]), &fs).unwrap_err();
        let no_op = crate::run(args("file.txt", &[("content", "content")]), &fs).unwrap_err();

        assert_eq!(
            empty_batch.to_string(),
            "Edit tool input is invalid. edits must contain at least one replacement."
        );
        assert_eq!(
            empty_old.to_string(),
            "oldText must not be empty in file.txt."
        );
        assert_eq!(
            no_op.to_string(),
            "No changes made to file.txt. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected."
        );
        assert_eq!(
            std::fs::read(root.path().join("file.txt")).unwrap(),
            b"content"
        );
    }

    #[test]
    fn rejects_a_normalized_replacement_that_restores_the_original_text() {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "‘hello’\n").unwrap();

        let error = crate::run(
            args("file.txt", &[("'hello'", "‘hello’")]),
            &fs(root.path()),
        )
        .unwrap_err();

        assert_eq!(
            error.to_string(),
            "No changes made to file.txt. The replacement produced identical content. This might indicate an issue with special characters or the text not existing as expected."
        );
        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "‘hello’\n"
        );
    }

    #[test]
    fn camel_case_schema_and_all_preparer_forms_match_pi() {
        // Pi behavior sheet item 15; source: core/tools/edit.ts:34-54,116-147.
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("file.txt"), "one two three four five").unwrap();
        let contract = crate::contract();
        assert!(
            contract.input_schema["properties"]["edits"]["items"]["properties"]
                .get("oldText")
                .is_some()
        );
        assert!(
            contract.input_schema["properties"]["edits"]
                .get("minItems")
                .is_none()
        );
        assert_eq!(
            contract.description,
            "Edit a single file using exact text replacement. Every edits[].oldText must match a unique, non-overlapping region of the original file. If two changes affect the same block or nearby lines, merge them into one edit instead of emitting overlapping edits. Do not include large unchanged regions just to connect distant changes."
        );
        assert_eq!(
            contract.input_schema,
            serde_json::json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string", "description": "Path to the file to edit (relative or absolute)"},
                    "edits": {
                        "type": "array",
                        "description": "One or more targeted replacements. Each edit is matched against the original file, not incrementally. Do not include overlapping or nested edits. If two changes touch the same block or nearby lines, merge them into one edit instead.",
                        "items": {
                            "type": "object",
                            "properties": {
                                "oldText": {"type": "string", "description": "Exact text for one targeted replacement. It must be unique in the original file and must not overlap with any other edits[].oldText in the same call."},
                                "newText": {"type": "string", "description": "Replacement text for this targeted edit."}
                            },
                            "required": ["oldText", "newText"]
                        }
                    }
                },
                "required": ["path", "edits"]
            })
        );

        let single: crate::EditArgs = serde_json::from_value(serde_json::json!({
            "path": "file.txt",
            "edits": {"oldText": "one", "newText": "ONE"}
        }))
        .unwrap();
        let encoded: crate::EditArgs = serde_json::from_value(serde_json::json!({
            "path": "file.txt",
            "edits": "[{\"oldText\":\"two\",\"newText\":\"TWO\"}]"
        }))
        .unwrap();
        let encoded_single: crate::EditArgs = serde_json::from_value(serde_json::json!({
            "path": "file.txt",
            "edits": "{\"oldText\":\"five\",\"newText\":\"FIVE\"}"
        }))
        .unwrap();
        let legacy: crate::EditArgs = serde_json::from_value(serde_json::json!({
            "path": "file.txt",
            "edits": [{"oldText": "three", "newText": "THREE"}],
            "oldText": "four",
            "newText": "FOUR"
        }))
        .unwrap();

        crate::run(single, &fs(root.path())).unwrap();
        crate::run(encoded, &fs(root.path())).unwrap();
        crate::run(encoded_single, &fs(root.path())).unwrap();
        crate::run(legacy, &fs(root.path())).unwrap();
        assert_eq!(
            std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
            "ONE TWO THREE FOUR FIVE"
        );
    }

    #[test]
    fn preparer_coerces_values_before_validation_like_pi() {
        // Pi source: agent validation.ts:317-349 and edit.ts:116-147.
        let parsed = crate::parse_cli_args(serde_json::json!({
            "path": 7,
            "edits": [{"oldText": true, "newText": 2}]
        }))
        .unwrap();

        assert_eq!(parsed.path, std::path::PathBuf::from("7"));
        assert_eq!(parsed.edits.len(), 1);
        assert_eq!(parsed.edits[0].old_text, "true");
        assert_eq!(parsed.edits[0].new_text, "2");
    }

    #[test]
    fn malformed_preparer_variants_return_pi_validation_envelopes() {
        // Pi source: agent validation.ts:341-349 and edit.ts:116-147.
        let malformed_string = crate::parse_cli_args(serde_json::json!({
            "path": "file.txt",
            "edits": "nope"
        }))
        .unwrap_err();
        let partial_object = crate::parse_cli_args(serde_json::json!({
            "path": "file.txt",
            "edits": {"oldText": "old"}
        }))
        .unwrap_err();
        let partial_legacy = crate::parse_cli_args(serde_json::json!({
            "path": "file.txt",
            "oldText": "old"
        }))
        .unwrap_err();
        let malformed_encoded_array = crate::parse_cli_args(serde_json::json!({
            "path": "file.txt",
            "edits": "[{\"oldText\":\"old\"}]"
        }))
        .unwrap_err();

        assert_eq!(
            malformed_string,
            "Validation failed for tool \"edit\":\n  - edits.0: must be object\n\nReceived arguments:\n{\n  \"edits\": \"nope\",\n  \"path\": \"file.txt\"\n}"
        );
        assert_eq!(
            partial_object,
            "Validation failed for tool \"edit\":\n  - edits.0.newText: must have required properties newText\n\nReceived arguments:\n{\n  \"edits\": {\n    \"oldText\": \"old\"\n  },\n  \"path\": \"file.txt\"\n}"
        );
        assert_eq!(
            partial_legacy,
            "Validation failed for tool \"edit\":\n  - edits: must have required properties edits\n\nReceived arguments:\n{\n  \"oldText\": \"old\",\n  \"path\": \"file.txt\"\n}"
        );
        assert_eq!(
            malformed_encoded_array,
            "Validation failed for tool \"edit\":\n  - edits.0.newText: must have required properties newText\n\nReceived arguments:\n{\n  \"edits\": [\n    {\n      \"oldText\": \"old\"\n    }\n  ],\n  \"path\": \"file.txt\"\n}"
        );
    }
}
