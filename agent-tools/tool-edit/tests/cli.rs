#![cfg(feature = "cli")]

#[test]
fn cli_edits_a_file_within_its_confined_root() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("file.txt"), "hello\nworld\n").unwrap();
    let input = serde_json::json!({
        "root": root.path(),
        "args": {
            "path": "file.txt",
            "edits": [{"oldText": "world", "newText": "there"}]
        }
    });

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_tool-edit"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(
        &mut child.stdin.take().unwrap(),
        serde_json::to_string(&input).unwrap().as_bytes(),
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();

    assert!(output.status.success(), "{output:?}");
    let value = serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap();
    assert_eq!(value["ok"]["edits_applied"], 1);
    assert_eq!(
        value["ok"]["text"],
        "Successfully replaced 1 block(s) in file.txt."
    );
    assert!(value["ok"]["details"]["diff"]
        .as_str()
        .unwrap()
        .contains("+2 there"));
    assert_eq!(
        std::fs::read_to_string(root.path().join("file.txt")).unwrap(),
        "hello\nthere\n"
    );
}

#[test]
fn cli_returns_pi_validation_envelope_for_malformed_preparer_input() {
    let root = tempfile::tempdir().unwrap();
    let input = serde_json::json!({
        "root": root.path(),
        "args": {"path": "file.txt", "edits": {"oldText": "old"}}
    });

    let mut child = std::process::Command::new(env!("CARGO_BIN_EXE_tool-edit"))
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    std::io::Write::write_all(
        &mut child.stdin.take().unwrap(),
        serde_json::to_string(&input).unwrap().as_bytes(),
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();

    assert_eq!(output.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({
            "error": "Validation failed for tool \"edit\":\n  - edits.0.newText: must have required properties newText\n\nReceived arguments:\n{\n  \"edits\": {\n    \"oldText\": \"old\"\n  },\n  \"path\": \"file.txt\"\n}"
        })
    );
}
