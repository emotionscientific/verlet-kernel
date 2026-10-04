use tokio::io::AsyncReadExt as _;
use tokio::io::AsyncWriteExt as _;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VirtualCommandOutput {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

impl VirtualCommandOutput {
    pub fn success(&self) -> bool {
        self.exit_code == 0
    }

    pub fn event_text(&self) -> String {
        let mut text = String::new();
        text.push_str(&self.stdout);
        text.push_str(&self.stderr);
        if self.exit_code != 0 {
            if !text.ends_with('\n') && !text.is_empty() {
                text.push('\n');
            }
            text.push_str(&format!("[exit_code={}]\n", self.exit_code));
        }
        text
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, strum::AsRefStr, strum::Display)]
pub enum ExternalExecutorKind {
    #[strum(serialize = "host")]
    HostBash,
    #[strum(serialize = "remote")]
    RemoteLinux,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalCommandInvocation {
    Script(String),
    Argv { command: String, args: Vec<String> },
}

impl ExternalCommandInvocation {
    pub fn label(&self) -> String {
        match self {
            Self::Script(script) => script.clone(),
            Self::Argv { command, args } if args.is_empty() => command.clone(),
            Self::Argv { command, args } => format!("{command} {}", args.join(" ")),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExecutionDeadline {
    pub started_at: std::time::SystemTime,
    pub deadline_at: std::time::SystemTime,
    pub timeout: std::time::Duration,
}

impl ExecutionDeadline {
    pub fn from_now(timeout: std::time::Duration) -> Self {
        let started_at = std::time::SystemTime::now();
        let deadline_at = started_at.checked_add(timeout).unwrap_or(started_at);
        Self {
            started_at,
            deadline_at,
            timeout,
        }
    }

    pub fn remaining(&self) -> std::time::Duration {
        self.deadline_at
            .duration_since(std::time::SystemTime::now())
            .unwrap_or_default()
    }

    pub fn timeout_ms(&self) -> u64 {
        u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalCommandRequest {
    pub invocation: ExternalCommandInvocation,
    pub executor: ExternalExecutorKind,
    pub cwd: std::path::PathBuf,
    pub stdin: Option<String>,
    pub deadline: ExecutionDeadline,
    pub max_output_bytes: usize,
}

impl ExternalCommandRequest {
    pub fn label(&self) -> String {
        self.invocation.label()
    }
}

#[async_trait::async_trait]
pub trait ExternalCommandExecutor: Send + Sync + 'static {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> crate::VerletProcessResult<ExternalCommandResult>;

    async fn exec_cancellable(
        &self,
        request: ExternalCommandRequest,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> crate::VerletProcessResult<ExternalCommandResult> {
        let _ = cancellation;
        self.exec(request).await
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalFileWrite {
    pub path: std::path::PathBuf,
    pub content: Vec<u8>,
}

impl ExternalFileWrite {
    pub fn new(path: impl Into<std::path::PathBuf>, content: impl Into<Vec<u8>>) -> Self {
        Self {
            path: path.into(),
            content: content.into(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalCommandResult {
    pub output: VirtualCommandOutput,
    pub file_writes: Vec<ExternalFileWrite>,
}

impl ExternalCommandResult {
    pub fn new(output: VirtualCommandOutput) -> Self {
        Self {
            output,
            file_writes: Vec::new(),
        }
    }

    pub fn with_file_write(mut self, write: ExternalFileWrite) -> Self {
        self.file_writes.push(write);
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct RejectingExternalCommandExecutor;

#[async_trait::async_trait]
impl ExternalCommandExecutor for RejectingExternalCommandExecutor {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> crate::VerletProcessResult<ExternalCommandResult> {
        Ok(ExternalCommandResult::new(VirtualCommandOutput {
            stdout: String::new(),
            stderr: format!(
                "external execution disabled for virtual bash command: {}\n",
                request.label()
            ),
            exit_code: 127,
            stdout_truncated: false,
            stderr_truncated: false,
        }))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HostBashExecutorConfig {
    pub shell: std::path::PathBuf,
    pub workspace_root: std::path::PathBuf,
}

impl HostBashExecutorConfig {
    pub fn new(workspace_root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            shell: resolve_host_bash(),
            workspace_root: workspace_root.into(),
        }
    }

    pub fn with_shell(mut self, shell: impl Into<std::path::PathBuf>) -> Self {
        self.shell = shell.into();
        self
    }
}

/// The first `bash` that exists at a conventional absolute path, else the bare
/// name so the spawn resolves it through `PATH`.
///
/// `/bin/bash` is not universal: distributions that do not populate a legacy
/// `/bin` ship only `/bin/sh` (NixOS is the case this was found on), and there
/// hardcoding `/bin/bash` makes every host bash spawn fail with `ENOENT`.
/// Probing first keeps the resolved path byte-identical wherever `/bin/bash`
/// does exist, so this changes nothing on Debian, Ubuntu or macOS. The bare-name
/// fallback matches how `ExternalCommandInvocation::Argv` already resolves host
/// commands. Callers that need an exact interpreter use
/// [`HostBashExecutorConfig::with_shell`].
fn resolve_host_bash() -> std::path::PathBuf {
    for candidate in ["/bin/bash", "/usr/bin/bash"] {
        let candidate = std::path::Path::new(candidate);
        if candidate.exists() {
            return candidate.to_path_buf();
        }
    }
    std::path::PathBuf::from("bash")
}

#[derive(Clone, Debug)]
pub struct HostBashExecutor {
    config: HostBashExecutorConfig,
}

impl HostBashExecutor {
    pub fn new(workspace_root: impl Into<std::path::PathBuf>) -> Self {
        Self {
            config: HostBashExecutorConfig::new(workspace_root),
        }
    }

    pub fn with_config(config: HostBashExecutorConfig) -> Self {
        Self { config }
    }

    fn host_cwd(&self, cwd: &std::path::Path) -> std::path::PathBuf {
        let workspace = std::path::Path::new("/workspace");
        if cwd == workspace {
            return self.config.workspace_root.clone();
        }
        if let Ok(relative) = cwd.strip_prefix(workspace) {
            return self.config.workspace_root.join(relative);
        }
        if cwd.is_relative() {
            return self.config.workspace_root.join(cwd);
        }
        self.config.workspace_root.clone()
    }

    async fn exec_with_cancellation(
        &self,
        request: ExternalCommandRequest,
        cancellation: Option<tokio_util::sync::CancellationToken>,
    ) -> crate::VerletProcessResult<ExternalCommandResult> {
        if request.executor != ExternalExecutorKind::HostBash {
            return Ok(ExternalCommandResult::new(VirtualCommandOutput {
                stdout: String::new(),
                stderr: format!(
                    "host bash executor cannot run {:?} command: {}\n",
                    request.executor,
                    request.label()
                ),
                exit_code: 127,
                stdout_truncated: false,
                stderr_truncated: false,
            }));
        }
        if request.deadline.remaining().is_zero() {
            return Ok(external_timeout_result("host bash exec timed out\n"));
        }

        let mut command = match &request.invocation {
            ExternalCommandInvocation::Script(script) => {
                let mut command = tokio::process::Command::new(&self.config.shell);
                command.arg("-lc").arg(script);
                command
            }
            ExternalCommandInvocation::Argv { command, args } => {
                let mut process = tokio::process::Command::new(command);
                process.args(args);
                process
            }
        };
        command
            .current_dir(self.host_cwd(&request.cwd))
            .stdin(if request.stdin.is_some() {
                std::process::Stdio::piped()
            } else {
                std::process::Stdio::null()
            })
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let mut child = command.spawn().map_err(crate::process_error)?;
        let child_id = child.id();
        #[cfg(unix)]
        let mut process_group_guard = ProcessGroupKillGuard::new(child_id);
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| crate::process_error("host bash stdout pipe was not available"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| crate::process_error("host bash stderr pipe was not available"))?;
        let stdout_task = tokio::spawn(read_capped_output(stdout, request.max_output_bytes));
        let stderr_task = tokio::spawn(read_capped_output(stderr, request.max_output_bytes));
        if let Some(stdin) = request.stdin
            && let Some(mut child_stdin) = child.stdin.take()
        {
            child_stdin
                .write_all(stdin.as_bytes())
                .await
                .map_err(crate::process_error)?;
        }

        enum HostExit {
            Exited(std::process::ExitStatus),
            TimedOut,
            Cancelled,
        }
        let cancellation_wait = async {
            match &cancellation {
                Some(cancellation) => cancellation.cancelled().await,
                None => std::future::pending::<()>().await,
            }
        };
        let mut exit = tokio::select! {
            status = child.wait() => HostExit::Exited(status.map_err(crate::process_error)?),
            _ = tokio::time::sleep(request.deadline.remaining()) => {
                terminate_external_child(&mut child, child_id).await;
                HostExit::TimedOut
            }
            _ = cancellation_wait => {
                terminate_external_child(&mut child, child_id).await;
                HostExit::Cancelled
            }
        };
        #[cfg(unix)]
        if matches!(&exit, HostExit::Exited(_)) {
            process_group_guard.disarm_if_group_gone();
        }
        let mut stdout_task = stdout_task;
        let mut stderr_task = stderr_task;
        let mut drained_output = None;
        if matches!(&exit, HostExit::Exited(_)) {
            let output_drain = async {
                let stdout = (&mut stdout_task).await.map_err(crate::process_error)?;
                let stderr = (&mut stderr_task).await.map_err(crate::process_error)?;
                Ok::<_, crate::VerletProcessError>((stdout, stderr))
            };
            tokio::pin!(output_drain);
            exit = tokio::select! {
                drained = &mut output_drain => {
                    drained_output = Some(drained?);
                    exit
                }
                _ = async {
                    match &cancellation {
                        Some(cancellation) => cancellation.cancelled().await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    terminate_reaped_external_group(child_id).await;
                    HostExit::Cancelled
                }
                _ = tokio::time::sleep(request.deadline.remaining()) => {
                    terminate_reaped_external_group(child_id).await;
                    HostExit::TimedOut
                }
            };
        }
        let (stdout, stderr) = match drained_output {
            Some(output) => output,
            None => (
                stdout_task.await.map_err(crate::process_error)?,
                stderr_task.await.map_err(crate::process_error)?,
            ),
        };
        let (stdout, stdout_truncated) = stdout?;
        let (mut stderr, stderr_truncated) = stderr?;
        let exit_code = match exit {
            HostExit::Exited(status) => status.code().unwrap_or(1),
            HostExit::TimedOut => {
                stderr.push_str("host bash exec timed out\n");
                124
            }
            HostExit::Cancelled => {
                stderr.push_str("host bash exec cancelled\n");
                130
            }
        };
        let result = ExternalCommandResult::new(VirtualCommandOutput {
            stdout,
            stderr,
            exit_code,
            stdout_truncated,
            stderr_truncated,
        });
        #[cfg(unix)]
        process_group_guard.disarm();
        Ok(result)
    }
}

#[async_trait::async_trait]
impl ExternalCommandExecutor for HostBashExecutor {
    async fn exec(
        &self,
        request: ExternalCommandRequest,
    ) -> crate::VerletProcessResult<ExternalCommandResult> {
        self.exec_with_cancellation(request, None).await
    }

    async fn exec_cancellable(
        &self,
        request: ExternalCommandRequest,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> crate::VerletProcessResult<ExternalCommandResult> {
        self.exec_with_cancellation(request, Some(cancellation))
            .await
    }
}

async fn terminate_external_child(child: &mut tokio::process::Child, child_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(process_group) = crate::signalable_process_group(child_id) {
        // SAFETY: process_group is a positive group id this child was made
        // leader of; crate::signalable_process_group rejects 0 and negatives.
        unsafe {
            libc::killpg(process_group, libc::SIGTERM);
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        unsafe {
            libc::killpg(process_group, libc::SIGKILL);
        }
        let _ = child.wait().await;
        reap_adopted_process_group(process_group).await;
        return;
    }
    let _ = child.kill().await;
    let _ = child.wait().await;
}

async fn terminate_reaped_external_group(child_id: Option<u32>) {
    #[cfg(unix)]
    if let Some(process_group) = crate::signalable_process_group(child_id) {
        // SAFETY: see terminate_external_child.
        unsafe {
            libc::killpg(process_group, libc::SIGTERM);
        }
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        unsafe {
            libc::killpg(process_group, libc::SIGKILL);
        }
        reap_adopted_process_group(process_group).await;
    }
    #[cfg(not(unix))]
    let _ = child_id;
}

#[cfg(unix)]
async fn reap_adopted_process_group(process_group: libc::pid_t) {
    // When Verlet itself is container PID 1, killed grandchildren can be
    // reparented here. Reap only children in the process group we just killed;
    // ordinary deployments get ECHILD and return immediately.
    for _ in 0..10 {
        loop {
            let mut status = 0;
            let reaped = unsafe { libc::waitpid(-process_group, &mut status, libc::WNOHANG) };
            if reaped <= 0 {
                break;
            }
        }
        if unsafe { libc::killpg(process_group, 0) } == -1 {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(unix)]
struct ProcessGroupKillGuard {
    process_group: Option<libc::pid_t>,
}

#[cfg(unix)]
impl ProcessGroupKillGuard {
    fn new(child_id: Option<u32>) -> Self {
        Self {
            process_group: crate::signalable_process_group(child_id),
        }
    }

    fn disarm(&mut self) {
        self.process_group = None;
    }

    fn disarm_if_group_gone(&mut self) {
        let Some(process_group) = self.process_group else {
            return;
        };
        if unsafe { libc::killpg(process_group, 0) } == -1
            && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        {
            self.disarm();
        }
    }
}

#[cfg(unix)]
impl Drop for ProcessGroupKillGuard {
    fn drop(&mut self) {
        if let Some(process_group) = self.process_group {
            unsafe {
                libc::killpg(process_group, libc::SIGKILL);
            }
        }
    }
}

fn external_timeout_result(message: impl Into<String>) -> ExternalCommandResult {
    let mut stderr = message.into();
    if !stderr.ends_with('\n') {
        stderr.push('\n');
    }
    ExternalCommandResult::new(VirtualCommandOutput {
        stdout: String::new(),
        stderr,
        exit_code: 124,
        stdout_truncated: false,
        stderr_truncated: false,
    })
}

async fn read_capped_output<R>(
    mut reader: R,
    max_output_bytes: usize,
) -> crate::VerletProcessResult<(String, bool)>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut retained = String::with_capacity(max_output_bytes.min(8192));
    let mut truncated = false;
    let mut buffer = [0_u8; 8192 + 3];
    let mut pending = 0;
    loop {
        let read = reader
            .read(&mut buffer[pending..pending + 8192])
            .await
            .map_err(crate::process_error)?;
        if read == 0 {
            if pending > 0
                && !truncated
                && matches!(
                    append_lossy_chunk(&mut retained, &buffer[..pending], max_output_bytes, true,),
                    LossyChunk::Truncated
                )
            {
                truncated = true;
            }
            break;
        }
        if truncated {
            pending = 0;
            continue;
        }
        let available = pending + read;
        match append_lossy_chunk(&mut retained, &buffer[..available], max_output_bytes, false) {
            LossyChunk::Complete => pending = 0,
            LossyChunk::Pending(start) => {
                buffer.copy_within(start..available, 0);
                pending = available - start;
            }
            LossyChunk::Truncated => {
                truncated = true;
                pending = 0;
            }
        }
    }
    Ok((retained, truncated))
}

enum LossyChunk {
    Complete,
    Pending(usize),
    Truncated,
}

fn append_lossy_chunk(
    output: &mut String,
    input: &[u8],
    max_output_bytes: usize,
    eof: bool,
) -> LossyChunk {
    let mut offset = 0;
    loop {
        match std::str::from_utf8(&input[offset..]) {
            Ok(valid) => {
                return if append_valid_prefix(output, valid, max_output_bytes) {
                    LossyChunk::Complete
                } else {
                    LossyChunk::Truncated
                };
            }
            Err(error) => {
                let valid_end = offset + error.valid_up_to();
                let valid = std::str::from_utf8(&input[offset..valid_end])
                    .expect("Utf8Error::valid_up_to must delimit valid UTF-8");
                if !append_valid_prefix(output, valid, max_output_bytes) {
                    return LossyChunk::Truncated;
                }
                let Some(invalid_bytes) = error.error_len() else {
                    if !eof {
                        return LossyChunk::Pending(valid_end);
                    }
                    return if append_replacement(output, max_output_bytes) {
                        LossyChunk::Complete
                    } else {
                        LossyChunk::Truncated
                    };
                };
                if !append_replacement(output, max_output_bytes) {
                    return LossyChunk::Truncated;
                }
                offset = valid_end + invalid_bytes;
            }
        }
    }
}

fn append_valid_prefix(output: &mut String, valid: &str, max_output_bytes: usize) -> bool {
    let available = max_output_bytes.saturating_sub(output.len());
    let mut end = available.min(valid.len());
    while !valid.is_char_boundary(end) {
        end -= 1;
    }
    output.push_str(&valid[..end]);
    end == valid.len()
}

fn append_replacement(output: &mut String, max_output_bytes: usize) -> bool {
    if output.len().saturating_add('\u{fffd}'.len_utf8()) > max_output_bytes {
        return false;
    }
    output.push('\u{fffd}');
    true
}

#[cfg(test)]
mod tests {
    use crate::execution::ExternalCommandExecutor as _;
    use tokio::io::AsyncWriteExt as _;

    #[test]
    fn executor_kind_strings_stay_abbreviated() {
        assert_eq!(
            crate::execution::ExternalExecutorKind::HostBash.as_ref(),
            "host"
        );
        assert_eq!(
            crate::execution::ExternalExecutorKind::RemoteLinux.as_ref(),
            "remote"
        );
    }

    #[tokio::test]
    async fn capped_reader_drains_but_never_retains_more_than_its_limit() {
        let (mut writer, reader) = tokio::io::duplex(64);
        let write = tokio::spawn(async move {
            writer.write_all(&vec![b'x'; 1025]).await.unwrap();
        });

        let (retained, truncated) = crate::execution::read_capped_output(reader, 1024)
            .await
            .unwrap();
        write.await.unwrap();

        assert_eq!(retained.len(), 1024);
        assert!(truncated);
    }

    #[tokio::test]
    async fn lossy_conversion_is_bounded_and_keeps_small_legacy_output() {
        let legacy = b"before\xffafter";
        let (text, truncated) = crate::execution::read_capped_output(&legacy[..], 64)
            .await
            .unwrap();
        assert_eq!(text, String::from_utf8_lossy(legacy));
        assert!(!truncated);

        let invalid = vec![0xff; 1024];
        let (text, truncated) = crate::execution::read_capped_output(&invalid[..], 1024)
            .await
            .unwrap();
        assert!(text.len() <= 1024);
        assert!(truncated);
    }

    #[tokio::test]
    async fn lossy_conversion_preserves_utf8_split_at_the_reader_boundary() {
        let mut input = vec![b'a'; 8191];
        input.extend_from_slice("💥tail".as_bytes());

        let (text, truncated) = crate::execution::read_capped_output(&input[..], input.len())
            .await
            .unwrap();

        assert_eq!(text.as_bytes(), input);
        assert!(!truncated);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn process_group_guard_disarms_after_reaping_an_empty_group() {
        let mut command = tokio::process::Command::new("/bin/sh");
        command.arg("-c").arg("exit 0");
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == -1 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }
        let mut child = command.spawn().unwrap();
        let mut guard = crate::execution::ProcessGroupKillGuard::new(child.id());
        child.wait().await.unwrap();

        guard.disarm_if_group_gone();

        assert!(guard.process_group.is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellable_host_bash_returns_partial_output_and_kills_the_process_group() {
        let root = std::env::temp_dir().join(format!(
            "verlet-host-cancellation-ready-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let ready_path = root.join("ready");
        let executor = crate::execution::HostBashExecutor::new(&root);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let request = crate::execution::ExternalCommandRequest {
            invocation: crate::execution::ExternalCommandInvocation::Script(
                "echo ready; trap '' TERM; (trap '' TERM; while :; do sleep 1; done) & echo child=$!; printf ready > ready; wait".to_string(),
            ),
            executor: crate::execution::ExternalExecutorKind::HostBash,
            cwd: std::path::PathBuf::from("/workspace"),
            stdin: None,
            deadline: crate::execution::ExecutionDeadline::from_now(std::time::Duration::from_secs(10)),
            max_output_bytes: 4096,
        };
        let run = tokio::spawn({
            let cancellation = cancellation.clone();
            async move { executor.exec_cancellable(request, cancellation).await }
        });
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while !ready_path.exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("host bash did not signal readiness before cancellation");
        cancellation.cancel();

        let result = tokio::time::timeout(std::time::Duration::from_secs(30), run)
            .await
            .expect("host bash cancellation should return promptly")
            .unwrap()
            .unwrap();
        assert_eq!(result.output.exit_code, 130);
        assert!(result.output.stdout.contains("ready\n"));
        let child = result
            .output
            .stdout
            .lines()
            .find_map(|line| line.strip_prefix("child="))
            .unwrap()
            .parse::<libc::pid_t>()
            .unwrap();
        let _ = std::fs::remove_dir_all(root);
        assert_eq!(unsafe { libc::kill(child, 0) }, -1);
        assert_eq!(
            std::io::Error::last_os_error().raw_os_error(),
            Some(libc::ESRCH)
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn cancellation_after_the_group_leader_exits_still_kills_pipe_holding_members() {
        let root = std::env::temp_dir().join(format!(
            "verlet-host-reaped-leader-{}",
            uuid::Uuid::now_v7()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let leader_path = root.join("leader.pid");
        let child_path = root.join("child.pid");
        let executor = crate::execution::HostBashExecutor::new(&root);
        let cancellation = tokio_util::sync::CancellationToken::new();
        let request = crate::execution::ExternalCommandRequest {
            invocation: crate::execution::ExternalCommandInvocation::Script(
                "echo $$ > leader.pid; (trap '' HUP TERM; while :; do sleep 1; done) & echo $! > child.pid; exit 0"
                    .to_string(),
            ),
            executor: crate::execution::ExternalExecutorKind::HostBash,
            cwd: std::path::PathBuf::from("/workspace"),
            stdin: None,
            deadline: crate::execution::ExecutionDeadline::from_now(std::time::Duration::from_secs(10)),
            max_output_bytes: 4096,
        };
        let mut run = tokio::spawn({
            let cancellation = cancellation.clone();
            async move { executor.exec_cancellable(request, cancellation).await }
        });
        let (leader, child) = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                let leader = std::fs::read_to_string(&leader_path)
                    .ok()
                    .and_then(|pid| pid.trim().parse::<libc::pid_t>().ok());
                let child = std::fs::read_to_string(&child_path)
                    .ok()
                    .and_then(|pid| pid.trim().parse::<libc::pid_t>().ok());
                if let (Some(leader), Some(child)) = (leader, child)
                    && unsafe { libc::kill(leader, 0) } == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    break (leader, child);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("host bash leader was not reaped before cancellation");

        cancellation.cancel();
        let result = match tokio::time::timeout(std::time::Duration::from_secs(30), &mut run).await
        {
            Ok(joined) => joined.unwrap().unwrap(),
            Err(_) => {
                run.abort();
                let _ = run.await;
                unsafe {
                    libc::kill(child, libc::SIGKILL);
                }
                panic!("cancellation was ignored after leader {leader} exited");
            }
        };
        let child_is_dead = unsafe {
            libc::kill(child, 0) == -1
                && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
        };
        if !child_is_dead {
            unsafe {
                libc::kill(child, libc::SIGKILL);
            }
        }
        let _ = std::fs::remove_dir_all(root);

        assert_eq!(result.output.exit_code, 130);
        assert!(child_is_dead, "pipe-holding process-group member survived");
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dropping_host_bash_execution_kills_the_owned_process_group() {
        let root = std::env::temp_dir().join(format!("verlet-host-drop-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&root).unwrap();
        let pid_path = root.join("child.pid");
        let executor = crate::execution::HostBashExecutor::new(&root);
        let request = crate::execution::ExternalCommandRequest {
            invocation: crate::execution::ExternalCommandInvocation::Script(
                "(trap '' TERM; while :; do sleep 1; done) & echo $! > child.pid; wait".to_string(),
            ),
            executor: crate::execution::ExternalExecutorKind::HostBash,
            cwd: std::path::PathBuf::from("/workspace"),
            stdin: None,
            deadline: crate::execution::ExecutionDeadline::from_now(
                std::time::Duration::from_secs(10),
            ),
            max_output_bytes: 4096,
        };
        let run = tokio::spawn(async move { executor.exec(request).await });
        let child = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Ok(pid) = std::fs::read_to_string(&pid_path)
                    && let Ok(pid) = pid.trim().parse::<libc::pid_t>()
                {
                    break pid;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("host bash did not expose its child pid");

        run.abort();
        let _ = run.await;
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if unsafe { libc::kill(child, 0) } == -1
                    && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping host bash left a process-group member alive");
        let _ = std::fs::remove_dir_all(root);
    }
}
