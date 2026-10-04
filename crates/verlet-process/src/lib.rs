pub mod bridge;
pub mod execution;
pub mod live;
pub mod process;

pub type VerletProcessResult<T> = Result<T, VerletProcessError>;

#[derive(Debug, thiserror::Error)]
pub enum VerletProcessError {
    #[error("process execution failed: {0}")]
    Execution(String),
}

pub(crate) fn process_error(err: impl std::fmt::Display) -> VerletProcessError {
    VerletProcessError::Execution(err.to_string())
}

/// A child pid narrowed to a process-group id that is safe to hand `killpg`.
///
/// `killpg(0, ..)` signals the *caller's* own process group, so an unchecked id
/// turns child cleanup into self-harm: the runtime would kill itself and every
/// sibling it shares a group with. Negative and out-of-range values are equally
/// unwanted. Every caller makes its child a group leader with `setpgid(0, 0)` in
/// a `pre_exec`, so the child pid *is* the group id, but the `u32` -> `pid_t`
/// cast can still yield 0 or a negative. This is the single place that is
/// checked, so `killpg` is never reached with one.
#[cfg(unix)]
pub(crate) fn signalable_process_group(child_id: Option<u32>) -> Option<libc::pid_t> {
    libc::pid_t::try_from(child_id?)
        .ok()
        .filter(|process_group| *process_group > 1)
}
