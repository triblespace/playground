use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use triblespace::core::blob::Bytes;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

use crate::branch_util::ensure_branch;
use crate::config::Config;
use crate::repo_util::{
    close_repo, current_branch_head, ensure_worker_name, init_repo, load_text, pull_workspace,
    push_workspace, refresh_cached_checkout, seed_metadata,
};
use crate::schema::playground_exec;
use crate::time_util::{epoch_interval, interval_key, now_epoch};
use crate::workspace_snapshot::{DEFAULT_WORKSPACE_BRANCH, restore_snapshot_merge};

const CONFIG_BRANCH_ID_HEX: &str = "4790808CF044F979FC7C2E47FCCB4A64";
const DEFAULT_EXEC_TIMEOUT_MS: u64 = 300_000;
const EXEC_CONTROL_POLL_MS: u64 = 100;

#[derive(Debug, Clone)]
struct CommandRequest {
    id: Id,
    command: Value<Handle<Blake3, LongString>>,
    requested_at: Option<Value<NsTAIInterval>>,
    cwd: Option<Value<Handle<Blake3, LongString>>>,
    stdin: Option<Value<Handle<Blake3, UnknownBlob>>>,
    stdin_text: Option<Value<Handle<Blake3, LongString>>>,
    timeout_ms: Option<Value<U256BE>>,
}

#[derive(Default)]
struct CommandRequestIndex {
    requests: HashMap<Id, CommandRequest>,
    in_progress_by_worker: HashSet<Id>,
    done: HashSet<Id>,
}

#[derive(Debug)]
struct ExecOutput {
    stdout: Vec<u8>,
    stderr: Vec<u8>,
    exit_code: Option<i32>,
    stdout_text: Option<String>,
    stderr_text: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct ExecCommandEnv {
    pile: String,
    config_branch_id: String,
    worker_id: String,
    turn_id: String,
}

pub(crate) fn run_exec_loop(
    config: Config,
    worker_id: Id,
    poll_ms: u64,
    stop: Option<Arc<AtomicBool>>,
) -> Result<()> {
    let default_cwd = config
        .exec
        .default_cwd
        .as_ref()
        .map(|path| path.to_string_lossy().to_string());

    let (mut repo, branch_id) = init_repo(&config).context("open triblespace repo")?;
    let result = (|| -> Result<()> {
        seed_metadata(&mut repo)?;
        let label = format!("exec-{}", id_prefix(worker_id));
        ensure_worker_name(&mut repo, branch_id, worker_id, &label)?;
        maybe_bootstrap_workspace(&mut repo, &config)?;
        let mut cached_head = None;
        let mut cached_catalog = TribleSet::new();
        let mut request_index = CommandRequestIndex::default();

        loop {
            if stop_requested(&stop) {
                break;
            }

            let branch_head = current_branch_head(&mut repo, branch_id)?;
            if branch_head == cached_head {
                sleep(Duration::from_millis(poll_ms));
                continue;
            }

            let mut ws = pull_workspace(&mut repo, branch_id, "pull workspace")?;
            let delta = refresh_cached_checkout(&mut ws, &mut cached_head, &mut cached_catalog)?;
            request_index.apply_delta(&cached_catalog, &delta, worker_id);
            let Some(request) = request_index.next_pending() else {
                sleep(Duration::from_millis(poll_ms));
                continue;
            };

            if stop_requested(&stop) {
                break;
            }

            let command = load_text(&mut ws, request.command).context("load command")?;
            let cwd = match request.cwd {
                Some(handle) => Some(load_text(&mut ws, handle).context("load cwd")?),
                None => default_cwd.clone(),
            };
            let stdin = load_stdin(&mut ws, &request).context("load stdin")?;
            let attempt: u64 = 1;
            let env = ExecCommandEnv {
                pile: config.pile_path.to_string_lossy().to_string(),
                config_branch_id: CONFIG_BRANCH_ID_HEX.to_string(),
                worker_id: format!("{worker_id:x}"),
                turn_id: format!("{request_id:x}", request_id = request.id),
            };

            let started_at = epoch_interval(now_epoch());
            let in_progress_id = ufoid();
            let mut change = TribleSet::new();
            change += entity! { &in_progress_id @
                playground_exec::kind: playground_exec::kind_in_progress,
                playground_exec::about_request: request.id,
                playground_exec::worker: worker_id,
                playground_exec::started_at: started_at,
                playground_exec::attempt: attempt,
            };
            ws.commit(change, None, Some("playground_exec in_progress"));
            push_workspace(&mut repo, &mut ws).context("push in_progress")?;

            let initial_timeout_ms = request
                .timeout_ms
                .and_then(u256be_to_u64)
                .unwrap_or(DEFAULT_EXEC_TIMEOUT_MS);
            let initial_timeout = Duration::from_millis(initial_timeout_ms);
            let started = Instant::now();
            let output = execute_command(
                &command,
                cwd.as_deref(),
                stdin,
                &env,
                initial_timeout,
                &stop,
                || {
                    if stop_requested(&stop) {
                        return Ok(None);
                    }

                    let branch_head = current_branch_head(&mut repo, branch_id)?;
                    if branch_head == cached_head {
                        return Ok(None);
                    }

                    let mut ws =
                        pull_workspace(&mut repo, branch_id, "pull workspace for control")?;
                    let delta =
                        refresh_cached_checkout(&mut ws, &mut cached_head, &mut cached_catalog)?;
                    request_index.apply_delta(&cached_catalog, &delta, worker_id);
                    Ok(collect_timeout_extension_ms(
                        &cached_catalog,
                        &delta,
                        request.id,
                        worker_id,
                    ))
                },
            );
            let ExecOutput {
                stdout,
                stderr,
                exit_code,
                stdout_text,
                stderr_text,
                error,
            } = output;
            let duration_ms = started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64;
            let finished_at = epoch_interval(now_epoch());

            let result_id = ufoid();
            let mut change = TribleSet::new();
            change += entity! { &result_id @
                playground_exec::kind: playground_exec::kind_command_result,
                playground_exec::about_request: request.id,
                playground_exec::finished_at: finished_at,
                playground_exec::attempt: attempt,
                playground_exec::duration_ms: duration_ms,
            };
            let stdout_handle = ws.put::<UnknownBlob, _>(Bytes::from_source(stdout));
            let stderr_handle = ws.put::<UnknownBlob, _>(Bytes::from_source(stderr));
            change += entity! { &result_id @
                playground_exec::stdout: stdout_handle,
                playground_exec::stderr: stderr_handle,
            };

            if let Some(exit_code) = exit_code.and_then(|code| u64::try_from(code).ok()) {
                change += entity! { &result_id @ playground_exec::exit_code: exit_code };
            }

            if let Some(stdout_text) = stdout_text {
                let handle = ws.put(stdout_text);
                change += entity! { &result_id @ playground_exec::stdout_text: handle };
            }

            if let Some(stderr_text) = stderr_text {
                let handle = ws.put(stderr_text);
                change += entity! { &result_id @ playground_exec::stderr_text: handle };
            }

            if let Some(error) = error {
                let handle = ws.put(error);
                change += entity! { &result_id @ playground_exec::error: handle };
            }

            ws.commit(change, None, Some("playground_exec result"));
            push_workspace(&mut repo, &mut ws).context("push result")?;
        }

        Ok(())
    })();

    if let Err(err) = close_repo(repo) {
        if result.is_ok() {
            return Err(err);
        }
        eprintln!("warning: failed to close pile cleanly: {err:#}");
    }

    result
}

fn stop_requested(stop: &Option<Arc<AtomicBool>>) -> bool {
    stop.as_ref()
        .map(|flag| flag.load(Ordering::Relaxed))
        .unwrap_or(false)
}

fn execute_command(
    command: &str,
    cwd: Option<&str>,
    stdin: Option<Bytes>,
    env: &ExecCommandEnv,
    initial_timeout: Duration,
    stop: &Option<Arc<AtomicBool>>,
    mut poll_timeout_extension: impl FnMut() -> Result<Option<u64>>,
) -> ExecOutput {
    let mut cmd = Command::new("sh");
    cmd.arg("-lc").arg(command);
    // Make faculties available as plain commands (e.g. `orient`, `memory`) without requiring
    // hard-coded absolute paths.
    let base_path = std::env::var("PATH").unwrap_or_default();
    let extra_path = "/workspace/faculties:/opt/playground/faculties";
    let merged_path = if base_path.trim().is_empty() {
        extra_path.to_string()
    } else {
        format!("{extra_path}:{base_path}")
    };
    cmd.env("PATH", merged_path);
    cmd.env("PILE", &env.pile);
    cmd.env("CONFIG_BRANCH_ID", &env.config_branch_id);
    cmd.env("WORKER_ID", &env.worker_id);
    cmd.env("TURN_ID", &env.turn_id);
    if let Some(cwd) = cwd {
        cmd.current_dir(cwd);
    }
    if stdin.is_some() {
        cmd.stdin(Stdio::piped());
    } else {
        cmd.stdin(Stdio::null());
    }
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    #[cfg(unix)]
    // Spawn each command in its own process group so timeout cancellation can terminate
    // the whole subtree, not only the top-level shell process.
    unsafe {
        cmd.pre_exec(|| {
            if libc::setpgid(0, 0) == 0 {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        });
    }

    let mut child = match cmd.spawn() {
        Ok(child) => child,
        Err(err) => {
            return ExecOutput {
                stdout: Vec::new(),
                stderr: Vec::new(),
                exit_code: None,
                stdout_text: None,
                stderr_text: None,
                error: Some(format!("spawn failed: {err}")),
            };
        }
    };

    let mut stdout_reader = child.stdout.take().map(spawn_output_reader);
    let mut stderr_reader = child.stderr.take().map(spawn_output_reader);

    if let Some(stdin) = stdin {
        if let Some(mut handle) = child.stdin.take() {
            let _ = handle.write_all(stdin.as_ref());
        }
    }

    let mut deadline = Instant::now() + initial_timeout;
    let wait_started = Instant::now();
    let mut timed_out = false;
    let mut timed_out_after: Option<Duration> = None;
    let mut killed_for_stop = false;
    let mut wait_error: Option<String> = None;
    let mut status_code: Option<i32> = None;

    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                status_code = status.code();
                break;
            }
            Ok(None) => {}
            Err(err) => {
                wait_error = Some(format!("wait failed: {err}"));
                let _ = terminate_process_tree(&mut child);
                break;
            }
        }

        match poll_timeout_extension() {
            Ok(Some(extension_ms)) => {
                let extension = Duration::from_millis(extension_ms);
                deadline = deadline.max(Instant::now() + extension);
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!("warning: timeout extension poll failed: {err:#}");
            }
        }

        if stop_requested(stop) {
            killed_for_stop = true;
            let _ = terminate_process_tree(&mut child);
            let _ = child.wait();
            status_code = Some(130);
            break;
        }

        if Instant::now() >= deadline {
            timed_out = true;
            timed_out_after = Some(wait_started.elapsed());
            let _ = terminate_process_tree(&mut child);
            let _ = child.wait();
            status_code = Some(124);
            break;
        }

        sleep(Duration::from_millis(EXEC_CONTROL_POLL_MS));
    }

    let stdout = stdout_reader
        .take()
        .map(join_output_reader)
        .unwrap_or_default();
    let mut stderr = stderr_reader
        .take()
        .map(join_output_reader)
        .unwrap_or_default();

    if timed_out {
        let timeout_hint =
            format_timeout_hint(timed_out_after.unwrap_or_else(|| wait_started.elapsed()));
        let mut msg = format!("{timeout_hint}\n").into_bytes();
        msg.extend_from_slice(&stderr);
        stderr = msg;
    } else if killed_for_stop {
        let mut msg = b"command interrupted: worker stop requested\n".to_vec();
        msg.extend_from_slice(&stderr);
        stderr = msg;
    }

    let stdout_text = std::str::from_utf8(&stdout).ok().map(str::to_string);
    let stderr_text = std::str::from_utf8(&stderr).ok().map(str::to_string);

    let error = if timed_out {
        Some(format_timeout_hint(
            timed_out_after.unwrap_or_else(|| wait_started.elapsed()),
        ))
    } else if killed_for_stop {
        Some("command interrupted: worker stop requested".to_string())
    } else {
        wait_error
    };

    ExecOutput {
        stdout,
        stderr,
        exit_code: status_code,
        stdout_text,
        stderr_text,
        error,
    }
}

fn spawn_output_reader<R>(mut reader: R) -> thread::JoinHandle<Vec<u8>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut bytes = Vec::new();
        let _ = reader.read_to_end(&mut bytes);
        bytes
    })
}

fn join_output_reader(handle: thread::JoinHandle<Vec<u8>>) -> Vec<u8> {
    handle.join().unwrap_or_default()
}

fn terminate_process_tree(child: &mut std::process::Child) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let pid = child.id() as i32;
        // Negative pid targets the entire process group created in pre_exec.
        unsafe {
            libc::kill(-pid, libc::SIGTERM);
        }
        sleep(Duration::from_millis(50));
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        child.kill()
    }
}

fn format_timeout_hint(duration: Duration) -> String {
    format!(
        "command timed out after {:.1}s; for long-running work, retry with `patience <duration> -- <command>`",
        duration.as_secs_f64()
    )
}

fn maybe_bootstrap_workspace(repo: &mut Repository<Pile>, config: &Config) -> Result<()> {
    let root = PathBuf::from("/workspace");
    let branch_id = config.workspace_branch_id.ok_or_else(|| {
        anyhow!(
            "config missing workspace_branch_id; run `playground config set workspace-branch-id <ID>`"
        )
    })?;

    if !root.exists() {
        fs::create_dir_all(&root)
            .with_context(|| format!("create workspace root {}", root.display()))?;
    }

    ensure_branch(repo, branch_id, DEFAULT_WORKSPACE_BRANCH)
        .with_context(|| format!("ensure workspace branch {branch_id:x}"))?;
    if let Some(report) = restore_snapshot_merge(repo, branch_id, None, &root)? {
        if report.created_entries > 0 || report.conflicting_entries > 0 {
            eprintln!(
                "workspace bootstrap: snapshot {snapshot:x}, lineage={}, merged={}, created={}, unchanged={}, conflicts={}",
                report.lineage_len,
                report.merged_entries,
                report.created_entries,
                report.unchanged_entries,
                report.conflicting_entries,
                snapshot = report.snapshot_id
            );
        }
    }
    Ok(())
}

impl CommandRequestIndex {
    fn apply_delta(&mut self, updated: &TribleSet, delta: &TribleSet, worker_id: Id) {
        if delta.is_empty() {
            return;
        }

        for (request_id, command) in find!(
            (request_id: Id, command: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?request_id @
                playground_exec::kind: playground_exec::kind_command_request,
                playground_exec::command_text: ?command,
            }])
        ) {
            self.requests.insert(
                request_id,
                CommandRequest {
                    id: request_id,
                    command,
                    requested_at: None,
                    cwd: None,
                    stdin: None,
                    stdin_text: None,
                    timeout_ms: None,
                },
            );
        }

        for (request_id, requested_at) in find!(
            (request_id: Id, requested_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?request_id @ playground_exec::requested_at: ?requested_at
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.requested_at = Some(requested_at);
            }
        }

        for (request_id, cwd) in find!(
            (request_id: Id, cwd: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?request_id @ playground_exec::cwd: ?cwd
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.cwd = Some(cwd);
            }
        }

        for (request_id, stdin) in find!(
            (request_id: Id, stdin: Value<Handle<Blake3, UnknownBlob>>),
            pattern_changes!(updated, delta, [{
                ?request_id @ playground_exec::stdin: ?stdin
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.stdin = Some(stdin);
            }
        }

        for (request_id, stdin_text) in find!(
            (request_id: Id, stdin_text: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?request_id @ playground_exec::stdin_text: ?stdin_text
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.stdin_text = Some(stdin_text);
            }
        }

        for (request_id, timeout_ms) in find!(
            (request_id: Id, timeout_ms: Value<U256BE>),
            pattern_changes!(updated, delta, [{
                ?request_id @ playground_exec::timeout_ms: ?timeout_ms
            }])
        ) {
            if let Some(entry) = self.requests.get_mut(&request_id) {
                entry.timeout_ms = Some(timeout_ms);
            }
        }

        for (request_id, in_progress_worker_id) in find!(
            (
                request_id: Id,
                in_progress_worker_id: Id
            ),
            pattern_changes!(updated, delta, [{
                _?event @
                playground_exec::kind: playground_exec::kind_in_progress,
                playground_exec::about_request: ?request_id,
                playground_exec::worker: ?in_progress_worker_id,
            }])
        ) {
            if in_progress_worker_id == worker_id {
                self.in_progress_by_worker.insert(request_id);
            }
        }

        for (request_id,) in find!(
            (request_id: Id),
            pattern_changes!(updated, delta, [{
                _?event @
                playground_exec::kind: playground_exec::kind_command_result,
                playground_exec::about_request: ?request_id,
            }])
        ) {
            self.done.insert(request_id);
        }
    }

    fn next_pending(&self) -> Option<CommandRequest> {
        let mut candidates: Vec<CommandRequest> = self
            .requests
            .values()
            .filter(|req| {
                !self.in_progress_by_worker.contains(&req.id) && !self.done.contains(&req.id)
            })
            .cloned()
            .collect();
        candidates.sort_by_key(|req| req.requested_at.map(interval_key).unwrap_or(i128::MIN));
        candidates.into_iter().next()
    }
}

fn collect_timeout_extension_ms(
    updated: &TribleSet,
    delta: &TribleSet,
    request_id: Id,
    worker_id: Id,
) -> Option<u64> {
    let mut extension_ms: Option<u64> = None;
    for (_event_id, timeout_ms) in find!(
        (_event_id: Id, timeout_ms: Value<U256BE>),
        pattern_changes!(updated, delta, [{
            ?_event_id @
            playground_exec::kind: playground_exec::kind_timeout_extension,
            playground_exec::about_request: request_id,
            playground_exec::worker: worker_id,
            playground_exec::timeout_ms: ?timeout_ms,
        }])
    ) {
        let Some(timeout_ms) = u256be_to_u64(timeout_ms) else {
            continue;
        };
        if timeout_ms == 0 {
            continue;
        }
        extension_ms = Some(extension_ms.map_or(timeout_ms, |current| current.max(timeout_ms)));
    }
    extension_ms
}

fn u256be_to_u64(value: Value<U256BE>) -> Option<u64> {
    let raw = value.raw;
    if raw[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    let bytes: [u8; 8] = raw[24..32].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

fn load_stdin(ws: &mut Workspace<Pile>, request: &CommandRequest) -> Result<Option<Bytes>> {
    if let Some(stdin) = request.stdin {
        let bytes: Bytes = ws.get(stdin).context("read stdin bytes")?;
        return Ok(Some(bytes));
    }

    if let Some(stdin_text) = request.stdin_text {
        let text = load_text(ws, stdin_text)?;
        return Ok(Some(Bytes::from_source(text.into_bytes())));
    }

    Ok(None)
}

fn id_prefix(id: Id) -> String {
    let raw: [u8; 16] = id.into();
    let mut out = String::with_capacity(8);
    for byte in raw.iter().take(4) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
