use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, sleep};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use triblespace::core::blob::Bytes;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

mod branch_util;
mod config;
mod diagnostics;
mod exec_worker;
mod llm_worker;
mod repo_ops;
mod repo_util;
mod schema;
mod time_util;
mod workspace_snapshot;

use config::Config;
use repo_util::{init_repo, load_text, push_workspace};
use schema::{openai_responses, playground_cog, playground_exec};
use time_util::{epoch_interval, interval_key, now_epoch};

#[derive(Subcommand, Debug)]
enum CommandMode {
    #[command(about = "Run core + LLM and start the exec worker in a Lima VM")]
    Run(RunArgs),
    #[command(about = "Run only the core loop (no LLM/exec workers)")]
    Core,
    #[command(about = "Run only the exec worker (remote host)")]
    Exec(WorkerArgs),
    #[command(about = "Run only the LLM worker (host)")]
    Llm(WorkerArgs),
    #[command(about = "Open the diagnostics dashboard")]
    Diagnostics(DiagnosticsArgs),
    Config {
        #[command(subcommand)]
        command: ConfigCommand,
    },
}

#[derive(Subcommand, Debug)]
enum ConfigCommand {
    Show {
        #[arg(long, default_value_t = false)]
        show_secrets: bool,
    },
    Set(ConfigSetArgs),
}

#[derive(Args, Debug, Clone)]
#[command(about = "Run core + LLM and start the exec worker in a Lima VM")]
struct RunArgs {
    #[arg(long)]
    poll_ms: Option<u64>,
    #[command(flatten)]
    lima: LimaExecArgs,
}

#[derive(Args, Debug, Clone)]
#[command(about = "Exec worker settings")]
struct WorkerArgs {
    #[arg(long)]
    worker_id: Option<String>,
    #[arg(long)]
    poll_ms: Option<u64>,
}

#[derive(Args, Debug, Clone)]
#[command(about = "Diagnostics dashboard settings")]
struct DiagnosticsArgs {
    #[arg(long, default_value_t = false)]
    headless: bool,
    #[arg(long)]
    out_dir: Option<PathBuf>,
    #[arg(long)]
    scale: Option<f32>,
    #[arg(long)]
    headless_wait_ms: Option<u64>,
}

#[derive(Args, Debug, Clone)]
#[command(about = "Lima VM settings for the exec worker")]
struct LimaExecArgs {
    #[arg(long, default_value = "playground")]
    instance: String,
    #[arg(long)]
    template: Option<PathBuf>,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, default_value = "/workspace")]
    vm_root: PathBuf,
    #[arg(long)]
    recreate: bool,
}
#[derive(Args, Debug, Clone)]
#[command(about = "Set a single config field in the pile")]
struct ConfigSetArgs {
    #[arg(
        value_enum,
        value_name = "FIELD",
        help = "Config field to set (see possible values below)."
    )]
    field: ConfigField,
    #[arg(
        value_name = "VALUE",
        help = "Value to set. Use @path to read from file; use null/none/empty to clear optional fields."
    )]
    value: String,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
#[value(rename_all = "kebab-case")]
enum ConfigField {
    SystemPrompt,
    SeedPrompt,
    Branch,
    BranchId,
    Author,
    AuthorRole,
    PersonaId,
    PollMs,
    LlmModel,
    LlmBaseUrl,
    LlmApiKey,
    LlmReasoningEffort,
    LlmStream,
    ExecDefaultCwd,
    ExecSandboxProfile,
}

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Playground runner that turns LLM responses into exec requests"
)]
struct Cli {
    #[arg(long, global = true)]
    pile: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<CommandMode>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let Some(command) = cli.command else {
        let mut command = Cli::command();
        command.print_help()?;
        println!();
        return Ok(());
    };
    match command {
        CommandMode::Run(args) => {
            let instance = resolve_instance_name(args.lima.instance.as_str());
            let pile_path = resolve_pile_path(cli.pile.clone(), instance.as_str());
            let config = Config::load(Some(pile_path.as_path())).context("load config")?;
            run_with_exec(config, args)
        }
        CommandMode::Core => {
            let instance = default_instance_name();
            let pile_path = resolve_pile_path(cli.pile.clone(), instance.as_str());
            let config = Config::load(Some(pile_path.as_path())).context("load config")?;
            run_loop(config)
        }
        CommandMode::Exec(args) => {
            let instance = default_instance_name();
            let pile_path = resolve_pile_path(cli.pile.clone(), instance.as_str());
            let config = Config::load(Some(pile_path.as_path())).context("load config")?;
            run_exec_worker(config, args)
        }
        CommandMode::Llm(args) => {
            let instance = default_instance_name();
            let pile_path = resolve_pile_path(cli.pile.clone(), instance.as_str());
            let config = Config::load(Some(pile_path.as_path())).context("load config")?;
            run_llm_worker(config, args)
        }
        CommandMode::Diagnostics(args) => {
            let _ = args;
            let instance = default_instance_name();
            let pile_path = resolve_pile_path(cli.pile.clone(), instance.as_str());
            diagnostics::set_default_pile(Some(pile_path));
            diagnostics::diagnostics();
            Ok(())
        }
        CommandMode::Config { command } => {
            let instance = default_instance_name();
            let pile_path = resolve_pile_path(cli.pile.clone(), instance.as_str());
            handle_config(Some(pile_path.as_path()), command)
        }
    }
}

fn run_with_exec(mut config: Config, args: RunArgs) -> Result<()> {
    let poll_ms = args.poll_ms.unwrap_or(config.poll_ms);
    config.poll_ms = poll_ms;

    let stop = Arc::new(AtomicBool::new(false));
    let llm_stop = stop.clone();
    let llm_config = config.clone();
    let llm_worker_id = *ufoid();
    let llm_handle = thread::spawn(move || {
        llm_worker::run_llm_loop(llm_config, llm_worker_id, poll_ms, Some(llm_stop))
    });

    prepare_lima_service(&config, &args.lima)?;

    let core_result = run_loop(config);
    stop.store(true, Ordering::Relaxed);

    let llm_result = llm_handle
        .join()
        .map_err(|_| anyhow!("llm worker panicked"))?;

    core_result?;
    llm_result.context("llm worker")?;
    Ok(())
}

fn run_exec_worker(config: Config, args: WorkerArgs) -> Result<()> {
    let poll_ms = args.poll_ms.unwrap_or(config.poll_ms);
    let worker_id = parse_worker_id(args.worker_id)?;
    exec_worker::run_exec_loop(config, worker_id, poll_ms, None)
}

fn run_llm_worker(config: Config, args: WorkerArgs) -> Result<()> {
    let poll_ms = args.poll_ms.unwrap_or(config.poll_ms);
    let worker_id = parse_worker_id(args.worker_id)?;
    llm_worker::run_llm_loop(config, worker_id, poll_ms, None)
}

fn handle_config(pile: Option<&Path>, command: ConfigCommand) -> Result<()> {
    let mut config = Config::load(pile).context("load config")?;
    match command {
        ConfigCommand::Show { show_secrets } => {
            print_config(&config, show_secrets);
        }
        ConfigCommand::Set(args) => {
            apply_config_set(&mut config, args)?;
            config.store().context("store config")?;
            print_config(&config, false);
        }
    }
    Ok(())
}

fn apply_config_set(config: &mut Config, args: ConfigSetArgs) -> Result<()> {
    match args.field {
        ConfigField::SystemPrompt => {
            config.system_prompt = load_value_or_file(args.value.as_str(), "system_prompt")?;
        }
        ConfigField::SeedPrompt => {
            config.seed_prompt = load_value_or_file(args.value.as_str(), "seed_prompt")?;
        }
        ConfigField::Branch => {
            config.branch = load_value_or_file(args.value.as_str(), "branch")?;
        }
        ConfigField::BranchId => {
            config.branch_id = parse_optional_hex_id(Some(args.value.as_str()), "branch_id")?;
        }
        ConfigField::Author => {
            config.author = load_value_or_file(args.value.as_str(), "author")?;
        }
        ConfigField::AuthorRole => {
            config.author_role = load_value_or_file(args.value.as_str(), "author_role")?;
        }
        ConfigField::PersonaId => {
            config.persona_id = parse_optional_hex_id(Some(args.value.as_str()), "persona_id")?;
        }
        ConfigField::PollMs => {
            config.poll_ms = parse_u64(args.value.as_str(), "poll_ms")?;
        }
        ConfigField::LlmModel => {
            config.llm.model = load_value_or_file(args.value.as_str(), "llm_model")?;
        }
        ConfigField::LlmBaseUrl => {
            config.llm.base_url = load_value_or_file(args.value.as_str(), "llm_base_url")?;
        }
        ConfigField::LlmApiKey => {
            config.llm.api_key = load_optional_string_or_file(args.value.as_str(), "llm_api_key")?;
        }
        ConfigField::LlmReasoningEffort => {
            config.llm.reasoning_effort =
                load_optional_string_or_file(args.value.as_str(), "llm_reasoning_effort")?;
        }
        ConfigField::LlmStream => {
            config.llm.stream = parse_bool(args.value.as_str(), "llm_stream")?;
        }
        ConfigField::ExecDefaultCwd => {
            config.exec.default_cwd = parse_optional_path(args.value.as_str());
        }
        ConfigField::ExecSandboxProfile => {
            config.exec.sandbox_profile =
                parse_optional_hex_id(Some(args.value.as_str()), "exec_sandbox_profile")?;
        }
    }
    Ok(())
}

fn parse_optional_hex_id(raw: Option<&str>, label: &str) -> Result<Option<Id>> {
    let Some(raw) = raw else {
        return Ok(None);
    };
    if raw.is_empty() {
        return Ok(None);
    }
    let id = Id::from_hex(raw).ok_or_else(|| anyhow!("invalid {label} {raw}"))?;
    Ok(Some(id))
}

fn parse_worker_id(raw: Option<String>) -> Result<Id> {
    if let Some(hex) = raw {
        let id = Id::from_hex(hex.as_str()).ok_or_else(|| anyhow!("invalid worker_id {hex}"))?;
        return Ok(id);
    }
    Ok(*ufoid())
}

fn prepare_lima_service(config: &Config, args: &LimaExecArgs) -> Result<()> {
    let repo_root = repo_root();
    let playground_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

    let instance = env_string("PLAYGROUND_LIMA_INSTANCE").unwrap_or_else(|| args.instance.clone());
    let vm_root = env_path("PLAYGROUND_LIMA_ROOT").unwrap_or_else(|| args.vm_root.clone());

    let pile_abs = absolute_pile_path(&config.pile_path)?;
    ensure_append_only(&pile_abs)?;
    let pile_root = pile_abs
        .parent()
        .ok_or_else(|| anyhow!("pile path missing parent directory"))?
        .to_path_buf();

    let persona_root = repo_root.join("personas").join(&instance);
    fs::create_dir_all(&persona_root).ok();
    let workspace_root = persona_root.join("workspace");
    fs::create_dir_all(&workspace_root).ok();

    let template = env_path("PLAYGROUND_LIMA_TEMPLATE")
        .or_else(|| args.template.clone())
        .unwrap_or_else(|| playground_root.join("scripts/lima.yaml.tmpl"));
    if !template.exists() {
        return Err(anyhow!("missing Lima template: {}", template.display()));
    }

    let config_path = env_path("PLAYGROUND_LIMA_CONFIG")
        .or_else(|| args.config.clone())
        .unwrap_or_else(|| persona_root.join("state/lima.yaml"));
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent).context("create Lima config directory")?;
    }

    let pile_name = pile_abs
        .file_name()
        .ok_or_else(|| anyhow!("pile path missing filename"))?;
    let pile_vm = PathBuf::from("/pile").join(pile_name);
    render_lima_template(
        &template,
        &config_path,
        &playground_root,
        &pile_root,
        &workspace_root,
        &pile_vm,
        &vm_root,
    )?;

    ensure_lima_instance(&instance, &config_path, args.recreate)?;
    Ok(())
}

fn ensure_lima_instance(instance: &str, config_path: &Path, recreate: bool) -> Result<()> {
    limactl_output(&["list"])?; // validates limactl exists
    let names = limactl_output(&["list", "--format", "{{.Name}}"])?;
    let exists = names.lines().any(|line| line.trim() == instance);

    let recreate = if recreate {
        true
    } else {
        env_flag("PLAYGROUND_LIMA_RECREATE")
    };

    if exists && recreate {
        limactl_output(&["delete", "--force", instance])?;
    }

    let exists = if recreate { false } else { exists };
    if exists {
        limactl_output(&["start", instance])?;
    } else {
        let config_arg = config_path.to_string_lossy();
        limactl_output(&["start", "--name", instance, config_arg.as_ref()])?;
    }
    Ok(())
}

fn limactl_output(args: &[&str]) -> Result<String> {
    let output = Command::new("limactl")
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .context("run limactl")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("limactl {:?} failed: {}", args, stderr.trim()));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn render_lima_template(
    template: &Path,
    out_path: &Path,
    playground_root: &Path,
    pile_root: &Path,
    workspace_root: &Path,
    pile_vm: &Path,
    vm_root: &Path,
) -> Result<()> {
    let mut text = fs::read_to_string(template)
        .with_context(|| format!("read Lima template {}", template.display()))?;

    let replacements = [
        ("__PLAYGROUND_ROOT__", playground_root),
        ("__PILE_ROOT__", pile_root),
        ("__WORKSPACE_ROOT__", workspace_root),
        ("__PILE_PATH__", pile_vm),
        ("__VM_ROOT__", vm_root),
    ];

    for (token, path) in replacements {
        text = text.replace(token, &path.to_string_lossy());
    }

    fs::write(out_path, text)
        .with_context(|| format!("write Lima config {}", out_path.display()))?;
    Ok(())
}

#[cfg(target_os = "macos")]
fn ensure_append_only(path: &Path) -> Result<()> {
    if !path.exists() {
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .with_context(|| format!("create pile file {}", path.display()))?;
    }
    let status = Command::new("chflags")
        .args(["uappnd", path.to_string_lossy().as_ref()])
        .status()
        .with_context(|| format!("set append-only on {}", path.display()))?;
    if !status.success() {
        return Err(anyhow!("chflags uappnd failed for {}", path.display()));
    }
    Ok(())
}

#[cfg(not(target_os = "macos"))]
fn ensure_append_only(_path: &Path) -> Result<()> {
    Ok(())
}

fn absolute_pile_path(path: &Path) -> Result<PathBuf> {
    let abs = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let parent = abs
        .parent()
        .ok_or_else(|| anyhow!("pile path has no parent"))?;
    fs::create_dir_all(parent).context("create pile directory")?;
    let parent_abs = parent
        .canonicalize()
        .context("canonicalize pile directory")?;
    let file = abs
        .file_name()
        .ok_or_else(|| anyhow!("pile path missing filename"))?;
    Ok(parent_abs.join(file))
}

fn default_instance_name() -> String {
    env_string("PLAYGROUND_INSTANCE")
        .or_else(|| env_string("PLAYGROUND_LIMA_INSTANCE"))
        .unwrap_or_else(|| "playground".to_string())
}

fn resolve_instance_name(default: &str) -> String {
    env_string("PLAYGROUND_LIMA_INSTANCE").unwrap_or_else(|| default.to_string())
}

fn default_pile_path(instance: &str) -> PathBuf {
    repo_root()
        .join("personas")
        .join(instance)
        .join("pile")
        .join("self.pile")
}

fn resolve_pile_path(explicit: Option<PathBuf>, instance: &str) -> PathBuf {
    explicit.unwrap_or_else(|| default_pile_path(instance))
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn env_flag(key: &str) -> bool {
    let value = std::env::var(key).ok();
    let Some(value) = value else {
        return false;
    };
    let trimmed = value.trim();
    trimmed == "1" || trimmed.eq_ignore_ascii_case("true") || trimmed.eq_ignore_ascii_case("yes")
}

fn env_string(key: &str) -> Option<String> {
    std::env::var(key).ok().and_then(|value| {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn env_path(key: &str) -> Option<PathBuf> {
    env_string(key).map(PathBuf::from)
}

fn load_value_or_file(raw: &str, label: &str) -> Result<String> {
    if let Some(path) = raw.strip_prefix('@') {
        return fs::read_to_string(path).with_context(|| format!("read {label} from {}", path));
    }
    Ok(raw.to_string())
}

fn load_optional_string_or_file(raw: &str, label: &str) -> Result<Option<String>> {
    let value = load_value_or_file(raw, label)?;
    let trimmed = value.trim();
    if is_nullish(trimmed) {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

fn parse_u64(raw: &str, label: &str) -> Result<u64> {
    raw.parse::<u64>()
        .map_err(|_| anyhow!("invalid {label} {raw}"))
}

fn parse_bool(raw: &str, label: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Ok(true),
        "false" | "0" | "no" => Ok(false),
        _ => Err(anyhow!("invalid {label} {raw} (expected true/false)")),
    }
}

fn parse_optional_path(raw: &str) -> Option<PathBuf> {
    if is_nullish(raw) {
        None
    } else {
        Some(PathBuf::from(raw))
    }
}

fn is_nullish(raw: &str) -> bool {
    let value = raw.trim().to_ascii_lowercase();
    value.is_empty() || value == "null" || value == "none"
}

fn print_config(config: &Config, show_secrets: bool) {
    println!("pile = \"{}\"", config.pile_path.display());
    println!("branch = \"{}\"", config.branch);
    println!(
        "branch_id = {}",
        config
            .branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!("poll_ms = {}", config.poll_ms);
    println!("author = \"{}\"", config.author);
    println!("author_role = \"{}\"", config.author_role);
    println!(
        "persona_id = {}",
        config
            .persona_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "system_prompt = \"{}\"",
        config.system_prompt.replace('\"', "\\\"")
    );
    println!(
        "seed_prompt = \"{}\"",
        config.seed_prompt.replace('\"', "\\\"")
    );

    println!("\n[llm]");
    println!("model = \"{}\"", config.llm.model);
    println!("base_url = \"{}\"", config.llm.base_url);
    match (&config.llm.api_key, show_secrets) {
        (Some(key), true) => println!("api_key = \"{}\"", key),
        (Some(_), false) => println!("api_key = \"<redacted>\""),
        (None, _) => println!("api_key = null"),
    }
    println!(
        "reasoning_effort = {}",
        config
            .llm
            .reasoning_effort
            .as_ref()
            .map(|value| format!("\"{}\"", value))
            .unwrap_or_else(|| "null".to_string())
    );
    println!("stream = {}", config.llm.stream);

    println!("\n[exec]");
    println!(
        "default_cwd = {}",
        config
            .exec
            .default_cwd
            .as_ref()
            .map(|path| format!("\"{}\"", path.display()))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "sandbox_profile = {}",
        config
            .exec
            .sandbox_profile
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
}

fn run_loop(config: Config) -> Result<()> {
    let (mut repo, branch_id) = init_repo(&config).context("open triblespace repo")?;
    repo_util::seed_metadata(&mut repo)?;
    let exec_cwd = config
        .exec
        .default_cwd
        .as_ref()
        .map(|path| path.to_string_lossy().to_string());
    let exec_profile = config.exec.sandbox_profile;

    let mut request_info = ensure_llm_request(&mut repo, branch_id, &config)?;

    loop {
        let llm_result =
            wait_for_llm_result(&mut repo, branch_id, request_info.id, config.poll_ms)?;
        if let Some(error) = llm_result.error {
            return Err(anyhow!("llm request failed: {error}"));
        }

        let command = llm_result.output_text.trim();
        if command.eq_ignore_ascii_case("exit") {
            break;
        }

        let command_request_id = ensure_command_request(
            &mut repo,
            branch_id,
            command,
            request_info.thought_id,
            exec_cwd.as_deref(),
            exec_profile,
        )?;
        let command_result =
            wait_for_command_result(&mut repo, branch_id, command_request_id, config.poll_ms)?;

        let prompt_body = format_exec_output(command, command_result.result);
        let prompt = compose_prompt(&config.system_prompt, &prompt_body);
        request_info = create_thought_and_request(
            &mut repo,
            branch_id,
            &prompt,
            Some(command_result.id),
            &config,
        )?;
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct LlmRequestInfo {
    id: Id,
    thought_id: Option<Id>,
}

#[derive(Debug, Clone)]
struct ThoughtRecord {
    id: Id,
    created_at_key: i128,
}

fn ensure_llm_request(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    config: &Config,
) -> Result<LlmRequestInfo> {
    loop {
        let mut ws = repo
            .pull(branch_id)
            .map_err(|err| anyhow!("pull workspace for llm request: {err:?}"))?;
        let catalog = ws.checkout(..).context("checkout workspace")?;

        if let Some(request) = latest_pending_llm_request(&catalog) {
            return Ok(request);
        }

        if let Some(thought_id) = latest_unrequested_thought(&catalog) {
            let request_id = create_request_for_thought(&mut ws, &catalog, thought_id, config)?;
            push_workspace(repo, &mut ws).context("push llm request")?;
            return Ok(LlmRequestInfo {
                id: request_id,
                thought_id: Some(thought_id),
            });
        }

        if let Some(exec_result) = latest_unprocessed_exec_result(&catalog) {
            let prompt = prompt_from_exec_result(&mut ws, &catalog, &exec_result, config)?;
            let request =
                create_thought_and_request(repo, branch_id, &prompt, Some(exec_result.id), config)?;
            return Ok(request);
        }

        if list_thoughts_by_created(&catalog).is_empty()
            && collect_command_results(&catalog).is_empty()
        {
            if !has_pending_command_request(&catalog) {
                drop(ws);
                let command = orient_bootstrap_command(config);
                ensure_command_request(
                    repo,
                    branch_id,
                    &command,
                    None,
                    config.exec.default_cwd.as_ref().and_then(|p| p.to_str()),
                    config.exec.sandbox_profile,
                )?;
            }
            sleep(Duration::from_millis(config.poll_ms));
            continue;
        }

        sleep(Duration::from_millis(config.poll_ms));
    }
}

fn orient_bootstrap_command(config: &Config) -> String {
    let _ = config;
    "./faculties/orient.rs show".to_string()
}

fn create_thought_and_request(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    prompt: &str,
    about_exec_result: Option<Id>,
    config: &Config,
) -> Result<LlmRequestInfo> {
    let mut ws = repo
        .pull(branch_id)
        .map_err(|err| anyhow!("pull workspace for thought: {err:?}"))?;
    let catalog = ws.checkout(..).context("checkout workspace")?;

    if let Some(exec_result_id) = about_exec_result {
        if let Some(thought_id) = thought_for_exec_result(&catalog, exec_result_id) {
            let request_id = create_request_for_thought(&mut ws, &catalog, thought_id, config)?;
            push_workspace(repo, &mut ws).context("push llm request")?;
            return Ok(LlmRequestInfo {
                id: request_id,
                thought_id: Some(thought_id),
            });
        }
    }

    let now = epoch_interval(now_epoch());
    let prompt_handle = ws.put(prompt.to_owned());
    let thought_id = ufoid();
    let mut change = TribleSet::new();
    change += entity! { &thought_id @
        playground_cog::kind: playground_cog::kind_thought,
        playground_cog::prompt: prompt_handle,
        playground_cog::created_at: now,
    };
    if let Some(exec_result_id) = about_exec_result {
        change += entity! { &thought_id @ playground_cog::about_exec_result: exec_result_id };
    }

    let request_id = ufoid();
    change += entity! { &request_id @
        openai_responses::kind: openai_responses::kind_request,
        openai_responses::about_thought: *thought_id,
        openai_responses::prompt: prompt_handle,
        openai_responses::requested_at: now,
        openai_responses::model: config.llm.model.as_str(),
    };

    ws.commit(change, None, Some("create thought + llm request"));
    push_workspace(repo, &mut ws).context("push thought + request")?;

    Ok(LlmRequestInfo {
        id: *request_id,
        thought_id: Some(*thought_id),
    })
}

fn create_request_for_thought(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    thought_id: Id,
    config: &Config,
) -> Result<Id> {
    if let Some(request_id) = request_for_thought(catalog, thought_id) {
        return Ok(request_id);
    }

    let Some(prompt_handle) = thought_prompt_handle(catalog, thought_id) else {
        return Err(anyhow!("thought {thought_id:x} missing prompt"));
    };
    let now = epoch_interval(now_epoch());
    let request_id = ufoid();
    let mut change = TribleSet::new();
    change += entity! { &request_id @
        openai_responses::kind: openai_responses::kind_request,
        openai_responses::about_thought: thought_id,
        openai_responses::prompt: prompt_handle,
        openai_responses::requested_at: now,
        openai_responses::model: config.llm.model.as_str(),
    };
    ws.commit(change, None, Some("create llm request"));
    Ok(*request_id)
}

fn latest_pending_llm_request(catalog: &TribleSet) -> Option<LlmRequestInfo> {
    let results = llm_request_results(catalog);
    for record in list_llm_requests_by_requested(catalog).into_iter().rev() {
        if results.contains(&record.id) {
            continue;
        }
        let thought_id = request_thought_id(catalog, record.id);
        return Some(LlmRequestInfo {
            id: record.id,
            thought_id,
        });
    }
    None
}

#[derive(Debug, Clone)]
struct LlmRequestRecord {
    id: Id,
    requested_at_key: i128,
}

fn list_llm_requests_by_requested(catalog: &TribleSet) -> Vec<LlmRequestRecord> {
    let mut requests = Vec::new();
    for (request_id, requested_at) in find!(
        (request_id: Id, requested_at: Value<NsTAIInterval>),
        pattern!(catalog, [{
            ?request_id @
            openai_responses::kind: openai_responses::kind_request,
            openai_responses::requested_at: ?requested_at,
        }])
    ) {
        requests.push(LlmRequestRecord {
            id: request_id,
            requested_at_key: interval_key(requested_at),
        });
    }
    requests.sort_by(|left, right| {
        left.requested_at_key
            .cmp(&right.requested_at_key)
            .then_with(|| left.id.cmp(&right.id))
    });
    requests
}

fn llm_request_results(catalog: &TribleSet) -> HashSet<Id> {
    find!(
        (request_id: Id),
        pattern!(catalog, [{
            _?result @
            openai_responses::kind: openai_responses::kind_result,
            openai_responses::about_request: ?request_id,
        }])
    )
    .into_iter()
    .map(|(request_id,)| request_id)
    .collect()
}

fn request_thought_id(catalog: &TribleSet, request_id: Id) -> Option<Id> {
    find!(
        (request: Id, thought: Id),
        pattern!(catalog, [{
            ?request @ openai_responses::about_thought: ?thought
        }])
    )
    .into_iter()
    .find_map(|(request, thought)| (request == request_id).then_some(thought))
}

fn request_for_thought(catalog: &TribleSet, thought_id: Id) -> Option<Id> {
    find!(
        (request: Id, thought: Id),
        pattern!(catalog, [{
            ?request @ openai_responses::about_thought: ?thought
        }])
    )
    .into_iter()
    .find_map(|(request, thought)| (thought == thought_id).then_some(request))
}

fn thought_prompt_handle(
    catalog: &TribleSet,
    thought_id: Id,
) -> Option<Value<Handle<Blake3, LongString>>> {
    find!(
        (thought: Id, prompt: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?thought @ playground_cog::prompt: ?prompt
        }])
    )
    .into_iter()
    .find_map(|(thought, prompt)| (thought == thought_id).then_some(prompt))
}

fn list_thoughts_by_created(catalog: &TribleSet) -> Vec<ThoughtRecord> {
    let mut thoughts = Vec::new();
    for (thought_id, created_at) in find!(
        (thought_id: Id, created_at: Value<NsTAIInterval>),
        pattern!(catalog, [{
            ?thought_id @
            playground_cog::kind: playground_cog::kind_thought,
            playground_cog::created_at: ?created_at,
        }])
    ) {
        thoughts.push(ThoughtRecord {
            id: thought_id,
            created_at_key: interval_key(created_at),
        });
    }
    thoughts.sort_by(|left, right| {
        left.created_at_key
            .cmp(&right.created_at_key)
            .then_with(|| left.id.cmp(&right.id))
    });
    thoughts
}

fn latest_unrequested_thought(catalog: &TribleSet) -> Option<Id> {
    let requested: HashSet<Id> = find!(
        (thought: Id),
        pattern!(catalog, [{
            _?request @ openai_responses::about_thought: ?thought
        }])
    )
    .into_iter()
    .map(|(thought,)| thought)
    .collect();

    list_thoughts_by_created(catalog)
        .into_iter()
        .rev()
        .find(|record| !requested.contains(&record.id))
        .map(|record| record.id)
}

fn thought_for_exec_result(catalog: &TribleSet, exec_result_id: Id) -> Option<Id> {
    find!(
        (thought: Id, exec_result: Id),
        pattern!(catalog, [{
            ?thought @ playground_cog::about_exec_result: ?exec_result
        }])
    )
    .into_iter()
    .find_map(|(thought, exec_result)| (exec_result == exec_result_id).then_some(thought))
}

#[derive(Debug)]
struct LlmResult {
    output_text: String,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct LlmResultInfo {
    finished_at: Option<Value<NsTAIInterval>>,
    attempt: Option<Value<U256BE>>,
    output_text: Option<Value<Handle<Blake3, LongString>>>,
    error: Option<Value<Handle<Blake3, LongString>>>,
}

fn wait_for_llm_result(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    request_id: Id,
    poll_ms: u64,
) -> Result<LlmResult> {
    loop {
        let mut ws = repo
            .pull(branch_id)
            .map_err(|err| anyhow!("pull workspace for llm result: {err:?}"))?;
        let catalog = ws.checkout(..).context("checkout workspace")?;
        if let Some(result) = latest_llm_result(&catalog, request_id) {
            return load_llm_result(&mut ws, result);
        }
        sleep(Duration::from_millis(poll_ms));
    }
}

fn latest_llm_result(catalog: &TribleSet, request_id: Id) -> Option<LlmResultInfo> {
    let mut results: HashMap<Id, LlmResultInfo> = HashMap::new();
    for (result_id, about_request) in find!(
        (result_id: Id, about_request: Id),
        pattern!(catalog, [{
            ?result_id @
            openai_responses::kind: openai_responses::kind_result,
            openai_responses::about_request: ?about_request,
        }])
    ) {
        if about_request != request_id {
            continue;
        }
        results.insert(
            result_id,
            LlmResultInfo {
                finished_at: None,
                attempt: None,
                output_text: None,
                error: None,
            },
        );
    }

    if results.is_empty() {
        return None;
    }

    for (result_id, finished_at) in find!(
        (result_id: Id, finished_at: Value<NsTAIInterval>),
        pattern!(catalog, [{
            ?result_id @ openai_responses::finished_at: ?finished_at
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.finished_at = Some(finished_at);
        }
    }

    for (result_id, attempt) in find!(
        (result_id: Id, attempt: Value<U256BE>),
        pattern!(catalog, [{
            ?result_id @ openai_responses::attempt: ?attempt
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.attempt = Some(attempt);
        }
    }

    for (result_id, output_text) in find!(
        (result_id: Id, output_text: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?result_id @ openai_responses::output_text: ?output_text
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.output_text = Some(output_text);
        }
    }

    for (result_id, error) in find!(
        (result_id: Id, error: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?result_id @ openai_responses::error: ?error
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.error = Some(error);
        }
    }

    let mut candidates: Vec<LlmResultInfo> = results.into_values().collect();
    candidates.sort_by_key(|res| {
        let attempt = res.attempt.and_then(u256be_to_u64).unwrap_or_default();
        let finished_at = res.finished_at.map(interval_key).unwrap_or(i128::MIN);
        (attempt, finished_at)
    });
    candidates.pop()
}

fn load_llm_result(ws: &mut Workspace<Pile>, result: LlmResultInfo) -> Result<LlmResult> {
    let output_text = result
        .output_text
        .map(|handle| load_text(ws, handle))
        .transpose()?
        .unwrap_or_default();
    let error = result
        .error
        .map(|handle| load_text(ws, handle))
        .transpose()?;

    Ok(LlmResult { output_text, error })
}

#[derive(Debug)]
struct ExecResult {
    stdout_text: Option<String>,
    stderr_text: Option<String>,
    stdout: Option<Bytes>,
    stderr: Option<Bytes>,
    exit_code: Option<u64>,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct CommandResultInfo {
    id: Id,
    about_request: Id,
    finished_at: Option<Value<NsTAIInterval>>,
    attempt: Option<Value<U256BE>>,
    stdout: Option<Value<Handle<Blake3, UnknownBlob>>>,
    stderr: Option<Value<Handle<Blake3, UnknownBlob>>>,
    stdout_text: Option<Value<Handle<Blake3, LongString>>>,
    stderr_text: Option<Value<Handle<Blake3, LongString>>>,
    exit_code: Option<Value<U256BE>>,
    error: Option<Value<Handle<Blake3, LongString>>>,
}

#[derive(Debug)]
struct CommandResultOutput {
    id: Id,
    result: ExecResult,
}

fn ensure_command_request(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    command: &str,
    thought_id: Option<Id>,
    default_cwd: Option<&str>,
    sandbox_profile: Option<Id>,
) -> Result<Id> {
    loop {
        let mut ws = repo
            .pull(branch_id)
            .map_err(|err| anyhow!("pull workspace for command request: {err:?}"))?;
        let catalog = ws.checkout(..).context("checkout workspace")?;
        if let Some(thought_id) = thought_id {
            if let Some(existing) = command_request_for_thought(&catalog, thought_id) {
                return Ok(existing);
            }
        }

        let request_id = ufoid();
        let now = epoch_interval(now_epoch());
        let command_handle = ws.put(command.to_owned());
        let mut change = TribleSet::new();
        change += entity! { &request_id @
            playground_exec::kind: playground_exec::kind_command_request,
            playground_exec::command_text: command_handle,
            playground_exec::requested_at: now,
        };
        if let Some(thought_id) = thought_id {
            change += entity! { &request_id @ playground_exec::about_thought: thought_id };
        }
        if let Some(cwd) = default_cwd {
            let handle = ws.put(cwd.to_owned());
            change += entity! { &request_id @ playground_exec::cwd: handle };
        }
        if let Some(profile) = sandbox_profile {
            change += entity! { &request_id @ playground_exec::sandbox_profile: profile };
        }
        ws.commit(change, None, Some("playground_exec request"));
        push_workspace(repo, &mut ws).context("push command request")?;
        return Ok(*request_id);
    }
}

fn command_request_for_thought(catalog: &TribleSet, thought_id: Id) -> Option<Id> {
    find!(
        (request_id: Id, about_thought: Id),
        pattern!(catalog, [{
            ?request_id @
            playground_exec::kind: playground_exec::kind_command_request,
            playground_exec::about_thought: ?about_thought,
        }])
    )
    .into_iter()
    .find_map(|(request_id, about_thought)| (about_thought == thought_id).then_some(request_id))
}

fn command_request_command_handle(
    catalog: &TribleSet,
    request_id: Id,
) -> Option<Value<Handle<Blake3, LongString>>> {
    find!(
        (request: Id, command: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{ ?request @ playground_exec::command_text: ?command }])
    )
    .into_iter()
    .find_map(|(request, command)| (request == request_id).then_some(command))
}

fn wait_for_command_result(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    request_id: Id,
    poll_ms: u64,
) -> Result<CommandResultOutput> {
    loop {
        let mut ws = repo
            .pull(branch_id)
            .map_err(|err| anyhow!("pull workspace for command result: {err:?}"))?;
        let catalog = ws.checkout(..).context("checkout workspace")?;
        if let Some(result) = latest_command_result(&catalog, request_id) {
            let exec_result = load_exec_result(&mut ws, result.clone())?;
            return Ok(CommandResultOutput {
                id: result.id,
                result: exec_result,
            });
        }
        sleep(Duration::from_millis(poll_ms));
    }
}

fn collect_command_results(catalog: &TribleSet) -> Vec<CommandResultInfo> {
    let mut results: HashMap<Id, CommandResultInfo> = HashMap::new();
    for (result_id, about_request) in find!(
        (result_id: Id, about_request: Id),
        pattern!(catalog, [{
            ?result_id @
            playground_exec::kind: playground_exec::kind_command_result,
            playground_exec::about_request: ?about_request,
        }])
    ) {
        results.insert(
            result_id,
            CommandResultInfo {
                id: result_id,
                about_request,
                finished_at: None,
                attempt: None,
                stdout: None,
                stderr: None,
                stdout_text: None,
                stderr_text: None,
                exit_code: None,
                error: None,
            },
        );
    }

    if results.is_empty() {
        return Vec::new();
    }

    for (result_id, finished_at) in find!(
        (result_id: Id, finished_at: Value<NsTAIInterval>),
        pattern!(catalog, [{
            ?result_id @ playground_exec::finished_at: ?finished_at
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.finished_at = Some(finished_at);
        }
    }

    for (result_id, attempt) in find!(
        (result_id: Id, attempt: Value<U256BE>),
        pattern!(catalog, [{
            ?result_id @ playground_exec::attempt: ?attempt
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.attempt = Some(attempt);
        }
    }

    for (result_id, stdout) in find!(
        (result_id: Id, stdout: Value<Handle<Blake3, UnknownBlob>>),
        pattern!(catalog, [{
            ?result_id @ playground_exec::stdout: ?stdout
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.stdout = Some(stdout);
        }
    }

    for (result_id, stderr) in find!(
        (result_id: Id, stderr: Value<Handle<Blake3, UnknownBlob>>),
        pattern!(catalog, [{
            ?result_id @ playground_exec::stderr: ?stderr
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.stderr = Some(stderr);
        }
    }

    for (result_id, stdout_text) in find!(
        (result_id: Id, stdout_text: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?result_id @ playground_exec::stdout_text: ?stdout_text
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.stdout_text = Some(stdout_text);
        }
    }

    for (result_id, stderr_text) in find!(
        (result_id: Id, stderr_text: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?result_id @ playground_exec::stderr_text: ?stderr_text
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.stderr_text = Some(stderr_text);
        }
    }

    for (result_id, exit_code) in find!(
        (result_id: Id, exit_code: Value<U256BE>),
        pattern!(catalog, [{
            ?result_id @ playground_exec::exit_code: ?exit_code
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.exit_code = Some(exit_code);
        }
    }

    for (result_id, error) in find!(
        (result_id: Id, error: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?result_id @ playground_exec::error: ?error
        }])
    ) {
        if let Some(entry) = results.get_mut(&result_id) {
            entry.error = Some(error);
        }
    }

    results.into_values().collect()
}

fn collect_command_requests(catalog: &TribleSet) -> Vec<Id> {
    find!(
        (request_id: Id),
        pattern!(catalog, [{
            ?request_id @ playground_exec::kind: playground_exec::kind_command_request
        }])
    )
    .into_iter()
    .map(|(request_id,)| request_id)
    .collect()
}

fn has_pending_command_request(catalog: &TribleSet) -> bool {
    let requests = collect_command_requests(catalog);
    if requests.is_empty() {
        return false;
    }
    let completed: HashSet<Id> = collect_command_results(catalog)
        .into_iter()
        .map(|result| result.about_request)
        .collect();
    requests.iter().any(|id| !completed.contains(id))
}

fn latest_command_result(catalog: &TribleSet, request_id: Id) -> Option<CommandResultInfo> {
    let mut candidates: Vec<CommandResultInfo> = collect_command_results(catalog)
        .into_iter()
        .filter(|result| result.about_request == request_id)
        .collect();
    candidates.sort_by_key(|res| {
        let attempt = res.attempt.and_then(u256be_to_u64).unwrap_or_default();
        let finished_at = res.finished_at.map(interval_key).unwrap_or(i128::MIN);
        (attempt, finished_at)
    });
    candidates.pop()
}

fn latest_unprocessed_exec_result(catalog: &TribleSet) -> Option<CommandResultInfo> {
    let used: HashSet<Id> = find!(
        (result_id: Id),
        pattern!(catalog, [{
            _?thought @ playground_cog::about_exec_result: ?result_id
        }])
    )
    .into_iter()
    .map(|(result_id,)| result_id)
    .collect();

    let mut candidates: Vec<CommandResultInfo> = collect_command_results(catalog)
        .into_iter()
        .filter(|result| !used.contains(&result.id))
        .collect();
    candidates.sort_by_key(|res| res.finished_at.map(interval_key).unwrap_or(i128::MIN));
    candidates.pop()
}

fn prompt_from_exec_result(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    exec_result: &CommandResultInfo,
    config: &Config,
) -> Result<String> {
    let Some(command_handle) = command_request_command_handle(catalog, exec_result.about_request)
    else {
        return Err(anyhow!(
            "command request {id:x} missing command text",
            id = exec_result.about_request
        ));
    };
    let command = load_text(ws, command_handle).context("load command for exec result")?;
    let exec_output = load_exec_result(ws, exec_result.clone())?;
    let body = format_exec_output(&command, exec_output);
    Ok(compose_prompt(&config.system_prompt, &body))
}

fn load_exec_result(ws: &mut Workspace<Pile>, result: CommandResultInfo) -> Result<ExecResult> {
    let stdout_text = result
        .stdout_text
        .map(|handle| load_text(ws, handle))
        .transpose()?;
    let stderr_text = result
        .stderr_text
        .map(|handle| load_text(ws, handle))
        .transpose()?;
    let stdout = result
        .stdout
        .map(|handle| ws.get(handle).context("read stdout bytes"))
        .transpose()?;
    let stderr = result
        .stderr
        .map(|handle| ws.get(handle).context("read stderr bytes"))
        .transpose()?;
    let exit_code = result.exit_code.and_then(u256be_to_u64);
    let error = result
        .error
        .map(|handle| load_text(ws, handle))
        .transpose()?;

    Ok(ExecResult {
        stdout_text,
        stderr_text,
        stdout,
        stderr,
        exit_code,
        error,
    })
}

fn format_exec_output(command: &str, result: ExecResult) -> String {
    let mut text = String::new();
    append_section(&mut text, "command", command);
    let stdout = format_output_text(result.stdout_text, result.stdout);
    append_section(&mut text, "stdout", stdout.as_str());
    let stderr = format_output_text(result.stderr_text, result.stderr);
    append_section(&mut text, "stderr", stderr.as_str());

    if let Some(error) = result.error {
        append_section(&mut text, "error", error.as_str());
    }

    let exit_code = result
        .exit_code
        .map(|code| code.to_string())
        .unwrap_or_else(|| "none".to_string());
    text.push_str(&format!("exit_code: {exit_code}\n"));
    text
}

fn compose_prompt(system_prompt: &str, body: &str) -> String {
    if system_prompt.trim().is_empty() {
        return body.to_string();
    }
    if body.trim().is_empty() {
        return system_prompt.to_string();
    }
    format!("{system_prompt}\n\n{body}")
}

fn append_section(text: &mut String, label: &str, body: &str) {
    text.push_str(label);
    text.push_str(":\n");
    text.push_str(body);
    if !body.ends_with('\n') {
        text.push('\n');
    }
    text.push('\n');
}

fn format_output_text(text: Option<String>, bytes: Option<Bytes>) -> String {
    if let Some(text) = text {
        return text;
    }
    if let Some(bytes) = bytes {
        return String::from_utf8_lossy(bytes.as_ref()).to_string();
    }
    String::new()
}

fn u256be_to_u64(value: Value<U256BE>) -> Option<u64> {
    let raw = value.raw;
    if raw[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    let bytes: [u8; 8] = raw[24..32].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}
