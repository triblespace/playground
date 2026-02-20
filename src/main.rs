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
use reqwest::blocking::Client;
use triblespace::core::blob::Bytes;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::metadata;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

mod archive_schema;
mod blob_refs;
mod branch_util;
mod chat_prompt;
mod config;
#[cfg(feature = "diagnostics")]
mod diagnostics;
mod exec_worker;
mod llm_worker;
mod relations_schema;
mod repo_ops;
mod repo_util;
mod schema;
mod time_util;
mod workspace_snapshot;

use archive_schema::{playground_archive, playground_archive_import};
use chat_prompt::ChatMessage;
use config::Config;
use relations_schema::playground_relations;
use repo_util::{
    close_repo, current_branch_head, init_repo, load_text, pull_workspace, push_workspace,
    refresh_cached_checkout,
};
use schema::{llm_chat, playground_cog, playground_context, playground_exec};
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
    #[cfg(feature = "diagnostics")]
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
    #[command(about = "Clear an optional config field in the pile")]
    Unset(ConfigUnsetArgs),
    #[command(about = "Manage LLM profiles (headspaces)")]
    Profile {
        #[command(subcommand)]
        command: ProfileCommand,
    },
}

#[derive(Subcommand, Debug)]
enum ProfileCommand {
    #[command(about = "List available profiles")]
    List,
    #[command(about = "Create a new profile and make it active")]
    Add {
        #[arg(value_name = "NAME")]
        name: String,
    },
    #[command(about = "Switch active profile by id or name")]
    Use {
        #[arg(value_name = "PROFILE")]
        profile: String,
    },
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
#[cfg(feature = "diagnostics")]
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
        help = "Value to set. Use @path to read from file; use `config unset` to clear optional fields."
    )]
    value: String,
}

#[derive(Args, Debug, Clone)]
#[command(about = "Clear an optional config field in the pile")]
struct ConfigUnsetArgs {
    #[arg(
        value_enum,
        value_name = "FIELD",
        help = "Optional config field to clear (see possible values below)."
    )]
    field: OptionalConfigField,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
#[value(rename_all = "kebab-case")]
enum ConfigField {
    SystemPrompt,
    Branch,
    BranchId,
    CompassBranchId,
    ExecBranchId,
    LocalMessagesBranchId,
    RelationsBranchId,
    TeamsBranchId,
    WorkspaceBranchId,
    ArchiveBranchId,
    WebBranchId,
    MediaBranchId,
    Author,
    AuthorRole,
    PersonaId,
    PollMs,
    LlmModel,
    LlmBaseUrl,
    LlmApiKey,
    TavilyApiKey,
    ExaApiKey,
    LlmReasoningEffort,
    LlmStream,
    LlmContextWindowTokens,
    LlmMaxOutputTokens,
    LlmPromptSafetyMarginTokens,
    LlmPromptCharsPerToken,
    LlmCompactionProfileId,
    LlmCompactionPrompt,
    LlmCompactionReductionFactor,
    ExecDefaultCwd,
    ExecSandboxProfile,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
#[value(rename_all = "kebab-case")]
enum OptionalConfigField {
    TeamsBranchId,
    PersonaId,
    LlmApiKey,
    TavilyApiKey,
    ExaApiKey,
    LlmReasoningEffort,
    LlmCompactionProfileId,
    LlmCompactionPrompt,
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
        #[cfg(feature = "diagnostics")]
        CommandMode::Diagnostics(args) => {
            let DiagnosticsArgs {
                headless,
                out_dir,
                scale,
                headless_wait_ms,
            } = args;
            let instance = default_instance_name();
            let pile_path = resolve_pile_path(cli.pile.clone(), instance.as_str());
            diagnostics::set_default_pile(Some(pile_path));
            diagnostics::run_diagnostics(headless, out_dir, scale, headless_wait_ms)
                .context("run diagnostics")?;
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
        ConfigCommand::Unset(args) => {
            apply_config_unset(&mut config, args.field)?;
            config.store().context("store config")?;
            print_config(&config, false);
        }
        ConfigCommand::Profile { command } => match command {
            ProfileCommand::List => {
                let profiles = config::list_llm_profiles(config.pile_path.as_path())
                    .context("list profiles")?;
                for profile in profiles {
                    let active = (config.llm_profile_id == Some(profile.id)).then_some("*");
                    let active = active.unwrap_or(" ");
                    println!("{active} {}\t{:x}", profile.name, profile.id);
                }
            }
            ProfileCommand::Add { name } => {
                config.llm_profile_id = Some(*genid());
                config.llm_profile_name = name;
                config.store().context("store config")?;
                print_config(&config, false);
            }
            ProfileCommand::Use { profile } => {
                let profile_id = resolve_profile_selector(&config, profile.as_str())?;
                let Some((llm, name)) =
                    config::load_llm_profile(config.pile_path.as_path(), profile_id)?
                else {
                    return Err(anyhow!("unknown profile {profile_id:x}"));
                };
                config.llm_profile_id = Some(profile_id);
                config.llm_profile_name = name;
                config.llm = llm;
                config.store().context("store config")?;
                print_config(&config, false);
            }
        },
    }
    Ok(())
}

fn resolve_profile_selector(config: &Config, raw: &str) -> Result<Id> {
    if let Ok(id) = parse_hex_id(raw, "profile_id") {
        return Ok(id);
    }
    let needle = raw.trim().to_lowercase();
    let profiles =
        config::list_llm_profiles(config.pile_path.as_path()).context("list profiles")?;
    let mut matches = profiles
        .into_iter()
        .filter(|profile| profile.name.to_lowercase() == needle);
    let Some(first) = matches.next() else {
        return Err(anyhow!("unknown profile '{raw}'"));
    };
    if matches.next().is_some() {
        return Err(anyhow!("profile name '{raw}' is ambiguous; use the hex id"));
    }
    Ok(first.id)
}

fn apply_config_set(config: &mut Config, args: ConfigSetArgs) -> Result<()> {
    match args.field {
        ConfigField::SystemPrompt => {
            config.system_prompt = load_value_or_file(args.value.as_str(), "system_prompt")?;
        }
        ConfigField::Branch => {
            config.branch = load_value_or_file(args.value.as_str(), "branch")?;
        }
        ConfigField::BranchId => {
            config.branch_id = Some(parse_hex_id(args.value.as_str(), "branch_id")?);
        }
        ConfigField::CompassBranchId => {
            config.compass_branch_id =
                Some(parse_hex_id(args.value.as_str(), "compass_branch_id")?);
        }
        ConfigField::ExecBranchId => {
            config.exec_branch_id = Some(parse_hex_id(args.value.as_str(), "exec_branch_id")?);
        }
        ConfigField::LocalMessagesBranchId => {
            config.local_messages_branch_id = Some(parse_hex_id(
                args.value.as_str(),
                "local_messages_branch_id",
            )?);
        }
        ConfigField::RelationsBranchId => {
            config.relations_branch_id =
                Some(parse_hex_id(args.value.as_str(), "relations_branch_id")?);
        }
        ConfigField::TeamsBranchId => {
            config.teams_branch_id = Some(parse_hex_id(args.value.as_str(), "teams_branch_id")?);
        }
        ConfigField::WorkspaceBranchId => {
            config.workspace_branch_id =
                Some(parse_hex_id(args.value.as_str(), "workspace_branch_id")?);
        }
        ConfigField::ArchiveBranchId => {
            config.archive_branch_id =
                Some(parse_hex_id(args.value.as_str(), "archive_branch_id")?);
        }
        ConfigField::WebBranchId => {
            config.web_branch_id = Some(parse_hex_id(args.value.as_str(), "web_branch_id")?);
        }
        ConfigField::MediaBranchId => {
            config.media_branch_id = Some(parse_hex_id(args.value.as_str(), "media_branch_id")?);
        }
        ConfigField::Author => {
            config.author = load_value_or_file(args.value.as_str(), "author")?;
        }
        ConfigField::AuthorRole => {
            config.author_role = load_value_or_file(args.value.as_str(), "author_role")?;
        }
        ConfigField::PersonaId => {
            config.persona_id = Some(parse_hex_id(args.value.as_str(), "persona_id")?);
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
            config.llm.api_key = Some(load_value_or_file_trimmed(
                args.value.as_str(),
                "llm_api_key",
            )?);
        }
        ConfigField::TavilyApiKey => {
            config.tavily_api_key = Some(load_value_or_file_trimmed(
                args.value.as_str(),
                "tavily_api_key",
            )?);
        }
        ConfigField::ExaApiKey => {
            config.exa_api_key = Some(load_value_or_file_trimmed(
                args.value.as_str(),
                "exa_api_key",
            )?);
        }
        ConfigField::LlmReasoningEffort => {
            config.llm.reasoning_effort = Some(load_value_or_file_trimmed(
                args.value.as_str(),
                "llm_reasoning_effort",
            )?);
        }
        ConfigField::LlmStream => {
            config.llm.stream = parse_bool(args.value.as_str(), "llm_stream")?;
        }
        ConfigField::LlmContextWindowTokens => {
            config.llm.context_window_tokens =
                parse_u64(args.value.as_str(), "llm_context_window_tokens")?;
        }
        ConfigField::LlmMaxOutputTokens => {
            config.llm.max_output_tokens = parse_u64(args.value.as_str(), "llm_max_output_tokens")?;
        }
        ConfigField::LlmPromptSafetyMarginTokens => {
            config.llm.prompt_safety_margin_tokens =
                parse_u64(args.value.as_str(), "llm_prompt_safety_margin_tokens")?;
        }
        ConfigField::LlmPromptCharsPerToken => {
            config.llm.prompt_chars_per_token =
                parse_u64(args.value.as_str(), "llm_prompt_chars_per_token")?;
        }
        ConfigField::LlmCompactionProfileId => {
            config.llm_compaction_profile_id = Some(parse_hex_id(
                args.value.as_str(),
                "llm_compaction_profile_id",
            )?);
        }
        ConfigField::LlmCompactionPrompt => {
            config.llm_compaction_prompt = Some(load_value_or_file(
                args.value.as_str(),
                "llm_compaction_prompt",
            )?);
        }
        ConfigField::LlmCompactionReductionFactor => {
            let factor = parse_u64(args.value.as_str(), "llm_compaction_reduction_factor")?;
            if factor < 2 {
                return Err(anyhow!("llm_compaction_reduction_factor must be >= 2"));
            }
            config.llm_compaction_reduction_factor = factor;
        }
        ConfigField::ExecDefaultCwd => {
            let value = load_value_or_file(args.value.as_str(), "exec_default_cwd")?;
            config.exec.default_cwd = Some(PathBuf::from(value.trim()));
        }
        ConfigField::ExecSandboxProfile => {
            config.exec.sandbox_profile =
                Some(parse_hex_id(args.value.as_str(), "exec_sandbox_profile")?);
        }
    }
    Ok(())
}

fn apply_config_unset(config: &mut Config, field: OptionalConfigField) -> Result<()> {
    match field {
        OptionalConfigField::TeamsBranchId => config.teams_branch_id = None,
        OptionalConfigField::PersonaId => config.persona_id = None,
        OptionalConfigField::LlmApiKey => config.llm.api_key = None,
        OptionalConfigField::TavilyApiKey => config.tavily_api_key = None,
        OptionalConfigField::ExaApiKey => config.exa_api_key = None,
        OptionalConfigField::LlmReasoningEffort => config.llm.reasoning_effort = None,
        OptionalConfigField::LlmCompactionProfileId => config.llm_compaction_profile_id = None,
        OptionalConfigField::LlmCompactionPrompt => config.llm_compaction_prompt = None,
        OptionalConfigField::ExecDefaultCwd => config.exec.default_cwd = None,
        OptionalConfigField::ExecSandboxProfile => config.exec.sandbox_profile = None,
    }
    Ok(())
}

fn parse_hex_id(raw: &str, label: &str) -> Result<Id> {
    let raw = raw.trim();
    Id::from_hex(raw).ok_or_else(|| anyhow!("invalid {label} {raw}"))
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

    // Lima's default user is typically the host username; allow overriding for portability.
    let vm_user = env_string("PLAYGROUND_LIMA_USER")
        .or_else(|| env_string("USER"))
        .unwrap_or_else(|| "lima".to_string());
    text = text.replace("__VM_USER__", vm_user.as_str());

    fs::write(out_path, text)
        .with_context(|| format!("write Lima config {}", out_path.display()))?;
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

fn load_value_or_file_trimmed(raw: &str, label: &str) -> Result<String> {
    Ok(load_value_or_file(raw, label)?.trim().to_string())
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

    println!("\n[branches]");
    println!(
        "compass_branch_id = {}",
        config
            .compass_branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "exec_branch_id = {}",
        config
            .exec_branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "local_messages_branch_id = {}",
        config
            .local_messages_branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "relations_branch_id = {}",
        config
            .relations_branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "teams_branch_id = {}",
        config
            .teams_branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "workspace_branch_id = {}",
        config
            .workspace_branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "archive_branch_id = {}",
        config
            .archive_branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "web_branch_id = {}",
        config
            .web_branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "media_branch_id = {}",
        config
            .media_branch_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );

    println!("\n[llm]");
    println!(
        "profile_id = {}",
        config
            .llm_profile_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!("profile_name = \"{}\"", config.llm_profile_name);
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
    println!(
        "context_window_tokens = {}",
        config.llm.context_window_tokens
    );
    println!("max_output_tokens = {}", config.llm.max_output_tokens);
    println!(
        "prompt_safety_margin_tokens = {}",
        config.llm.prompt_safety_margin_tokens
    );
    println!(
        "prompt_chars_per_token = {}",
        config.llm.prompt_chars_per_token
    );
    println!(
        "compaction_profile_id = {}",
        config
            .llm_compaction_profile_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "compaction_prompt = {}",
        config
            .llm_compaction_prompt
            .as_ref()
            .map(|value| format!("\"{}\"", value.replace('\"', "\\\"")))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "compaction_reduction_factor = {}",
        config.llm_compaction_reduction_factor
    );

    println!("\n[integrations]");
    match (&config.tavily_api_key, show_secrets) {
        (Some(key), true) => println!("tavily_api_key = \"{}\"", key),
        (Some(_), false) => println!("tavily_api_key = \"<redacted>\""),
        (None, _) => println!("tavily_api_key = null"),
    }
    match (&config.exa_api_key, show_secrets) {
        (Some(key), true) => println!("exa_api_key = \"{}\"", key),
        (Some(_), false) => println!("exa_api_key = \"<redacted>\""),
        (None, _) => println!("exa_api_key = null"),
    }

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

    let result = (|| -> Result<()> {
        let mut request_info = ensure_llm_request(&mut repo, branch_id, &config)?;

        loop {
            let llm_result =
                wait_for_llm_result(&mut repo, branch_id, request_info.id, config.poll_ms)?;
            if let Some(error) = llm_result.error {
                eprintln!(
                    "warning: llm request {request_id:x} failed: {error}",
                    request_id = request_info.id
                );
                request_info = ensure_llm_request(&mut repo, branch_id, &config)?;
                sleep(Duration::from_millis(config.poll_ms));
                continue;
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
            let command_result_id =
                wait_for_command_result(&mut repo, branch_id, command_request_id, config.poll_ms)?;
            request_info =
                create_thought_and_request(&mut repo, branch_id, Some(command_result_id), &config)?;
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

#[derive(Debug, Clone)]
struct LlmRequestInfo {
    id: Id,
    thought_id: Option<Id>,
}

#[derive(Debug, Clone)]
struct CoreLlmRequest {
    id: Id,
    requested_at: Option<Value<NsTAIInterval>>,
    thought_id: Option<Id>,
}

#[derive(Debug, Clone)]
struct CoreThought {
    id: Id,
    created_at: Option<Value<NsTAIInterval>>,
    prompt: Option<Value<Handle<Blake3, LongString>>>,
}

#[derive(Debug, Clone)]
struct CoreCommandRequest {
    id: Id,
    requested_at: Option<Value<NsTAIInterval>>,
    about_thought: Option<Id>,
    command: Option<Value<Handle<Blake3, LongString>>>,
}

#[derive(Debug, Clone)]
struct LlmResultEntry {
    about_request: Option<Id>,
    finished_at: Option<Value<NsTAIInterval>>,
    attempt: Option<Value<U256BE>>,
    output_text: Option<Value<Handle<Blake3, LongString>>>,
    reasoning_text: Option<Value<Handle<Blake3, LongString>>>,
    error: Option<Value<Handle<Blake3, LongString>>>,
}

#[derive(Default)]
struct CoreIndex {
    llm_requests: HashMap<Id, CoreLlmRequest>,
    llm_done_requests: HashSet<Id>,
    request_for_thought: HashMap<Id, Id>,
    thoughts: HashMap<Id, CoreThought>,
    thought_for_exec_result: HashMap<Id, Id>,
    requested_thoughts: HashSet<Id>,
    llm_results: HashMap<Id, LlmResultEntry>,
    command_requests: HashMap<Id, CoreCommandRequest>,
    command_request_for_thought: HashMap<Id, Id>,
    command_done_requests: HashSet<Id>,
    command_results: HashMap<Id, CommandResultInfo>,
    used_exec_results: HashSet<Id>,
}

fn ensure_llm_request(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    config: &Config,
) -> Result<LlmRequestInfo> {
    let mut cached_head = None;
    let mut cached_catalog = TribleSet::new();
    let mut core_index = CoreIndex::default();
    loop {
        let branch_head = current_branch_head(repo, branch_id)?;
        if branch_head == cached_head {
            sleep(Duration::from_millis(config.poll_ms));
            continue;
        }

        let mut ws = pull_workspace(repo, branch_id, "pull workspace for llm request")?;
        let delta = refresh_cached_checkout(&mut ws, &mut cached_head, &mut cached_catalog)?;
        core_index.apply_delta(&cached_catalog, &delta);

        if let Some(request) = core_index.latest_pending_llm_request() {
            return Ok(request);
        }

        if let Some(thought_id) = core_index.latest_unrequested_thought() {
            let request_id =
                create_request_for_thought_from_index(&mut ws, &core_index, thought_id, config)?;
            push_workspace(repo, &mut ws).context("push llm request")?;
            return Ok(LlmRequestInfo {
                id: request_id,
                thought_id: Some(thought_id),
            });
        }

        if let Some(exec_result) = core_index.latest_unprocessed_exec_result() {
            drop(ws);
            return create_thought_and_request(repo, branch_id, Some(exec_result.id), config);
        }

        if !core_index.has_pending_command_request() {
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
            sleep(Duration::from_millis(config.poll_ms));
            continue;
        }

        sleep(Duration::from_millis(config.poll_ms));
    }
}

fn orient_bootstrap_command(config: &Config) -> String {
    let _ = config;
    "orient show".to_string()
}

fn create_thought_and_request(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    about_exec_result: Option<Id>,
    config: &Config,
) -> Result<LlmRequestInfo> {
    let mut ws = pull_workspace(repo, branch_id, "pull workspace for thought")?;
    let catalog = ws.checkout(..).context("checkout workspace")?;
    let mut core_index = CoreIndex::default();
    core_index.apply_delta(&catalog, &catalog);

    if let Some(exec_result_id) = about_exec_result {
        if let Some(thought_id) = core_index.thought_for_exec_result(exec_result_id) {
            let request_id =
                create_request_for_thought_from_index(&mut ws, &core_index, thought_id, config)?;
            push_workspace(repo, &mut ws).context("push llm request")?;
            return Ok(LlmRequestInfo {
                id: request_id,
                thought_id: Some(thought_id),
            });
        }
    }

    let now = epoch_interval(now_epoch());
    let archive_catalog = load_optional_catalog(
        repo,
        config.archive_branch_id,
        "pull archive workspace for context history",
    )?;
    let relations_catalog = load_optional_catalog(
        repo,
        config.relations_branch_id,
        "pull relations workspace for context history",
    )?;
    let (prompt, compact_change) = if let Some(exec_result_id) = about_exec_result {
        prompt_for_exec_result_with_history(
            &mut ws,
            &core_index,
            &catalog,
            &archive_catalog,
            &relations_catalog,
            exec_result_id,
            config,
        )?
    } else {
        (
            serde_json::to_string(&[ChatMessage::system(config.system_prompt.clone())])
                .context("serialize chat prompt")?,
            TribleSet::new(),
        )
    };
    let prompt_handle = ws.put(prompt);
    let thought_id = ufoid();
    let mut change = TribleSet::new();
    change += compact_change;
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
        llm_chat::kind: llm_chat::kind_request,
        llm_chat::about_thought: *thought_id,
        llm_chat::prompt: prompt_handle,
        llm_chat::requested_at: now,
        llm_chat::model: config.llm.model.as_str(),
    };

    ws.commit(change, None, Some("create thought + llm request"));
    push_workspace(repo, &mut ws).context("push thought + request")?;

    Ok(LlmRequestInfo {
        id: *request_id,
        thought_id: Some(*thought_id),
    })
}

fn create_request_for_thought_from_index(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    thought_id: Id,
    config: &Config,
) -> Result<Id> {
    if let Some(request_id) = core_index.request_for_thought(thought_id) {
        return Ok(request_id);
    }

    let Some(prompt_handle) = core_index.thought_prompt_handle(thought_id) else {
        return Err(anyhow!("thought {thought_id:x} missing prompt"));
    };

    let now = epoch_interval(now_epoch());
    let request_id = ufoid();
    let mut change = TribleSet::new();
    change += entity! { &request_id @
        llm_chat::kind: llm_chat::kind_request,
        llm_chat::about_thought: thought_id,
        llm_chat::prompt: prompt_handle,
        llm_chat::requested_at: now,
        llm_chat::model: config.llm.model.as_str(),
    };
    ws.commit(change, None, Some("create llm request"));
    Ok(*request_id)
}

#[derive(Debug)]
struct LlmResult {
    output_text: String,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct LlmResultInfo {
    output_text: Option<Value<Handle<Blake3, LongString>>>,
    reasoning_text: Option<Value<Handle<Blake3, LongString>>>,
    error: Option<Value<Handle<Blake3, LongString>>>,
}

fn wait_for_llm_result(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    request_id: Id,
    poll_ms: u64,
) -> Result<LlmResult> {
    let mut cached_head = None;
    let mut cached_catalog = TribleSet::new();
    let mut core_index = CoreIndex::default();
    loop {
        let branch_head = current_branch_head(repo, branch_id)?;
        if branch_head == cached_head {
            sleep(Duration::from_millis(poll_ms));
            continue;
        }

        let mut ws = pull_workspace(repo, branch_id, "pull workspace for llm result")?;
        let delta = refresh_cached_checkout(&mut ws, &mut cached_head, &mut cached_catalog)?;
        core_index.apply_delta(&cached_catalog, &delta);
        if !delta_has_llm_result(&cached_catalog, &delta, request_id) {
            sleep(Duration::from_millis(poll_ms));
            continue;
        }
        if let Some(result) = core_index.latest_llm_result(request_id) {
            return load_llm_result(&mut ws, result);
        }
        sleep(Duration::from_millis(poll_ms));
    }
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

impl CoreIndex {
    fn apply_delta(&mut self, updated: &TribleSet, delta: &TribleSet) {
        if delta.is_empty() {
            return;
        }

        for (request_id,) in find!(
            (request_id: Id),
            pattern_changes!(updated, delta, [{
                ?request_id @ llm_chat::kind: llm_chat::kind_request
            }])
        ) {
            self.llm_requests
                .entry(request_id)
                .or_insert(CoreLlmRequest {
                    id: request_id,
                    requested_at: None,
                    thought_id: None,
                });
        }

        for (request_id, requested_at) in find!(
            (request_id: Id, requested_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?request_id @ llm_chat::requested_at: ?requested_at
            }])
        ) {
            if let Some(entry) = self.llm_requests.get_mut(&request_id) {
                entry.requested_at = Some(requested_at);
            }
        }

        for (request_id, thought_id) in find!(
            (request_id: Id, thought_id: Id),
            pattern_changes!(updated, delta, [{
                ?request_id @ llm_chat::about_thought: ?thought_id
            }])
        ) {
            if let Some(entry) = self.llm_requests.get_mut(&request_id) {
                entry.thought_id = Some(thought_id);
            }
            self.request_for_thought.insert(thought_id, request_id);
            self.requested_thoughts.insert(thought_id);
        }

        for (thought_id,) in find!(
            (thought_id: Id),
            pattern_changes!(updated, delta, [{
                ?thought_id @ playground_cog::kind: playground_cog::kind_thought
            }])
        ) {
            self.thoughts.entry(thought_id).or_insert(CoreThought {
                id: thought_id,
                created_at: None,
                prompt: None,
            });
        }

        for (thought_id, created_at) in find!(
            (thought_id: Id, created_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?thought_id @ playground_cog::created_at: ?created_at
            }])
        ) {
            if let Some(entry) = self.thoughts.get_mut(&thought_id) {
                entry.created_at = Some(created_at);
            }
        }

        for (thought_id, prompt) in find!(
            (thought_id: Id, prompt: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?thought_id @ playground_cog::prompt: ?prompt
            }])
        ) {
            if let Some(entry) = self.thoughts.get_mut(&thought_id) {
                entry.prompt = Some(prompt);
            }
        }

        for (thought_id, exec_result_id) in find!(
            (thought_id: Id, exec_result_id: Id),
            pattern_changes!(updated, delta, [{
                ?thought_id @ playground_cog::about_exec_result: ?exec_result_id
            }])
        ) {
            self.thought_for_exec_result
                .insert(exec_result_id, thought_id);
            self.used_exec_results.insert(exec_result_id);
        }

        for (result_id, about_request) in find!(
            (result_id: Id, about_request: Id),
            pattern_changes!(updated, delta, [{
                ?result_id @
                llm_chat::kind: llm_chat::kind_result,
                llm_chat::about_request: ?about_request,
            }])
        ) {
            self.llm_done_requests.insert(about_request);
            let entry = self.llm_results.entry(result_id).or_insert(LlmResultEntry {
                about_request: None,
                finished_at: None,
                attempt: None,
                output_text: None,
                reasoning_text: None,
                error: None,
            });
            entry.about_request = Some(about_request);
        }

        for (result_id, finished_at) in find!(
            (result_id: Id, finished_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?result_id @ llm_chat::finished_at: ?finished_at
            }])
        ) {
            if let Some(entry) = self.llm_results.get_mut(&result_id) {
                entry.finished_at = Some(finished_at);
            }
        }

        for (result_id, attempt) in find!(
            (result_id: Id, attempt: Value<U256BE>),
            pattern_changes!(updated, delta, [{
                ?result_id @ llm_chat::attempt: ?attempt
            }])
        ) {
            if let Some(entry) = self.llm_results.get_mut(&result_id) {
                entry.attempt = Some(attempt);
            }
        }

        for (result_id, output_text) in find!(
            (result_id: Id, output_text: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ llm_chat::output_text: ?output_text
            }])
        ) {
            if let Some(entry) = self.llm_results.get_mut(&result_id) {
                entry.output_text = Some(output_text);
            }
        }

        for (result_id, reasoning_text) in find!(
            (result_id: Id, reasoning_text: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ llm_chat::reasoning_text: ?reasoning_text
            }])
        ) {
            if let Some(entry) = self.llm_results.get_mut(&result_id) {
                entry.reasoning_text = Some(reasoning_text);
            }
        }

        for (result_id, error) in find!(
            (result_id: Id, error: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ llm_chat::error: ?error
            }])
        ) {
            if let Some(entry) = self.llm_results.get_mut(&result_id) {
                entry.error = Some(error);
            }
        }

        for (request_id,) in find!(
            (request_id: Id),
            pattern_changes!(updated, delta, [{
                ?request_id @ playground_exec::kind: playground_exec::kind_command_request
            }])
        ) {
            self.command_requests
                .entry(request_id)
                .or_insert(CoreCommandRequest {
                    id: request_id,
                    requested_at: None,
                    about_thought: None,
                    command: None,
                });
        }

        for (request_id, requested_at) in find!(
            (request_id: Id, requested_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?request_id @ playground_exec::requested_at: ?requested_at
            }])
        ) {
            if let Some(entry) = self.command_requests.get_mut(&request_id) {
                entry.requested_at = Some(requested_at);
            }
        }

        for (request_id, about_thought) in find!(
            (request_id: Id, about_thought: Id),
            pattern_changes!(updated, delta, [{
                ?request_id @ playground_exec::about_thought: ?about_thought
            }])
        ) {
            if let Some(entry) = self.command_requests.get_mut(&request_id) {
                entry.about_thought = Some(about_thought);
            }
            self.command_request_for_thought
                .insert(about_thought, request_id);
        }

        for (request_id, command) in find!(
            (request_id: Id, command: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?request_id @ playground_exec::command_text: ?command
            }])
        ) {
            if let Some(entry) = self.command_requests.get_mut(&request_id) {
                entry.command = Some(command);
            }
        }

        for (result_id, about_request) in find!(
            (result_id: Id, about_request: Id),
            pattern_changes!(updated, delta, [{
                ?result_id @
                playground_exec::kind: playground_exec::kind_command_result,
                playground_exec::about_request: ?about_request,
            }])
        ) {
            self.command_done_requests.insert(about_request);
            self.command_results
                .entry(result_id)
                .or_insert(CommandResultInfo {
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
                });
        }

        for (result_id, finished_at) in find!(
            (result_id: Id, finished_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?result_id @ playground_exec::finished_at: ?finished_at
            }])
        ) {
            if let Some(entry) = self.command_results.get_mut(&result_id) {
                entry.finished_at = Some(finished_at);
            }
        }

        for (result_id, attempt) in find!(
            (result_id: Id, attempt: Value<U256BE>),
            pattern_changes!(updated, delta, [{
                ?result_id @ playground_exec::attempt: ?attempt
            }])
        ) {
            if let Some(entry) = self.command_results.get_mut(&result_id) {
                entry.attempt = Some(attempt);
            }
        }

        for (result_id, stdout) in find!(
            (result_id: Id, stdout: Value<Handle<Blake3, UnknownBlob>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ playground_exec::stdout: ?stdout
            }])
        ) {
            if let Some(entry) = self.command_results.get_mut(&result_id) {
                entry.stdout = Some(stdout);
            }
        }

        for (result_id, stderr) in find!(
            (result_id: Id, stderr: Value<Handle<Blake3, UnknownBlob>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ playground_exec::stderr: ?stderr
            }])
        ) {
            if let Some(entry) = self.command_results.get_mut(&result_id) {
                entry.stderr = Some(stderr);
            }
        }

        for (result_id, stdout_text) in find!(
            (result_id: Id, stdout_text: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ playground_exec::stdout_text: ?stdout_text
            }])
        ) {
            if let Some(entry) = self.command_results.get_mut(&result_id) {
                entry.stdout_text = Some(stdout_text);
            }
        }

        for (result_id, stderr_text) in find!(
            (result_id: Id, stderr_text: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ playground_exec::stderr_text: ?stderr_text
            }])
        ) {
            if let Some(entry) = self.command_results.get_mut(&result_id) {
                entry.stderr_text = Some(stderr_text);
            }
        }

        for (result_id, exit_code) in find!(
            (result_id: Id, exit_code: Value<U256BE>),
            pattern_changes!(updated, delta, [{
                ?result_id @ playground_exec::exit_code: ?exit_code
            }])
        ) {
            if let Some(entry) = self.command_results.get_mut(&result_id) {
                entry.exit_code = Some(exit_code);
            }
        }

        for (result_id, error) in find!(
            (result_id: Id, error: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ playground_exec::error: ?error
            }])
        ) {
            if let Some(entry) = self.command_results.get_mut(&result_id) {
                entry.error = Some(error);
            }
        }
    }

    fn latest_pending_llm_request(&self) -> Option<LlmRequestInfo> {
        let mut candidates: Vec<CoreLlmRequest> = self
            .llm_requests
            .values()
            .filter(|request| !self.llm_done_requests.contains(&request.id))
            .cloned()
            .collect();
        candidates.sort_by_key(|request| {
            (
                request.requested_at.map(interval_key).unwrap_or(i128::MIN),
                request.id,
            )
        });
        candidates.pop().map(|request| LlmRequestInfo {
            id: request.id,
            thought_id: request.thought_id,
        })
    }

    fn latest_unrequested_thought(&self) -> Option<Id> {
        let mut candidates: Vec<CoreThought> = self
            .thoughts
            .values()
            .filter(|thought| !self.requested_thoughts.contains(&thought.id))
            .cloned()
            .collect();
        candidates.sort_by_key(|thought| (thought.created_at.map(interval_key), thought.id));
        candidates.pop().map(|thought| thought.id)
    }

    fn request_for_thought(&self, thought_id: Id) -> Option<Id> {
        self.request_for_thought.get(&thought_id).copied()
    }

    fn thought_for_exec_result(&self, exec_result_id: Id) -> Option<Id> {
        self.thought_for_exec_result.get(&exec_result_id).copied()
    }

    fn thought_prompt_handle(&self, thought_id: Id) -> Option<Value<Handle<Blake3, LongString>>> {
        self.thoughts
            .get(&thought_id)
            .and_then(|thought| thought.prompt)
    }

    fn latest_llm_result(&self, request_id: Id) -> Option<LlmResultInfo> {
        self.llm_results
            .values()
            .filter(|result| result.about_request == Some(request_id))
            .max_by_key(|result| llm_result_rank(result.attempt, result.finished_at))
            .map(|result| LlmResultInfo {
                output_text: result.output_text,
                reasoning_text: result.reasoning_text,
                error: result.error,
            })
    }

    fn has_pending_command_request(&self) -> bool {
        self.command_requests
            .values()
            .any(|request| !self.command_done_requests.contains(&request.id))
    }

    fn command_request_command_handle(
        &self,
        request_id: Id,
    ) -> Option<Value<Handle<Blake3, LongString>>> {
        self.command_requests
            .get(&request_id)
            .and_then(|request| request.command)
    }

    fn command_request_for_thought(&self, thought_id: Id) -> Option<Id> {
        self.command_request_for_thought.get(&thought_id).copied()
    }

    fn thought_for_command_request(&self, command_request_id: Id) -> Option<Id> {
        self.command_requests
            .get(&command_request_id)
            .and_then(|request| request.about_thought)
    }

    fn latest_command_result(&self, request_id: Id) -> Option<CommandResultInfo> {
        self.command_results
            .values()
            .filter(|result| result.about_request == request_id)
            .cloned()
            .max_by_key(command_result_rank)
    }

    fn latest_unprocessed_exec_result(&self) -> Option<CommandResultInfo> {
        self.command_results
            .values()
            .filter(|result| !self.used_exec_results.contains(&result.id))
            .cloned()
            .max_by_key(|result| result.finished_at.map(interval_key).unwrap_or(i128::MIN))
    }
}

fn llm_result_rank(
    attempt: Option<Value<U256BE>>,
    finished_at: Option<Value<NsTAIInterval>>,
) -> (u64, i128) {
    (
        attempt.and_then(u256be_to_u64).unwrap_or_default(),
        finished_at.map(interval_key).unwrap_or(i128::MIN),
    )
}

fn command_result_rank(result: &CommandResultInfo) -> (u64, i128) {
    (
        result.attempt.and_then(u256be_to_u64).unwrap_or_default(),
        result.finished_at.map(interval_key).unwrap_or(i128::MIN),
    )
}

fn ensure_command_request(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    command: &str,
    thought_id: Option<Id>,
    default_cwd: Option<&str>,
    sandbox_profile: Option<Id>,
) -> Result<Id> {
    let mut ws = pull_workspace(repo, branch_id, "pull workspace for command request")?;
    let mut cached_head = None;
    let mut cached_catalog = TribleSet::new();
    let mut core_index = CoreIndex::default();
    let delta = refresh_cached_checkout(&mut ws, &mut cached_head, &mut cached_catalog)?;
    core_index.apply_delta(&cached_catalog, &delta);

    if let Some(thought_id) = thought_id {
        if let Some(existing) = core_index.command_request_for_thought(thought_id) {
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
    Ok(*request_id)
}

fn wait_for_command_result(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    request_id: Id,
    poll_ms: u64,
) -> Result<Id> {
    let mut cached_head = None;
    let mut cached_catalog = TribleSet::new();
    let mut core_index = CoreIndex::default();
    loop {
        let branch_head = current_branch_head(repo, branch_id)?;
        if branch_head == cached_head {
            sleep(Duration::from_millis(poll_ms));
            continue;
        }

        let mut ws = pull_workspace(repo, branch_id, "pull workspace for command result")?;
        let delta = refresh_cached_checkout(&mut ws, &mut cached_head, &mut cached_catalog)?;
        core_index.apply_delta(&cached_catalog, &delta);
        if !delta_has_command_result(&cached_catalog, &delta, request_id) {
            sleep(Duration::from_millis(poll_ms));
            continue;
        }
        if let Some(result) = core_index.latest_command_result(request_id) {
            return Ok(result.id);
        }
        sleep(Duration::from_millis(poll_ms));
    }
}

fn delta_has_llm_result(updated: &TribleSet, delta: &TribleSet, request_id: Id) -> bool {
    find!(
        (about_request: Id),
        pattern_changes!(updated, delta, [{
            _?event @
            llm_chat::kind: llm_chat::kind_result,
            llm_chat::about_request: ?about_request,
        }])
    )
    .into_iter()
    .any(|(about_request,)| about_request == request_id)
}

fn delta_has_command_result(updated: &TribleSet, delta: &TribleSet, request_id: Id) -> bool {
    find!(
        (about_request: Id),
        pattern_changes!(updated, delta, [{
            _?event @
            playground_exec::kind: playground_exec::kind_command_result,
            playground_exec::about_request: ?about_request,
        }])
    )
    .into_iter()
    .any(|(about_request,)| about_request == request_id)
}

fn load_optional_catalog(
    repo: &mut Repository<Pile>,
    branch_id: Option<Id>,
    context: &str,
) -> Result<TribleSet> {
    let Some(branch_id) = branch_id else {
        return Ok(TribleSet::new());
    };
    let mut ws = pull_workspace(repo, branch_id, context)?;
    ws.checkout(..).context("checkout optional branch")
}

#[derive(Debug, Clone)]
struct ContextChunk {
    id: Id,
    level: u64,
    summary: Value<Handle<Blake3, LongString>>,
    start_at: Value<NsTAIInterval>,
    end_at: Value<NsTAIInterval>,
    left: Option<Id>,
    right: Option<Id>,
    about_exec_result: Option<Id>,
    about_archive_message: Option<Id>,
    archive_author: Option<Id>,
    archive_person: Option<Id>,
    archive_thread_root: Option<Id>,
    archive_conversation: Option<Value<Handle<Blake3, LongString>>>,
    archive_source_format: Option<String>,
}

#[derive(Default)]
struct ContextChunkIndex {
    chunks: HashMap<Id, ContextChunk>,
    // The LSM frontier: one "root" chunk per level (best-effort; if multiple exist, keep the
    // newest by end_at as the active chunk for merging).
    root_by_level: HashMap<u64, Id>,
    // Leaf chunks tie a single exec result to a compacted chunk.
    chunk_for_exec_result: HashMap<Id, Id>,
    // Leaf chunks tie a single imported archive message to a compacted chunk.
    chunk_for_archive_message: HashMap<Id, Id>,
}

#[derive(Debug, Clone)]
struct ArchiveMessageInfo {
    id: Id,
    author_id: Id,
    author_name: Option<Value<Handle<Blake3, LongString>>>,
    content: Value<Handle<Blake3, LongString>>,
    created_at: Value<NsTAIInterval>,
    thread_root_id: Id,
    conversation_id: Option<Value<Handle<Blake3, LongString>>>,
    source_format: Option<String>,
    source_message_id: Option<Value<Handle<Blake3, LongString>>>,
    source_author: Option<Value<Handle<Blake3, LongString>>>,
    source_role: Option<Value<Handle<Blake3, LongString>>>,
}

#[derive(Default)]
struct RelationsIndex {
    key_to_person_id: HashMap<String, Id>,
    person_label: HashMap<Id, String>,
}

fn prompt_for_exec_result_with_history(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    catalog: &TribleSet,
    archive_catalog: &TribleSet,
    relations_catalog: &TribleSet,
    exec_result_id: Id,
    config: &Config,
) -> Result<(String, TribleSet)> {
    let (mut messages, compact_change) = build_prompt_messages_with_compaction(
        ws,
        core_index,
        catalog,
        archive_catalog,
        relations_catalog,
        exec_result_id,
        config,
    )?;
    // Store the system prompt as an actual system role message instead of concatenating into a
    // monolithic user prompt string.
    messages.insert(0, ChatMessage::system(config.system_prompt.clone()));
    let prompt_json = serde_json::to_string(&messages).context("serialize chat prompt")?;
    Ok((prompt_json, compact_change))
}

fn build_prompt_messages_with_compaction(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    catalog: &TribleSet,
    archive_catalog: &TribleSet,
    relations_catalog: &TribleSet,
    exec_result_id: Id,
    config: &Config,
) -> Result<(Vec<ChatMessage>, TribleSet)> {
    let mut index = load_context_chunks(catalog);
    let body_budget_chars = prompt_body_budget_chars(config);
    let semantic_compactor = SemanticCompactor::new(config)?;

    // Sort all command results in chronological order (oldest -> newest).
    let mut results: Vec<CommandResultInfo> =
        core_index.command_results.values().cloned().collect();
    results.sort_by_key(|result| result.finished_at.map(interval_key).unwrap_or(i128::MIN));
    results.retain(|result| result.finished_at.is_some());

    let Some(current_pos) = results
        .iter()
        .position(|result| result.id == exec_result_id)
    else {
        return Err(anyhow!("exec result {exec_result_id:x} missing from index"));
    };
    let current_finished_at = results[current_pos]
        .finished_at
        .context("exec result missing finished_at")?;
    let current_finished_key = interval_key(current_finished_at);
    let results = results[..=current_pos].to_vec();
    let relations = load_relations_index(ws, relations_catalog)?;
    let archive_messages = load_archive_messages(archive_catalog);

    let mut compact_change = TribleSet::new();

    // Archive leaves are always represented as memory chunks (never as raw shell turns).
    for message in archive_messages
        .iter()
        .filter(|message| interval_key(message.created_at) <= current_finished_key)
    {
        if index.chunk_for_archive_message.contains_key(&message.id) {
            continue;
        }
        let author_name = load_optional_text(ws, message.author_name)?;
        let source_author = load_optional_text(ws, message.source_author)?;
        let source_role = load_optional_text(ws, message.source_role)?;
        let source_message_id = load_optional_text(ws, message.source_message_id)?;
        let conversation_id = load_optional_text(ws, message.conversation_id)?;
        let content = load_text(ws, message.content).context("load archive message content")?;
        let resolved_person =
            resolve_archive_person(&relations, author_name.as_deref(), source_author.as_deref());
        let leaf_summary = format_archive_output(
            message,
            author_name.as_deref(),
            source_author.as_deref(),
            source_role.as_deref(),
            source_message_id.as_deref(),
            conversation_id.as_deref(),
            content.as_str(),
            resolved_person
                .and_then(|person_id| relations.person_label.get(&person_id).map(|s| s.as_str())),
            resolved_person,
        );
        let leaf_summary_handle = ws.put(leaf_summary);
        let now = epoch_interval(now_epoch());
        let chunk_id = ufoid();

        compact_change += entity! { &chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::level: 0u64,
            playground_context::summary: leaf_summary_handle,
            playground_context::created_at: now,
            playground_context::start_at: message.created_at,
            playground_context::end_at: message.created_at,
            playground_context::about_archive_message: message.id,
            playground_context::archive_author: message.author_id,
            playground_context::archive_thread_root: message.thread_root_id,
        };
        if let Some(person_id) = resolved_person {
            compact_change += entity! { &chunk_id @
                playground_context::archive_person: person_id,
            };
        }
        if let Some(conversation_id) = message.conversation_id {
            compact_change += entity! { &chunk_id @
                playground_context::archive_conversation: conversation_id,
            };
        }
        if let Some(source_format) = message.source_format.as_deref() {
            compact_change += entity! { &chunk_id @
                playground_context::archive_source_format: source_format,
            };
        }

        let chunk = ContextChunk {
            id: *chunk_id,
            level: 0,
            summary: leaf_summary_handle,
            start_at: message.created_at,
            end_at: message.created_at,
            left: None,
            right: None,
            about_exec_result: None,
            about_archive_message: Some(message.id),
            archive_author: Some(message.author_id),
            archive_person: resolved_person,
            archive_thread_root: Some(message.thread_root_id),
            archive_conversation: message.conversation_id,
            archive_source_format: message.source_format.clone(),
        };
        index.chunk_for_archive_message.insert(message.id, chunk.id);
        insert_chunk_with_carry(
            ws,
            &mut index,
            &mut compact_change,
            chunk,
            &semantic_compactor,
        )?;
    }

    // Exec leaves are represented as memory chunks too; prompt selection is a stateless cover
    // over the chunk tree (not a separate "recent raw turns" bucket).
    for result in &results {
        if index.chunk_for_exec_result.contains_key(&result.id) {
            continue;
        }
        let finished_at = result
            .finished_at
            .context("command result missing finished_at")?;
        let command = load_command_for_result(ws, core_index, result)?;
        let reasoning_text = load_reasoning_for_exec_result(ws, core_index, result)?;
        let exec_output = load_exec_result(ws, result.clone())?;
        let leaf_summary =
            format_exec_output(command.as_str(), exec_output, reasoning_text.as_deref());
        let leaf_summary_handle = ws.put(leaf_summary);
        let now = epoch_interval(now_epoch());
        let chunk_id = ufoid();

        compact_change += entity! { &chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::level: 0u64,
            playground_context::summary: leaf_summary_handle,
            playground_context::created_at: now,
            playground_context::start_at: finished_at,
            playground_context::end_at: finished_at,
            playground_context::about_exec_result: result.id,
        };

        let chunk = ContextChunk {
            id: *chunk_id,
            level: 0,
            summary: leaf_summary_handle,
            start_at: finished_at,
            end_at: finished_at,
            left: None,
            right: None,
            about_exec_result: Some(result.id),
            about_archive_message: None,
            archive_author: None,
            archive_person: None,
            archive_thread_root: None,
            archive_conversation: None,
            archive_source_format: None,
        };
        index.chunk_for_exec_result.insert(result.id, chunk.id);
        insert_chunk_with_carry(
            ws,
            &mut index,
            &mut compact_change,
            chunk,
            &semantic_compactor,
        )?;
    }

    let (messages, _used_chars) = build_memory_cover_messages(ws, &index, body_budget_chars)?;
    Ok((messages, compact_change))
}

fn prompt_body_budget_chars(config: &Config) -> usize {
    // This is an intentionally cheap heuristic: we approximate tokens->chars and reserve space
    // for model output plus a small safety margin.
    let reserved = config
        .llm
        .max_output_tokens
        .saturating_add(config.llm.prompt_safety_margin_tokens);
    let input_tokens = config.llm.context_window_tokens.saturating_sub(reserved);
    let chars_per_token = config.llm.prompt_chars_per_token.max(1);

    let input_chars = u128_to_usize_saturating((input_tokens as u128) * (chars_per_token as u128));
    let system_chars = config.system_prompt.chars().count();
    input_chars.saturating_sub(system_chars)
}

fn u128_to_usize_saturating(value: u128) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

#[derive(Debug, Clone)]
struct MemoryCoverTurn {
    command: String,
    output: String,
    cost: usize,
}

#[derive(Debug, Clone, Copy)]
struct SplitCandidate {
    index: usize,
    parent_id: Id,
    left_id: Id,
    right_id: Id,
    extra_cost: usize,
    recency_key: i128,
}

fn build_memory_cover_messages(
    ws: &mut Workspace<Pile>,
    index: &ContextChunkIndex,
    budget_chars: usize,
) -> Result<(Vec<ChatMessage>, usize)> {
    if budget_chars == 0 {
        return Ok((Vec::new(), 0));
    }

    let mut seen_roots = HashSet::new();
    let mut cover: Vec<Id> = index
        .root_by_level
        .values()
        .copied()
        .filter(|id| seen_roots.insert(*id))
        .filter(|id| index.chunks.contains_key(id))
        .collect();
    cover.sort_by_key(|id| {
        index
            .chunks
            .get(id)
            .map(|chunk| interval_key(chunk.start_at))
            .unwrap_or(i128::MAX)
    });
    if cover.is_empty() {
        return Ok((Vec::new(), 0));
    }

    let mut turn_cache: HashMap<Id, MemoryCoverTurn> = HashMap::new();
    let mut used = 0usize;
    for chunk_id in &cover {
        let turn = memory_cover_turn(ws, index, &mut turn_cache, *chunk_id)?;
        used = used.saturating_add(turn.cost);
    }

    // If even the coarsest antichain exceeds budget, drop the oldest roots until the selected
    // cover fits.
    while used > budget_chars && !cover.is_empty() {
        let removed = cover.remove(0);
        let turn = memory_cover_turn(ws, index, &mut turn_cache, removed)?;
        used = used.saturating_sub(turn.cost);
    }
    if cover.is_empty() {
        return Ok((Vec::new(), 0));
    }

    loop {
        let remaining = budget_chars.saturating_sub(used);
        if remaining == 0 {
            break;
        }

        let mut best: Option<SplitCandidate> = None;
        for (cover_index, parent_id) in cover.iter().enumerate() {
            let Some(parent_chunk) = index.chunks.get(parent_id) else {
                continue;
            };
            let (Some(left_id), Some(right_id)) = (parent_chunk.left, parent_chunk.right) else {
                continue;
            };

            let parent_turn = memory_cover_turn(ws, index, &mut turn_cache, *parent_id)?;
            let left_turn = memory_cover_turn(ws, index, &mut turn_cache, left_id)?;
            let right_turn = memory_cover_turn(ws, index, &mut turn_cache, right_id)?;
            let children_cost = left_turn.cost.saturating_add(right_turn.cost);
            let extra_cost = children_cost.saturating_sub(parent_turn.cost);
            if extra_cost > remaining {
                continue;
            }

            let candidate = SplitCandidate {
                index: cover_index,
                parent_id: *parent_id,
                left_id,
                right_id,
                extra_cost,
                recency_key: interval_key(parent_chunk.end_at),
            };
            if is_better_split_candidate(candidate, best) {
                best = Some(candidate);
            }
        }

        let Some(candidate) = best else {
            break;
        };

        cover.splice(
            candidate.index..=candidate.index,
            [candidate.left_id, candidate.right_id],
        );
        used = used.saturating_add(candidate.extra_cost);
    }

    let mut messages = Vec::new();
    for chunk_id in cover {
        let turn = memory_cover_turn(ws, index, &mut turn_cache, chunk_id)?;
        messages.push(ChatMessage::assistant(turn.command.clone()));
        messages.push(ChatMessage::user(turn.output.clone()));
    }

    Ok((messages, used))
}

fn memory_cover_turn(
    ws: &mut Workspace<Pile>,
    index: &ContextChunkIndex,
    turn_cache: &mut HashMap<Id, MemoryCoverTurn>,
    chunk_id: Id,
) -> Result<MemoryCoverTurn> {
    if let Some(turn) = turn_cache.get(&chunk_id) {
        return Ok(turn.clone());
    }

    let chunk = index
        .chunks
        .get(&chunk_id)
        .with_context(|| format!("missing context chunk {:x}", chunk_id))?;
    let command = format!("memory {id:x}", id = chunk.id);
    let output = format_memory_output(ws, chunk)?;
    let cost = command
        .chars()
        .count()
        .saturating_add(output.chars().count());
    let turn = MemoryCoverTurn {
        command,
        output,
        cost,
    };
    turn_cache.insert(chunk_id, turn.clone());
    Ok(turn)
}

fn is_better_split_candidate(candidate: SplitCandidate, current: Option<SplitCandidate>) -> bool {
    let Some(current) = current else {
        return true;
    };
    if candidate.recency_key != current.recency_key {
        return candidate.recency_key > current.recency_key;
    }
    if candidate.extra_cost != current.extra_cost {
        return candidate.extra_cost > current.extra_cost;
    }
    if candidate.index != current.index {
        return candidate.index > current.index;
    }
    candidate.parent_id < current.parent_id
}

fn format_memory_output(ws: &mut Workspace<Pile>, chunk: &ContextChunk) -> Result<String> {
    let mut header = format!("mem {} lvl={}", id_prefix(chunk.id), chunk.level);
    if let Some(exec_id) = chunk.about_exec_result {
        header.push_str(&format!(" exec={}", id_prefix(exec_id)));
    }
    if let Some(message_id) = chunk.about_archive_message {
        header.push_str(&format!(" archive_msg={}", id_prefix(message_id)));
    }
    if let Some(author_id) = chunk.archive_author {
        header.push_str(&format!(" archive_author={}", id_prefix(author_id)));
    }
    if let Some(person_id) = chunk.archive_person {
        header.push_str(&format!(" person={}", id_prefix(person_id)));
    }
    if let Some(thread_root_id) = chunk.archive_thread_root {
        header.push_str(&format!(" thread={}", id_prefix(thread_root_id)));
    }
    if let (Some(left), Some(right)) = (chunk.left, chunk.right) {
        header.push_str(&format!(
            " children={} {}",
            id_prefix(left),
            id_prefix(right)
        ));
    }
    if let Some(source_format) = chunk.archive_source_format.as_deref() {
        header.push_str(&format!(" source={source_format}"));
    }

    let summary = load_text(ws, chunk.summary).context("load compacted history chunk")?;
    if let Some(conversation) = chunk.archive_conversation {
        let conversation_id =
            load_text(ws, conversation).context("load archive conversation id")?;
        return Ok(format!(
            "{header}\nconversation: {conversation_id}\n{}\n",
            summary.trim_end()
        ));
    }
    Ok(format!("{header}\n{}\n", summary.trim_end()))
}

fn id_prefix(id: Id) -> String {
    let raw: [u8; 16] = id.into();
    let mut out = String::with_capacity(8);
    for byte in raw.iter().take(4) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn load_command_for_result(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    exec_result: &CommandResultInfo,
) -> Result<String> {
    let Some(command_handle) = core_index.command_request_command_handle(exec_result.about_request)
    else {
        return Err(anyhow!(
            "command request {id:x} missing command text",
            id = exec_result.about_request
        ));
    };
    load_text(ws, command_handle).context("load command for exec result")
}

fn load_reasoning_for_exec_result(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    exec_result: &CommandResultInfo,
) -> Result<Option<String>> {
    let Some(thought_id) = core_index.thought_for_command_request(exec_result.about_request) else {
        return Ok(None);
    };
    let Some(request_id) = core_index.request_for_thought(thought_id) else {
        return Ok(None);
    };
    let Some(llm_result) = core_index.latest_llm_result(request_id) else {
        return Ok(None);
    };
    let Some(reasoning_handle) = llm_result.reasoning_text else {
        return Ok(None);
    };
    let reasoning_text = load_text(ws, reasoning_handle).context("load reasoning text")?;
    if reasoning_text.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some(reasoning_text))
}

fn load_archive_messages(catalog: &TribleSet) -> Vec<ArchiveMessageInfo> {
    let mut reply_to: HashMap<Id, Id> = HashMap::new();
    for (message_id, parent_id) in find!(
        (message_id: Id, parent_id: Id),
        pattern!(catalog, [{
            ?message_id @ playground_archive::reply_to: ?parent_id,
        }])
    ) {
        reply_to.insert(message_id, parent_id);
    }

    let mut author_name_by_author: HashMap<Id, Value<Handle<Blake3, LongString>>> = HashMap::new();
    for (author_id, author_name) in find!(
        (author_id: Id, author_name: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?author_id @ playground_archive::author_name: ?author_name,
        }])
    ) {
        author_name_by_author.insert(author_id, author_name);
    }

    let mut batch_by_message: HashMap<Id, Id> = HashMap::new();
    for (message_id, batch_id) in find!(
        (message_id: Id, batch_id: Id),
        pattern!(catalog, [{
            ?message_id @ playground_archive_import::batch: ?batch_id,
        }])
    ) {
        batch_by_message.insert(message_id, batch_id);
    }

    let mut source_format_by_batch: HashMap<Id, String> = HashMap::new();
    for (batch_id, source_format) in find!(
        (batch_id: Id, source_format: String),
        pattern!(catalog, [{
            ?batch_id @ playground_archive_import::source_format: ?source_format,
        }])
    ) {
        source_format_by_batch.insert(batch_id, source_format);
    }

    let mut conversation_by_batch: HashMap<Id, Value<Handle<Blake3, LongString>>> = HashMap::new();
    for (batch_id, conversation_id) in find!(
        (batch_id: Id, conversation_id: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?batch_id @ playground_archive_import::source_conversation_id: ?conversation_id,
        }])
    ) {
        conversation_by_batch.insert(batch_id, conversation_id);
    }

    let mut source_message_id_by_message: HashMap<Id, Value<Handle<Blake3, LongString>>> =
        HashMap::new();
    for (message_id, source_message_id) in find!(
        (message_id: Id, source_message_id: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?message_id @ playground_archive_import::source_message_id: ?source_message_id,
        }])
    ) {
        source_message_id_by_message.insert(message_id, source_message_id);
    }

    let mut source_author_by_message: HashMap<Id, Value<Handle<Blake3, LongString>>> =
        HashMap::new();
    for (message_id, source_author) in find!(
        (message_id: Id, source_author: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?message_id @ playground_archive_import::source_author: ?source_author,
        }])
    ) {
        source_author_by_message.insert(message_id, source_author);
    }

    let mut source_role_by_message: HashMap<Id, Value<Handle<Blake3, LongString>>> = HashMap::new();
    for (message_id, source_role) in find!(
        (message_id: Id, source_role: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?message_id @ playground_archive_import::source_role: ?source_role,
        }])
    ) {
        source_role_by_message.insert(message_id, source_role);
    }

    let mut messages = Vec::new();
    for (message_id, author_id, content, created_at) in find!(
        (
            message_id: Id,
            author_id: Id,
            content: Value<Handle<Blake3, LongString>>,
            created_at: Value<NsTAIInterval>
        ),
        pattern!(catalog, [{
            ?message_id @
                playground_archive::kind: playground_archive::kind_message,
                playground_archive::author: ?author_id,
                playground_archive::content: ?content,
                playground_archive::created_at: ?created_at,
        }])
    ) {
        let thread_root_id = archive_thread_root(message_id, &reply_to);
        let batch_id = batch_by_message.get(&message_id).copied();
        let conversation_id = batch_id.and_then(|id| conversation_by_batch.get(&id).copied());
        let source_format = batch_id.and_then(|id| source_format_by_batch.get(&id).cloned());
        messages.push(ArchiveMessageInfo {
            id: message_id,
            author_id,
            author_name: author_name_by_author.get(&author_id).copied(),
            content,
            created_at,
            thread_root_id,
            conversation_id,
            source_format,
            source_message_id: source_message_id_by_message.get(&message_id).copied(),
            source_author: source_author_by_message.get(&message_id).copied(),
            source_role: source_role_by_message.get(&message_id).copied(),
        });
    }

    messages.sort_by_key(|message| (interval_key(message.created_at), message.id));
    messages
}

fn archive_thread_root(message_id: Id, reply_to: &HashMap<Id, Id>) -> Id {
    let mut cursor = message_id;
    let mut seen = HashSet::new();
    while let Some(parent) = reply_to.get(&cursor).copied() {
        if !seen.insert(cursor) {
            break;
        }
        cursor = parent;
    }
    cursor
}

fn load_relations_index(
    ws: &mut Workspace<Pile>,
    relations_catalog: &TribleSet,
) -> Result<RelationsIndex> {
    let mut people = HashSet::new();
    for (person_id,) in find!(
        (person_id: Id),
        pattern!(relations_catalog, [{
            ?person_id @ metadata::tag: playground_relations::kind_person,
        }])
    ) {
        people.insert(person_id);
    }

    let mut alias_map: HashMap<Id, Vec<String>> = HashMap::new();
    for (person_id, alias) in find!(
        (person_id: Id, alias: String),
        pattern!(relations_catalog, [{
            ?person_id @ playground_relations::alias: ?alias,
        }])
    ) {
        alias_map.entry(person_id).or_default().push(alias);
    }

    let mut display_name_map: HashMap<Id, Value<Handle<Blake3, LongString>>> = HashMap::new();
    for (person_id, handle) in find!(
        (person_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(relations_catalog, [{
            ?person_id @ playground_relations::display_name: ?handle,
        }])
    ) {
        display_name_map.insert(person_id, handle);
    }

    let mut first_name_map: HashMap<Id, Value<Handle<Blake3, LongString>>> = HashMap::new();
    for (person_id, handle) in find!(
        (person_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(relations_catalog, [{
            ?person_id @ playground_relations::first_name: ?handle,
        }])
    ) {
        first_name_map.insert(person_id, handle);
    }

    let mut last_name_map: HashMap<Id, Value<Handle<Blake3, LongString>>> = HashMap::new();
    for (person_id, handle) in find!(
        (person_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(relations_catalog, [{
            ?person_id @ playground_relations::last_name: ?handle,
        }])
    ) {
        last_name_map.insert(person_id, handle);
    }

    let mut metadata_name_map: HashMap<Id, Value<Handle<Blake3, LongString>>> = HashMap::new();
    for (person_id, handle) in find!(
        (person_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(relations_catalog, [{
            ?person_id @ metadata::name: ?handle,
        }])
    ) {
        metadata_name_map.insert(person_id, handle);
    }

    let mut key_candidates: HashMap<String, HashSet<Id>> = HashMap::new();
    let mut labels = HashMap::new();
    for person_id in people {
        let metadata_name = metadata_name_map
            .get(&person_id)
            .copied()
            .map(|handle| load_text(ws, handle))
            .transpose()?;
        let display_name = display_name_map
            .get(&person_id)
            .copied()
            .map(|handle| load_text(ws, handle))
            .transpose()?;
        let first_name = first_name_map
            .get(&person_id)
            .copied()
            .map(|handle| load_text(ws, handle))
            .transpose()?;
        let last_name = last_name_map
            .get(&person_id)
            .copied()
            .map(|handle| load_text(ws, handle))
            .transpose()?;

        let mut keys = Vec::new();
        if let Some(name) = metadata_name.as_deref() {
            keys.push(name.to_string());
        }
        if let Some(display_name) = display_name.as_deref() {
            keys.push(display_name.to_string());
        }
        if let (Some(first_name), Some(last_name)) = (first_name.as_deref(), last_name.as_deref()) {
            keys.push(format!("{first_name} {last_name}"));
        }
        if let Some(aliases) = alias_map.get(&person_id) {
            keys.extend(aliases.iter().cloned());
        }

        for key in keys {
            let normalized = normalize_person_key(key.as_str());
            if normalized.is_empty() {
                continue;
            }
            key_candidates
                .entry(normalized)
                .or_default()
                .insert(person_id);
        }

        let label = display_name
            .or(metadata_name)
            .or_else(|| {
                if let (Some(first_name), Some(last_name)) = (first_name, last_name) {
                    Some(format!("{first_name} {last_name}"))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| id_prefix(person_id));
        labels.insert(person_id, label);
    }

    let mut key_to_person_id = HashMap::new();
    for (key, candidates) in key_candidates {
        if candidates.len() != 1 {
            continue;
        }
        if let Some(person_id) = candidates.into_iter().next() {
            key_to_person_id.insert(key, person_id);
        }
    }

    Ok(RelationsIndex {
        key_to_person_id,
        person_label: labels,
    })
}

fn normalize_person_key(raw: &str) -> String {
    raw.split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim()
        .to_lowercase()
}

fn resolve_archive_person(
    relations: &RelationsIndex,
    author_name: Option<&str>,
    source_author: Option<&str>,
) -> Option<Id> {
    let mut candidates = HashSet::new();
    for key in [author_name, source_author].into_iter().flatten() {
        let normalized = normalize_person_key(key);
        if normalized.is_empty() {
            continue;
        }
        if let Some(person_id) = relations.key_to_person_id.get(normalized.as_str()) {
            candidates.insert(*person_id);
        }
    }
    if candidates.len() == 1 {
        return candidates.into_iter().next();
    }
    None
}

fn load_optional_text(
    ws: &mut Workspace<Pile>,
    handle: Option<Value<Handle<Blake3, LongString>>>,
) -> Result<Option<String>> {
    handle.map(|handle| load_text(ws, handle)).transpose()
}

fn format_archive_output(
    message: &ArchiveMessageInfo,
    author_name: Option<&str>,
    source_author: Option<&str>,
    source_role: Option<&str>,
    source_message_id: Option<&str>,
    conversation_id: Option<&str>,
    content: &str,
    person_label: Option<&str>,
    person_id: Option<Id>,
) -> String {
    let mut text = String::new();
    append_section(&mut text, "source", "archive");
    append_section(
        &mut text,
        "archive_message_id",
        format!("{:x}", message.id).as_str(),
    );
    append_section(
        &mut text,
        "archive_author_id",
        format!("{:x}", message.author_id).as_str(),
    );
    append_section(
        &mut text,
        "archive_thread_root_id",
        format!("{:x}", message.thread_root_id).as_str(),
    );
    if let Some(source_format) = message.source_format.as_deref() {
        append_section(&mut text, "archive_source_format", source_format);
    }
    if let Some(conversation_id) = conversation_id {
        append_section(&mut text, "archive_conversation_id", conversation_id);
    }
    if let Some(source_message_id) = source_message_id {
        append_section(&mut text, "source_message_id", source_message_id);
    }
    if let Some(author_name) = author_name {
        append_section(&mut text, "author_name", author_name);
    }
    if let Some(source_author) = source_author {
        append_section(&mut text, "source_author", source_author);
    }
    if let Some(source_role) = source_role {
        append_section(&mut text, "source_role", source_role);
    }
    if let Some(person_id) = person_id {
        append_section(&mut text, "person_id", format!("{person_id:x}").as_str());
    }
    if let Some(person_label) = person_label {
        append_section(&mut text, "person_label", person_label);
    }
    append_section(&mut text, "message", content);
    text
}

fn load_context_chunks(catalog: &TribleSet) -> ContextChunkIndex {
    let mut index = ContextChunkIndex::default();

    for (chunk_id, level, summary, start_at, end_at) in find!(
        (
            chunk_id: Id,
            level: Value<U256BE>,
            summary: Value<Handle<Blake3, LongString>>,
            start_at: Value<NsTAIInterval>,
            end_at: Value<NsTAIInterval>
        ),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::level: ?level,
            playground_context::summary: ?summary,
            playground_context::start_at: ?start_at,
            playground_context::end_at: ?end_at,
        }])
    ) {
        let level = u256be_to_u64(level).unwrap_or_default();
        index.chunks.insert(
            chunk_id,
            ContextChunk {
                id: chunk_id,
                level,
                summary,
                start_at,
                end_at,
                left: None,
                right: None,
                about_exec_result: None,
                about_archive_message: None,
                archive_author: None,
                archive_person: None,
                archive_thread_root: None,
                archive_conversation: None,
                archive_source_format: None,
            },
        );
    }

    for (chunk_id, child_id) in find!(
        (chunk_id: Id, child_id: Id),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::left: ?child_id,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.left = Some(child_id);
        }
    }

    for (chunk_id, child_id) in find!(
        (chunk_id: Id, child_id: Id),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::right: ?child_id,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.right = Some(child_id);
        }
    }

    for (chunk_id, exec_result_id) in find!(
        (chunk_id: Id, exec_result_id: Id),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::about_exec_result: ?exec_result_id,
        }])
    ) {
        index.chunk_for_exec_result.insert(exec_result_id, chunk_id);
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.about_exec_result = Some(exec_result_id);
        }
    }

    for (chunk_id, archive_message_id) in find!(
        (chunk_id: Id, archive_message_id: Id),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::about_archive_message: ?archive_message_id,
        }])
    ) {
        index
            .chunk_for_archive_message
            .insert(archive_message_id, chunk_id);
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.about_archive_message = Some(archive_message_id);
        }
    }

    for (chunk_id, archive_author_id) in find!(
        (chunk_id: Id, archive_author_id: Id),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::archive_author: ?archive_author_id,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.archive_author = Some(archive_author_id);
        }
    }

    for (chunk_id, archive_person_id) in find!(
        (chunk_id: Id, archive_person_id: Id),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::archive_person: ?archive_person_id,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.archive_person = Some(archive_person_id);
        }
    }

    for (chunk_id, thread_root_id) in find!(
        (chunk_id: Id, thread_root_id: Id),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::archive_thread_root: ?thread_root_id,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.archive_thread_root = Some(thread_root_id);
        }
    }

    for (chunk_id, conversation_id) in find!(
        (chunk_id: Id, conversation_id: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::archive_conversation: ?conversation_id,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.archive_conversation = Some(conversation_id);
        }
    }

    for (chunk_id, source_format) in find!(
        (chunk_id: Id, source_format: String),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::archive_source_format: ?source_format,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.archive_source_format = Some(source_format);
        }
    }

    // Determine the LSM frontier by removing all chunks that are referenced as children.
    let mut children = HashSet::new();
    for chunk in index.chunks.values() {
        if let Some(left) = chunk.left {
            children.insert(left);
        }
        if let Some(right) = chunk.right {
            children.insert(right);
        }
    }

    for chunk in index.chunks.values() {
        if children.contains(&chunk.id) {
            continue;
        }
        let end_key = interval_key(chunk.end_at);
        match index
            .root_by_level
            .get(&chunk.level)
            .and_then(|id| index.chunks.get(id))
        {
            Some(existing) if interval_key(existing.end_at) >= end_key => {}
            _ => {
                index.root_by_level.insert(chunk.level, chunk.id);
            }
        }
    }

    index
}

fn insert_chunk_with_carry(
    ws: &mut Workspace<Pile>,
    index: &mut ContextChunkIndex,
    change: &mut TribleSet,
    mut carry: ContextChunk,
    semantic: &SemanticCompactor,
) -> Result<()> {
    let mut level = carry.level;
    loop {
        if let Some(existing_id) = index.root_by_level.remove(&level) {
            let existing = index
                .chunks
                .get(&existing_id)
                .cloned()
                .context("missing existing chunk for carry")?;

            // Order children by time to keep summaries consistent.
            let (left, right) = if interval_key(existing.start_at) <= interval_key(carry.start_at) {
                (existing, carry)
            } else {
                (carry, existing)
            };

            let left_text = load_text(ws, left.summary).context("load left chunk summary")?;
            let right_text = load_text(ws, right.summary).context("load right chunk summary")?;
            let merged_text = semantic
                .merge(left_text.as_str(), right_text.as_str())
                .context("semantic merge context chunks")?;
            let merged_handle = ws.put(merged_text);

            let now = epoch_interval(now_epoch());
            let parent_id = ufoid();
            let parent_level = level + 1;
            *change += entity! { &parent_id @
                playground_context::kind: playground_context::kind_chunk,
                playground_context::level: parent_level,
                playground_context::summary: merged_handle,
                playground_context::created_at: now,
                playground_context::start_at: left.start_at,
                playground_context::end_at: right.end_at,
                playground_context::left: left.id,
                playground_context::right: right.id,
            };

            carry = ContextChunk {
                id: *parent_id,
                level: parent_level,
                summary: merged_handle,
                start_at: left.start_at,
                end_at: right.end_at,
                left: Some(left.id),
                right: Some(right.id),
                about_exec_result: None,
                about_archive_message: None,
                archive_author: None,
                archive_person: None,
                archive_thread_root: None,
                archive_conversation: None,
                archive_source_format: None,
            };

            // Update chunk index for subsequent carry steps.
            index.chunks.insert(left.id, left);
            index.chunks.insert(right.id, right);
            index.chunks.insert(carry.id, carry.clone());

            level = parent_level;
            continue;
        }

        index.root_by_level.insert(level, carry.id);
        index.chunks.insert(carry.id, carry);
        return Ok(());
    }
}

struct SemanticCompactor {
    client: Client,
    endpoint_url: String,
    api_key: Option<String>,
    model: String,
    chars_per_token: u64,
    reduction_factor: u64,
    system_prompt: String,
}

const DEFAULT_COMPACTION_PROMPT: &str = "You are a context compaction module.\n\nGiven two prior memory chunks from a terminal-based agent, write a concise merged summary that preserves:\n- key actions taken\n- important results/outputs\n- errors and their causes\n- paths/ids that matter for follow-up\n\nOutput plain text only (no markdown), no code fences, no tool calls.\n";

impl SemanticCompactor {
    fn new(config: &Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("build semantic compaction http client")?;

        let mut model = config.llm.model.clone();
        let mut base_url = config.llm.base_url.clone();
        let mut api_key = config.llm.api_key.clone();
        let mut chars_per_token = config.llm.prompt_chars_per_token.max(1);
        if let Some(profile_id) = config.llm_compaction_profile_id {
            match config::load_llm_profile(config.pile_path.as_path(), profile_id) {
                Ok(Some((profile, _name))) => {
                    model = profile.model;
                    base_url = profile.base_url;
                    api_key = profile.api_key;
                    chars_per_token = profile.prompt_chars_per_token.max(1);
                }
                Ok(None) => eprintln!(
                    "warning: compaction profile {profile_id:x} not found; using active llm profile"
                ),
                Err(err) => eprintln!(
                    "warning: failed to load compaction profile {profile_id:x}: {err:#}; using active llm profile"
                ),
            }
        }

        Ok(Self {
            client,
            endpoint_url: chat_completions_url(base_url.as_str()),
            api_key,
            model,
            chars_per_token,
            reduction_factor: config.llm_compaction_reduction_factor.max(2),
            system_prompt: config
                .llm_compaction_prompt
                .clone()
                .unwrap_or_else(|| DEFAULT_COMPACTION_PROMPT.to_string()),
        })
    }

    fn merge(&self, left: &str, right: &str) -> Result<String> {
        let input_chars = left
            .chars()
            .count()
            .saturating_add(right.chars().count())
            .max(1);
        let target_chars = input_chars / (self.reduction_factor.max(2) as usize);
        let target_chars = target_chars.max(1);
        let max_tokens = target_chars.div_ceil(self.chars_per_token as usize) as u64;

        let user = format!(
            "CHUNK A:\n{left}\n\nCHUNK B:\n{right}\n\nMerge them into one summary and compress by ~1/{factor}. Keep critical details; drop repetition.",
            factor = self.reduction_factor,
        );
        let payload = serde_json::json!({
            "model": self.model,
            "messages": [
                {"role": "system", "content": self.system_prompt.as_str()},
                {"role": "user", "content": user},
            ],
            "stream": false,
            "temperature": 0,
            "max_tokens": max_tokens,
        });

        let mut last_err = None;
        for attempt in 1..=3usize {
            match self.send_once(&payload) {
                Ok(text) => return Ok(text),
                Err(err) => {
                    last_err = Some(err);
                    if attempt < 3 {
                        let backoff = 250_u64.saturating_mul(1_u64 << (attempt - 1));
                        sleep(Duration::from_millis(backoff));
                    }
                }
            }
        }

        Err(last_err.unwrap_or_else(|| anyhow!("semantic compaction failed without error detail")))
    }

    fn send_once(&self, payload: &serde_json::Value) -> Result<String> {
        let mut request = self.client.post(&self.endpoint_url);
        if let Some(api_key) = self.api_key.as_ref() {
            request = request.bearer_auth(api_key);
        }

        let response = request.json(payload).send().context("send http request")?;
        let status = response.status();
        if !status.is_success() {
            let body = response
                .text()
                .unwrap_or_else(|_| "<failed to read error body>".to_string());
            return Err(anyhow!(
                "semantic compaction request failed: HTTP {} for url ({}){}",
                status,
                self.endpoint_url,
                if body.trim().is_empty() {
                    "".to_string()
                } else {
                    format!(": {}", body.trim())
                }
            ));
        }

        let json: serde_json::Value = response.json().context("read response json")?;
        let text = extract_output_text(&json);
        Ok(text.trim().to_string())
    }
}

fn chat_completions_url(api_base_url: &str) -> String {
    let base = api_base_url.trim().trim_end_matches('/');
    if base.ends_with("/chat/completions") || base.ends_with("/completions") {
        return base.to_string();
    }
    if let Some(base) = base.strip_suffix("/responses") {
        return format!("{base}/chat/completions");
    }
    format!("{base}/chat/completions")
}

fn extract_output_text(json: &serde_json::Value) -> String {
    // Chat-completions style: choices[0].message.content
    if let Some(choices) = json.get("choices").and_then(|v| v.as_array()) {
        if let Some(first) = choices.first() {
            if let Some(message) = first.get("message") {
                let content = message.get("content");
                if let Some(text) = content.and_then(|v| v.as_str()) {
                    return text.to_string();
                }
                if let Some(parts) = content.and_then(|v| v.as_array()) {
                    let mut out = String::new();
                    for part in parts {
                        if part.get("type").and_then(|v| v.as_str()) == Some("text") {
                            if let Some(text) = part.get("text").and_then(|v| v.as_str()) {
                                out.push_str(text);
                            }
                        }
                    }
                    if !out.is_empty() {
                        return out;
                    }
                }
            }

            // Legacy completions-style fallback: choices[0].text
            if let Some(text) = first.get("text").and_then(|v| v.as_str()) {
                return text.to_string();
            }
        }
    }

    String::new()
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

fn format_exec_output(command: &str, result: ExecResult, reasoning_text: Option<&str>) -> String {
    let mut text = String::new();
    append_section(&mut text, "command", command);
    if let Some(reasoning_text) = reasoning_text {
        append_section(&mut text, "reasoning", reasoning_text);
    }
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
