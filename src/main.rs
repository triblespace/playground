use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, sleep};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use clap::{Args, CommandFactory, Parser, Subcommand, ValueEnum};
use reqwest::blocking::Client;
use triblespace::core::blob::Bytes;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::metadata;
use triblespace::core::repo::content as commit_content;
use triblespace::core::repo::parent as commit_parent;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::prelude::blobschemas::{LongString, SimpleArchive};
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
mod model_worker;
mod relations_schema;
mod repo_ops;
mod repo_util;
mod schema;
mod time_util;
mod workspace_snapshot;

use archive_schema::{playground_archive, playground_archive_import};
use chat_prompt::ChatMessage;
use config::{Config, MemoryLensConfig};
use relations_schema::playground_relations;
use repo_util::{
    close_repo, current_branch_head, init_repo, load_text, pull_workspace, push_workspace,
    refresh_cached_checkout,
};
use schema::{model_chat, playground_cog, playground_context, playground_exec};
use time_util::{epoch_interval, interval_key, now_epoch};

mod reason_events {
    use triblespace::prelude::attributes;
    use triblespace::prelude::blobschemas;
    use triblespace::prelude::valueschemas;

    attributes! {
        "B10329D5D1087D15A3DAFF7A7CC50696" as text: valueschemas::Handle<valueschemas::Blake3, blobschemas::LongString>;
        "FBA9BC32A457C7BFFDB7E0181D3E82A4" as created_at: valueschemas::NsTAIInterval;
        "E6B1C728F1AE9F46CAB4DBB60D1A9528" as about_turn: valueschemas::GenId;
        "721DED6DA776F2CF4FB91C54D9F82358" as worker: valueschemas::GenId;
        "514F4FE9F560FB155450462C8CF50749" as command_text: valueschemas::Handle<valueschemas::Blake3, blobschemas::LongString>;
    }
}

#[derive(Subcommand, Debug)]
enum CommandMode {
    #[command(about = "Run core + Model and start the exec worker in a Lima VM")]
    Run(RunArgs),
    #[command(about = "Run only the core loop (no model/exec workers)")]
    Core,
    #[command(about = "Run only the exec worker (remote host)")]
    Exec(WorkerArgs),
    #[command(about = "Run only the Model worker (host)")]
    Model(WorkerArgs),
    #[cfg(feature = "diagnostics")]
    #[command(about = "Open the diagnostics dashboard")]
    Diagnostics(DiagnosticsArgs),
    #[command(about = "Estimate/backfill context memory independent of Model requests")]
    Memory {
        #[command(subcommand)]
        command: MemoryCommand,
    },
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
}

#[derive(Subcommand, Debug)]
enum MemoryCommand {
    #[command(about = "Estimate pending compaction work and approximate token/cost usage")]
    Estimate(MemoryEstimateArgs),
    #[command(
        about = "Backfill context memory chunks from archive/exec without creating Model requests"
    )]
    Build(MemoryBuildArgs),
    #[command(about = "Consolidate current moment into memory by setting the moment boundary turn")]
    Consolidate(MemoryConsolidateArgs),
}

#[derive(Args, Debug, Clone)]
struct MemoryEstimateArgs {
    #[arg(long, default_value_t = false)]
    include_exec: bool,
    #[arg(long)]
    max_archive_leaves: Option<usize>,
    #[arg(long, default_value_t = 256)]
    sample_leaves: usize,
    #[arg(long)]
    input_cost_per_1m_tokens: Option<f64>,
    #[arg(long)]
    output_cost_per_1m_tokens: Option<f64>,
    #[arg(long)]
    cost_currency: Option<String>,
}

#[derive(Args, Debug, Clone)]
struct MemoryBuildArgs {
    #[arg(long, default_value_t = false)]
    include_exec: bool,
    #[arg(long)]
    max_archive_leaves: Option<usize>,
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[derive(Args, Debug, Clone)]
struct MemoryConsolidateArgs {
    /// Optional explicit turn id. If omitted, uses the latest finished exec turn.
    #[arg(long)]
    turn_id: Option<String>,
}

#[derive(Args, Debug, Clone)]
#[command(about = "Run core + Model and start the exec worker in a Lima VM")]
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
        help = "Value to set. Use @path to read from file, @- to read stdin; use `config unset` to clear optional fields."
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
    TavilyApiKey,
    ExaApiKey,
    MemoryCompactionArity,
    ExecDefaultCwd,
    ExecSandboxProfile,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
#[value(rename_all = "kebab-case")]
enum OptionalConfigField {
    TeamsBranchId,
    PersonaId,
    TavilyApiKey,
    ExaApiKey,
    ExecDefaultCwd,
    ExecSandboxProfile,
}

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Playground runner that turns Model output into exec requests"
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
        CommandMode::Model(args) => {
            let instance = default_instance_name();
            let pile_path = resolve_pile_path(cli.pile.clone(), instance.as_str());
            let config = Config::load(Some(pile_path.as_path())).context("load config")?;
            run_model_worker(config, args)
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
        CommandMode::Memory { command } => {
            let instance = default_instance_name();
            let pile_path = resolve_pile_path(cli.pile.clone(), instance.as_str());
            let config = Config::load(Some(pile_path.as_path())).context("load config")?;
            run_memory_command(config, command)
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
    let model_stop = stop.clone();
    let model_config = config.clone();
    let model_worker_id = *ufoid();
    let model_handle = thread::spawn(move || {
        model_worker::run_model_loop(model_config, model_worker_id, poll_ms, Some(model_stop))
    });

    prepare_lima_service(&config, &args.lima)?;

    let core_result = run_loop(config);
    stop.store(true, Ordering::Relaxed);

    let model_result = model_handle
        .join()
        .map_err(|_| anyhow!("model worker panicked"))?;

    core_result?;
    model_result.context("model worker")?;
    Ok(())
}

fn run_exec_worker(config: Config, args: WorkerArgs) -> Result<()> {
    let poll_ms = args.poll_ms.unwrap_or(config.poll_ms);
    let worker_id = parse_worker_id(args.worker_id)?;
    exec_worker::run_exec_loop(config, worker_id, poll_ms, None)
}

fn run_model_worker(config: Config, args: WorkerArgs) -> Result<()> {
    let poll_ms = args.poll_ms.unwrap_or(config.poll_ms);
    let worker_id = parse_worker_id(args.worker_id)?;
    model_worker::run_model_loop(config, worker_id, poll_ms, None)
}

#[derive(Debug, Clone)]
struct CompactionProfileInfo {
    model: String,
    base_url: String,
    chars_per_token: u64,
    source: String,
}

#[derive(Debug, Default, Clone)]
struct MergeSimulation {
    merge_calls: usize,
    merged_children_total: usize,
    final_runs_by_level: BTreeMap<u64, usize>,
}

fn run_memory_command(config: Config, command: MemoryCommand) -> Result<()> {
    match command {
        MemoryCommand::Estimate(args) => run_memory_estimate(config, args),
        MemoryCommand::Build(args) => run_memory_build(config, args),
        MemoryCommand::Consolidate(args) => run_memory_consolidate(config, args),
    }
}

fn memory_status(message: impl AsRef<str>) {
    eprintln!("[memory] {}", message.as_ref());
}

const MEMORY_PROGRESS_CHECKPOINT_EVERY: usize = 250;

fn memory_status_timed(stage: &str, started_at: Instant) {
    memory_status(format!(
        "{stage} ({})",
        format_elapsed(started_at.elapsed())
    ));
}

fn format_elapsed(duration: Duration) -> String {
    let secs = duration.as_secs_f64();
    if secs < 60.0 {
        format!("{secs:.2}s")
    } else {
        let mins = (secs / 60.0).floor() as u64;
        let rem = secs - (mins as f64) * 60.0;
        format!("{mins}m {rem:.1}s")
    }
}

fn run_memory_estimate(config: Config, args: MemoryEstimateArgs) -> Result<()> {
    let merge_arity = config.memory_compaction_arity.max(2) as usize;
    let profile = resolve_compaction_profile_info(&config);

    let (mut repo, branch_id) = init_repo(&config).context("open triblespace repo")?;
    repo_util::seed_metadata(&mut repo)?;
    let result = (|| -> Result<()> {
        memory_status("loading archive branch...");
        let stage = Instant::now();
        let archive_load = load_archive_messages_incremental(
            &mut repo,
            config.archive_branch_id,
            "pull archive workspace for memory estimate",
            "archive",
        )?;
        let archive_messages = archive_load.messages;
        let archive_coverage = archive_load.coverage;
        memory_status_timed(
            &format!(
                "archive branch loaded ({} message(s))",
                archive_messages.len()
            ),
            stage,
        );
        memory_status("loading relations branch...");
        let stage = Instant::now();
        let relations_catalog = load_optional_catalog(
            &mut repo,
            config.relations_branch_id,
            "pull relations workspace for memory estimate",
        )?;
        memory_status_timed("relations branch loaded", stage);
        memory_status("loading cognition workspace...");
        let stage = Instant::now();
        let mut ws = pull_workspace(&mut repo, branch_id, "pull workspace for memory estimate")?;
        let catalog = ws.checkout(..).context("checkout workspace")?;
        memory_status_timed("cognition workspace loaded", stage);

        let mut core_index = CoreIndex::default();
        let stage = Instant::now();
        core_index.apply_delta(&catalog, &catalog);
        memory_status_timed("cognition index built", stage);
        memory_status("indexing existing context chunks...");
        let stage = Instant::now();
        let index = load_context_chunks(&catalog);
        memory_status_timed(
            &format!("context index built: {} chunk(s)", index.chunks.len()),
            stage,
        );
        memory_status("building relations index...");
        let stage = Instant::now();
        let relations = load_relations_index(&mut ws, &relations_catalog)?;
        memory_status_timed(
            &format!(
                "relations index built: {} person key(s)",
                relations.key_to_person_id.len()
            ),
            stage,
        );

        let lens_ids: Vec<Id> = config.memory_lenses.iter().map(|lens| lens.id).collect();
        let pending_archive_total: usize = archive_messages
            .iter()
            .map(|msg| {
                lens_ids
                    .iter()
                    .filter(|lens_id| {
                        !index
                            .chunk_for_archive_message
                            .contains_key(&(msg.id, **lens_id))
                    })
                    .count()
            })
            .sum();
        let pending_archive = args
            .max_archive_leaves
            .map(|limit| pending_archive_total.min(limit))
            .unwrap_or(pending_archive_total);

        let pending_exec_total = if args.include_exec {
            sorted_finished_command_results(&core_index)
                .into_iter()
                .map(|result| {
                    lens_ids
                        .iter()
                        .filter(|lens_id| {
                            !index
                                .chunk_for_exec_result
                                .contains_key(&(result.id, **lens_id))
                        })
                        .count()
                })
                .sum()
        } else {
            0
        };
        let pending_exec = pending_exec_total;

        let new_leaves = pending_archive.saturating_add(pending_exec);
        memory_status(format!(
            "simulating carry merges for {new_leaves} pending leaves (k={merge_arity})..."
        ));
        let stage = Instant::now();
        let mut new_leaves_per_lens: HashMap<Id, usize> = HashMap::new();
        for message in &archive_messages {
            for lens_id in &lens_ids {
                if !index
                    .chunk_for_archive_message
                    .contains_key(&(message.id, *lens_id))
                {
                    *new_leaves_per_lens.entry(*lens_id).or_insert(0) += 1;
                }
            }
        }
        if args.include_exec {
            for result in sorted_finished_command_results(&core_index) {
                for lens_id in &lens_ids {
                    if !index
                        .chunk_for_exec_result
                        .contains_key(&(result.id, *lens_id))
                    {
                        *new_leaves_per_lens.entry(*lens_id).or_insert(0) += 1;
                    }
                }
            }
        }
        let sim = simulate_kary_merges(
            &index.roots_by_lens_level,
            merge_arity,
            &new_leaves_per_lens,
        );
        memory_status_timed("merge simulation complete", stage);

        memory_status("sampling existing context leaf summaries...");
        let stage = Instant::now();
        let (existing_chars_sum, existing_samples) =
            sample_existing_leaf_summary_chars(&mut ws, &index, args.sample_leaves)?;
        memory_status_timed(
            &format!("existing leaf sampling complete: {existing_samples} sample(s)"),
            stage,
        );
        memory_status("sampling pending archive summaries...");
        let stage = Instant::now();
        let (archive_chars_sum, archive_samples) = sample_pending_archive_leaf_summary_chars(
            &mut ws,
            archive_messages.as_slice(),
            &index,
            &relations,
            lens_ids.as_slice(),
            args.sample_leaves,
        )?;
        memory_status_timed(
            &format!("pending archive sampling complete: {archive_samples} sample(s)"),
            stage,
        );
        let sample_chars_sum = existing_chars_sum.saturating_add(archive_chars_sum);
        let sample_count = existing_samples.saturating_add(archive_samples);
        let avg_leaf_chars = if sample_count == 0 {
            800.0
        } else {
            (sample_chars_sum as f64) / (sample_count as f64)
        };

        let estimated_input_chars = (sim.merged_children_total as f64) * avg_leaf_chars;
        let estimated_output_chars = (sim.merge_calls as f64) * avg_leaf_chars;
        let input_tokens = (estimated_input_chars / profile.chars_per_token as f64).ceil();
        let output_tokens = (estimated_output_chars / profile.chars_per_token as f64).ceil();

        println!("memory estimate");
        println!("  model: {} ({})", profile.model, profile.source);
        println!("  base_url: {}", profile.base_url);
        println!(
            "  local_endpoint: {}",
            if looks_local_base_url(profile.base_url.as_str()) {
                "yes"
            } else {
                "no"
            }
        );
        println!("  chars_per_token: {}", profile.chars_per_token);
        println!("  merge_arity: {}", merge_arity);
        println!("  pending_archive: {}", pending_archive);
        println!("  pending_exec: {}", pending_exec);
        println!(
            "  archive_kind_messages: {}",
            archive_coverage.kind_message_total
        );
        println!(
            "  archive_imported_messages: {}",
            archive_coverage.imported_message_total
        );
        println!(
            "  archive_strict_imported_messages: {} ({:.2}%)",
            archive_coverage.strict_imported_total,
            archive_coverage.strict_imported_pct()
        );
        println!("  leaves_to_add: {}", new_leaves);
        println!("  estimated_merge_calls: {}", sim.merge_calls);
        println!("  estimated_input_tokens: {}", input_tokens as u64);
        println!("  estimated_output_tokens: {}", output_tokens as u64);
        println!("  sampled_leaf_summaries: {}", sample_count);
        println!("  sampled_avg_leaf_chars: {:.1}", avg_leaf_chars);
        if let (Some(in_price), Some(out_price)) = (
            args.input_cost_per_1m_tokens,
            args.output_cost_per_1m_tokens,
        ) {
            let estimated_cost =
                (input_tokens / 1_000_000.0) * in_price + (output_tokens / 1_000_000.0) * out_price;
            if let Some(currency) = args.cost_currency.as_deref() {
                println!("  estimated_cost: {:.4} {currency}", estimated_cost);
            } else {
                println!("  estimated_cost: {:.4}", estimated_cost);
            }
        } else {
            println!(
                "  estimated_cost: n/a (pass --input-cost-per-1m-tokens and --output-cost-per-1m-tokens)"
            );
        }
        println!("  frontier_after_backfill:");
        for (level, count) in sim.final_runs_by_level {
            println!("    L{level}: {count} run(s)");
        }
        memory_status("estimate complete.");

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

fn run_memory_build(config: Config, args: MemoryBuildArgs) -> Result<()> {
    let merge_arity = config.memory_compaction_arity.max(2) as usize;
    let profile = resolve_compaction_profile_info(&config);

    let (mut repo, branch_id) = init_repo(&config).context("open triblespace repo")?;
    repo_util::seed_metadata(&mut repo)?;
    let result = (|| -> Result<()> {
        memory_status("loading archive branch...");
        let stage = Instant::now();
        let archive_load = load_archive_messages_incremental(
            &mut repo,
            config.archive_branch_id,
            "pull archive workspace for memory build",
            "archive",
        )?;
        let archive_messages = archive_load.messages;
        let archive_coverage = archive_load.coverage;
        memory_status_timed(
            &format!(
                "archive branch loaded ({} message(s))",
                archive_messages.len()
            ),
            stage,
        );
        memory_status("loading relations branch...");
        let stage = Instant::now();
        let relations_catalog = load_optional_catalog(
            &mut repo,
            config.relations_branch_id,
            "pull relations workspace for memory build",
        )?;
        memory_status_timed("relations branch loaded", stage);
        memory_status("loading cognition workspace...");
        let stage = Instant::now();
        let mut ws = pull_workspace(&mut repo, branch_id, "pull workspace for memory build")?;
        let catalog = ws.checkout(..).context("checkout workspace")?;
        memory_status_timed("cognition workspace loaded", stage);
        let mut core_index = CoreIndex::default();
        let stage = Instant::now();
        core_index.apply_delta(&catalog, &catalog);
        memory_status_timed("cognition index built", stage);
        memory_status("indexing existing context chunks...");
        let stage = Instant::now();
        let mut index = load_context_chunks(&catalog);
        memory_status_timed(
            &format!("context index built: {} chunk(s)", index.chunks.len()),
            stage,
        );
        memory_status("building relations index...");
        let stage = Instant::now();
        let relations = load_relations_index(&mut ws, &relations_catalog)?;
        memory_status_timed(
            &format!(
                "relations index built: {} person key(s)",
                relations.key_to_person_id.len()
            ),
            stage,
        );

        println!("memory build");
        println!("  model: {} ({})", profile.model, profile.source);
        println!("  base_url: {}", profile.base_url);
        println!(
            "  local_endpoint: {}",
            if looks_local_base_url(profile.base_url.as_str()) {
                "yes"
            } else {
                "no"
            }
        );
        println!("  merge_arity: {}", merge_arity);
        println!(
            "  archive_kind_messages: {}",
            archive_coverage.kind_message_total
        );
        println!(
            "  archive_imported_messages: {}",
            archive_coverage.imported_message_total
        );
        println!(
            "  archive_strict_imported_messages: {} ({:.2}%)",
            archive_coverage.strict_imported_total,
            archive_coverage.strict_imported_pct()
        );
        if args.dry_run {
            println!("  mode: dry-run (no writes)");
            memory_status("dry-run complete.");
            return Ok(());
        }

        let semantic_compactor = SemanticCompactor::new(&config)?;
        let mut change = TribleSet::new();
        let mut stats = CompactionRunStats::default();
        memory_status("backfilling archive memory chunks...");
        let stage = Instant::now();
        let archive_added = ingest_archive_context_chunks(
            &mut ws,
            &mut index,
            &mut change,
            archive_messages.as_slice(),
            &relations,
            None,
            args.max_archive_leaves,
            merge_arity,
            &semantic_compactor,
            &mut stats,
            Some(500),
        )?;
        memory_status_timed(
            &format!("archive backfill pass complete: added {archive_added} chunk(s)"),
            stage,
        );
        let exec_added = if args.include_exec {
            memory_status("backfilling exec memory chunks...");
            let stage = Instant::now();
            let results = sorted_finished_command_results(&core_index);
            let exec_added = ingest_exec_context_chunks(
                &mut ws,
                &core_index,
                &mut index,
                &mut change,
                results.as_slice(),
                None,
                merge_arity,
                &semantic_compactor,
                &mut stats,
                Some(200),
            )?;
            memory_status_timed(
                &format!("exec backfill pass complete: added {exec_added} chunk(s)"),
                stage,
            );
            exec_added
        } else {
            0
        };

        if change.is_empty() {
            println!("  no pending memory chunks to backfill.");
            memory_status("build complete (nothing to write).");
            return Ok(());
        }

        memory_status("committing/pushing backfill to pile...");
        let stage = Instant::now();
        ws.commit(change, None, Some("memory backfill"));
        push_workspace(&mut repo, &mut ws).context("push memory backfill")?;
        memory_status_timed("backfill committed and pushed", stage);
        println!("  archive_leaves_added: {}", archive_added);
        println!("  exec_leaves_added: {}", exec_added);
        println!("  merge_calls: {}", stats.merge_calls);
        println!("  merged_children_total: {}", stats.merged_children_total);
        println!(
            "  merge_input_chars_total: {}",
            stats.merge_input_chars_total
        );
        println!(
            "  merge_output_chars_total: {}",
            stats.merge_output_chars_total
        );
        memory_status("build complete.");
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

fn run_memory_consolidate(config: Config, args: MemoryConsolidateArgs) -> Result<()> {
    let (mut repo, branch_id) = init_repo(&config).context("open triblespace repo")?;
    let result = (|| -> Result<Id> {
        let mut ws = pull_workspace(
            &mut repo,
            branch_id,
            "pull workspace for memory consolidate",
        )?;
        let catalog = ws.checkout(..).context("checkout workspace")?;
        let mut core_index = CoreIndex::default();
        core_index.apply_delta(&catalog, &catalog);

        let target_turn_id = if let Some(raw) = args.turn_id.as_deref() {
            let turn_id = parse_hex_id(raw, "turn_id")?;
            if !core_index.has_finished_command_result(turn_id) {
                return Err(anyhow!("turn_id {turn_id:x} is not a finished exec turn"));
            }
            turn_id
        } else {
            sorted_finished_command_results(&core_index)
                .into_iter()
                .last()
                .ok_or_else(|| anyhow!("no finished exec turns found"))?
                .id
        };

        let now = epoch_interval(now_epoch());
        let boundary_id = ufoid();
        let mut change = TribleSet::new();
        change += entity! { &boundary_id @
            playground_cog::kind: playground_cog::kind_moment_boundary,
            playground_cog::created_at: now,
            playground_cog::moment_boundary_turn_id: target_turn_id,
        };
        ws.commit(change, None, Some("flush moment boundary"));
        push_workspace(&mut repo, &mut ws).context("push moment boundary")?;
        Ok(target_turn_id)
    })();

    if let Err(err) = close_repo(repo) {
        if result.is_ok() {
            return Err(err);
        }
        eprintln!("warning: failed to close pile cleanly: {err:#}");
    }

    let target_turn_id = result?;
    println!("moment boundary turn set to {:x}", target_turn_id);
    Ok(())
}

fn resolve_compaction_profile_info(config: &Config) -> CompactionProfileInfo {
    let mut model = config.model.model.clone();
    let mut base_url = config.model.base_url.clone();
    let mut chars_per_token = config.model.chars_per_token.max(1);
    let mut source = "active profile".to_string();

    if let Some(profile_id) = config.compaction_profile_id {
        match config::load_model_profile(config.pile_path.as_path(), profile_id) {
            Ok(Some((profile, name))) => {
                model = profile.model;
                base_url = profile.base_url;
                chars_per_token = profile.chars_per_token.max(1);
                source = format!("compaction profile {name}");
            }
            Ok(None) => {
                eprintln!(
                    "warning: compaction profile {profile_id:x} not found; using active profile"
                );
            }
            Err(err) => {
                eprintln!(
                    "warning: failed to load compaction profile {profile_id:x}: {err:#}; using active profile"
                );
            }
        }
    }

    CompactionProfileInfo {
        model,
        base_url,
        chars_per_token,
        source,
    }
}

fn looks_local_base_url(base_url: &str) -> bool {
    let lower = base_url.to_ascii_lowercase();
    lower.contains("localhost")
        || lower.contains("127.0.0.1")
        || lower.contains("::1")
        || lower.contains("ollama")
}

fn simulate_kary_merges(
    roots_by_lens_level: &HashMap<(Id, u64), Vec<Id>>,
    merge_arity: usize,
    new_leaves_per_lens: &HashMap<Id, usize>,
) -> MergeSimulation {
    let k = merge_arity.max(2);
    let mut by_lens: BTreeMap<Id, BTreeMap<u64, usize>> = BTreeMap::new();
    for ((lens_id, level), runs) in roots_by_lens_level {
        if runs.is_empty() {
            continue;
        }
        by_lens
            .entry(*lens_id)
            .or_default()
            .insert(*level, runs.len());
    }
    let mut sim = MergeSimulation::default();

    for (lens_id, new_leaves) in new_leaves_per_lens {
        let counts = by_lens.entry(*lens_id).or_default();
        for _ in 0..*new_leaves {
            *counts.entry(0).or_insert(0) += 1;
            let mut level = 0u64;
            loop {
                let Some(count) = counts.get(&level).copied() else {
                    break;
                };
                if count < k {
                    break;
                }
                let carry_count = count / k;
                let remainder = count % k;
                if remainder == 0 {
                    counts.remove(&level);
                } else {
                    counts.insert(level, remainder);
                }
                *counts.entry(level + 1).or_insert(0) += carry_count;
                sim.merge_calls = sim.merge_calls.saturating_add(carry_count);
                sim.merged_children_total = sim
                    .merged_children_total
                    .saturating_add(carry_count.saturating_mul(k));
                level = level.saturating_add(1);
            }
        }
    }

    let mut combined: BTreeMap<u64, usize> = BTreeMap::new();
    for counts in by_lens.values() {
        for (level, count) in counts {
            *combined.entry(*level).or_insert(0) += *count;
        }
    }
    sim.final_runs_by_level = combined
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .collect();
    sim
}

fn sample_existing_leaf_summary_chars(
    ws: &mut Workspace<Pile>,
    index: &ContextChunkIndex,
    sample_size: usize,
) -> Result<(usize, usize)> {
    let mut leaves: Vec<&ContextChunk> = index
        .chunks
        .values()
        .filter(|chunk| chunk.level == 0)
        .collect();
    leaves.sort_by_key(|chunk| (interval_key(chunk.end_at), chunk.id));
    leaves.reverse();

    let mut total = 0usize;
    let mut count = 0usize;
    for chunk in leaves.into_iter().take(sample_size) {
        let text = load_text(ws, chunk.summary).context("load existing leaf summary")?;
        total = total.saturating_add(text.chars().count());
        count = count.saturating_add(1);
    }
    Ok((total, count))
}

fn sample_pending_archive_leaf_summary_chars(
    ws: &mut Workspace<Pile>,
    archive_messages: &[ArchiveMessageInfo],
    index: &ContextChunkIndex,
    relations: &RelationsIndex,
    lens_ids: &[Id],
    sample_size: usize,
) -> Result<(usize, usize)> {
    let mut total = 0usize;
    let mut count = 0usize;
    for message in archive_messages.iter() {
        if count >= sample_size {
            break;
        }
        if lens_ids.iter().all(|lens_id| {
            index
                .chunk_for_archive_message
                .contains_key(&(message.id, *lens_id))
        }) {
            continue;
        }

        let author_name = load_optional_text(ws, message.author_name)?;
        let source_author = load_text(ws, message.source_author)?;
        let source_role = load_text(ws, message.source_role)?;
        let source_message_id = load_text(ws, message.source_message_id)?;
        let conversation_id = load_text(ws, message.conversation_id)?;
        let content = load_text(ws, message.content).context("load archive message content")?;
        let resolved_person = resolve_archive_person(
            relations,
            author_name.as_deref(),
            Some(source_author.as_str()),
        );
        let leaf_summary = format_archive_output(
            message,
            author_name.as_deref(),
            source_author.as_str(),
            source_role.as_str(),
            source_message_id.as_str(),
            conversation_id.as_str(),
            content.as_str(),
            resolved_person
                .and_then(|person_id| relations.person_label.get(&person_id).map(|s| s.as_str())),
            resolved_person,
        );
        total = total.saturating_add(leaf_summary.chars().count());
        count = count.saturating_add(1);
    }
    Ok((total, count))
}

fn handle_config(pile: Option<&Path>, command: ConfigCommand) -> Result<()> {
    let mut config = Config::load(pile).context("load config")?;
    match command {
        ConfigCommand::Show { show_secrets } => {
            print_config(&config, show_secrets);
        }
        ConfigCommand::Set(args) => {
            apply_config_set(&mut config, args.field, args.value.as_str())?;
            config.store().context("store config")?;
            print_config(&config, false);
        }
        ConfigCommand::Unset(args) => {
            apply_config_unset(&mut config, args.field)?;
            config.store().context("store config")?;
            print_config(&config, false);
        }
    }
    Ok(())
}

fn apply_config_set(config: &mut Config, field: ConfigField, value: &str) -> Result<()> {
    match field {
        ConfigField::SystemPrompt => {
            config.system_prompt = load_value_or_file(value, "system_prompt")?;
        }
        ConfigField::Branch => {
            config.branch = load_value_or_file(value, "branch")?;
        }
        ConfigField::BranchId => {
            config.branch_id = Some(parse_hex_id(value, "branch_id")?);
        }
        ConfigField::CompassBranchId => {
            config.compass_branch_id = Some(parse_hex_id(value, "compass_branch_id")?);
        }
        ConfigField::ExecBranchId => {
            config.exec_branch_id = Some(parse_hex_id(value, "exec_branch_id")?);
        }
        ConfigField::LocalMessagesBranchId => {
            config.local_messages_branch_id =
                Some(parse_hex_id(value, "local_messages_branch_id")?);
        }
        ConfigField::RelationsBranchId => {
            config.relations_branch_id = Some(parse_hex_id(value, "relations_branch_id")?);
        }
        ConfigField::TeamsBranchId => {
            config.teams_branch_id = Some(parse_hex_id(value, "teams_branch_id")?);
        }
        ConfigField::WorkspaceBranchId => {
            config.workspace_branch_id = Some(parse_hex_id(value, "workspace_branch_id")?);
        }
        ConfigField::ArchiveBranchId => {
            config.archive_branch_id = Some(parse_hex_id(value, "archive_branch_id")?);
        }
        ConfigField::WebBranchId => {
            config.web_branch_id = Some(parse_hex_id(value, "web_branch_id")?);
        }
        ConfigField::MediaBranchId => {
            config.media_branch_id = Some(parse_hex_id(value, "media_branch_id")?);
        }
        ConfigField::Author => {
            config.author = load_value_or_file(value, "author")?;
        }
        ConfigField::AuthorRole => {
            config.author_role = load_value_or_file(value, "author_role")?;
        }
        ConfigField::PersonaId => {
            config.persona_id = Some(parse_hex_id(value, "persona_id")?);
        }
        ConfigField::PollMs => {
            config.poll_ms = parse_u64(value, "poll_ms")?;
        }
        ConfigField::TavilyApiKey => {
            config.tavily_api_key = Some(load_value_or_file_trimmed(value, "tavily_api_key")?);
        }
        ConfigField::ExaApiKey => {
            config.exa_api_key = Some(load_value_or_file_trimmed(value, "exa_api_key")?);
        }
        ConfigField::MemoryCompactionArity => {
            let factor = parse_u64(value, "memory_compaction_arity")?;
            if factor < 2 {
                return Err(anyhow!("memory_compaction_arity must be >= 2"));
            }
            config.memory_compaction_arity = factor;
        }
        ConfigField::ExecDefaultCwd => {
            let value = load_value_or_file(value, "exec_default_cwd")?;
            config.exec.default_cwd = Some(PathBuf::from(value.trim()));
        }
        ConfigField::ExecSandboxProfile => {
            config.exec.sandbox_profile = Some(parse_hex_id(value, "exec_sandbox_profile")?);
        }
    }
    Ok(())
}

fn apply_config_unset(config: &mut Config, field: OptionalConfigField) -> Result<()> {
    match field {
        OptionalConfigField::TeamsBranchId => config.teams_branch_id = None,
        OptionalConfigField::PersonaId => config.persona_id = None,
        OptionalConfigField::TavilyApiKey => config.tavily_api_key = None,
        OptionalConfigField::ExaApiKey => config.exa_api_key = None,
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
        if path == "-" {
            let mut value = String::new();
            std::io::stdin()
                .read_to_string(&mut value)
                .with_context(|| format!("read {label} from stdin"))?;
            return Ok(value);
        }
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

    println!("\n[model]");
    println!(
        "profile_id = {}",
        config
            .model_profile_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!("profile_name = \"{}\"", config.model_profile_name);
    println!("model = \"{}\"", config.model.model);
    println!("base_url = \"{}\"", config.model.base_url);
    match (&config.model.api_key, show_secrets) {
        (Some(key), true) => println!("api_key = \"{}\"", key),
        (Some(_), false) => println!("api_key = \"<redacted>\""),
        (None, _) => println!("api_key = null"),
    }
    println!(
        "reasoning_effort = {}",
        config
            .model
            .reasoning_effort
            .as_ref()
            .map(|value| format!("\"{}\"", value))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "reasoning_summary = {}",
        config
            .model
            .reasoning_summary
            .map(|summary| format!("\"{}\"", summary.as_str()))
            .unwrap_or_else(|| "null".to_string())
    );
    println!("stream = {}", config.model.stream);
    println!(
        "context_window_tokens = {}",
        config.model.context_window_tokens
    );
    println!("max_output_tokens = {}", config.model.max_output_tokens);
    println!(
        "context_safety_margin_tokens = {}",
        config.model.context_safety_margin_tokens
    );
    println!(
        "chars_per_token = {}",
        config.model.chars_per_token
    );
    println!(
        "compaction_profile_id = {}",
        config
            .compaction_profile_id
            .map(|id| format!("\"{id:x}\""))
            .unwrap_or_else(|| "null".to_string())
    );
    println!(
        "memory_compaction_arity = {}",
        config.memory_compaction_arity
    );
    println!("memory_lens_count = {}", config.memory_lenses.len());
    for lens in &config.memory_lenses {
        println!(
            "memory_lens.{}.id = \"{:x}\"",
            lens.name.replace(' ', "-"),
            lens.id
        );
        println!(
            "memory_lens.{}.max_output_tokens = {}",
            lens.name.replace(' ', "-"),
            lens.max_output_tokens
        );
        println!(
            "memory_lens.{}.prompt = \"{}\"",
            lens.name.replace(' ', "-"),
            lens.prompt.replace('\"', "\\\"")
        );
        println!(
            "memory_lens.{}.compaction_prompt = \"{}\"",
            lens.name.replace(' ', "-"),
            lens.compaction_prompt.replace('\"', "\\\"")
        );
    }

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
        let mut request_info = ensure_model_request(&mut repo, branch_id, &config)?;

        loop {
            let model_result =
                wait_for_model_result(&mut repo, branch_id, request_info.id, config.poll_ms)?;
            if let Some(error) = model_result.error {
                eprintln!(
                    "warning: model request {request_id:x} failed: {error}",
                    request_id = request_info.id
                );
                request_info = ensure_model_request(&mut repo, branch_id, &config)?;
                sleep(Duration::from_millis(config.poll_ms));
                continue;
            }

            let command = model_result.output_text.trim();
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
struct ModelRequestInfo {
    id: Id,
    thought_id: Option<Id>,
}

#[derive(Debug, Clone)]
struct CoreModelRequest {
    id: Id,
    requested_at: Option<Value<NsTAIInterval>>,
    thought_id: Option<Id>,
}

#[derive(Debug, Clone)]
struct CoreThought {
    id: Id,
    created_at: Option<Value<NsTAIInterval>>,
    context: Option<Value<Handle<Blake3, LongString>>>,
}

#[derive(Debug, Clone)]
struct CoreMomentBoundary {
    id: Id,
    created_at: Option<Value<NsTAIInterval>>,
    turn_id: Option<Id>,
}

#[derive(Debug, Clone)]
struct CoreCommandRequest {
    id: Id,
    requested_at: Option<Value<NsTAIInterval>>,
    about_thought: Option<Id>,
    command: Option<Value<Handle<Blake3, LongString>>>,
}

#[derive(Debug, Clone)]
struct ModelResultEntry {
    id: Id,
    about_request: Option<Id>,
    finished_at: Option<Value<NsTAIInterval>>,
    attempt: Option<Value<U256BE>>,
    output_text: Option<Value<Handle<Blake3, LongString>>>,
    reasoning_text: Option<Value<Handle<Blake3, LongString>>>,
    error: Option<Value<Handle<Blake3, LongString>>>,
}

#[derive(Debug, Clone)]
struct CoreReasonEvent {
    id: Id,
    about_turn: Option<Id>,
    created_at: Option<Value<NsTAIInterval>>,
    text: Option<Value<Handle<Blake3, LongString>>>,
    command_text: Option<Value<Handle<Blake3, LongString>>>,
    worker: Option<Id>,
}

#[derive(Default)]
struct CoreIndex {
    model_requests: HashMap<Id, CoreModelRequest>,
    model_done_requests: HashSet<Id>,
    request_for_thought: HashMap<Id, Id>,
    thoughts: HashMap<Id, CoreThought>,
    moment_boundaries: HashMap<Id, CoreMomentBoundary>,
    thought_for_exec_result: HashMap<Id, Id>,
    requested_thoughts: HashSet<Id>,
    model_results: HashMap<Id, ModelResultEntry>,
    command_requests: HashMap<Id, CoreCommandRequest>,
    command_request_for_thought: HashMap<Id, Id>,
    command_done_requests: HashSet<Id>,
    command_results: HashMap<Id, CommandResultInfo>,
    used_exec_results: HashSet<Id>,
    reason_events: HashMap<Id, CoreReasonEvent>,
    reason_event_ids_by_turn: HashMap<Id, HashSet<Id>>,
}

fn ensure_model_request(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    config: &Config,
) -> Result<ModelRequestInfo> {
    let mut cached_head = None;
    let mut cached_catalog = TribleSet::new();
    let mut core_index = CoreIndex::default();
    loop {
        let branch_head = current_branch_head(repo, branch_id)?;
        if branch_head == cached_head {
            sleep(Duration::from_millis(config.poll_ms));
            continue;
        }

        let mut ws = pull_workspace(repo, branch_id, "pull workspace for model request")?;
        let delta = refresh_cached_checkout(&mut ws, &mut cached_head, &mut cached_catalog)?;
        core_index.apply_delta(&cached_catalog, &delta);

        if let Some(request) = core_index.latest_pending_model_request() {
            return Ok(request);
        }

        if let Some(thought_id) = core_index.latest_unrequested_thought() {
            let request_id =
                create_request_for_thought_from_index(&mut ws, &core_index, thought_id, config)?;
            push_workspace(repo, &mut ws).context("push model request")?;
            return Ok(ModelRequestInfo {
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
) -> Result<ModelRequestInfo> {
    let mut ws = pull_workspace(repo, branch_id, "pull workspace for thought")?;
    let catalog = ws.checkout(..).context("checkout workspace")?;
    let mut core_index = CoreIndex::default();
    core_index.apply_delta(&catalog, &catalog);

    if let Some(exec_result_id) = about_exec_result {
        if let Some(thought_id) = core_index.thought_for_exec_result(exec_result_id) {
            let request_id =
                create_request_for_thought_from_index(&mut ws, &core_index, thought_id, config)?;
            push_workspace(repo, &mut ws).context("push model request")?;
            return Ok(ModelRequestInfo {
                id: request_id,
                thought_id: Some(thought_id),
            });
        }
    }

    let now = epoch_interval(now_epoch());
    let needs_archive_seed = !has_archive_context_chunks(&catalog);
    let archive_catalog = if needs_archive_seed {
        load_optional_catalog(
            repo,
            config.archive_branch_id,
            "pull archive workspace for context history",
        )?
    } else {
        TribleSet::new()
    };
    let relations_catalog = if needs_archive_seed {
        load_optional_catalog(
            repo,
            config.relations_branch_id,
            "pull relations workspace for context history",
        )?
    } else {
        TribleSet::new()
    };
    let (context_json, compact_change) = if let Some(exec_result_id) = about_exec_result {
        context_for_exec_result_with_history(
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
                .context("serialize context messages")?,
            TribleSet::new(),
        )
    };
    let context_handle = ws.put(context_json);
    let thought_id = ufoid();
    let mut change = TribleSet::new();
    change += compact_change;
    change += entity! { &thought_id @
        playground_cog::kind: playground_cog::kind_thought,
        playground_cog::context: context_handle,
        playground_cog::created_at: now,
    };
    if let Some(exec_result_id) = about_exec_result {
        change += entity! { &thought_id @ playground_cog::about_exec_result: exec_result_id };
    }

    let request_id = ufoid();
    change += entity! { &request_id @
        model_chat::kind: model_chat::kind_request,
        model_chat::about_thought: *thought_id,
        model_chat::context: context_handle,
        model_chat::requested_at: now,
        model_chat::model: config.model.model.as_str(),
    };

    ws.commit(change, None, Some("create thought + model request"));
    push_workspace(repo, &mut ws).context("push thought + request")?;

    Ok(ModelRequestInfo {
        id: *request_id,
        thought_id: Some(*thought_id),
    })
}

fn has_archive_context_chunks(catalog: &TribleSet) -> bool {
    find!(
        (chunk_id: Id),
        pattern!(catalog, [{ ?chunk_id @ playground_context::about_archive_message: _?message_id }])
    )
    .into_iter()
    .next()
    .is_some()
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

    let Some(context_handle) = core_index.thought_context_handle(thought_id) else {
        return Err(anyhow!("thought {thought_id:x} missing context"));
    };

    let now = epoch_interval(now_epoch());
    let request_id = ufoid();
    let mut change = TribleSet::new();
    change += entity! { &request_id @
        model_chat::kind: model_chat::kind_request,
        model_chat::about_thought: thought_id,
        model_chat::context: context_handle,
        model_chat::requested_at: now,
        model_chat::model: config.model.model.as_str(),
    };
    ws.commit(change, None, Some("create model request"));
    Ok(*request_id)
}

#[derive(Debug)]
struct ModelResult {
    output_text: String,
    error: Option<String>,
}

#[derive(Debug, Clone)]
struct ModelResultInfo {
    id: Id,
    output_text: Option<Value<Handle<Blake3, LongString>>>,
    reasoning_text: Option<Value<Handle<Blake3, LongString>>>,
    error: Option<Value<Handle<Blake3, LongString>>>,
}

fn wait_for_model_result(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    request_id: Id,
    poll_ms: u64,
) -> Result<ModelResult> {
    let mut cached_head = None;
    let mut cached_catalog = TribleSet::new();
    let mut core_index = CoreIndex::default();
    loop {
        let branch_head = current_branch_head(repo, branch_id)?;
        if branch_head == cached_head {
            sleep(Duration::from_millis(poll_ms));
            continue;
        }

        let mut ws = pull_workspace(repo, branch_id, "pull workspace for model result")?;
        let delta = refresh_cached_checkout(&mut ws, &mut cached_head, &mut cached_catalog)?;
        core_index.apply_delta(&cached_catalog, &delta);
        if !delta_has_model_result(&cached_catalog, &delta, request_id) {
            sleep(Duration::from_millis(poll_ms));
            continue;
        }
        if let Some(result) = core_index.latest_model_result(request_id) {
            return load_model_result(&mut ws, result);
        }
        sleep(Duration::from_millis(poll_ms));
    }
}

fn load_model_result(ws: &mut Workspace<Pile>, result: ModelResultInfo) -> Result<ModelResult> {
    let output_text = result
        .output_text
        .map(|handle| load_text(ws, handle))
        .transpose()?
        .unwrap_or_default();
    let error = result
        .error
        .map(|handle| load_text(ws, handle))
        .transpose()?;

    Ok(ModelResult { output_text, error })
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
                ?request_id @ model_chat::kind: model_chat::kind_request
            }])
        ) {
            self.model_requests
                .entry(request_id)
                .or_insert(CoreModelRequest {
                    id: request_id,
                    requested_at: None,
                    thought_id: None,
                });
        }

        for (request_id, requested_at) in find!(
            (request_id: Id, requested_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?request_id @ model_chat::requested_at: ?requested_at
            }])
        ) {
            if let Some(entry) = self.model_requests.get_mut(&request_id) {
                entry.requested_at = Some(requested_at);
            }
        }

        for (request_id, thought_id) in find!(
            (request_id: Id, thought_id: Id),
            pattern_changes!(updated, delta, [{
                ?request_id @ model_chat::about_thought: ?thought_id
            }])
        ) {
            if let Some(entry) = self.model_requests.get_mut(&request_id) {
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
                context: None,
            });
        }

        for (boundary_id,) in find!(
            (boundary_id: Id),
            pattern_changes!(updated, delta, [{
                ?boundary_id @ playground_cog::kind: playground_cog::kind_moment_boundary
            }])
        ) {
            self.moment_boundaries
                .entry(boundary_id)
                .or_insert(CoreMomentBoundary {
                    id: boundary_id,
                    created_at: None,
                    turn_id: None,
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
            if let Some(entry) = self.moment_boundaries.get_mut(&thought_id) {
                entry.created_at = Some(created_at);
            }
        }

        for (thought_id, context) in find!(
            (thought_id: Id, context: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?thought_id @ playground_cog::context: ?context
            }])
        ) {
            if let Some(entry) = self.thoughts.get_mut(&thought_id) {
                entry.context = Some(context);
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

        for (boundary_id, turn_id) in find!(
            (boundary_id: Id, turn_id: Id),
            pattern_changes!(updated, delta, [{
                ?boundary_id @ playground_cog::moment_boundary_turn_id: ?turn_id
            }])
        ) {
            if let Some(entry) = self.moment_boundaries.get_mut(&boundary_id) {
                entry.turn_id = Some(turn_id);
            }
        }

        for (result_id, about_request) in find!(
            (result_id: Id, about_request: Id),
            pattern_changes!(updated, delta, [{
                ?result_id @
                model_chat::kind: model_chat::kind_result,
                model_chat::about_request: ?about_request,
            }])
        ) {
            self.model_done_requests.insert(about_request);
            let entry = self.model_results.entry(result_id).or_insert(ModelResultEntry {
                id: result_id,
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
                ?result_id @ model_chat::finished_at: ?finished_at
            }])
        ) {
            if let Some(entry) = self.model_results.get_mut(&result_id) {
                entry.finished_at = Some(finished_at);
            }
        }

        for (result_id, attempt) in find!(
            (result_id: Id, attempt: Value<U256BE>),
            pattern_changes!(updated, delta, [{
                ?result_id @ model_chat::attempt: ?attempt
            }])
        ) {
            if let Some(entry) = self.model_results.get_mut(&result_id) {
                entry.attempt = Some(attempt);
            }
        }

        for (result_id, output_text) in find!(
            (result_id: Id, output_text: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ model_chat::output_text: ?output_text
            }])
        ) {
            if let Some(entry) = self.model_results.get_mut(&result_id) {
                entry.output_text = Some(output_text);
            }
        }

        for (result_id, reasoning_text) in find!(
            (result_id: Id, reasoning_text: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ model_chat::reasoning_text: ?reasoning_text
            }])
        ) {
            if let Some(entry) = self.model_results.get_mut(&result_id) {
                entry.reasoning_text = Some(reasoning_text);
            }
        }

        for (result_id, error) in find!(
            (result_id: Id, error: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?result_id @ model_chat::error: ?error
            }])
        ) {
            if let Some(entry) = self.model_results.get_mut(&result_id) {
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

        for (reason_id, text) in find!(
            (reason_id: Id, text: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?reason_id @ reason_events::text: ?text
            }])
        ) {
            let entry = self.reason_events.entry(reason_id).or_insert(CoreReasonEvent {
                id: reason_id,
                about_turn: None,
                created_at: None,
                text: None,
                command_text: None,
                worker: None,
            });
            entry.text = Some(text);
        }

        for (reason_id, created_at) in find!(
            (reason_id: Id, created_at: Value<NsTAIInterval>),
            pattern_changes!(updated, delta, [{
                ?reason_id @ reason_events::created_at: ?created_at
            }])
        ) {
            let entry = self.reason_events.entry(reason_id).or_insert(CoreReasonEvent {
                id: reason_id,
                about_turn: None,
                created_at: None,
                text: None,
                command_text: None,
                worker: None,
            });
            entry.created_at = Some(created_at);
        }

        for (reason_id, turn_id) in find!(
            (reason_id: Id, turn_id: Id),
            pattern_changes!(updated, delta, [{
                ?reason_id @ reason_events::about_turn: ?turn_id
            }])
        ) {
            let entry = self.reason_events.entry(reason_id).or_insert(CoreReasonEvent {
                id: reason_id,
                about_turn: None,
                created_at: None,
                text: None,
                command_text: None,
                worker: None,
            });
            if let Some(previous_turn) = entry.about_turn {
                if previous_turn != turn_id {
                    if let Some(ids) = self.reason_event_ids_by_turn.get_mut(&previous_turn) {
                        ids.remove(&reason_id);
                        if ids.is_empty() {
                            self.reason_event_ids_by_turn.remove(&previous_turn);
                        }
                    }
                }
            }
            entry.about_turn = Some(turn_id);
            self.reason_event_ids_by_turn
                .entry(turn_id)
                .or_default()
                .insert(reason_id);
        }

        for (reason_id, command_text) in find!(
            (reason_id: Id, command_text: Value<Handle<Blake3, LongString>>),
            pattern_changes!(updated, delta, [{
                ?reason_id @ reason_events::command_text: ?command_text
            }])
        ) {
            let entry = self.reason_events.entry(reason_id).or_insert(CoreReasonEvent {
                id: reason_id,
                about_turn: None,
                created_at: None,
                text: None,
                command_text: None,
                worker: None,
            });
            entry.command_text = Some(command_text);
        }

        for (reason_id, worker_id) in find!(
            (reason_id: Id, worker_id: Id),
            pattern_changes!(updated, delta, [{
                ?reason_id @ reason_events::worker: ?worker_id
            }])
        ) {
            let entry = self.reason_events.entry(reason_id).or_insert(CoreReasonEvent {
                id: reason_id,
                about_turn: None,
                created_at: None,
                text: None,
                command_text: None,
                worker: None,
            });
            entry.worker = Some(worker_id);
        }
    }

    fn latest_pending_model_request(&self) -> Option<ModelRequestInfo> {
        let mut candidates: Vec<CoreModelRequest> = self
            .model_requests
            .values()
            .filter(|request| !self.model_done_requests.contains(&request.id))
            .cloned()
            .collect();
        candidates.sort_by_key(|request| {
            (
                request.requested_at.map(interval_key).unwrap_or(i128::MIN),
                request.id,
            )
        });
        candidates.pop().map(|request| ModelRequestInfo {
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

    fn thought_context_handle(&self, thought_id: Id) -> Option<Value<Handle<Blake3, LongString>>> {
        self.thoughts
            .get(&thought_id)
            .and_then(|thought| thought.context)
    }

    fn latest_model_result(&self, request_id: Id) -> Option<ModelResultInfo> {
        self.model_results
            .values()
            .filter(|result| result.about_request == Some(request_id))
            .max_by_key(|result| model_result_rank(result.attempt, result.finished_at))
            .map(|result| ModelResultInfo {
                id: result.id,
                output_text: result.output_text,
                reasoning_text: result.reasoning_text,
                error: result.error,
            })
    }

    fn reason_events_for_turn(&self, turn_id: Id) -> Vec<CoreReasonEvent> {
        let Some(ids) = self.reason_event_ids_by_turn.get(&turn_id) else {
            return Vec::new();
        };
        let mut events: Vec<CoreReasonEvent> = ids
            .iter()
            .filter_map(|id| self.reason_events.get(id).cloned())
            .collect();
        events.sort_by_key(|event| (event.created_at.map(interval_key).unwrap_or(i128::MIN), event.id));
        events
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

    fn latest_moment_boundary_turn_id(&self) -> Option<Id> {
        self.moment_boundaries
            .values()
            .filter_map(|entry| {
                let created = entry.created_at.map(interval_key)?;
                let turn_id = entry.turn_id?;
                Some((created, entry.id, turn_id))
            })
            .max_by_key(|(created, boundary_id, _)| (*created, *boundary_id))
            .map(|(_, _, turn_id)| turn_id)
    }

    fn has_finished_command_result(&self, turn_id: Id) -> bool {
        self.command_results
            .get(&turn_id)
            .is_some_and(|result| result.finished_at.is_some())
    }
}

fn model_result_rank(
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

fn delta_has_model_result(updated: &TribleSet, delta: &TribleSet, request_id: Id) -> bool {
    find!(
        (about_request: Id),
        pattern_changes!(updated, delta, [{
            _?event @
            model_chat::kind: model_chat::kind_result,
            model_chat::about_request: ?about_request,
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

#[derive(Debug)]
struct BranchDiscovery {
    commit_count: usize,
    payload_commits: Vec<Value<Handle<Blake3, SimpleArchive>>>,
}

fn discover_branch_payload_commits(
    ws: &mut Workspace<Pile>,
    head: Value<Handle<Blake3, SimpleArchive>>,
    label: &str,
) -> Result<BranchDiscovery> {
    let mut stack = vec![head];
    let mut seen_commits = HashSet::new();
    let mut seen_payloads = HashSet::new();
    let mut commit_count = 0usize;
    let mut payload_commits = Vec::new();
    while let Some(commit) = stack.pop() {
        if !seen_commits.insert(commit) {
            continue;
        }
        commit_count += 1;
        if commit_count % 2_000 == 0 {
            memory_status(format!(
                "{label}: discovered {} commit(s) so far...",
                commit_count
            ));
        }

        let meta: TribleSet = ws.get(commit).context("read commit metadata")?;
        let mut content_iter = find!((c: Value<_>), pattern!(&meta, [{ commit_content: ?c }]));
        let content_opt = content_iter.next().map(|(c,)| c);
        if content_iter.next().is_some() {
            return Err(anyhow!("bad commit metadata: multiple content handles"));
        }
        if let Some(content_handle) = content_opt {
            if seen_payloads.insert(content_handle) {
                payload_commits.push(commit);
            }
        }

        for (parent,) in find!(
            (parent: Value<_>,),
            pattern!(&meta, [{ commit_parent: ?parent }])
        ) {
            stack.push(parent);
        }
    }
    Ok(BranchDiscovery {
        commit_count,
        payload_commits,
    })
}

fn load_archive_messages_incremental(
    repo: &mut Repository<Pile>,
    branch_id: Option<Id>,
    context: &str,
    label: &str,
) -> Result<ArchiveLoadResult> {
    let Some(branch_id) = branch_id else {
        return Ok(ArchiveLoadResult::default());
    };
    let mut ws = pull_workspace(repo, branch_id, context)?;
    let Some(head) = ws.head() else {
        memory_status(format!("{label}: branch is empty."));
        return Ok(ArchiveLoadResult::default());
    };

    let discover_started = Instant::now();
    memory_status(format!(
        "{label}: discovering reachable commits from head {:?}...",
        head
    ));
    let discovery = discover_branch_payload_commits(&mut ws, head, label)?;
    memory_status_timed(
        &format!(
            "{label}: discovered {} commit(s), {} unique payload commit(s)",
            discovery.commit_count,
            discovery.payload_commits.len()
        ),
        discover_started,
    );

    let mut payload_commits = discovery.payload_commits;
    if payload_commits.is_empty() {
        return Ok(ArchiveLoadResult::default());
    }
    // Process oldest payloads first so progress and growth are easier to read in checkpoints.
    payload_commits.reverse();

    let mut projection_catalog = TribleSet::new();
    let mut projection_counts = ArchiveProjectionCounts::default();
    let total_commits = payload_commits.len();
    let checkout_started = Instant::now();
    memory_status(format!(
        "{label}: scanning payload commit-by-commit ({total_commits} unique payload commit(s))..."
    ));
    for (commit_idx, commit) in payload_commits.into_iter().enumerate() {
        let commit_num = commit_idx + 1;
        let is_checkpoint = commit_idx == 0
            || commit_num % MEMORY_PROGRESS_CHECKPOINT_EVERY == 0
            || commit_num == total_commits;
        if is_checkpoint {
            memory_status(format!(
                "{label}: checkpoint before commit {commit_num}/{total_commits}..."
            ));
        }

        let one = [commit];
        let delta = ws
            .checkout(&one[..])
            .with_context(|| format!("checkout {label} commit {commit_num}"))?;
        let commit_tribles = delta.len();
        let projection_delta = filter_archive_projection_delta(&delta);
        if !projection_delta.is_empty() {
            projection_catalog += projection_delta;
        }
        if is_checkpoint {
            let current_counts = archive_projection_counts(&projection_catalog);
            let new_message_facts = current_counts
                .message_facts
                .saturating_sub(projection_counts.message_facts);
            let new_reply_links = current_counts
                .reply_links
                .saturating_sub(projection_counts.reply_links);
            let new_message_batch_links = current_counts
                .import_links
                .saturating_sub(projection_counts.import_links);
            memory_status(format!(
                "{label}: scanned commit {commit_num}/{total_commits} ({} tribles, message facts {} (+{}), reply links {} (+{}), import links {} (+{}))",
                commit_tribles,
                current_counts.message_facts,
                new_message_facts,
                current_counts.reply_links,
                new_reply_links,
                current_counts.import_links,
                new_message_batch_links
            ));
            projection_counts = current_counts;
        }
    }
    memory_status_timed(&format!("{label}: payload scan complete"), checkout_started);

    let coverage = archive_coverage_report(&projection_catalog);
    if coverage.strict_imported_total < coverage.imported_message_total {
        return Err(anyhow!(
            "{label}: archive coverage mismatch: strict imported pattern matches {}/{} imported message(s)",
            coverage.strict_imported_total,
            coverage.imported_message_total
        ));
    }
    let messages = load_archive_messages(&projection_catalog)?;
    Ok(ArchiveLoadResult { messages, coverage })
}

#[derive(Debug, Clone)]
struct ContextChunk {
    id: Id,
    lens_id: Id,
    level: u64,
    summary: Value<Handle<Blake3, LongString>>,
    start_at: Value<NsTAIInterval>,
    end_at: Value<NsTAIInterval>,
    children: Vec<Id>,
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
    // The LSM frontier for each lens: roots grouped by level and ordered by time.
    roots_by_lens_level: HashMap<(Id, u64), Vec<Id>>,
    // Leaf chunks tie a single exec result facet to a compacted chunk.
    chunk_for_exec_result: HashMap<(Id, Id), Id>,
    // Leaf chunks tie a single imported archive message facet to a compacted chunk.
    chunk_for_archive_message: HashMap<(Id, Id), Id>,
}

#[derive(Debug, Clone)]
struct ArchiveMessageInfo {
    id: Id,
    author_id: Id,
    author_name: Option<Value<Handle<Blake3, LongString>>>,
    content: Value<Handle<Blake3, LongString>>,
    created_at: Value<NsTAIInterval>,
    thread_root_id: Id,
    conversation_id: Value<Handle<Blake3, LongString>>,
    source_format: String,
    source_message_id: Value<Handle<Blake3, LongString>>,
    source_author: Value<Handle<Blake3, LongString>>,
    source_role: Value<Handle<Blake3, LongString>>,
}

#[derive(Debug, Clone, Default)]
struct ArchiveCoverageReport {
    kind_message_total: usize,
    imported_message_total: usize,
    strict_imported_total: usize,
}

impl ArchiveCoverageReport {
    fn strict_imported_pct(&self) -> f64 {
        if self.imported_message_total == 0 {
            return 100.0;
        }
        (self.strict_imported_total as f64) * 100.0 / (self.imported_message_total as f64)
    }

}

#[derive(Debug, Clone, Default)]
struct ArchiveLoadResult {
    messages: Vec<ArchiveMessageInfo>,
    coverage: ArchiveCoverageReport,
}

#[derive(Default)]
struct RelationsIndex {
    key_to_person_id: HashMap<String, Id>,
    person_label: HashMap<Id, String>,
}

#[derive(Debug, Default, Clone)]
struct CompactionRunStats {
    archive_leaves_added: usize,
    exec_leaves_added: usize,
    merge_calls: usize,
    merged_children_total: usize,
    merge_input_chars_total: usize,
    merge_output_chars_total: usize,
}

fn sorted_finished_command_results(core_index: &CoreIndex) -> Vec<CommandResultInfo> {
    let mut results: Vec<CommandResultInfo> =
        core_index.command_results.values().cloned().collect();
    results.sort_by_key(|result| result.finished_at.map(interval_key).unwrap_or(i128::MIN));
    results.retain(|result| result.finished_at.is_some());
    results
}

fn ingest_archive_context_chunks(
    ws: &mut Workspace<Pile>,
    index: &mut ContextChunkIndex,
    change: &mut TribleSet,
    archive_messages: &[ArchiveMessageInfo],
    relations: &RelationsIndex,
    max_created_at_key: Option<i128>,
    max_new: Option<usize>,
    merge_arity: usize,
    semantic_compactor: &SemanticCompactor,
    stats: &mut CompactionRunStats,
    progress_every: Option<usize>,
) -> Result<usize> {
    let started_at = Instant::now();
    let mut added = 0usize;
    let mut seen = 0usize;
    let mut skipped_existing = 0usize;
    let mut skipped_newer_than_cutoff = 0usize;
    for message in archive_messages {
        seen = seen.saturating_add(1);
        if max_new.is_some_and(|limit| added >= limit) {
            break;
        }
        if max_created_at_key.is_some_and(|max_key| interval_key(message.created_at) > max_key) {
            skipped_newer_than_cutoff = skipped_newer_than_cutoff.saturating_add(1);
            continue;
        }
        let missing_lenses: Vec<&MemoryLensConfig> = semantic_compactor
            .lenses()
            .iter()
            .filter(|lens| {
                !index
                    .chunk_for_archive_message
                    .contains_key(&(message.id, lens.id))
            })
            .collect();
        if missing_lenses.is_empty() {
            skipped_existing = skipped_existing.saturating_add(1);
            continue;
        }

        let author_name = load_optional_text(ws, message.author_name)?;
        let source_author = load_text(ws, message.source_author)?;
        let source_role = load_text(ws, message.source_role)?;
        let source_message_id = load_text(ws, message.source_message_id)?;
        let conversation_id = load_text(ws, message.conversation_id)?;
        let content = load_text(ws, message.content).context("load archive message content")?;
        let resolved_person = resolve_archive_person(
            relations,
            author_name.as_deref(),
            Some(source_author.as_str()),
        );
        let leaf_summary = format_archive_output(
            message,
            author_name.as_deref(),
            source_author.as_str(),
            source_role.as_str(),
            source_message_id.as_str(),
            conversation_id.as_str(),
            content.as_str(),
            resolved_person
                .and_then(|person_id| relations.person_label.get(&person_id).map(|s| s.as_str())),
            resolved_person,
        );
        for lens in missing_lenses {
            let leaf_summary_handle = ws.put(leaf_summary.clone());
            let now = epoch_interval(now_epoch());
            let chunk_id = ufoid();

            *change += entity! { &chunk_id @
                playground_context::kind: playground_context::kind_chunk,
                playground_context::lens_id: lens.id,
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
                *change += entity! { &chunk_id @ playground_context::archive_person: person_id };
            }
            *change += entity! { &chunk_id @
                playground_context::archive_conversation: message.conversation_id,
                playground_context::archive_source_format: message.source_format.as_str(),
            };

            let chunk = ContextChunk {
                id: *chunk_id,
                lens_id: lens.id,
                level: 0,
                summary: leaf_summary_handle,
                start_at: message.created_at,
                end_at: message.created_at,
                children: Vec::new(),
                about_exec_result: None,
                about_archive_message: Some(message.id),
                archive_author: Some(message.author_id),
                archive_person: resolved_person,
                archive_thread_root: Some(message.thread_root_id),
                archive_conversation: Some(message.conversation_id),
                archive_source_format: Some(message.source_format.clone()),
            };
            index
                .chunk_for_archive_message
                .insert((message.id, lens.id), chunk.id);
            insert_chunk_with_carry(
                ws,
                index,
                change,
                chunk,
                lens,
                merge_arity,
                semantic_compactor,
                stats,
            )?;
            stats.archive_leaves_added = stats.archive_leaves_added.saturating_add(1);
            added = added.saturating_add(1);
        }
        if progress_every.is_some_and(|step| step > 0 && added % step == 0) {
            memory_status(format!(
                "archive progress: added {added} chunk(s) (scanned {seen})"
            ));
        }
    }
    if progress_every.is_some() {
        memory_status(format!(
            "archive ingest finished: added {added} chunk(s), skipped existing {skipped_existing}, skipped cutoff {skipped_newer_than_cutoff} (scanned {seen}) in {}",
            format_elapsed(started_at.elapsed())
        ));
    }
    Ok(added)
}

fn ingest_exec_context_chunks(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    index: &mut ContextChunkIndex,
    change: &mut TribleSet,
    exec_results: &[CommandResultInfo],
    max_new: Option<usize>,
    merge_arity: usize,
    semantic_compactor: &SemanticCompactor,
    stats: &mut CompactionRunStats,
    progress_every: Option<usize>,
) -> Result<usize> {
    let started_at = Instant::now();
    let mut added = 0usize;
    let mut seen = 0usize;
    let mut skipped_existing = 0usize;
    for result in exec_results {
        seen = seen.saturating_add(1);
        if max_new.is_some_and(|limit| added >= limit) {
            break;
        }
        let missing_lenses: Vec<&MemoryLensConfig> = semantic_compactor
            .lenses()
            .iter()
            .filter(|lens| {
                !index
                    .chunk_for_exec_result
                    .contains_key(&(result.id, lens.id))
            })
            .collect();
        if missing_lenses.is_empty() {
            skipped_existing = skipped_existing.saturating_add(1);
            continue;
        }
        let finished_at = result
            .finished_at
            .context("command result missing finished_at")?;
        let turn_projection = load_exec_turn_projection(ws, core_index, result)?;
        let exec_output = load_exec_result(ws, result.clone())?;
        let leaf_outputs = format_exec_outputs_by_lens(
            result.id,
            turn_projection.command.as_str(),
            exec_output,
            turn_projection.reason_events.as_slice(),
            semantic_compactor,
        )?;
        let leaf_output_by_lens: HashMap<Id, String> = leaf_outputs.into_iter().collect();
        for lens in missing_lenses {
            let Some(leaf_summary) = leaf_output_by_lens.get(&lens.id) else {
                continue;
            };
            let leaf_summary_handle = ws.put(leaf_summary.clone());
            let now = epoch_interval(now_epoch());
            let chunk_id = ufoid();

            *change += entity! { &chunk_id @
                playground_context::kind: playground_context::kind_chunk,
                playground_context::lens_id: lens.id,
                playground_context::level: 0u64,
                playground_context::summary: leaf_summary_handle,
                playground_context::created_at: now,
                playground_context::start_at: finished_at,
                playground_context::end_at: finished_at,
                playground_context::about_exec_result: result.id,
            };

            let chunk = ContextChunk {
                id: *chunk_id,
                lens_id: lens.id,
                level: 0,
                summary: leaf_summary_handle,
                start_at: finished_at,
                end_at: finished_at,
                children: Vec::new(),
                about_exec_result: Some(result.id),
                about_archive_message: None,
                archive_author: None,
                archive_person: None,
                archive_thread_root: None,
                archive_conversation: None,
                archive_source_format: None,
            };
            index
                .chunk_for_exec_result
                .insert((result.id, lens.id), chunk.id);
            insert_chunk_with_carry(
                ws,
                index,
                change,
                chunk,
                lens,
                merge_arity,
                semantic_compactor,
                stats,
            )?;
            stats.exec_leaves_added = stats.exec_leaves_added.saturating_add(1);
            added = added.saturating_add(1);
        }
        if progress_every.is_some_and(|step| step > 0 && added % step == 0) {
            memory_status(format!(
                "exec progress: added {added} chunk(s) (scanned {seen})"
            ));
        }
    }
    if progress_every.is_some() {
        memory_status(format!(
            "exec ingest finished: added {added} chunk(s), skipped existing {skipped_existing} (scanned {seen}) in {}",
            format_elapsed(started_at.elapsed())
        ));
    }
    Ok(added)
}

fn context_for_exec_result_with_history(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    catalog: &TribleSet,
    archive_catalog: &TribleSet,
    relations_catalog: &TribleSet,
    exec_result_id: Id,
    config: &Config,
) -> Result<(String, TribleSet)> {
    let (mut messages, compact_change) = build_context_messages_with_compaction(
        ws,
        core_index,
        catalog,
        archive_catalog,
        relations_catalog,
        exec_result_id,
        config,
    )?;
    messages.insert(0, ChatMessage::system(config.system_prompt.clone()));
    let context_json = serde_json::to_string(&messages).context("serialize context messages")?;
    Ok((context_json, compact_change))
}

fn build_context_messages_with_compaction(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    catalog: &TribleSet,
    archive_catalog: &TribleSet,
    relations_catalog: &TribleSet,
    exec_result_id: Id,
    config: &Config,
) -> Result<(Vec<ChatMessage>, TribleSet)> {
    let mut index = load_context_chunks(catalog);
    let body_budget_chars = context_body_budget_chars(config);
    let semantic_compactor = SemanticCompactor::new(config)?;
    let merge_arity = config.memory_compaction_arity as usize;
    let primary_lens_id = config
        .memory_lenses
        .first()
        .map(|lens| lens.id)
        .ok_or_else(|| anyhow!("no configured memory lenses"))?;

    // Sort all command results in chronological order (oldest -> newest).
    let results = sorted_finished_command_results(core_index);

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

    let mut compact_change = TribleSet::new();
    let mut compaction_stats = CompactionRunStats::default();
    if index.chunk_for_archive_message.is_empty() && !archive_catalog.is_empty() {
        let relations = load_relations_index(ws, relations_catalog)?;
        let archive_messages = load_archive_messages(archive_catalog)?;
        ingest_archive_context_chunks(
            ws,
            &mut index,
            &mut compact_change,
            archive_messages.as_slice(),
            &relations,
            Some(current_finished_key),
            None,
            merge_arity,
            &semantic_compactor,
            &mut compaction_stats,
            None,
        )?;
    }
    ingest_exec_context_chunks(
        ws,
        core_index,
        &mut index,
        &mut compact_change,
        results.as_slice(),
        None,
        merge_arity,
        &semantic_compactor,
        &mut compaction_stats,
        None,
    )?;

    let moment_boundary_end_key = resolve_moment_boundary_end_key(
        results.as_slice(),
        core_index.latest_moment_boundary_turn_id(),
    );
    let (mut messages, used_chars, breath_idx) = build_memory_cover_messages(
        ws,
        &index,
        primary_lens_id,
        body_budget_chars,
        moment_boundary_end_key,
    )?;

    // Insert breath boundary between memory and moment segments.
    if !messages.is_empty() {
        let fill_pct = if body_budget_chars > 0 {
            (used_chars * 100) / body_budget_chars
        } else {
            0
        };
        let breath_output = format!(
            "context filled to {fill_pct}%. present moment begins."
        );
        messages.insert(breath_idx, ChatMessage::user(breath_output));
        messages.insert(breath_idx, ChatMessage::assistant("breath".to_string()));
    }

    // Project post-boundary exec results as raw shell interaction turns.
    if let Some(boundary) = moment_boundary_end_key {
        for result in &results {
            let Some(finished_at) = result.finished_at else {
                continue;
            };
            if interval_key(finished_at) <= boundary {
                continue;
            }
            let projection = load_exec_turn_projection(ws, core_index, result)?;
            let exec_output = load_exec_result(ws, result.clone())?;

            for event in &projection.reason_events {
                if should_project_reason_event(event) {
                    messages.push(ChatMessage::assistant(synthetic_reason_command(&event.text)));
                    messages.push(ChatMessage::user(synthetic_reason_output(event)));
                }
            }

            messages.push(ChatMessage::assistant(projection.command));
            messages.push(ChatMessage::user(format_moment_output(&exec_output)));
        }
    }

    if let Some(guard) = memory_loop_guard_message(ws, core_index, results.as_slice(), current_pos)?
    {
        messages.push(ChatMessage::user(guard));
    }
    Ok((messages, compact_change))
}

fn context_body_budget_chars(config: &Config) -> usize {
    // This is an intentionally cheap heuristic: we approximate tokens->chars and reserve space
    // for model output plus a small safety margin.
    let reserved = config
        .model
        .max_output_tokens
        .saturating_add(config.model.context_safety_margin_tokens);
    let input_tokens = config.model.context_window_tokens.saturating_sub(reserved);
    let chars_per_token = config.model.chars_per_token.max(1);

    let input_chars = u128_to_usize_saturating((input_tokens as u128) * (chars_per_token as u128));
    let system_chars = config.system_prompt.chars().count();
    input_chars.saturating_sub(system_chars)
}

fn u128_to_usize_saturating(value: u128) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn resolve_moment_boundary_end_key(
    results: &[CommandResultInfo],
    moment_boundary_turn_id: Option<Id>,
) -> Option<i128> {
    let target = moment_boundary_turn_id?;
    results.iter().find_map(|result| {
        if result.id != target {
            return None;
        }
        result.finished_at.map(interval_key)
    })
}

#[derive(Debug, Clone)]
struct MemoryCoverTurn {
    command: String,
    output: String,
    cost: usize,
}

#[derive(Debug, Clone)]
struct SplitCandidate {
    index: usize,
    parent_id: Id,
    child_ids: Vec<Id>,
    extra_cost: usize,
    recency_key: i128,
}

/// Returns (messages, used_chars, breath_insert_index).
fn build_memory_cover_messages(
    ws: &mut Workspace<Pile>,
    index: &ContextChunkIndex,
    lens_id: Id,
    budget_chars: usize,
    moment_boundary_end_key: Option<i128>,
) -> Result<(Vec<ChatMessage>, usize, usize)> {
    if budget_chars == 0 {
        return Ok((Vec::new(), 0, 0));
    }

    let mut seen_roots = HashSet::new();
    let mut cover: Vec<Id> = index
        .roots_by_lens_level
        .values()
        .flatten()
        .copied()
        .filter(|id| {
            index
                .chunks
                .get(id)
                .is_some_and(|chunk| chunk.lens_id == lens_id)
        })
        .filter(|id| seen_roots.insert(*id))
        .filter(|id| index.chunks.contains_key(id))
        // Exclude post-boundary chunks: moment turns are projected as raw shell
        // interaction, not as memory cover entries.
        .filter(|id| {
            moment_boundary_end_key.is_none_or(|boundary| {
                index
                    .chunks
                    .get(id)
                    .is_some_and(|chunk| interval_key(chunk.end_at) <= boundary)
            })
        })
        .collect();
    cover.sort_by_key(|id| {
        index
            .chunks
            .get(id)
            .map(|chunk| interval_key(chunk.start_at))
            .unwrap_or(i128::MAX)
    });
    if cover.is_empty() {
        return Ok((Vec::new(), 0, 0));
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
        return Ok((Vec::new(), 0, 0));
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
            if parent_chunk.children.len() < 2 {
                continue;
            }

            let parent_turn = memory_cover_turn(ws, index, &mut turn_cache, *parent_id)?;
            let mut children_cost = 0usize;
            for child_id in &parent_chunk.children {
                let child_turn = memory_cover_turn(ws, index, &mut turn_cache, *child_id)?;
                children_cost = children_cost.saturating_add(child_turn.cost);
            }
            let extra_cost = children_cost.saturating_sub(parent_turn.cost);
            if extra_cost > remaining {
                continue;
            }

            let candidate = SplitCandidate {
                index: cover_index,
                parent_id: *parent_id,
                child_ids: parent_chunk.children.clone(),
                extra_cost,
                recency_key: interval_key(parent_chunk.end_at),
            };
            if is_better_split_candidate(&candidate, best.as_ref()) {
                best = Some(candidate);
            }
        }

        let Some(candidate) = best else {
            break;
        };

        cover.splice(
            candidate.index..=candidate.index,
            candidate.child_ids.clone(),
        );
        used = used.saturating_add(candidate.extra_cost);
    }

    let mut messages = Vec::new();
    for chunk_id in cover {
        let turn = memory_cover_turn(ws, index, &mut turn_cache, chunk_id)?;
        messages.push(ChatMessage::assistant(turn.command.clone()));
        messages.push(ChatMessage::user(turn.output.clone()));
    }

    // All cover entries are memory (post-boundary chunks are excluded above),
    // so breath goes at the end.
    let breath_insert_index = messages.len();
    Ok((messages, used, breath_insert_index))
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
    let command = format!("memory {}", memory_ref(chunk.id));
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

fn is_better_split_candidate(candidate: &SplitCandidate, current: Option<&SplitCandidate>) -> bool {
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
    let mut header = format!(
        "mem {} lens={} lvl={}",
        memory_ref(chunk.id),
        id_prefix(chunk.lens_id),
        chunk.level
    );
    if let Some(exec_id) = chunk.about_exec_result {
        header.push_str(&format!(" turn_id={exec_id:x}"));
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
    if !chunk.children.is_empty() {
        header.push_str(" children=");
        for (idx, child_id) in chunk.children.iter().enumerate() {
            if idx > 0 {
                header.push(',');
            }
            header.push_str(memory_ref(*child_id).as_str());
        }
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

fn memory_ref(id: Id) -> String {
    let hex = format!("{id:x}");
    let end = usize::min(12, hex.len());
    hex[..end].to_string()
}

fn memory_command_id(command: &str) -> Option<&str> {
    let mut parts = command.split_whitespace();
    let first = parts.next()?;
    if first != "memory" && !first.ends_with("/memory.rs") && !first.ends_with("/memory") {
        return None;
    }
    parts.next()
}

fn memory_lookup_failed_text(stderr: &str, error: &str) -> bool {
    let failure_text = if error.is_empty() {
        stderr.to_string()
    } else if stderr.is_empty() {
        error.to_string()
    } else {
        format!("{stderr}\n{error}")
    };
    failure_text
        .to_ascii_lowercase()
        .contains("memory lookup failed")
}

fn memory_lookup_failed_result(command: &str, result: &ExecResult) -> bool {
    if memory_command_id(command).is_none() {
        return false;
    }

    let stderr = format_output_text(result.stderr_text.as_deref(), result.stderr.as_ref());
    let error = result.error.clone().unwrap_or_default();
    memory_lookup_failed_text(stderr.as_str(), error.as_str())
}

fn memory_loop_guard_message(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    results: &[CommandResultInfo],
    current_pos: usize,
) -> Result<Option<String>> {
    const MEMORY_FAILURE_LOOKBACK: usize = 3;

    let window_start = current_pos.saturating_sub(MEMORY_FAILURE_LOOKBACK - 1);
    let mut streak_len = 0usize;
    for result in results[window_start..=current_pos].iter().rev() {
        let command = load_command_for_result(ws, core_index, result)?;
        let Some(id_hint) = memory_command_id(command.as_str()) else {
            break;
        };
        let exec_output = load_exec_result(ws, result.clone())?;
        if !memory_lookup_failed_result(command.as_str(), &exec_output) {
            break;
        }
        streak_len = streak_len.saturating_add(1);
        if streak_len >= MEMORY_FAILURE_LOOKBACK {
            return Ok(Some(format!(
                "Memory lookup failed repeatedly on recent turns.\n\
                 Do not guess ids.\n\
                 Only run `memory <id>` for ids already visible in this context (`mem <id>` / `children=...`).\n\
                 If no valid id is available, stop memory lookups and run `orient show` or another concrete action.\n\
                 Last failed id hint: {id_hint}"
            )));
        }
    }
    Ok(None)
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
) -> Result<Option<(Id, String)>> {
    let Some(thought_id) = core_index.thought_for_command_request(exec_result.about_request) else {
        return Ok(None);
    };
    let Some(request_id) = core_index.request_for_thought(thought_id) else {
        return Ok(None);
    };
    let Some(result) = core_index.latest_model_result(request_id) else {
        return Ok(None);
    };
    let Some(reasoning_handle) = result.reasoning_text else {
        return Ok(None);
    };
    let reasoning_text = load_text(ws, reasoning_handle).context("load reasoning text")?;
    if reasoning_text.trim().is_empty() {
        return Ok(None);
    }
    Ok(Some((result.id, reasoning_text)))
}

fn archive_projection_attr_ids() -> [Id; 12] {
    [
        playground_archive::kind.id(),
        playground_archive::reply_to.id(),
        playground_archive::author.id(),
        playground_archive::author_name.id(),
        playground_archive::content.id(),
        playground_archive::created_at.id(),
        playground_archive_import::batch.id(),
        playground_archive_import::source_format.id(),
        playground_archive_import::source_conversation_id.id(),
        playground_archive_import::source_message_id.id(),
        playground_archive_import::source_author.id(),
        playground_archive_import::source_role.id(),
    ]
}

fn filter_archive_projection_delta(delta: &TribleSet) -> TribleSet {
    let attr_ids = archive_projection_attr_ids();
    let mut filtered = TribleSet::new();
    for trible in delta.iter() {
        if attr_ids.iter().any(|attr_id| attr_id == trible.a()) {
            filtered.insert(trible);
        }
    }
    filtered
}

#[derive(Debug, Clone, Copy, Default)]
struct ArchiveProjectionCounts {
    message_facts: usize,
    reply_links: usize,
    import_links: usize,
}

#[derive(Debug, Clone)]
struct CoreArchiveMessageRow {
    message_id: Id,
    author_id: Id,
    content: Value<Handle<Blake3, LongString>>,
    created_at: Value<NsTAIInterval>,
    batch_id: Id,
    source_format: String,
    conversation_id: Value<Handle<Blake3, LongString>>,
    source_message_id: Value<Handle<Blake3, LongString>>,
    source_author: Value<Handle<Blake3, LongString>>,
    source_role: Value<Handle<Blake3, LongString>>,
}

fn archive_projection_counts(catalog: &TribleSet) -> ArchiveProjectionCounts {
    let message_facts = find!(
        (message_id: Id),
        pattern!(catalog, [{
            ?message_id @
                playground_archive::kind: playground_archive::kind_message,
                playground_archive::author: _?author_id,
                playground_archive::content: _?content,
                playground_archive::created_at: _?created_at,
        }])
    )
    .collect::<HashSet<_>>()
    .len();

    let reply_links = find!(
        (message_id: Id),
        pattern!(catalog, [{
            ?message_id @ playground_archive::reply_to: _?parent_id,
        }])
    )
    .collect::<HashSet<_>>()
    .len();

    let import_links = find!(
        (message_id: Id),
        pattern!(catalog, [{
            ?message_id @ playground_archive_import::batch: _?batch_id,
        }])
    )
    .collect::<HashSet<_>>()
    .len();

    ArchiveProjectionCounts {
        message_facts,
        reply_links,
        import_links,
    }
}

fn load_archive_messages(catalog: &TribleSet) -> Result<Vec<ArchiveMessageInfo>> {
    let catalog = filter_archive_projection_delta(catalog);
    let mut reply_to = HashMap::new();
    for (message_id, parent_id) in find!(
        (message_id: Id, parent_id: Id),
        pattern!(&catalog, [{
            ?message_id @ playground_archive::reply_to: ?parent_id,
        }])
    ) {
        reply_to
            .entry(message_id)
            .and_modify(|current: &mut Id| {
                let current_key: [u8; 16] = (*current).into();
                let next_key: [u8; 16] = parent_id.into();
                if next_key < current_key {
                    *current = parent_id;
                }
            })
            .or_insert(parent_id);
    }

    let mut author_name_by_author = HashMap::new();
    for (author_id, author_name) in find!(
        (author_id: Id, author_name: Value<Handle<Blake3, LongString>>),
        pattern!(&catalog, [{
            ?author_id @ playground_archive::author_name: ?author_name,
        }])
    ) {
        author_name_by_author
            .entry(author_id)
            .or_insert(author_name);
    }

    let mut core_rows: Vec<CoreArchiveMessageRow> = find!(
        (
            message_id: Id,
            author_id: Id,
            content: Value<Handle<Blake3, LongString>>,
            created_at: Value<NsTAIInterval>,
            batch_id: Id,
            source_format: String,
            conversation_id: Value<Handle<Blake3, LongString>>,
            source_message_id: Value<Handle<Blake3, LongString>>,
            source_author: Value<Handle<Blake3, LongString>>,
            source_role: Value<Handle<Blake3, LongString>>
        ),
        pattern!(&catalog, [{
            ?message_id @
                playground_archive::kind: playground_archive::kind_message,
                playground_archive::author: ?author_id,
                playground_archive::content: ?content,
                playground_archive::created_at: ?created_at,
                playground_archive_import::batch: ?batch_id,
                playground_archive_import::source_message_id: ?source_message_id,
                playground_archive_import::source_author: ?source_author,
                playground_archive_import::source_role: ?source_role,
        },{
            ?batch_id @
                playground_archive_import::source_format: ?source_format,
                playground_archive_import::source_conversation_id: ?conversation_id,
        }])
    )
    .map(
        |(
            message_id,
            author_id,
            content,
            created_at,
            batch_id,
            source_format,
            conversation_id,
            source_message_id,
            source_author,
            source_role,
        )| CoreArchiveMessageRow {
            message_id,
            author_id,
            content,
            created_at,
            batch_id,
            source_format,
            conversation_id,
            source_message_id,
            source_author,
            source_role,
        },
    )
    .collect();

    core_rows.sort_by_key(|row| {
        let message_sort_key: [u8; 16] = row.message_id.into();
        let batch_sort_key: [u8; 16] = row.batch_id.into();
        (message_sort_key, batch_sort_key)
    });

    let mut by_message = HashMap::<Id, CoreArchiveMessageRow>::new();
    for row in core_rows {
        match by_message.entry(row.message_id) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(row);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let existing = entry.get();
                let existing_batch_key: [u8; 16] = existing.batch_id.into();
                let row_batch_key: [u8; 16] = row.batch_id.into();
                if row_batch_key < existing_batch_key {
                    entry.insert(row);
                }
            }
        }
    }

    let mut messages = Vec::with_capacity(by_message.len());
    for (message_id, row) in by_message {
        let thread_root_id = archive_thread_root(message_id, &reply_to);
        messages.push(ArchiveMessageInfo {
            id: message_id,
            author_id: row.author_id,
            author_name: author_name_by_author.get(&row.author_id).copied(),
            content: row.content,
            created_at: row.created_at,
            thread_root_id,
            conversation_id: row.conversation_id,
            source_format: row.source_format,
            source_message_id: row.source_message_id,
            source_author: row.source_author,
            source_role: row.source_role,
        });
    }
    messages.sort_by_key(|message| (interval_key(message.created_at), message.id));
    Ok(messages)
}

fn archive_coverage_report(catalog: &TribleSet) -> ArchiveCoverageReport {
    let kind_message_total = find!(
        (message_id: Id),
        pattern!(catalog, [{
            ?message_id @ playground_archive::kind: playground_archive::kind_message,
        }])
    )
    .collect::<HashSet<_>>()
    .len();

    let imported_message_total = find!(
        (message_id: Id),
        pattern!(catalog, [{
            ?message_id @
                playground_archive::kind: playground_archive::kind_message,
                playground_archive_import::batch: _?batch_id,
        }])
    )
    .collect::<HashSet<_>>()
    .len();

    let strict_imported_total = find!(
        (message_id: Id),
        pattern!(catalog, [{
            ?message_id @
                playground_archive::kind: playground_archive::kind_message,
                playground_archive_import::batch: _?batch_id,
                playground_archive::author: _?author_id,
                playground_archive::content: _?content,
                playground_archive::created_at: _?created_at,
                playground_archive_import::source_message_id: _?source_message_id,
                playground_archive_import::source_author: _?source_author,
                playground_archive_import::source_role: _?source_role,
        }])
    )
    .collect::<HashSet<_>>()
    .len();

    ArchiveCoverageReport {
        kind_message_total,
        imported_message_total,
        strict_imported_total,
    }
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
    source_author: &str,
    source_role: &str,
    source_message_id: &str,
    conversation_id: &str,
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
    append_section(&mut text, "archive_source_format", message.source_format.as_str());
    append_section(&mut text, "archive_conversation_id", conversation_id);
    append_section(&mut text, "source_message_id", source_message_id);
    if let Some(author_name) = author_name {
        append_section(&mut text, "author_name", author_name);
    }
    append_section(&mut text, "source_author", source_author);
    append_section(&mut text, "source_role", source_role);
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

    for (chunk_id, lens_id, level, summary, start_at, end_at) in find!(
        (
            chunk_id: Id,
            lens_id: Id,
            level: Value<U256BE>,
            summary: Value<Handle<Blake3, LongString>>,
            start_at: Value<NsTAIInterval>,
            end_at: Value<NsTAIInterval>
        ),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::lens_id: ?lens_id,
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
                lens_id,
                level,
                summary,
                start_at,
                end_at,
                children: Vec::new(),
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
            playground_context::child: ?child_id,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.children.push(child_id);
        }
    }

    // Legacy two-child edges.
    for (chunk_id, child_id) in find!(
        (chunk_id: Id, child_id: Id),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::left: ?child_id,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            chunk.children.push(child_id);
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
            chunk.children.push(child_id);
        }
    }

    let child_order: HashMap<Id, i128> = index
        .chunks
        .iter()
        .map(|(chunk_id, chunk)| (*chunk_id, interval_key(chunk.start_at)))
        .collect();
    for chunk in index.chunks.values_mut() {
        chunk.children.sort_by_key(|child_id| {
            (
                child_order.get(child_id).copied().unwrap_or(i128::MAX),
                *child_id,
            )
        });
        chunk.children.dedup();
    }

    for (chunk_id, exec_result_id) in find!(
        (chunk_id: Id, exec_result_id: Id),
        pattern!(catalog, [{
            ?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::about_exec_result: ?exec_result_id,
        }])
    ) {
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            index
                .chunk_for_exec_result
                .insert((exec_result_id, chunk.lens_id), chunk_id);
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
        if let Some(chunk) = index.chunks.get_mut(&chunk_id) {
            index
                .chunk_for_archive_message
                .insert((archive_message_id, chunk.lens_id), chunk_id);
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
        for child_id in &chunk.children {
            children.insert(*child_id);
        }
    }

    for chunk in index.chunks.values() {
        if children.contains(&chunk.id) {
            continue;
        }
        index
            .roots_by_lens_level
            .entry((chunk.lens_id, chunk.level))
            .or_default()
            .push(chunk.id);
    }
    for roots in index.roots_by_lens_level.values_mut() {
        roots.sort_by_key(|chunk_id| {
            index
                .chunks
                .get(chunk_id)
                .map(|chunk| (interval_key(chunk.start_at), *chunk_id))
                .unwrap_or((i128::MAX, *chunk_id))
        });
    }

    index
}

fn insert_chunk_with_carry(
    ws: &mut Workspace<Pile>,
    index: &mut ContextChunkIndex,
    change: &mut TribleSet,
    carry: ContextChunk,
    lens: &MemoryLensConfig,
    merge_arity: usize,
    semantic: &SemanticCompactor,
    stats: &mut CompactionRunStats,
) -> Result<()> {
    let merge_arity = merge_arity.max(2);
    let mut carry = carry;
    let mut level = carry.level;
    loop {
        index.chunks.insert(carry.id, carry.clone());
        let runs = index
            .roots_by_lens_level
            .entry((carry.lens_id, level))
            .or_default();
        runs.push(carry.id);
        runs.sort_by_key(|chunk_id| {
            index
                .chunks
                .get(chunk_id)
                .map(|chunk| (interval_key(chunk.start_at), *chunk_id))
                .unwrap_or((i128::MAX, *chunk_id))
        });

        if runs.len() < merge_arity {
            return Ok(());
        }

        let child_ids = std::mem::take(runs);
        if index
            .roots_by_lens_level
            .get(&(carry.lens_id, level))
            .map(Vec::is_empty)
            .unwrap_or(false)
        {
            index.roots_by_lens_level.remove(&(carry.lens_id, level));
        }

        let mut children = Vec::with_capacity(child_ids.len());
        for child_id in &child_ids {
            let child = index
                .chunks
                .get(child_id)
                .cloned()
                .with_context(|| format!("missing child chunk {child_id:x} for carry"))?;
            children.push(child);
        }
        children.sort_by_key(|child| (interval_key(child.start_at), child.id));
        let mut inputs = Vec::with_capacity(children.len());
        for child in &children {
            inputs.push(load_text(ws, child.summary).context("load child chunk summary")?);
        }
        let input_chars = inputs
            .iter()
            .map(|text| text.chars().count())
            .fold(0usize, usize::saturating_add);
        let merged_text = semantic
            .merge(lens, inputs.as_slice())
            .context("semantic merge context chunks")?;
        let output_chars = merged_text.chars().count();
        stats.merge_calls = stats.merge_calls.saturating_add(1);
        stats.merged_children_total = stats.merged_children_total.saturating_add(children.len());
        stats.merge_input_chars_total = stats.merge_input_chars_total.saturating_add(input_chars);
        stats.merge_output_chars_total =
            stats.merge_output_chars_total.saturating_add(output_chars);
        let merged_handle = ws.put(merged_text);

        let now = epoch_interval(now_epoch());
        let parent_id = ufoid();
        let parent_level = level + 1;
        let start_at = children
            .first()
            .map(|chunk| chunk.start_at)
            .context("carry merge missing first child")?;
        let end_at = children
            .last()
            .map(|chunk| chunk.end_at)
            .context("carry merge missing last child")?;

        *change += entity! { &parent_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::lens_id: lens.id,
            playground_context::level: parent_level,
            playground_context::summary: merged_handle,
            playground_context::created_at: now,
            playground_context::start_at: start_at,
            playground_context::end_at: end_at,
        };
        for child in &children {
            *change += entity! { &parent_id @
                playground_context::child: child.id,
            };
        }

        carry = ContextChunk {
            id: *parent_id,
            lens_id: lens.id,
            level: parent_level,
            summary: merged_handle,
            start_at,
            end_at,
            children: children.iter().map(|chunk| chunk.id).collect(),
            about_exec_result: None,
            about_archive_message: None,
            archive_author: None,
            archive_person: None,
            archive_thread_root: None,
            archive_conversation: None,
            archive_source_format: None,
        };
        level = parent_level;
    }
}

struct SemanticCompactor {
    client: Client,
    endpoint_url: String,
    api_key: Option<String>,
    model: String,
    chars_per_token: u64,
    memory_lenses: Vec<MemoryLensConfig>,
}

impl SemanticCompactor {
    fn new(config: &Config) -> Result<Self> {
        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .context("build semantic compaction http client")?;

        let mut model = config.model.model.clone();
        let mut base_url = config.model.base_url.clone();
        let mut api_key = config.model.api_key.clone();
        let mut chars_per_token = config.model.chars_per_token.max(1);
        if let Some(profile_id) = config.compaction_profile_id {
            match config::load_model_profile(config.pile_path.as_path(), profile_id) {
                Ok(Some((profile, _name))) => {
                    model = profile.model;
                    base_url = profile.base_url;
                    api_key = profile.api_key;
                    chars_per_token = profile.chars_per_token.max(1);
                }
                Ok(None) => eprintln!(
                    "warning: compaction profile {profile_id:x} not found; using active model profile"
                ),
                Err(err) => eprintln!(
                    "warning: failed to load compaction profile {profile_id:x}: {err:#}; using active model profile"
                ),
            }
        }

        Ok(Self {
            client,
            endpoint_url: chat_completions_url(base_url.as_str()),
            api_key,
            model,
            chars_per_token,
            memory_lenses: config.memory_lenses.clone(),
        })
    }

    fn lenses(&self) -> &[MemoryLensConfig] {
        self.memory_lenses.as_slice()
    }

    fn merge(&self, lens: &MemoryLensConfig, chunks: &[String]) -> Result<String> {
        if chunks.len() < 2 {
            return Err(anyhow!("semantic merge needs at least 2 chunks"));
        }
        let input_chars = chunks
            .iter()
            .map(|chunk| chunk.chars().count())
            .fold(0usize, usize::saturating_add)
            .max(1);
        let compression = chunks.len().max(2);
        let target_chars = input_chars / compression;
        let target_chars = target_chars.max(1);
        let max_tokens = target_chars.div_ceil(self.chars_per_token as usize) as u64;

        let mut user = String::new();
        for (idx, chunk) in chunks.iter().enumerate() {
            user.push_str(format!("CHUNK {}:\n{}\n\n", idx + 1, chunk).as_str());
        }
        user.push_str(
            format!(
                "Merge them into one summary and compress by ~1/{compression}. Keep critical details; drop repetition."
            )
            .as_str(),
        );
        let payload =
            self.build_payload(lens.compaction_prompt.as_str(), user.as_str(), max_tokens);

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

    fn build_payload(&self, system: &str, user: &str, max_output_tokens: u64) -> serde_json::Value {
        serde_json::json!({
            "model": self.model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "stream": false,
            "temperature": 0,
            "max_tokens": max_output_tokens.max(1),
        })
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

#[derive(Debug, Clone)]
struct ReasonProjectionEvent {
    id: Id,
    text: String,
    command_text: Option<String>,
    source: ReasonProjectionSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReasonProjectionSource {
    Logged,
    Model,
}

#[derive(Debug, Clone)]
struct ExecTurnProjection {
    command: String,
    reason_events: Vec<ReasonProjectionEvent>,
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

fn load_exec_turn_projection(
    ws: &mut Workspace<Pile>,
    core_index: &CoreIndex,
    exec_result: &CommandResultInfo,
) -> Result<ExecTurnProjection> {
    let mut reason_events = Vec::new();
    for event in core_index.reason_events_for_turn(exec_result.about_request) {
        let text = event
            .text
            .map(|handle| load_text(ws, handle))
            .transpose()
            .context("load reason event text")?
            .unwrap_or_default();
        let command_text = event
            .command_text
            .map(|handle| load_text(ws, handle))
            .transpose()
            .context("load reason event command text")?;
        reason_events.push(ReasonProjectionEvent {
            id: event.id,
            text,
            command_text,
            source: ReasonProjectionSource::Logged,
        });
    }

    if let Some((result_id, reasoning_text)) =
        load_reasoning_for_exec_result(ws, core_index, exec_result)?
    {
        reason_events.push(ReasonProjectionEvent {
            id: result_id,
            text: reasoning_text,
            command_text: None,
            source: ReasonProjectionSource::Model,
        });
    }

    let command = if let Some(command) = command_override_from_reason_events(reason_events.as_slice()) {
        command
    } else {
        load_command_for_result(ws, core_index, exec_result)
            .context("load command for exec turn projection")?
    };
    Ok(ExecTurnProjection {
        command,
        reason_events,
    })
}

fn command_override_from_reason_events(reason_events: &[ReasonProjectionEvent]) -> Option<String> {
    reason_events
        .iter()
        .rev()
        .find_map(|event| event.command_text.as_ref())
        .map(|command| command.clone())
}

fn should_project_reason_event(event: &ReasonProjectionEvent) -> bool {
    let reason_text = event.text.trim();
    if reason_text.is_empty() {
        return false;
    }
    if let Some(command_text) = event.command_text.as_deref() {
        if reason_text == command_text.trim() {
            return false;
        }
    }
    true
}

fn synthetic_reason_command(reason_text: &str) -> String {
    let compact = reason_text.split_whitespace().collect::<Vec<_>>().join(" ");
    let escaped = compact
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n");
    format!("reason \"{escaped}\"")
}

fn synthetic_reason_output(event: &ReasonProjectionEvent) -> String {
    let source = match event.source {
        ReasonProjectionSource::Logged => "logged",
        ReasonProjectionSource::Model => "model",
    };
    format!("reason_id: {:x}\nsource: {source}", event.id)
}

fn format_exec_outputs_by_lens(
    turn_id: Id,
    command: &str,
    result: ExecResult,
    reason_events: &[ReasonProjectionEvent],
    semantic_compactor: &SemanticCompactor,
) -> Result<Vec<(Id, String)>> {
    let ExecResult {
        stdout_text,
        stderr_text,
        stdout,
        stderr,
        exit_code,
        error,
    } = result;
    let stdout = format_output_text(stdout_text.as_deref(), stdout.as_ref());
    let stderr = format_output_text(stderr_text.as_deref(), stderr.as_ref());
    let error = error.unwrap_or_default();
    let exit_code_value = exit_code
        .map(|code| code.to_string())
        .unwrap_or_else(|| "none".to_string());
    let status = if exit_code.is_some_and(|code| code != 0) || !error.trim().is_empty() {
        "error"
    } else {
        "ok"
    };

    let mut text = String::new();
    append_section(&mut text, "turn_id", format!("{turn_id:x}").as_str());
    for event in reason_events {
        if !should_project_reason_event(event) {
            continue;
        }
        append_section(&mut text, "assistant", synthetic_reason_command(event.text.as_str()).as_str());
        append_section(&mut text, "user", synthetic_reason_output(event).as_str());
    }
    append_section(&mut text, "command", command);
    append_section(&mut text, "stdout", stdout.as_str());
    append_section(&mut text, "stderr", stderr.as_str());
    append_section(&mut text, "error", error.as_str());
    append_section(&mut text, "status", status);
    text.push_str(&format!("exit_code: {exit_code_value}\n"));
    Ok(semantic_compactor
        .lenses()
        .iter()
        .map(|lens| (lens.id, text.clone()))
        .collect())
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

fn format_output_text(text: Option<&str>, bytes: Option<&Bytes>) -> String {
    if let Some(text) = text {
        return text.to_string();
    }
    if let Some(bytes) = bytes {
        return String::from_utf8_lossy(bytes.as_ref()).to_string();
    }
    String::new()
}

/// Formats an exec result as concise shell output for moment turns.
/// Unlike `format_exec_outputs_by_lens`, this produces raw output without
/// section headers or lens duplication — just what the model would see from
/// an actual shell command.
fn format_moment_output(result: &ExecResult) -> String {
    let stdout = format_output_text(result.stdout_text.as_deref(), result.stdout.as_ref());
    let stderr = format_output_text(result.stderr_text.as_deref(), result.stderr.as_ref());
    let mut text = String::new();
    if !stdout.is_empty() {
        text.push_str(&stdout);
        if !stdout.ends_with('\n') {
            text.push('\n');
        }
    }
    if !stderr.is_empty() {
        text.push_str("stderr:\n");
        text.push_str(&stderr);
        if !stderr.ends_with('\n') {
            text.push('\n');
        }
    }
    if let Some(error) = &result.error {
        if !error.is_empty() {
            text.push_str("error: ");
            text.push_str(error);
            if !error.ends_with('\n') {
                text.push('\n');
            }
        }
    }
    if result.exit_code.is_some_and(|code| code != 0) {
        text.push_str(&format!("exit: {}\n", result.exit_code.unwrap()));
    }
    if text.is_empty() {
        text.push_str("[ok]\n");
    }
    text
}

fn u256be_to_u64(value: Value<U256BE>) -> Option<u64> {
    let raw = value.raw;
    if raw[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    let bytes: [u8; 8] = raw[24..32].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}
