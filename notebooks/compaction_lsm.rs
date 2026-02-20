use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use ed25519_dalek::SigningKey;
use eframe::egui;
use egui_plot::{Legend, Line, MarkerShape, Plot, PlotPoints, Points};
use hifitime::Epoch;
use rand::rngs::OsRng;
use tokenizers::Tokenizer;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

use GORBIE::NotebookCtx;
use GORBIE::cards::{DEFAULT_CARD_PADDING, with_padding};
use GORBIE::md;
use GORBIE::notebook;
use GORBIE::themes::colorhash;
use GORBIE::widgets::{
    Button, ChoiceToggle, Histogram, HistogramBucket, HistogramYAxis, ProgressBar, Slider,
};

#[allow(dead_code)]
#[path = "../src/config_schema.rs"]
mod config_schema;
#[allow(dead_code)]
#[path = "../src/context_schema.rs"]
mod context_schema;
#[allow(dead_code)]
#[path = "../src/exec_schema.rs"]
mod exec_schema;

use config_schema::playground_config;
use context_schema::playground_context;
use exec_schema::playground_exec;

const AGE_BUCKET_TARGET: usize = 8;
const TARGET_AGE_BIAS: f64 = 1.0;
const STEADY_STATE_START_RATIO: f32 = 0.9;
const MIN_RELEVANT_INSERTS: usize = 100_000;
const MAX_RELEVANT_INSERTS: usize = 1_000_000;
const TRACE_RENDER_LIMIT: usize = 256;
const CURVE_SCALE_LADDER: [f32; 14] = [
    0.5, 0.75, 1.0, 1.25, 1.5, 2.0, 3.0, 4.0, 6.0, 8.0, 12.0, 16.0, 24.0, 32.0,
];
#[allow(non_upper_case_globals)]
const CONFIG_BRANCH_ID: Id = id_hex!("4790808CF044F979FC7C2E47FCCB4A64");
const DEFAULT_CONTEXT_WINDOW_TOKENS: u64 = 32 * 1024;
const DEFAULT_MAX_OUTPUT_TOKENS: u64 = 1024;
const DEFAULT_PROMPT_SAFETY_MARGIN_TOKENS: u64 = 512;
const DEFAULT_PROMPT_CHARS_PER_TOKEN: u64 = 4;
const DEFAULT_CONTEXT_BRANCH_NAME: &str = "cognition";

#[derive(Debug, Clone, Default)]
struct NotebookArgs {
    text_mode: bool,
    pile_path: Option<PathBuf>,
    tokenizer_path: Option<PathBuf>,
    tokenizer_sample_limit: usize,
    inserts: Option<usize>,
}

#[derive(Debug, Clone)]
struct LlmSettings {
    model: String,
    base_url: String,
    context_window_tokens: u64,
    max_output_tokens: u64,
    prompt_safety_margin_tokens: u64,
    prompt_chars_per_token: u64,
}

impl Default for LlmSettings {
    fn default() -> Self {
        Self {
            model: "mistral-large-latest".to_string(),
            base_url: "https://api.mistral.ai/v1".to_string(),
            context_window_tokens: DEFAULT_CONTEXT_WINDOW_TOKENS,
            max_output_tokens: DEFAULT_MAX_OUTPUT_TOKENS,
            prompt_safety_margin_tokens: DEFAULT_PROMPT_SAFETY_MARGIN_TOKENS,
            prompt_chars_per_token: DEFAULT_PROMPT_CHARS_PER_TOKEN,
        }
    }
}

#[derive(Debug, Clone)]
struct ProfileCalibration {
    pile_path: PathBuf,
    context_branch_id: Option<Id>,
    context_branch_name: String,
    llm: LlmSettings,
    configured_context_window_tokens: u64,
    model_card_context_window_tokens: Option<u64>,
    prompt_budget_chars: usize,
    leaf_count: usize,
    avg_leaf_chars: Option<f64>,
    p50_leaf_chars: Option<usize>,
    p90_leaf_chars: Option<usize>,
    avg_leaf_tokens_estimate: Option<f64>,
    avg_leaf_tokens_exact: Option<f64>,
    tokenized_samples: usize,
}

fn xorshift64_next(rng: &mut u64) -> u64 {
    let mut x = *rng;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *rng = x;
    x
}

fn next_leaf_size_from_rng(base_leaf_size: usize, jitter: bool, rng: &mut u64) -> usize {
    if !jitter {
        return base_leaf_size.max(1);
    }
    let swing = (base_leaf_size / 2).max(24);
    let span = (swing * 2 + 1) as u64;
    let offset = (xorshift64_next(rng) % span) as i64 - (swing as i64);
    let base = base_leaf_size as i64;
    (base + offset).max(1) as usize
}

#[derive(Debug, Clone, Default)]
struct NotebookBootstrap {
    args: NotebookArgs,
    profile: Option<ProfileCalibration>,
    warnings: Vec<String>,
}

fn notebook_bootstrap() -> &'static NotebookBootstrap {
    static BOOTSTRAP: OnceLock<NotebookBootstrap> = OnceLock::new();
    BOOTSTRAP.get_or_init(build_notebook_bootstrap)
}

fn build_notebook_bootstrap() -> NotebookBootstrap {
    let mut bootstrap = NotebookBootstrap::default();
    match parse_notebook_args() {
        Ok(args) => {
            bootstrap.args = args;
        }
        Err(err) => {
            bootstrap
                .warnings
                .push(format!("argument parsing: {err:#}"));
            return bootstrap;
        }
    }

    if let Some(path) = bootstrap.args.pile_path.as_ref()
        && !path.exists()
    {
        bootstrap.warnings.push(format!(
            "pile path '{}' not found; using synthetic defaults",
            path.display()
        ));
        return bootstrap;
    }

    match load_profile_calibration(&bootstrap.args) {
        Ok(profile) => bootstrap.profile = profile,
        Err(err) => bootstrap
            .warnings
            .push(format!("profile calibration failed: {err:#}")),
    }

    bootstrap
}

fn parse_notebook_args() -> Result<NotebookArgs> {
    let mut args = NotebookArgs {
        text_mode: false,
        pile_path: Some(PathBuf::from("self.pile")),
        tokenizer_path: None,
        tokenizer_sample_limit: 2048,
        inserts: None,
    };

    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--text" | "--report" => args.text_mode = true,
            "--pile" => {
                let value = it
                    .next()
                    .ok_or_else(|| anyhow!("--pile expects a path argument"))?;
                args.pile_path = Some(PathBuf::from(value));
            }
            "--tokenizer" => {
                let value = it
                    .next()
                    .ok_or_else(|| anyhow!("--tokenizer expects a path argument"))?;
                args.tokenizer_path = Some(PathBuf::from(value));
            }
            "--tokenizer-sample-limit" => {
                let value = it
                    .next()
                    .ok_or_else(|| anyhow!("--tokenizer-sample-limit expects an integer"))?;
                args.tokenizer_sample_limit = value
                    .parse::<usize>()
                    .with_context(|| format!("invalid --tokenizer-sample-limit value '{value}'"))?;
                args.tokenizer_sample_limit = args.tokenizer_sample_limit.max(1);
            }
            "--inserts" => {
                let value = it
                    .next()
                    .ok_or_else(|| anyhow!("--inserts expects an integer"))?;
                args.inserts = Some(
                    value
                        .parse::<usize>()
                        .with_context(|| format!("invalid --inserts value '{value}'"))?,
                );
            }
            // Notebook macro handles these too; consume values so our parser stays aligned.
            "--out-dir" | "--scale" | "--pixels-per-point" | "--headless-wait-ms" => {
                if it.next().is_none() {
                    return Err(anyhow!("{arg} expects a value"));
                }
            }
            "--headless" => {}
            _ => {}
        }
    }

    Ok(args)
}

fn initial_state_from_bootstrap(bootstrap: &NotebookBootstrap) -> ViewState {
    let mut state = ViewState::default();
    if let Some(profile) = bootstrap.profile.as_ref() {
        if let Some(p50_leaf) = profile.p50_leaf_chars {
            state.base_leaf_size = p50_leaf.clamp(16, 4096);
        }
        state.context_budget = profile.prompt_budget_chars.clamp(200, 4_000_000);
    }
    if let Some(inserts) = bootstrap.args.inserts {
        state.set_total_inserted(inserts.clamp(1, MAX_RELEVANT_INSERTS));
    }
    state
}

fn load_profile_calibration(args: &NotebookArgs) -> Result<Option<ProfileCalibration>> {
    let Some(pile_path) = args.pile_path.as_ref() else {
        return Ok(None);
    };
    if !pile_path.exists() {
        return Ok(None);
    }

    let mut pile =
        Pile::open(pile_path).with_context(|| format!("open pile '{}'", pile_path.display()))?;
    pile.restore()
        .with_context(|| format!("restore pile '{}'", pile_path.display()))?;
    let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));

    let result = (|| -> Result<Option<ProfileCalibration>> {
        let mut ws = match repo.pull(CONFIG_BRANCH_ID) {
            Ok(ws) => ws,
            Err(_) => return Ok(None),
        };
        let catalog = ws.checkout(..).context("checkout config branch")?;
        let Some(config_id) = latest_config_entry(&catalog) else {
            return Ok(None);
        };

        let profile_id = load_id_attr(
            &catalog,
            config_id,
            playground_config::active_llm_profile_id,
        );
        let mut llm = load_llm_settings(&mut ws, &catalog, config_id)?;
        if let Some(profile_id) = profile_id
            && let Some(profile_entry_id) = latest_llm_profile_entry(&catalog, profile_id)
        {
            llm = load_llm_settings(&mut ws, &catalog, profile_entry_id)?;
        }
        let configured_context_window_tokens = llm.context_window_tokens;
        let model_card_context_window_tokens = model_card_context_window_tokens(llm.model.as_str());
        if let Some(card_tokens) = model_card_context_window_tokens {
            llm.context_window_tokens = llm.context_window_tokens.max(card_tokens);
        }

        let system_prompt = load_string_attr(
            &mut ws,
            &catalog,
            config_id,
            playground_config::system_prompt,
        )?
        .unwrap_or_default();
        let prompt_budget_chars = prompt_budget_chars_for_llm(&llm, system_prompt.as_str());

        let context_branch_id = load_id_attr(&catalog, config_id, playground_config::branch_id)
            .or_else(|| load_id_attr(&catalog, config_id, playground_config::exec_branch_id));
        let context_branch_name =
            load_string_attr(&mut ws, &catalog, config_id, playground_config::branch)?
                .unwrap_or_else(|| DEFAULT_CONTEXT_BRANCH_NAME.to_string());

        let (leaf_sizes, leaf_samples) = if let Some(branch_id) = context_branch_id {
            load_context_leaf_samples(&mut repo, branch_id, args.tokenizer_sample_limit)?
        } else {
            (Vec::new(), Vec::new())
        };

        let mut sorted_leaf_sizes = leaf_sizes.clone();
        sorted_leaf_sizes.sort_unstable();
        let leaf_count = sorted_leaf_sizes.len();
        let avg_leaf_chars = if leaf_count == 0 {
            None
        } else {
            Some(sorted_leaf_sizes.iter().sum::<usize>() as f64 / sorted_leaf_sizes.len() as f64)
        };
        let p50_leaf_chars = (!sorted_leaf_sizes.is_empty())
            .then(|| quantile_ceil(sorted_leaf_sizes.as_slice(), 0.5));
        let p90_leaf_chars = (!sorted_leaf_sizes.is_empty())
            .then(|| quantile_ceil(sorted_leaf_sizes.as_slice(), 0.9));
        let avg_leaf_tokens_estimate = avg_leaf_chars
            .map(|avg| avg / llm.prompt_chars_per_token.max(1) as f64)
            .filter(|value| value.is_finite());

        let (avg_leaf_tokens_exact, tokenized_samples) =
            if let Some(tokenizer_path) = args.tokenizer_path.as_ref() {
                match average_tokens_with_tokenizer(tokenizer_path, leaf_samples.as_slice()) {
                    Ok(Some((avg, used))) => (Some(avg), used),
                    Ok(None) => (None, 0),
                    Err(err) => {
                        eprintln!("warning: tokenizer stats unavailable: {err:#}");
                        (None, 0)
                    }
                }
            } else {
                (None, 0)
            };

        Ok(Some(ProfileCalibration {
            pile_path: pile_path.clone(),
            context_branch_id,
            context_branch_name,
            llm,
            configured_context_window_tokens,
            model_card_context_window_tokens,
            prompt_budget_chars,
            leaf_count,
            avg_leaf_chars,
            p50_leaf_chars,
            p90_leaf_chars,
            avg_leaf_tokens_estimate,
            avg_leaf_tokens_exact,
            tokenized_samples,
        }))
    })();

    let close_result = repo.into_storage().close().context("close pile");
    if let Err(err) = close_result {
        if result.is_ok() {
            return Err(err);
        }
        eprintln!("warning: failed to close pile cleanly: {err:#}");
    }

    result
}

fn latest_config_entry(catalog: &TribleSet) -> Option<Id> {
    let mut latest: Option<(Id, i128)> = None;
    for (config_id, updated_at) in find!(
        (config_id: Id, updated_at: Value<NsTAIInterval>),
        pattern!(catalog, [{
            ?config_id @
            playground_config::kind: playground_config::kind_config,
            playground_config::updated_at: ?updated_at,
        }])
    ) {
        let key = interval_key(updated_at);
        if latest.is_none_or(|(_, best_key)| key > best_key) {
            latest = Some((config_id, key));
        }
    }
    latest.map(|(config_id, _)| config_id)
}

fn latest_llm_profile_entry(catalog: &TribleSet, profile_id: Id) -> Option<Id> {
    let mut latest: Option<(Id, i128)> = None;
    for (entry_id, updated_at) in find!(
        (entry_id: Id, updated_at: Value<NsTAIInterval>),
        pattern!(catalog, [{
            ?entry_id @
            playground_config::kind: playground_config::kind_llm_profile,
            playground_config::llm_profile_id: profile_id,
            playground_config::updated_at: ?updated_at,
        }])
    ) {
        let key = interval_key(updated_at);
        if latest.is_none_or(|(_, best_key)| key > best_key) {
            latest = Some((entry_id, key));
        }
    }
    latest.map(|(entry_id, _)| entry_id)
}

fn load_llm_settings(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    entity_id: Id,
) -> Result<LlmSettings> {
    let mut llm = LlmSettings::default();
    if let Some(model) = load_string_attr(ws, catalog, entity_id, playground_config::llm_model)? {
        llm.model = model;
    }
    if let Some(base_url) =
        load_string_attr(ws, catalog, entity_id, playground_config::llm_base_url)?
    {
        llm.base_url = base_url;
    }
    if let Some(tokens) = load_u64_attr(
        catalog,
        entity_id,
        playground_config::llm_context_window_tokens,
    ) {
        llm.context_window_tokens = tokens;
    }
    if let Some(tokens) =
        load_u64_attr(catalog, entity_id, playground_config::llm_max_output_tokens)
    {
        llm.max_output_tokens = tokens;
    }
    if let Some(tokens) = load_u64_attr(
        catalog,
        entity_id,
        playground_config::llm_prompt_safety_margin_tokens,
    ) {
        llm.prompt_safety_margin_tokens = tokens;
    }
    if let Some(tokens) = load_u64_attr(
        catalog,
        entity_id,
        playground_config::llm_prompt_chars_per_token,
    ) {
        llm.prompt_chars_per_token = tokens.max(1);
    }
    Ok(llm)
}

fn model_card_context_window_tokens(model: &str) -> Option<u64> {
    let model = model.to_ascii_lowercase();
    if model.contains("mistral-large") {
        return Some(256_000);
    }
    None
}

fn has_conservative_output_cap(llm: &LlmSettings) -> bool {
    llm.context_window_tokens >= 128_000 && llm.max_output_tokens < 4096
}

fn load_context_leaf_samples(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    sample_limit: usize,
) -> Result<(Vec<usize>, Vec<String>)> {
    let mut ws = repo
        .pull(branch_id)
        .map_err(|err| anyhow!("pull context branch {branch_id:x}: {err:?}"))?;
    let catalog = ws.checkout(..).context("checkout context branch")?;
    let mut lengths = Vec::new();
    let mut samples = Vec::new();
    for (summary,) in find!(
        (summary: Value<Handle<Blake3, LongString>>),
        pattern!(&catalog, [{
            _?chunk_id @
            playground_context::kind: playground_context::kind_chunk,
            playground_context::level: 0u64,
            playground_context::summary: ?summary,
        }])
    ) {
        let text = load_blob_text(&mut ws, summary).context("read context summary")?;
        lengths.push(text.chars().count());
        if samples.len() < sample_limit {
            samples.push(text);
        }
    }
    if lengths.is_empty() {
        return load_exec_result_samples(&mut ws, &catalog, sample_limit);
    }
    Ok((lengths, samples))
}

fn load_exec_result_samples(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    sample_limit: usize,
) -> Result<(Vec<usize>, Vec<String>)> {
    let mut result_ids = HashSet::new();
    for (result_id,) in find!(
        (result_id: Id),
        pattern!(catalog, [{
            ?result_id @
            playground_exec::kind: playground_exec::kind_command_result,
        }])
    ) {
        result_ids.insert(result_id);
    }
    if result_ids.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    let mut grouped: HashMap<Id, Vec<String>> = HashMap::new();
    for (result_id, text) in find!(
        (result_id: Id, text: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{ ?result_id @ playground_exec::stdout_text: ?text }])
    ) {
        if !result_ids.contains(&result_id) {
            continue;
        }
        grouped
            .entry(result_id)
            .or_default()
            .push(load_blob_text(ws, text).context("read stdout_text")?);
    }
    for (result_id, text) in find!(
        (result_id: Id, text: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{ ?result_id @ playground_exec::stderr_text: ?text }])
    ) {
        if !result_ids.contains(&result_id) {
            continue;
        }
        grouped
            .entry(result_id)
            .or_default()
            .push(load_blob_text(ws, text).context("read stderr_text")?);
    }
    for (result_id, text) in find!(
        (result_id: Id, text: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{ ?result_id @ playground_exec::error: ?text }])
    ) {
        if !result_ids.contains(&result_id) {
            continue;
        }
        grouped
            .entry(result_id)
            .or_default()
            .push(load_blob_text(ws, text).context("read error text")?);
    }

    let mut lengths = Vec::new();
    let mut samples = Vec::new();
    for result_id in result_ids {
        let parts = grouped.remove(&result_id).unwrap_or_default();
        let merged = parts
            .into_iter()
            .map(|part| part.trim().to_string())
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>()
            .join("\n");
        if merged.is_empty() {
            continue;
        }
        lengths.push(merged.chars().count());
        if samples.len() < sample_limit {
            samples.push(merged);
        }
    }
    Ok((lengths, samples))
}

fn average_tokens_with_tokenizer(
    tokenizer_path: &Path,
    samples: &[String],
) -> Result<Option<(f64, usize)>> {
    if samples.is_empty() {
        return Ok(None);
    }
    let tokenizer = Tokenizer::from_file(tokenizer_path)
        .map_err(|err| anyhow!("load tokenizer '{}': {}", tokenizer_path.display(), err))?;
    let mut total_tokens = 0usize;
    for sample in samples {
        let encoding = tokenizer
            .encode(sample.as_str(), true)
            .map_err(|err| anyhow!("tokenize sample: {err}"))?;
        total_tokens = total_tokens.saturating_add(encoding.len());
    }
    Ok(Some((
        total_tokens as f64 / samples.len() as f64,
        samples.len(),
    )))
}

fn prompt_budget_chars_for_llm(llm: &LlmSettings, system_prompt: &str) -> usize {
    let reserved = llm
        .max_output_tokens
        .saturating_add(llm.prompt_safety_margin_tokens);
    let input_tokens = llm.context_window_tokens.saturating_sub(reserved);
    let input_chars = (input_tokens as u128) * (llm.prompt_chars_per_token.max(1) as u128);
    let input_chars = usize::try_from(input_chars).unwrap_or(usize::MAX);
    input_chars.saturating_sub(system_prompt.chars().count())
}

fn load_blob_text(
    ws: &mut Workspace<Pile>,
    handle: Value<Handle<Blake3, LongString>>,
) -> Result<String> {
    let view: View<str> = ws.get(handle).context("read text blob")?;
    Ok(view.as_ref().to_string())
}

fn load_string_attr(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    entity_id: Id,
    attr: Attribute<Handle<Blake3, LongString>>,
) -> Result<Option<String>> {
    let mut handles = find!(
        (entity: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{ ?entity @ attr: ?handle }])
    )
    .into_iter()
    .filter(|(entity, _)| *entity == entity_id);
    let Some((_, handle)) = handles.next() else {
        return Ok(None);
    };
    if handles.next().is_some() {
        return Err(anyhow!(
            "entity {entity_id:x} has multiple values for attribute {:x}",
            attr.id()
        ));
    }
    load_blob_text(ws, handle).map(Some)
}

fn load_id_attr(catalog: &TribleSet, entity_id: Id, attr: Attribute<GenId>) -> Option<Id> {
    find!(
        (entity: Id, value: Value<GenId>),
        pattern!(catalog, [{ ?entity @ attr: ?value }])
    )
    .into_iter()
    .find_map(|(entity, value)| (entity == entity_id).then_some(Id::from_value(&value)))
}

fn load_u64_attr(catalog: &TribleSet, entity_id: Id, attr: Attribute<U256BE>) -> Option<u64> {
    find!(
        (entity: Id, value: Value<U256BE>),
        pattern!(catalog, [{ ?entity @ attr: ?value }])
    )
    .into_iter()
    .find_map(|(entity, value)| (entity == entity_id).then_some(value))
    .and_then(u256be_to_u64)
}

fn u256be_to_u64(value: Value<U256BE>) -> Option<u64> {
    let raw = value.raw;
    if raw[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    let bytes: [u8; 8] = raw[24..32].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

fn interval_key(interval: Value<NsTAIInterval>) -> i128 {
    let (lower, _): (Epoch, Epoch) = interval.from_value();
    lower.to_tai_duration().total_nanoseconds()
}

fn run_text_report(bootstrap: &NotebookBootstrap) -> Result<()> {
    let mut state = initial_state_from_bootstrap(bootstrap);
    state.visible_leaves = state.stream.len();
    state.churn_sample_count = state.churn_sample_count.min(24);
    let params = CoverPolicyParams::from_state(&state);
    let sim = simulate(
        state.stream.as_slice(),
        state.visible_leaves,
        state.reduction_factor,
    );

    println!("# Compaction Policy Study (text report)");
    if let Some(profile) = bootstrap.profile.as_ref() {
        println!("- pile: {}", profile.pile_path.as_path().display());
        println!("- model: {} @ {}", profile.llm.model, profile.llm.base_url);
        println!(
            "- context window: {} tok (configured {}) | max output: {} tok | safety margin: {} tok | chars/token: {}",
            profile.llm.context_window_tokens,
            profile.configured_context_window_tokens,
            profile.llm.max_output_tokens,
            profile.llm.prompt_safety_margin_tokens,
            profile.llm.prompt_chars_per_token
        );
        if let Some(card_context) = profile.model_card_context_window_tokens {
            println!(
                "- model-card context window reference: {} tok",
                card_context
            );
        }
        if has_conservative_output_cap(&profile.llm) {
            println!(
                "- note: max_output_tokens looks conservative for this context window; consider raising it if completions are frequently cut off"
            );
        }
        println!(
            "- prompt body budget: {} chars",
            profile.prompt_budget_chars
        );
        if let Some(branch_id) = profile.context_branch_id {
            println!(
                "- context branch: {} ({:x})",
                profile.context_branch_name, branch_id
            );
        } else {
            println!("- context branch: {}", profile.context_branch_name);
        }
        if profile.leaf_count > 0 {
            println!(
                "- leaf summaries: n={} | avg={:.1} chars | p50={} | p90={}",
                profile.leaf_count,
                profile.avg_leaf_chars.unwrap_or_default(),
                profile.p50_leaf_chars.unwrap_or(0),
                profile.p90_leaf_chars.unwrap_or(0)
            );
            if let Some(avg_tokens) = profile.avg_leaf_tokens_exact {
                println!(
                    "- avg tokens/message (tokenizer): {:.2} over {} samples",
                    avg_tokens, profile.tokenized_samples
                );
            } else if let Some(avg_tokens) = profile.avg_leaf_tokens_estimate {
                println!("- avg tokens/message (estimate): {:.2}", avg_tokens);
            }
        }
    } else {
        println!("- no pile/profile calibration available; using synthetic defaults");
    }

    println!(
        "- simulation setup: inserts={} reduction={} budget={} chars policy={} sampling={}({})",
        state.visible_leaves,
        state.reduction_factor,
        state.context_budget,
        state.selection_policy.label(),
        state.churn_sampling_mode.label(),
        state.churn_sample_count
    );
    println!(
        "- tree stats: nodes={} merges={} frontier_roots={} frontier_chars={}",
        sim.nodes.len(),
        sim.merges,
        sim.roots_by_level.len(),
        sim.frontier_size()
    );

    let tail_steps = sampled_tail_steps(state.visible_leaves, state.churn_sample_count);
    let tail_window_start = tail_steps.first().copied().unwrap_or(1);
    let tail_window_end = tail_steps
        .last()
        .copied()
        .unwrap_or(state.visible_leaves.max(1));
    println!(
        "\n## Policy behavior (tail-consecutive window {}..={})",
        tail_window_start, tail_window_end
    );
    for policy in ALL_POLICIES {
        let selection = select_cover(&sim, state.context_budget, policy, params);
        let fill_ratio = if state.context_budget == 0 {
            0.0
        } else {
            selection.used_chars as f64 / state.context_budget as f64
        };
        let tail_samples = build_churn_trace_for_steps(
            state.stream.as_slice(),
            state.reduction_factor,
            state.context_budget,
            policy,
            params,
            &tail_steps,
            None,
        );
        let sparse_samples = build_churn_trace_with(
            state.stream.as_slice(),
            state.visible_leaves,
            state.reduction_factor,
            state.context_budget,
            policy,
            params,
            state.churn_sample_count,
            state.churn_sampling_mode,
        );
        let tail_summary = summarize_churn(&tail_samples, tail_window_start);
        let sparse_summary =
            summarize_churn(&sparse_samples, steady_state_min_step(state.visible_leaves));
        match (tail_summary, sparse_summary) {
            (Some(tail), Some(sparse)) => {
                println!(
                    "- {:>24} | fill {:>6.2}% | tail h-prefix {:>5.1}% h-suffix {:>6.2} h-set {:>6.2} | sparse h-prefix {:>5.1}% h-suffix {:>6.2}",
                    policy.label(),
                    fill_ratio * 100.0,
                    tail.avg_history_prefix_retention * 100.0,
                    tail.avg_history_suffix_churn,
                    tail.avg_history_set_churn,
                    sparse.avg_history_prefix_retention * 100.0,
                    sparse.avg_history_suffix_churn,
                );
            }
            (Some(tail), None) => {
                println!(
                    "- {:>24} | fill {:>6.2}% | tail h-prefix {:>5.1}% h-suffix {:>6.2} h-set {:>6.2}",
                    policy.label(),
                    fill_ratio * 100.0,
                    tail.avg_history_prefix_retention * 100.0,
                    tail.avg_history_suffix_churn,
                    tail.avg_history_set_churn,
                );
            }
            _ => {
                println!(
                    "- {:>24} | fill {:>6.2}% | no churn summary",
                    policy.label(),
                    fill_ratio * 100.0
                );
            }
        }
    }

    let sweep = run_policy_sweep(&state, &SweepConfig::default());
    println!("\n## Sweep top-5");
    for row in sweep.rows.iter().take(5) {
        println!(
            "- {:<32} score {:>7.2} | h-prefix {:>5.1}% | h-suffix {:>6.2} | h-set {:>6.2}",
            row.label,
            row.summary.score(),
            row.summary.avg_history_prefix_retention * 100.0,
            row.summary.avg_history_suffix_churn,
            row.summary.avg_history_set_churn
        );
    }

    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum SelectionPolicy {
    DistributionAware,
    DeterministicSuffix,
    DeterministicQuotaHeadroom,
    CurveHistory,
}

impl SelectionPolicy {
    fn label(self) -> &'static str {
        match self {
            SelectionPolicy::DistributionAware => "distribution",
            SelectionPolicy::DeterministicSuffix => "deterministic-suffix",
            SelectionPolicy::DeterministicQuotaHeadroom => "deterministic-quota-headroom",
            SelectionPolicy::CurveHistory => "curve-history",
        }
    }
}

const ALL_POLICIES: [SelectionPolicy; 4] = [
    SelectionPolicy::DistributionAware,
    SelectionPolicy::DeterministicSuffix,
    SelectionPolicy::DeterministicQuotaHeadroom,
    SelectionPolicy::CurveHistory,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TraceSamplingMode {
    Dense,
    Uniform,
    Log,
}

impl TraceSamplingMode {
    fn label(self) -> &'static str {
        match self {
            TraceSamplingMode::Dense => "dense",
            TraceSamplingMode::Uniform => "uniform",
            TraceSamplingMode::Log => "log",
        }
    }
}

fn policy_color(policy: SelectionPolicy) -> egui::Color32 {
    match policy {
        SelectionPolicy::DistributionAware => colorhash::ral_categorical(b"policy-distribution"),
        SelectionPolicy::DeterministicSuffix => colorhash::ral_categorical(b"policy-detsuffix"),
        SelectionPolicy::DeterministicQuotaHeadroom => {
            colorhash::ral_categorical(b"policy-detquota")
        }
        SelectionPolicy::CurveHistory => colorhash::ral_categorical(b"policy-curvehistory"),
    }
}

#[derive(Debug, Clone)]
struct SimNode {
    id: u64,
    level: u32,
    size: usize,
    start_leaf: usize,
    end_leaf: usize,
    left: Option<u64>,
    right: Option<u64>,
}

#[derive(Debug, Clone, Default)]
struct Simulation {
    nodes: Vec<SimNode>,
    roots_by_level: BTreeMap<u32, u64>,
    input_size: usize,
    merges: usize,
}

impl Simulation {
    fn node_map(&self) -> HashMap<u64, &SimNode> {
        let mut out = HashMap::with_capacity(self.nodes.len());
        for node in &self.nodes {
            out.insert(node.id, node);
        }
        out
    }

    fn frontier_size(&self) -> usize {
        let by_id = self.node_map();
        self.roots_by_level
            .values()
            .filter_map(|id| by_id.get(id))
            .map(|node| node.size)
            .sum()
    }

    fn roots_in_time_order(&self) -> Vec<u64> {
        let by_id = self.node_map();
        let mut roots: Vec<u64> = self.roots_by_level.values().copied().collect();
        roots.sort_by_key(|id| {
            by_id
                .get(id)
                .map(|node| node.start_leaf)
                .unwrap_or(usize::MAX)
        });
        roots
    }
}

#[derive(Debug, Clone)]
struct ViewState {
    base_leaf_size: usize,
    reduction_factor: u32,
    context_budget: usize,
    selection_policy: SelectionPolicy,
    det_fill_ratio: f32,
    det_safe_quantile: f32,
    moment_ratio: f32,
    show_advanced_controls: bool,
    churn_sample_count: usize,
    churn_sampling_mode: TraceSamplingMode,
    stream: Vec<usize>,
    stream_revision: u64,
    visible_leaves: usize,
    jitter: bool,
    rng: u64,
}

impl Default for ViewState {
    fn default() -> Self {
        let mut state = Self {
            base_leaf_size: 220,
            reduction_factor: 2,
            context_budget: 2200,
            selection_policy: SelectionPolicy::DistributionAware,
            det_fill_ratio: 0.85,
            det_safe_quantile: 0.9,
            moment_ratio: 0.25,
            show_advanced_controls: false,
            churn_sample_count: 512,
            churn_sampling_mode: TraceSamplingMode::Uniform,
            stream: Vec::new(),
            stream_revision: 0,
            visible_leaves: 0,
            jitter: true,
            rng: 0xA11CE5EED5A1F17B,
        };
        state.push_many(MIN_RELEVANT_INSERTS);
        state
    }
}

impl ViewState {
    fn next_leaf_size(&mut self) -> usize {
        next_leaf_size_from_rng(self.base_leaf_size, self.jitter, &mut self.rng)
    }

    fn push_many(&mut self, n: usize) {
        if n == 0 {
            return;
        }
        for _ in 0..n {
            let size = self.next_leaf_size();
            self.stream.push(size);
        }
        self.visible_leaves = self.stream.len();
        self.stream_revision = self.stream_revision.wrapping_add(1);
    }

    fn set_total_inserted(&mut self, total: usize) {
        if total > self.stream.len() {
            self.push_many(total - self.stream.len());
        } else if total < self.stream.len() {
            self.stream.truncate(total);
            self.visible_leaves = self.visible_leaves.min(self.stream.len());
            self.stream_revision = self.stream_revision.wrapping_add(1);
        }
    }
}

#[derive(Debug)]
struct InsertJobResult {
    added: Vec<usize>,
    final_rng: u64,
}

#[derive(Debug)]
struct InsertJob {
    start_len: usize,
    target_len: usize,
    progress: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    result: Arc<Mutex<Option<InsertJobResult>>>,
}

#[derive(Debug, Default)]
struct InsertState {
    job: Option<InsertJob>,
}

fn spawn_insert_job(state: &ViewState, target_len: usize) -> Option<InsertJob> {
    let start_len = state.stream.len();
    if target_len <= start_len {
        return None;
    }
    let to_generate = target_len - start_len;
    let base_leaf_size = state.base_leaf_size;
    let jitter = state.jitter;
    let start_rng = state.rng;
    let progress = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let result = Arc::new(Mutex::new(None));
    let progress_handle = Arc::clone(&progress);
    let done_handle = Arc::clone(&done);
    let result_handle = Arc::clone(&result);

    thread::spawn(move || {
        let mut rng = start_rng;
        let mut added = Vec::with_capacity(to_generate);
        for i in 0..to_generate {
            added.push(next_leaf_size_from_rng(base_leaf_size, jitter, &mut rng));
            if i % 256 == 255 {
                progress_handle.store(i + 1, Ordering::Relaxed);
            }
        }
        progress_handle.store(to_generate, Ordering::Relaxed);
        if let Ok(mut slot) = result_handle.lock() {
            *slot = Some(InsertJobResult {
                added,
                final_rng: rng,
            });
        }
        done_handle.store(true, Ordering::Release);
    });

    Some(InsertJob {
        start_len,
        target_len,
        progress,
        done,
        result,
    })
}

fn simulate(stream: &[usize], visible_leaves: usize, reduction_factor: u32) -> Simulation {
    let mut sim = Simulation::default();
    let mut roots_by_level: BTreeMap<u32, u64> = BTreeMap::new();
    let mut by_id: HashMap<u64, usize> = HashMap::new();
    let mut next_id = 1u64;
    let reduction = reduction_factor.max(2) as usize;

    for (leaf_idx, leaf_size) in stream.iter().copied().take(visible_leaves).enumerate() {
        sim.input_size = sim.input_size.saturating_add(leaf_size);
        let leaf_id = next_id;
        next_id += 1;
        let leaf = SimNode {
            id: leaf_id,
            level: 0,
            size: leaf_size.max(1),
            start_leaf: leaf_idx,
            end_leaf: leaf_idx,
            left: None,
            right: None,
        };
        by_id.insert(leaf_id, sim.nodes.len());
        sim.nodes.push(leaf);

        let mut carry = leaf_id;
        let mut level = 0u32;
        loop {
            let Some(existing) = roots_by_level.remove(&level) else {
                roots_by_level.insert(level, carry);
                break;
            };

            let left_idx = by_id[&existing];
            let right_idx = by_id[&carry];
            let (left_id, right_id) =
                if sim.nodes[left_idx].start_leaf <= sim.nodes[right_idx].start_leaf {
                    (existing, carry)
                } else {
                    (carry, existing)
                };

            let left = &sim.nodes[by_id[&left_id]];
            let right = &sim.nodes[by_id[&right_id]];
            let merged_size = ((left.size + right.size) / reduction).max(1);
            let parent_id = next_id;
            next_id += 1;

            let parent = SimNode {
                id: parent_id,
                level: level + 1,
                size: merged_size,
                start_leaf: left.start_leaf,
                end_leaf: right.end_leaf,
                left: Some(left_id),
                right: Some(right_id),
            };
            by_id.insert(parent_id, sim.nodes.len());
            sim.nodes.push(parent);
            sim.merges = sim.merges.saturating_add(1);

            carry = parent_id;
            level += 1;
        }
    }

    sim.roots_by_level = roots_by_level;
    sim
}

fn level_color(level: u32) -> egui::Color32 {
    let label = format!("lvl-{level}");
    colorhash::ral_categorical(label.as_bytes())
}

#[derive(Debug, Clone, Default)]
struct CoverSelection {
    cover: Vec<u64>,
    history_len: usize,
    moment_len: usize,
    used_chars: usize,
    dropped_roots: usize,
    splits: usize,
    steps: Vec<String>,
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    index: usize,
    parent_id: u64,
    left_id: u64,
    right_id: u64,
    extra_cost: usize,
    recency_key: usize,
    distribution_improvement: f64,
}

#[derive(Debug, Clone, Copy)]
struct CoverPolicyParams {
    det_fill_ratio: f32,
    det_safe_quantile: f32,
    moment_ratio: f32,
}

impl CoverPolicyParams {
    fn from_state(state: &ViewState) -> Self {
        Self {
            det_fill_ratio: state.det_fill_ratio.clamp(0.5, 0.98),
            det_safe_quantile: state.det_safe_quantile.clamp(0.5, 0.999),
            moment_ratio: state.moment_ratio.clamp(0.05, 0.8),
        }
    }
}

fn cover_turn_cost(node: &SimNode) -> usize {
    // Roughly mirrors "memory <id>" + "mem ... + summary" shape.
    let command_overhead = 16usize;
    let output_overhead = 64usize;
    node.size
        .saturating_add(command_overhead)
        .saturating_add(output_overhead)
}

fn simulation_leaf_count(sim: &Simulation) -> usize {
    sim.nodes
        .iter()
        .map(|node| node.end_leaf)
        .max()
        .map(|end| end.saturating_add(1))
        .unwrap_or(0)
}

fn age_bucket_for_end_leaf(
    end_leaf: usize,
    newest_leaf: usize,
    leaf_count: usize,
    bucket_count: usize,
) -> usize {
    let age = newest_leaf.saturating_sub(end_leaf);
    let mut bucket = age.saturating_mul(bucket_count) / leaf_count.max(1);
    if bucket >= bucket_count {
        bucket = bucket_count - 1;
    }
    bucket
}

fn target_age_weights(bucket_count: usize) -> Vec<f64> {
    let mut weights = Vec::with_capacity(bucket_count);
    for i in 0..bucket_count {
        let recency_rank = (bucket_count - i) as f64;
        weights.push(recency_rank.powf(TARGET_AGE_BIAS));
    }
    weights
}

fn distribution_error(
    bucket_chars: &[usize],
    total_chars: usize,
    target_weights: &[f64],
    target_weight_sum: f64,
) -> f64 {
    if total_chars == 0 || target_weight_sum <= 0.0 || bucket_chars.is_empty() {
        return 0.0;
    }
    let total = total_chars as f64;
    let mut error = 0.0f64;
    for (i, actual_chars) in bucket_chars.iter().copied().enumerate() {
        let actual = actual_chars as f64 / total;
        let target = target_weights[i] / target_weight_sum;
        let diff = actual - target;
        error += diff * diff;
    }
    error
}

fn split_preserves_level_monotonicity(
    cover: &[u64],
    by_id: &HashMap<u64, &SimNode>,
    split_index: usize,
    child_level: u32,
) -> bool {
    let prev_ok = if split_index == 0 {
        true
    } else {
        by_id
            .get(&cover[split_index - 1])
            .map(|prev| prev.level >= child_level)
            .unwrap_or(false)
    };
    if !prev_ok {
        return false;
    }

    let next_ok = if split_index + 1 >= cover.len() {
        true
    } else {
        by_id
            .get(&cover[split_index + 1])
            .map(|next| child_level >= next.level)
            .unwrap_or(false)
    };
    next_ok
}

fn select_cover(
    sim: &Simulation,
    budget_chars: usize,
    policy: SelectionPolicy,
    params: CoverPolicyParams,
) -> CoverSelection {
    let by_id = sim.node_map();
    let leaf_count = simulation_leaf_count(sim);
    if leaf_count == 0 || budget_chars == 0 {
        return CoverSelection {
            cover: Vec::new(),
            history_len: 0,
            moment_len: 0,
            used_chars: 0,
            dropped_roots: 0,
            splits: 0,
            steps: vec!["no visible leaves".to_string()],
        };
    }

    let (moment_cover, moment_start_leaf, moment_used, reserved_moment_budget) =
        select_moment_leaves(sim, &by_id, budget_chars, params.moment_ratio);
    let global_newest_leaf = leaf_count.saturating_sub(1);
    // Keep history budget stable turn-to-turn: reserve a fixed slice for moment, independent of
    // how much the newest raw leaves happened to consume this step.
    let history_budget = budget_chars.saturating_sub(reserved_moment_budget);
    let history_end_leaf = match moment_start_leaf {
        Some(start) => start.checked_sub(1),
        None => leaf_count.checked_sub(1),
    };
    let history_seed = build_history_seed_cover(sim, &by_id, history_end_leaf);

    let mut history = match policy {
        SelectionPolicy::DistributionAware => {
            select_cover_distribution(sim, history_budget, history_seed, global_newest_leaf)
        }
        SelectionPolicy::DeterministicSuffix => {
            select_cover_deterministic(sim, history_budget, history_seed)
        }
        SelectionPolicy::DeterministicQuotaHeadroom => select_cover_deterministic_quota(
            sim,
            history_budget,
            params,
            history_seed,
            global_newest_leaf,
        ),
        SelectionPolicy::CurveHistory => {
            select_cover_curve_history(sim, history_budget, history_seed, global_newest_leaf)
        }
    };

    let mut steps = Vec::new();
    steps.push(format!(
        "moment(split shared): budget={}, ratio={:.2}, reserved={}, used={}",
        budget_chars, params.moment_ratio, reserved_moment_budget, moment_used
    ));
    if let Some(start) = moment_start_leaf {
        steps.push(format!(
            "moment leaves [{}..{}]",
            start,
            leaf_count.saturating_sub(1)
        ));
    } else {
        steps.push("moment leaves: empty".to_string());
    }
    steps.push(format!("history budget {}", history_budget));
    steps.extend(
        history
            .steps
            .drain(..)
            .map(|step| format!("history: {step}")),
    );

    let history_nodes = history.cover.len();
    let moment_nodes = moment_cover.len();
    history.cover.extend(moment_cover);
    history.history_len = history_nodes;
    history.moment_len = moment_nodes;
    history.used_chars = history.used_chars.saturating_add(moment_used);
    history.steps = steps;
    history.steps.push(format!(
        "final: history_nodes={}, moment_nodes={}, used {} / {} chars",
        history_nodes, moment_nodes, history.used_chars, budget_chars
    ));
    history
}

fn history_leaf_metrics(history_newest_leaf: Option<usize>) -> Option<(usize, usize, usize)> {
    history_newest_leaf.map(|newest| {
        let leaf_count = newest.saturating_add(1).max(1);
        let bucket_count = leaf_count.min(AGE_BUCKET_TARGET).max(1);
        (newest, leaf_count, bucket_count)
    })
}

fn initial_history_step(prefix: &str, cover_len: usize, used: usize, budget: usize) -> String {
    format!("{prefix}: nodes={cover_len}, used {used} / {budget} chars")
}

fn empty_history_selection(reason: &str) -> CoverSelection {
    CoverSelection {
        cover: Vec::new(),
        history_len: 0,
        moment_len: 0,
        used_chars: 0,
        dropped_roots: 0,
        splits: 0,
        steps: vec![reason.to_string()],
    }
}

fn node_span(node: &SimNode) -> usize {
    node.end_leaf
        .saturating_sub(node.start_leaf)
        .saturating_add(1)
}

fn cover_cost(cover: &[u64], by_id: &HashMap<u64, &SimNode>) -> usize {
    cover
        .iter()
        .filter_map(|id| by_id.get(id).copied())
        .map(cover_turn_cost)
        .sum()
}

fn ordered_children(by_id: &HashMap<u64, &SimNode>, left: u64, right: u64) -> (u64, u64) {
    let left_start = by_id
        .get(&left)
        .map(|node| node.start_leaf)
        .unwrap_or(usize::MAX);
    let right_start = by_id
        .get(&right)
        .map(|node| node.start_leaf)
        .unwrap_or(usize::MAX);
    if left_start <= right_start {
        (left, right)
    } else {
        (right, left)
    }
}

fn leaf_ids_in_time_order(sim: &Simulation) -> Vec<u64> {
    let mut leaves: Vec<(usize, u64)> = sim
        .nodes
        .iter()
        .filter(|node| node.level == 0)
        .map(|node| (node.start_leaf, node.id))
        .collect();
    leaves.sort_by_key(|(idx, _)| *idx);
    leaves.into_iter().map(|(_, id)| id).collect()
}

fn collect_cover_for_range(
    node_id: u64,
    range_start: usize,
    range_end: usize,
    by_id: &HashMap<u64, &SimNode>,
    out: &mut Vec<u64>,
) {
    let Some(node) = by_id.get(&node_id).copied() else {
        return;
    };
    if node.end_leaf < range_start || node.start_leaf > range_end {
        return;
    }
    if node.start_leaf >= range_start && node.end_leaf <= range_end {
        out.push(node_id);
        return;
    }
    if let (Some(left), Some(right)) = (node.left, node.right) {
        let (left, right) = ordered_children(by_id, left, right);
        collect_cover_for_range(left, range_start, range_end, by_id, out);
        collect_cover_for_range(right, range_start, range_end, by_id, out);
        return;
    }
    out.push(node_id);
}

fn build_history_seed_cover(
    sim: &Simulation,
    by_id: &HashMap<u64, &SimNode>,
    history_end_leaf: Option<usize>,
) -> Vec<u64> {
    let Some(history_end_leaf) = history_end_leaf else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for root_id in sim.roots_in_time_order() {
        collect_cover_for_range(root_id, 0, history_end_leaf, by_id, &mut out);
    }
    out
}

fn select_moment_leaves(
    sim: &Simulation,
    by_id: &HashMap<u64, &SimNode>,
    budget_chars: usize,
    moment_ratio: f32,
) -> (Vec<u64>, Option<usize>, usize, usize) {
    if budget_chars == 0 {
        return (Vec::new(), None, 0, 0);
    }
    let moment_budget = ((budget_chars as f32) * moment_ratio.clamp(0.0, 1.0))
        .round()
        .clamp(0.0, budget_chars as f32) as usize;
    if moment_budget == 0 {
        return (Vec::new(), None, 0, 0);
    }

    let leaf_ids = leaf_ids_in_time_order(sim);
    let mut moment = Vec::new();
    let mut used = 0usize;
    for leaf_id in leaf_ids.iter().rev().copied() {
        let Some(leaf) = by_id.get(&leaf_id).copied() else {
            continue;
        };
        let cost = cover_turn_cost(leaf);
        if cost > budget_chars {
            continue;
        }
        if used.saturating_add(cost) > moment_budget {
            break;
        }
        moment.push(leaf_id);
        used = used.saturating_add(cost);
        if used >= moment_budget {
            break;
        }
    }
    moment.reverse();
    let moment_start_leaf = moment
        .first()
        .and_then(|id| by_id.get(id).map(|node| node.start_leaf));
    (moment, moment_start_leaf, used, moment_budget)
}

fn dyadic_base_span(age: usize) -> usize {
    let value = age.saturating_add(1);
    let highest_bit = (usize::BITS - 1).saturating_sub(value.leading_zeros()) as usize;
    1usize << highest_bit
}

fn allowed_span(age: usize, scale: f32) -> f64 {
    (dyadic_base_span(age) as f64) * (scale as f64)
}

fn node_violates_curve(node: &SimNode, global_newest_leaf: usize, scale: f32) -> bool {
    let age_end = global_newest_leaf.saturating_sub(node.end_leaf);
    let span = node_span(node) as f64;
    span > allowed_span(age_end, scale) + f64::EPSILON
}

fn apply_curve_constraint(
    mut cover: Vec<u64>,
    by_id: &HashMap<u64, &SimNode>,
    global_newest_leaf: usize,
    scale: f32,
    mut steps: Option<&mut Vec<String>>,
    trace_prefix: &str,
) -> (Vec<u64>, usize) {
    let mut splits = 0usize;
    loop {
        let mut changed = false;
        for idx in 0..cover.len() {
            let node_id = cover[idx];
            let Some(node) = by_id.get(&node_id).copied() else {
                continue;
            };
            if !node_violates_curve(node, global_newest_leaf, scale) {
                continue;
            }
            let (Some(left), Some(right)) = (node.left, node.right) else {
                continue;
            };
            let (left, right) = ordered_children(by_id, left, right);
            cover.splice(idx..=idx, [left, right]);
            splits = splits.saturating_add(1);
            if let Some(steps) = steps.as_deref_mut() {
                steps.push(format!(
                    "{trace_prefix} curve split #{:04} -> #{:04}, #{:04} (scale {:.2})",
                    node.id, left, right, scale
                ));
            }
            changed = true;
            break;
        }
        if !changed {
            break;
        }
    }
    (cover, splits)
}

fn refine_history_suffix(
    cover: &mut Vec<u64>,
    by_id: &HashMap<u64, &SimNode>,
    global_newest_leaf: usize,
    scale: f32,
    history_budget: usize,
    used_chars: &mut usize,
    steps: &mut Vec<String>,
) -> usize {
    let mut splits = 0usize;
    loop {
        let remaining = history_budget.saturating_sub(*used_chars);
        if remaining == 0 {
            break;
        }
        let mut changed = false;
        for idx in (0..cover.len()).rev() {
            let parent_id = cover[idx];
            let Some(parent) = by_id.get(&parent_id).copied() else {
                continue;
            };
            let (Some(left), Some(right)) = (parent.left, parent.right) else {
                continue;
            };
            let (left, right) = ordered_children(by_id, left, right);
            let Some(left_node) = by_id.get(&left).copied() else {
                continue;
            };
            let Some(right_node) = by_id.get(&right).copied() else {
                continue;
            };
            if node_violates_curve(left_node, global_newest_leaf, scale)
                || node_violates_curve(right_node, global_newest_leaf, scale)
            {
                continue;
            }
            let parent_cost = cover_turn_cost(parent);
            let child_cost = cover_turn_cost(left_node).saturating_add(cover_turn_cost(right_node));
            let extra = child_cost.saturating_sub(parent_cost);
            if extra > remaining {
                continue;
            }
            cover.splice(idx..=idx, [left, right]);
            *used_chars = used_chars.saturating_add(extra);
            splits = splits.saturating_add(1);
            steps.push(format!(
                "history refine split #{:04} -> #{:04}, #{:04} (+{} chars)",
                parent.id, left, right, extra
            ));
            changed = true;
            break;
        }
        if !changed {
            break;
        }
    }
    splits
}

fn select_cover_curve_history(
    sim: &Simulation,
    budget_chars: usize,
    mut cover: Vec<u64>,
    global_newest_leaf: usize,
) -> CoverSelection {
    let by_id = sim.node_map();
    if budget_chars == 0 {
        return empty_history_selection("history budget is zero");
    }
    if cover.is_empty() {
        return empty_history_selection("history seed cover is empty");
    }

    let mut steps = Vec::new();
    let mut dropped_roots = 0usize;
    let mut splits = 0usize;
    let mut used_chars = cover_cost(&cover, &by_id);
    steps.push(initial_history_step(
        "start[curve-history]",
        cover.len(),
        used_chars,
        budget_chars,
    ));

    let mut chosen_scale = CURVE_SCALE_LADDER.last().copied().unwrap_or(1.0);
    for scale in CURVE_SCALE_LADDER {
        let (candidate_cover, _) =
            apply_curve_constraint(cover.clone(), &by_id, global_newest_leaf, scale, None, "");
        let candidate_cost = cover_cost(&candidate_cover, &by_id);
        steps.push(format!(
            "scale {:.2}: history cost {} / {}",
            scale, candidate_cost, budget_chars
        ));
        chosen_scale = scale;
        if candidate_cost <= budget_chars {
            break;
        }
    }

    let (curve_cover, curve_splits) = apply_curve_constraint(
        cover,
        &by_id,
        global_newest_leaf,
        chosen_scale,
        Some(&mut steps),
        "",
    );
    cover = curve_cover;
    used_chars = cover_cost(&cover, &by_id);
    splits = splits.saturating_add(curve_splits);
    steps.push(format!(
        "selected scale {:.2} with history cost {} / {}",
        chosen_scale, used_chars, budget_chars
    ));

    let refine_splits = refine_history_suffix(
        &mut cover,
        &by_id,
        global_newest_leaf,
        chosen_scale,
        budget_chars,
        &mut used_chars,
        &mut steps,
    );
    splits = splits.saturating_add(refine_splits);

    while used_chars > budget_chars && !cover.is_empty() {
        let dropped_id = cover.remove(0);
        if let Some(node) = by_id.get(&dropped_id).copied() {
            let cost = cover_turn_cost(node);
            used_chars = used_chars.saturating_sub(cost);
            dropped_roots = dropped_roots.saturating_add(1);
            steps.push(format!(
                "drop oldest history node #{:04} [{}..{}], -{} chars => {} / {}",
                node.id, node.start_leaf, node.end_leaf, cost, used_chars, budget_chars
            ));
        }
    }
    if cover.is_empty() {
        steps.push("history cover empty after drops".to_string());
    }

    CoverSelection {
        cover,
        history_len: 0,
        moment_len: 0,
        used_chars,
        dropped_roots,
        splits,
        steps,
    }
}

fn select_cover_distribution(
    sim: &Simulation,
    budget_chars: usize,
    mut cover: Vec<u64>,
    global_newest_leaf: usize,
) -> CoverSelection {
    let by_id = sim.node_map();
    let (newest_leaf, leaf_count, bucket_count) =
        history_leaf_metrics(Some(global_newest_leaf)).expect("global newest leaf is always set");
    if cover.is_empty() {
        return empty_history_selection("history seed cover is empty");
    }

    let target_weights = target_age_weights(bucket_count);
    let target_weight_sum: f64 = target_weights.iter().sum();

    let mut used_chars = cover_cost(&cover, &by_id);
    let mut bucket_chars = vec![0usize; bucket_count];
    for node_id in &cover {
        let Some(node) = by_id.get(node_id).copied() else {
            continue;
        };
        let bucket = age_bucket_for_end_leaf(node.end_leaf, newest_leaf, leaf_count, bucket_count);
        bucket_chars[bucket] = bucket_chars[bucket].saturating_add(cover_turn_cost(node));
    }
    let mut current_error = distribution_error(
        &bucket_chars,
        used_chars,
        &target_weights,
        target_weight_sum,
    );

    let mut dropped_roots = 0usize;
    let mut splits = 0usize;
    let mut steps = Vec::new();
    steps.push(initial_history_step(
        "start[distribution]",
        cover.len(),
        used_chars,
        budget_chars,
    ));
    steps.push(format!("target_error={:.4}", current_error));

    loop {
        let remaining = budget_chars.saturating_sub(used_chars);
        if remaining == 0 {
            break;
        }

        let mut best: Option<Candidate> = None;
        for (idx, parent_id) in cover.iter().enumerate() {
            let Some(parent) = by_id.get(parent_id).copied() else {
                continue;
            };
            let (Some(left_id), Some(right_id)) = (parent.left, parent.right) else {
                continue;
            };
            let Some(left) = by_id.get(&left_id).copied() else {
                continue;
            };
            let Some(right) = by_id.get(&right_id).copied() else {
                continue;
            };

            let parent_cost = cover_turn_cost(parent);
            let children_cost = cover_turn_cost(left).saturating_add(cover_turn_cost(right));
            let extra_cost = children_cost.saturating_sub(parent_cost);
            if extra_cost > remaining {
                continue;
            }
            let child_level = parent.level.saturating_sub(1);
            if !split_preserves_level_monotonicity(&cover, &by_id, idx, child_level) {
                continue;
            }
            let candidate = Candidate {
                index: idx,
                parent_id: *parent_id,
                left_id,
                right_id,
                extra_cost,
                recency_key: parent.end_leaf,
                distribution_improvement: {
                    let mut projected = bucket_chars.clone();
                    let parent_bucket = age_bucket_for_end_leaf(
                        parent.end_leaf,
                        newest_leaf,
                        leaf_count,
                        bucket_count,
                    );
                    let left_bucket = age_bucket_for_end_leaf(
                        left.end_leaf,
                        newest_leaf,
                        leaf_count,
                        bucket_count,
                    );
                    let right_bucket = age_bucket_for_end_leaf(
                        right.end_leaf,
                        newest_leaf,
                        leaf_count,
                        bucket_count,
                    );
                    projected[parent_bucket] = projected[parent_bucket].saturating_sub(parent_cost);
                    projected[left_bucket] =
                        projected[left_bucket].saturating_add(cover_turn_cost(left));
                    projected[right_bucket] =
                        projected[right_bucket].saturating_add(cover_turn_cost(right));
                    let projected_used = used_chars.saturating_add(extra_cost);
                    let projected_error = distribution_error(
                        &projected,
                        projected_used,
                        &target_weights,
                        target_weight_sum,
                    );
                    current_error - projected_error
                },
            };
            if better_candidate(candidate, best) {
                best = Some(candidate);
            }
        }

        let Some(chosen) = best else {
            break;
        };
        cover.splice(
            chosen.index..=chosen.index,
            [chosen.left_id, chosen.right_id],
        );
        used_chars = used_chars.saturating_add(chosen.extra_cost);
        let Some(parent) = by_id.get(&chosen.parent_id).copied() else {
            continue;
        };
        let Some(left) = by_id.get(&chosen.left_id).copied() else {
            continue;
        };
        let Some(right) = by_id.get(&chosen.right_id).copied() else {
            continue;
        };
        let parent_cost = cover_turn_cost(parent);
        let left_cost = cover_turn_cost(left);
        let right_cost = cover_turn_cost(right);
        let parent_bucket =
            age_bucket_for_end_leaf(parent.end_leaf, newest_leaf, leaf_count, bucket_count);
        let left_bucket =
            age_bucket_for_end_leaf(left.end_leaf, newest_leaf, leaf_count, bucket_count);
        let right_bucket =
            age_bucket_for_end_leaf(right.end_leaf, newest_leaf, leaf_count, bucket_count);
        bucket_chars[parent_bucket] = bucket_chars[parent_bucket].saturating_sub(parent_cost);
        bucket_chars[left_bucket] = bucket_chars[left_bucket].saturating_add(left_cost);
        bucket_chars[right_bucket] = bucket_chars[right_bucket].saturating_add(right_cost);
        current_error = distribution_error(
            &bucket_chars,
            used_chars,
            &target_weights,
            target_weight_sum,
        );
        splits = splits.saturating_add(1);
        steps.push(format!(
            "split #{:04} -> #{:04}, #{:04} (+{} chars, Δerr {:+.4}) => {} / {} (error {:.4})",
            chosen.parent_id,
            chosen.left_id,
            chosen.right_id,
            chosen.extra_cost,
            chosen.distribution_improvement,
            used_chars,
            budget_chars,
            current_error
        ));
    }

    while used_chars > budget_chars && !cover.is_empty() {
        let dropped = cover.remove(0);
        if let Some(node) = by_id.get(&dropped).copied() {
            let cost = cover_turn_cost(node);
            used_chars = used_chars.saturating_sub(cost);
            let bucket =
                age_bucket_for_end_leaf(node.end_leaf, newest_leaf, leaf_count, bucket_count);
            bucket_chars[bucket] = bucket_chars[bucket].saturating_sub(cost);
            current_error = distribution_error(
                &bucket_chars,
                used_chars,
                &target_weights,
                target_weight_sum,
            );
            dropped_roots = dropped_roots.saturating_add(1);
            steps.push(format!(
                "drop oldest root #{:04} [{}..{}], -{} chars => {} / {} (error {:.4})",
                node.id,
                node.start_leaf,
                node.end_leaf,
                cost,
                used_chars,
                budget_chars,
                current_error
            ));
        }
    }
    if cover.is_empty() {
        steps.push("cover is empty after drops".to_string());
    }

    if steps.len() == 1 {
        steps.push("no changes needed".to_string());
    }

    CoverSelection {
        cover,
        history_len: 0,
        moment_len: 0,
        used_chars,
        dropped_roots,
        splits,
        steps,
    }
}

fn select_cover_deterministic(
    sim: &Simulation,
    budget_chars: usize,
    mut cover: Vec<u64>,
) -> CoverSelection {
    let by_id = sim.node_map();
    if cover.is_empty() {
        return empty_history_selection("history seed cover is empty");
    }
    let mut used_chars = cover_cost(&cover, &by_id);

    let mut dropped_roots = 0usize;
    let mut splits = 0usize;
    let mut steps = Vec::new();
    steps.push(initial_history_step(
        "start[det]",
        cover.len(),
        used_chars,
        budget_chars,
    ));

    loop {
        let remaining = budget_chars.saturating_sub(used_chars);
        if remaining == 0 {
            break;
        }

        let mut chosen: Option<Candidate> = None;
        for idx in (0..cover.len()).rev() {
            let parent_id = cover[idx];
            let Some(parent) = by_id.get(&parent_id).copied() else {
                continue;
            };
            let (Some(left_id), Some(right_id)) = (parent.left, parent.right) else {
                continue;
            };
            let Some(left) = by_id.get(&left_id).copied() else {
                continue;
            };
            let Some(right) = by_id.get(&right_id).copied() else {
                continue;
            };

            let parent_cost = cover_turn_cost(parent);
            let children_cost = cover_turn_cost(left).saturating_add(cover_turn_cost(right));
            let extra_cost = children_cost.saturating_sub(parent_cost);
            if extra_cost > remaining {
                continue;
            }
            let child_level = parent.level.saturating_sub(1);
            if !split_preserves_level_monotonicity(&cover, &by_id, idx, child_level) {
                continue;
            }
            chosen = Some(Candidate {
                index: idx,
                parent_id,
                left_id,
                right_id,
                extra_cost,
                recency_key: parent.end_leaf,
                distribution_improvement: 0.0,
            });
            break;
        }

        let Some(chosen) = chosen else {
            break;
        };

        cover.splice(
            chosen.index..=chosen.index,
            [chosen.left_id, chosen.right_id],
        );
        used_chars = used_chars.saturating_add(chosen.extra_cost);
        splits = splits.saturating_add(1);
        steps.push(format!(
            "split[det] #{:04} -> #{:04}, #{:04} (+{} chars) => {} / {}",
            chosen.parent_id,
            chosen.left_id,
            chosen.right_id,
            chosen.extra_cost,
            used_chars,
            budget_chars
        ));
    }

    while used_chars > budget_chars && !cover.is_empty() {
        let dropped = cover.remove(0);
        if let Some(node) = by_id.get(&dropped).copied() {
            let cost = cover_turn_cost(node);
            used_chars = used_chars.saturating_sub(cost);
            dropped_roots = dropped_roots.saturating_add(1);
            steps.push(format!(
                "drop oldest root #{:04} [{}..{}], -{} chars => {} / {}",
                node.id, node.start_leaf, node.end_leaf, cost, used_chars, budget_chars
            ));
        }
    }
    if cover.is_empty() {
        steps.push("cover is empty after drops".to_string());
    }

    if steps.len() == 1 {
        steps.push("no changes needed".to_string());
    }

    CoverSelection {
        cover,
        history_len: 0,
        moment_len: 0,
        used_chars,
        dropped_roots,
        splits,
        steps,
    }
}

fn quantile_ceil(values: &[usize], q: f32) -> usize {
    if values.is_empty() {
        return 1;
    }
    let q = q.clamp(0.0, 1.0);
    let idx = (((values.len().saturating_sub(1)) as f32) * q).ceil() as usize;
    values[idx.min(values.len().saturating_sub(1))].max(1)
}

fn slot_quotas(total_slots: usize, weights: &[f64]) -> Vec<usize> {
    if weights.is_empty() {
        return Vec::new();
    }
    if total_slots == 0 {
        return vec![0; weights.len()];
    }
    let weight_sum: f64 = weights.iter().sum();
    if weight_sum <= 0.0 {
        let mut out = vec![0usize; weights.len()];
        out[0] = total_slots;
        return out;
    }

    let mut floors = Vec::with_capacity(weights.len());
    let mut remainders = Vec::with_capacity(weights.len());
    let mut used = 0usize;
    for (idx, weight) in weights.iter().copied().enumerate() {
        let exact = (weight / weight_sum) * (total_slots as f64);
        let floor = exact.floor() as usize;
        floors.push(floor);
        remainders.push((idx, exact - floor as f64));
        used = used.saturating_add(floor);
    }

    let mut remaining = total_slots.saturating_sub(used);
    remainders.sort_by(|(ia, ra), (ib, rb)| rb.total_cmp(ra).then_with(|| ia.cmp(ib)));
    for (idx, _) in remainders {
        if remaining == 0 {
            break;
        }
        floors[idx] = floors[idx].saturating_add(1);
        remaining = remaining.saturating_sub(1);
    }
    floors
}

fn slot_deficit(counts: &[usize], quotas: &[usize]) -> isize {
    counts
        .iter()
        .copied()
        .zip(quotas.iter().copied())
        .map(|(count, quota)| quota.saturating_sub(count) as isize)
        .sum()
}

fn select_cover_deterministic_quota(
    sim: &Simulation,
    budget_chars: usize,
    params: CoverPolicyParams,
    mut cover: Vec<u64>,
    global_newest_leaf: usize,
) -> CoverSelection {
    let by_id = sim.node_map();
    if budget_chars == 0 {
        return empty_history_selection("history budget is zero");
    }
    let (newest_leaf, leaf_count, bucket_count) =
        history_leaf_metrics(Some(global_newest_leaf)).expect("global newest leaf is always set");
    if cover.is_empty() {
        return empty_history_selection("history seed cover is empty");
    }
    let target_weights = target_age_weights(bucket_count);

    let mut all_costs: Vec<usize> = sim
        .nodes
        .iter()
        .filter(|node| node.end_leaf <= newest_leaf)
        .map(cover_turn_cost)
        .collect();
    if all_costs.is_empty() {
        all_costs.push(1);
    }
    all_costs.sort_unstable();
    let safe_cost = quantile_ceil(&all_costs, params.det_safe_quantile).max(1);
    let effective_budget = ((budget_chars as f32) * params.det_fill_ratio)
        .round()
        .clamp(1.0, budget_chars as f32) as usize;
    let target_slots = (effective_budget / safe_cost).max(1);
    let target_slot_quotas = slot_quotas(target_slots, &target_weights);

    let mut used_chars = cover_cost(&cover, &by_id);
    let mut slot_counts = vec![0usize; bucket_count];
    for node_id in &cover {
        let Some(node) = by_id.get(node_id).copied() else {
            continue;
        };
        let bucket = age_bucket_for_end_leaf(node.end_leaf, newest_leaf, leaf_count, bucket_count);
        slot_counts[bucket] = slot_counts[bucket].saturating_add(1);
    }

    let mut dropped_roots = 0usize;
    let mut splits = 0usize;
    let mut steps = Vec::new();
    steps.push(initial_history_step(
        "start[detq]",
        cover.len(),
        used_chars,
        budget_chars,
    ));
    steps.push(format!(
        "fill={:.0}%, eff_budget={}, q={:.2}, safe_cost={}, target_slots={}",
        params.det_fill_ratio * 100.0,
        effective_budget,
        params.det_safe_quantile,
        safe_cost,
        target_slots
    ));

    loop {
        if cover.len() >= target_slots {
            break;
        }
        let remaining = effective_budget.saturating_sub(used_chars);
        if remaining == 0 {
            break;
        }

        let current_deficit = slot_deficit(&slot_counts, &target_slot_quotas);
        let mut best: Option<Candidate> = None;
        let mut best_deficit = current_deficit;
        for idx in (0..cover.len()).rev() {
            let parent_id = cover[idx];
            let Some(parent) = by_id.get(&parent_id).copied() else {
                continue;
            };
            let (Some(left_id), Some(right_id)) = (parent.left, parent.right) else {
                continue;
            };
            let Some(left) = by_id.get(&left_id).copied() else {
                continue;
            };
            let Some(right) = by_id.get(&right_id).copied() else {
                continue;
            };
            let parent_cost = cover_turn_cost(parent);
            let left_cost = cover_turn_cost(left);
            let right_cost = cover_turn_cost(right);
            let extra_cost = left_cost
                .saturating_add(right_cost)
                .saturating_sub(parent_cost);
            if extra_cost > remaining {
                continue;
            }
            let child_level = parent.level.saturating_sub(1);
            if !split_preserves_level_monotonicity(&cover, &by_id, idx, child_level) {
                continue;
            }
            let parent_bucket =
                age_bucket_for_end_leaf(parent.end_leaf, newest_leaf, leaf_count, bucket_count);
            let left_bucket =
                age_bucket_for_end_leaf(left.end_leaf, newest_leaf, leaf_count, bucket_count);
            let right_bucket =
                age_bucket_for_end_leaf(right.end_leaf, newest_leaf, leaf_count, bucket_count);
            let mut projected_counts = slot_counts.clone();
            projected_counts[parent_bucket] = projected_counts[parent_bucket].saturating_sub(1);
            projected_counts[left_bucket] = projected_counts[left_bucket].saturating_add(1);
            projected_counts[right_bucket] = projected_counts[right_bucket].saturating_add(1);
            let projected_deficit = slot_deficit(&projected_counts, &target_slot_quotas);
            let deficit_improvement = (current_deficit - projected_deficit) as f64;
            let candidate = Candidate {
                index: idx,
                parent_id,
                left_id,
                right_id,
                extra_cost,
                recency_key: parent.end_leaf,
                distribution_improvement: deficit_improvement,
            };

            if projected_deficit < best_deficit {
                best_deficit = projected_deficit;
                best = Some(candidate);
                continue;
            }
            if projected_deficit == best_deficit && better_candidate(candidate, best) {
                best = Some(candidate);
            }
        }

        let Some(chosen) = best else {
            break;
        };
        if chosen.distribution_improvement <= 0.0 {
            break;
        }

        cover.splice(
            chosen.index..=chosen.index,
            [chosen.left_id, chosen.right_id],
        );
        used_chars = used_chars.saturating_add(chosen.extra_cost);
        let Some(parent) = by_id.get(&chosen.parent_id).copied() else {
            continue;
        };
        let Some(left) = by_id.get(&chosen.left_id).copied() else {
            continue;
        };
        let Some(right) = by_id.get(&chosen.right_id).copied() else {
            continue;
        };
        let parent_bucket =
            age_bucket_for_end_leaf(parent.end_leaf, newest_leaf, leaf_count, bucket_count);
        let left_bucket =
            age_bucket_for_end_leaf(left.end_leaf, newest_leaf, leaf_count, bucket_count);
        let right_bucket =
            age_bucket_for_end_leaf(right.end_leaf, newest_leaf, leaf_count, bucket_count);
        slot_counts[parent_bucket] = slot_counts[parent_bucket].saturating_sub(1);
        slot_counts[left_bucket] = slot_counts[left_bucket].saturating_add(1);
        slot_counts[right_bucket] = slot_counts[right_bucket].saturating_add(1);
        splits = splits.saturating_add(1);
        steps.push(format!(
            "split[detq] #{:04} -> #{:04}, #{:04} (+{} chars, Δslot {:+.0}) => {} / {} (slots {} / {})",
            chosen.parent_id,
            chosen.left_id,
            chosen.right_id,
            chosen.extra_cost,
            chosen.distribution_improvement,
            used_chars,
            effective_budget,
            cover.len(),
            target_slots
        ));
    }

    while used_chars > effective_budget && !cover.is_empty() {
        let dropped = cover.remove(0);
        if let Some(node) = by_id.get(&dropped).copied() {
            let cost = cover_turn_cost(node);
            used_chars = used_chars.saturating_sub(cost);
            let bucket =
                age_bucket_for_end_leaf(node.end_leaf, newest_leaf, leaf_count, bucket_count);
            slot_counts[bucket] = slot_counts[bucket].saturating_sub(1);
            dropped_roots = dropped_roots.saturating_add(1);
            steps.push(format!(
                "drop oldest root #{:04} [{}..{}], -{} chars => {} / {}",
                node.id, node.start_leaf, node.end_leaf, cost, used_chars, effective_budget
            ));
        }
    }
    if cover.is_empty() {
        steps.push("cover is empty after drops".to_string());
    }

    if steps.len() == 1 {
        steps.push("no changes needed".to_string());
    }

    CoverSelection {
        cover,
        history_len: 0,
        moment_len: 0,
        used_chars,
        dropped_roots,
        splits,
        steps,
    }
}

fn better_candidate(candidate: Candidate, current: Option<Candidate>) -> bool {
    let Some(current) = current else {
        return true;
    };
    if candidate.distribution_improvement != current.distribution_improvement {
        return candidate.distribution_improvement > current.distribution_improvement;
    }
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

fn render_cover(
    ui: &mut egui::Ui,
    sim: &Simulation,
    selection: &CoverSelection,
    budget_chars: usize,
) {
    let by_id = sim.node_map();
    let nodes: Vec<&SimNode> = selection
        .cover
        .iter()
        .filter_map(|node_id| by_id.get(node_id).copied())
        .collect();
    if nodes.is_empty() {
        ui.label(egui::RichText::new("No cover nodes selected.").italics());
        return;
    }

    let total_span = nodes
        .iter()
        .map(|node| {
            node.end_leaf
                .saturating_sub(node.start_leaf)
                .saturating_add(1)
        })
        .sum::<usize>()
        .max(1);
    let total_cost = nodes
        .iter()
        .map(|node| cover_turn_cost(node))
        .sum::<usize>()
        .max(1);
    let fill_ratio = if budget_chars == 0 {
        0.0
    } else {
        (selection.used_chars as f32 / budget_chars as f32).clamp(0.0, 1.0)
    };

    ui.label(egui::RichText::new("Context fill").monospace());
    ui.add(
        ProgressBar::new(fill_ratio)
            .text(format!(
                "{} / {} chars ({:.1}%)",
                selection.used_chars,
                budget_chars,
                fill_ratio * 100.0
            ))
            .segments(40),
    );
    ui.add_space(6.0);

    enum CoverStripMode {
        Span,
        Equal,
        Cost,
    }

    let draw_strip = |ui: &mut egui::Ui, label: &str, mode: CoverStripMode| {
        ui.label(egui::RichText::new(label).monospace());
        let strip_width = ui.available_width().max(120.0);
        let strip_height = 32.0;
        let (strip_rect, _) =
            ui.allocate_exact_size(egui::vec2(strip_width, strip_height), egui::Sense::hover());
        let painter = ui.painter_at(strip_rect);
        let used_right = strip_rect.left() + strip_rect.width() * fill_ratio;
        let used_rect = egui::Rect::from_min_max(
            strip_rect.left_top(),
            egui::pos2(used_right, strip_rect.bottom()),
        );
        let free_rect = egui::Rect::from_min_max(
            egui::pos2(used_right, strip_rect.top()),
            strip_rect.right_bottom(),
        );
        painter.rect_filled(
            strip_rect.shrink(0.5),
            0.0,
            ui.visuals().widgets.noninteractive.weak_bg_fill,
        );
        if free_rect.width() > 0.0 {
            painter.rect_filled(
                free_rect.shrink(0.5),
                0.0,
                ui.visuals().widgets.inactive.bg_fill,
            );
        }
        painter.rect_stroke(
            strip_rect,
            0.0,
            egui::Stroke::new(1.0, ui.visuals().widgets.noninteractive.fg_stroke.color),
            egui::StrokeKind::Inside,
        );
        if free_rect.width() > 0.0 {
            painter.vline(
                used_right,
                strip_rect.y_range(),
                egui::Stroke::new(1.0, ui.visuals().widgets.active.fg_stroke.color),
            );
        }

        let mut x = strip_rect.left();
        for (idx, node) in nodes.iter().enumerate() {
            let span = node
                .end_leaf
                .saturating_sub(node.start_leaf)
                .saturating_add(1);
            let width = if idx + 1 == nodes.len() {
                used_rect.right() - x
            } else {
                match mode {
                    CoverStripMode::Equal => used_rect.width() / (nodes.len() as f32),
                    CoverStripMode::Span => used_rect.width() * (span as f32) / (total_span as f32),
                    CoverStripMode::Cost => {
                        used_rect.width() * (cover_turn_cost(node) as f32) / (total_cost as f32)
                    }
                }
            };
            if width <= 0.0 {
                continue;
            }
            let seg_rect = egui::Rect::from_min_max(
                egui::pos2(x, strip_rect.top()),
                egui::pos2((x + width).min(used_rect.right()), strip_rect.bottom()),
            );
            if seg_rect.width() <= 0.0 {
                continue;
            }
            let fill = level_color(node.level);
            painter.rect_filled(seg_rect.shrink(0.5), 0.0, fill);
            x = (x + width).min(used_rect.right());
        }

        ui.add_space(2.0);
        ui.horizontal(|ui| {
            ui.label(egui::RichText::new("oldest").monospace());
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                ui.label(egui::RichText::new("newest").monospace())
            });
        });
    };
    draw_strip(ui, "span-weighted cover", CoverStripMode::Span);
    ui.add_space(6.0);
    draw_strip(ui, "char-cost-weighted cover", CoverStripMode::Cost);
    ui.add_space(6.0);
    draw_strip(ui, "equal-width cover", CoverStripMode::Equal);
}

fn build_cover_age_histograms(
    sim: &Simulation,
    selection: &CoverSelection,
    visible_leaves: usize,
) -> (Vec<HistogramBucket<'static>>, Vec<HistogramBucket<'static>>) {
    if visible_leaves == 0 || selection.cover.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let by_id = sim.node_map();
    let bucket_count = visible_leaves.min(AGE_BUCKET_TARGET).max(1);
    let newest_leaf = visible_leaves.saturating_sub(1);
    let mut counts = vec![0u64; bucket_count];
    let mut chars = vec![0u64; bucket_count];

    for node_id in &selection.cover {
        let Some(node) = by_id.get(node_id).copied() else {
            continue;
        };
        let age = newest_leaf.saturating_sub(node.end_leaf);
        let mut bucket = age.saturating_mul(bucket_count) / visible_leaves.max(1);
        if bucket >= bucket_count {
            bucket = bucket_count - 1;
        }
        counts[bucket] = counts[bucket].saturating_add(1);
        chars[bucket] = chars[bucket].saturating_add(cover_turn_cost(node) as u64);
    }

    let mut count_buckets = Vec::with_capacity(bucket_count);
    let mut char_buckets = Vec::with_capacity(bucket_count);
    for bucket_idx in 0..bucket_count {
        let age_start = bucket_idx.saturating_mul(visible_leaves) / bucket_count;
        let age_end = (bucket_idx.saturating_add(1))
            .saturating_mul(visible_leaves)
            .saturating_div(bucket_count)
            .saturating_sub(1);
        let label = if age_start == age_end {
            format!("{age_start}")
        } else {
            format!("{age_start}-{age_end}")
        };
        count_buckets.push(
            HistogramBucket::new(counts[bucket_idx], label.clone()).tooltip(format!(
                "age (leaves ago): {label}\ncover nodes: {}",
                counts[bucket_idx]
            )),
        );
        char_buckets.push(
            HistogramBucket::new(chars[bucket_idx], label.clone()).tooltip(format!(
                "age (leaves ago): {label}\ncover chars: {}",
                chars[bucket_idx]
            )),
        );
    }

    (count_buckets, char_buckets)
}

fn build_target_share_lines(
    char_buckets: &[HistogramBucket<'_>],
) -> (Vec<[f64; 2]>, Vec<[f64; 2]>) {
    if char_buckets.is_empty() {
        return (Vec::new(), Vec::new());
    }

    let actual_total: f64 = char_buckets.iter().map(|bucket| bucket.value as f64).sum();
    let bucket_count = char_buckets.len();
    let target_weights = target_age_weights(bucket_count);
    let target_total: f64 = target_weights.iter().sum();

    let mut actual_points = Vec::with_capacity(bucket_count);
    let mut target_points = Vec::with_capacity(bucket_count);
    for i in 0..bucket_count {
        let x = i as f64;
        let actual = if actual_total > 0.0 {
            char_buckets[i].value as f64 / actual_total
        } else {
            0.0
        };
        let target = if target_total > 0.0 {
            target_weights[i] / target_total
        } else {
            0.0
        };
        actual_points.push([x, actual]);
        target_points.push([x, target]);
    }

    (actual_points, target_points)
}

#[derive(Debug, Clone, Copy)]
struct ChurnSample {
    step: usize,
    cover_len: usize,
    history_cover_len: usize,
    moment_cover_len: usize,
    suffix_churn: usize,
    set_churn: usize,
    prefix_retention: f64,
    history_suffix_churn: usize,
    history_set_churn: usize,
    history_prefix_retention: f64,
    moment_suffix_churn: usize,
    moment_set_churn: usize,
    moment_prefix_retention: f64,
}

#[derive(Debug, Clone, Copy)]
struct ChurnSummary {
    transitions: usize,
    window_start_step: usize,
    avg_history_prefix_retention: f64,
    avg_history_suffix_churn: f64,
    avg_history_set_churn: f64,
    worst_history_suffix_churn: usize,
    worst_history_set_churn: usize,
    avg_moment_prefix_retention: f64,
    avg_moment_suffix_churn: f64,
    avg_moment_set_churn: f64,
}

#[derive(Debug, Clone)]
struct PolicyTrace {
    policy: SelectionPolicy,
    samples: Vec<ChurnSample>,
    summary: Option<ChurnSummary>,
}

impl ChurnSummary {
    fn score(self) -> f64 {
        // Higher is better; sweep score intentionally uses only history metrics.
        (self.avg_history_prefix_retention * 100.0)
            - self.avg_history_suffix_churn
            - (0.5 * self.avg_history_set_churn)
            - (0.2 * self.worst_history_suffix_churn as f64)
    }
}

#[derive(Debug, Clone)]
struct SweepRow {
    label: String,
    policy: SelectionPolicy,
    fill_ratio: Option<f32>,
    safe_quantile: Option<f32>,
    summary: ChurnSummary,
}

#[derive(Debug, Clone, Default)]
struct SweepResults {
    rows: Vec<SweepRow>,
    visible_leaves: usize,
}

#[derive(Debug, Clone)]
struct SweepConfig {
    max_steps: usize,
    quota_fill_min: f32,
    quota_fill_max: f32,
    quota_fill_steps: usize,
    quota_q_min: f32,
    quota_q_max: f32,
    quota_q_steps: usize,
}

impl Default for SweepConfig {
    fn default() -> Self {
        Self {
            max_steps: 256,
            quota_fill_min: 0.70,
            quota_fill_max: 0.95,
            quota_fill_steps: 6,
            quota_q_min: 0.70,
            quota_q_max: 0.99,
            quota_q_steps: 7,
        }
    }
}

#[derive(Debug, Clone, Default)]
struct SweepState {
    cfg: SweepConfig,
    results: Option<SweepResults>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DerivedKey {
    stream_revision: u64,
    visible_leaves: usize,
    reduction_factor: u32,
    context_budget: usize,
    selection_policy: SelectionPolicy,
    det_fill_ratio_bits: u32,
    det_safe_quantile_bits: u32,
    moment_ratio_bits: u32,
    churn_sample_count: usize,
    churn_sampling_mode: TraceSamplingMode,
}

#[derive(Debug)]
struct DerivedData {
    key: Option<DerivedKey>,
    sim: Arc<Simulation>,
    cover: Arc<CoverSelection>,
    churn_samples: Arc<Vec<ChurnSample>>,
    policy_traces: Arc<Vec<PolicyTrace>>,
    job: Option<DerivedJob>,
}

#[derive(Debug, Clone)]
struct DerivedSnapshot {
    stream: Vec<usize>,
    visible_leaves: usize,
    reduction_factor: u32,
    context_budget: usize,
    selection_policy: SelectionPolicy,
    params: CoverPolicyParams,
    churn_sample_count: usize,
    churn_sampling_mode: TraceSamplingMode,
}

#[derive(Debug)]
struct DerivedJobResult {
    key: DerivedKey,
    sim: Simulation,
    cover: CoverSelection,
    churn_samples: Vec<ChurnSample>,
    policy_traces: Vec<PolicyTrace>,
}

#[derive(Debug)]
struct DerivedJob {
    key: DerivedKey,
    progress: Arc<AtomicUsize>,
    done: Arc<AtomicBool>,
    total_units: usize,
    result: Arc<Mutex<Option<DerivedJobResult>>>,
}

impl DerivedSnapshot {
    fn from_state(state: &ViewState) -> Self {
        Self {
            stream: state.stream.clone(),
            visible_leaves: state.visible_leaves,
            reduction_factor: state.reduction_factor,
            context_budget: state.context_budget,
            selection_policy: state.selection_policy,
            params: CoverPolicyParams::from_state(state),
            churn_sample_count: state.churn_sample_count,
            churn_sampling_mode: state.churn_sampling_mode,
        }
    }
}

fn spawn_derived_job(key: DerivedKey, snapshot: DerivedSnapshot) -> DerivedJob {
    let step_points = sampled_steps(
        snapshot.visible_leaves,
        snapshot.churn_sample_count,
        snapshot.churn_sampling_mode,
    );
    let step_units = step_points.len();
    let total_units = step_units
        .saturating_mul(ALL_POLICIES.len())
        .saturating_add(2)
        .max(1);
    let progress = Arc::new(AtomicUsize::new(0));
    let done = Arc::new(AtomicBool::new(false));
    let result = Arc::new(Mutex::new(None));
    let progress_handle = Arc::clone(&progress);
    let done_handle = Arc::clone(&done);
    let result_handle = Arc::clone(&result);

    thread::spawn(move || {
        let sim = simulate(
            snapshot.stream.as_slice(),
            snapshot.visible_leaves,
            snapshot.reduction_factor,
        );
        progress_handle.store(1, Ordering::Relaxed);
        let cover = select_cover(
            &sim,
            snapshot.context_budget,
            snapshot.selection_policy,
            snapshot.params,
        );
        progress_handle.store(2, Ordering::Relaxed);
        let churn_samples = build_churn_trace_for_steps(
            snapshot.stream.as_slice(),
            snapshot.reduction_factor,
            snapshot.context_budget,
            snapshot.selection_policy,
            snapshot.params,
            &step_points,
            Some((&progress_handle, 2)),
        );

        let steady_start = steady_state_min_step(snapshot.visible_leaves);
        let mut policy_traces = Vec::with_capacity(ALL_POLICIES.len());
        let mut progress_base = 2usize.saturating_add(step_units);
        for policy in ALL_POLICIES {
            let samples = if policy == snapshot.selection_policy {
                churn_samples.clone()
            } else {
                let samples = build_churn_trace_for_steps(
                    snapshot.stream.as_slice(),
                    snapshot.reduction_factor,
                    snapshot.context_budget,
                    policy,
                    snapshot.params,
                    &step_points,
                    Some((&progress_handle, progress_base)),
                );
                progress_base = progress_base.saturating_add(step_units);
                samples
            };
            let summary = summarize_churn(&samples, steady_start);
            policy_traces.push(PolicyTrace {
                policy,
                samples,
                summary,
            });
        }

        progress_handle.store(total_units, Ordering::Relaxed);
        if let Ok(mut slot) = result_handle.lock() {
            *slot = Some(DerivedJobResult {
                key,
                sim,
                cover,
                churn_samples,
                policy_traces,
            });
        }
        done_handle.store(true, Ordering::Release);
    });

    DerivedJob {
        key,
        progress,
        done,
        total_units,
        result,
    }
}

impl Default for DerivedData {
    fn default() -> Self {
        Self {
            key: None,
            sim: Arc::new(Simulation::default()),
            cover: Arc::new(CoverSelection::default()),
            churn_samples: Arc::new(Vec::new()),
            policy_traces: Arc::new(Vec::new()),
            job: None,
        }
    }
}

impl DerivedData {
    fn poll_job(&mut self) {
        let done = self
            .job
            .as_ref()
            .is_some_and(|job| job.done.load(Ordering::Acquire));
        if !done {
            return;
        }
        let Some(job) = self.job.take() else {
            return;
        };
        if let Ok(mut slot) = job.result.lock() {
            if let Some(result) = slot.take() {
                self.sim = Arc::new(result.sim);
                self.cover = Arc::new(result.cover);
                self.churn_samples = Arc::new(result.churn_samples);
                self.policy_traces = Arc::new(result.policy_traces);
                self.key = Some(result.key);
            }
        }
    }

    fn progress(&self) -> Option<(usize, usize)> {
        self.job.as_ref().map(|job| {
            (
                job.progress.load(Ordering::Relaxed).min(job.total_units),
                job.total_units.max(1),
            )
        })
    }

    fn refresh(&mut self, state: &ViewState) {
        self.poll_job();
        let key = DerivedKey::from_state(state);
        if self.key == Some(key) || self.job.as_ref().is_some_and(|job| job.key == key) {
            return;
        }
        if self.job.is_none() {
            self.job = Some(spawn_derived_job(key, DerivedSnapshot::from_state(state)));
        }
    }
}

impl DerivedKey {
    fn from_state(state: &ViewState) -> Self {
        Self {
            stream_revision: state.stream_revision,
            visible_leaves: state.visible_leaves,
            reduction_factor: state.reduction_factor,
            context_budget: state.context_budget,
            selection_policy: state.selection_policy,
            det_fill_ratio_bits: state.det_fill_ratio.to_bits(),
            det_safe_quantile_bits: state.det_safe_quantile.to_bits(),
            moment_ratio_bits: state.moment_ratio.to_bits(),
            churn_sample_count: state.churn_sample_count,
            churn_sampling_mode: state.churn_sampling_mode,
        }
    }
}

fn cover_churn(prev: &[u64], next: &[u64]) -> (usize, usize, f64) {
    let prefix_len = prev
        .iter()
        .zip(next.iter())
        .take_while(|(a, b)| a == b)
        .count();
    let suffix_churn = prev.len().max(next.len()).saturating_sub(prefix_len);

    let prev_set: HashSet<u64> = prev.iter().copied().collect();
    let next_set: HashSet<u64> = next.iter().copied().collect();
    let removed = prev_set.difference(&next_set).count();
    let added = next_set.difference(&prev_set).count();
    let set_churn = added.saturating_add(removed);

    let prefix_retention = if prev.is_empty() {
        1.0
    } else {
        prefix_len as f64 / prev.len() as f64
    };
    (suffix_churn, set_churn, prefix_retention)
}

fn unique_sorted_steps(mut steps: Vec<usize>, max_step: usize) -> Vec<usize> {
    for step in &mut steps {
        *step = (*step).clamp(1, max_step);
    }
    steps.sort_unstable();
    steps.dedup();
    if steps.first().copied() != Some(1) {
        steps.insert(0, 1);
    }
    if steps.last().copied() != Some(max_step) {
        steps.push(max_step);
    }
    steps
}

fn sampled_steps(
    visible_leaves: usize,
    sample_count: usize,
    mode: TraceSamplingMode,
) -> Vec<usize> {
    if visible_leaves == 0 {
        return Vec::new();
    }
    if visible_leaves <= 2 {
        return (1..=visible_leaves).collect();
    }

    let k = sample_count.max(2).min(visible_leaves);
    if k >= visible_leaves || mode == TraceSamplingMode::Dense {
        return (1..=visible_leaves).collect();
    }

    match mode {
        TraceSamplingMode::Dense => (1..=visible_leaves).collect(),
        TraceSamplingMode::Uniform => {
            let mut out = Vec::with_capacity(k);
            let denom = (k - 1) as f64;
            for i in 0..k {
                let t = i as f64 / denom;
                let step = 1 + ((visible_leaves - 1) as f64 * t).round() as usize;
                out.push(step);
            }
            unique_sorted_steps(out, visible_leaves)
        }
        TraceSamplingMode::Log => {
            let mut out = Vec::with_capacity(k);
            let max_ln = (visible_leaves as f64).ln();
            let denom = (k - 1) as f64;
            for i in 0..k {
                let t = i as f64 / denom;
                let step = ((1.0 - t) * 1.0f64.ln() + t * max_ln).exp().round() as usize;
                out.push(step);
            }
            unique_sorted_steps(out, visible_leaves)
        }
    }
}

fn sampled_tail_steps(visible_leaves: usize, sample_count: usize) -> Vec<usize> {
    if visible_leaves == 0 {
        return Vec::new();
    }
    let count = sample_count.max(2).min(visible_leaves);
    let start = visible_leaves.saturating_sub(count).saturating_add(1);
    (start..=visible_leaves).collect()
}

fn build_churn_trace_for_steps(
    stream: &[usize],
    reduction_factor: u32,
    context_budget: usize,
    selection_policy: SelectionPolicy,
    params: CoverPolicyParams,
    step_points: &[usize],
    progress: Option<(&AtomicUsize, usize)>,
) -> Vec<ChurnSample> {
    if step_points.is_empty() {
        return Vec::new();
    }
    let mut samples = Vec::with_capacity(step_points.len());
    let mut prev_cover: Vec<u64> = Vec::new();
    let mut prev_history_len = 0usize;
    for (idx, step) in step_points.iter().copied().enumerate() {
        let sim = simulate(stream, step, reduction_factor);
        let selection = select_cover(&sim, context_budget, selection_policy, params);
        let history_len = selection.history_len.min(selection.cover.len());
        let moment_len = selection.cover.len().saturating_sub(history_len);

        let (suffix_churn, set_churn, prefix_retention) = if prev_cover.is_empty() {
            (0, 0, 1.0)
        } else {
            cover_churn(&prev_cover, &selection.cover)
        };
        let (history_suffix_churn, history_set_churn, history_prefix_retention) =
            if prev_cover.is_empty() {
                (0, 0, 1.0)
            } else {
                let prev_history = &prev_cover[..prev_history_len.min(prev_cover.len())];
                let next_history = &selection.cover[..history_len];
                cover_churn(prev_history, next_history)
            };
        let (moment_suffix_churn, moment_set_churn, moment_prefix_retention) =
            if prev_cover.is_empty() {
                (0, 0, 1.0)
            } else {
                let prev_history_end = prev_history_len.min(prev_cover.len());
                let prev_moment = &prev_cover[prev_history_end..];
                let next_moment = &selection.cover[history_len..];
                cover_churn(prev_moment, next_moment)
            };

        samples.push(ChurnSample {
            step,
            cover_len: selection.cover.len(),
            history_cover_len: history_len,
            moment_cover_len: moment_len,
            suffix_churn,
            set_churn,
            prefix_retention,
            history_suffix_churn,
            history_set_churn,
            history_prefix_retention,
            moment_suffix_churn,
            moment_set_churn,
            moment_prefix_retention,
        });
        prev_history_len = history_len;
        prev_cover = selection.cover;
        if let Some((progress, base)) = progress {
            progress.store(
                base.saturating_add(idx).saturating_add(1),
                Ordering::Relaxed,
            );
        }
    }
    samples
}

fn build_churn_trace_with(
    stream: &[usize],
    visible_leaves: usize,
    reduction_factor: u32,
    context_budget: usize,
    selection_policy: SelectionPolicy,
    params: CoverPolicyParams,
    sample_count: usize,
    sampling_mode: TraceSamplingMode,
) -> Vec<ChurnSample> {
    if visible_leaves == 0 {
        return Vec::new();
    }
    let step_points = sampled_steps(visible_leaves, sample_count, sampling_mode);
    build_churn_trace_for_steps(
        stream,
        reduction_factor,
        context_budget,
        selection_policy,
        params,
        &step_points,
        None,
    )
}

fn steady_state_min_step(visible_leaves: usize) -> usize {
    if visible_leaves == 0 {
        return 1;
    }
    ((visible_leaves as f32) * STEADY_STATE_START_RATIO)
        .round()
        .clamp(1.0, visible_leaves as f32) as usize
}

fn evaluation_start_index(samples: &[ChurnSample], min_step: usize) -> usize {
    if samples.len() <= 1 {
        return 0;
    }
    let idx = samples
        .iter()
        .position(|sample| sample.step >= min_step)
        .unwrap_or(samples.len().saturating_sub(1));
    let mut start = idx.saturating_add(1).max(1);
    if start >= samples.len() {
        start = samples.len().saturating_sub(1);
    }
    start
}

fn summarize_churn(samples: &[ChurnSample], min_step: usize) -> Option<ChurnSummary> {
    if samples.len() < 2 {
        return None;
    }
    let start = evaluation_start_index(samples, min_step);
    let transitions = samples.len().saturating_sub(start);
    if transitions == 0 {
        return None;
    }
    let avg_history_prefix_retention = samples
        .iter()
        .skip(start)
        .map(|sample| sample.history_prefix_retention)
        .sum::<f64>()
        / transitions as f64;
    let avg_history_suffix_churn = samples
        .iter()
        .skip(start)
        .map(|sample| sample.history_suffix_churn as f64)
        .sum::<f64>()
        / transitions as f64;
    let avg_history_set_churn = samples
        .iter()
        .skip(start)
        .map(|sample| sample.history_set_churn as f64)
        .sum::<f64>()
        / transitions as f64;
    let worst_history_suffix_churn = samples
        .iter()
        .skip(start)
        .map(|sample| sample.history_suffix_churn)
        .max()
        .unwrap_or(0);
    let worst_history_set_churn = samples
        .iter()
        .skip(start)
        .map(|sample| sample.history_set_churn)
        .max()
        .unwrap_or(0);

    let avg_moment_prefix_retention = samples
        .iter()
        .skip(start)
        .map(|sample| sample.moment_prefix_retention)
        .sum::<f64>()
        / transitions as f64;
    let avg_moment_suffix_churn = samples
        .iter()
        .skip(start)
        .map(|sample| sample.moment_suffix_churn as f64)
        .sum::<f64>()
        / transitions as f64;
    let avg_moment_set_churn = samples
        .iter()
        .skip(start)
        .map(|sample| sample.moment_set_churn as f64)
        .sum::<f64>()
        / transitions as f64;
    Some(ChurnSummary {
        transitions,
        window_start_step: samples.get(start).map(|s| s.step).unwrap_or(min_step),
        avg_history_prefix_retention,
        avg_history_suffix_churn,
        avg_history_set_churn,
        worst_history_suffix_churn,
        worst_history_set_churn,
        avg_moment_prefix_retention,
        avg_moment_suffix_churn,
        avg_moment_set_churn,
    })
}

fn linspace(min: f32, max: f32, steps: usize) -> Vec<f32> {
    if steps <= 1 {
        return vec![min];
    }
    let mut out = Vec::with_capacity(steps);
    let span = max - min;
    let denom = (steps - 1) as f32;
    for i in 0..steps {
        let t = i as f32 / denom;
        out.push(min + span * t);
    }
    out
}

fn evaluate_sweep_row(
    state: &ViewState,
    policy: SelectionPolicy,
    params: CoverPolicyParams,
    visible_leaves: usize,
    sample_count: usize,
    sampling_mode: TraceSamplingMode,
    label: String,
    fill_ratio: Option<f32>,
    safe_quantile: Option<f32>,
) -> Option<SweepRow> {
    let samples = build_churn_trace_with(
        state.stream.as_slice(),
        visible_leaves,
        state.reduction_factor,
        state.context_budget,
        policy,
        params,
        sample_count,
        sampling_mode,
    );
    let summary = summarize_churn(&samples, steady_state_min_step(visible_leaves))?;
    Some(SweepRow {
        label,
        policy,
        fill_ratio,
        safe_quantile,
        summary,
    })
}

fn run_policy_sweep(state: &ViewState, cfg: &SweepConfig) -> SweepResults {
    let visible_leaves = state
        .visible_leaves
        .min(cfg.max_steps.max(2))
        .max(2)
        .min(state.stream.len().max(2));
    let sweep_sample_count = state.churn_sample_count.min(visible_leaves).max(2);
    let sweep_sampling_mode = state.churn_sampling_mode;
    let mut rows = Vec::new();

    if let Some(row) = evaluate_sweep_row(
        state,
        SelectionPolicy::DistributionAware,
        CoverPolicyParams {
            det_fill_ratio: state.det_fill_ratio,
            det_safe_quantile: state.det_safe_quantile,
            moment_ratio: state.moment_ratio,
        },
        visible_leaves,
        sweep_sample_count,
        sweep_sampling_mode,
        "distribution".to_string(),
        None,
        None,
    ) {
        rows.push(row);
    }
    if let Some(row) = evaluate_sweep_row(
        state,
        SelectionPolicy::DeterministicSuffix,
        CoverPolicyParams {
            det_fill_ratio: state.det_fill_ratio,
            det_safe_quantile: state.det_safe_quantile,
            moment_ratio: state.moment_ratio,
        },
        visible_leaves,
        sweep_sample_count,
        sweep_sampling_mode,
        "det-suffix".to_string(),
        None,
        None,
    ) {
        rows.push(row);
    }
    if let Some(row) = evaluate_sweep_row(
        state,
        SelectionPolicy::CurveHistory,
        CoverPolicyParams {
            det_fill_ratio: state.det_fill_ratio,
            det_safe_quantile: state.det_safe_quantile,
            moment_ratio: state.moment_ratio,
        },
        visible_leaves,
        sweep_sample_count,
        sweep_sampling_mode,
        format!("curve-history r={:.2}", state.moment_ratio),
        None,
        None,
    ) {
        rows.push(row);
    }

    let fill_min = cfg.quota_fill_min.min(cfg.quota_fill_max).clamp(0.5, 0.98);
    let fill_max = cfg.quota_fill_min.max(cfg.quota_fill_max).clamp(0.5, 0.98);
    let q_min = cfg.quota_q_min.min(cfg.quota_q_max).clamp(0.5, 0.999);
    let q_max = cfg.quota_q_min.max(cfg.quota_q_max).clamp(0.5, 0.999);
    let fills = linspace(fill_min, fill_max, cfg.quota_fill_steps.max(1));
    let quantiles = linspace(q_min, q_max, cfg.quota_q_steps.max(1));

    for fill in fills {
        for quantile in &quantiles {
            let params = CoverPolicyParams {
                det_fill_ratio: fill,
                det_safe_quantile: *quantile,
                moment_ratio: state.moment_ratio,
            };
            if let Some(row) = evaluate_sweep_row(
                state,
                SelectionPolicy::DeterministicQuotaHeadroom,
                params,
                visible_leaves,
                sweep_sample_count,
                sweep_sampling_mode,
                format!("detq f={fill:.2} q={quantile:.2}"),
                Some(fill),
                Some(*quantile),
            ) {
                rows.push(row);
            }
        }
    }

    rows.sort_by(|a, b| {
        b.summary
            .score()
            .total_cmp(&a.summary.score())
            .then_with(|| {
                b.summary
                    .avg_history_prefix_retention
                    .total_cmp(&a.summary.avg_history_prefix_retention)
            })
            .then_with(|| {
                a.summary
                    .avg_history_suffix_churn
                    .total_cmp(&b.summary.avg_history_suffix_churn)
            })
    });

    SweepResults {
        rows,
        visible_leaves,
    }
}

fn sweep_dominates(a: &SweepRow, b: &SweepRow) -> bool {
    let no_worse = a.summary.avg_history_prefix_retention >= b.summary.avg_history_prefix_retention
        && a.summary.avg_history_suffix_churn <= b.summary.avg_history_suffix_churn
        && a.summary.avg_history_set_churn <= b.summary.avg_history_set_churn;
    let strictly_better = a.summary.avg_history_prefix_retention
        > b.summary.avg_history_prefix_retention
        || a.summary.avg_history_suffix_churn < b.summary.avg_history_suffix_churn
        || a.summary.avg_history_set_churn < b.summary.avg_history_set_churn;
    no_worse && strictly_better
}

fn pareto_front_indices(rows: &[SweepRow]) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, row_i) in rows.iter().enumerate() {
        let dominated = rows
            .iter()
            .enumerate()
            .any(|(j, row_j)| i != j && sweep_dominates(row_j, row_i));
        if !dominated {
            out.push(i);
        }
    }
    out
}

#[notebook]
fn main(nb: &mut NotebookCtx) {
    let bootstrap = notebook_bootstrap();
    static TEXT_MODE_EXITED: AtomicBool = AtomicBool::new(false);
    if bootstrap.args.text_mode && !TEXT_MODE_EXITED.swap(true, Ordering::AcqRel) {
        match run_text_report(bootstrap) {
            Ok(()) => std::process::exit(0),
            Err(err) => {
                eprintln!("text report failed: {err:#}");
                std::process::exit(1);
            }
        }
    }
    let initial_state = initial_state_from_bootstrap(bootstrap);

    if !bootstrap.warnings.is_empty() {
        let warnings = bootstrap.warnings.clone();
        nb.view(move |ui| {
            with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
                ui.heading("Calibration warnings");
                for warning in &warnings {
                    ui.colored_label(egui::Color32::from_rgb(220, 96, 96), warning);
                }
            });
        });
    }

    if let Some(profile) = bootstrap.profile.clone() {
        nb.view(move |ui| {
            with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
                ui.heading("Profile calibration");
                ui.label(format!("{} @ {}", profile.llm.model, profile.llm.base_url));
                ui.label(format!(
                    "context={} tok (configured {}), max_output={} tok, margin={} tok, chars/token={}",
                    profile.llm.context_window_tokens,
                    profile.configured_context_window_tokens,
                    profile.llm.max_output_tokens,
                    profile.llm.prompt_safety_margin_tokens,
                    profile.llm.prompt_chars_per_token
                ));
                if let Some(card_context) = profile.model_card_context_window_tokens {
                    ui.label(format!("model-card context window: {} tok", card_context));
                }
                if has_conservative_output_cap(&profile.llm) {
                    ui.colored_label(
                        egui::Color32::from_rgb(232, 170, 102),
                        "max_output_tokens looks conservative for this context window",
                    );
                }
                ui.label(format!(
                    "prompt budget={} chars",
                    profile.prompt_budget_chars
                ));
                if let Some(branch_id) = profile.context_branch_id {
                    ui.label(format!(
                        "context branch: {} ({:x})",
                        profile.context_branch_name, branch_id
                    ));
                } else {
                    ui.label(format!("context branch: {}", profile.context_branch_name));
                }
                if profile.leaf_count > 0 {
                    ui.label(format!(
                        "leaf summaries: n={} avg={:.1} chars p50={} p90={}",
                        profile.leaf_count,
                        profile.avg_leaf_chars.unwrap_or_default(),
                        profile.p50_leaf_chars.unwrap_or(0),
                        profile.p90_leaf_chars.unwrap_or(0)
                    ));
                    if let Some(avg_tokens) = profile.avg_leaf_tokens_exact {
                        ui.label(format!(
                            "avg tokens/message (tokenizer): {:.2} over {} samples",
                            avg_tokens, profile.tokenized_samples
                        ));
                    } else if let Some(avg_tokens) = profile.avg_leaf_tokens_estimate {
                        ui.label(format!("avg tokens/message (estimate): {:.2}", avg_tokens));
                    }
                } else {
                    ui.label("no level-0 context chunks found yet");
                }
            });
        });
    }

    nb.view(|ui| {
        ui.add_space(4.0);
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "# Compaction Policy Study\n\
## Abstract\n\
This notebook studies context-cover selection over a binary carry compaction tree.\n\
We compare multiple policies under the same budget and stream, asking:\n\
**How can we balance distribution fidelity, prefix stability, and deterministic restart behavior?**\n\
\n\
## Model assumptions\n\
- Indexing: binary carry merges over time-adjacent leaves.\n\
- Selection output: an antichain cover under a strict context budget.\n\
- Objective space: distribution fidelity vs prefix stability vs determinism."
            );
        });
    });

    nb.view(|ui| {
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Experimental protocol\n\
Procedure (per insertion step):\n\
1. Rebuild compacted tree for the visible prefix.\n\
2. Run one selection policy with identical budget.\n\
3. Compare the selected cover against the previous checkpoint (dense or sampled).\n\
\n\
Primary readouts:\n\
- prefix retention (higher is better)\n\
- suffix churn (lower is better)\n\
- set churn (lower is better)"
            );
        });
    });

    let insert = nb.state("lsm-insert", InsertState::default(), |_ui, _insert| {});
    let state = nb.state("lsm-controls", initial_state, move |ui, state| {
        let mut insert_state = insert.read_mut(ui);
        if let Some(job) = insert_state.job.take() {
            if job.done.load(Ordering::Acquire) {
                if let Ok(mut slot) = job.result.lock() {
                    if let Some(result) = slot.take() {
                        state.stream.extend(result.added);
                        state.rng = result.final_rng;
                        state.visible_leaves = state.stream.len();
                        state.stream_revision = state.stream_revision.wrapping_add(1);
                    }
                }
            } else {
                insert_state.job = Some(job);
            }
        }
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            ui.label(egui::RichText::new("Controls").strong());
            let is_generating = insert_state.job.is_some();
            ui.horizontal_wrapped(|ui| {
                ui.label("High-scale presets");
                ui.add_enabled_ui(!is_generating, |ui| {
                    if ui.add(Button::new("100k")).clicked() {
                        state.set_total_inserted(100_000);
                    }
                    if ui.add(Button::new("250k")).clicked() {
                        state.set_total_inserted(250_000);
                    }
                    if ui.add(Button::new("500k")).clicked() {
                        state.set_total_inserted(500_000);
                    }
                    if ui.add(Button::new("1M")).clicked() {
                        state.set_total_inserted(1_000_000);
                    }
                });
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("Total inserted");
                let min_inserted = if state.show_advanced_controls {
                    0usize
                } else {
                    MIN_RELEVANT_INSERTS
                };
                let mut total_inserted = state.stream.len().max(min_inserted);
                ui.add_enabled_ui(!is_generating, |ui| {
                    ui.add(
                        egui::DragValue::new(&mut total_inserted)
                            .range(min_inserted..=MAX_RELEVANT_INSERTS)
                            .speed(128.0),
                    );
                });
                if !is_generating && total_inserted != state.stream.len() {
                    if total_inserted > state.stream.len() {
                        insert_state.job = spawn_insert_job(state, total_inserted);
                    } else {
                        state.set_total_inserted(total_inserted);
                    }
                }
                ui.label(format!("generated: {}", state.stream.len()));
            });
            if let Some(job) = &insert_state.job {
                let generated = job
                    .progress
                    .load(Ordering::Relaxed)
                    .min(job.target_len.saturating_sub(job.start_len));
                let total = job.target_len.saturating_sub(job.start_len).max(1);
                let ratio = (generated as f32 / total as f32).clamp(0.0, 1.0);
                ui.add(ProgressBar::new(ratio).segments(40).text(format!(
                    "Generating stream: +{} / +{} leaves ({} -> {})",
                    generated, total, job.start_len, job.target_len
                )));
                ui.ctx().request_repaint_after(Duration::from_millis(33));
            }
            ui.horizontal_wrapped(|ui| {
                ui.label("Messages shown");
                let max_visible = state.stream.len();
                ui.add(Slider::new(&mut state.visible_leaves, 0..=max_visible).text("messages"));
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("Selection policy");
                ui.add(
                    ChoiceToggle::new(&mut state.selection_policy)
                        .choice(SelectionPolicy::DistributionAware, "distribution")
                        .choice(SelectionPolicy::DeterministicSuffix, "det-suffix")
                        .choice(
                            SelectionPolicy::DeterministicQuotaHeadroom,
                            "det-quota-headroom",
                        )
                        .choice(SelectionPolicy::CurveHistory, "curve-history")
                        .small(),
                );
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("Moment ratio");
                ui.add(Slider::new(&mut state.moment_ratio, 0.05..=0.8).fixed_decimals(2));
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("Context budget");
                let max_budget = state
                    .context_budget
                    .max(20_000)
                    .saturating_mul(2)
                    .clamp(20_000, 4_000_000);
                ui.add(Slider::new(&mut state.context_budget, 200..=max_budget).text("chars"));
                ui.add(
                    egui::DragValue::new(&mut state.context_budget)
                        .range(200..=4_000_000)
                        .speed(256.0),
                );
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("Churn sampling");
                ui.add(
                    ChoiceToggle::new(&mut state.churn_sampling_mode)
                        .choice(TraceSamplingMode::Dense, "dense")
                        .choice(TraceSamplingMode::Uniform, "uniform")
                        .choice(TraceSamplingMode::Log, "log")
                        .small(),
                );
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("Churn sample count");
                ui.add(Slider::new(&mut state.churn_sample_count, 8..=2048).text("checkpoints"));
                ui.label(format!("mode: {}", state.churn_sampling_mode.label()));
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("Advanced controls");
                ui.add(ChoiceToggle::binary(
                    &mut state.show_advanced_controls,
                    "OFF",
                    "ON",
                ));
                if !state.show_advanced_controls {
                    ui.label(format!(
                        "steady-state mode (min inserts: {})",
                        MIN_RELEVANT_INSERTS
                    ));
                }
            });

            if state.show_advanced_controls {
                ui.add_space(6.0);
                ui.horizontal_wrapped(|ui| {
                    ui.label("Base leaf size");
                    ui.add(Slider::new(&mut state.base_leaf_size, 16..=1024).text("chars"));
                });
                ui.horizontal_wrapped(|ui| {
                    ui.label("Reduction factor");
                    ui.add(Slider::new(&mut state.reduction_factor, 2..=8).text("merge"));
                    ui.add_space(8.0);
                    ui.label("Jitter");
                    ui.add(ChoiceToggle::binary(&mut state.jitter, "OFF", "ON"));
                });
            }
            if state.show_advanced_controls
                || state.selection_policy == SelectionPolicy::DeterministicQuotaHeadroom
            {
                ui.horizontal_wrapped(|ui| {
                    ui.label("Deterministic fill");
                    ui.add(Slider::new(&mut state.det_fill_ratio, 0.5..=0.98).fixed_decimals(2));
                });
                ui.horizontal_wrapped(|ui| {
                    ui.label("Safe quantile");
                    ui.add(
                        Slider::new(&mut state.det_safe_quantile, 0.5..=0.999).fixed_decimals(3),
                    );
                });
            }
        });
    });

    let derived = nb.state("lsm-derived", DerivedData::default(), move |ui, derived| {
        let state = state.read(ui);
        derived.refresh(&state);
    });
    let sweep = nb.state("lsm-sweep", SweepState::default(), |_ui, _sweep| {});

    nb.view(move |ui| {
        let state_ref = state.read(ui);
        let progress = {
            let mut derived = derived.read_mut(ui);
            derived.refresh(&state_ref);
            derived.progress()
        };
        let Some((done, total)) = progress else {
            return;
        };
        let ratio = (done as f32 / total as f32).clamp(0.0, 1.0);
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            ui.label(egui::RichText::new("Recomputing metrics").strong());
            ui.add(
                ProgressBar::new(ratio)
                    .segments(40)
                    .text(format!("{} / {} units", done, total)),
            );
            ui.ctx().request_repaint_after(Duration::from_millis(33));
        });
    });

    nb.view(move |ui| {
        {
            let state_ref = state.read(ui);
            let mut derived = derived.read_mut(ui);
            derived.refresh(&state_ref);
        }
        let sim = {
            let derived = derived.read(ui);
            Arc::clone(&derived.sim)
        };
        let state_snapshot = state.read(ui).clone();
        let params = CoverPolicyParams::from_state(&state_snapshot);
        let effective_budget = ((state_snapshot.context_budget as f32) * params.det_fill_ratio)
            .round()
            .clamp(1.0, state_snapshot.context_budget as f32)
            as usize;
        let mut costs: Vec<usize> = sim.nodes.iter().map(cover_turn_cost).collect();
        costs.sort_unstable();
        let safe_cost = quantile_ceil(&costs, params.det_safe_quantile).max(1);
        let target_slots = (effective_budget / safe_cost).max(1);

        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Method sheet\n\
A compact summary of the active run configuration.\n\
\n\
_Hypothesis: deterministic quota + headroom should reduce churn while preserving budget safety._\n\
\n\
`N={}`  `reduction={}`  `budget={}`\n\
`policy={}`\n\
`churn_sampling={} ({})`\n\
`steady_state_start={:.0}%`\n\
`moment_ratio={:.2}`\n\
`detq: fill={:.2}  q={:.2}  effective_budget={}  safe_cost={}  target_slots={}`",
                state_snapshot.visible_leaves,
                state_snapshot.reduction_factor,
                state_snapshot.context_budget,
                state_snapshot.selection_policy.label(),
                state_snapshot.churn_sampling_mode.label(),
                state_snapshot.churn_sample_count,
                STEADY_STATE_START_RATIO * 100.0,
                state_snapshot.moment_ratio,
                params.det_fill_ratio,
                params.det_safe_quantile,
                effective_budget,
                safe_cost,
                target_slots
            );
        });
    });

    nb.view(move |ui| {
        let selected_policy = state.read(ui).selection_policy;
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Approaches\n\
Each policy receives the same inputs (`frontier`, budget, target-age weights). The only difference is the split-selection rule.\n\
\n\
**Active policy:** `{}`",
                selected_policy.label()
            );
        });
    });

    nb.view(move |ui| {
        let active = state.read(ui).selection_policy == SelectionPolicy::DistributionAware;
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Policy: `distribution` {}\n\
**Objective:** Minimize age-distribution error while fitting budget.\n\
\n\
**Shared pre-step (all policies):** reserve a raw-leaf **moment** slice from the newest events; this policy optimizes only the remaining **history** budget.\n\
\n\
**Core intuition:** treat cover refinement as an optimization problem.\n\
\n\
**Algorithm (detailed):**\n\
1. Build initial cover from frontier roots ordered oldest -> newest.\n\
2. Compute `used_chars`; while over budget, drop oldest roots.\n\
3. Enumerate every eligible split candidate:\n\
   - candidate must have two children,\n\
   - extra cost must fit remaining budget,\n\
   - split must preserve level monotonicity in cover order.\n\
4. For each candidate, project bucket chars and evaluate `Δerror = current_error - projected_error`.\n\
5. Select max `Δerror` (tie-break by recency, then cost, then index/id).\n\
6. Apply split, update bucket counts and repeat until no improving split fits.\n\
\n\
**Properties:**\n\
- Best age-share fit among current options.\n\
- Can rewrite older sections if that improves objective.\n\
\n\
**Failure mode:** noisy churn when many candidates have similar objective value.\n\
\n\
**Example:** if `[A,B,C,D]` and one split fits, it may choose `B -> (B1,B2)` over `D -> (D1,D2)` when that better fixes target histogram error.",
                if active { " (active)" } else { "" }
            );
        });
    });

    nb.view(move |ui| {
        let active = state.read(ui).selection_policy == SelectionPolicy::DeterministicSuffix;
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Policy: `deterministic-suffix` {}\n\
**Objective:** Maximize prefix stability and deterministic replay.\n\
\n\
**Shared pre-step (all policies):** reserve a raw-leaf **moment** slice from the newest events; this policy optimizes only the remaining **history** budget.\n\
\n\
**Core intuition:** keep historical prefix fixed; spend detail budget at the newest tail first.\n\
\n\
**Algorithm (detailed):**\n\
1. Build initial cover from frontier roots ordered oldest -> newest.\n\
2. Drop oldest roots until budget fits.\n\
3. While budget headroom exists:\n\
   - scan cover from right to left,\n\
   - take the first split that is valid (children exist, cost fits, monotonicity preserved),\n\
   - apply immediately,\n\
   - restart right-to-left scan.\n\
4. Stop when no valid split remains.\n\
\n\
**Why this yields stable prefixes:**\n\
- Edits are concentrated at rightmost splittable nodes,\n\
- left side changes only when right side cannot absorb additional detail.\n\
\n\
**Tradeoff:** not globally optimal for target distribution; explicitly prioritizes structural stability over distribution fit.\n\
\n\
**Example trace:** `[A,B,C,D] -> [A,B,C,D1,D2]`; if `D` cannot split, next tries `C`, then `B`, then `A`.",
                if active { " (active)" } else { "" }
            );
        });
    });

    nb.view(move |ui| {
        let active =
            state.read(ui).selection_policy == SelectionPolicy::DeterministicQuotaHeadroom;
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Policy: `deterministic-quota-headroom` {}\n\
**Objective:** Deterministic selection with explicit headroom and risk control.\n\
\n\
**Shared pre-step (all policies):** reserve a raw-leaf **moment** slice from the newest events; this policy optimizes only the remaining **history** budget.\n\
\n\
**Core intuition:** convert budget to a conservative slot plan, then accept only deficit-improving splits.\n\
\n\
**Algorithm (detailed):**\n\
1. Compute `effective_budget = budget * fill_ratio`.\n\
2. Estimate `safe_cost = quantile(node_costs, q)`.\n\
3. Derive `target_slots = floor(effective_budget / safe_cost)`.\n\
4. Map age-weight targets to integer slot quotas.\n\
5. Start with frontier cover, drop oldest until `used <= effective_budget`.\n\
6. Consider right-to-left split candidates; a split is accepted only if:\n\
   - cost fits remaining headroom,\n\
   - monotonicity holds,\n\
   - projected slot deficit strictly improves.\n\
7. Stop on no improvement.\n\
\n\
**Properties:**\n\
- Deterministic under fixed inputs,\n\
- less chance of operating near hard budget edge,\n\
- explicit control via `fill_ratio` and `q`.\n\
\n\
**Tradeoff:** can leave unused budget and may reject locally-good splits if they hurt global quota balance.",
                if active { " (active)" } else { "" }
            );
        });
    });

    nb.view(move |ui| {
        let active = state.read(ui).selection_policy == SelectionPolicy::CurveHistory;
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Policy: `curve-history` {}\n\
**Objective:** deterministic age-curve history cover under a fixed history budget.\n\
\n\
**Shared pre-step (all policies):** reserve a raw-leaf **moment** slice from the newest events; this policy optimizes only the remaining **history** budget.\n\
\n\
**Core intuition:** enforce an age->resolution curve over history, then spend leftover headroom via deterministic newest-first refinement.\n\
\n\
**Algorithm (detailed):**\n\
1. Build an initial history cover from frontier roots clipped to leaves older than moment.\n\
2. Fit a quantized curve scale `s` from a fixed ladder; choose smallest `s` where history fits.\n\
3. Enforce curve constraint by splitting violating history nodes:\n\
   `span(node) <= s * 2^floor(log2(age(node)+1))`.\n\
4. Use remaining history headroom for deterministic newest-first refinement splits.\n\
5. If still over budget (rare), drop oldest history nodes as a last resort.\n\
\n\
**Properties:**\n\
- deterministic replay,\n\
- high budget use without random suffix churn,\n\
- geometric coarse-to-fine bias by age.\n\
\n\
**Tradeoff:** this optimizes stability and interpretability, not direct histogram-error minimization.",
                if active { " (active)" } else { "" }
            );
        });
    });

    nb.view(move |ui| {
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Metrics\n\
Churn and stability are measured between consecutive insertion steps.\n\
\n\
`prefix_retention = lcp(prev_cover, next_cover) / len(prev_cover)`\n\
`suffix_churn = max(len(prev_cover), len(next_cover)) - lcp(prev_cover, next_cover)`\n\
`set_churn = |prev \\\\ next| + |next \\\\ prev|`\n\
\n\
Interpretation: high prefix retention with low churn indicates stronger turn-to-turn context stability."
            );
        });
    });

    nb.view(move |ui| {
        {
            let state_ref = state.read(ui);
            let mut derived = derived.read_mut(ui);
            derived.refresh(&state_ref);
        }
        let (policy_traces, visible_leaves) = {
            let derived = derived.read(ui);
            let visible_leaves = state.read(ui).visible_leaves;
            (Arc::clone(&derived.policy_traces), visible_leaves)
        };

        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            ui.label(egui::RichText::new("Cross-policy comparison").strong());
            ui.label(
                "All approaches are computed from the same stream, budget, and sampling schedule.",
            );
            if policy_traces.is_empty() {
                ui.label(egui::RichText::new("No policy traces yet.").italics());
                return;
            }

            let mut by_policy: HashMap<SelectionPolicy, &PolicyTrace> = HashMap::new();
            for trace in policy_traces.iter() {
                by_policy.insert(trace.policy, trace);
            }
            let steady_start = steady_state_min_step(visible_leaves);
            ui.label(format!(
                "steady-state window starts at step >= {} ({:.0}% of N)",
                steady_start,
                STEADY_STATE_START_RATIO * 100.0
            ));

            let mut best_policy: Option<(SelectionPolicy, f64)> = None;
            for policy in ALL_POLICIES {
                if let Some(trace) = by_policy.get(&policy) {
                    if let Some(summary) = trace.summary {
                        let score = summary.score();
                        if best_policy.is_none_or(|(_, best)| score > best) {
                            best_policy = Some((policy, score));
                        }
                    }
                }
            }
            if let Some((policy, score)) = best_policy {
                ui.label(format!(
                    "best history score: {} ({:.2})",
                    policy.label(),
                    score
                ));
            }

            ui.add_space(4.0);
            egui::Grid::new("all_policy_summary_grid")
                .striped(true)
                .show(ui, |ui| {
                    ui.label(egui::RichText::new("policy").monospace());
                    ui.label(egui::RichText::new("history prefix").monospace());
                    ui.label(egui::RichText::new("history suffix").monospace());
                    ui.label(egui::RichText::new("history set").monospace());
                    ui.label(egui::RichText::new("moment prefix").monospace());
                    ui.label(egui::RichText::new("moment suffix").monospace());
                    ui.label(egui::RichText::new("score").monospace());
                    ui.end_row();

                    for policy in ALL_POLICIES {
                        let Some(trace) = by_policy.get(&policy) else {
                            continue;
                        };
                        let label = if best_policy.is_some_and(|(best, _)| best == policy) {
                            format!("{} *", policy.label())
                        } else {
                            policy.label().to_string()
                        };
                        ui.colored_label(policy_color(policy), label);
                        if let Some(summary) = trace.summary {
                            ui.label(
                                egui::RichText::new(format!(
                                    "{:.1}%",
                                    summary.avg_history_prefix_retention * 100.0
                                ))
                                .monospace(),
                            );
                            ui.label(
                                egui::RichText::new(format!(
                                    "{:.2}",
                                    summary.avg_history_suffix_churn
                                ))
                                .monospace(),
                            );
                            ui.label(
                                egui::RichText::new(format!(
                                    "{:.2}",
                                    summary.avg_history_set_churn
                                ))
                                .monospace(),
                            );
                            ui.label(
                                egui::RichText::new(format!(
                                    "{:.1}%",
                                    summary.avg_moment_prefix_retention * 100.0
                                ))
                                .monospace(),
                            );
                            ui.label(
                                egui::RichText::new(format!(
                                    "{:.2}",
                                    summary.avg_moment_suffix_churn
                                ))
                                .monospace(),
                            );
                            ui.label(
                                egui::RichText::new(format!("{:.2}", summary.score())).monospace(),
                            );
                        } else {
                            ui.label("-");
                            ui.label("-");
                            ui.label("-");
                            ui.label("-");
                            ui.label("-");
                            ui.label("-");
                        }
                        ui.end_row();
                    }
                });

            ui.add_space(6.0);
            ui.label(egui::RichText::new("History suffix churn (all policies)").monospace());
            ui.push_id("all_policy_history_suffix_plot", |ui| {
                Plot::new("all_policy_history_suffix_plot")
                    .height(170.0)
                    .legend(Legend::default())
                    .include_y(0.0)
                    .show(ui, |plot_ui| {
                        for policy in ALL_POLICIES {
                            let Some(trace) = by_policy.get(&policy) else {
                                continue;
                            };
                            let points: Vec<[f64; 2]> = trace
                                .samples
                                .iter()
                                .map(|sample| {
                                    [sample.step as f64, sample.history_suffix_churn as f64]
                                })
                                .collect();
                            if points.is_empty() {
                                continue;
                            }
                            plot_ui.line(
                                Line::new(policy.label(), PlotPoints::from(points))
                                    .color(policy_color(policy)),
                            );
                        }
                    });
            });

            ui.add_space(6.0);
            ui.label(egui::RichText::new("History prefix retention (all policies)").monospace());
            ui.push_id("all_policy_history_prefix_plot", |ui| {
                Plot::new("all_policy_history_prefix_plot")
                    .height(170.0)
                    .legend(Legend::default())
                    .include_y(0.0)
                    .include_y(1.0)
                    .show(ui, |plot_ui| {
                        for policy in ALL_POLICIES {
                            let Some(trace) = by_policy.get(&policy) else {
                                continue;
                            };
                            let points: Vec<[f64; 2]> = trace
                                .samples
                                .iter()
                                .map(|sample| [sample.step as f64, sample.history_prefix_retention])
                                .collect();
                            if points.is_empty() {
                                continue;
                            }
                            plot_ui.line(
                                Line::new(policy.label(), PlotPoints::from(points))
                                    .color(policy_color(policy)),
                            );
                        }
                    });
            });
        });
    });

    nb.view(move |ui| {
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Terminology\n\
- **frontier**: one active root per compaction level after carry merges\n\
- **cover**: selected antichain of nodes sent into context\n\
- **prefix**: oldest (left) part of the cover in time order\n\
- **suffix**: newest (right) tail of the cover in time order\n\
- **moment**: newest raw leaves reserved as high-detail \"now\" context\n\
\n\
In `deterministic-suffix`, split attempts start at the right edge, so updates concentrate on the suffix while preserving the prefix when possible."
            );
        });
    });

    nb.view(move |ui| {
        let state_snapshot = state.read(ui).clone();
        let mut sweep = sweep.read_mut(ui);
        let visible_limit = state_snapshot.visible_leaves.max(2);
        let stream_limit = state_snapshot.stream.len().max(2);
        sweep.cfg.max_steps = sweep.cfg.max_steps.clamp(2, stream_limit);

        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Parameter sweep\n\
Grid-search over policies and deterministic quota parameters to compare stability/churn under identical stream and budget.\n\
Higher `score` is better (**history** stability/churn only; moment excluded).\n\
\n\
`score = 100*avg_history_prefix_retention - avg_history_suffix_churn - 0.5*avg_history_set_churn - 0.2*worst_history_suffix_churn`\n\
Window: steady-state only, from `step >= ceil(N * {:.2})`.",
                STEADY_STATE_START_RATIO
            );

            ui.horizontal_wrapped(|ui| {
                ui.label("Max steps");
                ui.add(Slider::new(&mut sweep.cfg.max_steps, 2..=visible_limit).text("messages"));
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("fill steps");
                ui.add(Slider::new(&mut sweep.cfg.quota_fill_steps, 1..=16).text("n"));
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("q steps");
                ui.add(Slider::new(&mut sweep.cfg.quota_q_steps, 1..=16).text("n"));
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("fill min");
                ui.add(
                    Slider::new(&mut sweep.cfg.quota_fill_min, 0.5..=0.98).fixed_decimals(2),
                );
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("fill max");
                ui.add(
                    Slider::new(&mut sweep.cfg.quota_fill_max, 0.5..=0.98).fixed_decimals(2),
                );
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("q min");
                ui.add(
                    Slider::new(&mut sweep.cfg.quota_q_min, 0.5..=0.999).fixed_decimals(3),
                );
            });
            ui.horizontal_wrapped(|ui| {
                ui.label("q max");
                ui.add(
                    Slider::new(&mut sweep.cfg.quota_q_max, 0.5..=0.999).fixed_decimals(3),
                );
            });

            ui.add_space(4.0);
            ui.horizontal(|ui| {
                if ui.add(Button::new("Run sweep")).clicked() {
                    sweep.results = Some(run_policy_sweep(&state_snapshot, &sweep.cfg));
                }
                if ui.add(Button::new("Clear")).clicked() {
                    sweep.results = None;
                }
            });

            let Some(results) = &sweep.results else {
                ui.add_space(6.0);
                ui.label(egui::RichText::new("No sweep results yet.").italics());
                return;
            };
            if results.rows.is_empty() {
                ui.add_space(6.0);
                ui.label(egui::RichText::new("Sweep produced no valid rows.").italics());
                return;
            }

            ui.add_space(6.0);
            let best = &results.rows[0];
            let pareto_idx = pareto_front_indices(&results.rows);
            let pareto_set: HashSet<usize> = pareto_idx.iter().copied().collect();
            md!(
                ui,
                "**Best candidate:** `{}`  \n\
score `{:.2}`  \n\
history avg prefix `{:.1}%`, history avg suffix `{:.2}`, history avg set `{:.2}`  \n\
moment avg prefix `{:.1}%`, moment avg suffix `{:.2}`, moment avg set `{:.2}`  \n\
evaluated at `N={}` steps (steady-state from step `{}` / {:.0}%). Pareto-front size: `{}`.",
                best.label,
                best.summary.score(),
                best.summary.avg_history_prefix_retention * 100.0,
                best.summary.avg_history_suffix_churn,
                best.summary.avg_history_set_churn,
                best.summary.avg_moment_prefix_retention * 100.0,
                best.summary.avg_moment_suffix_churn,
                best.summary.avg_moment_set_churn,
                results.visible_leaves,
                best.summary.window_start_step,
                STEADY_STATE_START_RATIO * 100.0,
                pareto_idx.len()
            );

            let mut dist_points = Vec::new();
            let mut det_suffix_points = Vec::new();
            let mut det_quota_points = Vec::new();
            let mut curve_history_points = Vec::new();
            let mut pareto_points = Vec::new();
            for (idx, row) in results.rows.iter().enumerate() {
                let point = [
                    row.summary.avg_history_suffix_churn,
                    row.summary.avg_history_prefix_retention * 100.0,
                ];
                match row.policy {
                    SelectionPolicy::DistributionAware => dist_points.push(point),
                    SelectionPolicy::DeterministicSuffix => det_suffix_points.push(point),
                    SelectionPolicy::DeterministicQuotaHeadroom => det_quota_points.push(point),
                    SelectionPolicy::CurveHistory => curve_history_points.push(point),
                }
                if pareto_set.contains(&idx) {
                    pareto_points.push(point);
                }
            }

            ui.add_space(4.0);
            ui.label(
                egui::RichText::new("Pareto view (history retention vs history churn)")
                    .monospace(),
            );
            ui.push_id("lsm_sweep_pareto_plot", |ui| {
                Plot::new("lsm_sweep_pareto_plot")
                    .height(180.0)
                    .legend(Legend::default())
                    .include_x(0.0)
                    .include_y(0.0)
                    .include_y(100.0)
                    .show(ui, |plot_ui| {
                        plot_ui.points(
                            Points::new("distribution", PlotPoints::from(dist_points))
                                .shape(MarkerShape::Circle)
                                .radius(3.0)
                                .color(policy_color(SelectionPolicy::DistributionAware)),
                        );
                        plot_ui.points(
                            Points::new("det-suffix", PlotPoints::from(det_suffix_points))
                                .shape(MarkerShape::Square)
                                .radius(3.0)
                                .color(policy_color(SelectionPolicy::DeterministicSuffix)),
                        );
                        plot_ui.points(
                            Points::new("det-quota", PlotPoints::from(det_quota_points))
                                .shape(MarkerShape::Diamond)
                                .radius(3.0)
                                .color(policy_color(
                                    SelectionPolicy::DeterministicQuotaHeadroom,
                                )),
                        );
                        plot_ui.points(
                            Points::new("curve-history", PlotPoints::from(curve_history_points))
                                .shape(MarkerShape::Cross)
                                .radius(3.0)
                                .color(policy_color(SelectionPolicy::CurveHistory)),
                        );
                        plot_ui.points(
                            Points::new("pareto", PlotPoints::from(pareto_points))
                                .shape(MarkerShape::Asterisk)
                                .radius(5.0)
                                .color(egui::Color32::WHITE),
                        );
                    });
            });

            ui.add_space(4.0);
            egui::ScrollArea::vertical()
                .id_salt("lsm_sweep_rows")
                .max_height(280.0)
                .show(ui, |ui| {
                    for (idx, row) in results.rows.iter().enumerate() {
                        let params = match (row.fill_ratio, row.safe_quantile) {
                            (Some(fill), Some(q)) => format!(" f={fill:.2} q={q:.2}"),
                            _ => String::new(),
                        };
                        let marker = if pareto_set.contains(&idx) { "*" } else { " " };
                        ui.label(
                            egui::RichText::new(format!(
                                "{} {:>2}. {:<24} {:<32} score {:>7.2}  h_pref {:>5.1}%  h_suf {:>5.2}  h_set {:>5.2}  h_worst({:>2}/{:>2})  m_pref {:>5.1}%  m_suf {:>5.2}  m_set {:>5.2}  n={}  from={}",
                                marker,
                                idx + 1,
                                row.policy.label(),
                                format!("{}{}", row.label, params),
                                row.summary.score(),
                                row.summary.avg_history_prefix_retention * 100.0,
                                row.summary.avg_history_suffix_churn,
                                row.summary.avg_history_set_churn,
                                row.summary.worst_history_suffix_churn,
                                row.summary.worst_history_set_churn,
                                row.summary.avg_moment_prefix_retention * 100.0,
                                row.summary.avg_moment_suffix_churn,
                                row.summary.avg_moment_set_churn,
                                row.summary.transitions,
                                row.summary.window_start_step,
                            ))
                            .monospace(),
                        );
                    }
                });
        });
    });

    nb.view(move |ui| {
        {
            let state_ref = state.read(ui);
            let mut derived = derived.read_mut(ui);
            derived.refresh(&state_ref);
        }
        let (sim, cover) = {
            let derived = derived.read(ui);
            (Arc::clone(&derived.sim), Arc::clone(&derived.cover))
        };
        let (visible_leaves, selection_policy) = {
            let s = state.read(ui);
            (s.visible_leaves, s.selection_policy)
        };

        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Results overview\n\
- leaves: `{}`\n\
- policy: `{}`\n\
- nodes: `{}`\n\
- merges: `{}`\n\
- frontier roots: `{}`\n\
- input size: `{}`\n\
- frontier size: `{}`\n\
- cover size: `{}`\n\
- history nodes in cover: `{}`\n\
- moment nodes in cover: `{}`",
                visible_leaves,
                selection_policy.label(),
                sim.nodes.len(),
                sim.merges,
                sim.roots_by_level.len(),
                sim.input_size,
                sim.frontier_size(),
                cover.used_chars,
                cover.history_len,
                cover.moment_len
            );
        });
    });

    nb.view(move |ui| {
        {
            let state_ref = state.read(ui);
            let mut derived = derived.read_mut(ui);
            derived.refresh(&state_ref);
        }
        let (sim, cover) = {
            let derived = derived.read(ui);
            (Arc::clone(&derived.sim), Arc::clone(&derived.cover))
        };
        let (context_budget, selection_policy) = {
            let s = state.read(ui);
            (s.context_budget, s.selection_policy)
        };
        let (det_fill, det_quantile) = {
            let s = state.read(ui);
            (s.det_fill_ratio, s.det_safe_quantile)
        };
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            ui.label(egui::RichText::new("Selected context cover").strong());
            ui.label("All policies share the same moment split (newest raw leaves); policy only shapes history.");
            match selection_policy {
                SelectionPolicy::DistributionAware => {
                    ui.label("Policy: distribution history. Drop oldest history nodes to fit budget, then split the eligible node that best improves target age-distribution fit.")
                }
                SelectionPolicy::DeterministicSuffix => {
                    ui.label("Policy: deterministic suffix history. Drop oldest history nodes to fit budget, then split the newest eligible node (right-to-left scan) for prefix stability.")
                }
                SelectionPolicy::DeterministicQuotaHeadroom => {
                    ui.label(format!(
                        "Policy: deterministic quota + headroom history. Fill target {:.0}% and safe cost quantile q={:.2}; split only when it improves slot deficits.",
                        det_fill * 100.0,
                        det_quantile
                    ))
                }
                SelectionPolicy::CurveHistory => {
                    ui.label("Policy: curve history. Fit a quantized age-curve scale for history and refine newest history nodes under remaining headroom.")
                }
            };
            render_cover(ui, &sim, &cover, context_budget);
        });
    });

    nb.view(move |ui| {
        {
            let state_ref = state.read(ui);
            let mut derived = derived.read_mut(ui);
            derived.refresh(&state_ref);
        }
        let samples = {
            let derived = derived.read(ui);
            Arc::clone(&derived.churn_samples)
        };
        let (selection_policy, sampling_mode, sample_count, visible_leaves) = {
            let s = state.read(ui);
            (
                s.selection_policy,
                s.churn_sampling_mode,
                s.churn_sample_count,
                s.visible_leaves,
            )
        };
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            ui.label(egui::RichText::new("Auto-insert churn").strong());
            ui.label(format!(
                "Policy: {}. Checkpoints: {} mode, up to {} points over {} visible messages.",
                selection_policy.label(),
                sampling_mode.label(),
                sample_count,
                visible_leaves
            ));
            if samples.len() < 2 {
                ui.label(egui::RichText::new("Need at least 2 visible messages.").italics());
                return;
            }

            let window_start_step = steady_state_min_step(visible_leaves);
            let eval_start = evaluation_start_index(&samples, window_start_step);
            let transitions = samples.len().saturating_sub(eval_start).max(1);
            let avg_suffix = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.suffix_churn as f64)
                .sum::<f64>()
                / transitions as f64;
            let avg_set = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.set_churn as f64)
                .sum::<f64>()
                / transitions as f64;
            let avg_history_set = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.history_set_churn as f64)
                .sum::<f64>()
                / transitions as f64;
            let avg_moment_set = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.moment_set_churn as f64)
                .sum::<f64>()
                / transitions as f64;
            let avg_prefix = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.prefix_retention)
                .sum::<f64>()
                / transitions as f64;
            let avg_history_prefix = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.history_prefix_retention)
                .sum::<f64>()
                / transitions as f64;
            let avg_moment_prefix = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.moment_prefix_retention)
                .sum::<f64>()
                / transitions as f64;
            let max_suffix = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.suffix_churn)
                .max()
                .unwrap_or(0);
            let max_history_suffix = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.history_suffix_churn)
                .max()
                .unwrap_or(0);
            let max_moment_suffix = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.moment_suffix_churn)
                .max()
                .unwrap_or(0);
            let max_set = samples
                .iter()
                .skip(eval_start)
                .map(|sample| sample.set_churn)
                .max()
                .unwrap_or(0);

            ui.horizontal_wrapped(|ui| {
                ui.label(format!(
                    "steady-state window starts at step >= {} (ratio {:.0}%)",
                    window_start_step,
                    STEADY_STATE_START_RATIO * 100.0
                ));
                ui.separator();
                ui.label(format!("transitions evaluated: {}", transitions));
                ui.separator();
                ui.label(format!("avg suffix churn: {:.2}", avg_suffix));
                ui.separator();
                ui.label(format!("avg set churn: {:.2}", avg_set));
                ui.separator();
                ui.label(format!("avg prefix retention: {:.1}%", avg_prefix * 100.0));
                ui.separator();
                ui.label(format!(
                    "history avg set: {:.2} (prefix {:.1}%)",
                    avg_history_set,
                    avg_history_prefix * 100.0
                ));
                ui.separator();
                ui.label(format!(
                    "moment avg set: {:.2} (prefix {:.1}%)",
                    avg_moment_set,
                    avg_moment_prefix * 100.0
                ));
                ui.separator();
                ui.label(format!("worst suffix churn: {}", max_suffix));
                ui.separator();
                ui.label(format!(
                    "worst split suffix (h/m): {}/{}",
                    max_history_suffix, max_moment_suffix
                ));
                ui.separator();
                ui.label(format!("worst set churn: {}", max_set));
            });

            let suffix_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.suffix_churn as f64])
                .collect();
            let set_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.set_churn as f64])
                .collect();
            let cover_size_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.cover_len as f64])
                .collect();
            let prefix_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.prefix_retention])
                .collect();
            let history_set_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.history_set_churn as f64])
                .collect();
            let moment_set_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.moment_set_churn as f64])
                .collect();
            let history_suffix_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.history_suffix_churn as f64])
                .collect();
            let moment_suffix_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.moment_suffix_churn as f64])
                .collect();
            let history_len_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.history_cover_len as f64])
                .collect();
            let moment_len_points: Vec<[f64; 2]> = samples
                .iter()
                .map(|sample| [sample.step as f64, sample.moment_cover_len as f64])
                .collect();

            ui.add_space(6.0);
            ui.label(egui::RichText::new("Node churn per insertion").monospace());
            ui.push_id("cover_churn_plot", |ui| {
                Plot::new("cover_churn_plot")
                    .height(150.0)
                    .legend(Legend::default())
                    .include_y(0.0)
                    .show(ui, |plot_ui| {
                        plot_ui.line(Line::new("suffix churn", PlotPoints::from(suffix_points)));
                        plot_ui.line(Line::new("set churn", PlotPoints::from(set_points)));
                        plot_ui.line(Line::new("cover size", PlotPoints::from(cover_size_points)));
                    });
            });

            ui.add_space(6.0);
            ui.label(egui::RichText::new("History vs moment churn").monospace());
            ui.push_id("cover_split_churn_plot", |ui| {
                Plot::new("cover_split_churn_plot")
                    .height(150.0)
                    .legend(Legend::default())
                    .include_y(0.0)
                    .show(ui, |plot_ui| {
                        plot_ui.line(Line::new(
                            "history set churn",
                            PlotPoints::from(history_set_points),
                        ));
                        plot_ui.line(Line::new(
                            "moment set churn",
                            PlotPoints::from(moment_set_points),
                        ));
                        plot_ui.line(Line::new(
                            "history suffix churn",
                            PlotPoints::from(history_suffix_points),
                        ));
                        plot_ui.line(Line::new(
                            "moment suffix churn",
                            PlotPoints::from(moment_suffix_points),
                        ));
                    });
            });

            ui.add_space(6.0);
            ui.label(egui::RichText::new("History vs moment cover size").monospace());
            ui.push_id("cover_split_size_plot", |ui| {
                Plot::new("cover_split_size_plot")
                    .height(120.0)
                    .legend(Legend::default())
                    .include_y(0.0)
                    .show(ui, |plot_ui| {
                        plot_ui.line(Line::new(
                            "history nodes",
                            PlotPoints::from(history_len_points),
                        ));
                        plot_ui.line(Line::new(
                            "moment nodes",
                            PlotPoints::from(moment_len_points),
                        ));
                    });
            });

            ui.add_space(6.0);
            ui.label(egui::RichText::new("Prefix retention per insertion").monospace());
            ui.push_id("cover_prefix_retention_plot", |ui| {
                Plot::new("cover_prefix_retention_plot")
                    .height(120.0)
                    .include_y(0.0)
                    .include_y(1.0)
                    .show(ui, |plot_ui| {
                        plot_ui.line(Line::new(
                            "prefix retention",
                            PlotPoints::from(prefix_points),
                        ));
                    });
            });
        });
    });

    nb.view(move |ui| {
        {
            let state_ref = state.read(ui);
            let mut derived = derived.read_mut(ui);
            derived.refresh(&state_ref);
        }
        let (sim, cover) = {
            let derived = derived.read(ui);
            (Arc::clone(&derived.sim), Arc::clone(&derived.cover))
        };
        let visible_leaves = state.read(ui).visible_leaves;
        let (count_buckets, char_buckets) =
            build_cover_age_histograms(&sim, &cover, visible_leaves);
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            ui.label(egui::RichText::new("Cover age distribution").strong());
            ui.label("Buckets are in leaves-ago (0 = newest).");
            if count_buckets.is_empty() {
                ui.label(egui::RichText::new("No selected cover yet.").italics());
                return;
            }

            ui.label(egui::RichText::new("Cover nodes by age").monospace());
            ui.push_id("cover_age_count_hist", |ui| {
                ui.add(
                    Histogram::new(&count_buckets, HistogramYAxis::Count)
                        .plot_height(96.0)
                        .max_x_labels(8),
                );
            });
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Cover chars by age").monospace());
            ui.push_id("cover_age_chars_hist", |ui| {
                ui.add(
                    Histogram::new(&char_buckets, HistogramYAxis::Count)
                        .plot_height(96.0)
                        .max_x_labels(8),
                );
            });
            ui.add_space(8.0);
            ui.label(egui::RichText::new("Char-share target vs actual").monospace());
            let (actual_points, target_points) = build_target_share_lines(&char_buckets);
            ui.push_id("cover_age_target_plot", |ui| {
                Plot::new("cover_age_target_plot")
                    .height(140.0)
                    .legend(Legend::default())
                    .include_y(0.0)
                    .include_y(1.0)
                    .show(ui, |plot_ui| {
                        plot_ui.line(Line::new("actual", PlotPoints::from(actual_points)));
                        plot_ui.line(Line::new("target", PlotPoints::from(target_points)));
                    });
            });
        });
    });

    nb.view(move |ui| {
        {
            let state_ref = state.read(ui);
            let mut derived = derived.read_mut(ui);
            derived.refresh(&state_ref);
        }
        let cover = {
            let derived = derived.read(ui);
            Arc::clone(&derived.cover)
        };
        let (context_budget, selection_policy) = {
            let s = state.read(ui);
            (s.context_budget, s.selection_policy)
        };
        with_padding(ui, DEFAULT_CARD_PADDING, |ui| {
            md!(
                ui,
                "## Selection trace\n\
Stepwise decision log for the active policy.\n\
\n\
- budget: `{}`\n\
- policy: `{}`\n\
- used: `{}`\n\
- dropped_roots: `{}`\n\
- splits: `{}`",
                context_budget,
                selection_policy.label(),
                cover.used_chars,
                cover.dropped_roots,
                cover.splits
            );
            ui.add_space(4.0);
            egui::ScrollArea::vertical()
                .id_salt("lsm_selection_trace")
                .max_height(220.0)
                .show(ui, |ui| {
                    let start = cover.steps.len().saturating_sub(TRACE_RENDER_LIMIT);
                    if start > 0 {
                        ui.label(
                            egui::RichText::new(format!(
                                "... {} earlier trace lines omitted ...",
                                start
                            ))
                            .monospace(),
                        );
                    }
                    for step in cover.steps.iter().skip(start) {
                        ui.label(egui::RichText::new(step).monospace());
                    }
                });
        });
    });
}
