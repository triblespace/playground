use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::metadata;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

use crate::branch_util::ensure_branch;
use crate::repo_ops::push_workspace;
use crate::schema::playground_config;
use crate::time_util::{epoch_interval, interval_key, now_epoch};

const DEFAULT_MODEL: &str = "gpt-oss:120b";
const DEFAULT_BASE_URL: &str = "http://localhost:11434/v1";
const DEFAULT_STREAM: bool = false;
const DEFAULT_CONTEXT_WINDOW_TOKENS: u64 = 32 * 1024;
const DEFAULT_MAX_OUTPUT_TOKENS: u64 = 1024;
const DEFAULT_PROMPT_SAFETY_MARGIN_TOKENS: u64 = 512;
const DEFAULT_PROMPT_CHARS_PER_TOKEN: u64 = 4;
const DEFAULT_COMPACTION_REDUCTION_FACTOR: u64 = 3;
const DEFAULT_SYSTEM_PROMPT: &str = "You are a terminal-based agent. Respond with exactly one shell command per turn. Output only raw command text: no markdown fences, no commentary prelude, no channel labels, and no multi-command blocks. Prefer faculties (available on PATH) over ad-hoc shell when applicable; run a faculty with no arguments to inspect usage. If unsure what to do next, run `orient show`.";
// The branch that carries the core cognition loop + exec/LLM request state.
const DEFAULT_BRANCH: &str = "cognition";
const DEFAULT_EXEC_BRANCH: &str = "cognition";
const DEFAULT_COMPASS_BRANCH: &str = "compass";
const DEFAULT_LOCAL_MESSAGES_BRANCH: &str = "local-messages";
const DEFAULT_RELATIONS_BRANCH: &str = "relations";
const DEFAULT_TEAMS_BRANCH: &str = "teams";
const DEFAULT_WORKSPACE_BRANCH: &str = "workspace";
const DEFAULT_ARCHIVE_BRANCH: &str = "archive";
const DEFAULT_WEB_BRANCH: &str = "web";
const DEFAULT_MEDIA_BRANCH: &str = "media";
const DEFAULT_AUTHOR: &str = "agent";
const DEFAULT_AUTHOR_ROLE: &str = "user";
const DEFAULT_POLL_MS: u64 = 1;
const DEFAULT_PILE_PATH: &str = "self.pile";
const CONFIG_BRANCH: &str = "config";
#[allow(non_upper_case_globals)]
const CONFIG_BRANCH_ID: Id = id_hex!("4790808CF044F979FC7C2E47FCCB4A64");

#[derive(Clone, Debug)]
pub struct Config {
    pub pile_path: PathBuf,
    pub llm: LlmConfig,
    pub llm_profile_id: Option<Id>,
    pub llm_profile_name: String,
    pub llm_compaction_profile_id: Option<Id>,
    pub llm_compaction_prompt: Option<String>,
    pub llm_compaction_reduction_factor: u64,
    pub tavily_api_key: Option<String>,
    pub exa_api_key: Option<String>,
    pub exec: ExecConfig,
    pub system_prompt: String,
    pub branch_id: Option<Id>,
    pub branch: String,
    pub compass_branch_id: Option<Id>,
    pub exec_branch_id: Option<Id>,
    pub local_messages_branch_id: Option<Id>,
    pub relations_branch_id: Option<Id>,
    pub teams_branch_id: Option<Id>,
    pub workspace_branch_id: Option<Id>,
    pub archive_branch_id: Option<Id>,
    pub web_branch_id: Option<Id>,
    pub media_branch_id: Option<Id>,
    pub author: String,
    pub author_role: String,
    pub persona_id: Option<Id>,
    pub poll_ms: u64,
}

#[derive(Clone, Debug)]
pub struct LlmConfig {
    pub model: String,
    pub base_url: String,
    pub api_key: Option<String>,
    pub reasoning_effort: Option<String>,
    pub stream: bool,
    pub context_window_tokens: u64,
    pub max_output_tokens: u64,
    pub prompt_safety_margin_tokens: u64,
    pub prompt_chars_per_token: u64,
}

#[derive(Clone, Debug)]
pub struct ExecConfig {
    pub default_cwd: Option<PathBuf>,
    pub sandbox_profile: Option<Id>,
}

#[derive(Clone, Debug)]
pub struct LlmProfileSummary {
    pub id: Id,
    pub name: String,
}

impl Default for ExecConfig {
    fn default() -> Self {
        Self {
            default_cwd: Some(PathBuf::from("/workspace")),
            sandbox_profile: None,
        }
    }
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self {
            model: default_model(),
            base_url: default_base_url(),
            api_key: None,
            reasoning_effort: None,
            stream: default_stream(),
            context_window_tokens: default_context_window_tokens(),
            max_output_tokens: default_max_output_tokens(),
            prompt_safety_margin_tokens: default_prompt_safety_margin_tokens(),
            prompt_chars_per_token: default_prompt_chars_per_token(),
        }
    }
}

impl Config {
    pub fn load(explicit: Option<&Path>) -> Result<Self> {
        let pile_path = explicit
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(DEFAULT_PILE_PATH));
        let mut config = load_from_pile(&pile_path)?;
        config.pile_path = pile_path;
        Ok(config)
    }

    #[allow(dead_code)]
    pub fn store(&self) -> Result<()> {
        let (mut repo, branch_id) = open_config_repo(&self.pile_path)?;
        let result = (|| -> Result<()> {
            let mut ws = repo
                .pull(branch_id)
                .map_err(|err| anyhow!("pull config workspace: {err:?}"))?;
            store_config(&mut ws, self).context("store config")?;
            push_workspace(&mut repo, &mut ws).context("push config")?;
            Ok(())
        })();

        if let Err(err) = close_repo(repo).context("close config pile") {
            if result.is_ok() {
                return Err(err);
            }
            eprintln!("warning: failed to close pile cleanly: {err:#}");
        }

        result
    }
}

pub fn list_llm_profiles(pile_path: &Path) -> Result<Vec<LlmProfileSummary>> {
    let (mut repo, branch_id) = open_config_repo(pile_path)?;
    let result = (|| -> Result<Vec<LlmProfileSummary>> {
        let mut ws = repo
            .pull(branch_id)
            .map_err(|err| anyhow!("pull config workspace: {err:?}"))?;
        let catalog = ws.checkout(..).context("checkout config workspace")?;

        let mut latest: HashMap<Id, (Id, i128)> = HashMap::new();
        for (entry_id, profile_id, updated_at) in find!(
            (entry_id: Id, profile_id: Value<GenId>, updated_at: Value<NsTAIInterval>),
            pattern!(&catalog, [{
                ?entry_id @
                playground_config::kind: playground_config::kind_llm_profile,
                playground_config::updated_at: ?updated_at,
                playground_config::llm_profile_id: ?profile_id,
            }])
        ) {
            let profile_id = Id::from_value(&profile_id);
            let key = interval_key(updated_at);
            latest
                .entry(profile_id)
                .and_modify(|slot| {
                    if key > slot.1 {
                        *slot = (entry_id, key);
                    }
                })
                .or_insert((entry_id, key));
        }

        let mut profiles = Vec::new();
        for (profile_id, (entry_id, _updated_key)) in latest {
            let name = load_string_attr(&mut ws, &catalog, entry_id, metadata::name)?
                .unwrap_or_else(|| format!("profile-{profile_id:x}"));
            profiles.push(LlmProfileSummary {
                id: profile_id,
                name,
            });
        }
        profiles.sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
        Ok(profiles)
    })();

    if let Err(err) = close_repo(repo).context("close config pile") {
        if result.is_ok() {
            return Err(err);
        }
        eprintln!("warning: failed to close pile cleanly: {err:#}");
    }

    result
}

pub fn load_llm_profile(pile_path: &Path, profile_id: Id) -> Result<Option<(LlmConfig, String)>> {
    let (mut repo, branch_id) = open_config_repo(pile_path)?;
    let result = (|| -> Result<Option<(LlmConfig, String)>> {
        let mut ws = repo
            .pull(branch_id)
            .map_err(|err| anyhow!("pull config workspace: {err:?}"))?;
        let catalog = ws.checkout(..).context("checkout config workspace")?;
        load_latest_llm_profile(&mut ws, &catalog, profile_id)
    })();

    if let Err(err) = close_repo(repo).context("close config pile") {
        if result.is_ok() {
            return Err(err);
        }
        eprintln!("warning: failed to close pile cleanly: {err:#}");
    }

    result
}

fn default_config(pile_path: PathBuf) -> Config {
    Config {
        pile_path,
        llm: LlmConfig::default(),
        llm_profile_id: None,
        llm_profile_name: "default".to_string(),
        llm_compaction_profile_id: None,
        llm_compaction_prompt: None,
        llm_compaction_reduction_factor: default_compaction_reduction_factor(),
        tavily_api_key: None,
        exa_api_key: None,
        exec: ExecConfig::default(),
        system_prompt: default_system_prompt(),
        branch_id: None,
        branch: default_branch(),
        compass_branch_id: None,
        exec_branch_id: None,
        local_messages_branch_id: None,
        relations_branch_id: None,
        teams_branch_id: None,
        workspace_branch_id: None,
        archive_branch_id: None,
        web_branch_id: None,
        media_branch_id: None,
        author: default_author(),
        author_role: default_author_role(),
        persona_id: None,
        poll_ms: default_poll_ms(),
    }
}

fn load_from_pile(pile_path: &Path) -> Result<Config> {
    let (mut repo, branch_id) = open_config_repo(pile_path)?;
    let result = (|| -> Result<Config> {
        let mut ws = repo
            .pull(branch_id)
            .map_err(|err| anyhow!("pull config workspace: {err:?}"))?;
        let catalog = ws.checkout(..).context("checkout config workspace")?;

        let mut config = if let Some(config) = load_latest_config(&mut ws, &catalog, pile_path)? {
            config
        } else {
            default_config(pile_path.to_path_buf())
        };

        let ids_changed = ensure_registered_branch_ids(&mut config);
        if ids_changed {
            store_config(&mut ws, &config).context("store config with branch ids")?;
            push_workspace(&mut repo, &mut ws).context("push config with branch ids")?;
        }
        ensure_registered_branches_exist(&mut repo, &config)?;
        Ok(config)
    })();

    if let Err(err) = close_repo(repo).context("close config pile") {
        if result.is_ok() {
            return Err(err);
        }
        eprintln!("warning: failed to close pile cleanly: {err:#}");
    }

    result
}

fn open_config_repo(pile_path: &Path) -> Result<(Repository<Pile>, Id)> {
    if let Some(parent) = pile_path.parent() {
        fs::create_dir_all(parent).context("create pile directory")?;
    }
    let mut pile = Pile::open(pile_path).context("open pile")?;
    if let Err(err) = pile.restore().context("restore pile") {
        let close_res = pile.close().context("close pile after restore failure");
        if let Err(close_err) = close_res {
            eprintln!("warning: failed to close pile cleanly: {close_err:#}");
        }
        return Err(err);
    }

    let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
    let branch_id = match ensure_config_branch(&mut repo) {
        Ok(branch_id) => branch_id,
        Err(err) => {
            let close_res = repo.close().context("close pile after init failure");
            if let Err(close_err) = close_res {
                eprintln!("warning: failed to close pile cleanly: {close_err:#}");
            }
            return Err(err);
        }
    };
    Ok((repo, branch_id))
}

fn ensure_config_branch(repo: &mut Repository<Pile>) -> Result<Id> {
    ensure_branch(repo, CONFIG_BRANCH_ID, CONFIG_BRANCH)
        .context("materialize fixed config branch")?;
    Ok(CONFIG_BRANCH_ID)
}

fn ensure_registered_branch_ids(config: &mut Config) -> bool {
    let mut changed = false;

    changed |= ensure_registered_branch_id(&mut config.branch_id);
    changed |= ensure_registered_branch_id(&mut config.exec_branch_id);
    changed |= ensure_registered_branch_id(&mut config.compass_branch_id);
    changed |= ensure_registered_branch_id(&mut config.local_messages_branch_id);
    changed |= ensure_registered_branch_id(&mut config.relations_branch_id);
    changed |= ensure_registered_branch_id(&mut config.workspace_branch_id);
    changed |= ensure_registered_branch_id(&mut config.archive_branch_id);
    changed |= ensure_registered_branch_id(&mut config.web_branch_id);
    changed |= ensure_registered_branch_id(&mut config.media_branch_id);
    changed |= ensure_registered_llm_profile_id(&mut config.llm_profile_id);

    changed
}

fn ensure_registered_branch_id(slot: &mut Option<Id>) -> bool {
    if slot.is_some() {
        return false;
    }
    *slot = Some(*genid());
    true
}

fn ensure_registered_llm_profile_id(slot: &mut Option<Id>) -> bool {
    if slot.is_some() {
        return false;
    }
    *slot = Some(*genid());
    true
}

fn ensure_registered_branches_exist(repo: &mut Repository<Pile>, config: &Config) -> Result<()> {
    let required = [
        (config.branch_id, config.branch.as_str()),
        (config.exec_branch_id, DEFAULT_EXEC_BRANCH),
        (config.compass_branch_id, DEFAULT_COMPASS_BRANCH),
        (
            config.local_messages_branch_id,
            DEFAULT_LOCAL_MESSAGES_BRANCH,
        ),
        (config.relations_branch_id, DEFAULT_RELATIONS_BRANCH),
        (config.workspace_branch_id, DEFAULT_WORKSPACE_BRANCH),
        (config.archive_branch_id, DEFAULT_ARCHIVE_BRANCH),
        (config.web_branch_id, DEFAULT_WEB_BRANCH),
        (config.media_branch_id, DEFAULT_MEDIA_BRANCH),
    ];

    for (id, name) in required {
        let id = id.ok_or_else(|| anyhow!("config missing id for branch '{name}'"))?;
        ensure_branch(repo, id, name)
            .with_context(|| format!("materialize branch '{name}' ({id:x})"))?;
    }

    // Optional integrations: allow a persona to omit the branch id to disable the facility.
    if let Some(id) = config.teams_branch_id {
        ensure_branch(repo, id, DEFAULT_TEAMS_BRANCH).with_context(|| {
            format!(
                "materialize branch '{name}' ({id:x})",
                name = DEFAULT_TEAMS_BRANCH
            )
        })?;
    }
    Ok(())
}

fn close_repo(repo: Repository<Pile>) -> Result<()> {
    repo.into_storage().close().context("close pile")
}

fn load_latest_config(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    pile_path: &Path,
) -> Result<Option<Config>> {
    let mut latest: Option<(Id, Value<NsTAIInterval>)> = None;

    for (config_id, updated_at) in find!(
        (config_id: Id, updated_at: Value<NsTAIInterval>),
        pattern!(catalog, [{
            ?config_id @
            playground_config::kind: playground_config::kind_config,
            playground_config::updated_at: ?updated_at,
        }])
    ) {
        let key = interval_key(updated_at);
        match latest {
            Some((_, current)) if interval_key(current) >= key => {}
            _ => latest = Some((config_id, updated_at)),
        }
    }

    let Some((config_id, _)) = latest else {
        return Ok(None);
    };

    let mut config = default_config(pile_path.to_path_buf());

    if let Some(prompt) =
        load_string_attr(ws, catalog, config_id, playground_config::system_prompt)?
    {
        config.system_prompt = prompt;
    }
    if let Some(branch) = load_string_attr(ws, catalog, config_id, playground_config::branch)? {
        config.branch = branch;
    }
    if let Some(author) = load_string_attr(ws, catalog, config_id, playground_config::author)? {
        config.author = author;
    }
    if let Some(role) = load_string_attr(ws, catalog, config_id, playground_config::author_role)? {
        config.author_role = role;
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::persona_id) {
        config.persona_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::active_llm_profile_id) {
        config.llm_profile_id = Some(id);
    }
    if let Some(id) = load_id_attr(
        catalog,
        config_id,
        playground_config::active_llm_compaction_profile_id,
    ) {
        config.llm_compaction_profile_id = Some(id);
    }
    if let Some(prompt) = load_string_attr(
        ws,
        catalog,
        config_id,
        playground_config::llm_compaction_prompt,
    )? {
        config.llm_compaction_prompt = Some(prompt);
    }
    if let Some(model) = load_string_attr(ws, catalog, config_id, playground_config::llm_model)? {
        config.llm.model = model;
    }
    if let Some(url) = load_string_attr(ws, catalog, config_id, playground_config::llm_base_url)? {
        config.llm.base_url = url;
    }
    if let Some(effort) = load_string_attr(
        ws,
        catalog,
        config_id,
        playground_config::llm_reasoning_effort,
    )? {
        config.llm.reasoning_effort = Some(effort);
    }
    if let Some(key) = load_string_attr(ws, catalog, config_id, playground_config::llm_api_key)? {
        config.llm.api_key = Some(key);
    }
    if let Some(key) = load_string_attr(ws, catalog, config_id, playground_config::tavily_api_key)?
    {
        config.tavily_api_key = Some(key);
    }
    if let Some(key) = load_string_attr(ws, catalog, config_id, playground_config::exa_api_key)? {
        config.exa_api_key = Some(key);
    }
    if let Some(cwd) =
        load_string_attr(ws, catalog, config_id, playground_config::exec_default_cwd)?
    {
        config.exec.default_cwd = Some(PathBuf::from(cwd));
    }

    if let Some(id) = load_id_attr(catalog, config_id, playground_config::branch_id) {
        config.branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::compass_branch_id) {
        config.compass_branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::exec_branch_id) {
        config.exec_branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(
        catalog,
        config_id,
        playground_config::local_messages_branch_id,
    ) {
        config.local_messages_branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::relations_branch_id) {
        config.relations_branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::teams_branch_id) {
        config.teams_branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::workspace_branch_id) {
        config.workspace_branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::archive_branch_id) {
        config.archive_branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::web_branch_id) {
        config.web_branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::media_branch_id) {
        config.media_branch_id = Some(id);
    }
    if let Some(id) = load_id_attr(catalog, config_id, playground_config::exec_sandbox_profile) {
        config.exec.sandbox_profile = Some(id);
    }
    if let Some(poll_ms) =
        load_u256_attr(catalog, config_id, playground_config::poll_ms).and_then(u256be_to_u64)
    {
        config.poll_ms = poll_ms;
    }
    if let Some(stream) =
        load_u256_attr(catalog, config_id, playground_config::llm_stream).and_then(u256be_to_u64)
    {
        config.llm.stream = stream != 0;
    }
    if let Some(tokens) = load_u256_attr(
        catalog,
        config_id,
        playground_config::llm_context_window_tokens,
    )
    .and_then(u256be_to_u64)
    {
        config.llm.context_window_tokens = tokens;
    }
    if let Some(tokens) =
        load_u256_attr(catalog, config_id, playground_config::llm_max_output_tokens)
            .and_then(u256be_to_u64)
    {
        config.llm.max_output_tokens = tokens;
    }
    if let Some(tokens) = load_u256_attr(
        catalog,
        config_id,
        playground_config::llm_prompt_safety_margin_tokens,
    )
    .and_then(u256be_to_u64)
    {
        config.llm.prompt_safety_margin_tokens = tokens;
    }
    if let Some(chars) = load_u256_attr(
        catalog,
        config_id,
        playground_config::llm_prompt_chars_per_token,
    )
    .and_then(u256be_to_u64)
    {
        config.llm.prompt_chars_per_token = chars;
    }
    if let Some(factor) = load_u256_attr(
        catalog,
        config_id,
        playground_config::llm_compaction_reduction_factor,
    )
    .and_then(u256be_to_u64)
    {
        config.llm_compaction_reduction_factor = factor.max(1);
    }

    if let Some(profile_id) = config.llm_profile_id {
        if let Some((llm, name)) = load_latest_llm_profile(ws, catalog, profile_id)? {
            config.llm = llm;
            config.llm_profile_name = name;
        }
    }

    Ok(Some(config))
}

fn load_latest_llm_profile(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    profile_id: Id,
) -> Result<Option<(LlmConfig, String)>> {
    let mut latest: Option<(Id, Value<NsTAIInterval>)> = None;

    for (entry_id, updated_at) in find!(
        (entry_id: Id, updated_at: Value<NsTAIInterval>),
        pattern!(catalog, [{
            ?entry_id @
            playground_config::kind: playground_config::kind_llm_profile,
            playground_config::updated_at: ?updated_at,
            playground_config::llm_profile_id: profile_id,
        }])
    ) {
        let key = interval_key(updated_at);
        match latest {
            Some((_, current)) if interval_key(current) >= key => {}
            _ => latest = Some((entry_id, updated_at)),
        }
    }

    let Some((entry_id, _)) = latest else {
        return Ok(None);
    };

    let mut llm = LlmConfig::default();
    if let Some(model) = load_string_attr(ws, catalog, entry_id, playground_config::llm_model)? {
        llm.model = model;
    }
    if let Some(url) = load_string_attr(ws, catalog, entry_id, playground_config::llm_base_url)? {
        llm.base_url = url;
    }
    if let Some(effort) = load_string_attr(
        ws,
        catalog,
        entry_id,
        playground_config::llm_reasoning_effort,
    )? {
        llm.reasoning_effort = Some(effort);
    }
    if let Some(key) = load_string_attr(ws, catalog, entry_id, playground_config::llm_api_key)? {
        llm.api_key = Some(key);
    }
    if let Some(stream) =
        load_u256_attr(catalog, entry_id, playground_config::llm_stream).and_then(u256be_to_u64)
    {
        llm.stream = stream != 0;
    }
    if let Some(tokens) = load_u256_attr(
        catalog,
        entry_id,
        playground_config::llm_context_window_tokens,
    )
    .and_then(u256be_to_u64)
    {
        llm.context_window_tokens = tokens;
    }
    if let Some(tokens) =
        load_u256_attr(catalog, entry_id, playground_config::llm_max_output_tokens)
            .and_then(u256be_to_u64)
    {
        llm.max_output_tokens = tokens;
    }
    if let Some(tokens) = load_u256_attr(
        catalog,
        entry_id,
        playground_config::llm_prompt_safety_margin_tokens,
    )
    .and_then(u256be_to_u64)
    {
        llm.prompt_safety_margin_tokens = tokens;
    }
    if let Some(chars) = load_u256_attr(
        catalog,
        entry_id,
        playground_config::llm_prompt_chars_per_token,
    )
    .and_then(u256be_to_u64)
    {
        llm.prompt_chars_per_token = chars;
    }
    let name = load_string_attr(ws, catalog, entry_id, metadata::name)?
        .unwrap_or_else(|| format!("profile-{profile_id:x}"));
    Ok(Some((llm, name)))
}

fn store_config(ws: &mut Workspace<Pile>, config: &Config) -> Result<()> {
    let now = epoch_interval(now_epoch());
    let config_id = ufoid();
    let profile_id = config
        .llm_profile_id
        .ok_or_else(|| anyhow!("config missing active LLM profile id"))?;

    let system_prompt = ws.put(config.system_prompt.clone());
    let branch = ws.put(config.branch.clone());
    let author = ws.put(config.author.clone());
    let author_role = ws.put(config.author_role.clone());
    let poll_ms: Value<U256BE> = config.poll_ms.to_value();

    let mut change = TribleSet::new();
    change += entity! { &config_id @
        playground_config::kind: playground_config::kind_config,
        playground_config::updated_at: now,
        playground_config::system_prompt: system_prompt,
        playground_config::branch: branch,
        playground_config::author: author,
        playground_config::author_role: author_role,
        playground_config::poll_ms: poll_ms,
        playground_config::active_llm_profile_id: profile_id,
    };
    let compaction_reduction_factor: Value<U256BE> =
        config.llm_compaction_reduction_factor.max(1).to_value();
    change += entity! { &config_id @
        playground_config::llm_compaction_reduction_factor: compaction_reduction_factor,
    };

    if let Some(id) = config.branch_id {
        change += entity! { &config_id @ playground_config::branch_id: id };
    }
    if let Some(id) = config.compass_branch_id {
        change += entity! { &config_id @ playground_config::compass_branch_id: id };
    }
    if let Some(id) = config.exec_branch_id {
        change += entity! { &config_id @ playground_config::exec_branch_id: id };
    }
    if let Some(id) = config.local_messages_branch_id {
        change += entity! { &config_id @ playground_config::local_messages_branch_id: id };
    }
    if let Some(id) = config.relations_branch_id {
        change += entity! { &config_id @ playground_config::relations_branch_id: id };
    }
    if let Some(id) = config.teams_branch_id {
        change += entity! { &config_id @ playground_config::teams_branch_id: id };
    }
    if let Some(id) = config.workspace_branch_id {
        change += entity! { &config_id @ playground_config::workspace_branch_id: id };
    }
    if let Some(id) = config.archive_branch_id {
        change += entity! { &config_id @ playground_config::archive_branch_id: id };
    }
    if let Some(id) = config.web_branch_id {
        change += entity! { &config_id @ playground_config::web_branch_id: id };
    }
    if let Some(id) = config.media_branch_id {
        change += entity! { &config_id @ playground_config::media_branch_id: id };
    }
    if let Some(id) = config.persona_id {
        change += entity! { &config_id @ playground_config::persona_id: id };
    }
    if let Some(id) = config.llm_compaction_profile_id {
        change += entity! { &config_id @ playground_config::active_llm_compaction_profile_id: id };
    }
    if let Some(prompt) = config.llm_compaction_prompt.as_ref() {
        let handle = ws.put(prompt.clone());
        change += entity! { &config_id @ playground_config::llm_compaction_prompt: handle };
    }
    if let Some(key) = config.tavily_api_key.as_ref() {
        let handle = ws.put(key.clone());
        change += entity! { &config_id @ playground_config::tavily_api_key: handle };
    }
    if let Some(key) = config.exa_api_key.as_ref() {
        let handle = ws.put(key.clone());
        change += entity! { &config_id @ playground_config::exa_api_key: handle };
    }
    if let Some(cwd) = config.exec.default_cwd.as_ref() {
        let handle = ws.put(cwd.to_string_lossy().to_string());
        change += entity! { &config_id @ playground_config::exec_default_cwd: handle };
    }
    if let Some(profile) = config.exec.sandbox_profile {
        change += entity! { &config_id @ playground_config::exec_sandbox_profile: profile };
    }

    let profile_entry_id = ufoid();
    let profile_name = ws.put(config.llm_profile_name.clone());
    let llm_model = ws.put(config.llm.model.clone());
    let llm_base_url = ws.put(config.llm.base_url.clone());
    let llm_stream: Value<U256BE> = if config.llm.stream { 1u64 } else { 0u64 }.to_value();
    let llm_context_window_tokens: Value<U256BE> = config.llm.context_window_tokens.to_value();
    let llm_max_output_tokens: Value<U256BE> = config.llm.max_output_tokens.to_value();
    let llm_prompt_safety_margin_tokens: Value<U256BE> =
        config.llm.prompt_safety_margin_tokens.to_value();
    let llm_prompt_chars_per_token: Value<U256BE> = config.llm.prompt_chars_per_token.to_value();

    change += entity! { &profile_entry_id @
        playground_config::kind: playground_config::kind_llm_profile,
        playground_config::updated_at: now,
        playground_config::llm_profile_id: profile_id,
        metadata::name: profile_name,
        playground_config::llm_model: llm_model,
        playground_config::llm_base_url: llm_base_url,
        playground_config::llm_stream: llm_stream,
        playground_config::llm_context_window_tokens: llm_context_window_tokens,
        playground_config::llm_max_output_tokens: llm_max_output_tokens,
        playground_config::llm_prompt_safety_margin_tokens: llm_prompt_safety_margin_tokens,
        playground_config::llm_prompt_chars_per_token: llm_prompt_chars_per_token,
    };

    if let Some(key) = config.llm.api_key.as_ref() {
        let handle = ws.put(key.clone());
        change += entity! { &profile_entry_id @ playground_config::llm_api_key: handle };
    }
    if let Some(effort) = config.llm.reasoning_effort.as_ref() {
        let handle = ws.put(effort.clone());
        change += entity! { &profile_entry_id @ playground_config::llm_reasoning_effort: handle };
    }

    ws.commit(change, None, Some("playground config"));
    Ok(())
}

fn load_string_attr(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    config_id: Id,
    attr: Attribute<Handle<Blake3, LongString>>,
) -> Result<Option<String>> {
    let mut handles = find!(
        (entity: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{ ?entity @ attr: ?handle }])
    )
    .into_iter()
    .filter(|(entity, _)| *entity == config_id);
    let Some((_, handle)) = handles.next() else {
        return Ok(None);
    };
    if handles.next().is_some() {
        let attr_id = attr.id();
        return Err(anyhow!(
            "config {config_id:x} has multiple values for attribute {attr_id:x}"
        ));
    }
    let view: View<str> = ws.get(handle).context("read config text")?;
    Ok(Some(view.as_ref().to_string()))
}

fn load_id_attr(catalog: &TribleSet, config_id: Id, attr: Attribute<GenId>) -> Option<Id> {
    find!(
        (entity: Id, value: Value<GenId>),
        pattern!(catalog, [{ ?entity @ attr: ?value }])
    )
    .into_iter()
    .find_map(|(entity, value)| (entity == config_id).then_some(Id::from_value(&value)))
}

fn load_u256_attr(
    catalog: &TribleSet,
    config_id: Id,
    attr: Attribute<U256BE>,
) -> Option<Value<U256BE>> {
    find!(
        (entity: Id, value: Value<U256BE>),
        pattern!(catalog, [{ ?entity @ attr: ?value }])
    )
    .into_iter()
    .find_map(|(entity, value)| (entity == config_id).then_some(value))
}

fn u256be_to_u64(value: Value<U256BE>) -> Option<u64> {
    let raw = value.raw;
    if raw[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    let bytes: [u8; 8] = raw[24..32].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

fn default_system_prompt() -> String {
    DEFAULT_SYSTEM_PROMPT.to_string()
}

fn default_model() -> String {
    DEFAULT_MODEL.to_string()
}

fn default_base_url() -> String {
    DEFAULT_BASE_URL.to_string()
}

fn default_stream() -> bool {
    DEFAULT_STREAM
}

fn default_context_window_tokens() -> u64 {
    DEFAULT_CONTEXT_WINDOW_TOKENS
}

fn default_max_output_tokens() -> u64 {
    DEFAULT_MAX_OUTPUT_TOKENS
}

fn default_prompt_safety_margin_tokens() -> u64 {
    DEFAULT_PROMPT_SAFETY_MARGIN_TOKENS
}

fn default_prompt_chars_per_token() -> u64 {
    DEFAULT_PROMPT_CHARS_PER_TOKEN
}

fn default_compaction_reduction_factor() -> u64 {
    DEFAULT_COMPACTION_REDUCTION_FACTOR
}

fn default_branch() -> String {
    DEFAULT_BRANCH.to_string()
}

fn default_author() -> String {
    DEFAULT_AUTHOR.to_string()
}

fn default_author_role() -> String {
    DEFAULT_AUTHOR_ROLE.to_string()
}

fn default_poll_ms() -> u64 {
    DEFAULT_POLL_MS
}
