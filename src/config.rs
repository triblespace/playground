use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

use crate::branch_util::ensure_branch_id;
use crate::repo_ops::push_workspace;
use crate::schema::playground_config;
use crate::time_util::{epoch_interval, interval_key, now_epoch};

const DEFAULT_MODEL: &str = "gpt-oss:120b";
const DEFAULT_BASE_URL: &str = "http://localhost:11434/v1/responses";
const DEFAULT_STREAM: bool = false;
const DEFAULT_SYSTEM_PROMPT: &str = "You are a terminal-based agent. Respond with exactly one shell command per turn. Output only raw command text: no markdown fences, no commentary prelude, no channel labels, and no multi-command blocks. Prefer faculties in /workspace/faculties over ad-hoc shell when applicable; run a faculty with no arguments to inspect usage. If unsure what to do next, run `/workspace/faculties/orient.rs show`.";
// The branch that carries the core cognition loop + exec/LLM request state.
const DEFAULT_BRANCH: &str = "cognition";
const DEFAULT_EXEC_BRANCH: &str = "cognition";
const DEFAULT_COMPASS_BRANCH: &str = "compass";
const DEFAULT_LOCAL_MESSAGES_BRANCH: &str = "local-messages";
const DEFAULT_RELATIONS_BRANCH: &str = "relations";
const DEFAULT_TEAMS_BRANCH: &str = "teams";
const DEFAULT_WORKSPACE_BRANCH: &str = "workspace";
const DEFAULT_AUTHOR: &str = "agent";
const DEFAULT_AUTHOR_ROLE: &str = "user";
const DEFAULT_POLL_MS: u64 = 1;
const DEFAULT_PILE_PATH: &str = "self.pile";
const CONFIG_BRANCH: &str = "config";

#[derive(Clone, Debug)]
pub struct Config {
    pub pile_path: PathBuf,
    pub llm: LlmConfig,
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
}

#[derive(Clone, Debug)]
pub struct ExecConfig {
    pub default_cwd: Option<PathBuf>,
    pub sandbox_profile: Option<Id>,
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
        let mut ws = repo
            .pull(branch_id)
            .map_err(|err| anyhow!("pull config workspace: {err:?}"))?;
        store_config(&mut ws, self).context("store config")?;
        push_workspace(&mut repo, &mut ws).context("push config")?;
        close_repo(repo).context("close config pile")?;
        Ok(())
    }
}

fn default_config(pile_path: PathBuf) -> Config {
    Config {
        pile_path,
        llm: LlmConfig::default(),
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
        author: default_author(),
        author_role: default_author_role(),
        persona_id: None,
        poll_ms: default_poll_ms(),
    }
}

fn load_from_pile(pile_path: &Path) -> Result<Config> {
    let (mut repo, branch_id) = open_config_repo(pile_path)?;
    let mut ws = repo
        .pull(branch_id)
        .map_err(|err| anyhow!("pull config workspace: {err:?}"))?;
    let catalog = ws.checkout(..).context("checkout config workspace")?;

    let config = if let Some(config) = load_latest_config(&mut ws, &catalog, pile_path)? {
        config
    } else {
        let mut config = default_config(pile_path.to_path_buf());
        // Make the default config fully concrete (ids instead of names).
        config.branch_id = Some(ensure_branch_id(&mut repo, config.branch.as_str())?);
        config.compass_branch_id = Some(ensure_branch_id(&mut repo, DEFAULT_COMPASS_BRANCH)?);
        config.exec_branch_id = Some(ensure_branch_id(&mut repo, DEFAULT_EXEC_BRANCH)?);
        config.local_messages_branch_id =
            Some(ensure_branch_id(&mut repo, DEFAULT_LOCAL_MESSAGES_BRANCH)?);
        config.relations_branch_id = Some(ensure_branch_id(&mut repo, DEFAULT_RELATIONS_BRANCH)?);
        config.teams_branch_id = Some(ensure_branch_id(&mut repo, DEFAULT_TEAMS_BRANCH)?);
        config.workspace_branch_id = Some(ensure_branch_id(&mut repo, DEFAULT_WORKSPACE_BRANCH)?);

        store_config(&mut ws, &config).context("store default config")?;
        push_workspace(&mut repo, &mut ws).context("push default config")?;
        config
    };

    close_repo(repo).context("close config pile")?;
    Ok(config)
}

fn open_config_repo(pile_path: &Path) -> Result<(Repository<Pile>, Id)> {
    if let Some(parent) = pile_path.parent() {
        fs::create_dir_all(parent).context("create pile directory")?;
    }
    let mut pile = Pile::open(pile_path).context("open pile")?;
    pile.restore().context("restore pile")?;

    let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
    let branch_id = ensure_branch_id(&mut repo, CONFIG_BRANCH)?;
    Ok((repo, branch_id))
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

    Ok(Some(config))
}

fn store_config(ws: &mut Workspace<Pile>, config: &Config) -> Result<()> {
    let now = epoch_interval(now_epoch());
    let config_id = ufoid();

    let system_prompt = ws.put(config.system_prompt.clone());
    let branch = ws.put(config.branch.clone());
    let author = ws.put(config.author.clone());
    let author_role = ws.put(config.author_role.clone());
    let llm_model = ws.put(config.llm.model.clone());
    let llm_base_url = ws.put(config.llm.base_url.clone());
    let poll_ms: Value<U256BE> = config.poll_ms.to_value();
    let llm_stream: Value<U256BE> = if config.llm.stream { 1u64 } else { 0u64 }.to_value();

    let mut change = TribleSet::new();
    change += entity! { &config_id @
        playground_config::kind: playground_config::kind_config,
        playground_config::updated_at: now,
        playground_config::system_prompt: system_prompt,
        playground_config::branch: branch,
        playground_config::author: author,
        playground_config::author_role: author_role,
        playground_config::poll_ms: poll_ms,
        playground_config::llm_model: llm_model,
        playground_config::llm_base_url: llm_base_url,
        playground_config::llm_stream: llm_stream,
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
    if let Some(id) = config.persona_id {
        change += entity! { &config_id @ playground_config::persona_id: id };
    }
    if let Some(key) = config.llm.api_key.as_ref() {
        let handle = ws.put(key.clone());
        change += entity! { &config_id @ playground_config::llm_api_key: handle };
    }
    if let Some(key) = config.tavily_api_key.as_ref() {
        let handle = ws.put(key.clone());
        change += entity! { &config_id @ playground_config::tavily_api_key: handle };
    }
    if let Some(key) = config.exa_api_key.as_ref() {
        let handle = ws.put(key.clone());
        change += entity! { &config_id @ playground_config::exa_api_key: handle };
    }
    if let Some(effort) = config.llm.reasoning_effort.as_ref() {
        let handle = ws.put(effort.clone());
        change += entity! { &config_id @ playground_config::llm_reasoning_effort: handle };
    }
    if let Some(cwd) = config.exec.default_cwd.as_ref() {
        let handle = ws.put(cwd.to_string_lossy().to_string());
        change += entity! { &config_id @ playground_config::exec_default_cwd: handle };
    }
    if let Some(profile) = config.exec.sandbox_profile {
        change += entity! { &config_id @ playground_config::exec_sandbox_profile: profile };
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
