use ed25519_dalek::{SecretKey, SigningKey};
use eframe::egui;
use hifitime::Epoch;
use rand_core::{OsRng, TryRngCore};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;
use triblespace::core::blob::schemas::longstring::LongString;
use triblespace::core::blob::schemas::simplearchive::SimpleArchive;
use triblespace::core::id::{ExclusiveId, Id};
use triblespace::core::metadata;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::core::trible::TribleSet;
use triblespace::core::value::Value;
use triblespace::core::value::schemas::hash::{Blake3, Handle};
use triblespace::core::value::schemas::time::NsTAIInterval;
use triblespace::macros::{entity, find, id_hex, pattern};
use triblespace::prelude::valueschemas::U256BE;
use triblespace::prelude::{BlobStore, BlobStoreGet, BranchStore, ToBlob, ToValue, View};

use GORBIE::NotebookCtx;
use GORBIE::NotebookConfig;
use GORBIE::cards::{DEFAULT_CARD_PADDING, with_padding};
use GORBIE::md;
use GORBIE::widgets::{Button, TextField};

use crate::schema::openai_responses;
use crate::schema::playground_exec;

mod archive {
    use triblespace::macros::id_hex;
    use triblespace::prelude::blobschemas::LongString;
    use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval};
    use triblespace::prelude::*;

    attributes! {
        "5F10520477A04E5FB322C85CC78C6762" as pub kind: GenId;
        "838CC157FFDD37C6AC7CC5A472E43ADB" as pub author: GenId;
        "E63EE961ABDB1D1BEC0789FDAFFB9501" as pub author_name: Handle<Blake3, LongString>;
        "ACF09FF3D62B73983A222313FF0C52D2" as pub content: Handle<Blake3, LongString>;
        "0DA5DD275AA34F86B0297CC35F1B7395" as pub created_at: NsTAIInterval;
    }

    #[allow(non_upper_case_globals)]
    pub const kind_message: Id = id_hex!("1A0841C92BBDA0A26EA9A8252D6ECD9B");
}

mod teams {
    use triblespace::prelude::blobschemas::LongString;
    use triblespace::prelude::valueschemas::{Blake3, GenId, Handle};
    use triblespace::prelude::*;

    attributes! {
        "1E525B603A0060D9FA132B3D4EE9538A" as pub chat: GenId;
        "B6089037C04529F55D2A2D1A668DBE95" as pub chat_id: Handle<Blake3, LongString>;
    }
}

type CommitHandle = Value<Handle<Blake3, SimpleArchive>>;
const EXEC_SCROLL_HEIGHT: f32 = 260.0;
const SUMMARY_SCROLL_HEIGHT: f32 = 220.0;
const LOCAL_MESSAGE_SCROLL_HEIGHT: f32 = 780.0;
const LOCAL_COMPOSE_HEIGHT: f32 = 80.0;
const RELATIONS_SCROLL_HEIGHT: f32 = 260.0;
const TEAMS_SCROLL_HEIGHT: f32 = 520.0;
const TEAMS_CHAT_LIST_WIDTH: f32 = 220.0;

static DIAGNOSTICS_PILE_OVERRIDE: OnceLock<Option<PathBuf>> = OnceLock::new();
static DIAGNOSTICS_HEADLESS: AtomicBool = AtomicBool::new(false);

pub fn set_default_pile(path: Option<PathBuf>) {
    let _ = DIAGNOSTICS_PILE_OVERRIDE.set(path);
}

fn diagnostics_default_pile() -> Option<PathBuf> {
    DIAGNOSTICS_PILE_OVERRIDE
        .get()
        .and_then(|path| path.as_ref().cloned())
}

fn diagnostics_is_headless() -> bool {
    DIAGNOSTICS_HEADLESS.load(Ordering::Relaxed)
}

const LOCAL_KIND_MESSAGE_ID: Id = id_hex!("A3556A66B00276797FCE8A2742AB850F");
const LOCAL_KIND_READ_ID: Id = id_hex!("B663C15BB6F2BF591EA870386DD48537");
const RELATIONS_KIND_PERSON_ID: Id = id_hex!("D8ADDE47121F4E7868017463EC860726");

const LOCAL_KIND_SPECS: [(Id, &str); 2] = [
    (LOCAL_KIND_MESSAGE_ID, "local_message"),
    (LOCAL_KIND_READ_ID, "local_read"),
];

mod local_messages {
    use triblespace::prelude::attributes;
    use triblespace::prelude::blobschemas;
    use triblespace::prelude::valueschemas;

    attributes! {
        "42C4DB210F7EAFAF38F179ADCB4A9D5B" as from: valueschemas::GenId;
        "95D58D3E68A43979F8AA51415541414C" as to: valueschemas::GenId;
        "23075866B369B5F393D43B30649469F6" as body: valueschemas::Handle<valueschemas::Blake3, blobschemas::LongString>;
        "53ECCC7489AF8D30EF385ED12073F4A3" as created_at: valueschemas::NsTAIInterval;

        "2213B191326E9B99605FA094E516E50E" as about_message: valueschemas::GenId;
        "99E92F483731FA6D59115A8D6D187A37" as reader: valueschemas::GenId;
        "934C5AD3DA8F7A2EB467460E50D17A4F" as read_at: valueschemas::NsTAIInterval;
    }
}

mod relations {
    use triblespace::prelude::attributes;
    use triblespace::prelude::blobschemas;
    use triblespace::prelude::valueschemas;

    attributes! {
        "8F162B593D390E1424394DBF6883A72C" as alias: valueschemas::ShortString;
        "32B22FBA3EC2ADC3FFEB48483FE8961F" as affinity: valueschemas::ShortString;
        "9B3329149D54CB9A8E8075E4AA862649" as teams_user_id: valueschemas::ShortString;
        "B563A063474CBE62ED25A8D0E9A1853C" as email: valueschemas::ShortString;
        "DC0916CB5F640984EFE359A33105CA9A" as display_name: valueschemas::Handle<valueschemas::Blake3, blobschemas::LongString>;
        "F0AD0BBFAC4C4C899637573DC965622E" as first_name: valueschemas::Handle<valueschemas::Blake3, blobschemas::LongString>;
        "764DD765142B3F4725B614BD3B9118EC" as last_name: valueschemas::Handle<valueschemas::Blake3, blobschemas::LongString>;
    }
}

#[derive(Clone, Debug)]
struct DashboardConfig {
    pile_path: String,
    exec_branches: String,
    local_message_branches: String,
    relations_branches: String,
    local_sender: String,
    local_recipient: String,
    local_reader: String,
    teams_branches: String,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        let default_pile = diagnostics_default_pile().unwrap_or_else(|| {
            let repo_root = repo_root();
            repo_root.join("self.pile")
        });
        Self {
            pile_path: default_pile.to_string_lossy().to_string(),
            exec_branches: "main".to_string(),
            local_message_branches: "local-messages".to_string(),
            relations_branches: "relations".to_string(),
            local_sender: "jp".to_string(),
            local_recipient: "agent".to_string(),
            local_reader: "agent".to_string(),
            teams_branches: "teams".to_string(),
        }
    }
}

struct DashboardState {
    config: DashboardConfig,
    repo: Option<Repository<Pile>>,
    repo_open_path: Option<PathBuf>,
    signing_key: SigningKey,
    snapshot: Option<Result<DashboardSnapshot, String>>,
    local_draft: String,
    local_send_error: Option<String>,
    local_send_notice: Option<String>,
    local_read_error: Option<String>,
    teams_selected_chat: Option<Id>,
}

impl Drop for DashboardState {
    fn drop(&mut self) {
        if let Some(repo) = self.repo.take() {
            let pile = repo.into_storage();
            let _ = pile.close();
        }
    }
}

impl Default for DashboardState {
    fn default() -> Self {
        Self {
            config: DashboardConfig::default(),
            repo: None,
            repo_open_path: None,
            signing_key: random_signing_key(),
            snapshot: None,
            local_draft: String::new(),
            local_send_error: None,
            local_send_notice: None,
            local_read_error: None,
            teams_selected_chat: None,
        }
    }
}

#[derive(Debug, Clone)]
struct BranchEntry {
    id: Id,
    name: Option<String>,
}

#[derive(Debug, Clone)]
struct ExecRow {
    command: String,
    status: ExecStatus,
    requested_at: Option<i128>,
    started_at: Option<i128>,
    finished_at: Option<i128>,
    exit_code: Option<u64>,
    worker: Option<Id>,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExecStatus {
    Pending,
    Running,
    Done,
    Failed,
}

#[derive(Debug, Clone)]
struct ExecSummary {
    pending: usize,
    running: usize,
    done: usize,
    failed: usize,
}

#[derive(Debug, Clone)]
struct LocalMessageRow {
    id: Id,
    created_at: Option<i128>,
    from_id: Id,
    to_id: Id,
    body: String,
    read_by_reader: bool,
}

#[derive(Debug, Clone)]
struct TeamsMessageRow {
    chat_id: Id,
    created_at: Option<i128>,
    author_name: Option<String>,
    content: String,
}

#[derive(Debug, Clone)]
struct TeamsChatRow {
    id: Id,
    label: String,
    last_at: Option<i128>,
    message_count: usize,
}

#[derive(Debug, Clone)]
struct ReasoningSummaryRow {
    created_at: Option<i128>,
    summary: String,
}

#[derive(Debug, Clone)]
struct RelationRow {
    id: Id,
    label: Option<String>,
    first_name: Option<String>,
    last_name: Option<String>,
    display_name: Option<String>,
    affinity: Option<String>,
    teams_user_id: Option<String>,
    email: Option<String>,
    note: Option<String>,
    aliases: Vec<String>,
}

#[derive(Debug, Clone)]
struct DashboardSnapshot {
    pile_path: PathBuf,
    branches: Vec<BranchEntry>,
    branch_data: HashMap<Id, BranchSnapshot>,
    exec_rows: Vec<ExecRow>,
    exec_summary: ExecSummary,
    exec_error: Option<String>,
    reasoning_summaries: Vec<ReasoningSummaryRow>,
    local_message_rows: Vec<LocalMessageRow>,
    local_message_error: Option<String>,
    local_sender_id: Option<Id>,
    local_recipient_id: Option<Id>,
    local_reader_id: Option<Id>,
    relations_people: Vec<RelationRow>,
    relations_error: Option<String>,
    relations_labels: HashMap<Id, String>,
    teams_messages: Vec<TeamsMessageRow>,
    teams_chats: Vec<TeamsChatRow>,
    teams_error: Option<String>,
    labels: HashMap<Id, String>,
    now_key: i128,
}

#[derive(Debug, Clone)]
struct BranchSnapshot {
    head: Option<CommitHandle>,
    data: TribleSet,
}

fn diagnostics_ui(nb: &mut NotebookCtx) {
    let padding = DEFAULT_CARD_PADDING;
    let dashboard = nb.state(
        "playground-diagnostics",
        DashboardState::default(),
        move |ui, state| {
            with_padding(ui, padding, |ui| {
                md!(
                    ui,
                    "# Playground Diagnostics\n\
_Live view of the agent pile, exec queue, and message activity._"
                );

                ui.separator();
                ui.heading("Config");
                ui.horizontal(|ui| {
                    ui.label("Pile");
                    ui.text_edit_singleline(&mut state.config.pile_path);
                });
                ui.horizontal(|ui| {
                    ui.label("Exec branches");
                    ui.text_edit_singleline(&mut state.config.exec_branches);
                });
                ui.horizontal(|ui| {
                    ui.label("Local message branches");
                    ui.text_edit_singleline(&mut state.config.local_message_branches);
                });
                ui.horizontal(|ui| {
                    ui.label("Relations branches");
                    ui.text_edit_singleline(&mut state.config.relations_branches);
                });
                ui.horizontal(|ui| {
                    ui.label("Local sender");
                    ui.text_edit_singleline(&mut state.config.local_sender);
                });
                ui.horizontal(|ui| {
                    ui.label("Local recipient");
                    ui.text_edit_singleline(&mut state.config.local_recipient);
                });
                ui.horizontal(|ui| {
                    ui.label("Local reader");
                    ui.text_edit_singleline(&mut state.config.local_reader);
                });
                ui.horizontal(|ui| {
                    ui.label("Teams branches");
                    ui.text_edit_singleline(&mut state.config.teams_branches);
                });
                if let Err(err) = ensure_repo_open(state) {
                    state.snapshot = Some(Err(err));
                } else {
                    refresh_snapshot(state);
                }
                if diagnostics_is_headless() {
                    // In headless capture we only need one snapshot.
                } else {
                    ui.ctx().request_repaint_after(Duration::from_millis(250));
                }
            });
        },
    );

    nb.view(move |ui| {
        let state = dashboard.read(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Overview");
            let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                return;
            };
            ui.horizontal(|ui| {
                ui.label(format!("Pile: {}", snapshot.pile_path.display()));
            });
            if !snapshot.branches.is_empty() {
                ui.label("Branches:");
                for branch in &snapshot.branches {
                    let label = branch.name.as_deref().unwrap_or("<unnamed>").to_string();
                    ui.label(format!("- {label} ({})", id_prefix(branch.id)));
                }
            }
        });
    });

    nb.view(move |ui| {
        let state = dashboard.read(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Exec queue");
            let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                return;
            };
            if let Some(err) = &snapshot.exec_error {
                ui.colored_label(egui::Color32::RED, err);
            } else {
                render_exec_summary(ui, &snapshot.exec_summary);
                render_exec_rows(ui, snapshot.now_key, &snapshot.exec_rows, &snapshot.labels);
            }
        });
    });

    nb.view(move |ui| {
        let state = dashboard.read(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Reasoning summaries");
            let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                return;
            };
            if snapshot.reasoning_summaries.is_empty() {
                ui.label("No summaries yet.");
            } else {
                render_reasoning_summaries(ui, snapshot.now_key, &snapshot.reasoning_summaries);
            }
        });
    });

    nb.view(move |ui| {
        let mut state = dashboard.read_mut(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Local messages");
            let snapshot = {
                let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                    return;
                };
                snapshot.clone()
            };
            if let Some(err) = &snapshot.local_message_error {
                ui.colored_label(egui::Color32::RED, err);
            } else {
                auto_ack_local_messages(
                    &mut state,
                    &snapshot.branches,
                    &snapshot.local_message_rows,
                    snapshot.local_reader_id,
                );
                render_local_messages(
                    ui,
                    snapshot.now_key,
                    &snapshot.local_message_rows,
                    snapshot.local_sender_id,
                    snapshot.local_reader_id,
                );
            }
            render_local_composer(ui, &mut state, &snapshot.branches, &snapshot);
        });
    });

    nb.view(move |ui| {
        let state = dashboard.read(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Relations");
            let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                return;
            };
            if let Some(err) = &snapshot.relations_error {
                ui.colored_label(egui::Color32::RED, err);
            } else {
                render_relations(ui, &snapshot.relations_people);
            }
        });
    });

    nb.view(move |ui| {
        let mut state = dashboard.read_mut(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Teams conversations");
            let snapshot = {
                let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                    return;
                };
                snapshot.clone()
            };
            if let Some(err) = &snapshot.teams_error {
                ui.colored_label(egui::Color32::RED, err);
            } else {
                render_teams_conversations(
                    ui,
                    &mut state,
                    snapshot.now_key,
                    &snapshot.teams_chats,
                    &snapshot.teams_messages,
                );
            }
        });
    });
}

pub fn run_diagnostics(
    headless: bool,
    out_dir: Option<PathBuf>,
    scale: Option<f32>,
    headless_wait_ms: Option<u64>,
) -> anyhow::Result<()> {
    DIAGNOSTICS_HEADLESS.store(headless, Ordering::Relaxed);

    let mut cfg = NotebookConfig::new("Playground Diagnostics");
    if headless {
        let out_dir = out_dir.unwrap_or_else(|| PathBuf::from("gorbie_capture"));
        cfg = if let Some(scale) = scale {
            cfg.with_headless_capture_scaled(out_dir, scale)
        } else {
            cfg.with_headless_capture(out_dir)
        };
        if let Some(wait_ms) = headless_wait_ms {
            cfg = cfg.with_headless_settle_timeout(Duration::from_millis(wait_ms));
        }
    }

    cfg.run(|nb| diagnostics_ui(nb))
        .map_err(|err| anyhow::anyhow!("diagnostics failed: {err:?}"))?;
    Ok(())
}

fn snapshot_or_message<'a>(
    ui: &mut egui::Ui,
    snapshot: &'a Option<Result<DashboardSnapshot, String>>,
) -> Option<&'a DashboardSnapshot> {
    match snapshot {
        None => {
            ui.label("No snapshot yet.");
            None
        }
        Some(Err(err)) => {
            ui.colored_label(egui::Color32::RED, err);
            None
        }
        Some(Ok(snapshot)) => Some(snapshot),
    }
}

fn ensure_repo_open(state: &mut DashboardState) -> Result<(), String> {
    let open_path = PathBuf::from(state.config.pile_path.trim());
    let path_changed = state
        .repo_open_path
        .as_ref()
        .map_or(true, |path| path != &open_path);
    if path_changed || state.repo.is_none() {
        if let Some(repo) = state.repo.take() {
            let pile = repo.into_storage();
            let _ = pile.close();
        }
        let mut pile = Pile::open(&open_path).map_err(|err| err.to_string())?;
        pile.restore().map_err(|err| err.to_string())?;
        let repo = Repository::new(pile, state.signing_key.clone());
        state.repo = Some(repo);
        state.repo_open_path = Some(open_path);
    }
    Ok(())
}

fn refresh_snapshot(state: &mut DashboardState) {
    let previous = state
        .snapshot
        .as_ref()
        .and_then(|result| result.as_ref().ok())
        .cloned();
    let config = state.config.clone();
    let repo = match state.repo.as_mut() {
        Some(repo) => repo,
        None => {
            state.snapshot = Some(Err("Repository not open.".to_string()));
            return;
        }
    };
    let result = load_snapshot(repo, &config, previous);
    state.snapshot = Some(result);
}

fn load_snapshot(
    repo: &mut Repository<Pile>,
    config: &DashboardConfig,
    previous: Option<DashboardSnapshot>,
) -> Result<DashboardSnapshot, String> {
    let pile_path = PathBuf::from(&config.pile_path);
    let mut branches = list_branches(repo.storage_mut())?;
    let mut previous_map = previous
        .as_ref()
        .filter(|snapshot| snapshot.pile_path == pile_path)
        .map(|snapshot| snapshot.branch_data.clone())
        .unwrap_or_default();

    let exec_refs = parse_branch_list(&config.exec_branches);
    let local_refs = parse_branch_list(&config.local_message_branches);
    let relations_refs = parse_branch_list(&config.relations_branches);
    let teams_refs = parse_branch_list(&config.teams_branches);

    let mut ensure_refs = Vec::new();
    ensure_refs.extend(exec_refs.iter().cloned());
    ensure_refs.extend(local_refs.iter().cloned());
    ensure_refs.extend(relations_refs.iter().cloned());
    ensure_refs.extend(teams_refs.iter().cloned());
    ensure_named_branches(repo, &mut branches, &ensure_refs)?;

    let branch_lookup = BranchLookup::new(&branches);
    let exec_res = resolve_branch_ids(&branch_lookup, &exec_refs);
    let local_res = resolve_branch_ids(&branch_lookup, &local_refs);
    let relations_res = resolve_branch_ids(&branch_lookup, &relations_refs);
    let teams_res = resolve_branch_ids(&branch_lookup, &teams_refs);

    let exec_error = exec_res.as_ref().err().cloned();
    let local_message_error = local_res.as_ref().err().cloned();
    let relations_error = relations_res.as_ref().err().cloned();
    let teams_error = teams_res.as_ref().err().cloned();

    let exec_ids = exec_res.unwrap_or_default();
    let local_ids = local_res.unwrap_or_default();
    let relations_ids = relations_res.unwrap_or_default();
    let teams_ids = teams_res.unwrap_or_default();

    let mut needed_ids: Vec<Id> = Vec::new();
    extend_unique(&mut needed_ids, &exec_ids);
    extend_unique(&mut needed_ids, &local_ids);
    extend_unique(&mut needed_ids, &relations_ids);
    extend_unique(&mut needed_ids, &teams_ids);

    let mut branch_data: HashMap<Id, BranchSnapshot> = HashMap::new();
    let mut reader_ws: Option<Workspace<Pile>> = None;

    for branch_id in &needed_ids {
        let snapshot = load_branch_snapshot(repo, *branch_id, previous_map.remove(branch_id))?;
        if reader_ws.is_none() {
            reader_ws = Some(
                repo.pull(*branch_id)
                    .map_err(|err| format!("pull branch: {err:?}"))?,
            );
        }
        branch_data.insert(*branch_id, snapshot);
    }

    let exec_data = union_branches(&branch_data, &exec_ids);
    let local_data = union_branches(&branch_data, &local_ids);
    let relations_data = union_branches(&branch_data, &relations_ids);
    let teams_data = union_branches(&branch_data, &teams_ids);

    let mut reader_ws = if let Some(ws) = reader_ws {
        ws
    } else {
        let now_key = epoch_key(now_epoch());
        return Ok(DashboardSnapshot {
            pile_path,
            branches,
            branch_data,
            exec_rows: Vec::new(),
            exec_summary: ExecSummary {
                pending: 0,
                running: 0,
                done: 0,
                failed: 0,
            },
            exec_error,
            reasoning_summaries: Vec::new(),
            local_message_rows: Vec::new(),
            local_message_error,
            local_sender_id: None,
            local_recipient_id: None,
            local_reader_id: None,
            relations_people: Vec::new(),
            relations_error,
            relations_labels: HashMap::new(),
            teams_messages: Vec::new(),
            teams_chats: Vec::new(),
            teams_error,
            labels: HashMap::new(),
            now_key,
        });
    };

    Ok(build_snapshot(
        exec_data,
        local_data,
        relations_data,
        teams_data,
        pile_path,
        branches,
        branch_data,
        exec_error,
        local_message_error,
        relations_error,
        teams_error,
        config,
        &mut reader_ws,
    ))
}

fn build_snapshot(
    exec_data: TribleSet,
    local_data: TribleSet,
    relations_data: TribleSet,
    teams_data: TribleSet,
    pile_path: PathBuf,
    branches: Vec<BranchEntry>,
    branch_data: HashMap<Id, BranchSnapshot>,
    exec_error: Option<String>,
    local_message_error: Option<String>,
    relations_error: Option<String>,
    teams_error: Option<String>,
    config: &DashboardConfig,
    ws: &mut Workspace<Pile>,
) -> DashboardSnapshot {
    let now_key = epoch_key(now_epoch());
    let relations_people = collect_relations_people(&relations_data, ws);
    let relations_labels = collect_relations_labels(&relations_people);
    let local_sender_id = resolve_person_ref(&relations_people, &config.local_sender);
    let local_recipient_id = resolve_person_ref(&relations_people, &config.local_recipient);
    let local_reader_id = resolve_person_ref(&relations_people, &config.local_reader);
    let exec_rows = collect_exec_rows(&exec_data, ws);
    let exec_summary = summarize_exec(&exec_rows);
    let reasoning_summaries = collect_reasoning_summaries(&exec_data, ws);
    let local_message_rows = collect_local_messages(&local_data, ws, local_reader_id);
    let (teams_messages, teams_chats) = collect_teams_messages(&teams_data, ws);
    let labels = collect_labels(&exec_data, ws);

    DashboardSnapshot {
        pile_path,
        branches,
        branch_data,
        exec_rows,
        exec_summary,
        exec_error,
        reasoning_summaries,
        local_message_rows,
        local_message_error,
        local_sender_id,
        local_recipient_id,
        local_reader_id,
        relations_people,
        relations_error,
        relations_labels,
        teams_messages,
        teams_chats,
        teams_error,
        labels,
        now_key,
    }
}

fn list_branches(pile: &mut Pile<Blake3>) -> Result<Vec<BranchEntry>, String> {
    let reader = pile.reader().map_err(|err| err.to_string())?;
    let iter = pile.branches().map_err(|err| err.to_string())?;
    let mut branches = Vec::new();
    for branch in iter {
        let branch_id = branch.map_err(|err| err.to_string())?;
        let name = match pile.head(branch_id).map_err(|err| err.to_string())? {
            None => Some("<unnamed>".to_string()),
            Some(meta_handle) => match reader.get::<TribleSet, _>(meta_handle) {
                Ok(metadata_set) => {
                    let mut names = find!(
                        (handle: Value<Handle<Blake3, LongString>>),
                        pattern!(&metadata_set, [{ metadata::name: ?handle }])
                    )
                    .into_iter();
                    match (names.next(), names.next()) {
                        (Some((handle,)), None) => reader
                            .get::<View<str>, _>(handle)
                            .ok()
                            .map(|view| view.as_ref().to_string())
                            .or_else(|| {
                                Some(format!(
                                    "<name blob missing ({})>",
                                    longstring_handle_prefix(handle)
                                ))
                            }),
                        _ => Some("<unnamed>".to_string()),
                    }
                }
                Err(_) => Some(format!(
                    "<metadata blob missing ({})>",
                    archive_handle_prefix(meta_handle)
                )),
            },
        };
        branches.push(BranchEntry {
            id: branch_id,
            name,
        });
    }
    Ok(branches)
}

struct BranchLookup {
    by_id: HashSet<Id>,
    by_name: HashMap<String, Vec<Id>>,
}

impl BranchLookup {
    fn new(branches: &[BranchEntry]) -> Self {
        let mut by_id = HashSet::new();
        let mut by_name: HashMap<String, Vec<Id>> = HashMap::new();
        for branch in branches {
            by_id.insert(branch.id);
            if let Some(name) = branch.name.clone() {
                by_name.entry(name).or_default().push(branch.id);
            }
        }
        Self { by_id, by_name }
    }
}

fn parse_branch_list(raw: &str) -> Vec<String> {
    raw.split(|c: char| c == ',' || c.is_whitespace())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

fn resolve_branch_ids(lookup: &BranchLookup, refs: &[String]) -> Result<Vec<Id>, String> {
    let mut ids = Vec::new();
    let mut seen = HashSet::new();
    for raw in refs {
        let trimmed = raw.trim();
        let id = if trimmed.len() == 32 && trimmed.chars().all(|c| c.is_ascii_hexdigit()) {
            let id = Id::from_hex(trimmed).ok_or_else(|| "invalid branch id".to_string())?;
            if !lookup.by_id.contains(&id) {
                return Err(format!("Branch id {} not found.", trimmed));
            }
            id
        } else if let Some(ids_for_name) = lookup.by_name.get(trimmed) {
            if ids_for_name.len() > 1 {
                return Err(format!(
                    "Branch name '{}' is ambiguous ({} matches).",
                    trimmed,
                    ids_for_name.len()
                ));
            }
            ids_for_name[0]
        } else {
            return Err(format!("Branch '{}' not found.", trimmed));
        };
        if seen.insert(id) {
            ids.push(id);
        }
    }
    Ok(ids)
}

fn ensure_named_branches(
    repo: &mut Repository<Pile>,
    branches: &mut Vec<BranchEntry>,
    refs: &[String],
) -> Result<(), String> {
    let mut known_names: HashSet<String> = branches
        .iter()
        .filter_map(|branch| branch.name.clone())
        .collect();

    for raw in refs {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            continue;
        }
        let is_hex = trimmed.len() == 32 && trimmed.chars().all(|c| c.is_ascii_hexdigit());
        if is_hex {
            continue;
        }
        if known_names.contains(trimmed) {
            continue;
        }

        let branch_id = repo
            .create_branch(trimmed, None)
            .map_err(|err| format!("create branch '{trimmed}': {err:?}"))?
            .release();
        branches.push(BranchEntry {
            id: branch_id,
            name: Some(trimmed.to_string()),
        });
        known_names.insert(trimmed.to_string());
    }

    Ok(())
}

fn extend_unique(out: &mut Vec<Id>, ids: &[Id]) {
    for id in ids {
        if !out.contains(id) {
            out.push(*id);
        }
    }
}

fn load_branch_snapshot(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    previous: Option<BranchSnapshot>,
) -> Result<BranchSnapshot, String> {
    let mut ws = repo
        .pull(branch_id)
        .map_err(|err| format!("pull branch: {err:?}"))?;
    let head = ws.head();
    let data = if let Some(prev_snapshot) = previous {
        if prev_snapshot.head == head {
            prev_snapshot.data
        } else if let (Some(prev_head), Some(_)) = (prev_snapshot.head, head) {
            match ws.checkout(prev_head..) {
                Ok(delta) => {
                    let mut data = prev_snapshot.data;
                    if !delta.is_empty() {
                        data.union(delta);
                    }
                    data
                }
                Err(_) => ws.checkout(..).map_err(|err| format!("checkout: {err}"))?,
            }
        } else if head.is_none() {
            TribleSet::new()
        } else {
            ws.checkout(..).map_err(|err| format!("checkout: {err}"))?
        }
    } else if head.is_none() {
        TribleSet::new()
    } else {
        ws.checkout(..).map_err(|err| format!("checkout: {err}"))?
    };

    Ok(BranchSnapshot { head, data })
}

fn union_branches(branch_data: &HashMap<Id, BranchSnapshot>, ids: &[Id]) -> TribleSet {
    let mut union = TribleSet::new();
    for id in ids {
        if let Some(snapshot) = branch_data.get(id) {
            union.union(snapshot.data.clone());
        }
    }
    union
}

fn collect_exec_rows(data: &TribleSet, ws: &mut Workspace<Pile>) -> Vec<ExecRow> {
    let mut rows: HashMap<Id, ExecRow> = HashMap::new();
    for (request_id, command) in find!(
        (request_id: Id, command: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{
            ?request_id @
            playground_exec::kind: playground_exec::kind_command_request,
            playground_exec::command_text: ?command,
        }])
    ) {
        let command_text = load_text(ws, command).unwrap_or_else(|| "<missing>".to_string());
        rows.insert(
            request_id,
            ExecRow {
                command: command_text,
                status: ExecStatus::Pending,
                requested_at: None,
                started_at: None,
                finished_at: None,
                exit_code: None,
                worker: None,
                error: None,
            },
        );
    }

    for (request_id, requested_at) in find!(
        (request_id: Id, requested_at: Value<NsTAIInterval>),
        pattern!(data, [{ ?request_id @ playground_exec::requested_at: ?requested_at }])
    ) {
        if let Some(row) = rows.get_mut(&request_id) {
            row.requested_at = Some(interval_key(requested_at));
        }
    }

    let attempts = load_attempts(data);
    let started_at = load_started_at(data);
    let finished_at = load_finished_at(data);
    let workers = load_workers(data);
    let exit_codes = load_exit_codes(data);
    let errors = load_errors(data, ws);

    let progress = latest_progress(data, &attempts, &started_at, &workers);
    let results = latest_results(data, &attempts, &finished_at, &exit_codes, &errors);

    for (request_id, row) in rows.iter_mut() {
        let progress_info = progress.get(request_id);
        let result_info = results.get(request_id);
        row.started_at = progress_info.and_then(|info| info.started_at);
        row.worker = progress_info.and_then(|info| info.worker);
        row.finished_at = result_info.and_then(|info| info.finished_at);
        row.exit_code = result_info.and_then(|info| info.exit_code);
        row.error = result_info.and_then(|info| info.error.clone());

        row.status = if let Some(result) = result_info {
            if result.error.is_some() {
                ExecStatus::Failed
            } else {
                ExecStatus::Done
            }
        } else if progress_info.is_some() {
            ExecStatus::Running
        } else {
            ExecStatus::Pending
        };
    }

    let mut list: Vec<ExecRow> = rows.into_values().collect();
    list.sort_by_key(|row| row.requested_at.unwrap_or(i128::MIN));
    list.reverse();
    list
}

fn collect_local_messages(
    data: &TribleSet,
    ws: &mut Workspace<Pile>,
    reader_id: Option<Id>,
) -> Vec<LocalMessageRow> {
    let mut rows = Vec::new();
    for (message_id, from, to, body_handle, created_at) in find!(
        (
            message_id: Id,
            from: Id,
            to: Id,
            body_handle: Value<Handle<Blake3, LongString>>,
            created_at: Value<NsTAIInterval>
        ),
        pattern!(&data, [{
            ?message_id @
            metadata::tag: &LOCAL_KIND_MESSAGE_ID,
            local_messages::from: ?from,
            local_messages::to: ?to,
            local_messages::body: ?body_handle,
            local_messages::created_at: ?created_at,
        }])
    ) {
        let body = load_text(ws, body_handle).unwrap_or_else(|| "<missing>".to_string());
        rows.push(LocalMessageRow {
            id: message_id,
            created_at: Some(interval_key(created_at)),
            from_id: from,
            to_id: to,
            body,
            read_by_reader: false,
        });
    }

    let mut reads: HashMap<Id, HashSet<Id>> = HashMap::new();
    for (_read_id, message_id, reader_id, read_at) in find!(
        (
            read_id: Id,
            message_id: Id,
            reader_id: Id,
            read_at: Value<NsTAIInterval>
        ),
        pattern!(&data, [{
            ?read_id @
            metadata::tag: &LOCAL_KIND_READ_ID,
            local_messages::about_message: ?message_id,
            local_messages::reader: ?reader_id,
            local_messages::read_at: ?read_at,
        }])
    ) {
        let _ = read_at;
        reads.entry(message_id).or_default().insert(reader_id);
    }

    if let Some(reader_id) = reader_id {
        for row in &mut rows {
            if let Some(readers) = reads.get(&row.id) {
                row.read_by_reader = readers.contains(&reader_id);
            }
        }
    }

    rows.sort_by_key(|row| row.created_at.unwrap_or(i128::MIN));
    rows.reverse();
    rows
}

fn collect_relations_people(data: &TribleSet, ws: &mut Workspace<Pile>) -> Vec<RelationRow> {
    let mut people: HashMap<Id, RelationRow> = HashMap::new();

    for (person_id,) in find!(
        (person_id: Id),
        pattern!(data, [{ ?person_id @ metadata::tag: &RELATIONS_KIND_PERSON_ID }])
    ) {
        people.insert(
            person_id,
            RelationRow {
                id: person_id,
                label: None,
                first_name: None,
                last_name: None,
                display_name: None,
                affinity: None,
                teams_user_id: None,
                email: None,
                note: None,
                aliases: Vec::new(),
            },
        );
    }

    for (person_id, handle) in find!(
        (person_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?person_id @ metadata::name: ?handle }])
    ) {
        if let Some(person) = people.get_mut(&person_id) {
            if person.label.is_none() {
                if let Some(value) = load_text(ws, handle) {
                    person.label = Some(value);
                }
            }
        }
    }

    for (person_id, handle) in find!(
        (person_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?person_id @ relations::display_name: ?handle }])
    ) {
        if let Some(person) = people.get_mut(&person_id) {
            if person.display_name.is_none() {
                if let Some(value) = load_text(ws, handle) {
                    person.display_name = Some(value);
                }
            }
        }
    }

    for (person_id, handle) in find!(
        (person_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?person_id @ relations::first_name: ?handle }])
    ) {
        if let Some(person) = people.get_mut(&person_id) {
            if person.first_name.is_none() {
                if let Some(value) = load_text(ws, handle) {
                    person.first_name = Some(value);
                }
            }
        }
    }

    for (person_id, handle) in find!(
        (person_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?person_id @ relations::last_name: ?handle }])
    ) {
        if let Some(person) = people.get_mut(&person_id) {
            if person.last_name.is_none() {
                if let Some(value) = load_text(ws, handle) {
                    person.last_name = Some(value);
                }
            }
        }
    }

    for (person_id, handle) in find!(
        (person_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?person_id @ metadata::description: ?handle }])
    ) {
        if let Some(person) = people.get_mut(&person_id) {
            if person.note.is_none() {
                if let Some(value) = load_text(ws, handle) {
                    person.note = Some(value);
                }
            }
        }
    }

    for (person_id, value) in find!(
        (person_id: Id, value: String),
        pattern!(data, [{ ?person_id @ relations::affinity: ?value }])
    ) {
        if let Some(person) = people.get_mut(&person_id) {
            if person.affinity.is_none() {
                person.affinity = Some(value);
            }
        }
    }

    for (person_id, value) in find!(
        (person_id: Id, value: String),
        pattern!(data, [{ ?person_id @ relations::teams_user_id: ?value }])
    ) {
        if let Some(person) = people.get_mut(&person_id) {
            if person.teams_user_id.is_none() {
                person.teams_user_id = Some(value);
            }
        }
    }

    for (person_id, value) in find!(
        (person_id: Id, value: String),
        pattern!(data, [{ ?person_id @ relations::email: ?value }])
    ) {
        if let Some(person) = people.get_mut(&person_id) {
            if person.email.is_none() {
                person.email = Some(value);
            }
        }
    }

    for (person_id, value) in find!(
        (person_id: Id, value: String),
        pattern!(data, [{ ?person_id @ relations::alias: ?value }])
    ) {
        if let Some(person) = people.get_mut(&person_id) {
            person.aliases.push(value);
        }
    }

    let mut list: Vec<RelationRow> = people.into_values().collect();
    list.sort_by(|a, b| a.label.cmp(&b.label).then_with(|| a.id.cmp(&b.id)));
    list
}

fn collect_relations_labels(people: &[RelationRow]) -> HashMap<Id, String> {
    let mut map = HashMap::new();
    for person in people {
        if let Some(label) = person.label.as_ref() {
            map.insert(person.id, label.clone());
        } else if let (Some(first), Some(last)) =
            (person.first_name.as_ref(), person.last_name.as_ref())
        {
            map.insert(person.id, format!("{first} {last}"));
        } else if let Some(first) = person.first_name.as_ref() {
            map.insert(person.id, first.clone());
        } else if let Some(last) = person.last_name.as_ref() {
            map.insert(person.id, last.clone());
        } else if let Some(name) = person.display_name.as_ref() {
            map.insert(person.id, name.clone());
        }
    }
    map
}

fn resolve_person_ref(people: &[RelationRow], raw: &str) -> Option<Id> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Some(id) = Id::from_hex(trimmed) {
        return Some(id);
    }
    for person in people {
        if let Some(label) = person.label.as_ref() {
            if label == trimmed {
                return Some(person.id);
            }
        }
        if let (Some(first), Some(last)) = (person.first_name.as_ref(), person.last_name.as_ref()) {
            let full = format!("{first} {last}");
            if full == trimmed {
                return Some(person.id);
            }
        }
        if let Some(first) = person.first_name.as_ref() {
            if first == trimmed {
                return Some(person.id);
            }
        }
        if let Some(last) = person.last_name.as_ref() {
            if last == trimmed {
                return Some(person.id);
            }
        }
        if let Some(name) = person.display_name.as_ref() {
            if name == trimmed {
                return Some(person.id);
            }
        }
        if person.aliases.iter().any(|alias| alias == trimmed) {
            return Some(person.id);
        }
    }
    None
}

fn collect_teams_messages(
    data: &TribleSet,
    ws: &mut Workspace<Pile>,
) -> (Vec<TeamsMessageRow>, Vec<TeamsChatRow>) {
    let mut author_names: HashMap<Id, String> = HashMap::new();
    for (author_id, name_handle) in find!(
        (author_id: Id, name_handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?author_id @ archive::author_name: ?name_handle }])
    ) {
        if let Some(name) = load_text(ws, name_handle) {
            author_names.insert(author_id, name);
        }
    }

    let mut chat_labels: HashMap<Id, String> = HashMap::new();
    for (chat_id, chat_handle) in find!(
        (chat_id: Id, chat_handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?chat_id @ teams::chat_id: ?chat_handle }])
    ) {
        if let Some(label) = load_text(ws, chat_handle) {
            chat_labels.insert(chat_id, label);
        }
    }

    let mut messages = Vec::new();
    for (_message_id, chat_id, author_id, content_handle, created_at) in find!(
        (
            message_id: Id,
            chat_id: Id,
            author_id: Id,
            content_handle: Value<Handle<Blake3, LongString>>,
            created_at: Value<NsTAIInterval>
        ),
        pattern!(data, [{
            ?message_id @
            archive::kind: archive::kind_message,
            teams::chat: ?chat_id,
            archive::author: ?author_id,
            archive::content: ?content_handle,
            archive::created_at: ?created_at,
        }])
    ) {
        let content = load_text(ws, content_handle).unwrap_or_else(|| "<missing>".to_string());
        let created_key = interval_key(created_at);
        let author_name = author_names.get(&author_id).cloned();
        messages.push(TeamsMessageRow {
            chat_id,
            created_at: Some(created_key),
            author_name,
            content,
        });
    }

    messages.sort_by_key(|row| row.created_at.unwrap_or(i128::MIN));
    messages.reverse();

    let mut chats: HashMap<Id, TeamsChatRow> = HashMap::new();
    for row in &messages {
        let entry = chats.entry(row.chat_id).or_insert_with(|| TeamsChatRow {
            id: row.chat_id,
            label: chat_labels
                .get(&row.chat_id)
                .cloned()
                .unwrap_or_else(|| id_prefix(row.chat_id)),
            last_at: None,
            message_count: 0,
        });
        entry.message_count += 1;
        if entry.last_at.map_or(true, |current| {
            row.created_at.unwrap_or(i128::MIN) > current
        }) {
            entry.last_at = row.created_at;
        }
    }

    let mut chat_list: Vec<TeamsChatRow> = chats.into_values().collect();
    chat_list.sort_by_key(|row| row.last_at.unwrap_or(i128::MIN));
    chat_list.reverse();

    (messages, chat_list)
}

fn collect_reasoning_summaries(
    data: &TribleSet,
    ws: &mut Workspace<Pile>,
) -> Vec<ReasoningSummaryRow> {
    let mut rows = Vec::new();
    for (_response_id, raw_handle, finished_at) in find!(
        (
            response_id: Id,
            raw_handle: Value<Handle<Blake3, LongString>>,
            finished_at: Value<NsTAIInterval>
        ),
        pattern!(data, [{
            ?response_id @
            openai_responses::kind: openai_responses::kind_result,
            openai_responses::response_raw: ?raw_handle,
            openai_responses::finished_at: ?finished_at,
        }])
    ) {
        let raw = load_text(ws, raw_handle).unwrap_or_default();
        let Some(response_json) = parse_response_json(&raw) else {
            continue;
        };
        let summaries = extract_reasoning_summaries(&response_json);
        if summaries.is_empty() {
            continue;
        }
        rows.push(ReasoningSummaryRow {
            created_at: Some(interval_key(finished_at)),
            summary: summaries.join("\n"),
        });
    }

    rows.sort_by_key(|row| row.created_at.unwrap_or(i128::MIN));
    rows.reverse();
    rows.truncate(10);
    rows
}

fn collect_labels(data: &TribleSet, ws: &mut Workspace<Pile>) -> HashMap<Id, String> {
    let mut map = HashMap::new();
    for (entity_id, handle) in find!(
        (entity_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?entity_id @ metadata::name: ?handle }])
    ) {
        if let Some(label) = load_text(ws, handle) {
            map.entry(entity_id).or_insert(label);
        }
    }
    map
}

fn render_local_composer(
    ui: &mut egui::Ui,
    state: &mut DashboardState,
    branches: &[BranchEntry],
    snapshot: &DashboardSnapshot,
) {
    let sender_label = snapshot
        .local_sender_id
        .and_then(|id| snapshot.relations_labels.get(&id).cloned())
        .unwrap_or_else(|| state.config.local_sender.clone());
    let recipient_label = snapshot
        .local_recipient_id
        .and_then(|id| snapshot.relations_labels.get(&id).cloned())
        .unwrap_or_else(|| state.config.local_recipient.clone());
    ui.horizontal(|ui| {
        ui.label("From");
        render_person_picker(
            ui,
            "local_sender_picker",
            &snapshot.relations_people,
            snapshot.local_sender_id,
            &mut state.config.local_sender,
        );
        ui.add_space(10.0);
        ui.label("To");
        render_person_picker(
            ui,
            "local_recipient_picker",
            &snapshot.relations_people,
            snapshot.local_recipient_id,
            &mut state.config.local_recipient,
        );
    });

    ui.horizontal(|ui| {
        ui.label("Auto-ack as");
        render_person_picker(
            ui,
            "local_reader_picker",
            &snapshot.relations_people,
            snapshot.local_reader_id,
            &mut state.config.local_reader,
        );
    });

    ui.small(format!("{sender_label} → {recipient_label}"));
    if snapshot.local_sender_id.is_none() {
        ui.colored_label(
            egui::Color32::RED,
            format!(
                "Unknown sender '{}' (check Relations branch).",
                state.config.local_sender
            ),
        );
    }
    if snapshot.local_recipient_id.is_none() {
        ui.colored_label(
            egui::Color32::RED,
            format!(
                "Unknown recipient '{}' (check Relations branch).",
                state.config.local_recipient
            ),
        );
    }

    let response = ui.add_sized(
        [ui.available_width(), LOCAL_COMPOSE_HEIGHT],
        TextField::multiline(&mut state.local_draft),
    );
    if state.local_draft.trim().is_empty() && !response.has_focus() {
        let hint_pos = response.rect.left_top() + egui::vec2(10.0, 6.0);
        ui.painter().text(
            hint_pos,
            egui::Align2::LEFT_TOP,
            "Type a message...",
            egui::TextStyle::Small.resolve(ui.style()),
            ui.visuals().weak_text_color(),
        );
    }
    if response.changed() {
        state.local_send_error = None;
        state.local_send_notice = None;
    }

    ui.horizontal(|ui| {
        if ui.add(Button::new("Send")).clicked() {
            send_local_message_from_ui(state, branches, snapshot);
        }
        if ui.add(Button::new("Clear")).clicked() {
            state.local_draft.clear();
            state.local_send_error = None;
            state.local_send_notice = None;
            state.local_read_error = None;
        }
        if let Some(note) = &state.local_send_notice {
            ui.label(note);
        }
    });
    if let Some(err) = &state.local_send_error {
        ui.colored_label(egui::Color32::RED, err);
    }
    if let Some(err) = &state.local_read_error {
        ui.colored_label(egui::Color32::RED, err);
    }
}

fn render_person_picker(
    ui: &mut egui::Ui,
    id_salt: &'static str,
    people: &[RelationRow],
    selected: Option<Id>,
    raw: &mut String,
) {
    let selected_text = selected
        .and_then(|id| people.iter().find(|person| person.id == id))
        .map(|person| {
            let label = person.label.as_deref().unwrap_or("<unnamed>");
            format!("{label} ({})", id_prefix(person.id))
        })
        .unwrap_or_else(|| raw.trim().to_string());
    egui::ComboBox::from_id_salt(id_salt)
        .selected_text(selected_text)
        .show_ui(ui, |ui| {
            for person in people {
                let label = person.label.as_deref().unwrap_or("<unnamed>");
                let display = format!("{label} ({})", id_prefix(person.id));
                if ui
                    .selectable_label(selected == Some(person.id), display)
                    .clicked()
                {
                    // Persist a stable reference; labels/aliases can change.
                    *raw = format!("{:x}", person.id);
                }
            }
        });
}

fn render_teams_conversations(
    ui: &mut egui::Ui,
    state: &mut DashboardState,
    now_key: i128,
    chats: &[TeamsChatRow],
    messages: &[TeamsMessageRow],
) {
    ui.horizontal(|ui| {
        ui.vertical(|ui| {
            ui.set_min_width(TEAMS_CHAT_LIST_WIDTH);
            ui.label("Chats");
            egui::ScrollArea::vertical()
                .id_salt("teams_chat_list_scroll")
                .max_height(TEAMS_SCROLL_HEIGHT)
                .show(ui, |ui| {
                    let all_selected = state.teams_selected_chat.is_none();
                    if ui.selectable_label(all_selected, "All chats").clicked() {
                        state.teams_selected_chat = None;
                    }
                    ui.add_space(6.0);
                    for chat in chats {
                        let selected = state.teams_selected_chat == Some(chat.id);
                        let label = format!("{} ({})", chat.label, chat.message_count);
                        if ui.selectable_label(selected, label).clicked() {
                            state.teams_selected_chat = Some(chat.id);
                        }
                        ui.small(format_age(now_key, chat.last_at));
                        ui.add_space(4.0);
                    }
                });
        });

        ui.add_space(12.0);

        ui.vertical(|ui| {
            let selected_chat = state.teams_selected_chat;
            let title = match selected_chat {
                Some(chat_id) => chats
                    .iter()
                    .find(|chat| chat.id == chat_id)
                    .map(|chat| chat.label.as_str())
                    .unwrap_or("Chat"),
                None => "All chats",
            };
            ui.label(title);

            egui::ScrollArea::vertical()
                .id_salt("teams_message_scroll")
                .max_height(TEAMS_SCROLL_HEIGHT)
                .show(ui, |ui| {
                    for row in messages {
                        if let Some(chat_id) = selected_chat {
                            if row.chat_id != chat_id {
                                continue;
                            }
                        }
                        let author = row.author_name.as_deref().unwrap_or("<unknown>");
                        let age = format_age(now_key, row.created_at);
                        let chat_label = chats
                            .iter()
                            .find(|chat| chat.id == row.chat_id)
                            .map(|chat| chat.label.as_str())
                            .unwrap_or("<chat>");

                        let meta = if selected_chat.is_some() {
                            format!("{author} · {age}")
                        } else {
                            format!("{chat_label} · {author} · {age}")
                        };
                        ui.small(meta);
                        ui.label(&row.content);
                        ui.add_space(8.0);
                    }
                });
        });
    });
}

fn send_local_message_from_ui(
    state: &mut DashboardState,
    branches: &[BranchEntry],
    snapshot: &DashboardSnapshot,
) {
    state.local_send_error = None;
    state.local_send_notice = None;
    state.local_read_error = None;

    let Some(repo) = state.repo.as_mut() else {
        state.local_send_error = Some("Repository not open.".to_string());
        return;
    };

    let body = state.local_draft.trim();
    if body.is_empty() {
        state.local_send_error = Some("Message is empty.".to_string());
        return;
    }

    let branch_lookup = BranchLookup::new(branches);
    let refs = parse_branch_list(&state.config.local_message_branches);
    let branch_id = match resolve_single_branch(&branch_lookup, &refs) {
        Ok(branch_id) => branch_id,
        Err(err) => {
            state.local_send_error = Some(err);
            return;
        }
    };

    let Some(from_id) = snapshot.local_sender_id else {
        state.local_send_error = Some(format!(
            "Unknown sender '{}' (check Relations branch).",
            state.config.local_sender
        ));
        return;
    };
    let Some(to_id) = snapshot.local_recipient_id else {
        state.local_send_error = Some(format!(
            "Unknown recipient '{}' (check Relations branch).",
            state.config.local_recipient
        ));
        return;
    };

    match send_local_message(repo, branch_id, from_id, to_id, body) {
        Ok(()) => {
            state.local_draft.clear();
            state.local_send_notice = Some("Message sent.".to_string());
        }
        Err(err) => {
            state.local_send_error = Some(err);
        }
    }
}

fn resolve_single_branch(lookup: &BranchLookup, refs: &[String]) -> Result<Id, String> {
    let ids = resolve_branch_ids(lookup, refs)?;
    if ids.is_empty() {
        return Err("Local message branch not found.".to_string());
    }
    if ids.len() > 1 {
        return Err("Select a single local message branch to send.".to_string());
    }
    Ok(ids[0])
}

fn send_local_message(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    from: Id,
    to: Id,
    body: &str,
) -> Result<(), String> {
    let mut ws = repo
        .pull(branch_id)
        .map_err(|err| format!("pull branch: {err:?}"))?;
    let mut change = ensure_local_metadata(&mut ws)?;

    let now = now_epoch();
    let now_interval: Value<NsTAIInterval> = (now, now).to_value();
    let message_id = triblespace::prelude::ufoid();
    let body_handle = ws.put(body.to_string());
    change += entity! { &message_id @
        metadata::tag: &LOCAL_KIND_MESSAGE_ID,
        local_messages::from: from,
        local_messages::to: to,
        local_messages::body: body_handle,
        local_messages::created_at: now_interval,
    };

    ws.commit(change, None, Some("local message"));
    repo.push(&mut ws)
        .map_err(|err| format!("push message: {err:?}"))?;
    Ok(())
}

fn auto_ack_local_messages(
    state: &mut DashboardState,
    branches: &[BranchEntry],
    rows: &[LocalMessageRow],
    reader_id: Option<Id>,
) {
    state.local_read_error = None;
    let Some(reader_id) = reader_id else {
        return;
    };
    let unread: Vec<Id> = rows
        .iter()
        .filter(|row| row.to_id == reader_id && !row.read_by_reader)
        .map(|row| row.id)
        .collect();
    if unread.is_empty() {
        return;
    }

    let Some(repo) = state.repo.as_mut() else {
        return;
    };
    let branch_lookup = BranchLookup::new(branches);
    let refs = parse_branch_list(&state.config.local_message_branches);
    let branch_id = match resolve_single_branch(&branch_lookup, &refs) {
        Ok(branch_id) => branch_id,
        Err(err) => {
            state.local_read_error = Some(err);
            return;
        }
    };

    if let Err(err) = ack_local_messages(repo, branch_id, &unread, reader_id) {
        state.local_read_error = Some(err);
    }
}

fn ack_local_messages(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    message_ids: &[Id],
    reader_id: Id,
) -> Result<(), String> {
    if message_ids.is_empty() {
        return Ok(());
    }
    let mut ws = repo
        .pull(branch_id)
        .map_err(|err| format!("pull branch: {err:?}"))?;
    let mut change = ensure_local_metadata(&mut ws)?;

    let now = now_epoch();
    let now_interval: Value<NsTAIInterval> = (now, now).to_value();
    for message_id in message_ids {
        let read_id = triblespace::prelude::ufoid();
        change += entity! { &read_id @
            metadata::tag: &LOCAL_KIND_READ_ID,
            local_messages::about_message: *message_id,
            local_messages::reader: reader_id,
            local_messages::read_at: now_interval,
        };
    }

    if !change.is_empty() {
        ws.commit(change, None, Some("local message read"));
        repo.push(&mut ws)
            .map_err(|err| format!("push read: {err:?}"))?;
    }
    Ok(())
}

fn ensure_local_metadata(ws: &mut Workspace<Pile>) -> Result<TribleSet, String> {
    let space = if ws.head().is_none() {
        TribleSet::new()
    } else {
        ws.checkout(..).map_err(|err| format!("checkout: {err}"))?
    };
    let mut change = TribleSet::new();

    let mut existing_kinds: HashSet<Id> = find!(
        (kind: Id),
        pattern!(&space, [{ ?kind @ metadata::name: _?handle }])
    )
    .into_iter()
    .map(|(kind,)| kind)
    .collect();

    for (id, label) in LOCAL_KIND_SPECS {
        if !existing_kinds.contains(&id) {
            let name_handle = label.to_owned().to_blob().get_handle::<Blake3>();
            change += entity! { ExclusiveId::force_ref(&id) @ metadata::name: name_handle };
            existing_kinds.insert(id);
        }
    }

    Ok(change)
}

#[derive(Debug, Clone)]
struct ProgressInfo {
    attempt: u64,
    started_at: Option<i128>,
    worker: Option<Id>,
}

#[derive(Debug, Clone)]
struct ResultInfo {
    attempt: u64,
    finished_at: Option<i128>,
    exit_code: Option<u64>,
    error: Option<String>,
}

fn load_attempts(data: &TribleSet) -> HashMap<Id, u64> {
    let mut attempts = HashMap::new();
    for (event_id, attempt) in find!(
        (event_id: Id, attempt: Value<U256BE>),
        pattern!(data, [{ ?event_id @ playground_exec::attempt: ?attempt }])
    ) {
        if let Some(value) = u256be_to_u64(attempt) {
            attempts.insert(event_id, value);
        }
    }
    attempts
}

fn load_started_at(data: &TribleSet) -> HashMap<Id, i128> {
    let mut intervals = HashMap::new();
    for (event_id, interval) in find!(
        (event_id: Id, interval: Value<NsTAIInterval>),
        pattern!(data, [{ ?event_id @ playground_exec::started_at: ?interval }])
    ) {
        intervals.insert(event_id, interval_key(interval));
    }
    intervals
}

fn load_finished_at(data: &TribleSet) -> HashMap<Id, i128> {
    let mut intervals = HashMap::new();
    for (event_id, interval) in find!(
        (event_id: Id, interval: Value<NsTAIInterval>),
        pattern!(data, [{ ?event_id @ playground_exec::finished_at: ?interval }])
    ) {
        intervals.insert(event_id, interval_key(interval));
    }
    intervals
}

fn load_workers(data: &TribleSet) -> HashMap<Id, Id> {
    let mut workers = HashMap::new();
    for (event_id, worker) in find!(
        (event_id: Id, worker: Id),
        pattern!(data, [{ ?event_id @ playground_exec::worker: ?worker }])
    ) {
        workers.insert(event_id, worker);
    }
    workers
}

fn load_exit_codes(data: &TribleSet) -> HashMap<Id, u64> {
    let mut codes = HashMap::new();
    for (event_id, exit_code) in find!(
        (event_id: Id, exit_code: Value<U256BE>),
        pattern!(data, [{ ?event_id @ playground_exec::exit_code: ?exit_code }])
    ) {
        if let Some(code) = u256be_to_u64(exit_code) {
            codes.insert(event_id, code);
        }
    }
    codes
}

fn load_errors(data: &TribleSet, ws: &mut Workspace<Pile>) -> HashMap<Id, String> {
    let mut errors = HashMap::new();
    for (event_id, error_handle) in find!(
        (event_id: Id, error_handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?event_id @ playground_exec::error: ?error_handle }])
    ) {
        if let Some(text) = load_text(ws, error_handle) {
            errors.insert(event_id, text);
        }
    }
    errors
}

fn latest_progress(
    data: &TribleSet,
    attempts: &HashMap<Id, u64>,
    started_at: &HashMap<Id, i128>,
    workers: &HashMap<Id, Id>,
) -> HashMap<Id, ProgressInfo> {
    let mut progress: HashMap<Id, ProgressInfo> = HashMap::new();
    for (event_id, request_id) in find!(
        (event_id: Id, request_id: Id),
        pattern!(data, [{
            ?event_id @
            playground_exec::kind: playground_exec::kind_in_progress,
            playground_exec::about_request: ?request_id,
        }])
    ) {
        let attempt = attempts.get(&event_id).copied().unwrap_or(0);
        let started_at = started_at.get(&event_id).copied();
        let worker = workers.get(&event_id).copied();
        let info = ProgressInfo {
            attempt,
            started_at,
            worker,
        };
        progress
            .entry(request_id)
            .and_modify(|existing| {
                if info.attempt > existing.attempt
                    || (info.attempt == existing.attempt
                        && info.started_at.unwrap_or(i128::MIN)
                            > existing.started_at.unwrap_or(i128::MIN))
                {
                    *existing = info.clone();
                }
            })
            .or_insert(info);
    }
    progress
}

fn latest_results(
    data: &TribleSet,
    attempts: &HashMap<Id, u64>,
    finished_at: &HashMap<Id, i128>,
    exit_codes: &HashMap<Id, u64>,
    errors: &HashMap<Id, String>,
) -> HashMap<Id, ResultInfo> {
    let mut results: HashMap<Id, ResultInfo> = HashMap::new();
    for (event_id, request_id) in find!(
        (event_id: Id, request_id: Id),
        pattern!(data, [{
            ?event_id @
            playground_exec::kind: playground_exec::kind_command_result,
            playground_exec::about_request: ?request_id,
        }])
    ) {
        let attempt = attempts.get(&event_id).copied().unwrap_or(0);
        let finished_at = finished_at.get(&event_id).copied();
        let exit_code = exit_codes.get(&event_id).copied();
        let error = errors.get(&event_id).cloned();
        let info = ResultInfo {
            attempt,
            finished_at,
            exit_code,
            error,
        };
        results
            .entry(request_id)
            .and_modify(|existing| {
                if info.attempt > existing.attempt
                    || (info.attempt == existing.attempt
                        && info.finished_at.unwrap_or(i128::MIN)
                            > existing.finished_at.unwrap_or(i128::MIN))
                {
                    *existing = info.clone();
                }
            })
            .or_insert(info);
    }
    results
}

fn summarize_exec(rows: &[ExecRow]) -> ExecSummary {
    let mut summary = ExecSummary {
        pending: 0,
        running: 0,
        done: 0,
        failed: 0,
    };
    for row in rows {
        match row.status {
            ExecStatus::Pending => summary.pending += 1,
            ExecStatus::Running => summary.running += 1,
            ExecStatus::Done => summary.done += 1,
            ExecStatus::Failed => summary.failed += 1,
        }
    }
    summary
}

fn render_exec_summary(ui: &mut egui::Ui, summary: &ExecSummary) {
    ui.horizontal(|ui| {
        ui.label(format!("Pending: {}", summary.pending));
        ui.label(format!("Running: {}", summary.running));
        ui.label(format!("Done: {}", summary.done));
        ui.label(format!("Failed: {}", summary.failed));
    });
}

fn render_exec_rows(
    ui: &mut egui::Ui,
    now_key: i128,
    rows: &[ExecRow],
    labels: &HashMap<Id, String>,
) {
    egui::ScrollArea::vertical()
        .id_salt("exec_rows_scroll")
        .max_height(EXEC_SCROLL_HEIGHT)
        .show(ui, |ui| {
            egui::Grid::new("exec_rows")
                .striped(true)
                .spacing(egui::Vec2::new(12.0, 6.0))
                .show(ui, |ui| {
                    ui.label("Status");
                    ui.label("Age");
                    ui.label("Command");
                    ui.label("Exit");
                    ui.label("Worker");
                    ui.end_row();

                    for row in rows {
                        ui.label(status_label(row.status));
                        ui.label(format_age(now_key, row.requested_at));
                        ui.monospace(truncate_single_line(&row.command, 80));
                        ui.label(
                            row.exit_code
                                .map(|code| code.to_string())
                                .unwrap_or_else(|| "-".to_string()),
                        );
                        ui.label(
                            row.worker
                                .map(|id| format_id(labels, id))
                                .unwrap_or_else(|| "-".to_string()),
                        );
                        ui.end_row();

                        if let Some(error) = &row.error {
                            ui.label("");
                            ui.label("error");
                            ui.label(truncate_single_line(error, 120));
                            ui.label("");
                            ui.label("");
                            ui.end_row();
                        }
                    }
                });
        });
}

fn render_reasoning_summaries(ui: &mut egui::Ui, now_key: i128, rows: &[ReasoningSummaryRow]) {
    egui::ScrollArea::vertical()
        .id_salt("reasoning_summary_scroll")
        .max_height(SUMMARY_SCROLL_HEIGHT)
        .show(ui, |ui| {
            for row in rows {
                ui.small(format_age(now_key, row.created_at));
                ui.label(&row.summary);
                ui.add_space(8.0);
            }
        });
}

fn render_local_messages(
    ui: &mut egui::Ui,
    now_key: i128,
    rows: &[LocalMessageRow],
    sender_id: Option<Id>,
    reader_id: Option<Id>,
) {
    egui::ScrollArea::vertical()
        .id_salt("local_messages_scroll")
        .min_scrolled_height(LOCAL_MESSAGE_SCROLL_HEIGHT)
        .max_height(LOCAL_MESSAGE_SCROLL_HEIGHT)
        .show(ui, |ui| {
            for row in rows {
                let is_sender = sender_id.map_or(false, |id| row.from_id == id);
                let age = format_age(now_key, row.created_at);
                let meta = if is_sender {
                    if reader_id.is_some() && row.read_by_reader {
                        format!("{age} · read")
                    } else {
                        format!("{age} · sent")
                    }
                } else {
                    age
                };

                let align = if is_sender {
                    egui::Layout::right_to_left(egui::Align::TOP)
                } else {
                    egui::Layout::left_to_right(egui::Align::TOP)
                };
                let bubble_color = if is_sender {
                    egui::Color32::from_rgb(92, 120, 155)
                } else {
                    egui::Color32::from_gray(70)
                };

                ui.with_layout(align, |ui| {
                    egui::Frame::NONE
                        .fill(bubble_color)
                        .corner_radius(egui::CornerRadius::same(6))
                        .inner_margin(egui::Margin::symmetric(10, 6))
                        .show(ui, |ui| {
                            ui.label(egui::RichText::new(&row.body).color(egui::Color32::WHITE));
                        });
                });
                ui.with_layout(align, |ui| {
                    ui.small(meta);
                });
                ui.add_space(6.0);
            }
        });
}

fn render_relations(ui: &mut egui::Ui, people: &[RelationRow]) {
    if people.is_empty() {
        ui.label("No relations.");
        return;
    }
    egui::ScrollArea::vertical()
        .id_salt("relations_scroll")
        .max_height(RELATIONS_SCROLL_HEIGHT)
        .show(ui, |ui| {
            for person in people {
                let label = person.label.as_deref().unwrap_or("<unnamed>");
                ui.label(format!("[{}] {}", id_prefix(person.id), label));
                let full_name = match (&person.first_name, &person.last_name) {
                    (Some(first), Some(last)) => Some(format!("{first} {last}")),
                    (Some(first), None) => Some(first.clone()),
                    (None, Some(last)) => Some(last.clone()),
                    (None, None) => None,
                };
                if let Some(name) = person.display_name.as_ref().or(full_name.as_ref()) {
                    ui.small(name);
                }
                if let Some(affinity) = &person.affinity {
                    ui.small(format!("affinity: {affinity}"));
                }
                if let Some(teams) = &person.teams_user_id {
                    ui.small(format!("teams: {teams}"));
                }
                if let Some(email) = &person.email {
                    ui.small(format!("email: {email}"));
                }
                if !person.aliases.is_empty() {
                    ui.small(format!("aliases: {}", person.aliases.join(", ")));
                }
                if let Some(note) = &person.note {
                    ui.small(format!("note: {}", truncate_single_line(note, 120)));
                }
                ui.add_space(8.0);
            }
        });
}

fn parse_response_json(raw: &str) -> Option<JsonValue> {
    if let Ok(value) = serde_json::from_str::<JsonValue>(raw) {
        return Some(value);
    }

    for line in raw.lines().rev() {
        if let Ok(event) = serde_json::from_str::<JsonValue>(line) {
            if event.get("type").and_then(JsonValue::as_str) == Some("response.completed") {
                if let Some(response) = event.get("response") {
                    return Some(response.clone());
                }
            }
        }
    }

    None
}

fn extract_reasoning_summaries(response: &JsonValue) -> Vec<String> {
    let mut summaries = Vec::new();
    let Some(output) = response.get("output").and_then(JsonValue::as_array) else {
        return summaries;
    };

    for item in output {
        let Some(item_type) = item.get("type").and_then(JsonValue::as_str) else {
            continue;
        };
        if item_type != "reasoning" {
            continue;
        }

        let Some(summary_items) = item.get("summary").and_then(JsonValue::as_array) else {
            continue;
        };
        for entry in summary_items {
            if entry.get("type").and_then(JsonValue::as_str) != Some("summary_text") {
                continue;
            }
            if let Some(text) = entry.get("text").and_then(JsonValue::as_str) {
                summaries.push(text.to_string());
            }
        }
    }

    summaries
}

fn status_label(status: ExecStatus) -> egui::RichText {
    match status {
        ExecStatus::Pending => egui::RichText::new("pending").color(egui::Color32::GRAY),
        ExecStatus::Running => egui::RichText::new("running").color(egui::Color32::LIGHT_BLUE),
        ExecStatus::Done => egui::RichText::new("done").color(egui::Color32::LIGHT_GREEN),
        ExecStatus::Failed => egui::RichText::new("failed").color(egui::Color32::LIGHT_RED),
    }
}

fn format_age(now_key: i128, maybe_key: Option<i128>) -> String {
    let Some(key) = maybe_key else {
        return "-".to_string();
    };
    let delta_ns = now_key.saturating_sub(key);
    let delta_s = (delta_ns / 1_000_000_000).max(0) as i64;
    if delta_s < 60 {
        format!("{delta_s}s")
    } else if delta_s < 60 * 60 {
        format!("{}m", delta_s / 60)
    } else if delta_s < 24 * 60 * 60 {
        format!("{}h", delta_s / 3600)
    } else {
        format!("{}d", delta_s / 86_400)
    }
}

fn truncate_single_line(text: &str, max: usize) -> String {
    let mut out = String::with_capacity(max);
    for ch in text.chars() {
        if out.len() >= max {
            out.push_str("…");
            break;
        }
        if ch == '\n' || ch == '\r' {
            out.push(' ');
        } else {
            out.push(ch);
        }
    }
    out
}

fn load_text(
    ws: &mut Workspace<Pile>,
    handle: Value<Handle<Blake3, LongString>>,
) -> Option<String> {
    ws.get::<View<str>, LongString>(handle)
        .ok()
        .map(|view| view.as_ref().to_string())
}

fn now_epoch() -> Epoch {
    Epoch::now().unwrap_or_else(|_| Epoch::from_gregorian_utc(1970, 1, 1, 0, 0, 0, 0))
}

fn epoch_key(epoch: Epoch) -> i128 {
    epoch.to_tai_duration().total_nanoseconds()
}

fn interval_key(interval: Value<NsTAIInterval>) -> i128 {
    let (lower, _): (Epoch, Epoch) = interval.from_value();
    epoch_key(lower)
}

fn u256be_to_u64(value: Value<U256BE>) -> Option<u64> {
    let raw = value.raw;
    if raw[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    let bytes: [u8; 8] = raw[24..32].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

fn id_prefix(id: Id) -> String {
    let raw: [u8; 16] = id.into();
    let mut out = String::with_capacity(8);
    for byte in raw.iter().take(4) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn longstring_handle_prefix(handle: Value<Handle<Blake3, LongString>>) -> String {
    let raw = handle.raw;
    let mut out = String::with_capacity(8);
    for byte in raw.iter().take(4) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn archive_handle_prefix(handle: Value<Handle<Blake3, SimpleArchive>>) -> String {
    let raw = handle.raw;
    let mut out = String::with_capacity(8);
    for byte in raw.iter().take(4) {
        out.push_str(&format!("{byte:02x}"));
    }
    out
}

fn format_id(labels: &HashMap<Id, String>, id: Id) -> String {
    labels.get(&id).cloned().unwrap_or_else(|| id_prefix(id))
}

fn repo_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().map(PathBuf::from).unwrap_or(manifest)
}

fn random_signing_key() -> SigningKey {
    let mut rng = OsRng;
    let mut secret = SecretKey::default();
    let _ = rng.try_fill_bytes(&mut secret);
    SigningKey::from_bytes(&secret)
}
