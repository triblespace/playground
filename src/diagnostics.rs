use ed25519_dalek::{SecretKey, SigningKey};
use eframe::egui;
use hifitime::Epoch;
use rand_core::{OsRng, TryRngCore};
use serde_json::Value as JsonValue;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
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
use triblespace::prelude::valueschemas::{GenId, U256BE};
use triblespace::prelude::{
    Attribute, BlobStore, BlobStoreGet, BranchStore, ToBlob, ToValue, TryFromValue, View,
};

use GORBIE::NotebookConfig;
use GORBIE::NotebookCtx;
use GORBIE::cards::{DEFAULT_CARD_PADDING, with_padding};
use GORBIE::md;
use GORBIE::themes::colorhash;
use GORBIE::widgets::{Button, TextField};

use crate::blob_refs::{PromptChunk, split_blob_refs};
use crate::schema::openai_responses;
use crate::schema::playground_config;
use crate::schema::playground_exec;
use crate::schema::playground_workspace;

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

mod compass {
    use triblespace::prelude::blobschemas::LongString;
    use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, ShortString};
    use triblespace::prelude::*;

    attributes! {
        "EE18CEC15C18438A2FAB670E2E46E00C" as pub title: Handle<Blake3, LongString>;
        "F9B56611861316B31A6C510B081C30B3" as pub created_at: ShortString;
        "5FF4941DCC3F6C35E9B3FD57216F69ED" as pub tag: ShortString;
        "9D2B6EBDA67E9BB6BE6215959D182041" as pub parent: GenId;

        "C1EAAA039DA7F486E4A54CC87D42E72C" as pub task: GenId;
        "61C44E0F8A73443ED592A713151E99A4" as pub status: ShortString;
        "8200ADEDC8D4D3D6D01CDC7396DF9AEC" as pub at: ShortString;
        "47351DF00B3DDA96CB305157CD53D781" as pub note: Handle<Blake3, LongString>;
    }
}

type CommitHandle = Value<Handle<Blake3, SimpleArchive>>;
const ACTIVITY_TIMELINE_HEIGHT: f32 = 980.0;
const LOCAL_COMPOSE_HEIGHT: f32 = 80.0;
const RELATIONS_SCROLL_HEIGHT: f32 = 260.0;
const TEAMS_SCROLL_HEIGHT: f32 = 520.0;
const TEAMS_CHAT_LIST_WIDTH: f32 = 220.0;
const WORKSPACE_SNAPSHOT_LIMIT: usize = 10;

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
const COMPASS_KIND_GOAL_ID: Id = id_hex!("83476541420F46402A6A9911F46FBA3B");
const COMPASS_KIND_STATUS_ID: Id = id_hex!("89602B3277495F4E214D4A417C8CF260");
const COMPASS_KIND_NOTE_ID: Id = id_hex!("D4E49A6F02A14E66B62076AE4C01715F");

const COMPASS_DEFAULT_STATUSES: [&str; 4] = ["todo", "doing", "blocked", "done"];

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
    config_branches: String,
    exec_branches: String,
    compass_branches: String,
    local_message_branches: String,
    relations_branches: String,
    local_me: String,
    local_peer: String,
    teams_branches: String,
    workspace_branches: String,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        let default_pile = diagnostics_default_pile().unwrap_or_else(|| {
            let repo_root = repo_root();
            repo_root.join("self.pile")
        });
        Self {
            pile_path: default_pile.to_string_lossy().to_string(),
            config_branches: "config".to_string(),
            exec_branches: "main".to_string(),
            compass_branches: "compass".to_string(),
            local_message_branches: "local-messages".to_string(),
            relations_branches: "relations".to_string(),
            local_me: "jp".to_string(),
            local_peer: "agent".to_string(),
            teams_branches: "teams".to_string(),
            workspace_branches: "workspace".to_string(),
        }
    }
}

struct DashboardState {
    config: DashboardConfig,
    repo: Option<Repository<Pile>>,
    repo_open_path: Option<PathBuf>,
    signing_key: SigningKey,
    snapshot: Option<Result<DashboardSnapshot, String>>,
    show_extra_branches: bool,
    local_draft: String,
    local_send_error: Option<String>,
    local_send_notice: Option<String>,
    config_reveal_secrets: bool,
    config_last_applied_id: Option<Id>,
    compass_expanded_goal: Option<Id>,
    teams_selected_chat: Option<Id>,
    workspace_selected_snapshot: Option<Id>,
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
            show_extra_branches: false,
            local_draft: String::new(),
            local_send_error: None,
            local_send_notice: None,
            config_reveal_secrets: false,
            config_last_applied_id: None,
            compass_expanded_goal: None,
            teams_selected_chat: None,
            workspace_selected_snapshot: None,
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
struct LocalMessageRow {
    id: Id,
    created_at: Option<i128>,
    from_id: Id,
    to_id: Id,
    body: String,
    readers: Vec<Id>,
}

#[derive(Debug, Clone)]
enum LocalMessageStatus {
    Unread,
    Read,
    Sent,
    ReadBy(String),
    Other,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimelineSource {
    Shell,
    Cognition,
    Teams,
    LocalMessages,
    Goals,
}

#[derive(Debug, Clone)]
enum TimelineEvent {
    Shell {
        status: ExecStatus,
        command: String,
        worker_label: Option<String>,
        exit_code: Option<u64>,
        error: Option<String>,
    },
    Cognition {
        summary: String,
    },
    Teams {
        author: String,
        chat_label: String,
        content: String,
    },
    LocalMessage {
        from_id: Id,
        to_id: Id,
        from_label: String,
        to_label: String,
        status: LocalMessageStatus,
        body: String,
        is_sender: bool,
    },
    GoalCreated {
        goal: CompassTaskRow,
    },
    GoalStatus {
        goal: CompassTaskRow,
        to_status: String,
    },
    GoalNote {
        goal: CompassTaskRow,
        note: String,
    },
}

#[derive(Debug, Clone)]
struct TimelineRow {
    at: Option<i128>,
    source: TimelineSource,
    event: TimelineEvent,
}

#[derive(Debug, Clone)]
struct CompassTaskRow {
    id: Id,
    id_prefix: String,
    title: String,
    tags: Vec<String>,
    created_at: String,
    status: String,
    status_at: Option<String>,
    note_count: usize,
    parent: Option<Id>,
}

impl CompassTaskRow {
    fn sort_key(&self) -> &str {
        self.status_at.as_deref().unwrap_or(&self.created_at)
    }
}

#[derive(Debug, Clone)]
struct CompassNoteRow {
    at: String,
    body: String,
}

#[derive(Debug, Clone)]
struct CompassStatusRow {
    task: Id,
    status: String,
    at: String,
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
struct AgentConfigRow {
    id: Id,
    updated_at: Option<i128>,
    persona_id: Option<Id>,
    branch: Option<String>,
    branch_id: Option<Id>,
    compass_branch_id: Option<Id>,
    exec_branch_id: Option<Id>,
    local_messages_branch_id: Option<Id>,
    relations_branch_id: Option<Id>,
    teams_branch_id: Option<Id>,
    workspace_branch_id: Option<Id>,
    author: Option<String>,
    author_role: Option<String>,
    poll_ms: Option<u64>,
    llm_model: Option<String>,
    llm_base_url: Option<String>,
    llm_reasoning_effort: Option<String>,
    llm_stream: Option<bool>,
    llm_api_key: Option<String>,
    tavily_api_key: Option<String>,
    exa_api_key: Option<String>,
    exec_default_cwd: Option<String>,
    exec_sandbox_profile: Option<Id>,
    system_prompt: Option<String>,
}

#[derive(Debug, Clone)]
struct DashboardSnapshot {
    pile_path: PathBuf,
    branches: Vec<BranchEntry>,
    branch_data: HashMap<Id, BranchSnapshot>,
    exec_error: Option<String>,
    agent_config: Option<AgentConfigRow>,
    agent_config_error: Option<String>,
    timeline_rows: Vec<TimelineRow>,
    compass_rows: Vec<(CompassTaskRow, usize)>,
    compass_notes: HashMap<Id, Vec<CompassNoteRow>>,
    compass_error: Option<String>,
    local_message_error: Option<String>,
    local_me_id: Option<Id>,
    local_peer_id: Option<Id>,
    relations_people: Vec<RelationRow>,
    relations_error: Option<String>,
    relations_labels: HashMap<Id, String>,
    teams_messages: Vec<TeamsMessageRow>,
    teams_chats: Vec<TeamsChatRow>,
    teams_error: Option<String>,
    workspace_snapshots: Vec<WorkspaceSnapshotRow>,
    workspace_entries: Vec<WorkspaceEntryRow>,
    workspace_error: Option<String>,
    now_key: i128,
}

#[derive(Debug, Clone)]
struct WorkspaceSnapshotRow {
    id: Id,
    created_at: Option<i128>,
    label: Option<String>,
    root_path: Option<String>,
    state_handle: Option<Value<Handle<Blake3, SimpleArchive>>>,
    entry_count: usize,
}

#[derive(Debug, Clone)]
struct WorkspaceEntryRow {
    kind: WorkspaceEntryKind,
    path: String,
    link_target: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkspaceEntryKind {
    File,
    Dir,
    Symlink,
    Unknown,
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
                let mut picker_branches = Vec::new();
                let repo_open_result = ensure_repo_open(state);
                if repo_open_result.is_ok() {
                    if let Some(repo) = state.repo.as_mut() {
                        picker_branches = list_branches(repo.storage_mut()).unwrap_or_default();
                        picker_branches
                            .sort_by(|a, b| a.name.cmp(&b.name).then_with(|| a.id.cmp(&b.id)));
                    }
                }
                ui.horizontal(|ui| {
                    ui.label("Config branches");
                    render_branch_picker(
                        ui,
                        "config_branch_picker",
                        &picker_branches,
                        &mut state.config.config_branches,
                    );
                });
                ui.horizontal(|ui| {
                    ui.label("Exec branches");
                    render_branch_picker(
                        ui,
                        "exec_branch_picker",
                        &picker_branches,
                        &mut state.config.exec_branches,
                    );
                });
                ui.horizontal(|ui| {
                    ui.label("Compass branches");
                    render_branch_picker(
                        ui,
                        "compass_branch_picker",
                        &picker_branches,
                        &mut state.config.compass_branches,
                    );
                });
                ui.horizontal(|ui| {
                    ui.label("Local message branches");
                    render_branch_picker(
                        ui,
                        "local_message_branch_picker",
                        &picker_branches,
                        &mut state.config.local_message_branches,
                    );
                });
                ui.horizontal(|ui| {
                    ui.label("Relations branches");
                    render_branch_picker(
                        ui,
                        "relations_branch_picker",
                        &picker_branches,
                        &mut state.config.relations_branches,
                    );
                });
                ui.horizontal(|ui| {
                    ui.label("Teams branches");
                    render_branch_picker(
                        ui,
                        "teams_branch_picker",
                        &picker_branches,
                        &mut state.config.teams_branches,
                    );
                });
                ui.horizontal(|ui| {
                    ui.label("Workspace branches");
                    render_branch_picker(
                        ui,
                        "workspace_branch_picker",
                        &picker_branches,
                        &mut state.config.workspace_branches,
                    );
                });

                if let Err(err) = repo_open_result {
                    state.snapshot = Some(Err(err.to_string()));
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
        let mut state = dashboard.read_mut(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Overview");
            let (pile_path, branches) = {
                let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                    return;
                };
                (snapshot.pile_path.clone(), snapshot.branches.clone())
            };

            ui.horizontal(|ui| {
                ui.label(format!("Pile: {}", pile_path.display()));
            });

            if branches.is_empty() {
                return;
            }

            let mut primary: Vec<BranchEntry> = Vec::new();
            let mut extra: Vec<BranchEntry> = Vec::new();
            for branch in branches {
                let label = branch.name.as_deref().unwrap_or("<unnamed>");
                if label.contains("--orphan-") || label.starts_with('<') {
                    extra.push(branch);
                } else {
                    primary.push(branch);
                }
            }

            ui.label(format!(
                "Branches: {} primary, {} extra",
                primary.len(),
                extra.len()
            ));

            ui.label("Primary:");
            for branch in &primary {
                let label = branch.name.as_deref().unwrap_or("<unnamed>");
                ui.label(format!("- {label} ({})", id_prefix(branch.id)));
            }

            if !extra.is_empty() {
                let button_label = if state.show_extra_branches {
                    "Hide extra branches"
                } else {
                    "Show extra branches"
                };
                if ui.add(Button::new(button_label)).clicked() {
                    state.show_extra_branches = !state.show_extra_branches;
                }

                if state.show_extra_branches {
                    ui.add_space(8.0);
                    ui.label("Extra:");
                    for branch in &extra {
                        let label = branch.name.as_deref().unwrap_or("<unnamed>");
                        ui.label(format!("- {label} ({})", id_prefix(branch.id)));
                    }
                }
            }
        });
    });

    nb.view(move |ui| {
        let mut state = dashboard.read_mut(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Agent config");
            let snapshot = {
                let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                    return;
                };
                snapshot.clone()
            };
            if let Some(err) = &snapshot.agent_config_error {
                ui.colored_label(egui::Color32::RED, err);
            } else {
                render_agent_config(
                    ui,
                    &mut state,
                    snapshot.now_key,
                    snapshot.agent_config.as_ref(),
                );
            }
        });
    });

    nb.view(move |ui| {
        let mut state = dashboard.read_mut(ui);
        ui.heading("Compass");
        let snapshot = {
            let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                return;
            };
            snapshot.clone()
        };
        if let Some(err) = &snapshot.compass_error {
            ui.colored_label(egui::Color32::RED, err);
            return;
        }

        if snapshot.compass_rows.is_empty() {
            ui.label("No goals yet.");
            return;
        }
        render_compass_swimlanes(
            ui,
            &mut state.compass_expanded_goal,
            &snapshot.compass_rows,
            &snapshot.compass_notes,
        );
    });

    nb.view(move |ui| {
        let state = dashboard.read(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Activity timeline");
            let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                return;
            };
            if let Some(err) = &snapshot.exec_error {
                ui.colored_label(egui::Color32::RED, format!("Exec branch: {err}"));
            }
            if snapshot.timeline_rows.is_empty() {
                ui.label("No activity yet.");
            } else {
                render_activity_timeline(ui, snapshot.now_key, &snapshot.timeline_rows);
            }
        });
    });

    nb.view(move |ui| {
        let mut state = dashboard.read_mut(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Local message composer");
            let snapshot = {
                let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                    return;
                };
                snapshot.clone()
            };
            if let Some(err) = &snapshot.local_message_error {
                ui.colored_label(egui::Color32::RED, err);
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

    nb.view(move |ui| {
        let mut state = dashboard.read_mut(ui);
        with_padding(ui, padding, |ui| {
            ui.heading("Workspace");
            let snapshot = {
                let Some(snapshot) = snapshot_or_message(ui, &state.snapshot) else {
                    return;
                };
                snapshot.clone()
            };
            if let Some(err) = &snapshot.workspace_error {
                ui.colored_label(egui::Color32::RED, err);
            }
            render_workspace(
                ui,
                &mut state,
                snapshot.now_key,
                &snapshot.workspace_snapshots,
                &snapshot.workspace_entries,
            );
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
    let workspace_selected = state.workspace_selected_snapshot;
    let repo = match state.repo.as_mut() {
        Some(repo) => repo,
        None => {
            state.snapshot = Some(Err("Repository not open.".to_string()));
            return;
        }
    };
    let result = load_snapshot(repo, &config, previous, workspace_selected);
    if let Ok(snapshot) = &result {
        if let Some(agent_config) = snapshot.agent_config.as_ref() {
            apply_branch_defaults_from_agent_config(state, agent_config);
        }
    }
    state.snapshot = Some(result);
}

fn apply_branch_defaults_from_agent_config(state: &mut DashboardState, config: &AgentConfigRow) {
    if state.config_last_applied_id == Some(config.id) {
        return;
    }

    // Use the config entry as a stable "defaults bundle" for diagnostics selectors.
    if let Some(id) = config.exec_branch_id.or(config.branch_id) {
        state.config.exec_branches = format!("{id:x}");
    }
    if let Some(id) = config.compass_branch_id {
        state.config.compass_branches = format!("{id:x}");
    }
    if let Some(id) = config.local_messages_branch_id {
        state.config.local_message_branches = format!("{id:x}");
    }
    if let Some(id) = config.relations_branch_id {
        state.config.relations_branches = format!("{id:x}");
    }
    if let Some(id) = config.teams_branch_id {
        state.config.teams_branches = format!("{id:x}");
    }
    if let Some(id) = config.workspace_branch_id {
        state.config.workspace_branches = format!("{id:x}");
    }

    state.config_last_applied_id = Some(config.id);
}

fn load_snapshot(
    repo: &mut Repository<Pile>,
    config: &DashboardConfig,
    previous: Option<DashboardSnapshot>,
    workspace_selected_snapshot: Option<Id>,
) -> Result<DashboardSnapshot, String> {
    let pile_path = PathBuf::from(&config.pile_path);
    let mut branches = list_branches(repo.storage_mut())?;
    let mut previous_map = previous
        .as_ref()
        .filter(|snapshot| snapshot.pile_path == pile_path)
        .map(|snapshot| snapshot.branch_data.clone())
        .unwrap_or_default();

    let config_refs = parse_branch_list(&config.config_branches);
    let exec_refs = parse_branch_list(&config.exec_branches);
    let compass_refs = parse_branch_list(&config.compass_branches);
    let local_refs = parse_branch_list(&config.local_message_branches);
    let relations_refs = parse_branch_list(&config.relations_branches);
    let teams_refs = parse_branch_list(&config.teams_branches);
    let workspace_refs = parse_branch_list(&config.workspace_branches);

    let mut ensure_refs = Vec::new();
    ensure_refs.extend(config_refs.iter().cloned());
    ensure_refs.extend(exec_refs.iter().cloned());
    ensure_refs.extend(compass_refs.iter().cloned());
    ensure_refs.extend(local_refs.iter().cloned());
    ensure_refs.extend(relations_refs.iter().cloned());
    ensure_refs.extend(teams_refs.iter().cloned());
    ensure_refs.extend(workspace_refs.iter().cloned());
    ensure_named_branches(repo, &mut branches, &ensure_refs)?;

    let branch_lookup = BranchLookup::new(&branches);
    let config_res = resolve_branch_ids(&branch_lookup, &config_refs);
    let exec_res = resolve_branch_ids(&branch_lookup, &exec_refs);
    let compass_res = resolve_branch_ids(&branch_lookup, &compass_refs);
    let local_res = resolve_branch_ids(&branch_lookup, &local_refs);
    let relations_res = resolve_branch_ids(&branch_lookup, &relations_refs);
    let teams_res = resolve_branch_ids(&branch_lookup, &teams_refs);
    let workspace_res = resolve_branch_ids(&branch_lookup, &workspace_refs);

    let agent_config_error = config_res.as_ref().err().cloned();
    let exec_error = exec_res.as_ref().err().cloned();
    let compass_error = compass_res.as_ref().err().cloned();
    let local_message_error = local_res.as_ref().err().cloned();
    let relations_error = relations_res.as_ref().err().cloned();
    let teams_error = teams_res.as_ref().err().cloned();
    let workspace_error = workspace_res.as_ref().err().cloned();

    let config_ids = config_res.unwrap_or_default();
    let exec_ids = exec_res.unwrap_or_default();
    let compass_ids = compass_res.unwrap_or_default();
    let local_ids = local_res.unwrap_or_default();
    let relations_ids = relations_res.unwrap_or_default();
    let teams_ids = teams_res.unwrap_or_default();
    let workspace_ids = workspace_res.unwrap_or_default();

    let mut needed_ids: Vec<Id> = Vec::new();
    extend_unique(&mut needed_ids, &config_ids);
    extend_unique(&mut needed_ids, &exec_ids);
    extend_unique(&mut needed_ids, &compass_ids);
    extend_unique(&mut needed_ids, &local_ids);
    extend_unique(&mut needed_ids, &relations_ids);
    extend_unique(&mut needed_ids, &teams_ids);
    extend_unique(&mut needed_ids, &workspace_ids);

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

    let config_data = union_branches(&branch_data, &config_ids);
    let exec_data = union_branches(&branch_data, &exec_ids);
    let compass_data = union_branches(&branch_data, &compass_ids);
    let local_data = union_branches(&branch_data, &local_ids);
    let relations_data = union_branches(&branch_data, &relations_ids);
    let teams_data = union_branches(&branch_data, &teams_ids);
    let workspace_data = union_branches(&branch_data, &workspace_ids);

    let mut reader_ws = if let Some(ws) = reader_ws {
        ws
    } else {
        let now_key = epoch_key(now_epoch());
        return Ok(DashboardSnapshot {
            pile_path,
            branches,
            branch_data,
            exec_error,
            agent_config: None,
            agent_config_error,
            timeline_rows: Vec::new(),
            compass_rows: Vec::new(),
            compass_notes: HashMap::new(),
            compass_error,
            local_message_error,
            local_me_id: None,
            local_peer_id: None,
            relations_people: Vec::new(),
            relations_error,
            relations_labels: HashMap::new(),
            teams_messages: Vec::new(),
            teams_chats: Vec::new(),
            teams_error,
            workspace_snapshots: Vec::new(),
            workspace_entries: Vec::new(),
            workspace_error,
            now_key,
        });
    };

    Ok(build_snapshot(
        config_data,
        exec_data,
        compass_data,
        local_data,
        relations_data,
        teams_data,
        workspace_data,
        pile_path,
        branches,
        branch_data,
        agent_config_error,
        exec_error,
        compass_error,
        local_message_error,
        relations_error,
        teams_error,
        workspace_error,
        config,
        &mut reader_ws,
        workspace_selected_snapshot,
    ))
}

fn build_snapshot(
    config_data: TribleSet,
    exec_data: TribleSet,
    compass_data: TribleSet,
    local_data: TribleSet,
    relations_data: TribleSet,
    teams_data: TribleSet,
    workspace_data: TribleSet,
    pile_path: PathBuf,
    branches: Vec<BranchEntry>,
    branch_data: HashMap<Id, BranchSnapshot>,
    agent_config_error: Option<String>,
    exec_error: Option<String>,
    compass_error: Option<String>,
    local_message_error: Option<String>,
    relations_error: Option<String>,
    teams_error: Option<String>,
    workspace_error: Option<String>,
    config: &DashboardConfig,
    ws: &mut Workspace<Pile>,
    workspace_selected_snapshot: Option<Id>,
) -> DashboardSnapshot {
    let now_key = epoch_key(now_epoch());
    let relations_people = collect_relations_people(&relations_data, ws);
    let relations_labels = collect_relations_labels(&relations_people);
    let local_me_id = resolve_person_ref(&relations_people, &config.local_me);
    let local_peer_id = resolve_person_ref(&relations_people, &config.local_peer);
    let agent_config = collect_agent_config(&config_data, ws);
    let exec_rows = collect_exec_rows(&exec_data, ws);
    let reasoning_summaries = collect_reasoning_summaries(&exec_data, ws);
    let compass_rows = collect_compass_rows(&compass_data, ws);
    let compass_status_rows = collect_compass_status_rows(&compass_data);
    let compass_notes = collect_compass_notes(&compass_data, ws);
    let local_message_rows = collect_local_messages(&local_data, ws);
    let (teams_messages, teams_chats) = collect_teams_messages(&teams_data, ws);
    let workspace_snapshots = collect_workspace_snapshots(&workspace_data, ws);
    let workspace_latest_id = workspace_snapshots.first().map(|row| row.id);
    let workspace_preview_id = workspace_selected_snapshot.or(workspace_latest_id);
    let workspace_entries = collect_workspace_entries(&workspace_data, ws, workspace_preview_id);
    let labels = collect_labels(&exec_data, ws);
    let timeline_rows = build_activity_timeline(
        &exec_rows,
        &reasoning_summaries,
        &local_message_rows,
        local_me_id,
        &relations_labels,
        &teams_messages,
        &teams_chats,
        &compass_rows,
        &compass_status_rows,
        &compass_notes,
        &labels,
    );

    DashboardSnapshot {
        pile_path,
        branches,
        branch_data,
        exec_error,
        agent_config,
        agent_config_error,
        timeline_rows,
        compass_rows,
        compass_notes,
        compass_error,
        local_message_error,
        local_me_id,
        local_peer_id,
        relations_people,
        relations_error,
        relations_labels,
        teams_messages,
        teams_chats,
        teams_error,
        workspace_snapshots,
        workspace_entries,
        workspace_error,
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
                        data += delta;
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
            union += snapshot.data.clone();
        }
    }
    union
}

fn collect_agent_config(data: &TribleSet, ws: &mut Workspace<Pile>) -> Option<AgentConfigRow> {
    let mut latest: Option<(Id, i128)> = None;
    for (config_id, updated_at) in find!(
        (config_id: Id, updated_at: Value<NsTAIInterval>),
        pattern!(data, [{
            ?config_id @
            playground_config::kind: playground_config::kind_config,
            playground_config::updated_at: ?updated_at,
        }])
    ) {
        let key = interval_key(updated_at);
        if latest.map_or(true, |(_, current)| key > current) {
            latest = Some((config_id, key));
        }
    }

    let Some((config_id, updated_key)) = latest else {
        return None;
    };

    let persona_id = load_optional_id_attr(data, config_id, playground_config::persona_id);
    let branch = load_optional_string_attr(data, ws, config_id, playground_config::branch);
    let branch_id = load_optional_id_attr(data, config_id, playground_config::branch_id);
    let compass_branch_id =
        load_optional_id_attr(data, config_id, playground_config::compass_branch_id);
    let exec_branch_id = load_optional_id_attr(data, config_id, playground_config::exec_branch_id);
    let local_messages_branch_id =
        load_optional_id_attr(data, config_id, playground_config::local_messages_branch_id);
    let relations_branch_id =
        load_optional_id_attr(data, config_id, playground_config::relations_branch_id);
    let teams_branch_id =
        load_optional_id_attr(data, config_id, playground_config::teams_branch_id);
    let workspace_branch_id =
        load_optional_id_attr(data, config_id, playground_config::workspace_branch_id);
    let author = load_optional_string_attr(data, ws, config_id, playground_config::author);
    let author_role =
        load_optional_string_attr(data, ws, config_id, playground_config::author_role);
    let poll_ms = load_optional_u64_attr(data, config_id, playground_config::poll_ms);
    let llm_model = load_optional_string_attr(data, ws, config_id, playground_config::llm_model);
    let llm_base_url =
        load_optional_string_attr(data, ws, config_id, playground_config::llm_base_url);
    let llm_reasoning_effort =
        load_optional_string_attr(data, ws, config_id, playground_config::llm_reasoning_effort);
    let llm_stream = load_optional_u64_attr(data, config_id, playground_config::llm_stream)
        .map(|value| value != 0);
    let llm_api_key =
        load_optional_string_attr(data, ws, config_id, playground_config::llm_api_key);
    let tavily_api_key =
        load_optional_string_attr(data, ws, config_id, playground_config::tavily_api_key);
    let exa_api_key =
        load_optional_string_attr(data, ws, config_id, playground_config::exa_api_key);
    let exec_default_cwd =
        load_optional_string_attr(data, ws, config_id, playground_config::exec_default_cwd);
    let exec_sandbox_profile =
        load_optional_id_attr(data, config_id, playground_config::exec_sandbox_profile);
    let system_prompt =
        load_optional_string_attr(data, ws, config_id, playground_config::system_prompt);

    Some(AgentConfigRow {
        id: config_id,
        updated_at: Some(updated_key),
        persona_id,
        branch,
        branch_id,
        compass_branch_id,
        exec_branch_id,
        local_messages_branch_id,
        relations_branch_id,
        teams_branch_id,
        workspace_branch_id,
        author,
        author_role,
        poll_ms,
        llm_model,
        llm_base_url,
        llm_reasoning_effort,
        llm_stream,
        llm_api_key,
        tavily_api_key,
        exa_api_key,
        exec_default_cwd,
        exec_sandbox_profile,
        system_prompt,
    })
}

fn load_optional_id_attr(data: &TribleSet, entity_id: Id, attr: Attribute<GenId>) -> Option<Id> {
    find!(
        (entity: Id, value: Value<GenId>),
        pattern!(data, [{ ?entity @ attr: ?value }])
    )
    .into_iter()
    .find_map(|(entity, value)| (entity == entity_id).then_some(Id::try_from_value(&value).ok())?)
}

fn load_optional_u64_attr(data: &TribleSet, entity_id: Id, attr: Attribute<U256BE>) -> Option<u64> {
    find!(
        (entity: Id, value: Value<U256BE>),
        pattern!(data, [{ ?entity @ attr: ?value }])
    )
    .into_iter()
    .find_map(|(entity, value)| (entity == entity_id).then_some(value))
    .and_then(u256be_to_u64)
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

fn collect_local_messages(data: &TribleSet, ws: &mut Workspace<Pile>) -> Vec<LocalMessageRow> {
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
            readers: Vec::new(),
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

    for row in &mut rows {
        if let Some(readers) = reads.get(&row.id) {
            let mut reader_list: Vec<Id> = readers.iter().copied().collect();
            reader_list.sort_by_key(|id| format!("{id:x}"));
            row.readers = reader_list;
        }
    }

    rows.sort_by_key(|row| row.created_at.unwrap_or(i128::MIN));
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

fn collect_workspace_snapshots(
    data: &TribleSet,
    ws: &mut Workspace<Pile>,
) -> Vec<WorkspaceSnapshotRow> {
    let mut rows = Vec::new();

    for (snapshot_id, created_at) in find!(
        (snapshot_id: Id, created_at: Value<NsTAIInterval>),
        pattern!(data, [{
            ?snapshot_id @
            playground_workspace::kind: playground_workspace::kind_snapshot,
            playground_workspace::created_at: ?created_at,
        }])
    ) {
        let created_key = interval_key(created_at);
        let label = load_optional_string_attr(data, ws, snapshot_id, playground_workspace::label);
        let root_path =
            load_optional_string_attr(data, ws, snapshot_id, playground_workspace::root_path);
        let state_handle =
            load_optional_archive_handle_attr(data, snapshot_id, playground_workspace::state);
        let entry_count = count_workspace_entries(data, snapshot_id);

        rows.push(WorkspaceSnapshotRow {
            id: snapshot_id,
            created_at: Some(created_key),
            label,
            root_path,
            state_handle,
            entry_count,
        });
    }

    rows.sort_by_key(|row| row.created_at.unwrap_or(i128::MIN));
    rows.reverse();
    rows.truncate(WORKSPACE_SNAPSHOT_LIMIT);
    rows
}

fn collect_workspace_entries(
    data: &TribleSet,
    ws: &mut Workspace<Pile>,
    snapshot_id: Option<Id>,
) -> Vec<WorkspaceEntryRow> {
    let Some(snapshot_id) = snapshot_id else {
        return Vec::new();
    };

    let snapshot_root =
        load_optional_string_attr(data, ws, snapshot_id, playground_workspace::root_path);
    let root_prefix = snapshot_root
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty() && *value != ".")
        .map(|value| value.trim_end_matches('/').to_string());

    let entry_ids: Vec<Id> = find!(
        (entry_id: Id),
        pattern!(data, [{
            snapshot_id @
            playground_workspace::entry: ?entry_id,
        }])
    )
    .into_iter()
    .map(|(id,)| id)
    .collect();

    let mut paths: HashMap<Id, Value<Handle<Blake3, LongString>>> = HashMap::new();
    for (entry_id, handle) in find!(
        (entry_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?entry_id @ playground_workspace::path: ?handle }])
    ) {
        paths.insert(entry_id, handle);
    }

    let mut kinds: HashMap<Id, Id> = HashMap::new();
    for (entry_id, kind) in find!(
        (entry_id: Id, kind: Id),
        pattern!(data, [{ ?entry_id @ playground_workspace::kind: ?kind }])
    ) {
        kinds.insert(entry_id, kind);
    }

    let mut link_targets: HashMap<Id, Value<Handle<Blake3, LongString>>> = HashMap::new();
    for (entry_id, handle) in find!(
        (entry_id: Id, handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{ ?entry_id @ playground_workspace::link_target: ?handle }])
    ) {
        link_targets.insert(entry_id, handle);
    }

    let mut rows = Vec::new();
    for entry_id in entry_ids {
        let path = paths
            .get(&entry_id)
            .copied()
            .and_then(|handle| load_text(ws, handle))
            .unwrap_or_else(|| "<missing>".to_string());
        let display_path = if let Some(prefix) = root_prefix.as_ref() {
            format!("{}/{}", prefix, path)
        } else {
            path
        };

        let kind = match kinds.get(&entry_id).copied() {
            Some(id) if id == playground_workspace::kind_file => WorkspaceEntryKind::File,
            Some(id) if id == playground_workspace::kind_dir => WorkspaceEntryKind::Dir,
            Some(id) if id == playground_workspace::kind_symlink => WorkspaceEntryKind::Symlink,
            Some(_) | None => WorkspaceEntryKind::Unknown,
        };

        let link_target = link_targets
            .get(&entry_id)
            .copied()
            .and_then(|handle| load_text(ws, handle));

        rows.push(WorkspaceEntryRow {
            kind,
            path: display_path,
            link_target,
        });
    }

    rows.sort_by(|a, b| a.path.cmp(&b.path));
    rows
}

fn count_workspace_entries(data: &TribleSet, snapshot_id: Id) -> usize {
    find!(
        (entry_id: Id),
        pattern!(data, [{
            snapshot_id @
            playground_workspace::entry: ?entry_id,
        }])
    )
    .into_iter()
    .count()
}

fn load_optional_string_attr(
    data: &TribleSet,
    ws: &mut Workspace<Pile>,
    entity_id: Id,
    attr: Attribute<Handle<Blake3, LongString>>,
) -> Option<String> {
    find!(
        (handle: Value<Handle<Blake3, LongString>>),
        pattern!(data, [{
            entity_id @
            attr: ?handle,
        }])
    )
    .into_iter()
    .next()
    .and_then(|(handle,)| load_text(ws, handle))
}

fn load_optional_archive_handle_attr(
    data: &TribleSet,
    entity_id: Id,
    attr: Attribute<Handle<Blake3, SimpleArchive>>,
) -> Option<Value<Handle<Blake3, SimpleArchive>>> {
    find!(
        (handle: Value<Handle<Blake3, SimpleArchive>>),
        pattern!(data, [{
            entity_id @
            attr: ?handle,
        }])
    )
    .into_iter()
    .next()
    .map(|(handle,)| handle)
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
    rows
}

fn build_activity_timeline(
    exec_rows: &[ExecRow],
    reasoning_rows: &[ReasoningSummaryRow],
    local_rows: &[LocalMessageRow],
    local_me_id: Option<Id>,
    relation_labels: &HashMap<Id, String>,
    teams_rows: &[TeamsMessageRow],
    teams_chats: &[TeamsChatRow],
    compass_rows: &[(CompassTaskRow, usize)],
    compass_status_rows: &[CompassStatusRow],
    compass_notes: &HashMap<Id, Vec<CompassNoteRow>>,
    labels: &HashMap<Id, String>,
) -> Vec<TimelineRow> {
    let mut rows = Vec::new();

    for row in exec_rows {
        rows.push(TimelineRow {
            at: row.finished_at.or(row.started_at).or(row.requested_at),
            source: TimelineSource::Shell,
            event: TimelineEvent::Shell {
                status: row.status,
                command: row.command.clone(),
                worker_label: row.worker.map(|worker_id| format_id(labels, worker_id)),
                exit_code: row.exit_code,
                error: row.error.clone(),
            },
        });
    }

    for row in reasoning_rows {
        rows.push(TimelineRow {
            at: row.created_at,
            source: TimelineSource::Cognition,
            event: TimelineEvent::Cognition {
                summary: row.summary.clone(),
            },
        });
    }

    for row in local_rows {
        let from_label = format_id(relation_labels, row.from_id);
        let to_label = format_id(relation_labels, row.to_id);
        let status = local_message_status(row, local_me_id, relation_labels);
        rows.push(TimelineRow {
            at: row.created_at,
            source: TimelineSource::LocalMessages,
            event: TimelineEvent::LocalMessage {
                from_id: row.from_id,
                to_id: row.to_id,
                from_label,
                to_label,
                status,
                body: row.body.clone(),
                is_sender: local_me_id == Some(row.from_id),
            },
        });
    }

    let mut team_chat_labels: HashMap<Id, String> = HashMap::new();
    for row in teams_chats {
        team_chat_labels.insert(row.id, row.label.clone());
    }
    for row in teams_rows {
        let author = row.author_name.as_deref().unwrap_or("unknown");
        let chat = team_chat_labels
            .get(&row.chat_id)
            .cloned()
            .unwrap_or_else(|| id_prefix(row.chat_id));
        rows.push(TimelineRow {
            at: row.created_at,
            source: TimelineSource::Teams,
            event: TimelineEvent::Teams {
                author: author.to_string(),
                chat_label: chat,
                content: row.content.clone(),
            },
        });
    }

    let mut goal_rows: HashMap<Id, CompassTaskRow> = HashMap::new();
    for (goal, _depth) in compass_rows {
        goal_rows.insert(goal.id, goal.clone());
        rows.push(TimelineRow {
            at: parse_compass_stamp(&goal.created_at),
            source: TimelineSource::Goals,
            event: TimelineEvent::GoalCreated { goal: goal.clone() },
        });
    }

    let fallback_goal = |task_id: Id| CompassTaskRow {
        id: task_id,
        id_prefix: id_prefix(task_id),
        title: format!("[{}]", id_prefix(task_id)),
        tags: Vec::new(),
        created_at: String::new(),
        status: "todo".to_string(),
        status_at: None,
        note_count: 0,
        parent: None,
    };

    for status in compass_status_rows {
        let mut goal = goal_rows
            .get(&status.task)
            .cloned()
            .unwrap_or_else(|| fallback_goal(status.task));
        goal.status = status.status.clone();
        goal.status_at = Some(status.at.clone());
        rows.push(TimelineRow {
            at: parse_compass_stamp(&status.at),
            source: TimelineSource::Goals,
            event: TimelineEvent::GoalStatus {
                goal,
                to_status: status.status.clone(),
            },
        });
    }

    for (task_id, notes) in compass_notes {
        let goal = goal_rows
            .get(task_id)
            .cloned()
            .unwrap_or_else(|| fallback_goal(*task_id));
        for note in notes {
            rows.push(TimelineRow {
                at: parse_compass_stamp(&note.at),
                source: TimelineSource::Goals,
                event: TimelineEvent::GoalNote {
                    goal: goal.clone(),
                    note: note.body.clone(),
                },
            });
        }
    }

    rows.sort_by_key(|row| row.at.unwrap_or(i128::MIN));
    rows.reverse();
    rows
}

fn parse_compass_stamp(stamp: &str) -> Option<i128> {
    Epoch::from_gregorian_str(stamp).ok().map(epoch_key)
}

fn collect_compass_rows(
    data: &TribleSet,
    ws: &mut Workspace<Pile>,
) -> Vec<(CompassTaskRow, usize)> {
    let mut tasks: HashMap<Id, CompassTaskRow> = HashMap::new();

    for (task_id, title_handle, created_at) in find!(
        (
            task_id: Id,
            title_handle: Value<Handle<Blake3, LongString>>,
            created_at: String
        ),
        pattern!(&data, [{
            ?task_id @
            metadata::tag: &COMPASS_KIND_GOAL_ID,
            compass::title: ?title_handle,
            compass::created_at: ?created_at,
        }])
    ) {
        if tasks.contains_key(&task_id) {
            continue;
        }
        let title = load_text(ws, title_handle).unwrap_or_else(|| "<missing>".to_string());
        tasks.insert(
            task_id,
            CompassTaskRow {
                id: task_id,
                id_prefix: id_prefix(task_id),
                title,
                tags: Vec::new(),
                created_at,
                status: "todo".to_string(),
                status_at: None,
                note_count: 0,
                parent: None,
            },
        );
    }

    for (task_id, tag) in find!(
        (task_id: Id, tag: String),
        pattern!(&data, [{ ?task_id @ metadata::tag: &COMPASS_KIND_GOAL_ID, compass::tag: ?tag }])
    ) {
        if let Some(task) = tasks.get_mut(&task_id) {
            task.tags.push(tag);
        }
    }

    for (task_id, parent_id) in find!(
        (task_id: Id, parent_id: Id),
        pattern!(&data, [{
            ?task_id @
            metadata::tag: &COMPASS_KIND_GOAL_ID,
            compass::parent: ?parent_id,
        }])
    ) {
        if let Some(task) = tasks.get_mut(&task_id) {
            task.parent = Some(parent_id);
        }
    }

    let mut status_map: HashMap<Id, (String, String)> = HashMap::new();
    for (task_id, status, at) in find!(
        (task_id: Id, status: String, at: String),
        pattern!(&data, [{
            _?event @
            metadata::tag: &COMPASS_KIND_STATUS_ID,
            compass::task: ?task_id,
            compass::status: ?status,
            compass::at: ?at,
        }])
    ) {
        status_map
            .entry(task_id)
            .and_modify(|current| {
                if at > current.1 {
                    *current = (status.clone(), at.clone());
                }
            })
            .or_insert_with(|| (status, at));
    }

    let mut note_counts: HashMap<Id, usize> = HashMap::new();
    for (task_id,) in find!(
        (task_id: Id),
        pattern!(&data, [{
            _?event @
            metadata::tag: &COMPASS_KIND_NOTE_ID,
            compass::task: ?task_id,
        }])
    ) {
        *note_counts.entry(task_id).or_insert(0) += 1;
    }

    for task in tasks.values_mut() {
        if let Some((status, at)) = status_map.get(&task.id) {
            task.status = status.clone();
            task.status_at = Some(at.clone());
        }
        if let Some(count) = note_counts.get(&task.id) {
            task.note_count = *count;
        }
        task.tags.sort();
        task.tags.dedup();
    }

    order_compass_rows(tasks.into_values().collect())
}

fn collect_compass_status_rows(data: &TribleSet) -> Vec<CompassStatusRow> {
    let mut rows = Vec::new();
    for (task_id, status, at) in find!(
        (task_id: Id, status: String, at: String),
        pattern!(&data, [{
            _?event @
            metadata::tag: &COMPASS_KIND_STATUS_ID,
            compass::task: ?task_id,
            compass::status: ?status,
            compass::at: ?at,
        }])
    ) {
        rows.push(CompassStatusRow {
            task: task_id,
            status,
            at,
        });
    }
    rows.sort_by(|a, b| b.at.cmp(&a.at));
    rows
}

fn collect_compass_notes(
    data: &TribleSet,
    ws: &mut Workspace<Pile>,
) -> HashMap<Id, Vec<CompassNoteRow>> {
    let mut map: HashMap<Id, Vec<CompassNoteRow>> = HashMap::new();

    for (task_id, note_handle, at) in find!(
        (task_id: Id, note_handle: Value<Handle<Blake3, LongString>>, at: String),
        pattern!(&data, [{
            _?event @
            metadata::tag: &COMPASS_KIND_NOTE_ID,
            compass::task: ?task_id,
            compass::note: ?note_handle,
            compass::at: ?at,
        }])
    ) {
        let body = load_text(ws, note_handle).unwrap_or_else(|| "<missing>".to_string());
        map.entry(task_id)
            .or_default()
            .push(CompassNoteRow { at, body });
    }

    for notes in map.values_mut() {
        // ISO-like timestamps sort lexicographically.
        notes.sort_by(|a, b| b.at.cmp(&a.at));
    }

    map
}

fn order_compass_rows(rows: Vec<CompassTaskRow>) -> Vec<(CompassTaskRow, usize)> {
    let mut by_id: HashMap<Id, CompassTaskRow> = HashMap::new();
    for row in rows {
        by_id.insert(row.id, row);
    }
    let ids: HashSet<Id> = by_id.keys().copied().collect();
    let mut children: HashMap<Id, Vec<Id>> = HashMap::new();
    let mut roots = Vec::new();

    for (id, row) in &by_id {
        if let Some(parent) = row.parent {
            if ids.contains(&parent) {
                children.entry(parent).or_default().push(*id);
                continue;
            }
        }
        roots.push(*id);
    }

    let sort_ids = |items: &mut Vec<Id>| {
        items.sort_by(|a, b| {
            let a_key = by_id.get(a).map(|row| row.sort_key()).unwrap_or("");
            let b_key = by_id.get(b).map(|row| row.sort_key()).unwrap_or("");
            b_key.cmp(a_key)
        });
    };

    sort_ids(&mut roots);
    for kids in children.values_mut() {
        sort_ids(kids);
    }

    let mut ordered = Vec::new();
    let mut visited = HashSet::new();

    fn walk(
        id: Id,
        depth: usize,
        by_id: &HashMap<Id, CompassTaskRow>,
        children: &HashMap<Id, Vec<Id>>,
        visited: &mut HashSet<Id>,
        out: &mut Vec<(CompassTaskRow, usize)>,
    ) {
        if !visited.insert(id) {
            return;
        }
        let Some(row) = by_id.get(&id) else {
            return;
        };
        out.push((row.clone(), depth));
        if let Some(kids) = children.get(&id) {
            for kid in kids {
                walk(*kid, depth + 1, by_id, children, visited, out);
            }
        }
    }

    for root in roots {
        walk(root, 0, &by_id, &children, &mut visited, &mut ordered);
    }

    for id in by_id.keys() {
        if !visited.contains(id) {
            walk(*id, 0, &by_id, &children, &mut visited, &mut ordered);
        }
    }

    ordered
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
    let me_label = snapshot
        .local_me_id
        .and_then(|id| snapshot.relations_labels.get(&id).cloned())
        .unwrap_or_else(|| state.config.local_me.clone());
    let peer_label = snapshot
        .local_peer_id
        .and_then(|id| snapshot.relations_labels.get(&id).cloned())
        .unwrap_or_else(|| state.config.local_peer.clone());

    ui.horizontal(|ui| {
        ui.label("Me");
        render_person_picker(
            ui,
            "local_me_picker",
            &snapshot.relations_people,
            snapshot.local_me_id,
            &mut state.config.local_me,
        );
        ui.add_space(10.0);
        ui.label("Peer");
        render_person_picker(
            ui,
            "local_peer_picker",
            &snapshot.relations_people,
            snapshot.local_peer_id,
            &mut state.config.local_peer,
        );
    });

    ui.small(format!("{me_label} → {peer_label}"));
    let me_known = snapshot.local_me_id.is_some();
    let peer_known = snapshot.local_peer_id.is_some();
    if !(me_known && peer_known) {
        ui.small("Select Me and Peer from Relations to enable sending.");
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
        let can_send = me_known && peer_known && !state.local_draft.trim().is_empty();
        if ui.add_enabled(can_send, Button::new("Send")).clicked() {
            send_local_message_from_ui(state, branches, snapshot);
        }
        if ui.add(Button::new("Clear")).clicked() {
            state.local_draft.clear();
            state.local_send_error = None;
            state.local_send_notice = None;
        }
        if let Some(note) = &state.local_send_notice {
            ui.label(note);
        }
    });
    if let Some(err) = &state.local_send_error {
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

fn render_branch_picker(
    ui: &mut egui::Ui,
    id_salt: &'static str,
    branches: &[BranchEntry],
    raw: &mut String,
) {
    if branches.is_empty() {
        return;
    }

    let mut name_counts: HashMap<String, usize> = HashMap::new();
    for branch in branches {
        if let Some(name) = branch.name.as_ref() {
            *name_counts.entry(name.clone()).or_insert(0) += 1;
        }
    }

    let refs = parse_branch_list(raw);
    let lookup = BranchLookup::new(branches);
    let selected_ids = resolve_branch_ids(&lookup, &refs).unwrap_or_default();

    let selected_text = if selected_ids.is_empty() {
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            "Pick...".to_string()
        } else {
            trimmed.to_string()
        }
    } else if selected_ids.len() == 1 {
        let id = selected_ids[0];
        branches
            .iter()
            .find(|branch| branch.id == id)
            .map(branch_display)
            .unwrap_or_else(|| format!("{} ({})", "<branch>", id_prefix(id)))
    } else {
        format!("{} branches", selected_ids.len())
    };

    let mut selected: HashSet<Id> = selected_ids.iter().copied().collect();
    let mut changed = false;
    egui::ComboBox::from_id_salt(id_salt)
        .selected_text(selected_text)
        .show_ui(ui, |ui| {
            if ui.button("Clear").clicked() {
                selected.clear();
                changed = true;
            }
            ui.separator();
            for branch in branches {
                let display = branch_display(branch);
                let mut is_selected = selected.contains(&branch.id);
                if ui.checkbox(&mut is_selected, display).changed() {
                    changed = true;
                    if is_selected {
                        selected.insert(branch.id);
                    } else {
                        selected.remove(&branch.id);
                    }
                }
            }
        });

    if changed {
        let mut parts = Vec::new();
        for branch in branches {
            if selected.contains(&branch.id) {
                parts.push(branch_ref(branch, &name_counts));
            }
        }
        *raw = parts.join(",");
    }
}

fn branch_display(branch: &BranchEntry) -> String {
    let name = branch.name.as_deref().unwrap_or("<unnamed>");
    format!("{name} ({})", id_prefix(branch.id))
}

fn branch_ref(branch: &BranchEntry, name_counts: &HashMap<String, usize>) -> String {
    if let Some(name) = branch.name.as_ref() {
        if !name.starts_with('<') && name_counts.get(name).copied().unwrap_or(0) == 1 {
            return name.clone();
        }
    }
    format!("{:x}", branch.id)
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
            ui.set_min_height(TEAMS_SCROLL_HEIGHT);
            ui.label("Chats");
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

        ui.add_space(12.0);

        ui.vertical(|ui| {
            ui.set_min_height(TEAMS_SCROLL_HEIGHT);
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
                render_blob_aware_text(ui, row.content.as_str(), None, None);
                ui.add_space(8.0);
            }
        });
    });
}

fn render_blob_aware_text(
    ui: &mut egui::Ui,
    text: &str,
    text_color: Option<egui::Color32>,
    max_width: Option<f32>,
) {
    if text.is_empty() {
        return;
    }
    if let Some(max_width) = max_width {
        ui.set_max_width(max_width);
    }
    ui.horizontal_wrapped(|ui| {
        for chunk in split_blob_refs(text) {
            match chunk {
                PromptChunk::Text(text) => {
                    if text.is_empty() {
                        continue;
                    }
                    let mut rich = egui::RichText::new(text);
                    if let Some(color) = text_color {
                        rich = rich.color(color);
                    }
                    ui.add(egui::Label::new(rich).wrap_mode(egui::TextWrapMode::Wrap));
                }
                PromptChunk::Blob(blob) => {
                    render_blob_chip(ui, &blob, text_color);
                }
            }
        }
    });
}

fn render_blob_chip(
    ui: &mut egui::Ui,
    blob: &crate::blob_refs::BlobRef,
    text_color: Option<egui::Color32>,
) {
    let mut label = format!("blob:{}", short_digest(blob.digest_hex.as_str()));
    if let Some(mime) = blob.mime.as_deref() {
        label.push(' ');
        label.push_str(mime);
    }
    let fill = colorhash::ral_categorical(blob.digest_hex.as_bytes());
    let chip_text = text_color.unwrap_or_else(|| colorhash::text_color_on(fill));
    egui::Frame::NONE
        .fill(fill)
        .corner_radius(egui::CornerRadius::same(4))
        .inner_margin(egui::Margin::symmetric(6, 2))
        .show(ui, |ui| {
            ui.label(egui::RichText::new(label).small().color(chip_text));
        });
}

fn short_digest(hex: &str) -> String {
    if hex.len() <= 12 {
        return hex.to_owned();
    }
    format!("{}…{}", &hex[..6], &hex[hex.len() - 4..])
}

fn send_local_message_from_ui(
    state: &mut DashboardState,
    branches: &[BranchEntry],
    snapshot: &DashboardSnapshot,
) {
    state.local_send_error = None;
    state.local_send_notice = None;

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

    let Some(from_id) = snapshot.local_me_id else {
        state.local_send_error = Some(format!(
            "Unknown me '{}' (check Relations branch).",
            state.config.local_me
        ));
        return;
    };
    let Some(to_id) = snapshot.local_peer_id else {
        state.local_send_error = Some(format!(
            "Unknown peer '{}' (check Relations branch).",
            state.config.local_peer
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

fn render_agent_config(
    ui: &mut egui::Ui,
    state: &mut DashboardState,
    now_key: i128,
    config: Option<&AgentConfigRow>,
) {
    let Some(config) = config else {
        ui.label("No config entries.");
        return;
    };

    let updated = format_age(now_key, config.updated_at);
    ui.label(format!(
        "Latest config: {} (updated {updated})",
        id_prefix(config.id)
    ));
    ui.add_space(8.0);

    egui::Grid::new("agent_config_grid")
        .striped(true)
        .spacing(egui::Vec2::new(12.0, 6.0))
        .show(ui, |ui| {
            ui.label("config_id");
            ui.monospace(format!("{:x}", config.id));
            ui.end_row();

            ui.label("persona_id");
            ui.monospace(
                config
                    .persona_id
                    .map(|id| format!("{id:x}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("branch");
            ui.label(config.branch.as_deref().unwrap_or("-"));
            ui.end_row();

            ui.label("branch_id");
            ui.monospace(
                config
                    .branch_id
                    .map(|id| format!("{id:x}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("exec_branch_id");
            ui.monospace(
                config
                    .exec_branch_id
                    .map(|id| format!("{id:x}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("compass_branch_id");
            ui.monospace(
                config
                    .compass_branch_id
                    .map(|id| format!("{id:x}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("local_messages_branch_id");
            ui.monospace(
                config
                    .local_messages_branch_id
                    .map(|id| format!("{id:x}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("relations_branch_id");
            ui.monospace(
                config
                    .relations_branch_id
                    .map(|id| format!("{id:x}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("teams_branch_id");
            ui.monospace(
                config
                    .teams_branch_id
                    .map(|id| format!("{id:x}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("workspace_branch_id");
            ui.monospace(
                config
                    .workspace_branch_id
                    .map(|id| format!("{id:x}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("author");
            ui.label(config.author.as_deref().unwrap_or("-"));
            ui.end_row();

            ui.label("author_role");
            ui.label(config.author_role.as_deref().unwrap_or("-"));
            ui.end_row();

            ui.label("poll_ms");
            ui.monospace(
                config
                    .poll_ms
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("llm.model");
            ui.label(config.llm_model.as_deref().unwrap_or("-"));
            ui.end_row();

            ui.label("llm.base_url");
            ui.label(config.llm_base_url.as_deref().unwrap_or("-"));
            ui.end_row();

            ui.label("llm.reasoning_effort");
            ui.label(config.llm_reasoning_effort.as_deref().unwrap_or("-"));
            ui.end_row();

            ui.label("llm.stream");
            ui.monospace(
                config
                    .llm_stream
                    .map(|value| value.to_string())
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();

            ui.label("llm.api_key");
            ui.horizontal(|ui| {
                let Some(key) = config.llm_api_key.as_deref() else {
                    ui.label("-");
                    return;
                };
                if state.config_reveal_secrets {
                    ui.monospace(key);
                } else {
                    ui.monospace(mask_secret(key));
                }
                let button = if state.config_reveal_secrets {
                    "Hide"
                } else {
                    "Reveal"
                };
                if ui.add(Button::new(button)).clicked() {
                    state.config_reveal_secrets = !state.config_reveal_secrets;
                    ui.ctx().request_repaint();
                }
            });
            ui.end_row();

            ui.label("integrations.tavily_api_key");
            ui.horizontal(|ui| {
                let Some(key) = config.tavily_api_key.as_deref() else {
                    ui.label("-");
                    return;
                };
                if state.config_reveal_secrets {
                    ui.monospace(key);
                } else {
                    ui.monospace(mask_secret(key));
                }
                let button = if state.config_reveal_secrets {
                    "Hide"
                } else {
                    "Reveal"
                };
                if ui.add(Button::new(button)).clicked() {
                    state.config_reveal_secrets = !state.config_reveal_secrets;
                    ui.ctx().request_repaint();
                }
            });
            ui.end_row();

            ui.label("integrations.exa_api_key");
            ui.horizontal(|ui| {
                let Some(key) = config.exa_api_key.as_deref() else {
                    ui.label("-");
                    return;
                };
                if state.config_reveal_secrets {
                    ui.monospace(key);
                } else {
                    ui.monospace(mask_secret(key));
                }
                let button = if state.config_reveal_secrets {
                    "Hide"
                } else {
                    "Reveal"
                };
                if ui.add(Button::new(button)).clicked() {
                    state.config_reveal_secrets = !state.config_reveal_secrets;
                    ui.ctx().request_repaint();
                }
            });
            ui.end_row();

            ui.label("exec.default_cwd");
            ui.label(config.exec_default_cwd.as_deref().unwrap_or("-"));
            ui.end_row();

            ui.label("exec.sandbox_profile");
            ui.monospace(
                config
                    .exec_sandbox_profile
                    .map(|id| format!("{id:x}"))
                    .unwrap_or_else(|| "-".to_string()),
            );
            ui.end_row();
        });

    if let Some(prompt) = config.system_prompt.as_deref() {
        ui.add_space(8.0);
        ui.label(egui::RichText::new("System prompt").monospace());
        egui::Frame::NONE
            .fill(egui::Color32::from_gray(55))
            .corner_radius(egui::CornerRadius::same(6))
            .inner_margin(egui::Margin::symmetric(10, 8))
            .show(ui, |ui| {
                ui.add(
                    egui::Label::new(egui::RichText::new(prompt).monospace())
                        .wrap_mode(egui::TextWrapMode::Wrap),
                );
            });
    }
}

fn mask_secret(secret: &str) -> String {
    let len = secret.chars().count();
    if len == 0 {
        return "<empty>".to_string();
    }
    if len <= 8 {
        return "*".repeat(len);
    }
    let prefix: String = secret.chars().take(4).collect();
    let suffix: String = secret
        .chars()
        .rev()
        .take(4)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("{prefix}…{suffix}")
}

fn render_activity_timeline(ui: &mut egui::Ui, now_key: i128, rows: &[TimelineRow]) {
    let render_rows = |ui: &mut egui::Ui| {
        for row in rows {
            render_timeline_row(ui, now_key, row);
            ui.add_space(8.0);
        }
    };

    let max_height = if diagnostics_is_headless() {
        1800.0
    } else {
        ACTIVITY_TIMELINE_HEIGHT
    };
    let min_scrolled_height = if diagnostics_is_headless() {
        320.0
    } else {
        ACTIVITY_TIMELINE_HEIGHT
    };
    egui::ScrollArea::vertical()
        .id_salt("activity_timeline_scroll")
        .auto_shrink([false, false])
        .min_scrolled_height(min_scrolled_height)
        .max_height(max_height)
        .show(ui, |ui| render_rows(ui));
}

fn render_timeline_row(ui: &mut egui::Ui, now_key: i128, row: &TimelineRow) {
    let (source_label, source_color) = timeline_source_style(row.source);
    ui.horizontal_wrapped(|ui| {
        ui.small(format_age(now_key, row.at));
        render_timeline_source_chip(ui, source_label, source_color);
    });

    match &row.event {
        TimelineEvent::Shell {
            status,
            command,
            worker_label,
            exit_code,
            error,
        } => {
            ui.horizontal_wrapped(|ui| {
                ui.label(format!("{}: {}", exec_status_text(*status), command));
            });
            let mut details = Vec::new();
            if let Some(worker_label) = worker_label.as_deref() {
                details.push(format!("worker {worker_label}"));
            }
            if let Some(code) = exit_code {
                details.push(format!("exit {code}"));
            }
            if !details.is_empty() {
                ui.small(details.join(" · "));
            }
            if let Some(error) = error.as_deref() {
                ui.colored_label(egui::Color32::LIGHT_RED, error);
            }
        }
        TimelineEvent::Cognition { summary } => {
            ui.add(
                egui::Label::new(egui::RichText::new(summary).monospace())
                    .wrap_mode(egui::TextWrapMode::Wrap),
            );
        }
        TimelineEvent::Teams {
            author,
            chat_label,
            content,
        } => {
            ui.small(format!("{author} in {chat_label}"));
            render_blob_aware_text(ui, content, None, None);
        }
        TimelineEvent::LocalMessage {
            from_id,
            to_id,
            from_label,
            to_label,
            status,
            body,
            is_sender,
        } => {
            render_timeline_local_message(
                ui, *from_id, *to_id, from_label, to_label, status, body, *is_sender, now_key,
                row.at,
            );
        }
        TimelineEvent::GoalCreated { goal } => {
            render_timeline_goal_event(ui, goal, None);
        }
        TimelineEvent::GoalStatus { goal, to_status } => {
            render_timeline_goal_event(ui, goal, Some(format!("status -> {to_status}")));
        }
        TimelineEvent::GoalNote { goal, note } => {
            render_timeline_goal_event(ui, goal, Some(format!("note: {note}")));
        }
    }
}

fn timeline_source_style(source: TimelineSource) -> (&'static str, egui::Color32) {
    match source {
        TimelineSource::Shell => ("shell", egui::Color32::from_rgb(92, 132, 201)),
        TimelineSource::Cognition => ("mind", egui::Color32::from_rgb(123, 107, 168)),
        TimelineSource::Teams => ("teams", egui::Color32::from_rgb(69, 124, 184)),
        TimelineSource::LocalMessages => ("local", egui::Color32::from_rgb(67, 149, 112)),
        TimelineSource::Goals => ("goals", egui::Color32::from_rgb(202, 168, 68)),
    }
}

fn render_timeline_source_chip(ui: &mut egui::Ui, label: &str, fill: egui::Color32) {
    let text_color = colorhash::text_color_on(fill);
    egui::Frame::NONE
        .fill(fill)
        .corner_radius(egui::CornerRadius::same(5))
        .inner_margin(egui::Margin::symmetric(8, 2))
        .show(ui, |ui| {
            ui.label(egui::RichText::new(label).small().color(text_color));
        });
}

#[allow(clippy::too_many_arguments)]
fn render_timeline_local_message(
    ui: &mut egui::Ui,
    from_id: Id,
    to_id: Id,
    from_label: &str,
    to_label: &str,
    status: &LocalMessageStatus,
    body: &str,
    is_sender: bool,
    now_key: i128,
    at: Option<i128>,
) {
    let bubble_width = (ui.available_width() * 0.75).max(220.0);
    let age = format_age(now_key, at);
    let meta = format!("{age} · {}", local_message_status_text(status));
    let align = if is_sender {
        egui::Layout::right_to_left(egui::Align::TOP)
    } else {
        egui::Layout::left_to_right(egui::Align::TOP)
    };
    let from_chip_color = colorhash::ral_categorical(from_id.as_ref());
    let to_chip_color = colorhash::ral_categorical(to_id.as_ref());
    let bubble_color = from_chip_color;
    let text_color = colorhash::text_color_on(bubble_color);

    ui.with_layout(align, |ui| {
        ui.vertical(|ui| {
            ui.horizontal(|ui| {
                render_person_chip(ui, from_label, from_chip_color);
                ui.small("→");
                render_person_chip(ui, to_label, to_chip_color);
                ui.add_space(6.0);
                render_local_status_chip(ui, status);
            });
            egui::Frame::NONE
                .fill(bubble_color)
                .corner_radius(egui::CornerRadius::same(6))
                .inner_margin(egui::Margin::symmetric(10, 6))
                .show(ui, |ui| {
                    ui.set_max_width(bubble_width);
                    render_blob_aware_text(ui, body, Some(text_color), Some(bubble_width));
                });
            ui.small(meta);
        });
    });
}

fn render_timeline_goal_event(ui: &mut egui::Ui, goal: &CompassTaskRow, detail: Option<String>) {
    render_goal_card(ui, goal, 0.0);
    if let Some(detail) = detail {
        ui.add(
            egui::Label::new(egui::RichText::new(detail).monospace())
                .wrap_mode(egui::TextWrapMode::Wrap),
        );
    }
}

fn render_compass_swimlanes(
    ui: &mut egui::Ui,
    expanded_goal: &mut Option<Id>,
    rows: &[(CompassTaskRow, usize)],
    notes: &HashMap<Id, Vec<CompassNoteRow>>,
) {
    if rows.is_empty() {
        ui.label("No goals yet.");
        return;
    }

    let render_lanes = |ui: &mut egui::Ui| {
        ui.spacing_mut().item_spacing.y = 0.0;

        let mut counts: HashMap<&str, usize> = HashMap::new();
        let mut extra_statuses: HashSet<&str> = HashSet::new();
        for (row, _) in rows {
            *counts.entry(row.status.as_str()).or_insert(0) += 1;
            if !COMPASS_DEFAULT_STATUSES.contains(&row.status.as_str()) {
                extra_statuses.insert(row.status.as_str());
            }
        }

        // Always show the canonical lanes (including done) so the UI keeps its shape.
        let mut statuses: Vec<String> = COMPASS_DEFAULT_STATUSES
            .iter()
            .map(|s| (*s).to_string())
            .collect();
        let mut extras: Vec<&str> = extra_statuses.into_iter().collect();
        extras.sort();
        statuses.extend(extras.into_iter().map(|s| s.to_string()));

        for status in statuses {
            let count = counts.get(status.as_str()).copied().unwrap_or(0);
            render_compass_swimlane(ui, expanded_goal, notes, rows, &status, count);
        }
    };

    // Headless captures render each card into a GPU texture; clamp height to avoid
    // exceeding backend texture limits when there are many goals.
    if diagnostics_is_headless() {
        egui::ScrollArea::vertical()
            .id_salt("compass_headless_scroll")
            .max_height(1600.0)
            .show(ui, |ui| ui.scope(render_lanes));
    } else {
        ui.scope(render_lanes);
    }
}

fn render_compass_swimlane(
    ui: &mut egui::Ui,
    expanded_goal: &mut Option<Id>,
    notes: &HashMap<Id, Vec<CompassNoteRow>>,
    rows: &[(CompassTaskRow, usize)],
    status: &str,
    count: usize,
) {
    egui::Frame::NONE
        .inner_margin(egui::Margin {
            left: 12,
            right: 12,
            top: 10,
            bottom: 10,
        })
        .show(ui, |ui| {
            // Fill the full card width; otherwise Frames can shrink to content.
            ui.set_min_width(ui.available_width());
            ui.label(
                egui::RichText::new(format!("{} ({count})", status.to_uppercase()))
                    .monospace()
                    .strong()
                    .color(status_color(status)),
            );
            ui.add_space(6.0);

            if count == 0 {
                ui.small("(empty)");
                return;
            }

            for (row, depth) in rows {
                if row.status != status {
                    continue;
                }
                render_compass_swimlane_row(ui, expanded_goal, notes, row, *depth);
                ui.add_space(6.0);
            }
        });
}

fn status_color(status: &str) -> egui::Color32 {
    match status {
        // Status colors: ready (green), caution (yellow), danger (red), ice (blue).
        "todo" => egui::Color32::from_rgb(70, 150, 95),
        "doing" => egui::Color32::from_rgb(200, 170, 60),
        "blocked" => egui::Color32::from_rgb(170, 70, 70),
        "done" => egui::Color32::from_rgb(65, 110, 170),
        _ => egui::Color32::from_rgb(95, 95, 95),
    }
}

fn draw_goal_status_bar(ui: &egui::Ui, rect: egui::Rect, color: egui::Color32) {
    // Draw inside the border so the thin outline stays crisp.
    let inset = 1.0;
    let bar_height = 4.0;
    let min = egui::pos2(rect.left() + inset, rect.top() + inset);
    let max = egui::pos2(rect.right() - inset, rect.top() + inset + bar_height);
    ui.painter()
        .rect_filled(egui::Rect::from_min_max(min, max), 0.0, color);
}

fn goal_right_text(row: &CompassTaskRow) -> String {
    let mut right_parts: Vec<String> = Vec::new();
    if !row.tags.is_empty() {
        right_parts.push(
            row.tags
                .iter()
                .map(|tag| format!("#{tag}"))
                .collect::<Vec<_>>()
                .join(" "),
        );
    }
    if row.note_count > 0 {
        right_parts.push(format!("{}n", row.note_count));
    }
    if let Some(parent) = row.parent {
        right_parts.push(format!("^{}", id_prefix(parent)));
    }
    right_parts.push(format!("[{}]", row.id_prefix));
    right_parts.join(" · ")
}

fn render_goal_card(ui: &mut egui::Ui, row: &CompassTaskRow, dep_indent: f32) -> egui::Response {
    let right_text = goal_right_text(row);
    let outline = ui.visuals().widgets.noninteractive.bg_stroke;
    let bar_color = status_color(&row.status);
    let inner = ui
        .horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 0.0;
            if dep_indent > 0.0 {
                ui.add_space(dep_indent);
            }

            egui::Frame::NONE
                .fill(egui::Color32::TRANSPARENT)
                .stroke(outline)
                .corner_radius(egui::CornerRadius::same(0))
                .inner_margin(egui::Margin {
                    left: 10,
                    right: 10,
                    top: 6,
                    bottom: 6,
                })
                .show(ui, |ui| {
                    // Fill the full remaining width so cards don't shrink to just their text.
                    let available_width = ui.available_width();
                    ui.set_min_width(available_width);

                    let title = row.title.clone();
                    ui.horizontal(|ui| {
                        let right_width = if right_text.is_empty() {
                            0.0
                        } else {
                            let font_id = egui::TextStyle::Monospace.resolve(ui.style());
                            ui.fonts_mut(|fonts| {
                                fonts
                                    .layout_no_wrap(
                                        right_text.clone(),
                                        font_id,
                                        egui::Color32::WHITE,
                                    )
                                    .size()
                                    .x
                            })
                        };
                        let gap = 12.0;
                        let title_width = (ui.available_width() - right_width - gap).max(40.0);
                        ui.add_sized(
                            [title_width, 0.0],
                            egui::Label::new(egui::RichText::new(title).monospace())
                                .halign(egui::Align::LEFT)
                                .wrap_mode(egui::TextWrapMode::Truncate),
                        );
                        ui.allocate_ui_with_layout(
                            egui::vec2(ui.available_width(), 0.0),
                            egui::Layout::right_to_left(egui::Align::Center),
                            |ui| {
                                if !right_text.is_empty() {
                                    ui.add(
                                        egui::Label::new(
                                            egui::RichText::new(&right_text).monospace(),
                                        )
                                        .halign(egui::Align::RIGHT)
                                        .wrap_mode(egui::TextWrapMode::Truncate),
                                    );
                                }
                            },
                        );
                    });
                })
        })
        .inner;

    draw_goal_status_bar(ui, inner.response.rect, bar_color);
    inner.response
}

fn render_compass_swimlane_row(
    ui: &mut egui::Ui,
    expanded_goal: &mut Option<Id>,
    notes: &HashMap<Id, Vec<CompassNoteRow>>,
    row: &CompassTaskRow,
    depth: usize,
) {
    // Show hierarchy via left-side lines (outside the goal box). Deeper goals
    // shift right a bit so the box "shrinks" from the left.
    const DEP_LINE_STEP: f32 = 6.0;
    const DEP_LINE_BASE: f32 = 8.0;
    let dep_lines = depth.min(3);
    let dep_indent = if dep_lines == 0 {
        0.0
    } else {
        (dep_lines as f32 * DEP_LINE_STEP) + DEP_LINE_BASE
    };

    let is_expanded = *expanded_goal == Some(row.id);
    let outline = ui.visuals().widgets.noninteractive.bg_stroke;
    let response_rect = render_goal_card(ui, row, dep_indent).rect;

    // Make the whole row clickable to toggle note display.
    let click_id = ui.make_persistent_id(("compass_goal", row.id));
    let response = ui.interact(response_rect, click_id, egui::Sense::click());
    if response.clicked() {
        if *expanded_goal == Some(row.id) {
            *expanded_goal = None;
        } else {
            *expanded_goal = Some(row.id);
        }
    }

    if depth == 0 {
        // still allow expansion
    }

    if is_expanded {
        let task_notes = notes.get(&row.id).map(Vec::as_slice).unwrap_or(&[]);
        ui.horizontal(|ui| {
            ui.spacing_mut().item_spacing.x = 0.0;
            if dep_indent > 0.0 {
                ui.add_space(dep_indent);
            }
            egui::Frame::NONE
                .fill(egui::Color32::TRANSPARENT)
                .stroke(outline)
                .corner_radius(egui::CornerRadius::same(0))
                .inner_margin(egui::Margin::symmetric(10, 6))
                .show(ui, |ui| {
                    let available_width = ui.available_width();
                    ui.set_min_width(available_width);
                    ui.set_max_width(available_width);

                    // Frame::show inherits the current layout; force a vertical stack for notes.
                    ui.with_layout(egui::Layout::top_down(egui::Align::LEFT), |ui| {
                        if task_notes.is_empty() {
                            ui.small("(no notes)");
                            return;
                        }
                        for note in task_notes {
                            ui.small(&note.at);
                            ui.add(
                                egui::Label::new(egui::RichText::new(&note.body))
                                    .wrap_mode(egui::TextWrapMode::Wrap),
                            );
                            ui.add_space(6.0);
                        }
                    });
                });
        });
        ui.add_space(6.0);
    }

    // Draw a small "dependency gutter" to the left of the goal box.
    let rect = response_rect;
    let painter = ui.painter();
    let stroke = egui::Stroke::new(1.2, egui::Color32::from_gray(130));
    for idx in 0..dep_lines {
        let x = rect.left() - dep_indent + 4.0 + (idx as f32 * DEP_LINE_STEP);
        let y1 = rect.top() + 0.5;
        let y2 = rect.bottom() - 0.5;
        painter.line_segment([egui::pos2(x, y1), egui::pos2(x, y2)], stroke);
    }
}

fn local_message_status(
    row: &LocalMessageRow,
    me_id: Option<Id>,
    labels: &HashMap<Id, String>,
) -> LocalMessageStatus {
    let read_by = |reader_id: Id| row.readers.iter().any(|id| *id == reader_id);
    match me_id {
        Some(me) if row.to_id == me => {
            if read_by(me) {
                LocalMessageStatus::Read
            } else {
                LocalMessageStatus::Unread
            }
        }
        Some(me) if row.from_id == me => {
            if read_by(row.to_id) {
                let to_label = format_id(labels, row.to_id);
                LocalMessageStatus::ReadBy(to_label)
            } else {
                LocalMessageStatus::Sent
            }
        }
        Some(me) if read_by(me) => LocalMessageStatus::Read,
        Some(_) => LocalMessageStatus::Other,
        None => {
            if read_by(row.to_id) {
                let to_label = format_id(labels, row.to_id);
                LocalMessageStatus::ReadBy(to_label)
            } else {
                LocalMessageStatus::Sent
            }
        }
    }
}

fn local_message_status_text(status: &LocalMessageStatus) -> String {
    match status {
        LocalMessageStatus::Unread => "unread".to_string(),
        LocalMessageStatus::Read => "read".to_string(),
        LocalMessageStatus::Sent => "sent".to_string(),
        LocalMessageStatus::ReadBy(label) => format!("read-by:{label}"),
        LocalMessageStatus::Other => "other".to_string(),
    }
}

fn local_message_status_color(status: &LocalMessageStatus) -> egui::Color32 {
    match status {
        LocalMessageStatus::Unread => egui::Color32::from_rgb(202, 118, 45),
        LocalMessageStatus::Read => egui::Color32::from_rgb(69, 141, 92),
        LocalMessageStatus::Sent => egui::Color32::from_rgb(107, 118, 130),
        LocalMessageStatus::ReadBy(_) => egui::Color32::from_rgb(74, 126, 183),
        LocalMessageStatus::Other => egui::Color32::from_rgb(122, 104, 164),
    }
}

fn render_local_status_chip(ui: &mut egui::Ui, status: &LocalMessageStatus) {
    let fill = local_message_status_color(status);
    let text_color = colorhash::text_color_on(fill);
    let label = truncate_single_line(&local_message_status_text(status), 40);
    egui::Frame::NONE
        .fill(fill)
        .corner_radius(egui::CornerRadius::same(5))
        .inner_margin(egui::Margin::symmetric(8, 2))
        .show(ui, |ui| {
            ui.label(egui::RichText::new(label).color(text_color).small());
        });
}

fn render_person_chip(ui: &mut egui::Ui, label: &str, fill: egui::Color32) {
    let text_color = colorhash::text_color_on(fill);
    let label = truncate_single_line(label, 48);
    egui::Frame::NONE
        .fill(fill)
        .corner_radius(egui::CornerRadius::same(5))
        .inner_margin(egui::Margin::symmetric(8, 2))
        .show(ui, |ui| {
            ui.label(egui::RichText::new(label).color(text_color).small());
        });
}

fn render_relations(ui: &mut egui::Ui, people: &[RelationRow]) {
    if people.is_empty() {
        ui.label("No relations.");
        return;
    }
    ui.set_min_height(RELATIONS_SCROLL_HEIGHT);
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
}

fn render_workspace(
    ui: &mut egui::Ui,
    state: &mut DashboardState,
    now_key: i128,
    snapshots: &[WorkspaceSnapshotRow],
    entries: &[WorkspaceEntryRow],
) {
    if snapshots.is_empty() {
        ui.label("No workspace snapshots.");
        return;
    }

    ui.label("Snapshots:");
    if ui
        .selectable_label(
            state.workspace_selected_snapshot.is_none(),
            "Follow latest snapshot",
        )
        .clicked()
    {
        state.workspace_selected_snapshot = None;
        ui.ctx().request_repaint();
    }

    for row in snapshots {
        let age = format_age(now_key, row.created_at);
        let label = row.label.as_deref().unwrap_or("-");
        let root = row.root_path.as_deref().unwrap_or(".");
        let state_hash = row
            .state_handle
            .map(archive_handle_prefix)
            .unwrap_or_else(|| "-".to_string());
        let text = format!(
            "{age}  {}  {label}  {root}  state:{state_hash}  ({})",
            id_prefix(row.id),
            row.entry_count
        );
        if ui
            .selectable_label(state.workspace_selected_snapshot == Some(row.id), text)
            .clicked()
        {
            state.workspace_selected_snapshot = Some(row.id);
            ui.ctx().request_repaint();
        }
    }

    let latest_id = snapshots.first().map(|row| row.id);
    let effective_id = state.workspace_selected_snapshot.or(latest_id);
    ui.add_space(8.0);

    match effective_id {
        None => {
            ui.label("No snapshot selected.");
        }
        Some(snapshot_id) => {
            let selected_row = snapshots.iter().find(|row| row.id == snapshot_id);
            if let Some(row) = selected_row {
                let age = format_age(now_key, row.created_at);
                let label = row.label.as_deref().unwrap_or("-");
                let root = row.root_path.as_deref().unwrap_or(".");
                let state_hash = row
                    .state_handle
                    .map(archive_handle_prefix)
                    .unwrap_or_else(|| "-".to_string());
                ui.label(format!(
                    "Entries for {age} {}  {label}  {root}  state:{state_hash}:",
                    id_prefix(row.id),
                ));
            } else {
                ui.label(format!("Entries for {}:", id_prefix(snapshot_id)));
                ui.small("Selected snapshot is not in the list (older than limit).");
            }

            let tree = build_workspace_tree(entries);
            render_workspace_tree(ui, "", &tree, true);
        }
    }
}

fn workspace_kind_tag(kind: WorkspaceEntryKind) -> &'static str {
    match kind {
        WorkspaceEntryKind::File => "F",
        WorkspaceEntryKind::Dir => "D",
        WorkspaceEntryKind::Symlink => "L",
        WorkspaceEntryKind::Unknown => "?",
    }
}

#[derive(Default)]
struct WorkspaceTreeNode {
    dirs: std::collections::BTreeMap<String, WorkspaceTreeNode>,
    files: Vec<WorkspaceTreeLeaf>,
}

#[derive(Clone)]
struct WorkspaceTreeLeaf {
    name: String,
    kind: WorkspaceEntryKind,
    link_target: Option<String>,
}

impl WorkspaceTreeNode {
    fn sort_recursive(&mut self) {
        self.files.sort_by(|a, b| a.name.cmp(&b.name));
        for child in self.dirs.values_mut() {
            child.sort_recursive();
        }
    }
}

fn build_workspace_tree(entries: &[WorkspaceEntryRow]) -> WorkspaceTreeNode {
    let mut root = WorkspaceTreeNode::default();

    for entry in entries {
        let parts: Vec<&str> = entry
            .path
            .split('/')
            .filter(|part| !part.is_empty())
            .collect();
        if parts.is_empty() {
            continue;
        }

        let mut node = &mut root;
        for dir in &parts[..parts.len().saturating_sub(1)] {
            node = node.dirs.entry((*dir).to_string()).or_default();
        }

        let name = parts[parts.len() - 1].to_string();
        match entry.kind {
            WorkspaceEntryKind::Dir => {
                node.dirs.entry(name).or_default();
            }
            kind => {
                node.files.push(WorkspaceTreeLeaf {
                    name,
                    kind,
                    link_target: entry.link_target.clone(),
                });
            }
        }
    }

    root.sort_recursive();
    root
}

fn render_workspace_tree(
    ui: &mut egui::Ui,
    base_path: &str,
    node: &WorkspaceTreeNode,
    open_one_root: bool,
) {
    let default_open_root = open_one_root && base_path.is_empty() && node.dirs.len() == 1;
    for (name, child) in &node.dirs {
        render_workspace_dir(ui, base_path, name, child, default_open_root);
    }

    for file in &node.files {
        let kind = workspace_kind_tag(file.kind);
        if let Some(target) = file.link_target.as_deref() {
            ui.monospace(format!(
                "{kind} {} -> {}",
                file.name,
                truncate_single_line(target, 80)
            ));
        } else {
            ui.monospace(format!("{kind} {}", file.name));
        }
    }
}

fn render_workspace_dir(
    ui: &mut egui::Ui,
    base_path: &str,
    name: &str,
    child: &WorkspaceTreeNode,
    default_open: bool,
) {
    let mut display = name.to_string();
    let mut full_path = if base_path.is_empty() {
        name.to_string()
    } else {
        format!("{base_path}/{name}")
    };

    let mut node = child;
    while node.files.is_empty() && node.dirs.len() == 1 {
        let (next_name, next_node) = node.dirs.iter().next().expect("len == 1");
        display.push('/');
        display.push_str(next_name);
        full_path.push('/');
        full_path.push_str(next_name);
        node = next_node;
    }

    egui::CollapsingHeader::new(egui::RichText::new(format!("D {display}")).monospace())
        .id_salt(format!("workspace_dir::{full_path}"))
        .default_open(default_open)
        .show(ui, |ui| {
            render_workspace_tree(ui, &full_path, node, false);
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

fn exec_status_text(status: ExecStatus) -> &'static str {
    match status {
        ExecStatus::Pending => "pending",
        ExecStatus::Running => "running",
        ExecStatus::Done => "done",
        ExecStatus::Failed => "failed",
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
