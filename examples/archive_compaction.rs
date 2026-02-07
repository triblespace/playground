#!/usr/bin/env -S watchexec -r cargo run --manifest-path playground/Cargo.toml --example archive_compaction

use std::collections::HashSet;
use std::fmt::Write;
use std::fs;
use std::path::{Path, PathBuf};

use ed25519_dalek::SigningKey;
use eframe::egui;
use rand::rngs::OsRng;
use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::blob::schemas::longstring::LongString;
use triblespace::core::blob::schemas::simplearchive::SimpleArchive;
use triblespace::core::blob::{Blob, ToBlob};
use triblespace::core::metadata;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{BlobStoreList, BlobStoreMeta, BranchStore, Repository};
use triblespace::core::trible::TribleSet;
use triblespace::core::value::Value;
use triblespace::core::value::schemas::hash::{Blake3, Handle};
use triblespace::macros::{find, pattern};
use triblespace::prelude::{BlobStore, BlobStoreGet};

use GORBIE::NotebookCtx;
use GORBIE::cards::{DEFAULT_CARD_PADDING, with_padding};
use GORBIE::dataflow::ComputedState;
use GORBIE::notebook;
use GORBIE::widgets;

type CommitHandle = Value<Handle<Blake3, SimpleArchive>>;

fn repo_root() -> PathBuf {
    let manifest = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest.parent().map(PathBuf::from).unwrap_or(manifest)
}

fn fmt_bytes(bytes: u64) -> String {
    const KB: f64 = 1024.0;
    const MB: f64 = 1024.0 * KB;
    const GB: f64 = 1024.0 * MB;
    let b = bytes as f64;
    if b >= GB {
        format!("{:.2} GiB", b / GB)
    } else if b >= MB {
        format!("{:.2} MiB", b / MB)
    } else if b >= KB {
        format!("{:.2} KiB", b / KB)
    } else {
        format!("{bytes} B")
    }
}

fn fmt_handle_short(handle: CommitHandle) -> String {
    let mut out = String::with_capacity(16);
    for b in handle.raw.iter().take(8) {
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn file_size(path: &Path) -> Result<u64, String> {
    fs::metadata(path)
        .map(|m| m.len())
        .map_err(|e| format!("stat {}: {e}", path.display()))
}

fn trible_fingerprint(raw: &[u8; 64]) -> u128 {
    let mut hasher = Blake3::new();
    hasher.update(raw);
    let digest = hasher.finalize();
    let bytes = digest.as_bytes();
    let mut lower = [0u8; 16];
    let lower_len = lower.len();
    lower.copy_from_slice(&bytes[bytes.len() - lower_len..]);
    u128::from_le_bytes(lower)
}

fn find_branch_by_name(
    pile: &mut Pile<Blake3>,
    branch_name: &str,
) -> Result<Option<triblespace::core::id::Id>, String> {
    let reader = pile
        .reader()
        .map_err(|err| format!("open pile reader: {err:?}"))?;
    let iter = pile
        .branches()
        .map_err(|err| format!("list branches: {err:?}"))?;
    let expected = branch_name.to_string().to_blob().get_handle::<Blake3>();

    for item in iter {
        let branch_id = item.map_err(|err| format!("branch id: {err:?}"))?;
        let Some(head) = pile
            .head(branch_id)
            .map_err(|err| format!("branch head: {err:?}"))?
        else {
            continue;
        };
        let meta: TribleSet = reader
            .get(head)
            .map_err(|err| format!("branch metadata blob: {err:?}"))?;

        let mut names = find!(
            (handle: Value<Handle<Blake3, LongString>>),
            pattern!(&meta, [{ metadata::name: ?handle }])
        )
        .into_iter();
        let Some((handle,)) = names.next() else {
            continue;
        };
        if names.next().is_some() {
            continue;
        }
        if handle == expected {
            return Ok(Some(branch_id));
        }
    }

    Ok(None)
}

#[derive(Clone, Debug)]
struct OverheadEstimate {
    blobs_total: u64,
    blob_bytes_total: u64,
    overhead_bytes: u64,
    overhead_bytes_per_blob: f64,
}

#[derive(Clone, Debug)]
struct TribleDuplication {
    total_tribles: u64,
    unique_tribles: u64,
    duplicate_copies: u64,
    repeated_tribles: u64,
}

#[derive(Clone, Debug)]
struct CompactionSnapshot {
    pile_path: PathBuf,
    pile_bytes: u64,
    branch_name: String,
    branch_id: triblespace::core::id::Id,
    head: Option<CommitHandle>,

    commit_blobs: u64,
    commit_blob_bytes: u64,

    content_blobs: u64,
    content_blob_bytes: u64,

    commits_without_content: u64,
    head_commit_blob_bytes: u64,

    estimated_saved_commit_bytes: u64,
    estimated_saved_blob_records: u64,
    estimated_saved_overhead_bytes: Option<u64>,

    overhead: Option<OverheadEstimate>,
    content_tribles: Option<TribleDuplication>,
}

fn compute_compaction(
    pile_path: PathBuf,
    branch_name: String,
    scan_full_pile_overhead: bool,
    scan_content_trible_duplication: bool,
) -> Result<CompactionSnapshot, String> {
    let pile_bytes = file_size(&pile_path)?;

    let mut pile = Pile::<Blake3>::open(&pile_path).map_err(|e| format!("open pile: {e:?}"))?;
    pile.restore().map_err(|e| format!("restore pile: {e:?}"))?;

    let Some(branch_id) = find_branch_by_name(&mut pile, &branch_name)? else {
        let _ = pile.close();
        return Err(format!("unknown branch {branch_name:?}"));
    };

    let reader = pile
        .reader()
        .map_err(|err| format!("open pile reader: {err:?}"))?;

    let signing_key = SigningKey::generate(&mut OsRng);
    let mut repo = Repository::new(pile, signing_key);

    let mut ws = repo
        .pull(branch_id)
        .map_err(|err| format!("pull branch {branch_id:x}: {err:?}"))?;

    let head = ws.head();
    let head_commit_blob_bytes = head
        .and_then(|h| reader.metadata(h).ok().flatten().map(|m| m.length))
        .unwrap_or(0);

    let mut commit_seen: HashSet<[u8; 32]> = HashSet::new();
    let mut content_seen: HashSet<[u8; 32]> = HashSet::new();
    let mut content_handles: Vec<CommitHandle> = Vec::new();

    let mut commit_blobs: u64 = 0;
    let mut commit_blob_bytes: u64 = 0;
    let mut commits_without_content: u64 = 0;

    let mut content_blobs: u64 = 0;
    let mut content_blob_bytes: u64 = 0;

    let mut stack = Vec::new();
    if let Some(head) = head {
        stack.push(head);
    }

    while let Some(commit) = stack.pop() {
        if !commit_seen.insert(commit.raw) {
            continue;
        }

        commit_blobs += 1;
        if let Ok(Some(meta)) = reader.metadata(commit) {
            commit_blob_bytes = commit_blob_bytes.saturating_add(meta.length);
        }

        let meta: TribleSet = ws
            .get(commit)
            .map_err(|err| format!("load commit {}: {err:?}", fmt_handle_short(commit)))?;

        // Parents.
        for (p,) in find!(
            (p: Value<Handle<Blake3, SimpleArchive>>),
            pattern!(&meta, [{ triblespace::core::repo::parent: ?p }])
        ) {
            stack.push(p);
        }

        // Content blob, if present.
        let content = find!(
            (c: Value<Handle<Blake3, SimpleArchive>>),
            pattern!(&meta, [{ triblespace::core::repo::content: ?c }])
        )
        .into_iter()
        .next()
        .map(|(c,)| c);

        match content {
            None => commits_without_content += 1,
            Some(content) => {
                if content_seen.insert(content.raw) {
                    content_handles.push(content);
                    content_blobs += 1;
                    if let Ok(Some(meta)) = reader.metadata(content) {
                        content_blob_bytes = content_blob_bytes.saturating_add(meta.length);
                    }
                }
            }
        }
    }

    let estimated_saved_commit_bytes = commit_blob_bytes.saturating_sub(head_commit_blob_bytes);
    let estimated_saved_blob_records = commit_blobs
        .saturating_sub(1)
        .saturating_add(content_blobs.saturating_sub(1));

    let overhead = if scan_full_pile_overhead {
        let mut blobs_total: u64 = 0;
        let mut blob_bytes_total: u64 = 0;
        for handle in reader.blobs() {
            let handle: Value<Handle<Blake3, UnknownBlob>> =
                handle.map_err(|err| format!("list blob: {err:?}"))?;
            blobs_total += 1;
            if let Ok(Some(meta)) = reader.metadata(handle) {
                blob_bytes_total = blob_bytes_total.saturating_add(meta.length);
            }
        }
        let overhead_bytes = pile_bytes.saturating_sub(blob_bytes_total);
        let overhead_bytes_per_blob = if blobs_total == 0 {
            0.0
        } else {
            overhead_bytes as f64 / blobs_total as f64
        };
        Some(OverheadEstimate {
            blobs_total,
            blob_bytes_total,
            overhead_bytes,
            overhead_bytes_per_blob,
        })
    } else {
        None
    };

    let estimated_saved_overhead_bytes = overhead
        .as_ref()
        .map(|o| (o.overhead_bytes_per_blob * estimated_saved_blob_records as f64).round() as u64);

    let content_tribles = if scan_content_trible_duplication {
        // Note: we intentionally deduplicate by *blob handle* (storage), not by
        // "commit references". If multiple commits point at the same content blob
        // it is only stored once already.
        //
        // We compute:
        // - total_tribles: sum of all tribles stored across reachable content blobs
        // - unique_tribles: distinct tribles across those blobs
        // - duplicate_copies: extra stored copies (total - unique)
        // - repeated_tribles: distinct tribles that appear in >1 blob
        let mut seen_once: HashSet<u128> = HashSet::new();
        let mut seen_multi: HashSet<u128> = HashSet::new();

        let mut total_tribles: u64 = 0;
        let mut duplicate_copies: u64 = 0;
        let mut repeated_tribles: u64 = 0;

        for handle in &content_handles {
            let blob: Blob<SimpleArchive> = reader.get(*handle).map_err(|err| {
                format!("load content blob {}: {err:?}", fmt_handle_short(*handle))
            })?;
            let bytes = blob.bytes.as_ref();
            if bytes.len() % 64 != 0 {
                return Err(format!(
                    "content blob {} has invalid SimpleArchive length {} (not multiple of 64)",
                    fmt_handle_short(*handle),
                    bytes.len()
                ));
            }
            for chunk in bytes.chunks_exact(64) {
                let raw: &[u8; 64] = chunk
                    .try_into()
                    .expect("chunks_exact guarantees 64-byte slices");
                total_tribles += 1;
                let key = trible_fingerprint(raw);
                if !seen_once.insert(key) {
                    duplicate_copies += 1;
                    if seen_multi.insert(key) {
                        repeated_tribles += 1;
                    }
                }
            }
        }

        let unique_tribles = total_tribles.saturating_sub(duplicate_copies);
        Some(TribleDuplication {
            total_tribles,
            unique_tribles,
            duplicate_copies,
            repeated_tribles,
        })
    } else {
        None
    };

    drop(ws);
    drop(reader);
    repo.close()
        .map_err(|err| format!("close pile {}: {err:?}", pile_path.display()))?;

    Ok(CompactionSnapshot {
        pile_path,
        pile_bytes,
        branch_name,
        branch_id,
        head,
        commit_blobs,
        commit_blob_bytes,
        content_blobs,
        content_blob_bytes,
        commits_without_content,
        head_commit_blob_bytes,
        estimated_saved_commit_bytes,
        estimated_saved_blob_records,
        estimated_saved_overhead_bytes,
        overhead,
        content_tribles,
    })
}

#[derive(Debug)]
struct ViewerState {
    pile_path: String,
    branch_name: String,
    scan_full_pile_overhead: bool,
    scan_content_trible_duplication: bool,
    snapshot: ComputedState<Option<Result<CompactionSnapshot, String>>>,
}

impl Default for ViewerState {
    fn default() -> Self {
        let repo_root = repo_root();
        let default_pile = repo_root.join("self.pile");
        Self {
            pile_path: default_pile.to_string_lossy().to_string(),
            branch_name: "archive".to_string(),
            scan_full_pile_overhead: false,
            scan_content_trible_duplication: false,
            snapshot: ComputedState::default(),
        }
    }
}

#[notebook]
fn main(nb: &mut NotebookCtx) {
    let padding = DEFAULT_CARD_PADDING;

    nb.view(|ui| {
        widgets::markdown(
            ui,
            "# Archive compaction estimator\n\nEstimates how much commit history would shrink *in bytes and blob count* if we squashed a branch into a single commit.\n\nNote: TribleSpace commits are append-only deltas; squashing mostly reduces **commit metadata** and **per-blob overhead**, not the underlying data payload size.",
        );
    });

    nb.state("compaction", ViewerState::default(), move |ui, state| {
        with_padding(ui, padding, |ui| {
            state.snapshot.poll();

            ui.horizontal(|ui| {
                ui.label("Pile:");
                ui.add_sized(
                    [ui.available_width(), ui.spacing().interact_size.y],
                    widgets::TextField::singleline(&mut state.pile_path),
                );
            });
            ui.add_space(6.0);
            ui.horizontal(|ui| {
                ui.label("Branch:");
                ui.add_sized(
                    [ui.available_width(), ui.spacing().interact_size.y],
                    widgets::TextField::singleline(&mut state.branch_name),
                );
            });

            ui.add_space(6.0);
            ui.horizontal_wrapped(|ui| {
                ui.label("Scan full pile overhead (slow):");
                ui.add(widgets::ChoiceToggle::binary(
                    &mut state.scan_full_pile_overhead,
                    "OFF",
                    "ON",
                ));
            });
            ui.add_space(6.0);
            ui.horizontal_wrapped(|ui| {
                ui.label("Scan content trible duplication (slow):");
                ui.add(widgets::ChoiceToggle::binary(
                    &mut state.scan_content_trible_duplication,
                    "OFF",
                    "ON",
                ));
            });
            ui.add_space(8.0);
            ui.horizontal(|ui| {
                let run_clicked = ui.add(widgets::Button::new("Compute")).clicked();
                if run_clicked && !state.snapshot.is_running() {
                    let pile_path = PathBuf::from(state.pile_path.trim());
                    let branch_name = state.branch_name.trim().to_string();
                    let scan_full_pile_overhead = state.scan_full_pile_overhead;
                    let scan_content_trible_duplication = state.scan_content_trible_duplication;
                    state.snapshot.spawn(move || {
                        Some(compute_compaction(
                            pile_path,
                            branch_name,
                            scan_full_pile_overhead,
                            scan_content_trible_duplication,
                        ))
                    });
                }

                if state.snapshot.is_running() {
                    ui.add(egui::Spinner::new());
                    ui.label("Working…");
                }
            });

            ui.add_space(10.0);

            match state.snapshot.value().as_ref() {
                None => {
                    ui.label(egui::RichText::new("No snapshot yet.").italics().small());
                }
                Some(Err(err)) => {
                    ui.label(
                        egui::RichText::new(err)
                            .color(ui.visuals().error_fg_color)
                            .monospace(),
                    );
                }
                Some(Ok(snapshot)) => {
                    ui.heading("Pile");
                    ui.add_space(4.0);
                    ui.label(format!(
                        "{} ({})",
                        snapshot.pile_path.display(),
                        fmt_bytes(snapshot.pile_bytes)
                    ));

                    ui.add_space(8.0);
                    ui.heading("Branch");
                    ui.add_space(4.0);
                    ui.label(format!("name: {}", snapshot.branch_name));
                    ui.label(format!("id: {:x}", snapshot.branch_id));
                    match snapshot.head {
                        None => ui.label("head: <empty>"),
                        Some(h) => ui.label(format!("head: {}", fmt_handle_short(h))),
                    };

                    ui.add_space(8.0);
                    ui.heading("Current History Cost");
                    ui.add_space(4.0);
                    ui.label(format!(
                        "commit blobs: {} ({})",
                        snapshot.commit_blobs,
                        fmt_bytes(snapshot.commit_blob_bytes)
                    ));
                    ui.label(format!(
                        "content blobs: {} ({})",
                        snapshot.content_blobs,
                        fmt_bytes(snapshot.content_blob_bytes)
                    ));
                    if snapshot.content_blobs > 0 {
                        ui.label(format!(
                            "avg content blob: {}",
                            fmt_bytes(snapshot.content_blob_bytes / snapshot.content_blobs)
                        ));
                    }
                    if snapshot.commit_blobs > 0 {
                        ui.label(format!(
                            "commits without content (merge/no-op): {}",
                            snapshot.commits_without_content
                        ));
                    }

                    ui.add_space(8.0);
                    ui.heading("Squash Estimate");
                    ui.add_space(4.0);
                    ui.label(format!(
                        "saved commit metadata bytes: {}",
                        fmt_bytes(snapshot.estimated_saved_commit_bytes)
                    ));
                    ui.label(format!(
                        "saved blob records (commit+content): {}",
                        snapshot.estimated_saved_blob_records
                    ));
                    ui.label(format!(
                        "head commit metadata size (kept): {}",
                        fmt_bytes(snapshot.head_commit_blob_bytes)
                    ));
                    ui.label(
                        egui::RichText::new(
                            "Note: commits are append-only deltas; squashing mostly reduces commit object churn and checkout overhead. If overlapping history re-committed the same facts, squashing can also deduplicate those repeated tribles.",
                        )
                        .small()
                        .italics(),
                    );

                    if let Some(content_tribles) = snapshot.content_tribles.as_ref() {
                        ui.add_space(10.0);
                        ui.heading("Content Trible Duplication");
                        ui.add_space(4.0);
                        ui.label(format!(
                            "stored tribles across content blobs: {}",
                            content_tribles.total_tribles
                        ));
                        ui.label(format!(
                            "unique tribles (union): {}",
                            content_tribles.unique_tribles
                        ));
                        ui.label(format!(
                            "extra copies (dedup by squash): {}",
                            content_tribles.duplicate_copies
                        ));
                        ui.label(format!(
                            "tribles that appear in >1 blob: {}",
                            content_tribles.repeated_tribles
                        ));
                        if content_tribles.total_tribles > 0 {
                            let dup_pct = (content_tribles.duplicate_copies as f64)
                                * 100.0
                                / (content_tribles.total_tribles as f64);
                            ui.label(format!("duplication rate: {dup_pct:.2}%"));
                        }
                    }

                    if let Some(overhead) = snapshot.overhead.as_ref() {
                        ui.add_space(10.0);
                        ui.heading("Pile Overhead (Full Scan)");
                        ui.add_space(4.0);
                        ui.label(format!(
                            "blobs: {} ({})",
                            overhead.blobs_total,
                            fmt_bytes(overhead.blob_bytes_total)
                        ));
                        ui.label(format!(
                            "pile overhead: {} (~{:.1} B/blob)",
                            fmt_bytes(overhead.overhead_bytes),
                            overhead.overhead_bytes_per_blob
                        ));
                        if let Some(saved) = snapshot.estimated_saved_overhead_bytes {
                            ui.label(format!(
                                "estimated overhead saved by squash: {}",
                                fmt_bytes(saved)
                            ));
                            ui.label(format!(
                                "estimated total saved: {}",
                                fmt_bytes(saved.saturating_add(snapshot.estimated_saved_commit_bytes))
                            ));
                        }
                    }
                }
            }
        });
    });
}
