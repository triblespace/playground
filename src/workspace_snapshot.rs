use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::fs;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, anyhow};
use triblespace::core::blob::Blob;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::prelude::blobschemas::{FileBytes, LongString};
use triblespace::prelude::valueschemas::{Blake3, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

use crate::repo_util::load_text;
use crate::schema::playground_workspace;
use crate::time_util::interval_key;

pub const DEFAULT_WORKSPACE_BRANCH: &str = "workspace";

#[derive(Debug, Clone, Copy)]
pub struct MergeRestoreReport {
    pub snapshot_id: Id,
    pub lineage_len: usize,
    pub merged_entries: usize,
    pub created_entries: usize,
    pub unchanged_entries: usize,
    pub conflicting_entries: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    File,
    Dir,
    Symlink,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedEntry {
    path: String,
    kind: EntryKind,
    mode: Option<u32>,
    bytes: Option<Vec<u8>>,
    link_target: Option<String>,
}

pub fn restore_snapshot_merge(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    snapshot_id: Option<Id>,
    target_root: &Path,
) -> Result<Option<MergeRestoreReport>> {
    let mut ws = repo
        .pull(branch_id)
        .map_err(|err| anyhow!("pull workspace branch: {err:?}"))?;
    let catalog = ws.checkout(..).context("checkout workspace branch")?;

    let snapshot_id = match snapshot_id {
        Some(id) => Some(id),
        None => latest_snapshot(&catalog).context("find latest snapshot")?,
    };
    let Some(snapshot_id) = snapshot_id else {
        return Ok(None);
    };

    if !target_root.exists() {
        fs::create_dir_all(target_root)
            .with_context(|| format!("create workspace root {}", target_root.display()))?;
    }

    let lineage = snapshot_lineage(&catalog, snapshot_id)?;
    let theirs_entries = merge_lineage_entries(&mut ws, &catalog, target_root, &lineage)?;
    let ours_entries = collect_workspace_entries(target_root)?;
    let entries = merge_entry_maps(&ours_entries, &theirs_entries)?;
    let merged_entries = entries.len();
    let (created_entries, unchanged_entries, conflicting_entries) = apply_merged_entries(&entries)?;

    Ok(Some(MergeRestoreReport {
        snapshot_id,
        lineage_len: lineage.len(),
        merged_entries,
        created_entries,
        unchanged_entries,
        conflicting_entries,
    }))
}

fn resolve_restore_root(target_root: &Path, root_path: Option<&str>) -> Result<PathBuf> {
    let Some(root_path) = root_path.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(target_root.to_path_buf());
    };

    if root_path == "." {
        return Ok(target_root.to_path_buf());
    }

    let raw = Path::new(root_path);
    if raw.is_absolute() {
        return Ok(raw.to_path_buf());
    }

    if raw
        .components()
        .any(|c| std::matches!(c, Component::ParentDir))
    {
        return Err(anyhow!("invalid snapshot root_path: {}", root_path));
    }

    Ok(target_root.join(raw))
}

fn snapshot_lineage(catalog: &TribleSet, snapshot_id: Id) -> Result<Vec<Id>> {
    let mut ordered = Vec::new();
    let mut visiting = HashSet::new();
    let mut visited = HashSet::new();
    snapshot_lineage_visit(
        catalog,
        snapshot_id,
        &mut visiting,
        &mut visited,
        &mut ordered,
    )?;
    Ok(ordered)
}

fn snapshot_lineage_visit(
    catalog: &TribleSet,
    snapshot_id: Id,
    visiting: &mut HashSet<Id>,
    visited: &mut HashSet<Id>,
    ordered: &mut Vec<Id>,
) -> Result<()> {
    if visited.contains(&snapshot_id) {
        return Ok(());
    }
    if !visiting.insert(snapshot_id) {
        return Err(anyhow!(
            "workspace snapshot parent cycle at {snapshot_id:x}"
        ));
    }
    for (parent_id,) in find!(
        (parent_id: Id),
        pattern!(catalog, [{ snapshot_id @ playground_workspace::parent_snapshot: ?parent_id }])
    ) {
        snapshot_lineage_visit(catalog, parent_id, visiting, visited, ordered)?;
    }
    visiting.remove(&snapshot_id);
    visited.insert(snapshot_id);
    ordered.push(snapshot_id);
    Ok(())
}

fn merge_lineage_entries(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    target_root: &Path,
    lineage: &[Id],
) -> Result<BTreeMap<PathBuf, CapturedEntry>> {
    let mut by_target = BTreeMap::<PathBuf, CapturedEntry>::new();
    for snapshot_id in lineage {
        let root_path =
            load_string_attr(ws, catalog, *snapshot_id, playground_workspace::root_path)?;
        let restore_root = resolve_restore_root(target_root, root_path.as_deref())?;
        let entries = collect_snapshot_entries(ws, catalog, *snapshot_id)?;
        for entry in entries {
            let rel = Path::new(entry.path.as_str());
            if rel.is_absolute()
                || rel
                    .components()
                    .any(|c| std::matches!(c, Component::ParentDir))
            {
                return Err(anyhow!("invalid snapshot path: {}", entry.path));
            }
            let target = restore_root.join(rel);
            by_target.insert(target, entry);
        }
    }
    Ok(by_target)
}

fn merge_entry_maps(
    ours: &BTreeMap<PathBuf, CapturedEntry>,
    theirs: &BTreeMap<PathBuf, CapturedEntry>,
) -> Result<Vec<(PathBuf, CapturedEntry)>> {
    let mut all_paths = BTreeSet::new();
    all_paths.extend(ours.keys().cloned());
    all_paths.extend(theirs.keys().cloned());

    let mut merged = Vec::new();
    for path in all_paths {
        let ours_entry = ours.get(&path);
        let theirs_entry = theirs.get(&path);
        let Some(entry) = merge_path(None, ours_entry, theirs_entry) else {
            continue;
        };
        merged.push((path, entry));
    }
    Ok(merged)
}

fn merge_path(
    base: Option<&CapturedEntry>,
    ours: Option<&CapturedEntry>,
    theirs: Option<&CapturedEntry>,
) -> Option<CapturedEntry> {
    if ours == theirs {
        return ours.cloned();
    }
    if base == ours {
        return theirs.cloned();
    }
    if base == theirs {
        return ours.cloned();
    }
    ours.cloned()
}

fn apply_merged_entries(entries: &[(PathBuf, CapturedEntry)]) -> Result<(usize, usize, usize)> {
    let mut created = 0usize;
    let mut unchanged = 0usize;
    let mut conflicts = 0usize;

    for (target, entry) in entries {
        match entry.kind {
            EntryKind::Dir => {
                match existing_file_type(target)? {
                    Some(file_type) if !file_type.is_dir() => {
                        conflicts += 1;
                        continue;
                    }
                    Some(_) => {
                        unchanged += 1;
                    }
                    None => {
                        fs::create_dir_all(target)
                            .with_context(|| format!("create dir {}", target.display()))?;
                        created += 1;
                    }
                }
                if let Some(mode) = entry.mode {
                    set_mode(target, mode)?;
                }
            }
            EntryKind::File => {
                let Some(parent) = target.parent() else {
                    conflicts += 1;
                    continue;
                };
                if fs::create_dir_all(parent).is_err() {
                    conflicts += 1;
                    continue;
                }

                let Some(bytes) = entry.bytes.as_ref() else {
                    conflicts += 1;
                    continue;
                };

                if let Some(file_type) = existing_file_type(target)? {
                    if !file_type.is_file() {
                        conflicts += 1;
                        continue;
                    }
                    if file_content_matches(target, bytes)? {
                        unchanged += 1;
                        if let Some(mode) = entry.mode {
                            set_mode(target, mode)?;
                        }
                        continue;
                    }
                    conflicts += 1;
                    continue;
                }

                fs::write(target, bytes)
                    .with_context(|| format!("write file {}", target.display()))?;
                if let Some(mode) = entry.mode {
                    set_mode(target, mode)?;
                }
                created += 1;
            }
            EntryKind::Symlink => {
                let Some(parent) = target.parent() else {
                    conflicts += 1;
                    continue;
                };
                if fs::create_dir_all(parent).is_err() {
                    conflicts += 1;
                    continue;
                }
                let Some(link_target) = entry.link_target.as_ref() else {
                    conflicts += 1;
                    continue;
                };
                if let Some(file_type) = existing_file_type(target)? {
                    if file_type.is_symlink() {
                        let existing = fs::read_link(target)
                            .with_context(|| format!("read symlink {}", target.display()))?;
                        if existing == PathBuf::from(link_target) {
                            unchanged += 1;
                            continue;
                        }
                    }
                    conflicts += 1;
                    continue;
                }
                create_symlink(Path::new(link_target), target)?;
                created += 1;
            }
        }
    }

    Ok((created, unchanged, conflicts))
}

fn file_content_matches(path: &Path, expected: &[u8]) -> Result<bool> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("read metadata {}", path.display()))?;
    if metadata.len() != expected.len() as u64 {
        return Ok(false);
    }
    let current = fs::read(path).with_context(|| format!("read file {}", path.display()))?;
    Ok(current.as_slice() == expected)
}

fn existing_file_type(path: &Path) -> Result<Option<fs::FileType>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata.file_type())),
        Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
        Err(err) => Err(anyhow!("read metadata {}: {err}", path.display())),
    }
}

fn collect_workspace_entries(root: &Path) -> Result<BTreeMap<PathBuf, CapturedEntry>> {
    let mut entries = Vec::new();
    collect_entries(root, root, &mut entries)?;
    let mut map = BTreeMap::new();
    for entry in entries {
        let target = root.join(Path::new(entry.path.as_str()));
        map.insert(target, entry);
    }
    Ok(map)
}

fn collect_entries(root: &Path, path: &Path, entries: &mut Vec<CapturedEntry>) -> Result<()> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("read metadata {}", path.display()))?;
    let file_type = metadata.file_type();

    if file_type.is_dir() {
        let mut children = fs::read_dir(path)
            .with_context(|| format!("read dir {}", path.display()))?
            .collect::<Result<Vec<_>, _>>()
            .with_context(|| format!("read dir entries {}", path.display()))?;
        children.sort_by_key(|entry| entry.path());
        for child in children {
            collect_entries(root, &child.path(), entries)?;
        }
        let rel = path
            .strip_prefix(root)
            .with_context(|| format!("compute relative path for {}", path.display()))?;
        if !rel.as_os_str().is_empty() {
            entries.push(CapturedEntry {
                path: rel.to_string_lossy().to_string(),
                kind: EntryKind::Dir,
                mode: mode_from_metadata(&metadata),
                bytes: None,
                link_target: None,
            });
        }
        return Ok(());
    }

    let rel = path
        .strip_prefix(root)
        .with_context(|| format!("compute relative path for {}", path.display()))?;
    let rel_path = rel.to_string_lossy().to_string();
    entries.push(capture_single_entry(path, rel_path)?);
    Ok(())
}

fn capture_single_entry(path: &Path, rel_path: String) -> Result<CapturedEntry> {
    let metadata =
        fs::symlink_metadata(path).with_context(|| format!("read metadata {}", path.display()))?;
    let file_type = metadata.file_type();

    if file_type.is_symlink() {
        let target =
            fs::read_link(path).with_context(|| format!("read link {}", path.display()))?;
        return Ok(CapturedEntry {
            path: rel_path,
            kind: EntryKind::Symlink,
            mode: mode_from_metadata(&metadata),
            bytes: None,
            link_target: Some(target.to_string_lossy().to_string()),
        });
    }

    if file_type.is_file() {
        let bytes = fs::read(path).with_context(|| format!("read file {}", path.display()))?;
        return Ok(CapturedEntry {
            path: rel_path,
            kind: EntryKind::File,
            mode: mode_from_metadata(&metadata),
            bytes: Some(bytes),
            link_target: None,
        });
    }

    Err(anyhow!("unsupported workspace path: {}", path.display()))
}

fn latest_snapshot(catalog: &TribleSet) -> Result<Option<Id>> {
    let mut latest: Option<(Id, Value<NsTAIInterval>)> = None;
    for (snapshot_id, created_at) in find!(
        (snapshot_id: Id, created_at: Value<NsTAIInterval>),
        pattern!(&catalog, [{
            ?snapshot_id @
            playground_workspace::kind: playground_workspace::kind_snapshot,
            playground_workspace::created_at: ?created_at,
        }])
    ) {
        let key = interval_key(created_at);
        match latest {
            Some((_, current)) if interval_key(current) >= key => {}
            _ => latest = Some((snapshot_id, created_at)),
        }
    }
    Ok(latest.map(|(id, _)| id))
}

fn collect_snapshot_entries(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    snapshot_id: Id,
) -> Result<Vec<CapturedEntry>> {
    let mut entries = Vec::new();
    for (entry_id,) in find!(
        (entry_id: Id),
        pattern!(catalog, [{
            snapshot_id @
            playground_workspace::entry: ?entry_id,
        }])
    ) {
        entries.push(load_entry(ws, catalog, entry_id)?);
    }
    Ok(entries)
}

fn load_entry(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    entry_id: Id,
) -> Result<CapturedEntry> {
    let kind = find!(
        (kind: Id),
        pattern!(catalog, [{
            entry_id @
            playground_workspace::kind: ?kind,
        }])
    )
    .into_iter()
    .next()
    .map(|(kind,)| kind)
    .ok_or_else(|| anyhow!("workspace entry missing kind: {entry_id:x}"))?;

    let path = load_string_attr(ws, catalog, entry_id, playground_workspace::path)?
        .ok_or_else(|| anyhow!("workspace entry missing path: {entry_id:x}"))?;

    let mode = load_u256_attr(catalog, entry_id, playground_workspace::mode)
        .and_then(u256be_to_u64)
        .and_then(|value| u32::try_from(value).ok());

    let bytes_handle =
        load_handle_attr::<FileBytes>(catalog, entry_id, playground_workspace::bytes);
    let bytes = match bytes_handle {
        Some(handle) => {
            let blob: Blob<FileBytes> = ws.get(handle).context("read workspace blob")?;
            Some(blob.bytes.as_ref().to_vec())
        }
        None => None,
    };

    let link_target = load_string_attr(ws, catalog, entry_id, playground_workspace::link_target)?;

    let entry_kind = if kind == playground_workspace::kind_file {
        EntryKind::File
    } else if kind == playground_workspace::kind_dir {
        EntryKind::Dir
    } else if kind == playground_workspace::kind_symlink {
        EntryKind::Symlink
    } else {
        return Err(anyhow!("unknown workspace entry kind: {kind:x}"));
    };

    Ok(CapturedEntry {
        path,
        kind: entry_kind,
        mode,
        bytes,
        link_target,
    })
}

fn load_string_attr(
    ws: &mut Workspace<Pile>,
    catalog: &TribleSet,
    entity_id: Id,
    attr: Attribute<Handle<Blake3, LongString>>,
) -> Result<Option<String>> {
    let handle = find!(
        (value: Value<Handle<Blake3, LongString>>),
        pattern!(catalog, [{
            entity_id @
            attr: ?value,
        }])
    )
    .into_iter()
    .next()
    .map(|(handle,)| handle);

    let Some(handle) = handle else {
        return Ok(None);
    };
    Ok(Some(load_text(ws, handle)?))
}

fn load_u256_attr(
    catalog: &TribleSet,
    entity_id: Id,
    attr: Attribute<U256BE>,
) -> Option<Value<U256BE>> {
    find!(
        (value: Value<U256BE>),
        pattern!(catalog, [{
            entity_id @
            attr: ?value,
        }])
    )
    .into_iter()
    .next()
    .map(|(value,)| value)
}

fn load_handle_attr<S>(
    catalog: &TribleSet,
    entity_id: Id,
    attr: Attribute<Handle<Blake3, S>>,
) -> Option<Value<Handle<Blake3, S>>>
where
    S: BlobSchema,
{
    find!(
        (value: Value<Handle<Blake3, S>>),
        pattern!(catalog, [{
            entity_id @
            attr: ?value,
        }])
    )
    .into_iter()
    .next()
    .map(|(value,)| value)
}

fn u256be_to_u64(value: Value<U256BE>) -> Option<u64> {
    let raw = value.raw;
    if raw[..24].iter().any(|byte| *byte != 0) {
        return None;
    }
    let bytes: [u8; 8] = raw[24..32].try_into().ok()?;
    Some(u64::from_be_bytes(bytes))
}

#[cfg(unix)]
fn mode_from_metadata(metadata: &fs::Metadata) -> Option<u32> {
    use std::os::unix::fs::MetadataExt;
    Some(metadata.mode())
}

#[cfg(not(unix))]
fn mode_from_metadata(_metadata: &fs::Metadata) -> Option<u32> {
    None
}

#[cfg(unix)]
fn set_mode(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(mode))
        .with_context(|| format!("set permissions {}", path.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_mode(_path: &Path, _mode: u32) -> Result<()> {
    Ok(())
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, link)
        .with_context(|| format!("create symlink {}", link.display()))?;
    Ok(())
}

#[cfg(not(unix))]
fn create_symlink(_target: &Path, _link: &Path) -> Result<()> {
    Err(anyhow!("symlink creation unsupported on this platform"))
}
