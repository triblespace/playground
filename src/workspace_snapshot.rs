use std::fs;
use std::path::{Component, Path};

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
enum EntryKind {
    File,
    Dir,
    Symlink,
}

#[derive(Debug, Clone)]
struct CapturedEntry {
    path: String,
    kind: EntryKind,
    mode: Option<u32>,
    bytes: Option<Vec<u8>>,
    link_target: Option<String>,
}

pub fn restore_snapshot(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    snapshot_id: Option<Id>,
    target_root: &Path,
    force: bool,
) -> Result<Option<Id>> {
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

    let entries = collect_snapshot_entries(&mut ws, &catalog, snapshot_id)?;
    restore_entries(target_root, &entries, force)?;

    Ok(Some(snapshot_id))
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

    let bytes_handle = load_handle_attr::<FileBytes>(catalog, entry_id, playground_workspace::bytes);
    let bytes = match bytes_handle {
        Some(handle) => {
            let blob: Blob<FileBytes> = ws.get(handle).context("read workspace blob")?;
            Some(blob.bytes.as_ref().to_vec())
        }
        None => None,
    };

    let link_target =
        load_string_attr(ws, catalog, entry_id, playground_workspace::link_target)?;

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

fn restore_entries(target_root: &Path, entries: &[CapturedEntry], force: bool) -> Result<()> {
    if !target_root.exists() {
        fs::create_dir_all(target_root).with_context(|| {
            format!("create workspace root {}", target_root.display())
        })?;
    }

    for entry in entries {
        let rel = Path::new(entry.path.as_str());
        if rel
            .is_absolute()
            || rel
                .components()
                .any(|c| std::matches!(c, Component::ParentDir))
        {
            return Err(anyhow!("invalid snapshot path: {}", entry.path));
        }
        let target = target_root.join(rel);
        match entry.kind {
            EntryKind::Dir => {
                fs::create_dir_all(&target)
                    .with_context(|| format!("create dir {}", target.display()))?;
                if let Some(mode) = entry.mode {
                    set_mode(&target, mode)?;
                }
            }
            EntryKind::Symlink => {
                let target_parent = target
                    .parent()
                    .ok_or_else(|| anyhow!("invalid snapshot path: {}", entry.path))?;
                fs::create_dir_all(target_parent).with_context(|| {
                    format!("create dir {}", target_parent.display())
                })?;
                if target.exists() {
                    if !force {
                        return Err(anyhow!("path exists: {}", target.display()));
                    }
                    remove_existing(&target)?;
                }
                let link_target = entry
                    .link_target
                    .as_ref()
                    .ok_or_else(|| anyhow!("missing link target for {}", entry.path))?;
                create_symlink(Path::new(link_target), &target)?;
            }
            EntryKind::File => {
                let target_parent = target
                    .parent()
                    .ok_or_else(|| anyhow!("invalid snapshot path: {}", entry.path))?;
                fs::create_dir_all(target_parent).with_context(|| {
                    format!("create dir {}", target_parent.display())
                })?;
                if target.exists() && !force {
                    return Err(anyhow!("path exists: {}", target.display()));
                }
                let bytes = entry
                    .bytes
                    .as_ref()
                    .ok_or_else(|| anyhow!("missing file contents for {}", entry.path))?;
                if force {
                    fs::write(&target, bytes)
                        .with_context(|| format!("write file {}", target.display()))?;
                } else {
                    let mut file = fs::OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(&target)
                        .with_context(|| format!("create file {}", target.display()))?;
                    use std::io::Write;
                    file.write_all(bytes)
                        .with_context(|| format!("write file {}", target.display()))?;
                }
                if let Some(mode) = entry.mode {
                    set_mode(&target, mode)?;
                }
            }
        }
    }

    Ok(())
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

fn remove_existing(path: &Path) -> Result<()> {
    if path.is_dir() {
        fs::remove_dir_all(path)
            .with_context(|| format!("remove dir {}", path.display()))?;
    } else {
        fs::remove_file(path)
            .with_context(|| format!("remove file {}", path.display()))?;
    }
    Ok(())
}
