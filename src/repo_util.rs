use std::fs;

use anyhow::{Context, Result, anyhow};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::blob::schemas::simplearchive::SimpleArchive;
use triblespace::core::id::ExclusiveId;
use triblespace::core::metadata;
use triblespace::core::repo::pile::{Pile, ReadError};
use triblespace::core::repo::{PullError, Repository, Workspace};
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, Handle};
use triblespace::prelude::*;

use crate::config::Config;
pub(crate) use crate::repo_ops::push_workspace;
use crate::schema::build_playground_metadata;

pub(crate) fn init_repo(config: &Config) -> Result<(Repository<Pile>, Id)> {
    if let Some(parent) = config.pile_path.parent() {
        fs::create_dir_all(parent).context("create pile directory")?;
    }
    let mut pile = Pile::open(&config.pile_path).context("open pile")?;
    pile.restore().context("restore pile")?;

    let mut repo = Repository::new(pile, SigningKey::generate(&mut OsRng));
    let branch_id = config.branch_id.ok_or_else(|| {
        anyhow!("config is missing branch_id; run `playground config set branch-id <ID>`")
    })?;
    pull_workspace(&mut repo, branch_id, &format!("pull branch {branch_id:x}"))?;

    Ok((repo, branch_id))
}

pub(crate) fn close_repo(repo: Repository<Pile>) -> Result<()> {
    repo.into_storage().close().context("close pile")
}

pub(crate) type CommitHandle = Value<Handle<Blake3, SimpleArchive>>;

pub(crate) fn current_branch_head(
    repo: &mut Repository<Pile>,
    branch_id: Id,
) -> Result<Option<CommitHandle>> {
    match repo.storage_mut().head(branch_id) {
        Ok(head) => Ok(head),
        Err(ReadError::CorruptPile { valid_length }) => {
            eprintln!(
                "warning: read branch head {branch_id:x}: corrupt pile tail (valid_length={valid_length}), restoring and retrying"
            );
            repo.storage_mut()
                .restore()
                .map_err(|err| anyhow!("restore pile after head corruption: {err:?}"))?;
            repo.storage_mut()
                .head(branch_id)
                .map_err(|err| anyhow!("read branch head {branch_id:x} after restore: {err:?}"))
        }
        Err(err) => Err(anyhow!("read branch head {branch_id:x}: {err:?}")),
    }
}

pub(crate) fn refresh_cached_checkout(
    ws: &mut Workspace<Pile>,
    cached_head: &mut Option<CommitHandle>,
    cached_catalog: &mut TribleSet,
) -> Result<TribleSet> {
    let head = ws.head();
    if *cached_head == head {
        return Ok(TribleSet::new());
    }

    let delta = ws
        .checkout(*cached_head..head)
        .context("checkout from cached head to current head")?;
    let mut data = cached_catalog.clone();
    data += delta.clone();

    *cached_catalog = data;
    *cached_head = head;
    Ok(delta)
}

pub(crate) fn seed_metadata(repo: &mut Repository<Pile>) -> Result<()> {
    let metadata = build_playground_metadata(repo.storage_mut())
        .map_err(|err| anyhow!("build playground metadata: {err:?}"))?;
    repo.set_default_metadata(metadata)
        .map_err(|err| anyhow!("set playground metadata: {err:?}"))?;
    Ok(())
}

pub(crate) fn load_text(
    ws: &mut Workspace<Pile>,
    handle: Value<Handle<Blake3, LongString>>,
) -> Result<String> {
    let view: View<str> = ws.get(handle).context("read text blob")?;
    Ok(view.as_ref().to_string())
}

pub(crate) fn ensure_worker_name(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    worker_id: Id,
    label: &str,
) -> Result<()> {
    let mut ws = pull_workspace(repo, branch_id, "pull workspace for worker name")?;
    let catalog = ws.checkout(..).context("checkout workspace")?;

    let exists = find!(
        (name_handle: Value<Handle<Blake3, LongString>>),
        pattern!(&catalog, [{ worker_id @ metadata::name: ?name_handle }])
    )
    .into_iter()
    .next()
    .is_some();
    if exists {
        return Ok(());
    }

    let name_blob = label.to_owned().to_blob();
    let name_handle = name_blob.get_handle::<Blake3>();
    repo.storage_mut()
        .put(name_blob)
        .map_err(|err| anyhow!("store worker name blob: {err:?}"))?;

    let mut change = TribleSet::new();
    change += entity! { ExclusiveId::force_ref(&worker_id) @
        metadata::name: name_handle
    };
    ws.commit(change, None, Some("worker name"));
    push_workspace(repo, &mut ws).context("push worker name")?;
    Ok(())
}

pub(crate) fn pull_workspace(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    context: &str,
) -> Result<Workspace<Pile>> {
    match repo.pull(branch_id) {
        Ok(ws) => Ok(ws),
        Err(err) => {
            let Some(valid_length) = pull_corrupt_valid_length(&err) else {
                return Err(anyhow!("{context}: {err:?}"));
            };
            eprintln!(
                "warning: {context}: corrupt pile tail (valid_length={valid_length}), restoring and retrying"
            );
            repo.storage_mut()
                .restore()
                .map_err(|restore_err| anyhow!("{context}: restore pile: {restore_err:?}"))?;
            repo.pull(branch_id)
                .map_err(|retry_err| anyhow!("{context} after restore: {retry_err:?}"))
        }
    }
}

fn pull_corrupt_valid_length<B: std::error::Error>(
    err: &PullError<ReadError, ReadError, B>,
) -> Option<usize> {
    match err {
        PullError::BlobReader(ReadError::CorruptPile { valid_length })
        | PullError::BranchStorage(ReadError::CorruptPile { valid_length }) => Some(*valid_length),
        _ => None,
    }
}
