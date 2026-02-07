use std::fs;

use anyhow::{Context, Result, anyhow};
use ed25519_dalek::SigningKey;
use rand::rngs::OsRng;
use triblespace::core::id::ExclusiveId;
use triblespace::core::metadata;
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, Handle};
use triblespace::prelude::*;

use crate::branch_util::find_branch_id;
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
    let branch_id = if let Some(branch_id) = config.branch_id {
        repo.pull(branch_id)
            .map(|_| ())
            .map_err(|err| anyhow!("pull branch {branch_id:x}: {err:?}"))?;
        branch_id
    } else {
        find_branch_id(repo.storage_mut(), config.branch.as_str())?.unwrap_or_else(|| {
            *repo
                .create_branch(config.branch.as_str(), None)
                .expect("create branch")
        })
    };

    Ok((repo, branch_id))
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
    let mut ws = repo
        .pull(branch_id)
        .map_err(|err| anyhow!("pull workspace for worker name: {err:?}"))?;
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
