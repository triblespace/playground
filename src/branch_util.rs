use anyhow::{Context, Result, anyhow};
use triblespace::core::metadata;
use triblespace::core::repo::Repository;
use triblespace::core::repo::pile::Pile;
use triblespace::prelude::*;

pub(crate) fn ensure_branch_id(repo: &mut Repository<Pile>, name: &str) -> Result<Id> {
    if let Some(id) = find_branch_id(repo.storage_mut(), name)? {
        repo.pull(id)
            .map(|_| ())
            .map_err(|err| anyhow!("pull branch {name}: {err:?}"))?;
        return Ok(id);
    }

    Ok(*repo
        .create_branch(name, None)
        .map_err(|err| anyhow!("create branch {name}: {err:?}"))?)
}

pub(crate) fn find_branch_id(pile: &mut Pile, name: &str) -> Result<Option<Id>> {
    let name_handle = name
        .to_owned()
        .to_blob()
        .get_handle::<valueschemas::Blake3>();
    let reader = pile.reader().context("pile reader")?;
    let iter = pile.branches().context("list branches")?;

    for branch in iter {
        let branch_id = branch.context("branch id")?;
        let Some(head) = pile.head(branch_id).context("branch head")? else {
            continue;
        };
        let metadata_set: TribleSet = reader.get(head).context("branch metadata")?;
        let mut names = find!(
            (handle: Value<valueschemas::Handle<valueschemas::Blake3, blobschemas::LongString>>),
            pattern!(&metadata_set, [{ metadata::name: ?handle }])
        )
        .into_iter();
        let Some(branch_name) = names.next().map(|(handle,)| handle) else {
            continue;
        };
        if names.next().is_some() {
            continue;
        }
        if branch_name == name_handle {
            return Ok(Some(branch_id));
        }
    }

    Ok(None)
}
