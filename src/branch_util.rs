use anyhow::{Result, anyhow};
use triblespace::core::repo::PushResult;
use triblespace::core::repo::Repository;
use triblespace::core::repo::branch as branch_meta;
use triblespace::core::repo::pile::Pile;
use triblespace::prelude::*;

pub(crate) fn ensure_branch(repo: &mut Repository<Pile>, branch_id: Id, name: &str) -> Result<()> {
    if repo
        .storage_mut()
        .head(branch_id)
        .map_err(|err| anyhow!("read branch {branch_id:x} head: {err:?}"))?
        .is_some()
    {
        return Ok(());
    }

    let name_blob = name.to_owned().to_blob();
    let name_handle = name_blob.get_handle::<valueschemas::Blake3>();
    repo.storage_mut()
        .put(name_blob)
        .map_err(|err| anyhow!("store branch name blob for {name}: {err:?}"))?;

    let metadata = branch_meta::branch_unsigned(branch_id, name_handle, None);
    let metadata_handle = repo
        .storage_mut()
        .put(metadata.to_blob())
        .map_err(|err| anyhow!("store branch metadata for {name}: {err:?}"))?;

    let push_result = repo
        .storage_mut()
        .update(branch_id, None, Some(metadata_handle))
        .map_err(|err| anyhow!("create branch {name} ({branch_id:x}): {err:?}"))?;
    match push_result {
        PushResult::Success() | PushResult::Conflict(_) => Ok(()),
    }
}
