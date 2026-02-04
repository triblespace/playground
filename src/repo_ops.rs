use anyhow::{Result, anyhow};
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::{Repository, Workspace};

pub(crate) fn push_workspace(repo: &mut Repository<Pile>, ws: &mut Workspace<Pile>) -> Result<()> {
    while let Some(mut conflict) = repo
        .try_push(ws)
        .map_err(|err| anyhow!("push workspace: {err:?}"))?
    {
        conflict
            .merge(ws)
            .map_err(|err| anyhow!("merge workspace: {err:?}"))?;
        *ws = conflict;
    }
    Ok(())
}
