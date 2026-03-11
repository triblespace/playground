#!/usr/bin/env -S rust-script
//! ```cargo
//! [dependencies]
//! anyhow = "1.0"
//! clap = { version = "4.5.4", features = ["derive"] }
//! ed25519-dalek = "2.1.1"
//! rand_core = "0.6.4"
//! triblespace = "0.18.0"
//! ```

use anyhow::{Result, anyhow};
use clap::Parser;
use ed25519_dalek::SigningKey;
use rand_core::OsRng;
use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use triblespace::core::repo::pile::Pile;
use triblespace::core::repo::Repository;
use triblespace::core::trible::{A_START, A_END, TribleSet};
use triblespace::macros::id_hex;
use triblespace::prelude::*;

/// The 7 per-protocol `kind` attribute IDs that were removed in the
/// "unify kind into metadata::tag" cleanup.
const OLD_KIND_ATTRS: [Id; 7] = [
    // archive (archive_schema)
    id_hex!("5F10520477A04E5FB322C85CC78C6762"),
    // cog_schema
    id_hex!("41F6FA1633D8CB6AC7B2741BA0E140F4"),
    // config_schema
    id_hex!("79F990573A9DCC91EF08A5F8CBA7AA25"),
    // context_schema
    id_hex!("07F063ECF1DC9FB3C1984BDB10B98BFA"),
    // exec_schema
    id_hex!("81E520987033BE71EB0AFFA8297DE613"),
    // model_chat_schema
    id_hex!("AA2F34973589295FA70B538D92CD30F8"),
    // workspace_schema
    id_hex!("E39FB34126FE01A32F1D4B3DAD0F1874"),
];

/// The unified `metadata::tag` attribute ID.
const METADATA_TAG_ATTR: Id = id_hex!("91C50E9FBB1F73E892EBD5FFDE46C251");

#[derive(Parser)]
#[command(
    name = "kind_to_tag",
    about = "Migrate per-protocol `kind` attributes to unified `metadata::tag`"
)]
struct Cli {
    /// Path to the pile file
    #[arg(long, default_value = "self.pile")]
    pile: PathBuf,
    /// Print what would change without writing
    #[arg(long)]
    dry_run: bool,
}

fn open_repo(path: &Path) -> Result<Repository<Pile>> {
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .map_err(|e| anyhow!("create pile dir {}: {e}", parent.display()))?;
    }
    let mut pile = Pile::open(path)
        .map_err(|e| anyhow!("open pile {}: {e:?}", path.display()))?;
    if let Err(err) = pile.restore() {
        let _ = pile.close();
        return Err(anyhow!("restore pile {}: {err:?}", path.display()));
    }

    let signing_key = SigningKey::generate(&mut OsRng);
    Repository::new(pile, signing_key, TribleSet::new())
        .map_err(|err| anyhow!("create repository: {err:?}"))
}

fn with_repo<T>(
    pile: &Path,
    f: impl FnOnce(&mut Repository<Pile>) -> Result<T>,
) -> Result<T> {
    let mut repo = open_repo(pile)?;
    let result = f(&mut repo);
    let close_res = repo.close().map_err(|e| anyhow!("close pile: {e:?}"));
    if let Err(err) = close_res {
        if result.is_ok() {
            return Err(err);
        }
        eprintln!("warning: failed to close pile cleanly: {err:#}");
    }
    result
}

fn rewrite_trible(trible: &Trible) -> Option<Trible> {
    let old_kind_set: HashSet<&Id> = OLD_KIND_ATTRS.iter().collect();
    if !old_kind_set.contains(trible.a()) {
        return None;
    }
    let mut data = trible.data;
    data[A_START..=A_END].copy_from_slice(METADATA_TAG_ATTR.as_ref());
    Trible::force_raw(data)
}

fn migrate_branch(
    repo: &mut Repository<Pile>,
    branch_id: Id,
    branch_name: &str,
    dry_run: bool,
) -> Result<usize> {
    let mut ws = repo
        .pull(branch_id)
        .map_err(|e| anyhow!("pull branch {branch_name} ({branch_id:x}): {e:?}"))?;
    let space = ws
        .checkout(..)
        .map_err(|e| anyhow!("checkout branch {branch_name}: {e:?}"))?;

    let mut rewritten = TribleSet::new();
    for trible in &space {
        if let Some(new_trible) = rewrite_trible(trible) {
            rewritten.insert(&new_trible);
        }
    }

    let delta = rewritten.difference(&space);
    let count = delta.len();

    if count == 0 {
        return Ok(0);
    }

    if dry_run {
        println!("  {branch_name} ({branch_id:x}): would rewrite {count} trible(s)");
        return Ok(count);
    }

    ws.commit(delta, "migrate kind attributes to metadata::tag");
    repo.push(&mut ws)
        .map_err(|e| anyhow!("push branch {branch_name}: {e:?}"))?;
    println!("  {branch_name} ({branch_id:x}): rewrote {count} trible(s)");
    Ok(count)
}

fn run(cli: Cli) -> Result<()> {
    println!(
        "Migrating per-protocol `kind` → `metadata::tag` in {}",
        cli.pile.display()
    );
    if cli.dry_run {
        println!("(dry run)");
    }

    with_repo(&cli.pile, |repo| {
        // Collect all branch IDs first.
        let branch_ids: Vec<Id> = {
            let pile = repo.storage_mut();
            let iter = pile
                .branches()
                .map_err(|e| anyhow!("list branches: {e:?}"))?;
            let mut ids = Vec::new();
            for branch in iter {
                ids.push(branch.map_err(|e| anyhow!("branch id: {e:?}"))?);
            }
            ids
        };

        // Try to resolve branch names for nicer output.
        let mut total = 0usize;
        for branch_id in &branch_ids {
            // Use the hex ID as the name; resolving names would require
            // reading branch metadata which adds complexity.
            let name = format!("{branch_id:x}");
            match migrate_branch(repo, *branch_id, &name, cli.dry_run) {
                Ok(n) => total += n,
                Err(e) => {
                    eprintln!("  warning: skipping branch {name}: {e:#}");
                }
            }
        }

        if total == 0 {
            println!("No kind attributes found — pile is already up to date.");
        } else if cli.dry_run {
            println!("Total: {total} trible(s) would be rewritten.");
        } else {
            println!("Done: rewrote {total} trible(s) across all branches.");
        }
        Ok(())
    })
}

fn main() -> Result<()> {
    run(Cli::parse())
}
