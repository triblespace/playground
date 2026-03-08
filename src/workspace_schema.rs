use triblespace::core::metadata;
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::{FileBytes, LongString, SimpleArchive};
use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

pub mod playground_workspace {
    use super::*;

    attributes! {
        "A95E92FB35943C570BE45FF811B0BD07" as pub created_at: NsTAIInterval;
        "5D36AA8480B30F62394911A003F20DDF" as pub parent_snapshot: GenId;
        "B667B02CEB4493232632473ECB782287" as pub root_path: Handle<Blake3, LongString>;
        "813B3BFA590103FFAD324FC72CDDC3F5" as pub state: Handle<Blake3, SimpleArchive>;
        "435869D280EC3123D391A32025C6F3CC" as pub label: Handle<Blake3, LongString>;
        "C69E168C68E317858A62BA51FC326E97" as pub entry: GenId;
        "1032F072E6730AB40A6F5F568C4C23EB" as pub path: Handle<Blake3, LongString>;
        "C91379DEDA545341C8C7A7B4DA65C8FE" as pub mode: U256BE;
        "5FBC9E963E2BA9E2CC9E7B7C12587FBB" as pub bytes: Handle<Blake3, FileBytes>;
        "6AD64B466D4AB7B7E14D8C28DFFC592F" as pub link_target: Handle<Blake3, LongString>;
    }

    /// Root id for describing the playground_workspace protocol.
    #[allow(non_upper_case_globals)]
    pub const playground_workspace_metadata: Id = id_hex!("A2FFA9482870C13D310D0E5F1C54137B");

    /// Tag for workspace snapshot entities.
    #[allow(non_upper_case_globals)]
    pub const kind_snapshot: Id = id_hex!("1620AABF5A93D4897DFFE728308D358E");
    /// Tag for workspace file entries.
    #[allow(non_upper_case_globals)]
    pub const kind_file: Id = id_hex!("4B8C79B3B6E84C2187C078C533737718");
    /// Tag for workspace directory entries.
    #[allow(non_upper_case_globals)]
    pub const kind_dir: Id = id_hex!("7010C177AE931A2E3116AE742914D23F");
    /// Tag for workspace symlink entries.
    #[allow(non_upper_case_globals)]
    pub const kind_symlink: Id = id_hex!("486FCFF53CAD57EAD3DCFB7D903B245B");

}

pub fn build_playground_workspace_metadata<B>(
    blobs: &mut B,
) -> std::result::Result<Fragment, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let attrs = playground_workspace::describe(blobs)?;

    let mut protocol = entity! { ExclusiveId::force_ref(&playground_workspace::playground_workspace_metadata) @
        metadata::name: blobs.put("playground_workspace")?,
        metadata::description: blobs.put("Playground workspace protocol.")?,
        metadata::tag: metadata::KIND_PROTOCOL,
        metadata::attribute*: attrs,
    };

    protocol += entity! { ExclusiveId::force_ref(&playground_workspace::kind_snapshot) @
        metadata::name: blobs.put("kind_snapshot")?,
        metadata::description: blobs.put("Workspace snapshot entity kind.")?,
        metadata::tag: metadata::KIND_TAG,
    };
    protocol += entity! { ExclusiveId::force_ref(&playground_workspace::kind_file) @
        metadata::name: blobs.put("kind_file")?,
        metadata::description: blobs.put("Workspace file entry entity kind.")?,
        metadata::tag: metadata::KIND_TAG,
    };
    protocol += entity! { ExclusiveId::force_ref(&playground_workspace::kind_dir) @
        metadata::name: blobs.put("kind_dir")?,
        metadata::description: blobs.put("Workspace directory entry entity kind.")?,
        metadata::tag: metadata::KIND_TAG,
    };
    protocol += entity! { ExclusiveId::force_ref(&playground_workspace::kind_symlink) @
        metadata::name: blobs.put("kind_symlink")?,
        metadata::description: blobs.put("Workspace symlink entry entity kind.")?,
        metadata::tag: metadata::KIND_TAG,
    };

    Ok(protocol)
}
