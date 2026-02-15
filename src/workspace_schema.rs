use triblespace::core::metadata;
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::{FileBytes, LongString, SimpleArchive};
use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

pub mod playground_workspace {
    use super::*;

    attributes! {
        "E39FB34126FE01A32F1D4B3DAD0F1874" as pub kind: GenId;
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

    /// Tag for playground_workspace protocol metadata.
    #[allow(non_upper_case_globals)]
    pub const tag_protocol: Id = id_hex!("17692551EAC2594296E6ED4C55E9A033");
    /// Tag for kind constants in the playground_workspace protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_kind: Id = id_hex!("0B36383E3DD32197C333D75DB57C2DA9");
    /// Tag for attribute constants in the playground_workspace protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_attribute: Id = id_hex!("96A6F67D5047FF1CCD4DEC11FD244AC7");
    /// Tag for tag constants in the playground_workspace protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_tag: Id = id_hex!("B734BF2E666FB00D538DC6AABE41C20C");
}

pub fn describe<B>(blobs: &mut B) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut tribles = TribleSet::new();

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::playground_workspace_metadata) @
        metadata::name: blobs.put("playground_workspace_metadata".to_string())?,
        metadata::description: blobs.put(
            "Root id for describing the playground_workspace protocol.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_protocol,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::tag_protocol) @
        metadata::name: blobs.put("tag_protocol".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_workspace protocol metadata.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::tag_kind) @
        metadata::name: blobs.put("tag_kind".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_workspace protocol kind constants.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::tag_attribute) @
        metadata::name: blobs.put("tag_attribute".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_workspace protocol attributes.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::tag_tag) @
        metadata::name: blobs.put("tag_tag".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_workspace protocol tag constants.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::kind_snapshot) @
        metadata::name: blobs.put("kind_snapshot".to_string())?,
        metadata::description: blobs.put(
            "Workspace snapshot entity kind.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::kind_file) @
        metadata::name: blobs.put("kind_file".to_string())?,
        metadata::description: blobs.put(
            "Workspace file entry entity kind.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::kind_dir) @
        metadata::name: blobs.put("kind_dir".to_string())?,
        metadata::description: blobs.put(
            "Workspace directory entry entity kind.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::kind_symlink) @
        metadata::name: blobs.put("kind_symlink".to_string())?,
        metadata::description: blobs.put(
            "Workspace symlink entry entity kind.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_kind,
    };

    Ok(tribles)
}

pub fn build_playground_workspace_metadata<B>(
    blobs: &mut B,
) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut metadata = describe(blobs)?;

    metadata.union(<GenId as metadata::ConstDescribe>::describe(blobs)?);
    metadata.union(<NsTAIInterval as metadata::ConstDescribe>::describe(blobs)?);
    metadata.union(<U256BE as metadata::ConstDescribe>::describe(blobs)?);
    metadata.union(<Handle<Blake3, LongString> as metadata::ConstDescribe>::describe(blobs)?);
    metadata.union(<Handle<Blake3, SimpleArchive> as metadata::ConstDescribe>::describe(blobs)?);
    metadata.union(<Handle<Blake3, FileBytes> as metadata::ConstDescribe>::describe(blobs)?);
    metadata.union(<SimpleArchive as metadata::ConstDescribe>::describe(blobs)?);
    metadata.union(<FileBytes as metadata::ConstDescribe>::describe(blobs)?);

    metadata.union(describe_attribute(blobs, &playground_workspace::kind)?);
    metadata.union(describe_attribute(
        blobs,
        &playground_workspace::created_at,
    )?);
    metadata.union(describe_attribute(
        blobs,
        &playground_workspace::parent_snapshot,
    )?);
    metadata.union(describe_attribute(blobs, &playground_workspace::root_path)?);
    metadata.union(describe_attribute(blobs, &playground_workspace::state)?);
    metadata.union(describe_attribute(blobs, &playground_workspace::label)?);
    metadata.union(describe_attribute(blobs, &playground_workspace::entry)?);
    metadata.union(describe_attribute(blobs, &playground_workspace::path)?);
    metadata.union(describe_attribute(blobs, &playground_workspace::mode)?);
    metadata.union(describe_attribute(blobs, &playground_workspace::bytes)?);
    metadata.union(describe_attribute(
        blobs,
        &playground_workspace::link_target,
    )?);

    Ok(metadata)
}

fn describe_attribute<B, S>(
    blobs: &mut B,
    attribute: &Attribute<S>,
) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
    S: ValueSchema,
{
    let mut tribles = metadata::Describe::describe(attribute, blobs)?.into_facts();
    let attribute_id = attribute.id();
    tribles += entity! { ExclusiveId::force_ref(&attribute_id) @
        metadata::tag: playground_workspace::tag_attribute,
    };
    Ok(tribles)
}
