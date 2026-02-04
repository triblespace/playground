use triblespace::core::metadata;
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::{FileBytes, LongString};
use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

pub mod playground_workspace {
    use super::*;

    attributes! {
        "E39FB34126FE01A32F1D4B3DAD0F1874" as pub kind: GenId;
        "A95E92FB35943C570BE45FF811B0BD07" as pub created_at: NsTAIInterval;
        "B667B02CEB4493232632473ECB782287" as pub root_path: Handle<Blake3, LongString>;
        "435869D280EC3123D391A32025C6F3CC" as pub label: Handle<Blake3, LongString>;
        "C69E168C68E317858A62BA51FC326E97" as pub entry: GenId;
        "1032F072E6730AB40A6F5F568C4C23EB" as pub path: Handle<Blake3, LongString>;
        "C91379DEDA545341C8C7A7B4DA65C8FE" as pub mode: U256BE;
        "5FBC9E963E2BA9E2CC9E7B7C12587FBB" as pub bytes: Handle<Blake3, FileBytes>;
        "6AD64B466D4AB7B7E14D8C28DFFC592F" as pub link_target: Handle<Blake3, LongString>;
    }

    /// Root id for describing the playground_workspace protocol.
    #[allow(non_upper_case_globals)]
    pub const playground_workspace_metadata: Id =
        id_hex!("A2FFA9482870C13D310D0E5F1C54137B");

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
        metadata::shortname: "playground_workspace_metadata",
        metadata::name: blobs.put::<LongString, _>(
            "Root id for describing the playground_workspace protocol.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_protocol,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::tag_protocol) @
        metadata::shortname: "tag_protocol",
        metadata::name: blobs.put::<LongString, _>(
            "Tag for playground_workspace protocol metadata.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::tag_kind) @
        metadata::shortname: "tag_kind",
        metadata::name: blobs.put::<LongString, _>(
            "Tag for playground_workspace protocol kind constants.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::tag_attribute) @
        metadata::shortname: "tag_attribute",
        metadata::name: blobs.put::<LongString, _>(
            "Tag for playground_workspace protocol attributes.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::tag_tag) @
        metadata::shortname: "tag_tag",
        metadata::name: blobs.put::<LongString, _>(
            "Tag for playground_workspace protocol tag constants.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::kind_snapshot) @
        metadata::shortname: "kind_snapshot",
        metadata::name: blobs.put::<LongString, _>(
            "Workspace snapshot entity kind.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::kind_file) @
        metadata::shortname: "kind_file",
        metadata::name: blobs.put::<LongString, _>(
            "Workspace file entry entity kind.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::kind_dir) @
        metadata::shortname: "kind_dir",
        metadata::name: blobs.put::<LongString, _>(
            "Workspace directory entry entity kind.".to_string(),
        )?,
        metadata::tag: playground_workspace::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_workspace::kind_symlink) @
        metadata::shortname: "kind_symlink",
        metadata::name: blobs.put::<LongString, _>(
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

    metadata.union(<GenId as metadata::ConstMetadata>::describe(blobs)?);
    metadata.union(<NsTAIInterval as metadata::ConstMetadata>::describe(blobs)?);
    metadata.union(<U256BE as metadata::ConstMetadata>::describe(blobs)?);
    metadata.union(<Handle<Blake3, LongString> as metadata::ConstMetadata>::describe(blobs)?);
    metadata.union(<Handle<Blake3, FileBytes> as metadata::ConstMetadata>::describe(blobs)?);
    metadata.union(<FileBytes as metadata::ConstMetadata>::describe(blobs)?);

    macro_rules! add_attribute {
        ($attribute:expr, $name:expr) => {
            metadata.union(describe_attribute(blobs, &$attribute, $name)?);
        };
    }

    add_attribute!(playground_workspace::kind, "playground_workspace_kind");
    add_attribute!(
        playground_workspace::created_at,
        "playground_workspace_created_at"
    );
    add_attribute!(
        playground_workspace::root_path,
        "playground_workspace_root_path"
    );
    add_attribute!(playground_workspace::label, "playground_workspace_label");
    add_attribute!(playground_workspace::entry, "playground_workspace_entry");
    add_attribute!(playground_workspace::path, "playground_workspace_path");
    add_attribute!(playground_workspace::mode, "playground_workspace_mode");
    add_attribute!(playground_workspace::bytes, "playground_workspace_bytes");
    add_attribute!(
        playground_workspace::link_target,
        "playground_workspace_link_target"
    );

    Ok(metadata)
}

fn describe_attribute<B, S>(
    blobs: &mut B,
    attribute: &Attribute<S>,
    name: &str,
) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
    S: ValueSchema,
{
    let mut tribles = metadata::Metadata::describe(attribute, blobs)?;
    let handle = blobs.put::<LongString, _>(name.to_owned())?;
    let attribute_id = metadata::Metadata::id(attribute);
    tribles += entity! { ExclusiveId::force_ref(&attribute_id) @
        metadata::shortname: name,
        metadata::name: handle,
        metadata::tag: playground_workspace::tag_attribute,
    };
    Ok(tribles)
}
