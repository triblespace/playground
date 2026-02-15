use triblespace::core::metadata;
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval};
use triblespace::prelude::*;

pub mod playground_cog {
    use super::*;

    attributes! {
        "07F063ECF1DC9FB3C1984BDB10B98BFA" as pub kind: GenId;
        "FA6090FB00EEE2F5EF1E51F1F68EA5B8" as pub prompt: Handle<Blake3, LongString>;
        "99F834C6A6A050DECBE42D639288B559" as pub created_at: NsTAIInterval;
        "D986EF113EFA588E6247420A06DA87BA" as pub about_exec_result: GenId;
    }

    /// Root id for describing the playground_cog protocol.
    #[allow(non_upper_case_globals)]
    pub const playground_cog_metadata: Id = id_hex!("369BE69D185F799CA5370205D34FC120");

    /// Tag for thought entities.
    #[allow(non_upper_case_globals)]
    pub const kind_thought: Id = id_hex!("26FA0606BCF4AA73F868B029596828DB");

    /// Tag for playground_cog protocol metadata.
    #[allow(non_upper_case_globals)]
    pub const tag_protocol: Id = id_hex!("3B4417FCEBB29775EA2C2C9CB569505C");
    /// Tag for kind constants in the playground_cog protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_kind: Id = id_hex!("725BAB37F8F32537DD3374E5F0E6AA35");
    /// Tag for attribute constants in the playground_cog protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_attribute: Id = id_hex!("AA5B7AC4F42CB4DC06878E88E546B5DF");
    /// Tag for tag constants in the playground_cog protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_tag: Id = id_hex!("D05FA0E7791634CA02F9F9DE125ECCBF");
}

pub fn describe<B>(blobs: &mut B) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut tribles = TribleSet::new();

    tribles += entity! { ExclusiveId::force_ref(&playground_cog::playground_cog_metadata) @
        metadata::name: blobs.put("playground_cog_metadata".to_string())?,
        metadata::description: blobs.put(
            "Root id for describing the playground_cog protocol.".to_string(),
        )?,
        metadata::tag: playground_cog::tag_protocol,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_cog::tag_protocol) @
        metadata::name: blobs.put("tag_protocol".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_cog protocol metadata.".to_string(),
        )?,
        metadata::tag: playground_cog::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_cog::tag_kind) @
        metadata::name: blobs.put("tag_kind".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_cog protocol kind constants.".to_string(),
        )?,
        metadata::tag: playground_cog::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_cog::tag_attribute) @
        metadata::name: blobs.put("tag_attribute".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_cog protocol attributes.".to_string(),
        )?,
        metadata::tag: playground_cog::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_cog::tag_tag) @
        metadata::name: blobs.put("tag_tag".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_cog protocol tag constants.".to_string(),
        )?,
        metadata::tag: playground_cog::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_cog::kind_thought) @
        metadata::name: blobs.put("kind_thought".to_string())?,
        metadata::description: blobs.put(
            "Thought entity kind.".to_string(),
        )?,
        metadata::tag: playground_cog::tag_kind,
    };

    Ok(tribles)
}

pub fn build_playground_cog_metadata<B>(
    blobs: &mut B,
) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut metadata = describe(blobs)?;

    metadata += <GenId as metadata::ConstDescribe>::describe(blobs)?;
    metadata += <NsTAIInterval as metadata::ConstDescribe>::describe(blobs)?;
    metadata += <Handle<Blake3, LongString> as metadata::ConstDescribe>::describe(blobs)?;

    metadata += describe_attribute(blobs, &playground_cog::kind)?;
    metadata += describe_attribute(blobs, &playground_cog::prompt)?;
    metadata += describe_attribute(blobs, &playground_cog::created_at)?;
    metadata += describe_attribute(blobs, &playground_cog::about_exec_result)?;

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
        metadata::tag: playground_cog::tag_attribute,
    };
    Ok(tribles)
}
