use triblespace::core::metadata;
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{
    Blake3, GenId, Handle, NsTAIInterval, ShortString, U256BE,
};
use triblespace::prelude::*;

pub mod playground_context {
    use super::*;

    attributes! {
        "81E520987033BE71EB0AFFA8297DE613" as pub kind: GenId;
        "8D5B05B6360EDFB6101A3E9A73A32F43" as pub level: U256BE;
        "3292CF0B3B6077991D8ECE6E2973D4B6" as pub summary: Handle<Blake3, LongString>;
        "3D5865566AF5118471DA1FF7F87CB791" as pub created_at: NsTAIInterval;
        "4EAF7FE3122A0AE2D8309B79DCCB8D75" as pub start_at: NsTAIInterval;
        "95D629052C40FA09B378DDC507BEA0D3" as pub end_at: NsTAIInterval;
        "CB97C36A32DEC70E0D1149E7C5D88588" as pub left: GenId;
        "087D07E3D9D94F0C4E96813C7BC5E74C" as pub right: GenId;
        "316834CC6B0EA6F073BF5362D67AC530" as pub about_exec_result: GenId;
        "A4E2B712CA28AB1EED76C34DE72AFA39" as pub about_archive_message: GenId;
        "0369F021B2CF6CC26BA5DBF3179A5AF2" as pub archive_author: GenId;
        "34681ECA3FAEC24970801BC24CD318A9" as pub archive_person: GenId;
        "2BEDFA649267106369790C09B02C6958" as pub archive_thread_root: GenId;
        "40487DA1794795FDDBFD128E0B746EE6" as pub archive_conversation: Handle<Blake3, LongString>;
        "49669D61D012A65A69999A214E8FFDA2" as pub archive_source_format: ShortString;
    }

    /// Root id for describing the playground_context protocol.
    #[allow(non_upper_case_globals)]
    pub const playground_context_metadata: Id = id_hex!("2B490ED2CEAC5496F7F9601724B99A48");

    /// Tag for context chunk entities.
    #[allow(non_upper_case_globals)]
    pub const kind_chunk: Id = id_hex!("40E6004417F9B767AFF1F138DE3D3AAC");

    /// Tag for playground_context protocol metadata.
    #[allow(non_upper_case_globals)]
    pub const tag_protocol: Id = id_hex!("D98E0CF3A7452F6F42DCD2F64E3D87CB");
    /// Tag for kind constants in the playground_context protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_kind: Id = id_hex!("FB4C9FFBE1CB6FB92E41915E35B95EF4");
    /// Tag for attribute constants in the playground_context protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_attribute: Id = id_hex!("1BE7411A75F1244AEF7713EBEF866E78");
    /// Tag for tag constants in the playground_context protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_tag: Id = id_hex!("3BA5BD0CEAB802DDE13FBA7B983B4C1A");
}

pub fn describe<B>(blobs: &mut B) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut tribles = TribleSet::new();

    tribles += entity! { ExclusiveId::force_ref(&playground_context::playground_context_metadata) @
        metadata::name: blobs.put("playground_context_metadata".to_string())?,
        metadata::description: blobs.put(
            "Root id for describing the playground_context protocol.".to_string(),
        )?,
        metadata::tag: playground_context::tag_protocol,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_context::tag_protocol) @
        metadata::name: blobs.put("tag_protocol".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_context protocol metadata.".to_string(),
        )?,
        metadata::tag: playground_context::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_context::tag_kind) @
        metadata::name: blobs.put("tag_kind".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_context protocol kind constants.".to_string(),
        )?,
        metadata::tag: playground_context::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_context::tag_attribute) @
        metadata::name: blobs.put("tag_attribute".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_context protocol attributes.".to_string(),
        )?,
        metadata::tag: playground_context::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_context::tag_tag) @
        metadata::name: blobs.put("tag_tag".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_context protocol tag constants.".to_string(),
        )?,
        metadata::tag: playground_context::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_context::kind_chunk) @
        metadata::name: blobs.put("kind_chunk".to_string())?,
        metadata::description: blobs.put(
            "Context chunk entity kind.".to_string(),
        )?,
        metadata::tag: playground_context::tag_kind,
    };

    Ok(tribles)
}

pub fn build_playground_context_metadata<B>(
    blobs: &mut B,
) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut metadata = describe(blobs)?;

    metadata += <GenId as metadata::ConstDescribe>::describe(blobs)?;
    metadata += <NsTAIInterval as metadata::ConstDescribe>::describe(blobs)?;
    metadata += <U256BE as metadata::ConstDescribe>::describe(blobs)?;
    metadata += <ShortString as metadata::ConstDescribe>::describe(blobs)?;
    metadata += <Handle<Blake3, LongString> as metadata::ConstDescribe>::describe(blobs)?;

    metadata += describe_attribute(blobs, &playground_context::kind)?;
    metadata += describe_attribute(blobs, &playground_context::level)?;
    metadata += describe_attribute(blobs, &playground_context::summary)?;
    metadata += describe_attribute(blobs, &playground_context::created_at)?;
    metadata += describe_attribute(blobs, &playground_context::start_at)?;
    metadata += describe_attribute(blobs, &playground_context::end_at)?;
    metadata += describe_attribute(blobs, &playground_context::left)?;
    metadata += describe_attribute(blobs, &playground_context::right)?;
    metadata += describe_attribute(blobs, &playground_context::about_exec_result)?;
    metadata += describe_attribute(blobs, &playground_context::about_archive_message)?;
    metadata += describe_attribute(blobs, &playground_context::archive_author)?;
    metadata += describe_attribute(blobs, &playground_context::archive_person)?;
    metadata += describe_attribute(blobs, &playground_context::archive_thread_root)?;
    metadata += describe_attribute(blobs, &playground_context::archive_conversation)?;
    metadata += describe_attribute(blobs, &playground_context::archive_source_format)?;

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
    let mut tribles = TribleSet::new();
    tribles += metadata::Describe::describe(attribute, blobs)?;
    let attribute_id = attribute.id();
    tribles += entity! { ExclusiveId::force_ref(&attribute_id) @
        metadata::tag: playground_context::tag_attribute,
    };
    Ok(tribles)
}
