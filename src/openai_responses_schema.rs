use triblespace::core::metadata;
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{
    Blake3, GenId, Handle, NsTAIInterval, ShortString, U256BE,
};
use triblespace::prelude::*;

pub mod openai_responses {
    use super::*;

    attributes! {
        "5F10520477A04E5FB322C85CC78C6762" as pub kind: GenId;
        "5A14A02113CE43A59881D0717726F465" as pub about_request: GenId;
        "DA8E31E47919337B3E00724EBE32D14E" as pub about_thought: GenId;
        "C1FFE9D4FEC549C09C96639665561DFE" as pub model: ShortString;
        "B6BF5BEE9961D6C0F4F825088DD2C3F2" as pub prompt: Handle<Blake3, LongString>;
        "0DA5DD275AA34F86B0297CC35F1B7395" as pub requested_at: NsTAIInterval;
        "430B9CD43A3BC414E730B29BCFD6349B" as pub request_raw: Handle<Blake3, LongString>;
        "4FC561A8EC8E9D750445AE8A0BE5E094" as pub worker: GenId;
        "1DE7C6BCE0223199368070A82EA23A7E" as pub started_at: NsTAIInterval;
        "8CAEF4617646F8C9E90BC9A3ED3D0496" as pub attempt: U256BE;
        "238CF718317A94DB46B8D75E7CB6D609" as pub finished_at: NsTAIInterval;
        "B1B904590F0FA70AD1BA247F3D23A6CC" as pub output_text: Handle<Blake3, LongString>;
        "E41A91D2C68640AA86AB31A2CAB2858F" as pub response_raw: Handle<Blake3, LongString>;
        "BD1635514288254E9CB0448CC07C8B65" as pub response_json_root: GenId;
        "9E9B829C473E416E9150D4B94A6A2DC4" as pub error: Handle<Blake3, LongString>;
    }

    /// Root id for describing the openai_responses protocol.
    #[allow(non_upper_case_globals)]
    pub const openai_responses_metadata: Id = id_hex!("E714890E7F711B393B6249A3E7198B89");

    /// Tag for response request entities.
    #[allow(non_upper_case_globals)]
    pub const kind_request: Id = id_hex!("1524B4C030D4F10365D9DCEE801A09C8");
    /// Tag for in-progress entities.
    #[allow(non_upper_case_globals)]
    pub const kind_in_progress: Id = id_hex!("16C69FC4928D54BF93E6F3222B4685A7");
    /// Tag for response result entities.
    #[allow(non_upper_case_globals)]
    pub const kind_result: Id = id_hex!("DE498E4697F9F01219C75E7BC183DB91");

    /// Tag for openai_responses protocol metadata.
    #[allow(non_upper_case_globals)]
    pub const tag_protocol: Id = id_hex!("4E2AFB139125A2294B4D464C150D48FC");
    /// Tag for kind constants in the openai_responses protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_kind: Id = id_hex!("3E8E162D4BD697DE01083D0E529F49C1");
    /// Tag for attribute constants in the openai_responses protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_attribute: Id = id_hex!("6A2166D684C571DC18769CAC44260A4D");
    /// Tag for tag constants in the openai_responses protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_tag: Id = id_hex!("737CB4E3D88A2942C2725F978D620135");
}

pub fn describe<B>(blobs: &mut B) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut tribles = TribleSet::new();

    tribles += entity! { ExclusiveId::force_ref(&openai_responses::openai_responses_metadata) @
        metadata::name: blobs.put("openai_responses_metadata".to_string())?,
        metadata::description: blobs.put(
            "Root id for describing the openai_responses protocol.".to_string(),
        )?,
        metadata::tag: openai_responses::tag_protocol,
    };

    tribles += entity! { ExclusiveId::force_ref(&openai_responses::tag_protocol) @
        metadata::name: blobs.put("tag_protocol".to_string())?,
        metadata::description: blobs.put(
            "Tag for openai_responses protocol metadata.".to_string(),
        )?,
        metadata::tag: openai_responses::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&openai_responses::tag_kind) @
        metadata::name: blobs.put("tag_kind".to_string())?,
        metadata::description: blobs.put(
            "Tag for openai_responses protocol kind constants.".to_string(),
        )?,
        metadata::tag: openai_responses::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&openai_responses::tag_attribute) @
        metadata::name: blobs.put("tag_attribute".to_string())?,
        metadata::description: blobs.put(
            "Tag for openai_responses protocol attributes.".to_string(),
        )?,
        metadata::tag: openai_responses::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&openai_responses::tag_tag) @
        metadata::name: blobs.put("tag_tag".to_string())?,
        metadata::description: blobs.put(
            "Tag for openai_responses protocol tag constants.".to_string(),
        )?,
        metadata::tag: openai_responses::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&openai_responses::kind_request) @
        metadata::name: blobs.put("kind_request".to_string())?,
        metadata::description: blobs.put(
            "Response request entity kind.".to_string(),
        )?,
        metadata::tag: openai_responses::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&openai_responses::kind_in_progress) @
        metadata::name: blobs.put("kind_in_progress".to_string())?,
        metadata::description: blobs.put(
            "Response in-progress entity kind.".to_string(),
        )?,
        metadata::tag: openai_responses::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&openai_responses::kind_result) @
        metadata::name: blobs.put("kind_result".to_string())?,
        metadata::description: blobs.put(
            "Response result entity kind.".to_string(),
        )?,
        metadata::tag: openai_responses::tag_kind,
    };

    Ok(tribles)
}

pub fn build_openai_responses_metadata<B>(
    blobs: &mut B,
) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut metadata = describe(blobs)?;

    metadata.union(<GenId as metadata::ConstMetadata>::describe(blobs)?);
    metadata.union(<NsTAIInterval as metadata::ConstMetadata>::describe(blobs)?);
    metadata.union(<U256BE as metadata::ConstMetadata>::describe(blobs)?);
    metadata.union(<ShortString as metadata::ConstMetadata>::describe(blobs)?);
    metadata.union(<Handle<Blake3, LongString> as metadata::ConstMetadata>::describe(blobs)?);

    metadata.union(describe_attribute(blobs, &openai_responses::kind)?);
    metadata.union(describe_attribute(blobs, &openai_responses::about_request)?);
    metadata.union(describe_attribute(blobs, &openai_responses::about_thought)?);
    metadata.union(describe_attribute(blobs, &openai_responses::model)?);
    metadata.union(describe_attribute(blobs, &openai_responses::prompt)?);
    metadata.union(describe_attribute(blobs, &openai_responses::requested_at)?);
    metadata.union(describe_attribute(blobs, &openai_responses::request_raw)?);
    metadata.union(describe_attribute(blobs, &openai_responses::worker)?);
    metadata.union(describe_attribute(blobs, &openai_responses::started_at)?);
    metadata.union(describe_attribute(blobs, &openai_responses::attempt)?);
    metadata.union(describe_attribute(blobs, &openai_responses::finished_at)?);
    metadata.union(describe_attribute(blobs, &openai_responses::output_text)?);
    metadata.union(describe_attribute(blobs, &openai_responses::response_raw)?);
    metadata.union(describe_attribute(blobs, &openai_responses::response_json_root)?);
    metadata.union(describe_attribute(blobs, &openai_responses::error)?);

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
    let mut tribles = metadata::Metadata::describe(attribute, blobs)?;

    let attribute_id = metadata::Metadata::id(attribute);
    tribles += entity! { ExclusiveId::force_ref(&attribute_id) @
        metadata::tag: openai_responses::tag_attribute,
    };
    Ok(tribles)
}
