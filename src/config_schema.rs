use triblespace::core::metadata;
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

pub mod playground_config {
    use super::*;

    attributes! {
        "79F990573A9DCC91EF08A5F8CBA7AA25" as pub kind: GenId;
        "DDF83FEC915816ACAE7F3FEBB57E5137" as pub updated_at: NsTAIInterval;
        "950B556A74F71AC7CB008AB23FBB6544" as pub system_prompt: Handle<Blake3, LongString>;
        "DE84D8C836025EA26AC050A27544A332" as pub seed_prompt: Handle<Blake3, LongString>;
        "35E36AE7B60AD946661BD63B3CD64672" as pub branch: Handle<Blake3, LongString>;
        "4E2F9CA7A8456DED8C43A3BE741ADA58" as pub branch_id: GenId;
        "F0F90572249284CD57E48580369DEB6D" as pub author: Handle<Blake3, LongString>;
        "98A194178CFD7CBB915C1BC9EB561A7F" as pub author_role: Handle<Blake3, LongString>;
        "D1DC11B303725409AB8A30C6B59DB2D7" as pub persona_id: GenId;
        "698519DFB681FABC3F06160ACAC9DA8E" as pub poll_ms: U256BE;
        "85BE7BDA465B3CB0F800F76EEF8FAC9B" as pub llm_model: Handle<Blake3, LongString>;
        "B216CFBBF85AA1350B142D510E26268B" as pub llm_base_url: Handle<Blake3, LongString>;
        "55F3FFD721AF7C1258E45BC91CDBF30F" as pub llm_api_key: Handle<Blake3, LongString>;
        "BA4E05799CA2ACDCF3F9350FC8742F2F" as pub llm_reasoning_effort: Handle<Blake3, LongString>;
        "5F04F7A0EB4EBBE6161022B336F83513" as pub llm_stream: U256BE;
        "120F9C6BBB103FAFFB31A66E2ABC15E6" as pub exec_default_cwd: Handle<Blake3, LongString>;
        "D18A351B6E03A460E4F400D97D285F96" as pub exec_sandbox_profile: GenId;
    }

    /// Root id for describing the playground_config protocol.
    #[allow(non_upper_case_globals)]
    pub const playground_config_metadata: Id = id_hex!("F696CB4F22D5EAEE7E42A820F9458A35");

    /// Tag for configuration entries.
    #[allow(non_upper_case_globals)]
    pub const kind_config: Id = id_hex!("A8DCBFD625F386AA7CDFD62A81183E82");

    /// Tag for playground_config protocol metadata.
    #[allow(non_upper_case_globals)]
    pub const tag_protocol: Id = id_hex!("B66C73996CEC00801602A6EF02E31204");
    /// Tag for kind constants in the playground_config protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_kind: Id = id_hex!("A38225D7CE9A623A6B2CA8041E61500C");
    /// Tag for attribute constants in the playground_config protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_attribute: Id = id_hex!("23F4D0F15815FD7883EE80D0E3B41B5D");
    /// Tag for tag constants in the playground_config protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_tag: Id = id_hex!("BB763F3C469D355E6895A4EEA481E554");
}

pub fn describe<B>(blobs: &mut B) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut tribles = TribleSet::new();

    tribles += entity! { ExclusiveId::force_ref(&playground_config::playground_config_metadata) @
        metadata::name: blobs.put("playground_config_metadata".to_string())?,
        metadata::description: blobs.put(
            "Root id for describing the playground_config protocol.".to_string(),
        )?,
        metadata::tag: playground_config::tag_protocol,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_config::tag_protocol) @
        metadata::name: blobs.put("tag_protocol".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_config protocol metadata.".to_string(),
        )?,
        metadata::tag: playground_config::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_config::tag_kind) @
        metadata::name: blobs.put("tag_kind".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_config protocol kind constants.".to_string(),
        )?,
        metadata::tag: playground_config::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_config::tag_attribute) @
        metadata::name: blobs.put("tag_attribute".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_config protocol attributes.".to_string(),
        )?,
        metadata::tag: playground_config::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_config::tag_tag) @
        metadata::name: blobs.put("tag_tag".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_config protocol tag constants.".to_string(),
        )?,
        metadata::tag: playground_config::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_config::kind_config) @
        metadata::name: blobs.put("kind_config".to_string())?,
        metadata::description: blobs.put(
            "Configuration entry entity kind.".to_string(),
        )?,
        metadata::tag: playground_config::tag_kind,
    };

    Ok(tribles)
}

pub fn build_playground_config_metadata<B>(
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

    macro_rules! add_attribute {
        ($attribute:expr, $name:expr) => {
            metadata.union(describe_attribute(blobs, &$attribute, $name)?);
        };
    }

    add_attribute!(playground_config::kind, "config_kind");
    add_attribute!(playground_config::updated_at, "config_updated_at");
    add_attribute!(playground_config::system_prompt, "config_system_prompt");
    add_attribute!(playground_config::seed_prompt, "config_seed_prompt");
    add_attribute!(playground_config::branch, "config_branch");
    add_attribute!(playground_config::branch_id, "config_branch_id");
    add_attribute!(playground_config::author, "config_author");
    add_attribute!(playground_config::author_role, "config_author_role");
    add_attribute!(playground_config::persona_id, "config_persona_id");
    add_attribute!(playground_config::poll_ms, "config_poll_ms");
    add_attribute!(playground_config::llm_model, "config_llm_model");
    add_attribute!(playground_config::llm_base_url, "config_llm_base_url");
    add_attribute!(playground_config::llm_api_key, "config_llm_api_key");
    add_attribute!(
        playground_config::llm_reasoning_effort,
        "config_llm_reasoning_effort"
    );
    add_attribute!(playground_config::llm_stream, "config_llm_stream");
    add_attribute!(
        playground_config::exec_default_cwd,
        "config_exec_default_cwd"
    );
    add_attribute!(
        playground_config::exec_sandbox_profile,
        "config_exec_sandbox_profile"
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

    let attribute_id = metadata::Metadata::id(attribute);
    tribles += entity! { ExclusiveId::force_ref(&attribute_id) @
        metadata::name: (blobs.put(name.to_owned())?),
        metadata::tag: playground_config::tag_attribute,
    };
    Ok(tribles)
}
