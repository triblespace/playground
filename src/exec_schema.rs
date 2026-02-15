use triblespace::core::blob::schemas::UnknownBlob;
use triblespace::core::metadata;
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{Blake3, GenId, Handle, NsTAIInterval, U256BE};
use triblespace::prelude::*;

pub mod playground_exec {
    use super::*;

    attributes! {
        "AA2F34973589295FA70B538D92CD30F8" as pub kind: GenId;
        "79DD6A1A02E598033EDCE5C667E8E3E6" as pub command_text: Handle<Blake3, LongString>;
        "4A7EA49FD72113D2DC497B407994B4F9" as pub cwd: Handle<Blake3, LongString>;
        "17F4EA6F885F359C4CA967EE8478FA13" as pub stdin: Handle<Blake3, UnknownBlob>;
        "FC48EA2441A1EECAC29C6A2032C09C1E" as pub stdin_text: Handle<Blake3, LongString>;
        "7FFF32386EBB2AE92094B7D88DE2743D" as pub timeout_ms: U256BE;
        "6A968C3FA5667F591D7C41B497CE4559" as pub sandbox_profile: GenId;
        "AAD2627FB70DC16F6ADF8869AE1B203F" as pub requested_at: NsTAIInterval;
        "C4C3870642CAB5F55E7E575B1A62E640" as pub about_request: GenId;
        "28D60463309BCEE8C855A9921CA70669" as pub about_message: GenId;
        "90307D583A8F085828E1007AE432BF86" as pub about_thought: GenId;
        "442A275ABC6834231FC65A4B89773ECD" as pub worker: GenId;
        "B878792F16C0C27C776992FA053A2218" as pub started_at: NsTAIInterval;
        "79474B948670C7D0322C309EB65219F8" as pub attempt: U256BE;
        "B4B81B90EFB4D1F5EE62DDE9CB48025D" as pub finished_at: NsTAIInterval;
        "B68F9025545C7E616EB90C6440220348" as pub exit_code: U256BE;
        "579EA2A82FB6A4D5B1E409D4F7747E2F" as pub stdout: Handle<Blake3, UnknownBlob>;
        "6F1CB839CAE28A34C5107F36EB7939C3" as pub stderr: Handle<Blake3, UnknownBlob>;
        "CA7AF66AAF5105EC15625ED14E1A2AC0" as pub stdout_text: Handle<Blake3, LongString>;
        "BE4D1876B22EAF93AAD1175DB76D1C72" as pub stderr_text: Handle<Blake3, LongString>;
        "26AD99A81ACA4EE8A6C37CE02A4CC53D" as pub duration_ms: U256BE;
        "E9C77284C7DDCF522A8AC4622FE3FB11" as pub error: Handle<Blake3, LongString>;
    }

    /// Root id for describing the playground_exec protocol.
    #[allow(non_upper_case_globals)]
    pub const playground_exec_metadata: Id = id_hex!("94563964DFC622200FAE6E5383D0B4FC");

    /// Tag for command request entities.
    #[allow(non_upper_case_globals)]
    pub const kind_command_request: Id = id_hex!("3D2512DAE86B14B9049930F3146A3188");
    /// Tag for in-progress entities.
    #[allow(non_upper_case_globals)]
    pub const kind_in_progress: Id = id_hex!("2D81A8D840822CF082DE5DE569B53730");
    /// Tag for command result entities.
    #[allow(non_upper_case_globals)]
    pub const kind_command_result: Id = id_hex!("DF7165210F066E84D93E9A430BB0D4BD");

    /// Tag for playground_exec protocol metadata.
    #[allow(non_upper_case_globals)]
    pub const tag_protocol: Id = id_hex!("7DD49A7AA43C616ABB43217609391D1D");
    /// Tag for kind constants in the playground_exec protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_kind: Id = id_hex!("61E122D2379B8F4A577C3B8B65431799");
    /// Tag for attribute constants in the playground_exec protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_attribute: Id = id_hex!("F328F16FB07BA80BBAF06974A739C038");
    /// Tag for tag constants in the playground_exec protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_tag: Id = id_hex!("13425C99A6CBAF48E5B7A1FB8BE72F04");
}

pub fn describe<B>(blobs: &mut B) -> std::result::Result<TribleSet, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let mut tribles = TribleSet::new();

    tribles += entity! { ExclusiveId::force_ref(&playground_exec::playground_exec_metadata) @
        metadata::name: blobs.put("playground_exec_metadata".to_string())?,
        metadata::description: blobs.put(
            "Root id for describing the playground_exec protocol.".to_string(),
        )?,
        metadata::tag: playground_exec::tag_protocol,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_exec::tag_protocol) @
        metadata::name: blobs.put("tag_protocol".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_exec protocol metadata.".to_string(),
        )?,
        metadata::tag: playground_exec::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_exec::tag_kind) @
        metadata::name: blobs.put("tag_kind".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_exec protocol kind constants.".to_string(),
        )?,
        metadata::tag: playground_exec::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_exec::tag_attribute) @
        metadata::name: blobs.put("tag_attribute".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_exec protocol attributes.".to_string(),
        )?,
        metadata::tag: playground_exec::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_exec::tag_tag) @
        metadata::name: blobs.put("tag_tag".to_string())?,
        metadata::description: blobs.put(
            "Tag for playground_exec protocol tag constants.".to_string(),
        )?,
        metadata::tag: playground_exec::tag_tag,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_exec::kind_command_request) @
        metadata::name: blobs.put("kind_command_request".to_string())?,
        metadata::description: blobs.put(
            "Command request entity kind.".to_string(),
        )?,
        metadata::tag: playground_exec::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_exec::kind_in_progress) @
        metadata::name: blobs.put("kind_in_progress".to_string())?,
        metadata::description: blobs.put(
            "Command in-progress entity kind.".to_string(),
        )?,
        metadata::tag: playground_exec::tag_kind,
    };

    tribles += entity! { ExclusiveId::force_ref(&playground_exec::kind_command_result) @
        metadata::name: blobs.put("kind_command_result".to_string())?,
        metadata::description: blobs.put(
            "Command result entity kind.".to_string(),
        )?,
        metadata::tag: playground_exec::tag_kind,
    };

    Ok(tribles)
}

pub fn build_playground_exec_metadata<B>(
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
    metadata.union(<Handle<Blake3, UnknownBlob> as metadata::ConstDescribe>::describe(blobs)?);
    metadata.union(<UnknownBlob as metadata::ConstDescribe>::describe(blobs)?);

    metadata.union(describe_attribute(blobs, &playground_exec::kind)?);
    metadata.union(describe_attribute(blobs, &playground_exec::command_text)?);
    metadata.union(describe_attribute(blobs, &playground_exec::cwd)?);
    metadata.union(describe_attribute(blobs, &playground_exec::stdin)?);
    metadata.union(describe_attribute(blobs, &playground_exec::stdin_text)?);
    metadata.union(describe_attribute(blobs, &playground_exec::timeout_ms)?);
    metadata.union(describe_attribute(
        blobs,
        &playground_exec::sandbox_profile,
    )?);
    metadata.union(describe_attribute(blobs, &playground_exec::requested_at)?);
    metadata.union(describe_attribute(blobs, &playground_exec::about_request)?);
    metadata.union(describe_attribute(blobs, &playground_exec::about_message)?);
    metadata.union(describe_attribute(blobs, &playground_exec::about_thought)?);
    metadata.union(describe_attribute(blobs, &playground_exec::worker)?);
    metadata.union(describe_attribute(blobs, &playground_exec::started_at)?);
    metadata.union(describe_attribute(blobs, &playground_exec::attempt)?);
    metadata.union(describe_attribute(blobs, &playground_exec::finished_at)?);
    metadata.union(describe_attribute(blobs, &playground_exec::exit_code)?);
    metadata.union(describe_attribute(blobs, &playground_exec::stdout)?);
    metadata.union(describe_attribute(blobs, &playground_exec::stderr)?);
    metadata.union(describe_attribute(blobs, &playground_exec::stdout_text)?);
    metadata.union(describe_attribute(blobs, &playground_exec::stderr_text)?);
    metadata.union(describe_attribute(blobs, &playground_exec::duration_ms)?);
    metadata.union(describe_attribute(blobs, &playground_exec::error)?);

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
        metadata::tag: playground_exec::tag_attribute,
    };
    Ok(tribles)
}
