use triblespace::core::metadata;
use triblespace::macros::id_hex;
use triblespace::prelude::blobschemas::LongString;
use triblespace::prelude::valueschemas::{
    Blake3, GenId, Handle, NsTAIInterval,
};
use triblespace::prelude::*;

pub mod playground_context {
    use super::*;

    attributes! {
        "81E520987033BE71EB0AFFA8297DE613" as pub kind: GenId;
        "3292CF0B3B6077991D8ECE6E2973D4B6" as pub summary: Handle<Blake3, LongString>;
        "3D5865566AF5118471DA1FF7F87CB791" as pub created_at: NsTAIInterval;
        "4EAF7FE3122A0AE2D8309B79DCCB8D75" as pub start_at: NsTAIInterval;
        "95D629052C40FA09B378DDC507BEA0D3" as pub end_at: NsTAIInterval;
        "CB97C36A32DEC70E0D1149E7C5D88588" as pub left: GenId;
        "087D07E3D9D94F0C4E96813C7BC5E74C" as pub right: GenId;
        "9B83D68AECD6888AA9CE95E754494768" as pub child: GenId;
        "316834CC6B0EA6F073BF5362D67AC530" as pub about_exec_result: GenId;
        "A4E2B712CA28AB1EED76C34DE72AFA39" as pub about_archive_message: GenId;
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
    /// Tag for tag constants in the playground_context protocol.
    #[allow(non_upper_case_globals)]
    pub const tag_tag: Id = id_hex!("3BA5BD0CEAB802DDE13FBA7B983B4C1A");
}

pub fn build_playground_context_metadata<B>(
    blobs: &mut B,
) -> std::result::Result<Fragment, B::PutError>
where
    B: BlobStore<Blake3>,
{
    let attrs = playground_context::describe(blobs)?;

    let mut protocol = entity! { ExclusiveId::force_ref(&playground_context::playground_context_metadata) @
        metadata::name: blobs.put("playground_context".to_string())?,
        metadata::description: blobs.put("Playground context protocol.".to_string())?,
        metadata::tag: playground_context::tag_protocol,
        metadata::attribute*: attrs.exports(),
    };
    protocol += attrs.into_facts();

    protocol += <GenId as metadata::ConstDescribe>::describe(blobs)?;
    protocol += <NsTAIInterval as metadata::ConstDescribe>::describe(blobs)?;
    protocol += <Handle<Blake3, LongString> as metadata::ConstDescribe>::describe(blobs)?;

    protocol += entity! { ExclusiveId::force_ref(&playground_context::tag_protocol) @
        metadata::name: blobs.put("tag_protocol".to_string())?,
        metadata::tag: playground_context::tag_tag,
    };
    protocol += entity! { ExclusiveId::force_ref(&playground_context::tag_kind) @
        metadata::name: blobs.put("tag_kind".to_string())?,
        metadata::tag: playground_context::tag_tag,
    };
    protocol += entity! { ExclusiveId::force_ref(&playground_context::tag_tag) @
        metadata::name: blobs.put("tag_tag".to_string())?,
        metadata::tag: playground_context::tag_tag,
    };

    protocol += entity! { ExclusiveId::force_ref(&playground_context::kind_chunk) @
        metadata::name: blobs.put("kind_chunk".to_string())?,
        metadata::description: blobs.put("Context chunk entity kind.".to_string())?,
        metadata::tag: playground_context::tag_kind,
    };

    Ok(protocol)
}
