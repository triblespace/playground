#[path = "cog_schema.rs"]
mod cog_schema;
#[path = "exec_schema.rs"]
mod exec_schema;
#[path = "openai_responses_schema.rs"]
mod openai_responses_schema;
#[path = "workspace_schema.rs"]
mod workspace_schema;

#[allow(unused_imports)]
pub use cog_schema::playground_cog;
#[allow(unused_imports)]
pub use exec_schema::playground_exec;
#[allow(unused_imports)]
pub use openai_responses_schema::openai_responses;
#[allow(unused_imports)]
pub use workspace_schema::playground_workspace;

pub fn build_playground_metadata<B>(
    blobs: &mut B,
) -> std::result::Result<triblespace::prelude::TribleSet, B::PutError>
where
    B: triblespace::prelude::BlobStore<triblespace::prelude::valueschemas::Blake3>,
{
    let mut metadata = exec_schema::build_playground_exec_metadata(blobs)?;
    metadata += config_schema::build_playground_config_metadata(blobs)?;
    metadata += cog_schema::build_playground_cog_metadata(blobs)?;
    metadata += openai_responses_schema::build_openai_responses_metadata(blobs)?;
    metadata += workspace_schema::build_playground_workspace_metadata(blobs)?;
    Ok(metadata)
}
#[path = "config_schema.rs"]
mod config_schema;
pub use config_schema::playground_config;
