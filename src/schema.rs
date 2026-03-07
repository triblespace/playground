#[path = "cog_schema.rs"]
mod cog_schema;
#[path = "config_schema.rs"]
mod config_schema;
#[path = "context_schema.rs"]
mod context_schema;
#[path = "exec_schema.rs"]
mod exec_schema;
#[path = "model_chat_schema.rs"]
mod model_chat_schema;
#[path = "workspace_schema.rs"]
mod workspace_schema;

pub use cog_schema::playground_cog;
pub use config_schema::playground_config;
pub use context_schema::playground_context;
pub use exec_schema::playground_exec;
pub use model_chat_schema::model_chat;
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
    metadata += context_schema::build_playground_context_metadata(blobs)?;
    metadata += model_chat_schema::build_model_chat_metadata(blobs)?;
    metadata += workspace_schema::build_playground_workspace_metadata(blobs)?;
    Ok(metadata.into_facts())
}
