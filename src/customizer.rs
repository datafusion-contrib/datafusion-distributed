use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

#[async_trait::async_trait]
pub trait Customizer: PhysicalExtensionCodec + Send + Sync {
    /// Customize the context before planning a a query.
    async fn customize(&self, ctx: &mut SessionContext) -> Result<(), Box<dyn std::error::Error>>;
}
