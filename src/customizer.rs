use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

#[async_trait::async_trait]
pub trait Customizer: PhysicalExtensionCodec + Send + Sync {
    /// Customize the context before planning a query.
    /// This may include registering new file formats or introducing additional
    /// `PhysicalPlan` operators.
    ///
    /// To support serialization of customized plans for distributed execution,
    /// a `Codec` may also be required.
    async fn customize(&self, ctx: &mut SessionContext) -> Result<(), Box<dyn std::error::Error>>;
}
