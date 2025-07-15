use datafusion::prelude::SessionContext;

pub trait CtxCustomizer {
    /// Customize the context before planning a a query.
    fn customize(&self, ctx: &mut SessionContext) -> Result<(), Box<dyn std::error::Error>>;
}
