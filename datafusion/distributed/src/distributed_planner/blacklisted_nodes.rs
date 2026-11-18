use datafusion::common::HashSet;
use datafusion::common::Result;
use datafusion::common::tree_node::TreeNode;
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
use std::any::TypeId;
use std::sync::Arc;

/// [SessionConfig] extension holding the [ExecutionPlan] types that prevent a plan from being
/// distributed.
#[derive(Default, Clone)]
struct BlacklistedNodes(HashSet<TypeId>);

pub(crate) fn add_distributed_blacklisted_node<T: ExecutionPlan>(cfg: &mut SessionConfig) {
    let mut blacklisted = cfg
        .get_extension::<BlacklistedNodes>()
        .map(|v| v.as_ref().clone())
        .unwrap_or_default();
    blacklisted.0.insert(TypeId::of::<T>());
    cfg.set_extension(Arc::new(blacklisted));
}

/// Whether `plan` contains any node blacklisted in `cfg`.
pub(crate) fn contains_blacklisted_node(
    plan: &Arc<dyn ExecutionPlan>,
    cfg: &SessionConfig,
) -> Result<bool> {
    let Some(blacklisted) = cfg.get_extension::<BlacklistedNodes>() else {
        return Ok(false);
    };
    // `as_ref()` is needed: calling `type_id()` on the `Arc` returns the Arc's own type id.
    plan.exists(|node| {
        let type_id = match node.downcast_delegate() {
            Some(delegate) => delegate.type_id(),
            None => node.as_ref().type_id(),
        };
        Ok(blacklisted.0.contains(&type_id))
    })
}
