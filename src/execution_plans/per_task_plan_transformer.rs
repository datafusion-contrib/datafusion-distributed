use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use datafusion::common::{Result, not_impl_err};
use datafusion::config::{ConfigField, ConfigOptions, Visit};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Transforms a physical plan on a per-task basis at the coordinator before serialization.
///
/// When the coordinator distributes a stage to multiple workers, it normally serializes
/// the same plan bytes for every task. Implementing this trait allows the coordinator to
/// produce a distinct serialized plan per task — for example, by emptying file groups that
/// a given task will not process, reducing unnecessary disk-cache population on workers.
///
/// # Example
///
/// ```rust
/// use std::sync::Arc;
/// use datafusion::physical_plan::ExecutionPlan;
/// use datafusion_distributed::{DistributedExt, PerTaskPlanTransformer};
/// use datafusion::execution::SessionStateBuilder;
///
/// #[derive(Debug)]
/// struct MyTransformer;
///
/// impl PerTaskPlanTransformer for MyTransformer {
///     fn transform_for_task(
///         &self,
///         plan: Arc<dyn ExecutionPlan>,
///         _task_index: usize,
///         _task_count: usize,
///     ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
///         // Return a (possibly modified) plan for this task.
///         Ok(plan)
///     }
/// }
///
/// let _state = SessionStateBuilder::new()
///     .with_distributed_per_task_plan_transformer(MyTransformer)
///     .build();
/// ```
pub trait PerTaskPlanTransformer: Debug + Send + Sync {
    /// Returns a plan specialized for `task_index` out of `task_count` total tasks.
    ///
    /// Called once per task by the coordinator before serializing the plan for that task.
    /// The returned plan replaces the original in the bytes sent to the worker.
    fn transform_for_task(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        task_index: usize,
        task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Wrapper stored in [`DistributedConfig`] that holds an optional [`PerTaskPlanTransformer`].
#[derive(Clone, Default)]
pub(crate) struct PerTaskPlanTransformerExtension(
    pub(crate) Option<Arc<dyn PerTaskPlanTransformer>>,
);

impl PerTaskPlanTransformerExtension {
    pub(crate) fn get(&self) -> Option<&Arc<dyn PerTaskPlanTransformer>> {
        self.0.as_ref()
    }
}

impl ConfigField for PerTaskPlanTransformerExtension {
    fn visit<V: Visit>(&self, _: &mut V, _: &str, _: &'static str) {}

    fn set(&mut self, _: &str, _: &str) -> Result<()> {
        not_impl_err!("PerTaskPlanTransformerExtension cannot be set via string config")
    }
}

impl Debug for PerTaskPlanTransformerExtension {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PerTaskPlanTransformerExtension")
    }
}

/// Reads the registered [`PerTaskPlanTransformer`] from [`ConfigOptions`], if any.
pub(crate) fn get_per_task_plan_transformer(
    cfg: &ConfigOptions,
) -> Option<Arc<dyn PerTaskPlanTransformer>> {
    cfg.extensions
        .get::<DistributedConfig>()?
        .__private_per_task_plan_transformer
        .get()
        .map(Arc::clone)
}

/// Registers a [`PerTaskPlanTransformer`] in the [`SessionConfig`].
pub fn set_distributed_per_task_plan_transformer(
    cfg: &mut SessionConfig,
    transformer: impl PerTaskPlanTransformer + 'static,
) {
    let opts = cfg.options_mut();
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_per_task_plan_transformer =
            PerTaskPlanTransformerExtension(Some(Arc::new(transformer)));
    } else {
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_per_task_plan_transformer: PerTaskPlanTransformerExtension(Some(
                    Arc::new(transformer),
                )),
                ..Default::default()
            },
        )
    }
}

/// Registers a [`PerTaskPlanTransformer`] (already wrapped in [`Arc`]) in the [`SessionConfig`].
pub fn set_distributed_per_task_plan_transformer_arc(
    cfg: &mut SessionConfig,
    transformer: Arc<dyn PerTaskPlanTransformer>,
) {
    let opts = cfg.options_mut();
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_per_task_plan_transformer =
            PerTaskPlanTransformerExtension(Some(transformer));
    } else {
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_per_task_plan_transformer: PerTaskPlanTransformerExtension(Some(
                    transformer,
                )),
                ..Default::default()
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DistributedExt;
    use datafusion::execution::SessionStateBuilder;

    #[derive(Debug)]
    struct NoOpTransformer;

    impl PerTaskPlanTransformer for NoOpTransformer {
        fn transform_for_task(
            &self,
            plan: Arc<dyn ExecutionPlan>,
            _task_index: usize,
            _task_count: usize,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(plan)
        }
    }

    #[test]
    fn test_set_and_get_transformer() {
        let state = SessionStateBuilder::new()
            .with_distributed_per_task_plan_transformer(NoOpTransformer)
            .build();

        let transformer = get_per_task_plan_transformer(state.config().options());
        assert!(transformer.is_some());
    }

    #[test]
    fn test_no_transformer_by_default() {
        let state = SessionStateBuilder::new().build();
        let transformer = get_per_task_plan_transformer(state.config().options());
        assert!(transformer.is_none());
    }
}
