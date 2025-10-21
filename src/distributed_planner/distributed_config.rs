use datafusion::common::extensions_options;
use datafusion::config::{ConfigExtension, ConfigField, Visit, default_config_transform};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

#[derive(Clone)]
#[allow(clippy::type_complexity)]
pub struct PlanDependantUsize(
    pub(crate) Arc<dyn Fn(&Arc<dyn ExecutionPlan>) -> usize + Send + Sync>,
);

pub trait IntoPlanDependantUsize {
    fn into_plan_dependant_usize(self) -> PlanDependantUsize;
}

impl IntoPlanDependantUsize for usize {
    fn into_plan_dependant_usize(self) -> PlanDependantUsize {
        PlanDependantUsize(Arc::new(move |_| self))
    }
}

impl<T: Fn(&Arc<dyn ExecutionPlan>) -> usize + Send + Sync + 'static> IntoPlanDependantUsize for T {
    fn into_plan_dependant_usize(self) -> PlanDependantUsize {
        PlanDependantUsize(Arc::new(self))
    }
}

impl Default for PlanDependantUsize {
    fn default() -> Self {
        PlanDependantUsize(Arc::new(|_| 0))
    }
}

impl Debug for PlanDependantUsize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlanDependantUsize")
    }
}

impl Display for PlanDependantUsize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlanDependantUsize")
    }
}

impl ConfigField for PlanDependantUsize {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description);
    }

    fn set(&mut self, _: &str, value: &str) -> datafusion::common::Result<()> {
        *self = default_config_transform::<usize>(value)?.into_plan_dependant_usize();
        Ok(())
    }
}

extensions_options! {
    pub struct DistributedConfig {
        /// Upon shuffling data, this defines how many tasks are employed into performing the shuffling.
        /// ```text
        ///  ( task 1 )  ( task 2 ) ( task 3 )
        ///      ▲           ▲          ▲
        ///      └────┬──────┴─────┬────┘
        ///       ( task 1 )  ( task 2 )       N tasks
        /// ```
        /// This parameter defines N
        pub network_shuffle_tasks: Option<PlanDependantUsize>, default = None
        /// Upon merging multiple tasks into one, this defines how many tasks are merged.
        /// ```text
        ///              ( task 1 )
        ///                  ▲
        ///      ┌───────────┴──────────┐
        ///  ( task 1 )  ( task 2 ) ( task 3 )  N tasks
        /// ```
        /// This parameter defines N
        pub network_coalesce_tasks: Option<PlanDependantUsize>, default = None
    }
}

impl ConfigExtension for DistributedConfig {
    const PREFIX: &'static str = "distributed";
}

impl DistributedConfig {
    /// Sets the amount of tasks used in a network shuffle operation.
    pub fn with_network_shuffle_tasks(mut self, tasks: impl IntoPlanDependantUsize) -> Self {
        self.network_shuffle_tasks = Some(tasks.into_plan_dependant_usize());
        self
    }

    /// Sets the amount of tasks used in a network coalesce operation.
    pub fn with_network_coalesce_tasks(mut self, tasks: impl IntoPlanDependantUsize) -> Self {
        self.network_coalesce_tasks = Some(tasks.into_plan_dependant_usize());
        self
    }
}

pub(crate) fn set_distributed_network_coalesce_tasks(
    cfg: &mut SessionConfig,
    tasks: impl IntoPlanDependantUsize,
) {
    let ext = &mut cfg.options_mut().extensions;
    let Some(prev) = ext.get_mut::<DistributedConfig>() else {
        return ext.insert(DistributedConfig::default().with_network_coalesce_tasks(tasks));
    };
    prev.network_coalesce_tasks = Some(tasks.into_plan_dependant_usize());
}

pub(crate) fn set_distributed_network_shuffle_tasks(
    cfg: &mut SessionConfig,
    tasks: impl IntoPlanDependantUsize,
) {
    let ext = &mut cfg.options_mut().extensions;
    let Some(prev) = ext.get_mut::<DistributedConfig>() else {
        return ext.insert(DistributedConfig::default().with_network_shuffle_tasks(tasks));
    };
    prev.network_shuffle_tasks = Some(tasks.into_plan_dependant_usize());
}
