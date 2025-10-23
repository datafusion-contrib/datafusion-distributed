use datafusion::common::extensions_options;
use datafusion::config::ConfigExtension;
use datafusion::prelude::SessionConfig;
use std::fmt::Debug;

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
        pub network_shuffle_tasks: Option<usize>, default = None
        /// Upon merging multiple tasks into one, this defines how many tasks are merged.
        /// ```text
        ///              ( task 1 )
        ///                  ▲
        ///      ┌───────────┴──────────┐
        ///  ( task 1 )  ( task 2 ) ( task 3 )  N tasks
        /// ```
        /// This parameter defines N
        pub network_coalesce_tasks: Option<usize>, default = None
    }
}

impl ConfigExtension for DistributedConfig {
    const PREFIX: &'static str = "distributed";
}

impl DistributedConfig {
    /// Sets the amount of tasks used in a network shuffle operation.
    pub fn with_network_shuffle_tasks(mut self, tasks: usize) -> Self {
        self.network_shuffle_tasks = Some(tasks);
        self
    }

    /// Sets the amount of tasks used in a network coalesce operation.
    pub fn with_network_coalesce_tasks(mut self, tasks: usize) -> Self {
        self.network_coalesce_tasks = Some(tasks);
        self
    }
}

pub(crate) fn set_distributed_network_coalesce_tasks(cfg: &mut SessionConfig, tasks: usize) {
    let ext = &mut cfg.options_mut().extensions;
    let Some(prev) = ext.get_mut::<DistributedConfig>() else {
        return ext.insert(DistributedConfig::default().with_network_coalesce_tasks(tasks));
    };
    prev.network_coalesce_tasks = Some(tasks);
}

pub(crate) fn set_distributed_network_shuffle_tasks(cfg: &mut SessionConfig, tasks: usize) {
    let ext = &mut cfg.options_mut().extensions;
    let Some(prev) = ext.get_mut::<DistributedConfig>() else {
        return ext.insert(DistributedConfig::default().with_network_shuffle_tasks(tasks));
    };
    prev.network_shuffle_tasks = Some(tasks);
}
