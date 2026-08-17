use crate::TaskKey;
use crate::dynamic_filtering::{
    discover_dynamic_filter_consumers, discover_dynamic_filter_producers,
};
use datafusion::common::{HashMap, HashSet, Result};
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::metrics::Count;
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(super) struct PlannedDynamicFilter {
    // Producer and consumer tasks for a dynamic filter.
    //
    // Note that it is not guaranteed that every task within a stage produces / consumes dynamic filters. For
    // example, a distributed union may prevent a dynamic filter from appearing in all tasks. So, we
    // store task keys rather than stage ids.
    pub(super) producer_tasks: HashSet<TaskKey>,
    pub(super) consumer_tasks: HashSet<TaskKey>,
}

#[derive(Default)]
pub(super) struct DynamicFilterRegistryState {
    pub(super) filters: HashMap<u64, PlannedDynamicFilter>,
}

/// Query-scoped hub for distributed dynamic filtering.
///
/// It stores the locations of dynamic filters and their runtime state. Informs the coordinator
/// - where dynamic filter updates are coming from
/// - how/if dynamic filter updates should be merged
/// - where dynamic filter updates should be forwarded
pub(crate) struct DynamicFilterRegistry {
    pub(super) state: Mutex<DynamicFilterRegistryState>,
    dynamic_filter_updates_received: Count,
}

impl DynamicFilterRegistry {
    pub(crate) fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            state: Mutex::new(DynamicFilterRegistryState::default()),
            dynamic_filter_updates_received: MetricBuilder::new(metrics)
                .global_counter("dynamic_filter_updates_received"),
        }
    }

    pub(crate) fn record_update_received(&self) {
        self.dynamic_filter_updates_received.add(1);
    }

    /// Adds any dynamic filter producers and consumers found in `plan` to the registry.
    pub(crate) fn register_task(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_key: TaskKey,
    ) -> Result<()> {
        let producers = discover_dynamic_filter_producers(plan)?;
        // We can safely ignore anchors because they are not evaluated by network boundaries. This
        // means they do not need updates forwarded to them.
        let consumers = discover_dynamic_filter_consumers(plan)?.consumers;

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        for producer in producers {
            state
                .filters
                .entry(producer.id)
                .or_default()
                .producer_tasks
                .insert(task_key);
        }
        for consumer in consumers {
            state
                .filters
                .entry(consumer.id)
                .or_default()
                .consumer_tasks
                .insert(task_key);
        }
        Ok(())
    }
}
