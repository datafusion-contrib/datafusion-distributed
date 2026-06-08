#[cfg(test)]
use super::parquet::register_parquet_tables;
use crate::NetworkBoundaryExt;
use crate::common::serialize_uuid;
use crate::coordinator::DistributedExec;
use crate::stage::Stage;
use crate::worker::generated::worker::TaskKey;
#[cfg(test)]
use crate::{DistributedConfig, DistributedExt, TaskEstimation, TaskEstimator};
use datafusion::{
    common::{HashMap, HashSet},
    physical_plan::ExecutionPlan,
};
#[cfg(test)]
use datafusion::{
    config::ConfigOptions,
    execution::{context::SessionContext, session_state::SessionStateBuilder},
    physical_plan::displayable,
    prelude::SessionConfig,
};
#[cfg(test)]
use itertools::Itertools;
use std::sync::Arc;

/// count_plan_nodes counts the number of execution plan nodes in a plan using BFS traversal.
/// This does NOT traverse child stages, only the execution plan tree within this stage.
/// Network boundary nodes are counted but their children (which belong to child stages) are not traversed.
pub fn count_plan_nodes_up_to_network_boundary(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut count = 0;
    let mut queue = vec![plan];

    while let Some(plan) = queue.pop() {
        // Include the network boundary in the count.
        count += 1;

        // Stop at network boundaries - don't traverse into child stages
        if plan.as_ref().is_network_boundary() {
            continue;
        }

        // Add children to the queue for BFS traversal
        for child in plan.children() {
            queue.push(child);
        }
    }
    count
}

/// Returns
/// - a map of all stages
/// - a set of all the task keys (one per task)
pub fn get_stages_and_task_keys(
    stage: &DistributedExec,
) -> (HashMap<usize, &Stage>, HashSet<TaskKey>) {
    let mut i = 0;
    let mut queue = find_input_stages(stage);
    let mut task_keys = HashSet::new();
    let mut stages_map = HashMap::new();

    while i < queue.len() {
        let stage = queue[i];
        stages_map.insert(stage.num(), stage);
        i += 1;

        // Add each task.
        for j in 0..stage.task_count() {
            task_keys.insert(TaskKey {
                query_id: serialize_uuid(&stage.query_id()),
                stage_id: stage.num() as u64,
                task_number: j as u64,
            });
        }

        // Add any child stages
        queue.extend(find_input_stages(stage.local_plan().unwrap().as_ref()));
    }
    (stages_map, task_keys)
}

fn find_input_stages(plan: &dyn ExecutionPlan) -> Vec<&Stage> {
    let mut result = vec![];
    for child in plan.children() {
        if let Some(plan) = child.as_network_boundary() {
            result.push(plan.input_stage());
        } else {
            result.extend(find_input_stages(child.as_ref()));
        }
    }
    result
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct TestPlan {
    ctx: SessionContext,
}

#[cfg(test)]
impl TestPlan {
    pub async fn physical_plan(&self, query: &String) -> Arc<dyn ExecutionPlan> {
        //dbg!(&self.ctx.state().config_options().execution);
        let mut queries = query.split(';').collect_vec();
        let last_query = queries.pop().unwrap();
        for query in queries {
            self.ctx.sql(query).await.unwrap();
        }
        register_parquet_tables(&self.ctx).await.unwrap();
        let df = self.ctx.sql(last_query).await.unwrap();
        //dbg!(&self.ctx.state().config_options().execution);
        df.create_physical_plan().await.unwrap()
    }

    pub fn plan_to_string(plan: Arc<dyn ExecutionPlan>) -> String {
        displayable(plan.as_ref()).indent(true).to_string()
    }

    pub fn get_ctx(&self) -> &SessionContext {
        &self.ctx
    }
}

#[cfg(test)]
pub(crate) struct TestPlanBuilder {
    config_closures: Vec<Box<dyn FnOnce(SessionConfig) -> SessionConfig + 'static>>,
    state_closures: Vec<Box<dyn FnOnce(SessionStateBuilder) -> SessionStateBuilder + 'static>>,
}

#[cfg(test)]
impl TestPlanBuilder {
    pub fn new() -> Self {
        Self {
            config_closures: Vec::new(),
            state_closures: Vec::new(),
        }
    }

    pub fn add_config(mut self, f: impl FnOnce(SessionConfig) -> SessionConfig + 'static) -> Self {
        self.config_closures.push(Box::new(f));
        self
    }

    pub fn add_state(
        mut self,
        f: impl FnOnce(SessionStateBuilder) -> SessionStateBuilder + 'static,
    ) -> Self {
        self.state_closures.push(Box::new(f));
        self
    }

    pub fn with_broadcast_enabled(mut self, enabled: bool) -> Self {
        let state_closure = move |mut b: SessionStateBuilder| {
            b.set_distributed_option_extension(DistributedConfig {
                broadcast_joins: enabled,
                ..Default::default()
            });
            b
        };
        self.state_closures.push(Box::new(state_closure));
        self
    }

    pub async fn build(self) -> TestPlan {
        let mut config = SessionConfig::new();
        for config_closure in self.config_closures {
            config = config_closure(config);
        }
        let mut state = SessionStateBuilder::new().with_config(config);
        for state_closure in self.state_closures {
            state = state_closure(state);
        }
        let ctx = SessionContext::new_with_state(state.build());
        TestPlan { ctx }
    }
}

#[cfg(test)]
#[derive(Debug)]
pub(crate) struct BuildSideOneTaskEstimator;

#[cfg(test)]
impl TaskEstimator for BuildSideOneTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        if !plan.children().is_empty() {
            return None;
        }
        let schema = plan.schema();
        let has_min_temp = schema.fields().iter().any(|f| f.name() == "MinTemp");
        let has_max_temp = schema.fields().iter().any(|f| f.name() == "MaxTemp");
        if has_min_temp && !has_max_temp {
            Some(TaskEstimation::maximum(1))
        } else {
            None
        }
    }

    fn scale_up_leaf_node(
        &self,
        _: &Arc<dyn ExecutionPlan>,
        _: usize,
        _: &ConfigOptions,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        None
    }
}
