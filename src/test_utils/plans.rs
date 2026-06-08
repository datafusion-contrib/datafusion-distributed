#[cfg(test)]
use super::parquet::register_parquet_tables;
use crate::NetworkBoundaryExt;
use crate::common::serialize_uuid;
use crate::coordinator::DistributedExec;
use crate::stage::Stage;
use crate::worker::generated::worker::TaskKey;
#[cfg(test)]
use crate::{DistributedConfig, DistributedExt, SessionStateBuilderExt, TaskEstimation, TaskEstimator, display_plan_ascii, test_utils::in_memory_channel_resolver::InMemoryWorkerResolver};
use datafusion::{
    common::{HashMap, HashSet},
    physical_plan::ExecutionPlan,
};
#[cfg(test)]
use datafusion::{
    config::ConfigOptions,
    execution::{SessionState, context::SessionContext, session_state::SessionStateBuilder},
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
    pub async fn physical_plan(&self, query: &str) -> Arc<dyn ExecutionPlan> {
        let mut queries = query.split(';').collect_vec();
        let last_query = queries.pop().unwrap();
        for query in queries {
            self.ctx.sql(query).await.unwrap();
        }
        // registration must run here bc some `SET datafusion.execution.target_partitions=2` query
        // dont take effect on parquet after registration
        register_parquet_tables(&self.ctx).await.unwrap();
        let df = self.ctx.sql(last_query).await.unwrap();
        df.create_physical_plan().await.unwrap()
    }

    pub async fn physical_plan_as_string(&self, query: &str) -> String {
        let plan = self.physical_plan(query).await;
        displayable(plan.as_ref()).indent(true).to_string()
    }

    pub async fn physical_plan_as_ascii(&self, query: &str, show_metrics: bool) -> String {
        display_plan_ascii(self.physical_plan(query).await.as_ref(), show_metrics)
    }

    pub fn get_ctx(&self) -> &SessionContext {
        &self.ctx
    }
}

#[cfg(test)]
pub(crate) struct TestPlanBuilder {
    target_partitions: Option<usize>,
    num_workers: Option<usize>,
    distributed_planner: bool,
    distributed_cardinality_effect_task_scale_factor: Option<f64>,
    distributed_files_per_task: Option<usize>,
    information_schema: Option<bool>,
    broadcast_joins: bool,
    distributed_task_estimator: Option<Arc<dyn TaskEstimator + Send + Sync + 'static>>,
    distributed_partial_reduce: Option<bool>,
}

#[cfg(test)]
impl TestPlanBuilder {
    pub fn new() -> Self {
        Self { 
            target_partitions: None,
            num_workers: None,
            distributed_planner: false,
            distributed_cardinality_effect_task_scale_factor: None,
            distributed_files_per_task: None,
            information_schema: None,
            broadcast_joins: false,
            distributed_task_estimator: None,
            distributed_partial_reduce: None
        } 
    }

    pub fn target_partitions(mut self, target_partitions: usize) -> Self {
        self.target_partitions = Some(target_partitions);
        self
    }

    pub fn num_workers(mut self, num_workers: usize) -> Self {
        self.num_workers = Some(num_workers);
        self
    }

    pub fn distributed_planner(mut self) -> Self {
        self.distributed_planner = true;
        self
    }

    pub fn distributed_cardinality_effect_task_scale_factor(mut self, factor: f64) -> Self {
        self.distributed_cardinality_effect_task_scale_factor = Some(factor);
        self
    }

    pub fn distributed_files_per_task(mut self, files_per_task: usize) -> Self {
        self.distributed_files_per_task = Some(files_per_task);
        self
    }

    pub fn information_schema(mut self, enabled: bool) -> Self {
        self.information_schema = Some(enabled);
        self
    }

    pub fn broadcast_joins(mut self, enabled: bool) -> Self {
        self.broadcast_joins = enabled;
        self
    }

    pub fn distributed_task_estimator(
        mut self, 
        task_estimator: impl TaskEstimator + Send + Sync + 'static,
    ) -> Self {
        self.distributed_task_estimator = Some(Arc::new(task_estimator));
        self
    }

    pub fn distributed_partial_reduce(mut self, enabled: bool) -> Self {
        self.distributed_partial_reduce = Some(enabled);
        self
    }

    fn build_config(&self) -> SessionConfig {
        // distributed config
        let mut d_cfg = DistributedConfig::default();
        d_cfg.broadcast_joins = self.broadcast_joins;
        // config block
        let mut config = SessionConfig::new();
        config.set_distributed_option_extension(d_cfg);
        if let Some(n) = self.target_partitions {
            config = config.with_target_partitions(n);
        }
        if let Some(n) = self.distributed_files_per_task {
            config = config.with_distributed_files_per_task(n)
                .expect("`distributed_files_per_task` expects a distributed config");
        }
        if let Some(enabled) = self.information_schema {
            config = config.with_information_schema(enabled);
        }
        config
    }

    fn build_state(&self, config: SessionConfig) -> SessionState {
        let mut state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config);
        if let Some(n) = self.num_workers {
            state = state.with_distributed_worker_resolver(InMemoryWorkerResolver::new(n));
        }
        if self.distributed_planner {
            state = state.with_distributed_planner();
        }
        if let Some(f) = self.distributed_cardinality_effect_task_scale_factor {
            state = state.with_distributed_cardinality_effect_task_scale_factor(f)
                .expect("Error setting `distributed_cardinality_effect_task_scale_factor` in `build`");
        }
        if let Some(t) = self.distributed_task_estimator.clone() {
            state = state.with_distributed_task_estimator(t);
        }
        if let Some(enabled) = self.distributed_partial_reduce {
            state = state.with_distributed_partial_reduce(enabled)
                .unwrap()
        }
        state.build()
    }

    pub fn build(&self) -> TestPlan {
        let config = self.build_config();
        let state = self.build_state(config);
        TestPlan { 
            ctx: SessionContext::new_with_state(state)
        }
    }

}

#[cfg(test)]
impl Default for TestPlanBuilder {
    fn default() -> Self {
        Self { 
            target_partitions: Some(4),
            num_workers: Some(3),
            distributed_planner: false,
            distributed_cardinality_effect_task_scale_factor: None,
            distributed_files_per_task: None,
            information_schema: Some(false),
            broadcast_joins: false,
            distributed_task_estimator: None,
            distributed_partial_reduce: None
        }
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
