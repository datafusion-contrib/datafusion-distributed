use crate::NetworkBoundaryExt;
use crate::distributed_ext::DistributedExt;
use crate::execution_plans::DistributedExec;
use crate::protobuf::StageKey;
use crate::stage::Stage;
use crate::test_utils::in_memory_channel_resolver::InMemoryWorkerResolver;
#[cfg(test)]
use crate::{DistributedConfig, TaskEstimation, TaskEstimator};
#[cfg(test)]
use datafusion::config::ConfigOptions;
use datafusion::{
    common::{HashMap, HashSet},
    execution::{SessionStateBuilder, context::SessionContext},
    physical_plan::{ExecutionPlan, displayable},
    prelude::SessionConfig,
};
#[cfg(test)]
use itertools::Itertools;
use std::sync::Arc;

use super::parquet::register_parquet_tables;

/// count_plan_nodes counts the number of execution plan nodes in a plan using BFS traversal.
/// This does NOT traverse child stages, only the execution plan tree within this stage.
/// Excludes [NetworkBoundary] nodes from the count.
pub fn count_plan_nodes(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut count = 0;
    let mut queue = vec![plan];

    while let Some(plan) = queue.pop() {
        // Skip [NetworkBoundary] nodes from the count.
        if plan.as_ref().is_network_boundary() {
            continue;
        }

        count += 1;

        // Add children to the queue for BFS traversal
        for child in plan.children() {
            queue.push(child);
        }
    }
    count
}

/// Returns
/// - a map of all stages
/// - a set of all the stage keys (one per task)
pub fn get_stages_and_stage_keys(
    stage: &DistributedExec,
) -> (HashMap<usize, &Stage>, HashSet<StageKey>) {
    let mut i = 0;
    let mut queue = find_input_stages(stage);
    let mut stage_keys = HashSet::new();
    let mut stages_map = HashMap::new();

    while i < queue.len() {
        let stage = queue[i];
        stages_map.insert(stage.num, stage);
        i += 1;

        // Add each task.
        for j in 0..stage.tasks.len() {
            stage_keys.insert(StageKey::new(
                stage.query_id.as_bytes().to_vec().into(),
                stage.num as u64,
                j as u64,
            ));
        }

        // Add any child stages
        queue.extend(find_input_stages(stage.plan.decoded().unwrap().as_ref()));
    }
    (stages_map, stage_keys)
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

/// Creates a physical plan from SQL without applying broadcast insertion or distribution.
/// Used for snapshotting the baseline physical plan in tests.
pub async fn sql_to_physical_plan(
    query: &str,
    target_partitions: usize,
    num_workers: usize,
) -> String {
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_information_schema(true);

    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(num_workers))
        .build();

    let ctx = SessionContext::new_with_state(state);
    register_parquet_tables(&ctx).await.unwrap();

    let df = ctx.sql(query).await.unwrap();
    let physical_plan = df.create_physical_plan().await.unwrap();

    format!("{}", displayable(physical_plan.as_ref()).indent(true))
}

#[cfg(test)]
pub(crate) fn base_session_builder(
    target_partitions: usize,
    num_workers: usize,
    broadcast_enabled: bool,
) -> SessionStateBuilder {
    let mut config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_information_schema(true);

    let d_cfg = DistributedConfig {
        broadcast_joins: broadcast_enabled,
        ..Default::default()
    };
    config.set_distributed_option_extension(d_cfg);

    SessionStateBuilder::new()
        .with_default_features()
        .with_config(config)
        .with_distributed_worker_resolver(InMemoryWorkerResolver::new(num_workers))
}

#[cfg(test)]
#[derive(Clone, Copy, Debug)]
pub(crate) struct TestPlanOptions {
    pub(crate) target_partitions: usize,
    pub(crate) num_workers: usize,
    pub(crate) broadcast_enabled: bool,
}

#[cfg(test)]
impl Default for TestPlanOptions {
    fn default() -> Self {
        Self {
            target_partitions: 4,
            num_workers: 4,
            broadcast_enabled: false,
        }
    }
}

#[cfg(test)]
pub(crate) async fn context_with_query(
    builder: SessionStateBuilder,
    query: &str,
) -> (SessionContext, String) {
    let state = builder.build();
    let ctx = SessionContext::new_with_state(state);
    let mut queries = query.split(';').collect_vec();
    let last_query = queries.pop().unwrap();

    for query in queries {
        ctx.sql(query).await.unwrap();
    }

    register_parquet_tables(&ctx).await.unwrap();
    (ctx, last_query.to_string())
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
