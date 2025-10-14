use bytes::Bytes;
use datafusion::{
    common::{HashMap, HashSet},
    physical_plan::ExecutionPlan,
};
use std::sync::Arc;

use crate::distributed_physical_optimizer_rule::NetworkBoundaryExt;
use crate::execution_plans::DistributedExec;
use crate::protobuf::StageKey;
use crate::stage::Stage;

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
            let stage_key = StageKey {
                query_id: Bytes::from(stage.query_id.as_bytes().to_vec()),
                stage_id: stage.num as u64,
                task_number: j as u64,
            };
            stage_keys.insert(stage_key);
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
            if let Some(stage) = plan.input_stage() {
                result.push(stage);
            }
        } else {
            result.extend(find_input_stages(child.as_ref()));
        }
    }
    result
}
