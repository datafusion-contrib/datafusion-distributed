use datafusion::{
    common::{HashMap, HashSet},
    physical_plan::ExecutionPlan,
};
use std::sync::Arc;

use crate::NetworkBoundaryExt;
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
