use std::sync::Arc;

use datafusion::{
    common::{HashMap, HashSet},
    physical_plan::ExecutionPlan,
};

use crate::{
    StageExec,
    execution_plans::{NetworkCoalesceExec, NetworkShuffleExec},
    protobuf::StageKey,
};

/// count_plan_nodes counts the number of execution plan nodes in a plan using BFS traversal.
/// This does NOT traverse child stages, only the execution plan tree within this stage.
/// Excludes [NetworkBoundary] nodes from the count.
pub fn count_plan_nodes(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut count = 0;
    let mut queue = vec![plan];

    while let Some(plan) = queue.pop() {
        // Skip [NetworkBoundary] nodes from the count.
        if !plan.as_any().is::<NetworkCoalesceExec>() && !plan.as_any().is::<NetworkShuffleExec>() {
            count += 1;
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
/// - a set of all the stage keys (one per task)
pub fn get_stages_and_stage_keys(
    stage: &StageExec,
) -> (HashMap<usize, &StageExec>, HashSet<StageKey>) {
    let query_id = stage.query_id;
    let mut i = 0;
    let mut queue = vec![stage];
    let mut stage_keys = HashSet::new();
    let mut stages_map = HashMap::new();

    while i < queue.len() {
        let stage = queue[i];
        stages_map.insert(stage.num, stage);
        i += 1;

        // Add each task.
        for j in 0..stage.tasks.len() {
            let stage_key = StageKey {
                query_id: query_id.to_string(),
                stage_id: stage.num as u64,
                task_number: j as u64,
            };
            stage_keys.insert(stage_key);
        }

        // Add any child stages
        queue.extend(stage.child_stages_iter());
    }
    (stages_map, stage_keys)
}
