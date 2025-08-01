use std::sync::Arc;

use datafusion::{
    common::{
        internal_datafusion_err, internal_err,
        tree_node::{Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter, TreeNodeVisitor},
    },
    error::Result,
    physical_plan::{ExecutionPlan, ExecutionPlanProperties},
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::{
    remote::{DefaultWorkerAssignment, WorkerAssignment},
    ExecutionStage,
};

use super::{ExecutionTask, WorkerAddr};

/// TaskPlanner creates a new `ExecutionTask` for the given stage and worker addresses.
///
/// This is accomplished by walking down the [`ExecutionStage`] tree, and for each stage
/// we encounter, create a number of tasks equal to the number of partitions divied by
/// `partitions_per_task`.
///
/// [`ExecutionTask::execute`] can be invoked on the resulting task object to return a SendableRecordBatchStream.
pub struct TaskPlanner {
    partitions_per_task: usize,
    /// The physical extension codec used by tasks to encode custom ExecutionPlans
    codec: Option<Arc<dyn PhysicalExtensionCodec>>,
}

impl TaskPlanner {
    /// Creates a new `TaskPlanner` with the given `partitions_per_task` and optional codec.
    pub fn new(partitions_per_task: usize, codec: Option<Arc<dyn PhysicalExtensionCodec>>) -> Self {
        Self {
            partitions_per_task,
            codec,
        }
    }

    /// Createa an [`ExecutionTask`] for the given `ExecutionStage`
    pub fn create_task(&mut self, stage: Arc<ExecutionStage>) -> Result<Arc<dyn ExecutionPlan>> {
        let num_partitions = stage.plan.output_partitioning().partition_count();
        let top_level_tasks = self.tasks_from_stage(stage.clone())?;

        // if there is one top level task, return it, otherwise create a task to consume them
        let task = if top_level_tasks.len() == 1 {
            top_level_tasks[0].clone()
        } else {
            let mut task = ExecutionTask::new(
                0, // unused stage number reserved for this particular case
                stage.plan.clone(),
                &(0..num_partitions).collect::<Vec<_>>(),
                top_level_tasks
                    .iter()
                    .map(|t| t.clone() as Arc<dyn ExecutionPlan>)
                    .collect(),
                self.codec.clone(),
            );
            task.name = format!("TaskPlanner: {}", stage.name());
            Arc::new(task)
        };
        Ok(task)
    }

    fn tasks_from_stage(
        &mut self,
        stage: Arc<ExecutionStage>,
    ) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        let num_partitions = stage.plan.output_partitioning().partition_count();
        let partition_groups = (0..num_partitions)
            .collect::<Vec<_>>()
            .chunks(self.partitions_per_task)
            .map(|chunk| chunk.to_vec())
            .collect::<Vec<_>>();

        println!(
            "TaskPlanner: Stage {} partition groups: {:?}",
            stage.name(),
            partition_groups,
        );

        let child_tasks = stage
            .inputs
            .iter()
            .map(|input| self.tasks_from_stage(input.clone()))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();

        let tasks = partition_groups
            .iter()
            .map(|partition_group| {
                Ok(Arc::new(ExecutionTask::new(
                    stage.num,
                    stage.plan.clone(),
                    partition_group,
                    child_tasks.clone(),
                    self.codec.clone(),
                )) as Arc<dyn ExecutionPlan>)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(tasks)
    }
}
