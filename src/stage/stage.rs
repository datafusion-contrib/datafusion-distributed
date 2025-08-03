use std::any::Any;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use itertools::Itertools;
use tokio::sync::RwLock;

use crate::task::ExecutionTask;

/// A unit of isolation for a portion of a physical execution plan
/// that can be executed independently.
///
/// see https://howqueryengineswork.com/13-distributed-query.html
///
#[derive(Debug)]
pub struct ExecutionStage {
    /// Our stage number
    pub num: usize,
    /// Our stage name
    pub name: String,
    /// The physical execution plan that this stage will execute.
    pub plan: Arc<dyn ExecutionPlan>,
    /// The input stages to this stage
    pub inputs: Vec<Arc<dyn ExecutionPlan>>,
    /// Our tasks which tell us how finely grained to execute the partitions in
    /// the plan
    pub tasks: Vec<ExecutionTask>,
    /// An optional codec to assist in serializing and deserializing this stage
    pub codec: Option<Arc<dyn PhysicalExtensionCodec>>,
    /// tree depth of our location in the stage tree, used for display only
    pub(crate) depth: AtomicU64,
}

impl Clone for ExecutionStage {
    /// Creates a shallow clone of this `ExecutionStage`.   The plan, input stages,
    /// and codec are cloned Arcs as we dont need to duplicate the underlying data,
    fn clone(&self) -> Self {
        ExecutionStage {
            num: self.num,
            name: self.name.clone(),
            plan: self.plan.clone(),
            inputs: self.inputs.to_vec(),
            tasks: self.tasks.clone(),
            codec: self.codec.clone(),
            depth: AtomicU64::new(self.depth.load(Ordering::Relaxed)),
        }
    }
}

impl ExecutionStage {
    /// Creates a new `ExecutionStage` with the given plan and inputs.  One task will be created
    /// responsible for partitions in the plan.
    pub fn new(num: usize, plan: Arc<dyn ExecutionPlan>, inputs: Vec<Arc<ExecutionStage>>) -> Self {
        println!(
            "Creating ExecutionStage: {}, with inputs {}",
            num,
            inputs
                .iter()
                .map(|s| format!("{}", s.num))
                .collect::<Vec<_>>()
                .join(", ")
        );

        let name = format!("Stage {:<3}", num);
        let partition_group = (0..plan.properties().partitioning.partition_count())
            .map(|p| p as u64)
            .collect();
        ExecutionStage {
            num,
            name,
            plan,
            inputs: inputs
                .into_iter()
                .map(|s| s as Arc<dyn ExecutionPlan>)
                .collect(),
            tasks: vec![ExecutionTask {
                worker_addr: None,
                partition_group,
            }],
            codec: None,
            depth: AtomicU64::new(0),
        }
    }

    /// Recalculate the tasks for this stage based on the number of partitions in the plan
    /// and the maximum number of partitions per task.
    ///
    /// This will unset any worker assignments
    pub fn with_maximum_partitions_per_task(mut self, max_partitions_per_task: usize) -> Self {
        let partitions = self.plan.properties().partitioning.partition_count();

        self.tasks = (0..partitions)
            .chunks(max_partitions_per_task)
            .into_iter()
            .map(|partition_group| ExecutionTask {
                worker_addr: None,
                partition_group: partition_group
                    .collect::<Vec<_>>()
                    .into_iter()
                    .map(|p| p as u64)
                    .collect(),
            })
            .collect();
        self
    }

    /// Sets the codec for this stage, which is used to serialize and deserialize the plan
    /// and its inputs.
    pub fn with_codec(mut self, codec: Arc<dyn PhysicalExtensionCodec>) -> Self {
        self.codec = Some(codec);
        self
    }

    /// Returns the name of this stage
    pub fn name(&self) -> String {
        format!("Stage {:<3}", self.num)
    }

    /// Returns an iterator over the child stages of this stage cast as &ExecutionStage
    /// which can be useful
    pub fn child_stages_iter(&self) -> impl Iterator<Item = &ExecutionStage> {
        self.inputs
            .iter()
            .filter_map(|s| s.as_any().downcast_ref::<ExecutionStage>())
    }

    /// Returns the name of this stage including child stage numbers if any.
    pub fn name_with_children(&self) -> String {
        let child_str = if self.inputs.is_empty() {
            "".to_string()
        } else {
            format!(
                " Child Stages:[{}] ",
                self.child_stages_iter()
                    .map(|s| format!("{}", s.num))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };
        format!("Stage {:<3}{}", self.num, child_str)
    }

    pub(crate) fn depth(&self) -> usize {
        self.depth.load(Ordering::Relaxed) as usize
    }
}

impl ExecutionPlan for ExecutionStage {
    fn name(&self) -> &str {
        &self.name
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inputs.iter().collect()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(ExecutionStage {
            num: self.num,
            name: self.name.clone(),
            plan: self.plan.clone(),
            inputs: children,
            tasks: self.tasks.clone(),
            codec: self.codec.clone(),
            depth: AtomicU64::new(self.depth.load(Ordering::Relaxed)),
        }))
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.plan.properties()
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::error::Result<datafusion::execution::SendableRecordBatchStream> {
        let stage = self
            .as_any()
            .downcast_ref::<ExecutionStage>()
            .expect("Unwrapping myself should always work");

        // insert the stage into the context so that ExecutionPlan nodes
        // that care about the stage can access it
        let config = context
            .session_config()
            .clone()
            .with_extension(Arc::new(stage.clone()));

        let new_ctx =
            SessionContext::new_with_config_rt(config, context.runtime_env().clone()).task_ctx();

        stage.plan.execute(partition, new_ctx)
    }
}
