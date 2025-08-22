use std::sync::Arc;

use datafusion::common::{exec_err, internal_err};
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use itertools::Itertools;
use rand::Rng;
use url::Url;
use uuid::Uuid;
use crate::channel_manager_ext::get_channel_resolver;
use crate::ChannelResolver;
use crate::task::ExecutionTask;

/// A unit of isolation for a portion of a physical execution plan
/// that can be executed independently and across a network boundary.  
/// It implements [`ExecutionPlan`] and can be executed to produce a
/// stream of record batches.
///
/// An ExecutionTask is a finer grained unit of work compared to an ExecutionStage.
/// One ExecutionStage will create one or more ExecutionTasks
///
/// When an [`ExecutionStage`] is execute()'d if will execute its plan and return a stream
/// of record batches.
///
/// If the stage has input stages, then it those input stages will be executed on remote resources
/// and will be provided the remainder of the stage tree.
///
/// For example if our stage tree looks like this:
///
/// ```text
///                       ┌─────────┐
///                       │ stage 1 │
///                       └───┬─────┘
///                           │
///                    ┌──────┴────────┐
///               ┌────┴────┐     ┌────┴────┐
///               │ stage 2 │     │ stage 3 │
///               └────┬────┘     └─────────┘
///                    │
///             ┌──────┴────────┐
///        ┌────┴────┐     ┌────┴────┐
///        │ stage 4 │     │ Stage 5 │
///        └─────────┘     └─────────┘                    
///
/// ```
///  
/// Then executing Stage 1 will run its plan locally.  Stage 1 has two inputs, Stage 2 and Stage 3.  We
/// know these will execute on remote resources.   As such the plan for Stage 1 must contain an
/// [`ArrowFlightReadExec`] node that will read the results of Stage 2 and Stage 3 and coalese the
/// results.
///
/// When Stage 1's [`ArrowFlightReadExec`] node is executed, it makes an ArrowFlightRequest to the
/// host assigned in the Stage.  It provides the following Stage tree serialilzed in the body of the
/// Arrow Flight Ticket:
///
/// ```text
///               ┌─────────┐     
///               │ Stage 2 │    
///               └────┬────┘   
///                    │
///             ┌──────┴────────┐
///        ┌────┴────┐     ┌────┴────┐
///        │ Stage 4 │     │ Stage 5 │
///        └─────────┘     └─────────┘                    
///
/// ```
///
/// The receiving ArrowFlightEndpoint will then execute Stage 2 and will repeat this process.
///
/// When Stage 4 is executed, it has no input tasks, so it is assumed that the plan included in that
/// Stage can complete on its own; its likely holding a leaf node in the overall phyysical plan and
/// producing data from a [`DataSourceExec`].
#[derive(Debug, Clone)]
pub struct ExecutionStage {
    /// Our query_id
    pub query_id: Uuid,
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
    /// tree depth of our location in the stage tree, used for display only
    pub depth: usize,
}

impl ExecutionStage {
    /// Creates a new `ExecutionStage` with the given plan and inputs.  One task will be created
    /// responsible for partitions in the plan.
    pub fn new(
        query_id: Uuid,
        num: usize,
        plan: Arc<dyn ExecutionPlan>,
        inputs: Vec<Arc<ExecutionStage>>,
    ) -> Self {
        let name = format!("Stage {:<3}", num);
        let partition_group = (0..plan.properties().partitioning.partition_count())
            .map(|p| p as u64)
            .collect();
        ExecutionStage {
            query_id,
            num,
            name,
            plan,
            inputs: inputs
                .into_iter()
                .map(|s| s as Arc<dyn ExecutionPlan>)
                .collect(),
            tasks: vec![ExecutionTask::new(partition_group)],
            depth: 0,
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
            .map(|partition_group| {
                ExecutionTask::new(
                    partition_group
                        .collect::<Vec<_>>()
                        .into_iter()
                        .map(|p| p as u64)
                        .collect(),
                )
            })
            .collect();
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

    pub fn try_assign(
        self,
        channel_resolver: &impl ChannelResolver
    ) -> Result<Self> {
        let urls: Vec<Url> = channel_resolver.get_urls()?;
        if urls.is_empty() {
            return internal_err!("No URLs found in ChannelManager");
        }

        Ok(self)
    }

    fn try_assign_urls(&self, urls: &[Url]) -> Result<Self> {
        let assigned_children = self
            .child_stages_iter()
            .map(|child| {
                child
                    .clone() // TODO: avoid cloning if possible
                    .try_assign_urls(urls)
                    .map(|c| Arc::new(c) as Arc<dyn ExecutionPlan>)
            })
            .collect::<Result<Vec<_>>>()?;

        // pick a random starting position
        let mut rng = rand::thread_rng();
        let start_idx = rng.gen_range(0..urls.len());

        let assigned_tasks = self
            .tasks
            .iter()
            .enumerate()
            .map(|(i, task)| {
                let url = &urls[(start_idx + i) % urls.len()];
                task.clone().with_assignment(url)
            })
            .collect::<Vec<_>>();

        let assigned_stage = ExecutionStage {
            query_id: self.query_id,
            num: self.num,
            name: self.name.clone(),
            plan: self.plan.clone(),
            inputs: assigned_children,
            tasks: assigned_tasks,
            depth: self.depth,
        };

        Ok(assigned_stage)
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
            query_id: self.query_id,
            num: self.num,
            name: self.name.clone(),
            plan: self.plan.clone(),
            inputs: children,
            tasks: self.tasks.clone(),
            depth: self.depth,
        }))
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        self.plan.properties()
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

        let Some(channel_resolver) = get_channel_resolver(context.session_config()) else {
            return exec_err!("ChannelManager not found in session config");
        };

        let urls = channel_resolver.get_urls()?;

        let assigned_stage = stage
            .try_assign_urls(&urls)
            .map(Arc::new)
            .map_err(|e| DataFusionError::Execution(e.to_string()))?;

        // insert the stage into the context so that ExecutionPlan nodes
        // that care about the stage can access it
        let config = context
            .session_config()
            .clone()
            .with_extension(assigned_stage.clone());

        let new_ctx =
            SessionContext::new_with_config_rt(config, context.runtime_env().clone()).task_ctx();

        assigned_stage.plan.execute(partition, new_ctx)
    }
}
