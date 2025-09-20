use crate::distributed_physical_optimizer_rule::limit_tasks_err;
use crate::execution_plans::DistributedTaskContext;
use crate::StageExec;
use datafusion::common::{exec_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::{
    error::Result,
    execution::SendableRecordBatchStream,
    physical_plan::{
        DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
        PlanProperties,
    },
};
use std::{fmt::Formatter, sync::Arc};

/// This is a simple [ExecutionPlan] that isolates a set of N partitions from an input
/// [ExecutionPlan] with M partitions, where N < M.
///
/// It will advertise to upper nodes that only N partitions are available, even though the child
/// plan might have more.
///
/// The partitions exposed to upper nodes depend on:
/// 1. the amount of tasks in the stage in which [PartitionIsolatorExec] is in.
/// 2. the task index executing the [PartitionIsolatorExec] node.
///
/// ```text
///                                ┌───────────────────────────┐                                   ■
///                                │    NetworkCoalesceExec    │                                   │
///                                │         (task 1)          │                                   │
///                                └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬┘                                Stage N+1
///                                 │1││2││3││4││5││6││7││8││9│                                    │
///                                 └─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘└─┘                                    │
///                                 ▲  ▲  ▲   ▲  ▲  ▲   ▲  ▲  ▲                                    ■
///   ┌──┬──┬───────────────────────┴──┴──┘   │  │  │   └──┴──┴──────────────────────┬──┬──┐
///   │  │  │                                 │  │  │                                │  │  │       ■
///  ┌─┐┌─┐┌─┐                               ┌─┐┌─┐┌─┐                              ┌─┐┌─┐┌─┐      │
///  │1││2││3│                               │4││5││6│                              │7││8││9│      │
/// ┌┴─┴┴─┴┴─┴──────────────────┐  ┌─────────┴─┴┴─┴┴─┴─────────┐ ┌──────────────────┴─┴┴─┴┴─┴┐     │
/// │   PartitionIsolatorExec   │  │   PartitionIsolatorExec   │ │   PartitionIsolatorExec   │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └─▲──▲──▲───────────────────┘  └──────────▲──▲──▲──────────┘ └───────────────────▲──▲──▲─┘     │
///   │  │  │  ◌  ◌  ◌  ◌  ◌  ◌      ◌  ◌  ◌  │  │  │  ◌  ◌  ◌     ◌  ◌  ◌  ◌  ◌  ◌  │  │  │    Stage N
///   │  │  │  │  │  │  │  │  │      │  │  │  │  │  │  │  │  │     │  │  │  │  │  │  │  │  │       │
///  ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐    ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐   ┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐┌─┐      │
///  │1││2││3││4││5││6││7││8││9│    │1││2││3││4││5││6││7││8││9│   │1││2││3││4││5││6││7││8││9│      │
/// ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐  ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐ ┌┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┴─┴┐     │
/// │      DataSourceExec       │  │      DataSourceExec       │ │      DataSourceExec       │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
#[derive(Debug)]
pub enum PartitionIsolatorExec {
    Pending(PartitionIsolatorPendingExec),
    Ready(PartitionIsolatorReadyExec),
}

#[derive(Debug)]
pub struct PartitionIsolatorPendingExec {
    input: Arc<dyn ExecutionPlan>,
}

#[derive(Debug)]
pub struct PartitionIsolatorReadyExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) properties: PlanProperties,
    pub(crate) n_tasks: usize,
}

impl PartitionIsolatorExec {
    pub fn new_pending(input: Arc<dyn ExecutionPlan>) -> Self {
        PartitionIsolatorExec::Pending(PartitionIsolatorPendingExec { input })
    }

    pub fn ready(&self, n_tasks: usize) -> Result<Self, DataFusionError> {
        let Self::Pending(pending) = self else {
            return plan_err!("PartitionIsolatorExec is already ready");
        };

        let input_partitions = pending.input.properties().partitioning.partition_count();
        if n_tasks > input_partitions {
            return Err(limit_tasks_err(input_partitions));
        }

        let partition_count = (input_partitions as f64 / n_tasks as f64).ceil() as usize;

        let properties = pending
            .input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(partition_count));

        Ok(Self::Ready(PartitionIsolatorReadyExec {
            input: pending.input.clone(),
            properties,
            n_tasks,
        }))
    }

    pub(crate) fn new_ready(
        input: Arc<dyn ExecutionPlan>,
        n_tasks: usize,
    ) -> Result<Self, DataFusionError> {
        Self::new_pending(input).ready(n_tasks)
    }

    pub(crate) fn partition_group(&self, task_i: usize, n_tasks: usize) -> Vec<usize> {
        let Self::Ready(ready) = self else {
            return vec![];
        };

        let input_partitions = ready.input.output_partitioning().partition_count();
        let ppt = (input_partitions as f64 / n_tasks as f64).ceil() as usize;
        ((ppt * task_i)..(ppt * (task_i + 1))).collect()
    }
}

impl DisplayAs for PartitionIsolatorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "PartitionIsolatorExec",)
    }
}

impl ExecutionPlan for PartitionIsolatorExec {
    fn name(&self) -> &str {
        "PartitionIsolatorExec"
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        match self {
            PartitionIsolatorExec::Pending(pending) => pending.input.properties(),
            PartitionIsolatorExec::Ready(ready) => &ready.properties,
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match self {
            PartitionIsolatorExec::Pending(pending) => vec![&pending.input],
            PartitionIsolatorExec::Ready(ready) => vec![&ready.input],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!(
                "PartitionIsolatorExec wrong number of children, expected 1, got {}",
                children.len()
            );
        }

        Ok(Arc::new(match self.as_ref() {
            PartitionIsolatorExec::Pending(_) => Self::new_pending(children[0].clone()),
            PartitionIsolatorExec::Ready(ready) => {
                Self::new_pending(children[0].clone()).ready(ready.n_tasks)?
            }
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let Self::Ready(self_ready) = self else {
            return exec_err!("PartitionIsolatorExec is not ready");
        };

        let task_context = DistributedTaskContext::from_ctx(&context);
        let stage = StageExec::from_ctx(&context)?;

        let partition_group = self.partition_group(task_context.task_index, stage.tasks.len());

        let partitions_in_input = self_ready
            .input
            .properties()
            .output_partitioning()
            .partition_count();

        // if our partition group is [7,8,9] and we are asked for parittion 1,
        // then look up that index in our group and execute that partition, in this
        // example partition 8

        let output_stream = match partition_group.get(partition) {
            Some(actual_partition_number) => {
                if *actual_partition_number >= partitions_in_input {
                    //trace!("{} returning empty stream", ctx_name);
                    Ok(
                        Box::pin(EmptyRecordBatchStream::new(self_ready.input.schema()))
                            as SendableRecordBatchStream,
                    )
                } else {
                    self_ready.input.execute(*actual_partition_number, context)
                }
            }
            None => Ok(
                Box::pin(EmptyRecordBatchStream::new(self_ready.input.schema()))
                    as SendableRecordBatchStream,
            ),
        };
        output_stream
    }
}
