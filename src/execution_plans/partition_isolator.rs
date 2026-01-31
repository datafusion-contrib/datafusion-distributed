use crate::DistributedTaskContext;
use crate::common::require_one_child;
use datafusion::execution::TaskContext;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::{
    error::Result,
    execution::SendableRecordBatchStream,
    physical_plan::{
        DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
        PlanProperties,
    },
};
use futures::TryStreamExt;
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
pub struct PartitionIsolatorExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    pub(crate) properties: PlanProperties,
    pub(crate) n_tasks: usize,
    pub(crate) metrics: ExecutionPlanMetricsSet,
}

impl PartitionIsolatorExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, n_tasks: usize) -> Self {
        let input_partitions = input.properties().partitioning.partition_count();

        let partition_count = Self::partition_groups(input_partitions, n_tasks)[0].len();

        let properties = input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(partition_count));

        Self {
            input: input.clone(),
            properties,
            n_tasks,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    fn partition_groups(input_partitions: usize, n_tasks: usize) -> Vec<Vec<usize>> {
        let q = input_partitions / n_tasks;
        let r = input_partitions % n_tasks;

        let mut off = 0;
        (0..n_tasks)
            .map(|i| q + if i < r { 1 } else { 0 })
            .map(|n| {
                let result = (off..(off + n)).collect();
                off += n;
                result
            })
            .collect()
    }

    pub(crate) fn partition_group(
        input_partitions: usize,
        task_i: usize,
        n_tasks: usize,
    ) -> Vec<usize> {
        Self::partition_groups(input_partitions, n_tasks)[task_i].clone()
    }
}

impl DisplayAs for PartitionIsolatorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_partitions = self.input.output_partitioning().partition_count();
        let partition_groups = Self::partition_groups(input_partitions, self.n_tasks);

        let n: usize = partition_groups.iter().map(|v| v.len()).sum();
        let mut partitions = vec![];
        for _ in 0..self.n_tasks {
            partitions.push(vec!["__".to_string(); n]);
        }

        write!(f, "PartitionIsolatorExec: ")?;
        for (i, partition_group) in partition_groups.iter().enumerate() {
            for (j, p) in partition_group.iter().enumerate() {
                partitions[i][*p] = format!("p{j}")
            }
            write!(f, "t{i}:[{}] ", partitions[i].join(","))?;
        }
        Ok(())
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
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let input = require_one_child(children)?;
        Ok(Arc::new(Self::new(input, self.n_tasks)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_context = DistributedTaskContext::from_ctx(&context);

        let metric = MetricBuilder::new(&self.metrics).output_rows(partition);
        let input_partitions = self.input.output_partitioning().partition_count();

        let partition_group = Self::partition_group(
            input_partitions,
            task_context.task_index,
            task_context.task_count,
        );

        // if our partition group is [7,8,9] and we are asked for parittion 1,
        // then look up that index in our group and execute that partition, in this
        // example partition 8

        match partition_group.get(partition) {
            Some(actual_partition_number) => {
                if *actual_partition_number >= input_partitions {
                    //trace!("{} returning empty stream", ctx_name);
                    Ok(Box::pin(EmptyRecordBatchStream::new(self.input.schema()))
                        as SendableRecordBatchStream)
                } else {
                    Ok(Box::pin(RecordBatchStreamAdapter::new(
                        self.schema(),
                        self.input
                            .execute(*actual_partition_number, context)?
                            .inspect_ok(move |v| metric.add(v.num_rows())),
                    )))
                }
            }
            None => Ok(Box::pin(EmptyRecordBatchStream::new(self.input.schema()))
                as SendableRecordBatchStream),
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_groups() {
        assert_eq!(
            PartitionIsolatorExec::partition_groups(2, 1),
            vec![vec![0, 1]]
        );
        assert_eq!(
            PartitionIsolatorExec::partition_groups(6, 2),
            vec![vec![0, 1, 2], vec![3, 4, 5]]
        );
        assert_eq!(
            PartitionIsolatorExec::partition_groups(6, 3),
            vec![vec![0, 1], vec![2, 3], vec![4, 5]]
        );
        assert_eq!(
            PartitionIsolatorExec::partition_groups(6, 4),
            vec![vec![0, 1], vec![2, 3], vec![4], vec![5]]
        );
        assert_eq!(
            PartitionIsolatorExec::partition_groups(10, 3),
            vec![vec![0, 1, 2, 3], vec![4, 5, 6], vec![7, 8, 9]]
        );
        assert_eq!(
            PartitionIsolatorExec::partition_groups(10, 4),
            vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7], vec![8, 9]]
        );
    }
}
