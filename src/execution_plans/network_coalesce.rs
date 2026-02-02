use crate::common::require_one_child;
use crate::distributed_planner::NetworkBoundary;
use crate::execution_plans::common::scale_partitioning_props;
use crate::flight_service::WorkerConnectionPool;
use crate::metrics::proto::MetricsSetProto;
use crate::protobuf::{AppMetadata, StageKey};
use crate::stage::{MaybeEncodedPlan, Stage};
use crate::{DistributedTaskContext, ExecutionTask};
use dashmap::DashMap;
use datafusion::common::{exec_err, plan_err};
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan, PlanProperties,
};
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use uuid::Uuid;

/// [ExecutionPlan] that coalesces partitions from multiple tasks into a one or more task without
/// performing any repartition, and maintaining the same partitioning scheme.
///
/// This is the equivalent of a [CoalescePartitionsExec] but coalescing tasks across the network
/// between distributed stages.
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
/// ┌┴─┴┴─┴┴─┴──────────────────┐  ┌─────────┴─┴┴─┴┴─┴─────────┐ ┌──────────────────┴─┴┴─┴┴─┴┐  Stage N
/// │  Arc<dyn ExecutionPlan>   │  │  Arc<dyn ExecutionPlan>   │ │  Arc<dyn ExecutionPlan>   │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// The communication between two stages across a [NetworkCoalesceExec] has two implications:
///
/// - Stage N+1 may have one or more tasks. Each consumer task reads a contiguous group of upstream
///   tasks from Stage N.
/// - Output partitioning for Stage N+1 is sized based on the maximum upstream-group size. When
///   groups are uneven, consumer tasks with smaller groups return empty streams for the “extra”
///   partitions.
/// ```text
///                    ┌───────────────────────────┐        ┌───────────────────────────┐          ■
///                    │    NetworkCoalesceExec    │        │    NetworkCoalesceExec    │          │
///                    │         (task 1)          │        │         (task 2)          │          │
///                    └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬─────────┘        └┬─┬┬─┬┬─┬┬─┬┬─┬┬─┬─────────┘       Stage N+1
///                     │1││2││3││4││5││6│                   │7││8││9││_││_││_│                    │
///                     └─┘└─┘└─┘└─┘└─┘└─┘                   └─┘└─┘└─┘└─┘└─┘└─┘                    │
///                      ▲  ▲  ▲  ▲  ▲  ▲                     ▲  ▲  ▲                              ■
///   ┌──┬──┬────────────┴──┴──┘  └──┴──┴─────┬──┬──┐         └──┴──┴────────────────┬──┬──┐
///   │  │  │                                 │  │  │                                │  │  │       ■
///  ┌─┐┌─┐┌─┐                               ┌─┐┌─┐┌─┐                              ┌─┐┌─┐┌─┐      │
///  │1││2││3│                               │4││5││6│                              │7││8││9│      │
/// ┌┴─┴┴─┴┴─┴──────────────────┐  ┌─────────┴─┴┴─┴┴─┴─────────┐ ┌──────────────────┴─┴┴─┴┴─┴┐  Stage N
/// │  Arc<dyn ExecutionPlan>   │  │  Arc<dyn ExecutionPlan>   │ │  Arc<dyn ExecutionPlan>   │     │
/// │         (task 1)          │  │         (task 2)          │ │         (task 3)          │     │
/// └───────────────────────────┘  └───────────────────────────┘ └───────────────────────────┘     ■
/// ```
///
/// This node has two variants.
/// 1. Pending: acts as a placeholder for the distributed optimization step to mark it as ready.
/// 2. Ready: runs within a distributed stage and queries the next input stage over the network
///    using Arrow Flight.
#[derive(Debug, Clone)]
pub struct NetworkCoalesceExec {
    /// the properties we advertise for this execution plan
    pub(crate) properties: PlanProperties,
    pub(crate) input_stage: Stage,
    pub(crate) worker_connections: WorkerConnectionPool,
    /// metrics_collection is used to collect metrics from child tasks. It is initially
    /// instantiated as an empty [DashMap] (see `try_decode` in `distributed_codec.rs`).
    /// Metrics are populated here via [NetworkCoalesceExec::execute].
    ///
    /// An instance may receive metrics for 0 to N child tasks, where N is the number of tasks in
    /// the stage it is reading from. This is because, by convention, the Worker sends metrics for
    /// a task to the last NetworkCoalesceExec to read from it, which may or may not be this
    /// instance.
    pub(crate) metrics_collection: Arc<DashMap<StageKey, Vec<MetricsSetProto>>>,
}

impl NetworkCoalesceExec {
    /// Builds a new [NetworkCoalesceExec] in "Pending" state.
    ///
    /// Typically, this node should be placed right after nodes that coalesce all the input
    /// partitions into one, for example:
    /// - [CoalescePartitionsExec]
    /// - [SortPreservingMergeExec]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        query_id: Uuid,
        num: usize,
        task_count: usize,
        input_task_count: usize,
    ) -> Result<Self> {
        if task_count == 0 {
            return plan_err!("NetworkCoalesceExec cannot be executed with task_count=0");
        }

        // Each output task coalesces a group of input tasks. We size the output partition count
        // per output task based on the maximum group size, returning empty streams for tasks with
        // smaller groups.
        let max_input_task_count = input_task_count.div_ceil(task_count).max(1);
        Ok(Self {
            properties: scale_partitioning_props(input.properties(), |p| p * max_input_task_count),
            input_stage: Stage {
                query_id,
                num,
                plan: MaybeEncodedPlan::Decoded(input),
                tasks: vec![ExecutionTask { url: None }; input_task_count],
            },
            worker_connections: WorkerConnectionPool::new(input_task_count),
            metrics_collection: Default::default(),
        })
    }
}

impl NetworkBoundary for NetworkCoalesceExec {
    fn input_stage(&self) -> &Stage {
        &self.input_stage
    }

    fn with_input_stage(&self, input_stage: Stage) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.clone();
        self_clone.input_stage = input_stage;
        Ok(Arc::new(self_clone))
    }
}

impl DisplayAs for NetworkCoalesceExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let input_tasks = self.input_stage.tasks.len();
        let partitions = self.properties.partitioning.partition_count();
        let stage = self.input_stage.num;
        write!(
            f,
            "[Stage {stage}] => NetworkCoalesceExec: output_partitions={partitions}, input_tasks={input_tasks}",
        )
    }
}

impl ExecutionPlan for NetworkCoalesceExec {
    fn name(&self) -> &str {
        "NetworkCoalesceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.input_stage.plan {
            MaybeEncodedPlan::Decoded(v) => vec![v],
            MaybeEncodedPlan::Encoded(_) => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut self_clone = self.as_ref().clone();
        self_clone.input_stage.plan = MaybeEncodedPlan::Decoded(require_one_child(children)?);
        Ok(Arc::new(self_clone))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let task_context = DistributedTaskContext::from_ctx(&context);
        if task_context.task_index >= task_context.task_count {
            return exec_err!(
                "NetworkCoalesceExec invalid task context: task_index={} >= task_count={}",
                task_context.task_index,
                task_context.task_count
            );
        }

        let partitions_per_task = self
            .properties()
            .partitioning
            .partition_count()
            .checked_div(
                self.input_stage
                    .tasks
                    .len()
                    .div_ceil(task_context.task_count)
                    .max(1),
            )
            .unwrap_or(0);
        if partitions_per_task == 0 {
            return exec_err!("NetworkCoalesceExec has 0 partitions per input task");
        }

        let input_task_count = self.input_stage.tasks.len();
        let group = task_group(
            input_task_count,
            task_context.task_index,
            task_context.task_count,
        );

        let input_task_offset = partition / partitions_per_task;
        let target_partition = partition % partitions_per_task;

        // Some consumer tasks are assigned fewer upstream tasks when
        // `input_task_count % task_count != 0` (uneven grouping).
        // We still size partitions based on the maximum group size, so partitions that
        // would map to a missing upstream task slot are treated as padding and return
        // an empty stream (no network call).
        if input_task_offset >= group.len {
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.schema())));
        }

        let target_task = group.start_task + input_task_offset;

        let worker_connection = self.worker_connections.get_or_init_worker_connection(
            &self.input_stage,
            0..partitions_per_task,
            target_task,
            &context,
        )?;

        let metrics_collection = Arc::clone(&self.metrics_collection);

        let stream = worker_connection.stream_partition(target_partition, move |meta| {
            if let Some(AppMetadata::MetricsCollection(m)) = meta.content {
                for task_metrics in m.tasks {
                    if let Some(stage_key) = task_metrics.stage_key {
                        metrics_collection.insert(stage_key, task_metrics.metrics);
                    };
                }
            }
        })?;

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.worker_connections.metrics.clone_inner())
    }
}

#[derive(Debug, Clone, Copy)]
struct TaskGroup {
    /// The first input task index in this group.
    start_task: usize,
    /// The number of input tasks in this group.
    len: usize,
    /// The maximum possible group size across all groups.
    ///
    /// When groups are uneven (input_tasks % task_count != 0), some groups are shorter. We still
    /// size the output partitioning based on this max and return empty streams for the extra
    /// partitions in smaller groups.
    max_len: usize,
}

/// Returns the contiguous group of input tasks assigned to DistributedTaskContext::task_index.
fn task_group(input_task_count: usize, task_index: usize, task_count: usize) -> TaskGroup {
    if task_count == 0 {
        return TaskGroup {
            start_task: 0,
            len: 0,
            max_len: 0,
        };
    }

    // Split `input_task_count` into `task_count` contiguous groups.
    // - base_tasks_per_group: floor(input_task_count / task_count)
    // - groups_with_extra_task: first N groups that get one extra task (remainder)
    let base_tasks_per_group = input_task_count / task_count;
    let groups_with_extra_task = input_task_count % task_count;

    let len = base_tasks_per_group + usize::from(task_index < groups_with_extra_task);
    let start_task = (task_index * base_tasks_per_group) + task_index.min(groups_with_extra_task);
    let max_len = base_tasks_per_group + usize::from(groups_with_extra_task > 0);

    TaskGroup {
        start_task,
        len,
        max_len,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::empty::EmptyExec;

    #[derive(Clone, Copy)]
    struct Case {
        name: &'static str,
        input_tasks: usize,
        consumer_tasks: usize,
    }

    fn expected_groups(input_tasks: usize, consumer_tasks: usize) -> Vec<(usize, usize)> {
        assert!(consumer_tasks > 0, "consumer_tasks must be non-zero");

        let base_tasks_per_group = input_tasks / consumer_tasks;
        let groups_with_extra_task = input_tasks % consumer_tasks;
        let mut groups = Vec::with_capacity(consumer_tasks);
        let mut start_task = 0;

        for task_index in 0..consumer_tasks {
            let len = base_tasks_per_group + usize::from(task_index < groups_with_extra_task);
            groups.push((start_task, len));
            start_task += len;
        }

        groups
    }

    fn assert_case(case: Case) -> Result<()> {
        const STAGE_NUM: usize = 1;

        // Child plan used only for properties/schema (we won't reach network codepaths).
        let child: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let child_partitions = child.properties().partitioning.partition_count();

        let exec = NetworkCoalesceExec::try_new(
            Arc::clone(&child),
            Uuid::nil(),
            STAGE_NUM,
            case.consumer_tasks,
            case.input_tasks,
        )?;

        // Output partitions are sized by the maximum group size.
        let max_group_size = case.input_tasks.div_ceil(case.consumer_tasks).max(1);
        assert_eq!(
            exec.properties().partitioning.partition_count(),
            child_partitions * max_group_size
        );

        let groups = expected_groups(case.input_tasks, case.consumer_tasks);
        assert_eq!(groups.len(), case.consumer_tasks);

        let mut seen = vec![false; case.input_tasks];
        let mut expected_start = 0;
        let mut padding_slots = 0;

        for (index, (start, len)) in groups.into_iter().enumerate() {
            assert_eq!(
                start, expected_start,
                "case {} group {} should be contiguous",
                case.name, index
            );
            assert!(
                start + len <= case.input_tasks,
                "case {} group {} exceeds input task count",
                case.name,
                index
            );

            for task in start..(start + len) {
                assert!(
                    !seen[task],
                    "case {} input task {} appears twice",
                    case.name, task
                );
                seen[task] = true;
            }

            expected_start = start + len;
            padding_slots += max_group_size - len;
        }

        assert_eq!(
            expected_start, case.input_tasks,
            "case {} groups should cover all input tasks",
            case.name
        );
        assert!(
            seen.iter().all(|v| *v),
            "case {} missing at least one input task",
            case.name
        );

        let total_slots = case.consumer_tasks * max_group_size;
        let total_padding = total_slots - case.input_tasks;
        assert_eq!(
            padding_slots, total_padding,
            "case {} padding slots mismatch",
            case.name
        );

        Ok(())
    }

    #[test]
    fn validates_partition_coverage_across_task_counts() -> Result<()> {
        const ONE_TO_MANY_INPUT: usize = 1;
        const ONE_TO_MANY_OUTPUT: usize = 3;
        const MANY_TO_ONE_INPUT: usize = 4;
        const MANY_TO_ONE_OUTPUT: usize = 1;
        const MANY_TO_FEWER_INPUT: usize = 5;
        const MANY_TO_FEWER_OUTPUT: usize = 2;
        const FEWER_TO_MANY_INPUT: usize = 2;
        const FEWER_TO_MANY_OUTPUT: usize = 5;

        const CASES: [Case; 4] = [
            Case {
                name: "1_to_n",
                input_tasks: ONE_TO_MANY_INPUT,
                consumer_tasks: ONE_TO_MANY_OUTPUT,
            },
            Case {
                name: "n_to_1",
                input_tasks: MANY_TO_ONE_INPUT,
                consumer_tasks: MANY_TO_ONE_OUTPUT,
            },
            Case {
                name: "n_to_m_n_gt_m",
                input_tasks: MANY_TO_FEWER_INPUT,
                consumer_tasks: MANY_TO_FEWER_OUTPUT,
            },
            Case {
                name: "m_to_n_n_gt_m",
                input_tasks: FEWER_TO_MANY_INPUT,
                consumer_tasks: FEWER_TO_MANY_OUTPUT,
            },
        ];

        for case in CASES {
            assert_case(case)?;
        }

        Ok(())
    }
}
