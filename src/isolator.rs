use std::{fmt::Formatter, sync::Arc};

use datafusion::{
    common::internal_datafusion_err,
    error::Result,
    execution::SendableRecordBatchStream,
    physical_plan::{
        DisplayAs, DisplayFormatType, EmptyRecordBatchStream, ExecutionPlan,
        ExecutionPlanProperties, Partitioning, PlanProperties,
    },
};

use crate::{
    logging::{error, trace},
    vocab::{CtxHost, CtxPartitionGroup},
};

/// This executor isolates partitions from the input plan. It will advertise that it has all
/// the partitions and when asked to execute, it will return empty streams for any partition that
/// is not in its partition group.
///
/// This allows us to execute Repartition Exec's on different processes. The idea is that each
/// process reads all the entire input partitions but only outputs the partitions in its partition
/// group.
#[derive(Debug)]
pub struct PartitionIsolatorExec {
    pub input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    pub partition_count: usize,
}

impl PartitionIsolatorExec {
    // new creates a new PartitionIsolatorExec. It will advertise that is has partition_count
    // partitions but return empty streams for any partitions not in its group.
    // TODO: Ideally, we only advertise partitions in the partition group. This way, the parent
    // only needs to call execute(0), execute(1) etc if there's 2 partitions in the group. Right now,
    // we don't know the number of partitions in the group, so we have to advertise all and the
    // parent will call execute(0)..execute(partition_count-1).
    pub fn new(input: Arc<dyn ExecutionPlan>, partition_count: usize) -> Self {
        // We advertise that we only have partition_count partitions
        let properties = input
            .properties()
            .clone()
            .with_partitioning(Partitioning::UnknownPartitioning(partition_count));

        Self {
            input,
            properties,
            partition_count,
        }
    }
}

impl DisplayAs for PartitionIsolatorExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(
            f,
            "PartitionIsolatorExec [providing upto {} partitions]",
            self.partition_count
        )
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

    fn children(&self) -> Vec<&std::sync::Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // TODO: generalize this
        assert_eq!(children.len(), 1);
        Ok(Arc::new(Self::new(
            children[0].clone(),
            self.partition_count,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let config = context.session_config();
        let partition_group = &config
            .get_extension::<CtxPartitionGroup>()
            .ok_or(internal_datafusion_err!(
                "PartitionGroup not set in session config"
            ))?
            .0;

        if partition > self.partition_count {
            error!(
                "PartitionIsolatorExec asked to execute partition {} but only has {} partitions",
                partition, self.partition_count
            );
            return Err(internal_datafusion_err!(
                "Invalid partition {} for PartitionIsolatorExec",
                partition
            ));
        }

        let ctx_name = &context
            .session_config()
            .get_extension::<CtxHost>()
            .map(|ctx_host| ctx_host.0.to_string())
            .unwrap_or("unknown_context_host!".to_string());

        let partitions_in_input = self.input.output_partitioning().partition_count() as u64;

        if partition_group.len() == 0 {
            trace!(
                "{} returning empty stream due to empty partition group",
                ctx_name
            );
            return Ok(Box::pin(EmptyRecordBatchStream::new(self.input.schema()))
                as SendableRecordBatchStream);
        }

        // TODO(#59): This is inefficient. Once partition groups are well defined ranges, this
        // check will be faster.
        match partition_group.contains(&(partition as u64)) {
            true => {
                trace!(
                    "PartitionIsolatorExec::execute: {}, partition_group={:?}, requested \
                     partition={} \ninput partitions={}",
                    ctx_name,
                    partition_group,
                    partition,
                    partitions_in_input
                );
                trace!("{} returning actual stream", ctx_name);
                self.input.execute(partition, context)
            }
            false => {
                trace!("{} returning empty stream", ctx_name);
                Ok(Box::pin(EmptyRecordBatchStream::new(self.input.schema()))
                    as SendableRecordBatchStream)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{record_batch_exec::RecordBatchExec, vocab::CtxPartitionGroup};
    use arrow::array::{Int32Array, RecordBatch};
    use datafusion::{
        arrow::datatypes::{DataType, Field, Schema},
        prelude::SessionContext,
    };
    use futures::StreamExt;
    use std::sync::Arc;

    fn create_test_record_batch_exec() -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "col1",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        Arc::new(RecordBatchExec::new(batch))
    }

    #[test]
    fn test_partition_isolator_exec() {
        let input = create_test_record_batch_exec();
        let partition_count = 3;
        let isolator = PartitionIsolatorExec::new(input, partition_count);

        // Test success case: valid partition with partition group
        let ctx = SessionContext::new();
        let partition_group = vec![0u64, 1u64, 2u64];
        {
            let state = ctx.state_ref();
            let mut guard = state.write();
            let config = guard.config_mut();
            config.set_extension(Arc::new(CtxPartitionGroup(partition_group)));
        }

        let task_context = ctx.task_ctx();

        // Success case: execute valid partition
        let result = isolator.execute(0, task_context.clone());
        assert!(result.is_ok());

        // Error case: try to execute partition beyond partition_count
        let result = isolator.execute(4, task_context.clone());
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("Invalid partition 4 for PartitionIsolatorExec"));

        // Error case: test empty task context (missing group extension)
        let empty_ctx = SessionContext::new();
        let empty_task_context = empty_ctx.task_ctx();

        let result = isolator.execute(0, empty_task_context.clone());
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("PartitionGroup not set in session config"));

        let result = isolator.execute(1, empty_task_context);
        assert!(result.is_err());
        assert!(result
            .err()
            .unwrap()
            .to_string()
            .contains("PartitionGroup not set in session config"));
    }

    #[tokio::test]
    async fn test_partition_isolator_exec_with_group() {
        let input = create_test_record_batch_exec();
        let partition_count = 6;
        let isolator = PartitionIsolatorExec::new(input, partition_count);

        // Partition group is a subset of the partitions.
        let ctx = SessionContext::new();
        let partition_group = vec![1u64, 2u64, 3u64, 4u64];
        {
            let state = ctx.state_ref();
            let mut guard = state.write();
            let config = guard.config_mut();
            config.set_extension(Arc::new(CtxPartitionGroup(partition_group)));
        }

        let task_context = ctx.task_ctx();
        for i in 0..6 {
            let result = isolator.execute(i, task_context.clone());
            assert!(result.is_ok());
            let mut stream = result.unwrap();
            let next_batch = stream.next().await;
            if i == 0 || i == 5 {
                assert!(
                    next_batch.is_none(),
                    "Expected EmptyRecordBatchStream to produce no batches"
                );
            } else {
                assert!(
                    next_batch.is_some(),
                    "Expected Stream to produce non-empty batches"
                );
            }
        }
    }
}
