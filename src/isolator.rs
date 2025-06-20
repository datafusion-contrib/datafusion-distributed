use std::{fmt::Formatter, sync::Arc};

use datafusion::{
    common::internal_datafusion_err,
    error::Result,
    execution::SendableRecordBatchStream,
    physical_plan::{
        DisplayAs,
        DisplayFormatType,
        EmptyRecordBatchStream,
        ExecutionPlan,
        ExecutionPlanProperties,
        Partitioning,
        PlanProperties,
    },
};

use crate::{
    logging::{error, trace},
    vocab::{CtxName, CtxPartitionGroup},
};

/// This is a simple execution plan that isolates a partition from the input
/// plan It will advertise that it has a single partition and when
/// asked to execute, it will execute a particular partition from the child
/// input plan.
///
/// This allows us to execute Repartition Exec's on different processes
/// by showing each one only a single child partition
#[derive(Debug)]
pub struct PartitionIsolatorExec {
    pub input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
    pub partition_count: usize,
}

impl PartitionIsolatorExec {
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
        self: std::sync::Arc<Self>,
        children: Vec<std::sync::Arc<dyn ExecutionPlan>>,
    ) -> Result<std::sync::Arc<dyn ExecutionPlan>> {
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
        context: std::sync::Arc<datafusion::execution::TaskContext>,
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

        let ctx_name = context
            .session_config()
            .get_extension::<CtxName>()
            .ok_or_else(|| internal_datafusion_err!("CtxName not set in session config"))?
            .0
            .clone();

        let partitions_in_input = self.input.output_partitioning().partition_count() as u64;

        let output_stream = match partition_group.get(partition) {
            Some(actual_partition_number) => {
                trace!(
                    "PartitionIsolatorExec::execute: {}, partition_group={:?}, requested \
                     partition={} actual={},\ninput partitions={}",
                    ctx_name,
                    partition_group,
                    partition,
                    *actual_partition_number,
                    partitions_in_input
                );
                if *actual_partition_number >= partitions_in_input {
                    trace!("{} returning empty stream", ctx_name);
                    Ok(Box::pin(EmptyRecordBatchStream::new(self.input.schema()))
                        as SendableRecordBatchStream)
                } else {
                    trace!("{} returning actual stream", ctx_name);
                    self.input
                        .execute(*actual_partition_number as usize, context)
                }
            }
            None => Ok(Box::pin(EmptyRecordBatchStream::new(self.input.schema()))
                as SendableRecordBatchStream),
        };
        output_stream
    }
}
