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

/// Partition isolation execution plan for distributed query processing
///
/// This execution plan provides a critical capability for distributed DataFusion by isolating
/// specific partitions from a child execution plan. It enables distributed execution of
/// operations like `RepartitionExec` by presenting each worker with a virtualized view
/// containing only the partitions they should process.
///
/// # Distributed Execution Model
/// In distributed DataFusion, operations like repartitioning need to be split across workers.
/// Instead of each worker seeing all partitions of the input, `PartitionIsolatorExec` creates
/// a filtered view where each worker only sees the partitions assigned to it.
///
/// # Partition Mapping Process
/// 1. **Partition Assignment**: The distributed planner assigns specific input partitions to each worker
/// 2. **Context Extension**: Partition assignments are stored in the session context as `CtxPartitionGroup`
/// 3. **Isolation**: When executed, this plan maps logical partition numbers to actual input partitions
/// 4. **Empty Handling**: Returns empty streams for unassigned or out-of-range partitions
///
/// # Use Cases
/// - **RepartitionExec Distribution**: Each worker processes only its assigned partitions
/// - **Load Balancing**: Enables even distribution of work across available workers  
/// - **Resource Isolation**: Prevents workers from accessing partitions outside their assignment
/// - **Fault Tolerance**: Isolated execution reduces impact of individual worker failures
///
/// # Example Scenario
/// ```text
/// Input Plan (8 partitions): [P0, P1, P2, P3, P4, P5, P6, P7]
///
/// Worker 1 PartitionIsolator: Shows partitions [P0, P1] as [0, 1]
/// Worker 2 PartitionIsolator: Shows partitions [P2, P3] as [0, 1]  
/// Worker 3 PartitionIsolator: Shows partitions [P4, P5] as [0, 1]
/// Worker 4 PartitionIsolator: Shows partitions [P6, P7] as [0, 1]
/// ```
///
/// # Context Dependencies
/// Requires the following context extensions to function properly:
/// - `CtxPartitionGroup`: Maps logical partition indices to actual input partition numbers
/// - `CtxHost`: Provides worker identification for logging and debugging
#[derive(Debug)]
pub struct PartitionIsolatorExec {
    /// The child execution plan from which partitions will be isolated
    /// This plan contains the full set of input partitions that will be filtered
    pub input: Arc<dyn ExecutionPlan>,

    /// Cached plan properties with modified partitioning information
    /// Reports the number of partitions this isolator will provide rather than
    /// the actual number of partitions in the input plan
    properties: PlanProperties,

    /// Number of partitions this isolator will advertise as available
    /// This is typically less than or equal to the input plan's partition count
    /// and represents the maximum partition index that can be requested
    pub partition_count: usize,
}

impl PartitionIsolatorExec {
    /// Creates a new partition isolator execution plan
    ///
    /// This constructor wraps an input execution plan with partition isolation capabilities,
    /// allowing distributed workers to see only a subset of the input partitions. The isolator
    /// modifies the plan properties to advertise the specified partition count rather than
    /// the actual input partition count.
    ///
    /// # Partitioning Strategy
    /// The isolator creates an `UnknownPartitioning` with the specified partition count.
    /// This tells DataFusion that partitions exist but their distribution properties
    /// are not statically known, which is appropriate for dynamically assigned partitions
    /// in distributed execution.
    ///
    /// # Arguments
    /// * `input` - The child execution plan to isolate partitions from
    /// * `partition_count` - Number of partitions this isolator will advertise
    ///
    /// # Returns
    /// * `PartitionIsolatorExec` - Configured isolator ready for distributed execution
    pub fn new(input: Arc<dyn ExecutionPlan>, partition_count: usize) -> Self {
        // Modify plan properties to advertise the isolated partition count
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
    /// Formats the partition isolator for display in query plan visualization
    ///
    /// Provides a human-readable representation showing the number of partitions
    /// this isolator will provide. This appears in EXPLAIN output and debugging
    /// information to help understand the distributed execution structure.
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

    /// Executes a specific partition of this isolation plan
    ///
    /// This method implements the core partition isolation logic by mapping the requested
    /// logical partition number to an actual input partition using the context-provided
    /// partition group mapping. It handles various edge cases including out-of-range
    /// partitions and missing assignments.
    ///
    /// # Partition Mapping Process
    /// 1. **Context Extraction**: Retrieves partition group mapping from session context
    /// 2. **Bounds Checking**: Validates requested partition is within advertised range
    /// 3. **Mapping Lookup**: Maps logical partition to actual input partition number
    /// 4. **Range Validation**: Ensures mapped partition exists in input plan
    /// 5. **Execution**: Delegates to input plan or returns empty stream as appropriate
    ///
    /// # Arguments
    /// * `partition` - Logical partition number to execute (0-based index)
    /// * `context` - Execution context containing partition group mapping
    ///
    /// # Returns
    /// * `SendableRecordBatchStream` - Data stream for the requested partition
    ///   - Actual data stream if partition is mapped and valid
    ///   - Empty stream if partition is unmapped or out of range
    ///
    /// # Errors
    /// Returns an error if:
    /// - Partition group context extension is missing
    /// - Requested partition exceeds the configured partition count
    ///
    /// # Context Requirements
    /// Requires these session context extensions:
    /// - `CtxPartitionGroup`: Maps logical partitions to input partition numbers
    /// - `CtxHost`: Worker identification for logging (optional but recommended)
    ///
    /// # Example Execution Flow
    /// ```text
    /// Input: partition=1, partition_group=[None, Some(5), Some(3)]
    /// 1. Look up partition_group[1] → Some(5)
    /// 2. Check if input plan has partition 5 → Yes
    /// 3. Execute input.execute(5, context)
    /// 4. Return the resulting stream
    /// ```
    fn execute(
        &self,
        partition: usize,
        context: std::sync::Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // Extract partition group mapping from session context
        let config = context.session_config();
        let partition_group = &config
            .get_extension::<CtxPartitionGroup>()
            .ok_or(internal_datafusion_err!(
                "PartitionGroup not set in session config"
            ))?
            .0;

        // Validate requested partition is within configured bounds
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

        // Extract worker context name for logging
        let ctx_name = &context
            .session_config()
            .get_extension::<CtxHost>()
            .map(|ctx_host| ctx_host.0.to_string())
            .unwrap_or("unknown_context_host!".to_string());

        // Get input plan's actual partition count for validation
        let partitions_in_input = self.input.output_partitioning().partition_count() as u64;

        // Map logical partition to actual input partition and execute
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

                // Check if mapped partition exists in input plan
                if *actual_partition_number >= partitions_in_input {
                    trace!("{} returning empty stream", ctx_name);
                    // Return empty stream for out-of-range partitions
                    Ok(Box::pin(EmptyRecordBatchStream::new(self.input.schema()))
                        as SendableRecordBatchStream)
                } else {
                    trace!("{} returning actual stream", ctx_name);
                    // Execute the actual mapped partition
                    self.input
                        .execute(*actual_partition_number as usize, context)
                }
            }
            None => {
                // Return empty stream for unmapped partitions
                Ok(Box::pin(EmptyRecordBatchStream::new(self.input.schema()))
                    as SendableRecordBatchStream)
            }
        };
        output_stream
    }
}
