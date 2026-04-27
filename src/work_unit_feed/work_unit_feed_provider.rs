use crate::WorkUnit;
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use futures::stream::BoxStream;
use std::fmt::Debug;
use std::sync::Arc;

/// Extension point for building user-defined work unit streams consumed by a
/// [`crate::WorkUnitFeed`] embedded in a leaf [`datafusion::physical_plan::ExecutionPlan`].
///
/// Implement this trait on a type that knows how to produce the per-partition stream of
/// work items (e.g. file addresses, external queries, key ranges) that the leaf plan needs
/// at runtime. Then wrap the implementation with [`crate::WorkUnitFeed::new`] and store
/// the resulting [`crate::WorkUnitFeed`] as a field of your [`ExecutionPlan`] node.
///
/// In a distributed context the provider is only invoked on the **coordinating** stage
/// that initiates the query. The work units it produces are serialized and streamed over
/// the network to the workers, which expose the same typed stream to the leaf plan as if
/// it were running locally.
///
/// See [`WorkUnitFeedProvider::feed`] for the per-call contract.
pub trait WorkUnitFeedProvider: Send + Sync + Debug {
    type WorkUnit: WorkUnit + Default;

    /// Builds a [`WorkUnit`] stream for the given `partition`.
    ///
    /// This method is never invoked in a remote worker. On workers, the equivalent
    /// leaf plan uses a remote provider that pulls the work units off the network —
    /// user code doesn't need to implement that case.
    ///
    /// When implementing this method, [DistributedWorkUnitFeedContext] can be extracted from
    /// the [TaskContext], and it contains information about the amount of distributed tasks to
    /// which [WorkUnit]s should be fanned out.
    ///
    /// The implementation should be prepared to return `P*T` feeds, where `P` is the number of
    /// partitions of the [datafusion::physical_plan::ExecutionPlan] to which the
    /// [WorkUnitFeedProvider] is attached and `T` is the number of tasks to which it should fanout
    ///
    /// For more information about how [WorkUnit] feeds work, refer to the [crate::WorkUnitFeed]
    /// docs.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use std::sync::Arc;
    /// # use datafusion_distributed::{DistributedWorkUnitFeedContext, WorkUnitFeedProvider};
    /// # use datafusion::common::Result;
    /// # use datafusion::execution::TaskContext;
    /// # use futures::stream::BoxStream;
    /// # use futures::StreamExt;
    ///
    /// #[derive(Debug)]
    /// struct MyFeedProvider {
    ///     output_partitions: usize
    /// };
    ///
    /// #[derive(Clone, PartialEq, ::prost::Message)]
    /// struct MyCustomWorkUnit {
    ///     #[prost(string, tag = "1")]
    ///     custom_field: String,
    /// }
    ///
    /// impl WorkUnitFeedProvider for MyFeedProvider {
    ///     type WorkUnit = MyCustomWorkUnit;
    ///
    ///     fn feed(
    ///         &self,
    ///         partition: usize,
    ///         ctx: Arc<TaskContext>,
    ///     ) -> Result<BoxStream<'static, Result<Self::WorkUnit>>> {
    ///         let feed_ctx = DistributedWorkUnitFeedContext::from_ctx(&ctx);
    ///
    ///         // this method will be called `feed_ctx.fan_out_tasks * self.output_partitions`
    ///         // times.
    ///         Ok(futures::stream::empty().boxed())
    ///     }
    /// }
    /// ```
    fn feed(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Self::WorkUnit>>>;
}

/// Provides contextual information about where a [WorkUnitFeedProvider] is being executed. When
/// using [WorkUnitFeedProvider] in distributed queries, it might be getting executed in the
/// coordinating stage, or it might be getting executed just locally because the query did not
/// need any remote execution.
pub struct DistributedWorkUnitFeedContext {
    /// The number of distributed tasks to which the [WorkUnitFeedProvider] should fan out.
    pub fan_out_tasks: usize,
}

impl DistributedWorkUnitFeedContext {
    /// Gets the [DistributedWorkUnitFeedContext] from the [TaskContext] as an extension.
    /// If no [DistributedWorkUnitFeedContext] is present, returns one valid for single-node
    /// execution.
    pub fn from_ctx(ctx: &Arc<TaskContext>) -> Arc<Self> {
        ctx.session_config()
            .get_extension::<Self>()
            .unwrap_or(Arc::new(DistributedWorkUnitFeedContext {
                fan_out_tasks: 1,
            }))
    }
}
