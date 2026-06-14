//! This example demonstrates how to **build a custom distributed plan by injecting network
//! boundaries yourself**, instead of relying on the automatic distributed planner to decide
//! where the stages go.
//!
//! Distributed DataFusion exposes [`NetworkShuffleExec`] and [`NetworkCoalesceExec`] as
//! public, constructible nodes. If a physical plan already contains network boundaries when it
//! reaches the distributed planner, the planner does **not** try to distribute it on its own —
//! it simply finalises the boundaries you placed (assigning each stage a unique id, eliding
//! ones that aren't needed) and wraps the result in a `DistributedExec`. The natural place to
//! inject them is a [`PhysicalOptimizerRule`], which runs while the physical plan is being
//! built.
//!
//! Here we build a **progressive partial-reduction tree** for a `GROUP BY` aggregation over the
//! `weather` parquet table. Rather than gathering every leaf task into a single node with one
//! wide coalesce, we reduce the data at every level of the tree:
//!
//! ```text
//!  Final            (1 task)   <- finishes the aggregation
//!    NetworkCoalesceExec  M -> 1
//!  PartialReduce    (M tasks)  <- merges partial states, shrinking the data again
//!    NetworkCoalesceExec  N -> M
//!  Partial          (N tasks)  <- first partial reduce, one task per slice of files
//!    DistributedLeafExec(weather)
//! ```
//!
//! The key node is [`AggregateMode::PartialReduce`]: unlike a plain coalesce, which only
//! concatenates partition streams, `PartialReduce` merges partial-aggregate states into fewer
//! partial-aggregate states. Chaining it at each level means less data crosses each network
//! hop. A single `Final` aggregation on the root finishes the job.
//!
//! We only inject the boundaries; the leaf is left as a plain parquet scan. The distributed
//! planner still runs the registered `TaskEstimator` over each stage's leaves, so the default
//! file-scan estimator splits the parquet files across the leaf-stage tasks for us — no manual
//! leaf handling needed.
//!
//! Run this example with (from the repo root, so the `testdata/weather` files resolve):
//! ```bash
//! cargo run --features integration --example custom_distributed_partial_reduction_tree "SELECT \"RainToday\", count(*) FROM weather GROUP BY \"RainToday\"" --show-distributed-plan
//! cargo run --features integration --example custom_distributed_partial_reduction_tree "SELECT \"RainToday\", count(*) FROM weather GROUP BY \"RainToday\""
//! cargo run --features integration --example custom_distributed_partial_reduction_tree "SELECT \"RainToday\", avg(\"MinTemp\") FROM weather GROUP BY \"RainToday\"" --leaf-tasks 3 --mid-tasks 2
//! ```

use arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode, PhysicalGroupBy};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::test_utils::in_memory_channel_resolver::{
    InMemoryChannelResolver, InMemoryWorkerResolver,
};
use datafusion_distributed::{
    DistributedExt, NetworkCoalesceExec, SessionStateBuilderExt, WorkerQueryContext,
    display_plan_ascii,
};
use futures::TryStreamExt;
use std::sync::Arc;
use structopt::StructOpt;

/// Injects a progressive partial-reduction tree in place of the standard two-phase aggregation.
///
/// A two-phase aggregate plan (after physical planning) looks roughly like:
/// ```text
/// AggregateExec(mode=FinalPartitioned)   <- the node we match and replace
///   RepartitionExec(Hash)
///     AggregateExec(mode=Partial)
///       DataSourceExec(weather)          <- left as-is; the planner's TaskEstimator splits it
/// ```
///
/// We reuse the group-by / aggregate expressions from the existing `Partial` node and rebuild
/// the pipeline as `Partial → NetworkCoalesce(N→M) → PartialReduce → NetworkCoalesce(M→1) → Final`.
#[derive(Debug)]
struct PartialReductionTreeRule {
    leaf_tasks: usize,
    mid_tasks: usize,
}

impl PhysicalOptimizerRule for PartialReductionTreeRule {
    fn name(&self) -> &str {
        "partial_reduction_tree"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = plan.transform_down(|node| {
            // Match the top (finalising) aggregate of a two-phase aggregation.
            let Some(top_agg) = node.downcast_ref::<AggregateExec>() else {
                return Ok(Transformed::no(node));
            };
            if matches!(
                top_agg.mode(),
                AggregateMode::Partial | AggregateMode::PartialReduce
            ) {
                return Ok(Transformed::no(node));
            }

            // Find the `Partial` aggregate and the parquet leaf beneath it.
            let Some(partial_node) =
                find_node(&node, |p| is_aggregate_mode(p, AggregateMode::Partial))
            else {
                return Ok(Transformed::no(node));
            };
            if find_node(&partial_node, is_data_source).is_none() {
                return Ok(Transformed::no(node));
            }
            let partial = partial_node.downcast_ref::<AggregateExec>().unwrap();

            // The partial-aggregate subtree (with its plain parquet leaf) is the producer stage,
            // running on `leaf_tasks` tasks. We don't split the leaf ourselves: the distributed
            // planner runs the registered `TaskEstimator` over each stage's leaves, and the
            // default file-scan estimator splits the parquet files across the stage's tasks.
            let producer = Arc::clone(&partial_node);

            // Group-by/aggregate expressions reused for the merge stages. After a `Partial`
            // aggregate, the group-by columns sit at the front of the output schema, so the
            // merge stages reference them positionally (`Column::new(name, i)`).
            let merge_group_by = positional_group_by(partial.group_expr());
            let aggr = partial.aggr_expr().to_vec();
            let filter = partial.filter_expr().to_vec();
            let input_schema = partial.input_schema();

            // Level 1: gather N leaf tasks down to M, then PartialReduce to shrink again.
            let nc1: Arc<dyn ExecutionPlan> = Arc::new(NetworkCoalesceExec::try_new(
                producer,
                self.leaf_tasks,
                self.mid_tasks,
            )?);
            let mid_collect: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(nc1));
            let partial_reduce: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
                AggregateMode::PartialReduce,
                merge_group_by.clone(),
                aggr.clone(),
                filter.clone(),
                mid_collect,
                Arc::clone(&input_schema),
            )?);

            // Level 2: gather M tasks down to a single task, then finish the aggregation.
            let nc2: Arc<dyn ExecutionPlan> = Arc::new(NetworkCoalesceExec::try_new(
                partial_reduce,
                self.mid_tasks,
                1,
            )?);
            let root_collect: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(nc2));
            let final_agg: Arc<dyn ExecutionPlan> = Arc::new(AggregateExec::try_new(
                AggregateMode::Final,
                merge_group_by,
                aggr,
                filter,
                root_collect,
                input_schema,
            )?);

            // Replace the original finalising aggregate with our tree. `Jump` stops the walk
            // from descending into the boundaries we just inserted.
            Ok(Transformed::new(final_agg, true, TreeNodeRecursion::Jump))
        })?;

        Ok(result.data)
    }
}

/// Rebuilds the group-by so each key references the partial-aggregate output column by position,
/// which is what the `PartialReduce` and `Final` merge stages consume.
fn positional_group_by(orig: &PhysicalGroupBy) -> PhysicalGroupBy {
    PhysicalGroupBy::new(
        orig.expr()
            .iter()
            .enumerate()
            .map(|(i, (_, name))| (Arc::new(Column::new(name, i)) as _, name.clone()))
            .collect(),
        orig.null_expr()
            .iter()
            .enumerate()
            .map(|(i, (_, name))| (Arc::new(Column::new(name, i)) as _, name.clone()))
            .collect(),
        orig.groups().to_vec(),
        orig.has_grouping_set(),
    )
}

fn is_aggregate_mode(plan: &Arc<dyn ExecutionPlan>, mode: AggregateMode) -> bool {
    plan.downcast_ref::<AggregateExec>()
        .is_some_and(|a| *a.mode() == mode)
}

fn is_data_source(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.is::<DataSourceExec>()
}

/// Returns a clone of the first node (top-down) matching `predicate`.
fn find_node(
    plan: &Arc<dyn ExecutionPlan>,
    predicate: impl Fn(&Arc<dyn ExecutionPlan>) -> bool,
) -> Option<Arc<dyn ExecutionPlan>> {
    let mut found = None;
    plan.apply(|node| {
        if predicate(node) {
            found = Some(Arc::clone(node));
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .unwrap();
    found
}

#[derive(StructOpt)]
#[structopt(
    name = "custom_distributed_partial_reduction_tree",
    about = "Manually injected network boundaries"
)]
struct Args {
    /// The SQL query to run (a `GROUP BY` aggregation over the `weather` table).
    query: String,

    /// Number of leaf tasks performing the first partial reduction.
    #[structopt(long, default_value = "3")]
    leaf_tasks: usize,

    /// Number of intermediate tasks in the middle of the reduction tree.
    #[structopt(long, default_value = "2")]
    mid_tasks: usize,

    /// Render the distributed plan instead of executing the query.
    #[structopt(long)]
    show_distributed_plan: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::from_args();

    let worker_resolver = InMemoryWorkerResolver::new(args.leaf_tasks.max(args.mid_tasks).max(1));
    // The plan is built from standard DataFusion nodes (DataSourceExec, AggregateExec, ...) plus
    // the network boundaries, all handled by the built-in codec, so no custom codec is needed.
    let channel_resolver =
        InMemoryChannelResolver::from_session_builder(|ctx: WorkerQueryContext| async move {
            Ok(ctx.builder.build())
        });

    let state = SessionStateBuilder::new()
        .with_default_features()
        // Our rule injects the network boundaries while the physical plan is built...
        .with_physical_optimizer_rule(Arc::new(PartialReductionTreeRule {
            leaf_tasks: args.leaf_tasks,
            mid_tasks: args.mid_tasks,
        }))
        // ...and the distributed planner finalises the boundaries it finds.
        .with_distributed_worker_resolver(worker_resolver)
        .with_distributed_channel_resolver(channel_resolver)
        .with_distributed_planner()
        .build();

    let ctx = SessionContext::from(state);
    ctx.register_parquet("weather", "testdata/weather", ParquetReadOptions::default())
        .await?;

    let df = ctx.sql(&args.query).await?;
    if args.show_distributed_plan {
        let plan = df.create_physical_plan().await?;
        println!("{}", display_plan_ascii(plan.as_ref(), false));
    } else {
        let batches = df.execute_stream().await?.try_collect::<Vec<_>>().await?;
        println!("{}", pretty_format_batches(&batches)?);
    }
    Ok(())
}
