//! Demonstrates **custom task routing** for **cache affinity**: consistently routing each parquet
//! file to the *same* worker so that worker can serve it from an in-memory cache on repeat queries.
//!
//! Rather than a custom data source, the example inserts a [`CacheExec`] node above every
//! [`DataSourceExec`] via a [`PhysicalOptimizerRule`]. Any existing parquet table gains caching
//! without changes to the table registration.
//!
//! Routing is a two-step pipeline:
//! - [`CachedFileScanConfigTaskEstimator::scale_up_leaf_node`] assigns each file to a task slot by
//!   hashing its path (mod task_count), so the same file always lands in the same slot.
//! - [`CachedFileScanConfigTaskEstimator::route_tasks`] maps slot `i` to `sorted_urls[i % n]`,
//!   so each slot always reaches the same worker URL.
//!
//! Together these guarantee that each worker consistently reads the same set of files and its
//! in-memory cache stays warm on repeat queries.
//!
//! ```bash
//! cargo run --features integration --example custom_worker_url_routing \
//!     'SELECT "RainToday", COUNT(*) AS days, AVG("Rainfall") AS avg_mm FROM weather GROUP BY "RainToday"'
//! cargo run --features integration --example custom_worker_url_routing \
//!     'SELECT "RainToday", COUNT(*) AS days, AVG("Rainfall") AS avg_mm FROM weather GROUP BY "RainToday"' \
//!     --show-distributed-plan
//! ```

use dashmap::DashMap;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, internal_err};
use datafusion::config::ConfigOptions;
use datafusion::datasource::physical_plan::{FileGroup, FileScanConfig};
use datafusion::execution::{SendableRecordBatchStream, SessionStateBuilder, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::stream::{
    RecordBatchReceiverStreamBuilder, RecordBatchStreamAdapter,
};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use datafusion_distributed::test_utils::localhost::{
    LocalHostWorkerResolver, spawn_worker_service,
};
use datafusion_distributed::{
    DistributedConfig, DistributedExt, DistributedLeafExec, SessionStateBuilderExt, TaskEstimation,
    TaskEstimator, TaskRoutingContext, WorkerQueryContext, display_plan_ascii,
};
use datafusion_proto::physical_plan::{PhysicalExtensionCodec, PhysicalProtoConverterExtension};
use datafusion_proto::{TryFromProto, protobuf};
use futures::TryStreamExt;
use prost::Message;
use std::error::Error;
use std::fmt;
use std::hash::{DefaultHasher, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};
use structopt::StructOpt;
use tokio::net::TcpListener;
use url::Url;

/// Worker-level cache shared across all task invocations on the same worker, keyed by a stable
/// hash of the file group being scanned.
type WorkerFileCache = DashMap<usize, Vec<RecordBatch>>;

/// [`ExecutionPlan`] that wraps a [`DataSourceExec`] and caches the batches it produces.
/// The cache is stored in the worker's session extension so it persists across task invocations.
#[derive(Debug)]
struct CacheExec {
    child: Arc<dyn ExecutionPlan>,
}

impl CacheExec {
    fn new(child: Arc<dyn ExecutionPlan>) -> Arc<Self> {
        Arc::new(Self { child })
    }
}

impl DisplayAs for CacheExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CacheExec")
    }
}

impl ExecutionPlan for CacheExec {
    fn name(&self) -> &str {
        "CacheExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.child.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(CacheExec::new(children.remove(0)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let cache = context.session_config().get_extension::<WorkerFileCache>();

        // Compute the stable key from the child's file group.
        let key = self
            .child
            .downcast_ref::<DataSourceExec>()
            .and_then(|dse| dse.data_source().downcast_ref::<FileScanConfig>())
            .map(|fsc| hash_key(&fsc.file_groups[partition]));

        // Cache hit: return the previously accumulated batches.
        let (Some(key), Some(cache)) = (key, cache.clone()) else {
            return self.child.execute(partition, context);
        };

        if let Some(cached_batches) = cache.get(&key) {
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::iter(cached_batches.clone().into_iter().map(Ok)),
            )));
        }

        // Cache miss: read from child and populate the cache while forwarding.
        let mut stream = self.child.execute(partition, context)?;
        let mut builder = RecordBatchReceiverStreamBuilder::new(schema, 1);
        let tx = builder.tx();
        builder.spawn(async move {
            let mut accumulated = Vec::new();
            while let Some(batch) = stream.try_next().await? {
                accumulated.push(batch.clone());
                if tx.send(Ok(batch)).await.is_err() {
                    break;
                }
            }
            cache.insert(key, accumulated);
            Ok(())
        });
        Ok(builder.build())
    }
}

/// Stable hash for a [`FileGroup`]: serialise each file's path via its protobuf representation so
/// the key is independent of in-memory ordering.
fn hash_key(file_group: &FileGroup) -> usize {
    let mut hasher = DefaultHasher::new();
    for file in file_group.files() {
        let serialized = protobuf::PartitionedFile::try_from_proto(file).unwrap();
        hasher.write(&serialized.encode_to_vec());
    }
    hasher.finish() as usize
}

/// Assigns each parquet file to a task slot (by hashing its path) and pins each slot to a worker.
#[derive(Debug)]
struct CachedFileScanConfigTaskEstimator;

impl TaskEstimator for CachedFileScanConfigTaskEstimator {
    fn task_estimation(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Option<TaskEstimation> {
        plan.downcast_ref::<CacheExec>()?;
        Some(TaskEstimation::desired(usize::MAX))
    }

    fn scale_up_leaf_node(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_count: usize,
        cfg: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(cache_exec) = plan.downcast_ref::<CacheExec>() else {
            return Ok(None);
        };
        let Some(dse) = cache_exec.child.downcast_ref::<DataSourceExec>() else {
            return Ok(None);
        };
        let Some(fsc) = dse.data_source().downcast_ref::<FileScanConfig>() else {
            return Ok(None);
        };

        // Hash each file to a slot so that the same file always lands in the same variant
        // regardless of the original file_groups layout.
        let mut per_task_files: Vec<Vec<_>> = vec![vec![]; task_count];
        for file in fsc.file_groups.iter().flat_map(|g| g.files()) {
            let idx = hash_key(&FileGroup::new(vec![file.clone()])) % task_count;
            per_task_files[idx].push(file.clone());
        }

        let target_partitions = cfg.execution.target_partitions;
        let variants = (0..task_count)
            .map(|i| {
                let files = std::mem::take(&mut per_task_files[i]);
                // Spread files across up to `target_partitions` FileGroups so DataFusion can
                // read them in parallel within the task.
                let n_groups = files.len().clamp(1, target_partitions);
                let mut groups: Vec<Vec<_>> = vec![vec![]; n_groups];
                for (j, file) in files.into_iter().enumerate() {
                    groups[j % n_groups].push(file);
                }
                let mut new_fsc = fsc.clone();
                new_fsc.file_groups = groups.into_iter().map(FileGroup::new).collect();
                CacheExec::new(DataSourceExec::from_data_source(new_fsc)) as Arc<dyn ExecutionPlan>
            })
            .collect::<Vec<_>>();

        Ok(Some(Arc::new(DistributedLeafExec::try_new(
            Arc::clone(plan),
            variants,
        )?)))
    }

    fn route_tasks(&self, ctx: &TaskRoutingContext<'_>) -> Result<Option<Vec<Url>>> {
        let d_cfg = DistributedConfig::from_task_context(&ctx.task_ctx)?;
        let available_urls = d_cfg.worker_resolver().get_urls()?;

        let mut routed = None;
        ctx.plan.apply(|node| {
            if let Some(leaf) = node.downcast_ref::<DistributedLeafExec>()
                && leaf.original().downcast_ref::<CacheExec>().is_some()
            {
                // Sort URLs so the slot→worker mapping is deterministic across planning passes.
                let mut urls = available_urls.to_vec();
                urls.sort();
                routed = Some(
                    (0..ctx.task_count)
                        .map(|i| urls[i % urls.len()].clone())
                        .collect(),
                );
                return Ok(TreeNodeRecursion::Stop);
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(routed)
    }
}

/// Codec for [`CacheExec`]. The child (`DataSourceExec(FileScanConfig)`) is encoded by the
/// framework as a standard plan node in `PhysicalExtensionNode.inputs`; `CacheExec` itself carries
/// no extra bytes so encode is a no-op and decode just re-wraps the decoded child.
#[derive(Debug)]
struct CachedFileScanCodec;

impl PhysicalExtensionCodec for CachedFileScanCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [child] = inputs else {
            return internal_err!("CacheExec expects exactly 1 child, got {}", inputs.len());
        };
        Ok(CacheExec::new(Arc::clone(child)))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if node.downcast_ref::<CacheExec>().is_none() {
            return internal_err!("Expected CacheExec, got {}", node.name());
        }
        Ok(())
    }
}

/// [`PhysicalOptimizerRule`] that wraps every leaf `DataSourceExec(FileScanConfig)` in a
/// [`CacheExec`]. Any `register_parquet` table gains caching transparently.
#[derive(Debug)]
struct CachedFileScanConfigRule;

impl PhysicalOptimizerRule for CachedFileScanConfigRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // transform_up (post-order) visits children before their parent, so when we wrap a
        // DataSourceExec in CacheExec the traversal has already finished with that subtree and
        // won't descend into the new CacheExec's child again.
        plan.transform_up(|node| {
            let Some(dse) = node.downcast_ref::<DataSourceExec>() else {
                return Ok(Transformed::no(node));
            };
            if dse.data_source().downcast_ref::<FileScanConfig>().is_none() {
                return Ok(Transformed::no(node));
            }
            Ok(Transformed::yes(CacheExec::new(node)))
        })
        .data()
    }

    fn name(&self) -> &str {
        "CachedFileScanConfigRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[derive(StructOpt)]
#[structopt(
    name = "custom_worker_url_routing",
    about = "Cache-affine parquet scan: each file always routes to the same worker"
)]
struct Args {
    /// SQL query to run against the `weather` table.
    query: String,

    /// Render the distributed plan instead of executing the query.
    #[structopt(long)]
    show_distributed_plan: bool,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::from_args();

    // Spawn three workers. Each gets its own in-memory cache injected as a session extension so
    // that the cache outlives individual task invocations.
    let n_workers = 3;
    let mut ports = Vec::new();
    for _ in 0..n_workers {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        ports.push(listener.local_addr()?.port());
        let cache = Arc::new(WorkerFileCache::new());
        #[allow(clippy::disallowed_methods)]
        tokio::spawn(spawn_worker_service(
            move |ctx: WorkerQueryContext| {
                let mut builder = ctx.builder;
                let cfg = builder.config().get_or_insert_default();
                cfg.set_distributed_user_codec(CachedFileScanCodec);
                cfg.set_extension(Arc::clone(&cache));
                async move { Ok(builder.build()) }
            },
            listener,
        ));
    }
    tokio::time::sleep(Duration::from_millis(200)).await;

    let mut state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(Arc::new(CachedFileScanConfigRule))
        .with_distributed_worker_resolver(LocalHostWorkerResolver::new(ports))
        .with_distributed_planner()
        .with_distributed_user_codec(CachedFileScanCodec)
        .with_distributed_task_estimator(CachedFileScanConfigTaskEstimator)
        .build();
    state
        .config_mut()
        .set_extension(Arc::new(WorkerFileCache::new()));

    let ctx = SessionContext::from(state);
    ctx.register_parquet("weather", "testdata/weather", ParquetReadOptions::default())
        .await?;

    if args.show_distributed_plan {
        let plan = ctx.sql(&args.query).await?.create_physical_plan().await?;
        println!("{}", display_plan_ascii(plan.as_ref(), false));
        return Ok(());
    }

    // Run the same query twice: cold (files read from disk) then warm (served from cache).
    for pass in ["cold", "warm"] {
        let start = Instant::now();
        let df = ctx.sql(&args.query).await?;
        let batches = df.execute_stream().await?.try_collect::<Vec<_>>().await?;
        let elapsed = start.elapsed();
        println!(
            "=== {pass} pass done after {}ms ===\n{}",
            elapsed.as_millis(),
            pretty_format_batches(&batches)?
        );
    }
    Ok(())
}
