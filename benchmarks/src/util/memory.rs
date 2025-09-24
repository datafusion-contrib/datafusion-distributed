use dashmap::{DashMap, Entry};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{exec_err, extensions_options, plan_err};
use datafusion::config::{ConfigExtension, ConfigOptions};
use datafusion::error::DataFusionError;
use datafusion::execution::{FunctionRegistry, SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, displayable,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use futures::{FutureExt, StreamExt};
use prost::Message;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::{Arc, LazyLock};
use tokio::sync::OnceCell;

type Key = (String, usize);
type Value = Arc<OnceCell<Vec<RecordBatch>>>;
static CACHE: LazyLock<DashMap<Key, Value>> = LazyLock::new(DashMap::default);

/// Caches all the record batches in a global [CACHE] on the first run, and serves
/// them from the cache in any subsequent run.
#[derive(Debug, Clone)]
pub struct InMemoryCacheExec {
    inner: Arc<dyn ExecutionPlan>,
}

extensions_options! {
    /// Marker used by the [InMemoryCacheExec] that determines wether its fine
    /// to load data from disk because we are warming up, or not.
    ///
    /// If this marker is not present during InMemoryCacheExec::execute(), and
    /// the data was not loaded in-memory already, the query will fail.
    pub struct WarmingUpMarker {
        is_warming_up: bool, default = false
    }
}

impl ConfigExtension for WarmingUpMarker {
    const PREFIX: &'static str = "in-memory-cache-exec";
}

impl WarmingUpMarker {
    pub fn warming_up() -> Self {
        Self {
            is_warming_up: true,
        }
    }
}

impl ExecutionPlan for InMemoryCacheExec {
    fn name(&self) -> &str {
        "InMemoryDataSourceExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.inner.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inner: children[0].clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let once = {
            let inner_display = displayable(self.inner.as_ref()).one_line().to_string();
            let entry = CACHE.entry((inner_display, partition));
            if matches!(entry, Entry::Vacant(_))
                && !context
                    .session_config()
                    .options()
                    .extensions
                    .get::<WarmingUpMarker>()
                    .map(|v| v.is_warming_up)
                    .unwrap_or_default()
            {
                return exec_err!("InMemoryCacheExec is not yet warmed up");
            }
            let once = entry.or_insert(Arc::new(OnceCell::new()));
            once.value().clone()
        };

        let inner = Arc::clone(&self.inner);

        let stream = async move {
            let batches = once
                .get_or_try_init(|| async move {
                    let mut stream = inner.execute(partition, context)?;
                    let mut batches = vec![];
                    while let Some(batch) = stream.next().await {
                        batches.push(batch?);
                    }
                    Ok::<_, DataFusionError>(batches)
                })
                .await?;
            Ok(batches.clone())
        }
        .into_stream()
        .map(|v| match v {
            Ok(batch) => futures::stream::iter(batch.into_iter().map(Ok)).boxed(),
            Err(err) => futures::stream::once(async { Err(err) }).boxed(),
        })
        .flatten();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.inner.schema(),
            stream,
        )))
    }
}

impl DisplayAs for InMemoryCacheExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        writeln!(f, "InMemoryDataSourceExec")
    }
}

#[derive(Clone, PartialEq, ::prost::Message)]
struct InMemoryCacheExecProto {
    #[prost(string, tag = "1")]
    name: String,
}

#[derive(Debug)]
pub struct InMemoryCacheExecCodec;

impl PhysicalExtensionCodec for InMemoryCacheExecCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let Ok(proto) = InMemoryCacheExecProto::decode(buf) else {
            return plan_err!("no InMemoryDataSourceExecProto");
        };
        if proto.name != "InMemoryDataSourceExec" {
            return plan_err!("unsupported InMemoryDataSourceExec proto: {:?}", proto.name);
        };
        Ok(Arc::new(InMemoryCacheExec {
            inner: inputs[0].clone(),
        }))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        if !node.as_any().is::<InMemoryCacheExec>() {
            return plan_err!("no InMemoryDataSourceExec");
        };
        let proto = InMemoryCacheExecProto {
            name: "InMemoryDataSourceExec".to_string(),
        };
        let Ok(_) = proto.encode(buf) else {
            return plan_err!("no InMemoryDataSourceExecProto");
        };

        Ok(())
    }
}

/// Wraps any plan without children with an [InMemoryCacheExec] node.
#[derive(Debug)]
pub struct InMemoryDataSourceRule;

impl PhysicalOptimizerRule for InMemoryDataSourceRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(plan
            .transform_up(|plan| {
                if plan.children().is_empty() {
                    Ok(Transformed::yes(Arc::new(InMemoryCacheExec {
                        inner: plan.clone(),
                    })))
                } else {
                    Ok(Transformed::no(plan))
                }
            })?
            .data)
    }

    fn name(&self) -> &str {
        "InMemoryDataSourceRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
