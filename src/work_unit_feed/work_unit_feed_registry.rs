use crate::config_extension_ext::set_distributed_option_extension;
use crate::{DistributedConfig, WorkUnit, WorkUnitFeed, WorkUnitFeedProvider};
use datafusion::common::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use futures::StreamExt;
use futures::stream::BoxStream;
use std::sync::Arc;
use uuid::Uuid;

/// Type-erased view over a [`WorkUnitFeed<T>`] for any `T: WorkUnitFeedProvider`.
///
/// The distributed layer needs to traverse arbitrary user plans and, for each leaf node
/// that embeds a [`WorkUnitFeed`], pull the per-partition work unit streams and forward
/// them to the workers. Since every leaf may use a different concrete `T`, the registry
/// hands out `&dyn ErasedWorkUnitFeed` fat pointers so callers can work with feeds of
/// any shape uniformly.
pub(crate) trait ErasedWorkUnitFeed: Send + Sync {
    /// Unique identifier of the feed (same UUID as the concrete `WorkUnitFeed`).
    fn id(&self) -> Uuid;

    /// Produces a stream of boxed [`WorkUnit`]s for the given partition.
    ///
    /// Each item is boxed to erase the concrete `T::WorkUnit` type. Callers
    /// typically just need `WorkUnit` trait methods like [`WorkUnit::encode_to_bytes`].
    fn feed(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Box<dyn WorkUnit>>>>;
}

impl<T> ErasedWorkUnitFeed for WorkUnitFeed<T>
where
    T: WorkUnitFeedProvider + 'static,
    T::WorkUnit: 'static,
{
    fn id(&self) -> Uuid {
        self.id
    }

    fn feed(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<BoxStream<'static, Result<Box<dyn WorkUnit>>>> {
        let stream = WorkUnitFeed::feed(self, partition, ctx)?;
        Ok(stream
            .map(|res| res.map(|wu| Box::new(wu) as Box<dyn WorkUnit>))
            .boxed())
    }
}

/// A registry entry is a closure that, given an [`ExecutionPlan`], returns a reference
/// to the [`WorkUnitFeed<T>`] stored inside it (if the plan is of the expected type).
///
/// Users register these via [`crate::DistributedExt::set_distributed_work_unit_feed`].
/// This trait abstracts over the concrete `T` so the registry can store a heterogeneous
/// list of getters; each getter hands back a `&dyn ErasedWorkUnitFeed` so callers can
/// interact with the feed without knowing `T`.
trait WorkUnitFeedGetter: Send + Sync {
    fn get_work_unit_feed<'a>(
        &self,
        node: &'a Arc<dyn ExecutionPlan>,
    ) -> Option<&'a dyn ErasedWorkUnitFeed>;
}

/// Blanket impl: any closure `Fn(&Arc<dyn ExecutionPlan>) -> Option<&WorkUnitFeed<T>>`
/// is a registry entry. The higher-ranked lifetime bound (`for<'a>`) lets the
/// returned reference borrow from the input `node`.
impl<T, F> WorkUnitFeedGetter for F
where
    T: WorkUnitFeedProvider + 'static,
    T::WorkUnit: 'static,
    F: for<'a> Fn(&'a Arc<dyn ExecutionPlan>) -> Option<&'a WorkUnitFeed<T>>
        + Send
        + Sync
        + 'static,
{
    fn get_work_unit_feed<'a>(
        &self,
        node: &'a Arc<dyn ExecutionPlan>,
    ) -> Option<&'a dyn ErasedWorkUnitFeed> {
        // Coerce the concrete `&WorkUnitFeed<T>` to `&dyn ErasedWorkUnitFeed`
        // at this boundary, which is where the type is still known.
        (self)(node).map(|feed| feed as &dyn ErasedWorkUnitFeed)
    }
}

#[derive(Default, Clone)]
pub(crate) struct WorkUnitFeedRegistry {
    entries: Vec<Arc<dyn WorkUnitFeedGetter>>,
}

impl WorkUnitFeedRegistry {
    pub(crate) fn get_work_unit_feed<'a>(
        &self,
        node: &'a Arc<dyn ExecutionPlan>,
    ) -> Option<&'a dyn ErasedWorkUnitFeed> {
        for entry in &self.entries {
            if let Some(feed) = entry.get_work_unit_feed(node) {
                return Some(feed);
            }
        }
        None
    }
}

pub(crate) fn set_distributed_work_unit_feed<T, F>(cfg: &mut SessionConfig, getter: F)
where
    T: WorkUnitFeedProvider + 'static,
    T::WorkUnit: 'static,
    F: Fn(&Arc<dyn ExecutionPlan>) -> Option<&WorkUnitFeed<T>> + Send + Sync + 'static,
{
    let opts = cfg.options_mut();
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg
            .__private_work_unit_feed_registry
            .entries
            .push(Arc::new(getter));
    } else {
        let mut registry = WorkUnitFeedRegistry::default();
        registry.entries.push(Arc::new(getter));
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_work_unit_feed_registry: registry,
                ..Default::default()
            },
        )
    }
}
