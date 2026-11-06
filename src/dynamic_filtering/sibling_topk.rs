use crate::MaybeEncoded;
use crate::dynamic_filtering::{
    apply_dynamic_filter_update, discover_dynamic_filter_consumers,
    is_remote_dynamic_filtering_enabled,
};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, extensions_options, internal_err};
use datafusion::config::ConfigExtension;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{DynamicFilterPhysicalExpr, lit};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::filter_pushdown::FilterPushdown;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use datafusion::prelude::SessionConfig;
use prost::Message;
use std::any::Any;
use std::sync::Arc;

extensions_options! {
    /// Optional sharing of Top-K bounds between tasks executing the same sort.
    ///
    /// Install with [`crate::DistributedExt::with_distributed_option_extension`].
    /// Remote dynamic filtering and DataFusion's general and Top-K dynamic filter pushdown
    /// must also be enabled. Local sort bounds retain their independent state.
    pub struct SiblingTopKConfig {
        /// Share fetched-sort bounds with sibling tasks. Unsupported pushdown paths keep
        /// their existing local filtering without applying the sibling bound.
        pub enabled: bool, default = false
    }
}

impl ConfigExtension for SiblingTopKConfig {
    const PREFIX: &'static str = "distributed_sibling_topk";
}

pub(crate) type SiblingTopKConsumers = HashMap<u64, Arc<DynamicFilterPhysicalExpr>>;

pub(crate) fn sibling_topk_enabled(config: &SessionConfig) -> Result<bool> {
    Ok(config
        .options()
        .extensions
        .get::<SiblingTopKConfig>()
        .is_some_and(|config| config.enabled)
        && config
            .options()
            .optimizer
            .enable_topk_dynamic_filter_pushdown
        && is_remote_dynamic_filtering_enabled(config)?)
}

/// Restrict sharing to fetched sorts whose bounds already reach a real local consumer.
pub(crate) fn sibling_topk_producer_ids(plan: &Arc<dyn ExecutionPlan>) -> Result<HashSet<u64>> {
    let consumers: HashSet<_> = discover_dynamic_filter_consumers(plan)?
        .consumers
        .into_iter()
        .map(|consumer| consumer.id)
        .collect();
    let mut ids = HashSet::new();
    plan.apply(|node| {
        if node
            .downcast_ref::<SortExec>()
            .is_some_and(|sort| sort.fetch().is_some())
        {
            for expression in node.dynamic_expressions_produced() {
                if expression.is::<DynamicFilterPhysicalExpr>() {
                    let Some(id) = expression.expression_id() else {
                        return internal_err!("Top-K dynamic filter is missing its expression ID");
                    };
                    if consumers.contains(&id) {
                        ids.insert(id);
                    }
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(ids)
}

/// Give sibling updates their own state and runtime ID, leaving the local producer intact.
pub(crate) fn attach_sibling_topk_consumers(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: &TaskContext,
    requested_producer_ids: &[u64],
) -> Result<(Arc<dyn ExecutionPlan>, SiblingTopKConsumers)> {
    let mut consumers = HashMap::new();
    if requested_producer_ids.is_empty() {
        return Ok((plan, consumers));
    }
    let eligible = if sibling_topk_enabled(task_ctx.session_config())? {
        sibling_topk_producer_ids(&plan)?
    } else {
        HashSet::new()
    };
    let mut occupied = expression_ids(&plan)?;
    let transformed = plan.transform_up(|node| {
        let Some(sort) = node.downcast_ref::<SortExec>() else {
            return Ok(Transformed::no(node));
        };
        if sort.fetch().is_none() {
            return Ok(Transformed::no(node));
        }
        let Some(producer) = sort
            .dynamic_expressions_produced()
            .into_iter()
            .find(|expr| {
                expr.expression_id()
                    .is_some_and(|id| requested_producer_ids.contains(&id))
            })
        else {
            return Ok(Transformed::no(node));
        };
        let id = producer
            .expression_id()
            .expect("requested producer has an ID");
        occupied.insert(id);
        let remote = match consumers.get(&id) {
            Some(remote) => Arc::clone(remote),
            None => new_remote_filter(&producer, &mut occupied)?,
        };
        // Worker customization can disable pushdown or remove the local consumer after
        // registration. Keep a separate target even when no pushdown remains possible.
        consumers.insert(id, remote.clone());
        // Public downcasts may delegate through a wrapper with different child semantics.
        if !(node.as_ref() as &dyn Any).is::<SortExec>() || !eligible.contains(&id) {
            return Ok(Transformed::no(node));
        }
        let temporary = Arc::new(sort.clone().with_dynamic_filter_expr(remote.clone())?);
        let optimized = FilterPushdown::new_post_optimization()
            .optimize(temporary, task_ctx.session_config().options())?;
        // Keep an unattached target for unsupported custom pushdown paths. The coordinator
        // can still send updates, but they must never overwrite this task's local producer.
        let Some(updated_sort) = optimized.downcast_ref::<SortExec>() else {
            return Ok(Transformed::no(node));
        };
        let input = updated_sort.input();
        let attached = discover_dynamic_filter_consumers(input)?
            .consumers
            .iter()
            .any(|consumer| Some(consumer.id) == remote.expression_id());
        if !attached || operator_shape(input)? != operator_shape(sort.input())? {
            return Ok(Transformed::no(node));
        }
        let updated = node.replace_children(
            vec![Arc::clone(input)],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        Ok(Transformed::yes(updated))
    })?;
    Ok((transformed.data, consumers))
}

pub(crate) fn apply_sibling_topk_update(
    original_id: u64,
    target: &Arc<DynamicFilterPhysicalExpr>,
    source: &MaybeEncoded<Arc<dyn PhysicalExpr>>,
    source_schema: &Schema,
    task_ctx: &TaskContext,
) -> Result<()> {
    let mut proto = source.to_proto(task_ctx)?;
    if proto.expr_id != Some(original_id) {
        return internal_err!("sibling Top-K update has a mismatched producer ID");
    }
    proto.expr_id = target.expression_id();
    apply_dynamic_filter_update(
        target,
        &MaybeEncoded::Encoded(proto.encode_to_vec()),
        source_schema,
        task_ctx,
    )
}

fn expression_ids(plan: &Arc<dyn ExecutionPlan>) -> Result<HashSet<u64>> {
    let mut ids = HashSet::new();
    plan.apply(|node| {
        node.apply_expressions(&mut |root| {
            root.apply(|expression| {
                if let Some(id) = expression.expression_id() {
                    ids.insert(id);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(ids)
}

fn new_remote_filter(
    producer: &Arc<dyn PhysicalExpr>,
    occupied: &mut HashSet<u64>,
) -> Result<Arc<DynamicFilterPhysicalExpr>> {
    // Decoded expression IDs do not necessarily advance the process-local ID generator.
    for _ in 0..=occupied.len() {
        let remote = Arc::new(DynamicFilterPhysicalExpr::new(
            producer.children().into_iter().map(Arc::clone).collect(),
            lit(true),
        ));
        if remote.expression_id().is_some_and(|id| occupied.insert(id)) {
            return Ok(remote);
        }
    }
    internal_err!("could not allocate a distinct sibling Top-K filter ID")
}

fn operator_shape(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<(String, usize, usize)>> {
    let mut shape = vec![];
    plan.apply(|node| {
        shape.push((
            node.name().to_owned(),
            node.children().len(),
            node.properties().partitioning.partition_count(),
        ));
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(shape)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DistributedConfig, DistributedExt};
    use datafusion::arrow::array::{BooleanArray, Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream,
    };
    use futures::FutureExt;
    use std::fmt::Formatter;

    #[test]
    fn delegated_sort_identity_does_not_replace_the_wrappers_child_with_sort_input() -> Result<()> {
        let config = SessionConfig::new()
            .with_distributed_option_extension(DistributedConfig::default())
            .with_distributed_option_extension(SiblingTopKConfig { enabled: true });
        let context = TaskContext::default().with_session_config(config);
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let local = Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
        let source = Arc::new(ParquetSource::new(schema).with_predicate(local.clone()));
        let input = DataSourceExec::from_data_source(
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
                .with_file(PartitionedFile::new("unused.parquet", 1))
                .build(),
        );
        let order = LexOrdering::new(vec![PhysicalSortExpr::new_default(key)]).unwrap();
        let sort = Arc::new(
            SortExec::new(order, input)
                .with_fetch(Some(1))
                .with_dynamic_filter_expr(local.clone())?,
        );
        let wrapped: Arc<dyn ExecutionPlan> = Arc::new(SortWrapper(sort));
        let id = local.expression_id().unwrap();

        let (updated, targets) = attach_sibling_topk_consumers(wrapped, &context, &[id])?;

        let wrapper = (updated.as_ref() as &dyn Any)
            .downcast_ref::<SortWrapper>()
            .unwrap();
        let child = wrapper.children()[0];
        assert!((child.as_ref() as &dyn Any).is::<SortExec>());
        let producer = child.dynamic_expressions_produced().pop().unwrap();
        assert!(Arc::ptr_eq(&producer, &(local as Arc<dyn PhysicalExpr>)));
        let remote_id = targets[&id].expression_id().unwrap();
        assert_ne!(id, remote_id);
        assert!(
            discover_dynamic_filter_consumers(child)?
                .consumers
                .iter()
                .any(|consumer| consumer.id == remote_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn requested_producer_keeps_a_sink_when_worker_disables_sharing_and_removes_its_consumer()
    -> Result<()> {
        let config = SessionConfig::new()
            .with_distributed_option_extension(DistributedConfig::default())
            .with_distributed_option_extension(SiblingTopKConfig { enabled: false });
        let context = TaskContext::default().with_session_config(config);
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let local = Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
        let order = LexOrdering::new(vec![PhysicalSortExpr::new_default(key)]).unwrap();
        let sort: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(order, Arc::new(EmptyExec::new(schema.clone())))
                .with_fetch(Some(1))
                .with_dynamic_filter_expr(local.clone())?,
        );
        let id = local.expression_id().unwrap();
        assert!(sibling_topk_producer_ids(&sort)?.is_empty());

        let (updated, targets) = attach_sibling_topk_consumers(sort.clone(), &context, &[id])?;

        assert!(Arc::ptr_eq(&sort, &updated));
        assert!(
            discover_dynamic_filter_consumers(&updated)?
                .consumers
                .is_empty()
        );
        let bound = Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(false)));
        bound.mark_complete();
        let mut proto = MaybeEncoded::Decoded(bound as Arc<dyn PhysicalExpr>).to_proto(&context)?;
        proto.expr_id = Some(id);
        apply_sibling_topk_update(
            id,
            &targets[&id],
            &MaybeEncoded::Encoded(proto.encode_to_vec()),
            &schema,
            &context,
        )?;
        assert_eq!(targets[&id].current()?.as_ref(), lit(false).as_ref());
        assert_eq!(local.current()?.as_ref(), lit(true).as_ref());
        assert!(futures::poll!(local.wait_complete().boxed()).is_pending());
        Ok(())
    }

    #[tokio::test]
    async fn remote_completion_preserves_local_updates_and_projection_remapping() -> Result<()> {
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let source = Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let local = Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let remote = Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let remapped = remote
            .clone()
            .with_new_children(vec![Arc::new(Column::new("alias", 1))])?;
        source.update(Arc::new(BinaryExpr::new(
            key.clone(),
            Operator::GtEq,
            lit(10_i32),
        )))?;
        source.mark_complete();
        let id = source.expression_id().unwrap();
        let source: Arc<dyn PhysicalExpr> = source;
        let context = TaskContext::default();
        let schema = Schema::new(vec![Field::new("key", DataType::Int32, true)]);
        apply_sibling_topk_update(
            id,
            &remote,
            &MaybeEncoded::Decoded(source),
            &schema,
            &context,
        )?;
        assert!(futures::poll!(local.wait_complete().boxed()).is_pending());
        local.update(Arc::new(BinaryExpr::new(key, Operator::GtEq, lit(20_i32))))?;
        assert_ne!(local.current()?.to_string(), remote.current()?.to_string());

        let batch = RecordBatch::try_from_iter(vec![
            ("ignored", Arc::new(Int32Array::from(vec![0, 0, 0, 0])) as _),
            (
                "alias",
                Arc::new(Int32Array::from(vec![Some(9), Some(10), None, Some(11)])) as _,
            ),
        ])?;
        let evaluated = remapped.evaluate(&batch)?.into_array(4)?;
        assert_eq!(
            evaluated.as_any().downcast_ref::<BooleanArray>().unwrap(),
            &BooleanArray::from(vec![Some(false), Some(true), None, Some(true)])
        );
        Ok(())
    }

    /// Unlike a transparent wrapper, this wrapper exposes the wrapped sort as its child.
    #[derive(Debug)]
    struct SortWrapper(Arc<dyn ExecutionPlan>);

    impl DisplayAs for SortWrapper {
        fn fmt_as(&self, format: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
            self.0.fmt_as(format, f)
        }
    }

    impl ExecutionPlan for SortWrapper {
        fn name(&self) -> &str {
            "SortWrapper"
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            self.0.properties()
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.0]
        }
        fn apply_expressions(
            &self,
            f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            self.0.apply_expressions(f)
        }
        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self(children[0].clone())))
        }
        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.0.execute(partition, context)
        }
        fn dynamic_expressions_produced(&self) -> Vec<Arc<dyn PhysicalExpr>> {
            self.0.dynamic_expressions_produced()
        }
        fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
            Some(self.0.as_ref())
        }
    }
}
