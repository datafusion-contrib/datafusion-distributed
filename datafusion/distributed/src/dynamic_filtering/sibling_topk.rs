use crate::DistributedConfig;
use crate::dynamic_filtering::{
    discover_dynamic_filter_consumers, is_remote_dynamic_filtering_enabled,
};
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, internal_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{DynamicFilterPhysicalExpr, lit};
use datafusion::physical_expr_common::physical_expr::is_volatile;
use datafusion::physical_plan::filter_pushdown::{FilterPushdownPhase, PushedDown};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, replace_children_if_necessary};
use datafusion::prelude::SessionConfig;
use std::any::Any;
use std::sync::Arc;

pub(crate) type SiblingTopKConsumers = HashMap<u64, Arc<DynamicFilterPhysicalExpr>>;

pub(crate) fn sibling_topk_enabled(config: &SessionConfig) -> Result<bool> {
    Ok(
        DistributedConfig::from_session_config(config)?.enable_sibling_topk_filter_pushdown
            && config
                .options()
                .optimizer
                .enable_topk_dynamic_filter_pushdown
            && is_remote_dynamic_filtering_enabled(config)?,
    )
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
    plan.apply(|node| {
        let Some(sort) = node.downcast_ref::<SortExec>() else {
            return Ok(TreeNodeRecursion::Continue);
        };
        if sort.fetch().is_none() {
            return Ok(TreeNodeRecursion::Continue);
        }
        for producer in sort.dynamic_expressions_produced() {
            let Some(id) = producer.expression_id() else {
                return internal_err!("Top-K dynamic filter is missing its expression ID");
            };
            if requested_producer_ids.contains(&id) && !consumers.contains_key(&id) {
                occupied.insert(id);
                consumers.insert(id, new_remote_filter(&producer, &mut occupied)?);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    // Keep separate targets even if worker customization removed a consumer or disabled
    // pushdown. Requested updates must never fall through to the local producer state.
    let plan = if eligible.is_empty() {
        plan
    } else {
        attach_to_sources(
            plan,
            vec![],
            &eligible,
            &consumers,
            task_ctx.session_config().options(),
        )?
    };
    Ok((plan, consumers))
}

/// Route only optional sibling predicates. Existing producers and their pushed filters
/// remain untouched; unsupported paths need no residual FilterExec.
fn attach_to_sources(
    plan: Arc<dyn ExecutionPlan>,
    mut filters: Vec<Arc<dyn PhysicalExpr>>,
    eligible: &HashSet<u64>,
    consumers: &SiblingTopKConsumers,
    config: &ConfigOptions,
) -> Result<Arc<dyn ExecutionPlan>> {
    filters.retain(|filter| !is_volatile(filter));
    // A delegated downcast can expose an inner source without a way to rebuild its wrapper.
    let actual = plan.as_ref() as &dyn Any;
    if let Some(scan) = actual.downcast_ref::<DataSourceExec>() {
        if filters.is_empty() {
            return Ok(plan);
        }
        return Ok(
            match scan
                .data_source()
                .try_pushdown_filters(filters, config)?
                .updated_node
            {
                Some(source)
                    if source.eq_properties().schema() == &scan.schema()
                        && source.output_partitioning().partition_count()
                            == scan.properties().partitioning.partition_count() =>
                {
                    Arc::new(scan.clone().with_data_source(source))
                }
                // Task output partitions and schema were assigned before worker attachment.
                _ => plan,
            },
        );
    }
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let filter_count = filters.len();
    let mut child_filters = if filters.is_empty() {
        vec![vec![]; children.len()]
    } else {
        let description =
            plan.gather_filters_for_pushdown(FilterPushdownPhase::Post, filters, config)?;
        let parent_filters = description.parent_filters();
        if parent_filters.len() != children.len()
            || parent_filters
                .iter()
                .any(|filters| filters.len() != filter_count)
        {
            return internal_err!("{} returned invalid sibling filter routes", plan.name());
        }
        parent_filters
            .into_iter()
            .map(|filters| {
                filters
                    .into_iter()
                    .filter_map(|filter| {
                        matches!(filter.discriminant, PushedDown::Yes).then_some(filter.predicate)
                    })
                    .collect()
            })
            .collect()
    };
    if let Some(sort) = actual.downcast_ref::<SortExec>() {
        for producer in sort.dynamic_expressions_produced() {
            if let Some(id) = producer.expression_id()
                && eligible.contains(&id)
                && let Some(remote) = consumers.get(&id)
            {
                child_filters[0].push(Arc::clone(remote) as _);
            }
        }
    }
    let updated_children = children
        .into_iter()
        .zip(child_filters)
        .map(|(child, filters)| {
            attach_to_sources(Arc::clone(child), filters, eligible, consumers, config)
        })
        .collect::<Result<_>>()?;
    replace_children_if_necessary(plan, updated_children)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynamic_filtering::apply_dynamic_filter_update;
    use crate::{DistributedExt, MaybeEncoded};
    use datafusion::arrow::array::{BooleanArray, Int32Array, RecordBatch};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{BinaryExpr, Column};
    use datafusion::physical_expr::{LexOrdering, Partitioning, PhysicalSortExpr};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::{
        DisplayAs, DisplayFormatType, PlanProperties, SendableRecordBatchStream,
    };
    use futures::FutureExt;
    use prost::Message;
    use std::fmt::Formatter;

    #[test]
    fn sharing_requires_remote_and_both_dynamic_filter_pushdown_settings() -> Result<()> {
        let mut config =
            SessionConfig::new().with_distributed_option_extension(DistributedConfig {
                enable_sibling_topk_filter_pushdown: true,
                ..Default::default()
            });
        assert!(sibling_topk_enabled(&config)?);

        config
            .options_mut()
            .optimizer
            .enable_topk_dynamic_filter_pushdown = false;
        assert!(!sibling_topk_enabled(&config)?);
        config
            .options_mut()
            .optimizer
            .enable_topk_dynamic_filter_pushdown = true;

        config
            .options_mut()
            .optimizer
            .enable_dynamic_filter_pushdown = false;
        assert!(!sibling_topk_enabled(&config)?);
        config
            .options_mut()
            .optimizer
            .enable_dynamic_filter_pushdown = true;

        DistributedConfig::from_config_options_mut(config.options_mut())?.remote_dynamic_filters =
            false;
        assert!(!sibling_topk_enabled(&config)?);
        Ok(())
    }

    #[test]
    fn delegated_sort_identity_does_not_replace_the_wrappers_child_with_sort_input() -> Result<()> {
        let config = SessionConfig::new().with_distributed_option_extension(DistributedConfig {
            enable_sibling_topk_filter_pushdown: true,
            ..Default::default()
        });
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

    #[test]
    fn source_update_cannot_replace_the_assigned_partition_count() -> Result<()> {
        let config = SessionConfig::new().with_distributed_option_extension(DistributedConfig {
            enable_sibling_topk_filter_pushdown: true,
            ..Default::default()
        });
        let context = TaskContext::default().with_session_config(config);
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let local = Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
        let source = Arc::new(ParquetSource::new(schema).with_predicate(local.clone()));
        let source = FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), source)
            .with_file(PartitionedFile::new("unused.parquet", 1))
            .build();
        // Replacing the source would recompute this assigned interface from its one group.
        let scan = DataSourceExec::new(Arc::new(source))
            .with_partitioning(Partitioning::UnknownPartitioning(2));
        let order = LexOrdering::new(vec![PhysicalSortExpr::new_default(key)]).unwrap();
        let plan: Arc<dyn ExecutionPlan> = Arc::new(
            SortExec::new(order, Arc::new(scan))
                .with_fetch(Some(1))
                .with_dynamic_filter_expr(local.clone())?,
        );
        let id = local.expression_id().unwrap();
        assert!(sibling_topk_producer_ids(&plan)?.contains(&id));

        let (updated, targets) = attach_sibling_topk_consumers(plan.clone(), &context, &[id])?;

        assert!(Arc::ptr_eq(&plan, &updated));
        assert_eq!(targets.len(), 1);
        let remote_id = targets[&id].expression_id().unwrap();
        assert_ne!(id, remote_id);
        assert!(
            discover_dynamic_filter_consumers(&updated)?
                .consumers
                .iter()
                .all(|consumer| consumer.id != remote_id)
        );
        Ok(())
    }

    #[tokio::test]
    async fn requested_producer_keeps_a_sink_when_worker_disables_sharing_and_removes_its_consumer()
    -> Result<()> {
        let config =
            SessionConfig::new().with_distributed_option_extension(DistributedConfig::default());
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
        apply_dynamic_filter_update(
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
        let source = MaybeEncoded::Decoded(source as Arc<dyn PhysicalExpr>);
        let context = TaskContext::default();
        let schema = Schema::new(vec![Field::new("key", DataType::Int32, true)]);
        assert!(
            apply_dynamic_filter_update(
                remote.expression_id().unwrap(),
                &remote,
                &source,
                &schema,
                &context,
            )
            .is_err()
        );
        assert_eq!(remote.current()?.as_ref(), lit(true).as_ref());
        apply_dynamic_filter_update(id, &remote, &source, &schema, &context)?;
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
