use crate::dynamic_filtering::{discover_dynamic_filter_consumers, dynamic_filter_producer_schema};
use crate::{
    ApplyDynamicFilter, CoordinatorToWorkerMsg, MaybeEncoded, ProducedDynamicFilter, TaskKey,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, exec_err, internal_err};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_expr_common::metrics::{ExecutionPlanMetricsSet, MetricBuilder};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::metrics::Count;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use datafusion_proto::protobuf::{
    PhysicalBinaryExprNode, PhysicalDynamicFilterNode, PhysicalExprNode,
};
use prost::Message;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DynamicFilterMergePolicy {
    /// Wait for every planned producer task to report a complete dynamic filter, then
    /// union and forward the filter. Used for partitioned joins.
    UnionWhenAllComplete,
    /// Wait for any producer to report a complete dynamic filter and forward it.
    /// Used for collect left joins.
    ForwardFirstComplete,
    /// Intersect all updates from producers and immediately forward them. Used
    /// for TopK and Min/Max aggregate dynamic filters where intersecting updates
    /// is safe.
    IntersectOnUpdate,
}

#[derive(Default)]
pub(super) struct PlannedDynamicFilter {
    // Producer and consumer tasks for a dynamic filter.
    //
    // Note that it is not guaranteed that every task within a stage produces / consumes dynamic filters. For
    // example, a distributed union may prevent a dynamic filter from appearing in all tasks. So, we
    // store task keys rather than stage ids.
    //
    /// Registered producers and their latest accepted snapshots, if any.
    pub(super) producers: HashMap<TaskKey, Option<PhysicalDynamicFilterNode>>,
    pub(super) consumer_tasks: HashSet<TaskKey>,
    /// Eligible producers receive updates through independent remote filter state.
    sibling_tasks: HashSet<TaskKey>,

    /// Populated upon task registration when a producer is found.
    ///
    /// The behavior to take when an update arrives from a producer.
    merge_mode: Option<DynamicFilterMergePolicy>,
    /// Schema of the original producer arguments, shared by all producer tasks.
    pub(super) producer_schema: Option<SchemaRef>,

    /// Populated when an update is ready to be sent to consumers.
    ///
    /// Full dynamic filter containing the merged predicate and its completion state.
    pub(super) merged: Option<PhysicalDynamicFilterNode>,
    /// Immutable snapshot shared by local and remote delivery, encoded once per change.
    merged_bytes: Option<Vec<u8>>,
}

#[derive(Default)]
pub(super) struct DynamicFilterRegistryState {
    pub(super) filters: HashMap<u64, PlannedDynamicFilter>,
    /// Track which stages have registered all of their tasks.
    pub(super) sealed_stages: HashSet<usize>,
    task_senders: HashMap<TaskKey, UnboundedSender<CoordinatorToWorkerMsg>>,
    delivered: HashSet<(u64, TaskKey)>,
}

/// Query-scoped hub for distributed dynamic filtering.
///
/// It stores the locations of dynamic filters and their runtime state. Informs the coordinator
/// - where dynamic filter updates are coming from
/// - how/if dynamic filter updates should be merged
/// - where dynamic filter updates should be forwarded
pub(crate) struct DynamicFilterRegistry {
    pub(super) state: Mutex<DynamicFilterRegistryState>,
    dynamic_filter_updates_received: Count,
    query_finished: CancellationToken,
}

impl DynamicFilterRegistry {
    pub(crate) fn new(
        metrics: &ExecutionPlanMetricsSet,
        query_finished: CancellationToken,
    ) -> Self {
        Self {
            state: Mutex::new(DynamicFilterRegistryState::default()),
            dynamic_filter_updates_received: MetricBuilder::new(metrics)
                .global_counter("dynamic_filter_updates_received"),
            query_finished,
        }
    }

    pub(crate) fn record_update_received(&self) {
        self.dynamic_filter_updates_received.add(1);
    }

    /// Adds any dynamic filter producers and consumers found in `plan` to the registry.
    pub(crate) fn register_task(
        &self,
        task_specialized_plan: &Arc<dyn ExecutionPlan>,
        task_key: TaskKey,
        share_sibling_topk: bool,
    ) -> Result<()> {
        let mut producers = vec![];
        // Anchors are metadata only; only real consumers need forwarded updates.
        let consumers = discover_dynamic_filter_consumers(task_specialized_plan)?.consumers;

        task_specialized_plan.apply(|node| {
            // `CollectLeft` joins broadcast an equivalent build side to every producer task,
            // so we can forward the first completed dynamic filter.
            //
            // TopK and MIN/MAX bounds are independently useful and incremental, so we can use
            // all updates.
            //
            // Any remaining dynamic filter should wait for completion and should be merged
            // at the coordinator before being applied.
            let merge_mode = if node
                .downcast_ref::<HashJoinExec>()
                .is_some_and(|join| matches!(join.partition_mode(), PartitionMode::CollectLeft))
            {
                DynamicFilterMergePolicy::ForwardFirstComplete
            } else if node.is::<SortExec>() || node.is::<AggregateExec>() {
                DynamicFilterMergePolicy::IntersectOnUpdate
            } else {
                DynamicFilterMergePolicy::UnionWhenAllComplete
            };
            let produced_ids: HashSet<_> = node
                .dynamic_expressions_produced()
                .into_iter()
                .filter_map(|expression| {
                    expression
                        .downcast_ref::<DynamicFilterPhysicalExpr>()
                        .map(|_| expression.expression_id())
                })
                .map(|id| match id {
                    Some(id) => Ok(id),
                    None => {
                        internal_err!("DynamicFilterPhysicalExpr did not have an expression ID")
                    }
                })
                .collect::<Result<_>>()?;
            if !produced_ids.is_empty() {
                let schema = dynamic_filter_producer_schema(node.as_ref())?;
                let sibling_topk = share_sibling_topk
                    && node
                        .downcast_ref::<SortExec>()
                        .is_some_and(|sort| sort.fetch().is_some());
                producers.extend(produced_ids.iter().map(|id| {
                    let share = sibling_topk && consumers.iter().any(|consumer| consumer.id == *id);
                    (*id, merge_mode, Arc::clone(&schema), share)
                }));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        for (id, merge_mode, producer_schema, share) in producers {
            let filter = state.filters.entry(id).or_default();
            if let Some(existing) = &filter.producer_schema
                && existing != &producer_schema
            {
                return internal_err!("Dynamic filter {id} has conflicting producer schemas");
            }
            filter.producer_schema.get_or_insert(producer_schema);
            if share {
                filter.sibling_tasks.insert(task_key);
            }
            filter.merge_mode = Some(match filter.merge_mode {
                Some(existing) if existing != merge_mode => {
                    return internal_err!(
                        "Dynamic filter {id} has conflicting merge modes: \
                         {existing:?} and {merge_mode:?}"
                    );
                }
                Some(existing) => existing,
                None => merge_mode,
            });
            filter.producers.entry(task_key).or_insert(None);
        }
        for consumer in consumers {
            state
                .filters
                .entry(consumer.id)
                .or_default()
                .consumer_tasks
                .insert(task_key);
        }
        Ok(())
    }

    pub(crate) fn register_sender(
        &self,
        task_key: TaskKey,
        sender: UnboundedSender<CoordinatorToWorkerMsg>,
    ) -> Result<()> {
        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        state.task_senders.insert(task_key, sender);
        state.delivered.retain(|(_, task)| *task != task_key);
        let ids = state.filters.keys().copied().collect::<Vec<_>>();
        for id in ids {
            self.dispatch(&mut state, id)?;
        }
        Ok(())
    }

    /// Mark that a stage has registered all of its tasks.
    pub(crate) fn seal_stage(&self, stage_id: usize) -> Result<()> {
        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        state.sealed_stages.insert(stage_id);
        let ids = state.filters.keys().copied().collect::<Vec<_>>();
        for id in ids {
            if Self::merge(&mut state, id) {
                self.dispatch(&mut state, id)?;
            }
        }
        Ok(())
    }

    /// Records a producer's latest dynamic-filter update and recomputes the merged filter.
    pub(crate) fn record_dynamic_filter_update(
        &self,
        task_key: TaskKey,
        report: ProducedDynamicFilter,
        task_ctx: &TaskContext,
    ) -> Result<()> {
        self.record_update_received();
        let expression = report.expression.to_proto(task_ctx)?;
        if expression.expr_id != Some(report.expression_id) {
            return exec_err!(
                "Dynamic filter {} from task {task_key:?} has mismatched expression ID {:?}",
                report.expression_id,
                expression.expr_id
            );
        }
        let Some(ExprType::DynamicFilter(dynamic_filter)) = expression.expr_type else {
            return exec_err!(
                "Expected dynamic filter expression for filter {} from task {task_key:?}",
                report.expression_id
            );
        };
        if dynamic_filter.inner_expr.is_none() {
            return exec_err!(
                "Missing inner expression for dynamic filter {} from task {task_key:?}",
                report.expression_id
            );
        }

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        let Some(filter) = state.filters.get_mut(&report.expression_id) else {
            return exec_err!(
                "Received update for unregistered dynamic filter {} from task {task_key:?}",
                report.expression_id
            );
        };
        let Some(previous) = filter.producers.get_mut(&task_key) else {
            return exec_err!(
                "Task {task_key:?} is not a registered producer of dynamic filter {}",
                report.expression_id
            );
        };
        if (filter.merge_mode != Some(DynamicFilterMergePolicy::IntersectOnUpdate)
            && !dynamic_filter.is_complete)
            || previous
                .as_ref()
                .is_some_and(|previous| previous.is_complete || previous == dynamic_filter.as_ref())
        {
            return Ok(());
        }
        *previous = Some(*dynamic_filter);
        if Self::merge(&mut state, report.expression_id) {
            self.dispatch(&mut state, report.expression_id)?;
        }
        Ok(())
    }

    /// Merges partial dynamic filters together for the provided dynamic filter
    /// id only if there are enough updates present.
    fn merge(state: &mut DynamicFilterRegistryState, id: u64) -> bool {
        let Some(filter) = state.filters.get_mut(&id) else {
            return false;
        };
        let previous = filter.merged.as_ref();
        if previous.is_some_and(|filter| filter.is_complete) {
            return false;
        }
        let Some(mode) = filter.merge_mode else {
            return false;
        };
        let all_complete = !filter.producers.is_empty()
            && filter.producers.iter().all(|(task, report)| {
                state.sealed_stages.contains(&task.stage_id)
                    && report.as_ref().is_some_and(|report| report.is_complete)
            });
        if mode == DynamicFilterMergePolicy::UnionWhenAllComplete && !all_complete {
            return false;
        }

        let mut reports: Vec<_> = filter
            .producers
            .iter()
            .filter_map(|(task, report)| report.as_ref().map(|report| (task, report)))
            .collect();
        reports.sort_unstable_by_key(|(key, _)| (key.stage_id, key.task_number));
        if mode == DynamicFilterMergePolicy::ForwardFirstComplete {
            reports.truncate(1);
        }
        let Some((_, template)) = reports.first() else {
            return false;
        };
        let inner_expr = merge_predicates(
            reports
                .iter()
                .filter_map(|(_, report)| report.inner_expr.as_deref().cloned())
                .collect(),
            mode,
        )
        .map(Box::new);
        let is_complete = mode == DynamicFilterMergePolicy::ForwardFirstComplete || all_complete;
        if previous.is_some_and(|previous| {
            previous.inner_expr == inner_expr && previous.is_complete == is_complete
        }) {
            return false;
        }
        let mut merged = (*template).clone();
        merged.inner_expr = inner_expr;
        merged.is_complete = is_complete;
        // Use a synthetic generation number for the merged filter. Each partial update has it's own generation
        // is not useful here.
        merged.generation = previous.map_or(0, |previous| previous.generation + 1);
        filter.merged_bytes = Some(
            PhysicalExprNode {
                expr_id: Some(id),
                expr_type: Some(ExprType::DynamicFilter(Box::new(merged.clone()))),
            }
            .encode_to_vec(),
        );
        filter.merged = Some(merged);
        state
            .delivered
            .retain(|(expression_id, _)| *expression_id != id);
        true
    }

    // Merge and enqueue under the same lock so successive snapshots cannot overtake each other.
    // Unbounded channel sends do not wait for the network or the receiving worker.
    fn dispatch(&self, state: &mut DynamicFilterRegistryState, id: u64) -> Result<()> {
        if self.query_finished.is_cancelled() {
            return Ok(());
        }
        let Some(filter) = state.filters.get(&id) else {
            return Ok(());
        };
        let Some(expression) = &filter.merged_bytes else {
            return Ok(());
        };
        let Some(producer_schema) = &filter.producer_schema else {
            return internal_err!(
                "Dynamic filter {id} has a merged predicate but no producer schema"
            );
        };
        for &task_key in &filter.consumer_tasks {
            // Ordinary task-local consumers are already updated directly by their producer.
            // Eligible Top-K producers instead receive into separate remote state; their
            // own older cutoff in the merged bound is redundant with the live local bound.
            if (filter.producers.contains_key(&task_key)
                && !filter.sibling_tasks.contains(&task_key))
                || state.delivered.contains(&(id, task_key))
            {
                continue;
            }
            let Some(sender) = state.task_senders.get(&task_key) else {
                continue;
            };
            let update = CoordinatorToWorkerMsg::ApplyDynamicFilter(Box::new(ApplyDynamicFilter {
                expression_id: id,
                expression: MaybeEncoded::Encoded(expression.clone()),
                producer_schema: Arc::clone(producer_schema),
            }));
            if sender.send(update).is_err() {
                // Closing the channel is expected only once query shutdown has started.
                if !self.query_finished.is_cancelled() {
                    return exec_err!(
                        "Failed to send dynamic filter {id} to task {task_key:?}: channel closed"
                    );
                }
                return Ok(());
            }
            state.delivered.insert((id, task_key));
        }
        Ok(())
    }
}

/// Merges [`PhysicalExprNode`] together. Merge behavior (ex. AND vs OR) is determined
/// by the provided [`DynamicFilterMergePolicy`].
fn merge_predicates(
    mut predicates: Vec<PhysicalExprNode>,
    mode: DynamicFilterMergePolicy,
) -> Option<PhysicalExprNode> {
    match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(PhysicalExprNode {
            expr_id: None,
            expr_type: Some(ExprType::BinaryExpr(Box::new(PhysicalBinaryExprNode {
                l: None,
                r: None,
                op: if mode == DynamicFilterMergePolicy::IntersectOnUpdate {
                    "And"
                } else {
                    "Or"
                }
                .to_owned(),
                operands: predicates,
            }))),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_physical_expr;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_expr::expressions::{Column, lit};
    use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
    use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
    use uuid::Uuid;

    #[test]
    fn producer_without_a_local_consumer_does_not_receive_sibling_updates() -> Result<()> {
        let registry =
            DynamicFilterRegistry::new(&ExecutionPlanMetricsSet::new(), CancellationToken::new());
        let (eligible_plan, id) = topk_plan()?;
        let without_consumer = eligible_plan.clone().replace_children(
            vec![Arc::new(EmptyExec::new(eligible_plan.schema()))],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )?;
        let eligible = TaskKey {
            query_id: Uuid::new_v4(),
            stage_id: 1,
            task_number: 0,
        };
        let ineligible = TaskKey {
            task_number: 1,
            ..eligible
        };
        registry.register_task(&eligible_plan, eligible, true)?;
        registry.register_task(&without_consumer, ineligible, true)?;
        registry.seal_stage(1)?;
        let (sender, mut eligible_updates) = unbounded_channel();
        registry.register_sender(eligible, sender)?;
        let (sender, mut ineligible_updates) = unbounded_channel();
        registry.register_sender(ineligible, sender)?;

        registry.record_dynamic_filter_update(
            eligible,
            report(id, false, false)?,
            &TaskContext::default(),
        )?;
        assert!(ineligible_updates.try_recv().is_err());
        assert_eq!(
            receive(&mut eligible_updates)?.inner_expr,
            report_predicate(false)?
        );
        registry.record_dynamic_filter_update(
            ineligible,
            report(id, true, true)?,
            &TaskContext::default(),
        )?;

        let update = receive(&mut eligible_updates)?;
        assert!(!update.is_complete);
        assert!(ineligible_updates.try_recv().is_err());
        Ok(())
    }

    fn topk_plan() -> Result<(Arc<dyn ExecutionPlan>, u64)> {
        let schema = Arc::new(Schema::new(vec![Field::new("key", DataType::Int32, true)]));
        let key: Arc<dyn PhysicalExpr> = Arc::new(Column::new("key", 0));
        let dynamic = Arc::new(DynamicFilterPhysicalExpr::new(vec![key.clone()], lit(true)));
        let input = Arc::new(FilterExec::try_new(
            dynamic.clone(),
            Arc::new(EmptyExec::new(schema)),
        )?);
        let order = LexOrdering::new(vec![PhysicalSortExpr::new_default(key)]).unwrap();
        let sort = SortExec::new(order, input)
            .with_fetch(Some(1))
            .with_dynamic_filter_expr(dynamic.clone())?;
        Ok((Arc::new(sort), dynamic.expression_id().unwrap()))
    }

    fn report(id: u64, value: bool, complete: bool) -> Result<ProducedDynamicFilter> {
        let expression = Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(value)));
        if complete {
            expression.mark_complete();
        }
        let expression: Arc<dyn PhysicalExpr> = expression;
        let mut proto = encode_physical_expr(&expression, &TaskContext::default())?;
        proto.expr_id = Some(id);
        Ok(ProducedDynamicFilter {
            expression_id: id,
            expression: MaybeEncoded::Encoded(proto.encode_to_vec()),
        })
    }

    fn report_predicate(value: bool) -> Result<Option<Box<PhysicalExprNode>>> {
        Ok(Some(Box::new(encode_physical_expr(
            &lit(value),
            &TaskContext::default(),
        )?)))
    }

    fn receive(
        receiver: &mut UnboundedReceiver<CoordinatorToWorkerMsg>,
    ) -> Result<PhysicalDynamicFilterNode> {
        let CoordinatorToWorkerMsg::ApplyDynamicFilter(update) = receiver.try_recv().unwrap()
        else {
            panic!("expected a dynamic-filter update");
        };
        let Some(ExprType::DynamicFilter(filter)) = update
            .expression
            .to_proto(&TaskContext::default())?
            .expr_type
        else {
            panic!("expected a dynamic-filter expression");
        };
        Ok(*filter)
    }
}
