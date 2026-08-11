use crate::dynamic_filtering::discover_dynamic_filter_consumers;
use crate::{
    ApplyDynamicFilter, CoordinatorToWorkerMsg, MaybeEncoded, ProducedDynamicFilter, TaskKey,
};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, internal_err};
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DynamicFilterMergeMode {
    /// Wait for every planned producer task to report a complete dynamic filter, then
    /// merge and forward the filter. Used for partitioned joins.
    AllProducersComplete,
    /// Wait for any producer to report a complete dynamic filter and forward it.
    /// Used for collect left joins.
    FirstProducerComplete,
    /// Forward any update received from any producer, merging updates from
    /// different producers. Used for TopK dynamic filters in aggregates and sorts.
    Incremental,
}

#[derive(Default)]
pub(super) struct PlannedDynamicFilter {
    pub(super) merge_mode: Option<DynamicFilterMergeMode>,
    // Producer and consumer tasks for a dynamic filter.
    //
    // Note that it is not guaranteed that every task within a stage produces / consumes dynamic filters. For
    // example, a distributed union may prevent a dynamic filter from appearing in all tasks. So, we
    // store task keys rather than stage ids.
    pub(super) producer_tasks: HashSet<TaskKey>,
    pub(super) consumer_tasks: HashSet<TaskKey>,
    /// Latest accepted snapshot from each producer task.
    pub(super) producer_filters: HashMap<TaskKey, PhysicalDynamicFilterNode>,
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
}

impl DynamicFilterRegistry {
    pub(crate) fn new(metrics: &ExecutionPlanMetricsSet) -> Self {
        Self {
            state: Mutex::new(DynamicFilterRegistryState::default()),
            dynamic_filter_updates_received: MetricBuilder::new(metrics)
                .global_counter("dynamic_filter_updates_received"),
        }
    }

    pub(crate) fn record_update_received(&self) {
        self.dynamic_filter_updates_received.add(1);
    }

    /// Adds any dynamic filter producers and consumers found in `plan` to the registry.
    pub(crate) fn register_task(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_key: TaskKey,
    ) -> Result<()> {
        let mut producers = vec![];

        plan.apply(|node| {
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
                DynamicFilterMergeMode::FirstProducerComplete
            } else if node.downcast_ref::<SortExec>().is_some()
                || node.downcast_ref::<AggregateExec>().is_some()
            {
                DynamicFilterMergeMode::Incremental
            } else {
                DynamicFilterMergeMode::AllProducersComplete
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
            producers.extend(produced_ids.iter().map(|id| (*id, merge_mode)));
            Ok(TreeNodeRecursion::Continue)
        })?;
        // We can safely ignore anchors because they are not evaluated by network boundaries. This
        // means they do not need updates forwarded to them.
        let consumers = discover_dynamic_filter_consumers(plan)?.consumers;

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        for (id, merge_mode) in producers {
            let filter = state.filters.entry(id).or_default();
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
            filter.producer_tasks.insert(task_key);
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
    ) {
        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        state.task_senders.insert(task_key, sender);
        state.delivered.retain(|(_, task)| *task != task_key);
        let ids = state.filters.keys().copied().collect::<Vec<_>>();
        for id in ids {
            Self::dispatch(&mut state, id);
        }
    }

    /// Drops the routing registry's channel handles before the query-end notification. Otherwise,
    /// these retained senders would keep coordinator-to-worker streams alive indefinitely.
    pub(crate) fn clear_senders(&self) {
        self.state
            .lock()
            .expect("dynamic filter registry poisoned")
            .task_senders
            .clear();
    }

    /// Mark that a stage has registered all of its tasks.
    pub(crate) fn seal_stage(&self, stage_id: usize) {
        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        state.sealed_stages.insert(stage_id);
        let ids = state.filters.keys().copied().collect::<Vec<_>>();
        for id in ids {
            if Self::try_merge(&mut state, id) {
                Self::dispatch(&mut state, id);
            }
        }
    }

    /// Records a producer's latest dynamic-filter update and recomputes the merged filter.
    pub(crate) fn record_dynamic_filter_update(
        &self,
        task_key: TaskKey,
        report: ProducedDynamicFilter,
        task_ctx: &TaskContext,
    ) {
        self.record_update_received();
        let Ok(expression) = report.expression.to_proto(task_ctx) else {
            return;
        };
        if expression.expr_id != Some(report.expression_id) {
            return;
        }
        let Some(ExprType::DynamicFilter(dynamic_filter)) = expression.expr_type else {
            return;
        };
        if dynamic_filter.inner_expr.is_none() {
            return;
        }

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        let Some(filter) = state.filters.get_mut(&report.expression_id) else {
            return;
        };
        if !filter.producer_tasks.contains(&task_key)
            || (filter.merge_mode != Some(DynamicFilterMergeMode::Incremental)
                && !dynamic_filter.is_complete)
            || filter
                .producer_filters
                .get(&task_key)
                .is_some_and(|previous| previous.is_complete || previous == dynamic_filter.as_ref())
        {
            return;
        }
        filter.producer_filters.insert(task_key, *dynamic_filter);
        if Self::try_merge(&mut state, report.expression_id) {
            Self::dispatch(&mut state, report.expression_id);
        }
    }

    /// Merges partial dynamic filters together for the provided dynamic filter
    /// id only if there are enough updates present.
    fn try_merge(state: &mut DynamicFilterRegistryState, id: u64) -> bool {
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
        let all_complete = !filter.producer_tasks.is_empty()
            && filter.producer_tasks.iter().all(|task| {
                state.sealed_stages.contains(&task.stage_id)
                    && filter
                        .producer_filters
                        .get(task)
                        .is_some_and(|f| f.is_complete)
            });
        if mode == DynamicFilterMergeMode::AllProducersComplete && !all_complete {
            return false;
        }

        let mut reports: Vec<_> = filter.producer_filters.iter().collect();
        reports.sort_unstable_by_key(|(key, _)| (key.stage_id, key.task_number));
        if mode == DynamicFilterMergeMode::FirstProducerComplete {
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
        )
        .map(Box::new);
        let is_complete = mode == DynamicFilterMergeMode::FirstProducerComplete || all_complete;
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
    fn dispatch(state: &mut DynamicFilterRegistryState, id: u64) {
        let Some(filter) = state.filters.get(&id) else {
            return;
        };
        let Some(expression) = &filter.merged_bytes else {
            return;
        };
        for &task_key in &filter.consumer_tasks {
            // A task-local consumer is already updated directly by its producer.
            if filter.producer_tasks.contains(&task_key)
                || state.delivered.contains(&(id, task_key))
            {
                continue;
            }
            let Some(sender) = state.task_senders.get(&task_key) else {
                continue;
            };
            state.delivered.insert((id, task_key));
            // Dynamic filtering is fail-open: a closed task channel never fails the query and is
            // not retried because the task can no longer consume an update.
            let _ = sender.send(CoordinatorToWorkerMsg::ApplyDynamicFilter(Box::new(
                ApplyDynamicFilter {
                    expression_id: id,
                    expression: MaybeEncoded::Encoded(expression.clone()),
                },
            )));
        }
    }
}

/// Merges [`PhysicalExprNode`] together by ORing them.
fn merge_predicates(mut predicates: Vec<PhysicalExprNode>) -> Option<PhysicalExprNode> {
    match predicates.len() {
        0 => None,
        1 => predicates.pop(),
        _ => Some(PhysicalExprNode {
            expr_id: None,
            expr_type: Some(ExprType::BinaryExpr(Box::new(PhysicalBinaryExprNode {
                l: None,
                r: None,
                op: "Or".to_owned(),
                operands: predicates,
            }))),
        }),
    }
}
