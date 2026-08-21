use crate::{
    ApplyDynamicFilter, CoordinatorToWorkerMsg, DistributedCodec, NetworkBoundaryExt,
    ProducedDynamicFilter, TaskKey,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result};
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{BinaryExpr, DynamicFilterPhysicalExpr};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
use datafusion_proto::protobuf::PhysicalExprNode;
use datafusion_proto::protobuf::physical_expr_node::ExprType;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::UnboundedSender;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum PlannedHashJoinMode {
    CollectLeft,
    Partitioned,
}

#[derive(Default)]
pub(super) struct PlannedDynamicFilter {
    pub(super) mode: Option<PlannedHashJoinMode>,
    pub(super) producer_schema: Option<SchemaRef>,
    pub(super) producer_tasks: HashSet<TaskKey>,
    pub(super) consumer_tasks: HashSet<TaskKey>,
    pub(super) producer_stages: HashSet<usize>,
    pub(super) disabled: bool,
    /// Latest observed producer state for each task, including incomplete states.
    pub(super) reports: HashMap<TaskKey, PhysicalExprNode>,
    /// Number of producer tasks whose latest state is complete.
    pub(super) completed_producer_count: usize,
    pub(super) merged: Option<PhysicalExprNode>,
}

#[derive(Default)]
pub(super) struct DynamicFilterRegistryState {
    pub(super) filters: HashMap<u64, PlannedDynamicFilter>,
    pub(super) sealed_stages: HashSet<usize>,
    task_senders: HashMap<TaskKey, UnboundedSender<CoordinatorToWorkerMsg>>,
    delivered: HashSet<(u64, TaskKey)>,
}

/// Query-scoped runtime topology for distributed hash-join dynamic filters.
///
/// This is intentionally independent from the completed-consumer reports used to render plans.
pub(crate) struct DynamicFilterRegistry {
    pub(super) state: Mutex<DynamicFilterRegistryState>,
    task_ctx: Arc<TaskContext>,
}

impl DynamicFilterRegistry {
    pub(crate) fn new(task_ctx: Arc<TaskContext>) -> Self {
        Self {
            state: Mutex::new(DynamicFilterRegistryState::default()),
            task_ctx,
        }
    }

    pub(crate) fn register_task(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        task_key: TaskKey,
    ) -> Result<()> {
        let mut producers = vec![];
        let mut consumers = HashSet::new();

        plan.apply(|node| {
            if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                let mode = match join.partition_mode() {
                    PartitionMode::CollectLeft => Some(PlannedHashJoinMode::CollectLeft),
                    PartitionMode::Partitioned => Some(PlannedHashJoinMode::Partitioned),
                    PartitionMode::Auto => None,
                };
                if let Some(mode) = mode {
                    for expression in node.dynamic_expressions_produced() {
                        if expression
                            .downcast_ref::<DynamicFilterPhysicalExpr>()
                            .is_some()
                        {
                            producers.push((
                                expression.expression_id().expect(
                                    "DynamicFilterPhysicalExpr always has an expression ID",
                                ),
                                mode,
                                join.right().schema(),
                            ));
                        }
                    }
                }
            }

            // Network-boundary expressions are visibility anchors, not destinations.
            if node.is_network_boundary() {
                return Ok(TreeNodeRecursion::Continue);
            }
            let produced = node.dynamic_expressions_produced();
            node.apply_expressions(&mut |root| {
                root.apply(|expression| {
                    if expression
                        .downcast_ref::<DynamicFilterPhysicalExpr>()
                        .is_some()
                        && !produced
                            .iter()
                            .any(|produced| Arc::ptr_eq(produced, expression))
                    {
                        consumers.insert(
                            expression
                                .expression_id()
                                .expect("DynamicFilterPhysicalExpr always has an expression ID"),
                        );
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
            })?;
            Ok(TreeNodeRecursion::Continue)
        })?;

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        for (id, mode, producer_schema) in producers {
            let filter = state.filters.entry(id).or_default();
            if filter.mode.is_some_and(|existing| existing != mode)
                || filter
                    .producer_schema
                    .as_ref()
                    .is_some_and(|existing| existing.as_ref() != producer_schema.as_ref())
            {
                filter.disabled = true;
                continue;
            }
            filter.mode = Some(mode);
            filter.producer_schema = Some(producer_schema);
            filter.producer_tasks.insert(task_key);
            filter.producer_stages.insert(task_key.stage_id);
        }
        for id in consumers {
            state
                .filters
                .entry(id)
                .or_default()
                .consumer_tasks
                .insert(task_key);
        }
        drop(state);
        self.try_merge_all();
        Ok(())
    }

    pub(crate) fn register_sender(
        &self,
        task_key: TaskKey,
        sender: UnboundedSender<CoordinatorToWorkerMsg>,
    ) {
        self.state
            .lock()
            .expect("dynamic filter registry poisoned")
            .task_senders
            .insert(task_key, sender);
        self.dispatch_all();
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

    pub(crate) fn seal_stage(&self, stage_id: usize) {
        self.state
            .lock()
            .expect("dynamic filter registry poisoned")
            .sealed_stages
            .insert(stage_id);
        self.try_merge_all();
    }

    pub(crate) fn add_report(&self, task_key: TaskKey, report: ProducedDynamicFilter) {
        if report.expression.expr_id != Some(report.expression_id) {
            return;
        }
        let Some((generation, is_complete)) = report_state(&report.expression) else {
            return;
        };

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        let Some(filter) = state.filters.get_mut(&report.expression_id) else {
            return;
        };
        if filter.disabled || filter.merged.is_some() || !filter.producer_tasks.contains(&task_key)
        {
            return;
        }

        let advances_state = match filter.reports.get(&task_key) {
            None => true,
            Some(previous) => {
                let Some((previous_generation, previous_is_complete)) = report_state(previous)
                else {
                    // Only validated reports are inserted below.
                    unreachable!("stored producer report must be a dynamic filter")
                };
                // A complete report is terminal. Otherwise, keep only a newer generation, except
                // that completion may replace an incomplete report at the same generation because
                // DataFusion's mark_complete() does not increment the generation.
                !previous_is_complete
                    && (generation > previous_generation
                        || (generation == previous_generation && is_complete))
            }
        };
        if !advances_state {
            return;
        }

        if is_complete {
            filter.completed_producer_count += 1;
        }
        filter.reports.insert(task_key, report.expression);
        drop(state);
        self.try_merge(report.expression_id);
    }

    fn try_merge_all(&self) {
        let ids: Vec<_> = self
            .state
            .lock()
            .expect("dynamic filter registry poisoned")
            .filters
            .keys()
            .copied()
            .collect();
        for id in ids {
            self.try_merge(id);
        }
    }

    fn try_merge(&self, id: u64) {
        if let Err(_error) = self.try_merge_inner(id)
            && let Some(filter) = self
                .state
                .lock()
                .expect("dynamic filter registry poisoned")
                .filters
                .get_mut(&id)
        {
            // Dynamic filtering is an optimization. A malformed or unsupported report must
            // not fail the query.
            filter.disabled = true;
        }
        self.dispatch(id);
    }

    fn try_merge_inner(&self, id: u64) -> Result<()> {
        let (mode, schema, reports) = {
            let state = self.state.lock().expect("dynamic filter registry poisoned");
            let Some(filter) = state.filters.get(&id) else {
                return Ok(());
            };
            if filter.disabled || filter.merged.is_some() {
                return Ok(());
            }
            let (Some(mode), Some(schema)) = (filter.mode, filter.producer_schema.clone()) else {
                return Ok(());
            };

            let mut task_keys: Vec<_> = filter.producer_tasks.iter().copied().collect();
            task_keys.sort_unstable_by_key(|key| (key.stage_id, key.task_number));
            let ready_keys = match mode {
                PlannedHashJoinMode::CollectLeft => task_keys
                    .into_iter()
                    .find(|task_key| {
                        filter
                            .reports
                            .get(task_key)
                            .and_then(report_state)
                            .is_some_and(|(_, is_complete)| is_complete)
                    })
                    .into_iter()
                    .collect::<Vec<_>>(),
                PlannedHashJoinMode::Partitioned => {
                    let stages_sealed = filter
                        .producer_stages
                        .iter()
                        .all(|stage| state.sealed_stages.contains(stage));
                    if !stages_sealed
                        || task_keys.is_empty()
                        || filter.completed_producer_count != task_keys.len()
                    {
                        return Ok(());
                    }
                    task_keys
                }
            };
            if ready_keys.is_empty() {
                return Ok(());
            }
            let reports = ready_keys
                .into_iter()
                .map(|task_key| filter.reports[&task_key].clone())
                .collect::<Vec<_>>();
            (mode, schema, reports)
        };

        let codec = DistributedCodec::new_combined_with_user(self.task_ctx.session_config());
        let expressions = reports
            .iter()
            .map(|report| {
                let expression =
                    parse_physical_expr(report, &self.task_ctx, schema.as_ref(), &codec)?;
                let dynamic_filter = expression
                    .downcast_ref::<DynamicFilterPhysicalExpr>()
                    .ok_or_else(|| {
                        datafusion::common::internal_datafusion_err!(
                            "producer report was not a DynamicFilterPhysicalExpr"
                        )
                    })?;
                dynamic_filter.current()
            })
            .collect::<Result<Vec<_>>>()?;
        let merged = match mode {
            PlannedHashJoinMode::CollectLeft => Arc::clone(&expressions[0]),
            PlannedHashJoinMode::Partitioned => balanced_or(expressions),
        };
        let merged = serialize_physical_expr(&merged, &codec)?;

        let mut state = self.state.lock().expect("dynamic filter registry poisoned");
        if let Some(filter) = state.filters.get_mut(&id)
            && !filter.disabled
        {
            filter.merged.get_or_insert(merged);
        }
        Ok(())
    }

    fn dispatch_all(&self) {
        let ids = self
            .state
            .lock()
            .expect("dynamic filter registry poisoned")
            .filters
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for id in ids {
            self.dispatch(id);
        }
    }

    fn dispatch(&self, id: u64) {
        let deliveries = {
            let mut state = self.state.lock().expect("dynamic filter registry poisoned");
            let Some(filter) = state.filters.get(&id) else {
                return;
            };
            let Some(predicate) = filter.merged.clone() else {
                return;
            };
            let producer_tasks = filter.producer_tasks.clone();
            let consumer_tasks = filter.consumer_tasks.clone();

            let mut deliveries = vec![];
            for task_key in consumer_tasks {
                if producer_tasks.contains(&task_key) {
                    // A task-local consumer is already updated directly by its producer.
                    state.delivered.insert((id, task_key));
                    continue;
                }
                if state.delivered.contains(&(id, task_key)) {
                    continue;
                }
                let Some(sender) = state.task_senders.get(&task_key).cloned() else {
                    continue;
                };
                state.delivered.insert((id, task_key));
                deliveries.push((sender, predicate.clone()));
            }
            deliveries
        };

        for (sender, predicate) in deliveries {
            // Dynamic filtering is fail-open: a closed task channel never fails the query and is
            // not retried because the task can no longer consume an update.
            let _ = sender.send(CoordinatorToWorkerMsg::ApplyDynamicFilter(Box::new(
                ApplyDynamicFilter {
                    expression_id: id,
                    predicate,
                },
            )));
        }
    }
}

fn balanced_or(mut expressions: Vec<Arc<dyn PhysicalExpr>>) -> Arc<dyn PhysicalExpr> {
    debug_assert!(!expressions.is_empty());
    while expressions.len() > 1 {
        let mut next = Vec::with_capacity(expressions.len().div_ceil(2));
        let mut current = std::mem::take(&mut expressions).into_iter();
        while let Some(left) = current.next() {
            next.push(match current.next() {
                Some(right) => Arc::new(BinaryExpr::new(left, Operator::Or, right)) as _,
                None => left,
            });
        }
        expressions = next;
    }
    expressions.pop().expect("non-empty expressions")
}

fn report_state(expression: &PhysicalExprNode) -> Option<(u64, bool)> {
    let Some(ExprType::DynamicFilter(filter)) = expression.expr_type.as_ref() else {
        return None;
    };
    Some((filter.generation, filter.is_complete))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, CaseExpr, Column, DynamicFilterPhysicalExpr, lit,
    };
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::filter::FilterExec;
    use datafusion::prelude::SessionContext;
    use datafusion_proto::physical_plan::from_proto::parse_physical_expr;
    use datafusion_proto::physical_plan::to_proto::serialize_physical_expr;
    use datafusion_proto::protobuf::PhysicalDynamicFilterNode;
    use uuid::Uuid;

    #[test]
    fn registers_task_roles_and_seals_stage() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let left = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let probe = Arc::new(EmptyExec::new(Arc::clone(&schema))) as Arc<dyn ExecutionPlan>;
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            lit(true),
        ));
        let id = dynamic_filter.expression_id().unwrap();
        let right =
            Arc::new(FilterExec::try_new(dynamic_filter.clone(), probe)?) as Arc<dyn ExecutionPlan>;
        let join = HashJoinExec::try_new(
            left,
            right,
            vec![(Arc::new(Column::new("a", 0)), Arc::new(Column::new("a", 0)))],
            None,
            &JoinType::Inner,
            None,
            PartitionMode::Partitioned,
            NullEquality::NullEqualsNothing,
            false,
        )?
        .with_dynamic_filter_expr(dynamic_filter)?;
        let plan = Arc::new(join) as Arc<dyn ExecutionPlan>;
        let task_key = TaskKey {
            query_id: Uuid::nil(),
            stage_id: 3,
            task_number: 1,
        };

        let registry = DynamicFilterRegistry::new(SessionContext::new().task_ctx());
        registry.register_task(&plan, task_key)?;
        registry.seal_stage(3);

        let state = registry.state.lock().unwrap();
        let filter = state.filters.get(&id).unwrap();
        assert_eq!(filter.mode, Some(PlannedHashJoinMode::Partitioned));
        assert_eq!(filter.producer_tasks, HashSet::from([task_key]));
        assert_eq!(filter.consumer_tasks, HashSet::from([task_key]));
        assert!(state.sealed_stages.contains(&3));
        Ok(())
    }

    #[test]
    fn stores_latest_report_and_counts_completion_once() {
        let expression_id = 42;
        let task_key = task_key(0);
        let registry = registry_with_producer(expression_id, task_key);

        registry.add_report(task_key, report(expression_id, 1, false));
        registry.add_report(task_key, report(expression_id, 0, false));
        registry.add_report(task_key, report(expression_id, 2, false));
        {
            let state = registry.state.lock().unwrap();
            let filter = &state.filters[&expression_id];
            assert_eq!(report_state(&filter.reports[&task_key]), Some((2, false)));
            assert_eq!(filter.completed_producer_count, 0);
        }

        // Completion is a state change even though it has the same generation as the last update.
        registry.add_report(task_key, report(expression_id, 2, true));
        registry.add_report(task_key, report(expression_id, 2, true));
        registry.add_report(task_key, report(expression_id, 3, false));
        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert_eq!(report_state(&filter.reports[&task_key]), Some((2, true)));
        assert_eq!(filter.completed_producer_count, 1);
    }

    #[test]
    fn ignores_reports_from_unknown_tasks_and_expression_ids() {
        let expression_id = 42;
        let producer_task = task_key(0);
        let registry = registry_with_producer(expression_id, producer_task);

        registry.add_report(task_key(1), report(expression_id, 1, true));
        registry.add_report(producer_task, report(expression_id + 1, 1, true));
        let state = registry.state.lock().unwrap();
        let filter = &state.filters[&expression_id];
        assert!(filter.reports.is_empty());
        assert_eq!(filter.completed_producer_count, 0);
    }

    fn registry_with_producer(expression_id: u64, task_key: TaskKey) -> DynamicFilterRegistry {
        let registry = DynamicFilterRegistry::new(SessionContext::new().task_ctx());
        registry.state.lock().unwrap().filters.insert(
            expression_id,
            PlannedDynamicFilter {
                producer_tasks: HashSet::from([task_key]),
                ..Default::default()
            },
        );
        registry
    }

    fn report(expression_id: u64, generation: u64, is_complete: bool) -> ProducedDynamicFilter {
        ProducedDynamicFilter {
            expression_id,
            expression: PhysicalExprNode {
                expr_id: Some(expression_id),
                expr_type: Some(ExprType::DynamicFilter(Box::new(
                    PhysicalDynamicFilterNode {
                        generation,
                        is_complete,
                        ..Default::default()
                    },
                ))),
            },
        }
    }

    #[test]
    fn partitioned_waits_for_all_tasks_then_merges_in_task_order() -> Result<()> {
        let task_ctx = SessionContext::new().task_ctx();
        let registry = DynamicFilterRegistry::new(Arc::clone(&task_ctx));
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let id = 99;
        let task_0 = task_key(0);
        let task_1 = task_key(1);
        registry.state.lock().unwrap().filters.insert(
            id,
            PlannedDynamicFilter {
                mode: Some(PlannedHashJoinMode::Partitioned),
                producer_schema: Some(Arc::clone(&schema)),
                producer_tasks: HashSet::from([task_0, task_1]),
                producer_stages: HashSet::from([3]),
                ..Default::default()
            },
        );
        registry.seal_stage(3);

        let task_1_predicate = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Lt,
            lit(20_i32),
        )) as Arc<dyn PhysicalExpr>;
        registry.add_report(
            task_1,
            producer_report(id, Arc::clone(&task_1_predicate), false, &task_ctx)?,
        );
        {
            let state = registry.state.lock().unwrap();
            assert_eq!(state.filters[&id].completed_producer_count, 0);
            assert!(state.filters[&id].merged.is_none());
        }

        registry.add_report(
            task_1,
            producer_report(id, task_1_predicate, true, &task_ctx)?,
        );
        {
            let state = registry.state.lock().unwrap();
            assert_eq!(state.filters[&id].completed_producer_count, 1);
            assert!(state.filters[&id].merged.is_none());
        }

        registry.add_report(
            task_0,
            producer_report(
                id,
                Arc::new(BinaryExpr::new(
                    Arc::new(Column::new("a", 0)),
                    Operator::Gt,
                    lit(10_i32),
                )),
                true,
                &task_ctx,
            )?,
        );
        let merged = registry.state.lock().unwrap().filters[&id]
            .merged
            .clone()
            .unwrap();
        let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
        let merged = parse_physical_expr(&merged, &task_ctx, &schema, &codec)?;
        assert_eq!(merged.to_string(), "a@0 > 10 OR a@0 < 20");
        Ok(())
    }

    #[test]
    fn collect_left_uses_first_completed_task() -> Result<()> {
        let task_ctx = SessionContext::new().task_ctx();
        let registry = DynamicFilterRegistry::new(Arc::clone(&task_ctx));
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let id = 100;
        registry.state.lock().unwrap().filters.insert(
            id,
            PlannedDynamicFilter {
                mode: Some(PlannedHashJoinMode::CollectLeft),
                producer_schema: Some(Arc::clone(&schema)),
                producer_tasks: HashSet::from([task_key(0), task_key(1)]),
                producer_stages: HashSet::from([3]),
                ..Default::default()
            },
        );
        registry.add_report(
            task_key(1),
            producer_report(id, lit(true), true, &task_ctx)?,
        );
        assert!(registry.state.lock().unwrap().filters[&id].merged.is_some());
        Ok(())
    }

    #[test]
    fn partition_aware_cases_are_ored_whole() -> Result<()> {
        let case_0 = Arc::new(CaseExpr::try_new(
            Some(Arc::new(Column::new("partition", 0))),
            vec![(lit(0_i32), lit(true))],
            Some(lit(false)),
        )?) as Arc<dyn PhysicalExpr>;
        let case_1 = Arc::new(CaseExpr::try_new(
            Some(Arc::new(Column::new("partition", 0))),
            vec![(lit(1_i32), lit(true))],
            Some(lit(false)),
        )?) as Arc<dyn PhysicalExpr>;

        let merged = balanced_or(vec![case_0, case_1]);
        let merged = merged.downcast_ref::<BinaryExpr>().unwrap();
        assert_eq!(merged.op(), &Operator::Or);
        assert!(merged.left().downcast_ref::<CaseExpr>().is_some());
        assert!(merged.right().downcast_ref::<CaseExpr>().is_some());
        Ok(())
    }

    #[test]
    fn routes_once_to_remote_consumers_and_skips_local_consumers() {
        let registry = DynamicFilterRegistry::new(SessionContext::new().task_ctx());
        let producer = task_key(0);
        let remote_consumer = task_key(1);
        registry.state.lock().unwrap().filters.insert(
            42,
            PlannedDynamicFilter {
                producer_tasks: HashSet::from([producer]),
                consumer_tasks: HashSet::from([producer, remote_consumer]),
                merged: Some(PhysicalExprNode::default()),
                ..Default::default()
            },
        );

        let (producer_tx, mut producer_rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_sender(producer, producer_tx);
        assert!(producer_rx.try_recv().is_err());

        let (consumer_tx, mut consumer_rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_sender(remote_consumer, consumer_tx);
        let CoordinatorToWorkerMsg::ApplyDynamicFilter(filter) = consumer_rx.try_recv().unwrap()
        else {
            panic!("expected dynamic-filter update");
        };
        assert_eq!(filter.expression_id, 42);

        let (replacement_tx, mut replacement_rx) = tokio::sync::mpsc::unbounded_channel();
        registry.register_sender(remote_consumer, replacement_tx);
        assert!(replacement_rx.try_recv().is_err());
    }

    fn task_key(task_number: usize) -> TaskKey {
        TaskKey {
            query_id: Uuid::nil(),
            stage_id: 3,
            task_number,
        }
    }
    fn producer_report(
        expression_id: u64,
        predicate: Arc<dyn PhysicalExpr>,
        is_complete: bool,
        task_ctx: &Arc<TaskContext>,
    ) -> Result<ProducedDynamicFilter> {
        let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(
            vec![Arc::new(Column::new("a", 0))],
            predicate,
        )) as Arc<dyn PhysicalExpr>;
        if is_complete {
            dynamic_filter
                .downcast_ref::<DynamicFilterPhysicalExpr>()
                .unwrap()
                .mark_complete();
        }
        let codec = DistributedCodec::new_combined_with_user(task_ctx.session_config());
        let mut expression = serialize_physical_expr(&dynamic_filter, &codec)?;
        expression.expr_id = Some(expression_id);
        Ok(ProducedDynamicFilter {
            expression_id,
            expression,
        })
    }
}
