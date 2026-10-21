use crate::execution_plans::{MaterializeExec, NetworkMaterializedScanExec};
use crate::stage::{LocalStage, Stage};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{HashMap, HashSet, Result, internal_datafusion_err, not_impl_err};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::materialized_cte::{MaterializedCteExec, MaterializedCteScanExec};
use datafusion::physical_plan::recursive_query::RecursiveQueryExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};
use std::sync::Arc;
use uuid::Uuid;

/// Replace process-local CTE handles with independently addressed readers of one
/// producer. Discover the whole plan first: scalar-subquery scans can live outside
/// their producer's continuation. The resulting plan is still executable locally.
pub(super) fn lower_materialized_ctes(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut lowering = Lowering::default();
    let mut recursive = false;
    plan.apply(|node| {
        if let Some(cte) = node.downcast_ref::<MaterializedCteExec>() {
            lowering
                .bodies
                .insert(cte.buffer().id(), Arc::clone(cte.children()[0]));
        }
        if let Some(scan) = node.downcast_ref::<MaterializedCteScanExec>() {
            lowering
                .readers
                .entry(scan.id())
                .or_default()
                .push(scan.properties().output_partitioning().partition_count());
        }
        recursive |= node.is::<RecursiveQueryExec>();
        Ok(TreeNodeRecursion::Continue)
    })?;
    if lowering.bodies.is_empty() {
        return Ok(plan);
    }
    if recursive {
        return not_impl_err!(
            "Distributed materialized CTEs in recursive queries require per-iteration materialization state"
        );
    }
    lowering.lower(plan)
}

#[derive(Default)]
struct Lowering {
    bodies: HashMap<u64, Arc<dyn ExecutionPlan>>,
    readers: HashMap<u64, Vec<usize>>,
    producers: HashMap<u64, Arc<dyn ExecutionPlan>>,
    assigned_readers: HashSet<(u64, usize)>,
    building: HashSet<u64>,
}

impl Lowering {
    fn lower(&mut self, plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(cte) = plan.downcast_ref::<MaterializedCteExec>() {
            return self.lower(Arc::clone(cte.children()[1]));
        }
        if let Some(scan) = plan.downcast_ref::<MaterializedCteScanExec>() {
            let id = scan.id();
            let producer = self.producer(id)?;
            let partitions = scan.properties().output_partitioning().partition_count();
            // Dependencies may be lowered before the reference that discovered them,
            // so allocation need not follow the initial tree traversal order.
            let readers = &self.readers[&id];
            let reader_id = readers
                .iter()
                .enumerate()
                .find_map(|(index, &count)| {
                    (count == partitions && !self.assigned_readers.contains(&(id, index)))
                        .then_some(index)
                })
                .ok_or_else(|| {
                    internal_datafusion_err!("No reader slot for materialized CTE {id}")
                })?;
            let offset = readers[..reader_id].iter().sum();
            self.assigned_readers.insert((id, reader_id));
            let result = NetworkMaterializedScanExec::new(
                id,
                reader_id,
                offset,
                partitions,
                scan.schema(),
                Stage::Local(LocalStage {
                    query_id: Uuid::nil(),
                    num: 0,
                    plan: producer,
                    tasks: 1,
                    metrics_set: Default::default(),
                }),
            );
            return Ok(Arc::new(result));
        }
        let children = plan
            .children()
            .into_iter()
            .map(|child| self.lower(Arc::clone(child)))
            .collect::<Result<_>>()?;
        plan.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn producer(&mut self, id: u64) -> Result<Arc<dyn ExecutionPlan>> {
        if let Some(producer) = self.producers.get(&id) {
            return Ok(Arc::clone(producer));
        }
        if !self.building.insert(id) {
            return not_impl_err!("Cyclic materialization dependency for CTE {id}");
        }
        let body = self.bodies.get(&id).cloned().ok_or_else(|| {
            internal_datafusion_err!("No producer found for materialized CTE {id}")
        })?;
        let body = self.lower(body)?;
        // One materialization task, with a potentially distributed body.
        let body = Arc::new(CoalescePartitionsExec::new(body));
        let readers = self.readers.get(&id).cloned().ok_or_else(|| {
            internal_datafusion_err!("No readers found for materialized CTE {id}")
        })?;
        let producer: Arc<dyn ExecutionPlan> = Arc::new(MaterializeExec::new(body, id, readers));
        self.producers.insert(id, Arc::clone(&producer));
        self.building.remove(&id);
        Ok(producer)
    }
}
