use crate::distributed_planner::{NetworkBoundary, ProducerHead};
use crate::worker::{PendingTaskUrl, RemoteStageRef};
use crate::{NetworkBroadcastExec, NetworkCoalesceExec, NetworkShuffleExec, Stage};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{Result, internal_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr_common::metrics::MetricsSet;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, Statistics, StatisticsArgs,
    apply_expression_roots,
};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// A coordinator-only boundary whose tasks become executable independently of their peers.
/// Boundaries inside worker plans keep concrete destinations and never use this wrapper.
pub(crate) struct PendingNetworkBoundaryExec {
    boundary: Arc<dyn NetworkBoundary>,
    workers: Vec<PendingTaskUrl>,
    dynamic_filter_anchors: Vec<Arc<dyn PhysicalExpr>>,
    query_finished: CancellationToken,
}

impl PendingNetworkBoundaryExec {
    pub(crate) fn new(
        boundary: Arc<dyn NetworkBoundary>,
        workers: Vec<PendingTaskUrl>,
        dynamic_filter_anchors: Vec<Arc<dyn PhysicalExpr>>,
        query_finished: CancellationToken,
    ) -> Self {
        Self {
            boundary,
            workers,
            dynamic_filter_anchors,
            query_finished,
        }
    }
}

impl Debug for PendingNetworkBoundaryExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PendingNetworkBoundaryExec")
            .field("boundary", &self.boundary)
            .field("tasks", &self.workers.len())
            .finish()
    }
}

impl DisplayAs for PendingNetworkBoundaryExec {
    fn fmt_as(&self, display: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.boundary.fmt_as(display, f)
    }
}

impl NetworkBoundary for PendingNetworkBoundaryExec {
    fn input_stage(&self) -> &Stage {
        self.boundary.input_stage()
    }

    fn with_input_stage(&self, stage: Stage) -> Result<Arc<dyn NetworkBoundary>> {
        self.boundary.with_input_stage(stage)
    }

    fn producer_head(&self, consumer_tasks: usize) -> Result<ProducerHead> {
        self.boundary.producer_head(consumer_tasks)
    }
}

impl ExecutionPlan for PendingNetworkBoundaryExec {
    fn name(&self) -> &str {
        self.boundary.name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.boundary.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return internal_err!("A pending network boundary cannot accept children");
        }
        Ok(self)
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        apply_expression_roots(self.dynamic_filter_anchors.iter(), f)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stage = self.input_stage();
        let pending = RemoteStageRef::Pending {
            query_id: stage.query_id(),
            num: stage.num(),
            workers: &self.workers,
            query_finished: &self.query_finished,
        };
        let boundary: &dyn ExecutionPlan = self.boundary.as_ref();
        if let Some(boundary) = boundary.downcast_ref::<NetworkCoalesceExec>() {
            return boundary.execute_remote(partition, context, pending);
        }
        if let Some(boundary) = boundary.downcast_ref::<NetworkShuffleExec>() {
            return boundary.execute_remote(partition, context, pending);
        }
        if let Some(boundary) = boundary.downcast_ref::<NetworkBroadcastExec>() {
            return boundary.execute_remote(partition, context, pending);
        }
        internal_err!("Unsupported pending network boundary")
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.boundary.metrics()
    }

    fn statistics_from_inputs(
        &self,
        _input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        Ok(Arc::new(Statistics::new_unknown(&self.schema())))
    }
}
