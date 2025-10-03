use datafusion::physical_plan::metrics::MetricsSet;
use std::sync::Arc;

use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// A transparent wrapper that delegates all execution to its child but returns custom metrics. This node is invisible during display.
/// The structure of a plan tree is closely tied to the [TaskMetricsRewriter].
pub struct MetricsWrapperExec {
    inner: Arc<dyn ExecutionPlan>,
    /// metrics for this plan node.
    metrics: MetricsSet,
    /// children is initially None. When used by the [TaskMetricsRewriter], the children will be updated
    /// to point at other wrapped nodes.
    children: Option<Vec<Arc<dyn ExecutionPlan>>>,
}

impl MetricsWrapperExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, metrics: MetricsSet) -> Self {
        Self {
            inner,
            metrics,
            children: None,
        }
    }

    /// Returns the inner execution plan.
    pub(crate) fn get_inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }
}

/// MetricsWrapperExec is invisible during display.
impl DisplayAs for MetricsWrapperExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

/// MetricsWrapperExec is visible when debugging.
impl Debug for MetricsWrapperExec {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "MetricsWrapperExec ({:?})", self.inner)
    }
}

impl ExecutionPlan for MetricsWrapperExec {
    fn name(&self) -> &str {
        "MetricsWrapperExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        unimplemented!("MetricsWrapperExec does not implement properties")
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.children {
            Some(children) => children.iter().collect(),
            None => self.inner.children(),
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MetricsWrapperExec {
            inner: self.inner.clone(),
            metrics: self.metrics.clone(),
            children: Some(children),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _contex: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("MetricsWrapperExec does not implement execute")
    }

    // metrics returns the wrapped metrics.
    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone())
    }
}
