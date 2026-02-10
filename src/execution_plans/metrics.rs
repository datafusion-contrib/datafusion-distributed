use datafusion::physical_plan::metrics::MetricsSet;
use std::sync::Arc;

use datafusion::common::Statistics;
use datafusion::error::Result;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, PlanProperties};
use delegate::delegate;
use std::any::Any;
use std::fmt::{Debug, Formatter};

/// A transparent wrapper that delegates all execution to its child but returns custom metrics. This node is invisible during display.
/// The structure of a plan tree is closely tied to the [TaskMetricsRewriter].
pub(crate) struct MetricsWrapperExec {
    inner: Arc<dyn ExecutionPlan>,
    /// metrics for this plan node.
    metrics: MetricsSet,
}

impl MetricsWrapperExec {
    pub(crate) fn new(inner: Arc<dyn ExecutionPlan>, metrics: MetricsSet) -> Self {
        Self { inner, metrics }
    }
}

/// MetricsWrapperExec is invisible during display.
impl DisplayAs for MetricsWrapperExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
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
    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn properties(&self) -> &PlanProperties;
            fn as_any(&self) -> &dyn Any;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics>;
        }
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.inner.children()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MetricsWrapperExec {
            inner: Arc::clone(&self.inner).with_new_children(children.clone())?,
            metrics: self.metrics.clone(),
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
