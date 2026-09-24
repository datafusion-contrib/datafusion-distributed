use datafusion::common::Result;
use datafusion::common::not_impl_err;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    ReplaceChildrenOptions,
};
use delegate::delegate;
use std::fmt::Formatter;
use std::sync::Arc;

/// Display-only task variants of one operator, preserving its children and metric positions.
#[derive(Debug)]
pub(crate) struct TaskVariantsExec {
    pub(crate) inner: Arc<dyn ExecutionPlan>,
    pub(crate) variants: Vec<(usize, Arc<dyn ExecutionPlan>)>,
}

impl DisplayAs for TaskVariantsExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}: task_variants={{", self.inner.name())?;
        for (i, (task, variant)) in self.variants.iter().enumerate() {
            if i != 0 {
                write!(f, ", ")?;
            }
            write!(f, "t{task}: [")?;
            variant.fmt_as(t, f)?;
            write!(f, "]")?;
        }
        write!(f, "}}")
    }
}

impl ExecutionPlan for TaskVariantsExec {
    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn properties(&self) -> &Arc<PlanProperties>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
        }
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        for (_, variant) in &self.variants {
            if variant.apply_expressions(f)? == TreeNodeRecursion::Stop {
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            inner: Arc::clone(&self.inner).replace_children(
                children,
                ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
            )?,
            variants: self.variants.clone(),
        }))
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        not_impl_err!("TaskVariantsExec is only used for displaying executed plans")
    }

    fn downcast_delegate(&self) -> Option<&dyn ExecutionPlan> {
        Some(self.inner.as_ref())
    }
}
