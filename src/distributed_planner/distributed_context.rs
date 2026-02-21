use crate::common::require_one_child;
use crate::distributed_planner::rules::AnnotatedPlan;
use datafusion::common::{Result, not_impl_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
pub struct DistributedContext {
    pub(crate) original_single_node_plan: Arc<dyn ExecutionPlan>,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    pub(crate) annotated_plan: Mutex<Option<AnnotatedPlan>>,
}

impl DistributedContext {
    pub(crate) fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            original_single_node_plan: Arc::clone(&plan),
            plan,
            annotated_plan: Mutex::new(None),
        }
    }

    pub fn ensure<'a>(
        rule: &dyn PhysicalOptimizerRule,
        plan: &'a Arc<dyn ExecutionPlan>,
    ) -> Result<&'a Self> {
        let Some(this) = plan.as_any().downcast_ref::<Self>() else {
            return plan_err!(
                "Rule {} received a plan {}, but {} was expected.",
                rule.name(),
                plan.name(),
                Self::static_name()
            );
        };
        if this.plan.as_any().is::<Self>() {
            let name = Self::static_name();
            return plan_err!(
                "Rule {} received a {name}, but it had another nexted {name} in it. There is an error in one of the PhysicalOptimizerRules that is double-nesting {name}.",
                rule.name()
            );
        }
        Ok(this)
    }
}

impl DisplayAs for DistributedContext {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "DistributedContext")
    }
}

impl ExecutionPlan for DistributedContext {
    fn name(&self) -> &str {
        "DistributedContext"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        self.plan.properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.plan]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self {
            original_single_node_plan: Arc::clone(&self.original_single_node_plan),
            plan: require_one_child(children)?,
            annotated_plan: Mutex::new(self.annotated_plan.lock().unwrap().take()),
        }))
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        not_impl_err!("DistributedContext does not support execution!")
    }
}
