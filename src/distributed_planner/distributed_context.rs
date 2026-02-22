use crate::common::require_one_child;
use datafusion::common::{Result, not_impl_err, plan_err};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// [ExecutionPlan] implementation that sits immediately above another plan, and is there just to
/// add contextual information for distributed planning.
///
/// All [PhysicalOptimizerRule]s are expected to accept a [DistributedContext] as a head node, and
/// return a [DistributedContext] after the [PhysicalOptimizerRule::optimize] pass.
///
/// Users implementing custom [PhysicalOptimizerRule]s for Distributed DataFusion should always
/// call [DistributedContext::ensure] at the beginning of their rule.
#[derive(Debug)]
pub struct DistributedContext {
    pub(crate) original_single_node_plan: Arc<dyn ExecutionPlan>,
    pub(crate) plan: Arc<dyn ExecutionPlan>,
}

impl DistributedContext {
    pub(crate) fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            original_single_node_plan: Arc::clone(&plan),
            plan,
        }
    }

    /// Ensures that the [PhysicalOptimizerRule] is running under a [DistributedContext].
    ///
    /// This function is expected to be called at the beginning of each [PhysicalOptimizerRule]:
    ///
    /// ```rust
    /// # use datafusion::physical_optimizer::PhysicalOptimizerRule;
    /// # use datafusion::physical_plan::ExecutionPlan;
    /// # use datafusion::config::ConfigOptions;
    /// # use datafusion::common::Result;
    /// # use datafusion_distributed::DistributedContext;
    /// # use std::sync::Arc;
    ///
    /// #[derive(Debug)]
    /// pub struct MyRule;
    ///
    /// impl PhysicalOptimizerRule for MyRule {
    ///     fn optimize(&self, plan: Arc<dyn ExecutionPlan>, config: &ConfigOptions) -> Result<Arc<dyn ExecutionPlan>> {
    ///         DistributedContext::ensure(self, &plan)?;
    ///         // Add your logic here
    ///         Ok(plan)
    ///     }
    ///  
    ///     fn name(&self) -> &str {
    ///         "MyRule"
    ///     }
    ///
    ///     fn schema_check(&self) -> bool {
    ///         true
    ///     }
    /// }
    /// ```
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
        }))
    }

    fn execute(&self, _: usize, _: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        not_impl_err!("DistributedContext does not support execution!")
    }
}
