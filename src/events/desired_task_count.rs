use super::common::EventHandlerChain;
use async_trait::async_trait;
use datafusion::common::{Result, plan_err};
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
use num_traits::AsPrimitive;
use std::fmt::{Debug, Formatter};
use std::num::NonZeroUsize;
use std::sync::Arc;

/// Annotation attached to a single [ExecutionPlan] that determines how many distributed tasks
/// it should run on.
#[derive(Clone, Copy)]
pub struct TaskCountAnnotation {
    /// The node's estimated load, expressed in task units. The distributed planner combines
    /// this soft estimate with estimates from other nodes in the same stage before choosing a
    /// task count. Fractional values are allowed.
    pub(crate) soft: f64,

    /// An optional task-count restriction, independent of the soft load estimate.
    pub(crate) restriction: TaskCountRestriction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TaskCountRestriction {
    None,
    Exact(NonZeroUsize),
    Min(NonZeroUsize),
}

impl TaskCountAnnotation {
    /// Creates an annotation with a soft load estimate and no task-count restriction.
    ///
    /// The estimate is measured in tasks and may be fractional. The planner combines it with
    /// other nodes' estimates in the same stage before choosing an integer task count.
    pub fn soft<T: AsPrimitive<f64> + 'static>(value: T) -> Self {
        Self {
            soft: value.as_(),
            restriction: TaskCountRestriction::None,
        }
    }

    /// Gets the soft task count.
    pub fn get_soft(&self) -> f64 {
        self.soft
    }

    /// Requires the node to execute in exactly `exact` tasks, while retaining `soft` as its load
    /// estimate. If another node imposes an incompatible exact count, planning fails.
    ///
    /// The `soft` value is required, as it's needed for estimating the amount of tasks upstream of
    /// the node, regardless of the exact restriction the annotated node imposes. `soft` can be
    /// left to 0 indicating that nodes upstream should not account for any load introduced by this
    /// node.
    pub fn exact<T: AsPrimitive<f64> + 'static>(exact: NonZeroUsize, soft: T) -> Self {
        Self {
            soft: soft.as_(),
            restriction: TaskCountRestriction::Exact(exact),
        }
    }

    /// Gets the exact task count if any.
    pub fn get_exact(&self) -> Option<NonZeroUsize> {
        match self.restriction {
            TaskCountRestriction::None => None,
            TaskCountRestriction::Exact(v) => Some(v),
            TaskCountRestriction::Min(_) => None,
        }
    }

    /// Requires the node to execute in at least `min` tasks, while retaining `soft` as its load
    /// estimate. The planner may assign more than `min` tasks based on the load estimate and
    /// other nodes' requirements.
    pub(crate) fn min<T: AsPrimitive<f64> + 'static>(min: NonZeroUsize, soft: T) -> Self {
        Self {
            soft: soft.as_(),
            restriction: TaskCountRestriction::Min(min),
        }
    }
}

/// Information supplied when the planner asks a handler for a node's desired task count.
///
/// Handlers may return a [`DesiredTaskCountEventResponse`] for any execution-plan node. The
/// planner reconciles responses from the nodes in the same stage into its final task count.
#[derive(Clone, Copy)]
pub struct DesiredTaskCountEvent<'a> {
    /// The execution-plan node being evaluated.
    pub plan: &'a Arc<dyn ExecutionPlan>,
    /// The session configuration that registered the handlers and holds query options.
    pub session_config: &'a SessionConfig,
}

/// Result of running a [TaskEstimator] on a leaf node. It tells the distributed planner hints
/// about how many tasks should be used in [Stage]s that contain leaf nodes.
pub struct DesiredTaskCountEventResponse {
    /// Estimated load and any exact task-count requirement for this node.
    /// The planner reconciles loads within a stage, while exact requirements must be satisfied.
    pub task_count: TaskCountAnnotation,
}

impl DesiredTaskCountEventResponse {
    /// Creates an annotation with a soft load estimate and no task-count restriction.
    ///
    /// The estimate is measured in tasks and may be fractional. The planner combines it with
    /// other nodes' estimates in the same stage before choosing an integer task count.
    pub fn soft<T: AsPrimitive<f64> + 'static>(value: T) -> Self {
        DesiredTaskCountEventResponse {
            task_count: TaskCountAnnotation::soft(value),
        }
    }

    /// Requires the node to execute in exactly `exact` tasks, while retaining `soft` as its load
    /// estimate. If another node imposes an incompatible exact count, planning fails.
    ///
    /// The `soft` value is required, as it's needed for estimating the amount of tasks upstream of
    /// the node, regardless of the exact restriction the annotated node imposes. `soft` can be
    /// left to 0 indicating that nodes upstream should not account for any load introduced by this
    /// node.
    pub fn exact<T: AsPrimitive<f64> + 'static>(exact: NonZeroUsize, soft: T) -> Self {
        DesiredTaskCountEventResponse {
            task_count: TaskCountAnnotation::exact(exact, soft),
        }
    }

    /// Tells the distributed planner that this node does not impose a finite desired task count.
    pub fn unbounded() -> Self {
        DesiredTaskCountEventResponse::soft(f64::MAX)
    }
}

#[async_trait]
pub trait DesiredTaskCountHandler: Send + Sync + 'static {
    /// Function applied to each node that returns a [DesiredTaskCountEventResponse] hinting how
    /// many tasks should be used in the [Stage] containing that node, or an error if the hint
    /// cannot be determined.
    ///
    /// Handlers are asynchronous and may await metadata or external services. Handler functions
    /// return a [`DesiredTaskCountFuture`] so their futures can borrow from the event.
    ///
    /// Some nodes like unions and certain types of joins are managed by this project, and this
    /// event handler will not run on those nodes.
    ///
    /// If no estimation is returned from any of the registered [DesiredTaskCountHandler]s, then:
    /// - If the node is a leaf node, an exact count of one is assumed, so the leaf
    ///   is executed in a single task.
    /// - If the node is a normal node in the plan, then the maximum task count from its children
    ///   is inherited.
    async fn handle(
        &self,
        ev: DesiredTaskCountEvent<'_>,
    ) -> Option<Result<DesiredTaskCountEventResponse>>;
}

impl From<TaskCountAnnotation> for usize {
    fn from(annotation: TaskCountAnnotation) -> Self {
        annotation.as_usize()
    }
}

impl TaskCountAnnotation {
    /// Resolves the load and restriction to an integer task count.
    pub fn as_usize(&self) -> usize {
        let tasks = (self.soft.ceil() as usize).max(1);
        match self.restriction {
            TaskCountRestriction::None => tasks,
            TaskCountRestriction::Exact(exact) => exact.get(),
            TaskCountRestriction::Min(min) => tasks.max(min.get()),
        }
    }

    pub(crate) fn merge(self, other: TaskCountAnnotation) -> Result<Self> {
        let restriction = match (self.restriction, other.restriction) {
            (TaskCountRestriction::None, restriction)
            | (restriction, TaskCountRestriction::None) => restriction,
            (TaskCountRestriction::Exact(a), TaskCountRestriction::Exact(b)) => {
                if a != b {
                    return plan_err!("Incompatible exact task counts {a} and {b}");
                }
                TaskCountRestriction::Exact(a)
            }
            (TaskCountRestriction::Min(a), TaskCountRestriction::Min(b)) => {
                TaskCountRestriction::Min(a.max(b))
            }
            (TaskCountRestriction::Exact(exact), TaskCountRestriction::Min(min))
            | (TaskCountRestriction::Min(min), TaskCountRestriction::Exact(exact)) => {
                if exact < min {
                    return plan_err!(
                        "Exact task count {exact} is below the required minimum {min}"
                    );
                }
                TaskCountRestriction::Exact(exact)
            }
        };
        Ok(Self {
            soft: self.soft.max(other.soft),
            restriction,
        })
    }
}

impl Debug for TaskCountAnnotation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskCountAnnotation: load={:.2}", self.soft)?;
        match self.restriction {
            TaskCountRestriction::None => {}
            TaskCountRestriction::Exact(exact) => write!(f, ", exact={exact}")?,
            TaskCountRestriction::Min(min) => write!(f, ", min={min}")?,
        }
        Ok(())
    }
}

#[async_trait]
impl<F> DesiredTaskCountHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(DesiredTaskCountEvent<'a>) -> Option<Result<DesiredTaskCountEventResponse>>,
{
    async fn handle(
        &self,
        ev: DesiredTaskCountEvent<'_>,
    ) -> Option<Result<DesiredTaskCountEventResponse>> {
        self(ev)
    }
}

#[async_trait]
impl DesiredTaskCountHandler for usize {
    async fn handle(
        &self,
        ev: DesiredTaskCountEvent<'_>,
    ) -> Option<Result<DesiredTaskCountEventResponse>> {
        ev.plan
            .children()
            .is_empty()
            .then(|| Ok(DesiredTaskCountEventResponse::soft(*self)))
    }
}

#[async_trait]
impl DesiredTaskCountHandler for Arc<dyn DesiredTaskCountHandler> {
    async fn handle(
        &self,
        ev: DesiredTaskCountEvent<'_>,
    ) -> Option<Result<DesiredTaskCountEventResponse>> {
        self.as_ref().handle(ev).await
    }
}

pub(crate) type DesiredTaskCountHandlers = EventHandlerChain<dyn DesiredTaskCountHandler>;

impl DesiredTaskCountHandlers {
    pub(crate) async fn handle(
        ev: DesiredTaskCountEvent<'_>,
    ) -> Option<Result<DesiredTaskCountEventResponse>> {
        let handlers = ev
            .session_config
            .get_extension::<DesiredTaskCountHandlers>()?;
        for handler in handlers.iter() {
            if let Some(response) = handler.handle(ev).await {
                return Some(response);
            }
        }
        None
    }
}
