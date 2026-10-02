use super::common::EventHandlerChain;
use async_trait::async_trait;
use datafusion::common::{Result, plan_err};
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
use num_traits::AsPrimitive;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Annotation attached to a single [ExecutionPlan] that determines how many distributed tasks
/// it should run on.
#[derive(Clone, Copy)]
pub struct TaskCountAnnotation {
    /// The load of a node measured in tasks required to properly execute it. This number is used
    /// as a hint for the distributed planner to decide on a final task count for a stage.
    /// This value is reconciled with other [TaskCountAnnotation]s provided by other nodes in the
    /// same stage.
    pub soft: f64,
    /// Exact number of tasks that should be allocated to the node.
    pub hard: Option<usize>,
}

impl TaskCountAnnotation {
    /// Builds a new [TaskCountAnnotation] with a provided `soft` value and no `hard` value.
    ///
    /// A `soft` value is a fractional number indicating the amount of ideal tasks in which a node
    /// should be executed. It's "soft" in the sense that the provided value is not a strictly
    /// required, and therefore, it might get reconciled with other values resulting in a different
    /// task count outcome.
    ///
    /// The `soft` value of a [TaskCountAnnotation] is linearly proportional to the load of the
    /// node, whether this is memory of CPU.
    pub fn soft<T: AsPrimitive<f64> + 'static>(value: T) -> Self {
        Self {
            soft: value.as_(),
            hard: None,
        }
    }

    /// Annotates an existing [TaskCountAnnotation] with a `hard` value, meaning that the planner
    /// should respect the provided value no matter what. If the planner tries to reconcile this
    /// value with a different incompatible one, it will fail (e.g. hard=1 + hard=3).
    ///
    /// The `hard` value is optional, but the `soft` value is mandatory, users wanting to build a
    /// [TaskCountAnnotation] must always pass through providing a `soft` value as well:
    ///
    /// ```rust
    /// # use datafusion_distributed::TaskCountAnnotation;
    ///
    /// let annotation = TaskCountAnnotation::soft(0.15).hard(1);
    /// ```
    pub fn hard(mut self, value: usize) -> Self {
        self.hard = Some(value);
        self
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
    /// The number of tasks that should be used in the [Stage] containing the leaf node.
    ///
    /// Even if implementations get to decide this number, there are situations where it can
    /// get overridden:
    /// - If a [Stage] contains multiple leaf nodes, the one that declares the biggest
    ///   task_count wins.
    /// - If there are less available workers than this number, the number of available workers
    ///   is chosen.
    pub task_count: TaskCountAnnotation,
}

impl DesiredTaskCountEventResponse {
    pub fn hard(mut self, value: usize) -> Self {
        self.task_count.hard = Some(value);
        self
    }

    pub fn soft<T: AsPrimitive<f64> + 'static>(value: T) -> Self {
        DesiredTaskCountEventResponse {
            task_count: TaskCountAnnotation {
                soft: value.as_(),
                hard: None,
            },
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
    /// All the [TaskEstimator] registered in the session will be applied to the node
    /// until one returns an estimation.
    ///
    ///
    /// If no estimation is returned from any of the registered [TaskEstimator]s, then:
    /// - If the node is a leaf node,`Maximum(1)` is assumed, hinting the distributed planner
    ///   that the leaf node cannot be distributed across tasks.
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
    pub fn as_usize(&self) -> usize {
        if let Some(exact) = self.hard {
            return exact;
        }
        (self.soft.ceil() as usize).max(1)
    }

    pub(crate) fn merge(self, other: TaskCountAnnotation) -> Result<Self> {
        Ok(Self {
            hard: match (self.hard, other.hard) {
                (Some(a), Some(b)) => {
                    if a != b {
                        return plan_err!("Incompatible hard task counts {a} and {b}");
                    }
                    Some(a)
                }
                (Some(a), None) => Some(a),
                (None, Some(b)) => Some(b),
                (None, None) => None,
            },
            soft: self.soft.max(other.soft),
        })
    }
}

impl Debug for TaskCountAnnotation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskCountAnnotation: soft={:.2}", self.soft)?;
        if let Some(hard) = self.hard {
            write!(f, ", hard={hard}")?;
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
