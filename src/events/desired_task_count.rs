use super::TaskCountAnnotation::{Desired, Maximum};
use super::common::EventHandlerChain;
use datafusion::execution::config::SessionConfig;
use datafusion::physical_plan::ExecutionPlan;
use std::sync::Arc;

/// Annotation attached to a single [ExecutionPlan] that determines how many distributed tasks
/// it should run on.
#[derive(Debug, Clone, Copy)]
pub enum TaskCountAnnotation {
    /// The desired number of distributed tasks for this node. The final task count for the
    /// annotated node might not be exactly this number, it is more like a hint, so depending
    /// on the desired task count of adjacent nodes, the final task count might change.
    Desired(usize),
    /// Sets a maximum number of distributed tasks for this node. Typically used with the inner
    /// value of 1, stating that this node cannot be executed in a distributed fashion.
    Maximum(usize),
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
    /// Tells the distributed planner that the evaluated stage can have **at maximum** the provided
    /// number of tasks, setting a hard upper limit.
    ///
    /// Returning `DesiredTaskCountEventResponse::maximum(1)` tells the distributed planner that the
    /// evaluated stage cannot be distributed.
    ///
    /// Even if a `DesiredTaskCountEventResponse::maximum(N)` is provided, any other node in the
    /// same stage providing a value of `DesiredTaskCountEventResponse::maximum(M)` where `M` < `N`
    /// will have preference.
    pub fn maximum(value: usize) -> Self {
        DesiredTaskCountEventResponse {
            task_count: Maximum(value),
        }
    }

    /// Tells the distributed planner that the evaluated can **optimally** have the provided
    /// number of tasks, setting a soft task count hint that can be overridden by others.
    ///
    /// The provided `DesiredTaskCountEventResponse::desired(N)` can be overridden by:
    /// - Other nodes providing a `DesiredTaskCountEventResponse::desired(M)` where `M` > `N`.
    /// - Any other node providing a `DesiredTaskCountEventResponse::maximum(M)` where `M` can be
    ///   anything.
    pub fn desired(value: usize) -> Self {
        DesiredTaskCountEventResponse {
            task_count: Desired(value),
        }
    }
}

pub trait DesiredTaskCountHandler: Send + Sync + 'static {
    /// Function applied to each node that returns a [DesiredTaskCountEventResponse] hinting how
    /// many tasks should be used in the [Stage] containing that node.
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
    fn handle(&self, ev: DesiredTaskCountEvent) -> Option<DesiredTaskCountEventResponse>;
}

impl From<TaskCountAnnotation> for usize {
    fn from(annotation: TaskCountAnnotation) -> Self {
        annotation.as_usize()
    }
}

impl TaskCountAnnotation {
    pub fn as_usize(&self) -> usize {
        match self {
            Desired(desired) => *desired,
            Maximum(maximum) => *maximum,
        }
    }

    pub(crate) fn limit(self, limit: usize) -> Self {
        match self {
            Desired(desired) => Desired(desired.min(limit)),
            Maximum(maximum) => Maximum(maximum.min(limit)),
        }
    }

    pub(crate) fn merge(self, other: TaskCountAnnotation) -> Self {
        match (self, other) {
            (Desired(a), Desired(b)) => Desired(std::cmp::max(a, b)),
            (Desired(_), Maximum(b)) => Maximum(b),
            (Maximum(a), Desired(_)) => Maximum(a),
            (Maximum(a), Maximum(b)) => Maximum(std::cmp::min(a, b)),
        }
    }
}

impl<F> DesiredTaskCountHandler for F
where
    F: Send + Sync + 'static,
    F: for<'a> Fn(DesiredTaskCountEvent<'a>) -> Option<DesiredTaskCountEventResponse>,
{
    fn handle(&self, ev: DesiredTaskCountEvent) -> Option<DesiredTaskCountEventResponse> {
        self(ev)
    }
}

impl DesiredTaskCountHandler for usize {
    fn handle(&self, ev: DesiredTaskCountEvent) -> Option<DesiredTaskCountEventResponse> {
        ev.plan
            .children()
            .is_empty()
            .then(|| DesiredTaskCountEventResponse::desired(*self))
    }
}

impl DesiredTaskCountHandler for Arc<dyn DesiredTaskCountHandler> {
    fn handle(&self, ev: DesiredTaskCountEvent) -> Option<DesiredTaskCountEventResponse> {
        self.as_ref().handle(ev)
    }
}

pub(crate) type DesiredTaskCountHandlers = EventHandlerChain<dyn DesiredTaskCountHandler>;

impl DesiredTaskCountHandlers {
    pub(crate) fn handle(ev: DesiredTaskCountEvent) -> Option<DesiredTaskCountEventResponse> {
        ev.session_config
            .get_extension::<DesiredTaskCountHandlers>()?
            .find_map(|handler| handler.handle(ev))
    }
}
