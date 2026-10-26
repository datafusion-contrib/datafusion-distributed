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
    /// The node's soft task-count estimate. The distributed planner combines this estimate with
    /// estimates from other nodes in the same stage before choosing a task count. Fractional
    /// values are allowed.
    pub(crate) soft: f64,

    /// An optional task-count restriction, independent of the soft estimate.
    pub(crate) restriction: TaskCountRestriction,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TaskCountRestriction {
    None,
    Exact(NonZeroUsize),
    Min(NonZeroUsize),
    Max(NonZeroUsize),
    /// Inclusive range of (min, max)
    Range(NonZeroUsize, NonZeroUsize),
}

impl TaskCountRestriction {
    pub(crate) fn merge(self, other: Self) -> Result<Self> {
        let restriction = match (self, other) {
            (Self::None, restriction) | (restriction, Self::None) => restriction,
            (Self::Exact(a), TaskCountRestriction::Exact(b)) => {
                if a != b {
                    return plan_err!("Incompatible exact task counts {a} and {b}");
                }
                Self::Exact(a)
            }
            (Self::Min(a), TaskCountRestriction::Min(b)) => Self::Min(a.max(b)),
            (Self::Max(a), TaskCountRestriction::Max(b)) => Self::Max(a.min(b)),
            (Self::Exact(exact), TaskCountRestriction::Min(min))
            | (Self::Min(min), TaskCountRestriction::Exact(exact)) => {
                if exact < min {
                    return plan_err!(
                        "Exact task count {exact} is below the required minimum {min}"
                    );
                }
                Self::Exact(exact)
            }
            (Self::Exact(exact), TaskCountRestriction::Max(max))
            | (Self::Max(max), TaskCountRestriction::Exact(exact)) => {
                if exact > max {
                    return plan_err!(
                        "Exact task count {exact} is above the required maximum {max}"
                    );
                }
                Self::Exact(exact)
            }
            (Self::Exact(exact), TaskCountRestriction::Range(min, max))
            | (Self::Range(min, max), TaskCountRestriction::Exact(exact)) => {
                if !(min..=max).contains(&exact) {
                    return plan_err!(
                        "Exact task count {exact} is outside of the required range {min}..={max}"
                    );
                }
                Self::Exact(exact)
            }
            (Self::Range(rmin, rmax), TaskCountRestriction::Min(min))
            | (Self::Min(min), TaskCountRestriction::Range(rmin, rmax)) => {
                if min > rmax {
                    return plan_err!(
                        "Minimum task count {min} is above the required maximum {rmax}"
                    );
                }
                let min = min.max(rmin);
                if min == rmax {
                    Self::Exact(min)
                } else {
                    Self::Range(min, rmax)
                }
            }
            (Self::Range(rmin, rmax), TaskCountRestriction::Max(max))
            | (Self::Max(max), TaskCountRestriction::Range(rmin, rmax)) => {
                if max < rmin {
                    return plan_err!(
                        "Maximum task count {max} is below the required minimum {rmin}"
                    );
                }
                let max = max.min(rmax);
                if rmin == max {
                    Self::Exact(max)
                } else {
                    Self::Range(rmin, max)
                }
            }
            (Self::Min(min), TaskCountRestriction::Max(max))
            | (Self::Max(max), TaskCountRestriction::Min(min)) => {
                if min > max {
                    return plan_err!(
                        "Minimum task count {min} is above the required maximum {max}"
                    );
                }
                if min == max {
                    Self::Exact(min)
                } else {
                    Self::Range(min, max)
                }
            }
            (Self::Range(amin, amax), TaskCountRestriction::Range(bmin, bmax)) => {
                // ranges must overlap.
                if !(amin <= bmax && bmin <= amax) {
                    return plan_err!(
                        "Incompatible task count ranges of {amin}..={amax} and {bmin}..={bmax}"
                    );
                }
                let min = amin.max(bmin);
                let max = amax.min(bmax);
                if min == max {
                    Self::Exact(min)
                } else {
                    Self::Range(min, max)
                }
            }
        };
        Ok(restriction)
    }
}

impl TaskCountAnnotation {
    /// Creates an annotation with a soft task-count estimate and no task-count restriction.
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

    /// Requires the node to execute in exactly `exact` tasks, while retaining `soft` as its soft
    /// task-count estimate. If another node imposes an incompatible exact count, planning fails.
    ///
    /// The `soft` value is required, as it's needed for estimating the amount of tasks upstream of
    /// the node, regardless of the exact restriction the annotated node imposes. `soft` can be
    /// left to 0 indicating that nodes upstream should not include this node's estimate when
    /// choosing their task count.
    pub fn exact<T: AsPrimitive<f64> + 'static>(exact: NonZeroUsize, soft: T) -> Self {
        Self {
            soft: soft.as_(),
            restriction: TaskCountRestriction::Exact(exact),
        }
    }

    /// Gets the exact task count if any.
    pub fn get_exact(&self) -> Option<NonZeroUsize> {
        match self.restriction {
            TaskCountRestriction::Exact(v) => Some(v),
            _ => None,
        }
    }

    /// Gets the minimum task count if any.
    pub fn get_min(&self) -> Option<NonZeroUsize> {
        match self.restriction {
            TaskCountRestriction::Exact(min)
            | TaskCountRestriction::Min(min)
            | TaskCountRestriction::Range(min, _) => Some(min),
            _ => None,
        }
    }

    /// Gets the maximum task count if any.
    pub fn get_max(&self) -> Option<NonZeroUsize> {
        match self.restriction {
            TaskCountRestriction::Exact(max)
            | TaskCountRestriction::Max(max)
            | TaskCountRestriction::Range(_, max) => Some(max),
            _ => None,
        }
    }

    /// Requires the node to execute in at least `min` tasks, while retaining `soft` as its soft
    /// task-count estimate. The planner may assign more than `min` tasks based on this estimate
    /// and other nodes' requirements.
    pub fn min<T: AsPrimitive<f64> + 'static>(min: NonZeroUsize, soft: T) -> Self {
        Self {
            soft: soft.as_(),
            restriction: TaskCountRestriction::Min(min),
        }
    }

    /// Requires the node to execute in at most `max` tasks, while retaining `soft` as its soft
    /// task-count estimate. The planner may assign fewer than `max` tasks based on this estimate
    /// and other nodes' requirements.
    pub fn max<T: AsPrimitive<f64> + 'static>(max: NonZeroUsize, soft: T) -> Self {
        Self {
            soft: soft.as_(),
            restriction: TaskCountRestriction::Max(max),
        }
    }

    /// Requires the node to execute in between `min` and `max` tasks (inclusive), while
    /// retaining `soft` as its soft task-count estimate. The planner may assign any number in
    /// that range based on this estimate and other nodes' requirements.
    ///
    /// Returns an error if `min` is greater than `max`. If `min` equals `max`, the result is an
    /// exact requirement, as if built with [`TaskCountAnnotation::exact`].
    pub fn try_range<T: AsPrimitive<f64> + 'static>(
        min: NonZeroUsize,
        max: NonZeroUsize,
        soft: T,
    ) -> Result<Self> {
        if min > max {
            return plan_err!(
                "Task count range minimum {min} must be less than or equal to its maximum {max}"
            );
        }
        let restriction = if min == max {
            TaskCountRestriction::Exact(min)
        } else {
            TaskCountRestriction::Range(min, max)
        };
        Ok(Self {
            soft: soft.as_(),
            restriction,
        })
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
    /// Soft task-count estimate and any task-count restriction (exact, minimum, maximum or range)
    /// for this node. The planner reconciles estimates within a stage, while restrictions must be
    /// satisfied.
    pub task_count: TaskCountAnnotation,
}

impl DesiredTaskCountEventResponse {
    /// Creates an annotation with a soft task-count estimate and no task-count restriction.
    ///
    /// The estimate is measured in tasks and may be fractional. The planner combines it with
    /// other nodes' estimates in the same stage before choosing an integer task count.
    pub fn soft<T: AsPrimitive<f64> + 'static>(value: T) -> Self {
        DesiredTaskCountEventResponse {
            task_count: TaskCountAnnotation::soft(value),
        }
    }

    /// Requires the node to execute in exactly `exact` tasks, while retaining `soft` as its soft
    /// task-count estimate. If another node imposes an incompatible exact count, planning fails.
    ///
    /// The `soft` value is required, as it's needed for estimating the amount of tasks upstream of
    /// the node, regardless of the exact restriction the annotated node imposes. `soft` can be
    /// left to 0 indicating that nodes upstream should not include this node's estimate when
    /// choosing their task count.
    pub fn exact<T: AsPrimitive<f64> + 'static>(exact: NonZeroUsize, soft: T) -> Self {
        DesiredTaskCountEventResponse {
            task_count: TaskCountAnnotation::exact(exact, soft),
        }
    }

    /// Requires the node to execute in at least `min` tasks, while retaining `soft` as its soft
    /// task-count estimate. If another node imposes an incompatible count, planning fails.
    pub fn min<T: AsPrimitive<f64> + 'static>(min: NonZeroUsize, soft: T) -> Self {
        DesiredTaskCountEventResponse {
            task_count: TaskCountAnnotation::min(min, soft),
        }
    }

    /// Requires the node to execute in at most `max` tasks, while retaining `soft` as its soft
    /// task-count estimate. If another node imposes an incompatible count, planning fails.
    pub fn max<T: AsPrimitive<f64> + 'static>(max: NonZeroUsize, soft: T) -> Self {
        DesiredTaskCountEventResponse {
            task_count: TaskCountAnnotation::max(max, soft),
        }
    }

    /// Requires the node to execute in between `min` and `max` tasks (inclusive), while retaining
    /// `soft` as its soft task-count estimate. If another node imposes an incompatible count,
    /// planning fails. Returns an error if `min` is greater than `max`.
    pub fn try_range<T: AsPrimitive<f64> + 'static>(
        min: NonZeroUsize,
        max: NonZeroUsize,
        soft: T,
    ) -> Result<Self> {
        Ok(DesiredTaskCountEventResponse {
            task_count: TaskCountAnnotation::try_range(min, max, soft)?,
        })
    }

    /// Requires the node to execute in exactly one task without contributing a soft task-count
    /// estimate upstream.
    pub fn exactly_one() -> Self {
        DesiredTaskCountEventResponse::exact(NonZeroUsize::MIN, 0.0)
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
    /// Resolves the soft estimate and restriction to an integer task count.
    pub fn as_usize(&self) -> usize {
        let tasks = (self.soft.ceil() as usize).max(1);
        match self.restriction {
            TaskCountRestriction::None => tasks,
            TaskCountRestriction::Exact(exact) => exact.get(),
            TaskCountRestriction::Min(min) => tasks.max(min.get()),
            TaskCountRestriction::Max(max) => tasks.min(max.get()),
            TaskCountRestriction::Range(min, max) => min.get().max(tasks).min(max.get()),
        }
    }

    pub(crate) fn merge(self, other: TaskCountAnnotation) -> Result<Self> {
        let restriction = self.restriction.merge(other.restriction)?;
        Ok(Self {
            soft: self.soft.max(other.soft),
            restriction,
        })
    }
}

impl Debug for TaskCountAnnotation {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskCountAnnotation: soft={:.2}", self.soft)?;
        match self.restriction {
            TaskCountRestriction::None => {}
            TaskCountRestriction::Exact(exact) => write!(f, ", exact={exact}")?,
            TaskCountRestriction::Min(min) => write!(f, ", min={min}")?,
            TaskCountRestriction::Max(max) => write!(f, ", max={max}")?,
            TaskCountRestriction::Range(min, max) => write!(f, ", range={min}..={max}")?,
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
