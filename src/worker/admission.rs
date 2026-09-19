use crate::common::RetryOutcome;
use crate::{OpenTaskRequest, SetPlanRequest, TaskKey, Worker};
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use http::HeaderMap;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use uuid::Uuid;

/// Where the coordinator should retry a task reservation rejected by a worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryTarget {
    /// Retry the reservation against the same worker.
    SameWorker,
    /// Select a different worker before retrying the reservation.
    DifferentWorker,
}

/// Admission-only task information passed to a [`WorkerAdmissionController`].
///
/// This deliberately excludes the physical plan so admission can finish without invoking plan
/// codecs, session builders, samplers, or any other task side effects.
#[derive(Clone, Debug)]
pub struct WorkerAdmissionRequest {
    /// The task that the coordinator wants to reserve.
    pub task_key: TaskKey,
    /// The unique placement attempt. Every retry uses a different ID.
    pub attempt_id: Uuid,
    /// The number of tasks sharing this task's subplan.
    pub task_count: usize,
    /// Propagated HTTP headers for tenant, authentication, and configuration policies.
    pub headers: HeaderMap,
}

impl WorkerAdmissionRequest {
    fn from_open_task(request: OpenTaskRequest, headers: HeaderMap) -> Self {
        Self {
            task_key: request.task_key,
            attempt_id: request.attempt_id,
            task_count: request.task_count,
            headers,
        }
    }
}

/// An opaque, cloneable guard representing resources reserved during worker admission.
///
/// The worker holds this value until task state is removed. Admission controllers can wrap an
/// owned semaphore permit or another RAII guard with [`Self::new`] to release capacity
/// automatically when the reservation is abandoned or the task completes.
#[derive(Clone, Default)]
pub struct WorkerAdmissionPermit {
    _guard: Option<Arc<dyn Send + Sync>>,
}

impl WorkerAdmissionPermit {
    /// Keeps `guard` alive until the reservation is abandoned or its committed task is removed.
    pub fn new(guard: impl Send + Sync + 'static) -> Self {
        Self {
            _guard: Some(Arc::new(guard)),
        }
    }
}

impl Debug for WorkerAdmissionPermit {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerAdmissionPermit")
            .finish_non_exhaustive()
    }
}

/// A worker-authored admission failure and its optional retry directive.
#[derive(Debug)]
pub struct WorkerAdmissionRejection {
    error: DataFusionError,
    retry_target: Option<RetryTarget>,
}

impl WorkerAdmissionRejection {
    /// Rejects the task permanently. The coordinator returns this error without retrying.
    pub fn fatal(error: DataFusionError) -> Self {
        Self {
            error,
            retry_target: None,
        }
    }

    /// Rejects the task and directs the coordinator to retry the reservation.
    pub fn retry(error: DataFusionError, target: RetryTarget) -> Self {
        Self {
            error,
            retry_target: Some(target),
        }
    }

    /// Returns the error reported to the coordinator.
    pub fn error(&self) -> &DataFusionError {
        &self.error
    }

    /// Returns the worker's retry directive, or `None` for a fatal rejection.
    pub fn retry_target(&self) -> Option<RetryTarget> {
        self.retry_target
    }

    pub(crate) fn into_parts(self) -> (DataFusionError, Option<RetryTarget>) {
        (self.error, self.retry_target)
    }

    pub(crate) fn into_datafusion_error(self) -> DataFusionError {
        let (error, retry_target) = self.into_parts();
        match retry_target {
            Some(RetryTarget::SameWorker) => RetryOutcome::SameUrl.tag(error),
            Some(RetryTarget::DifferentWorker) => RetryOutcome::OtherUrl.tag(error),
            None => error,
        }
    }
}

impl From<DataFusionError> for WorkerAdmissionRejection {
    fn from(error: DataFusionError) -> Self {
        Self::fatal(error)
    }
}

/// Decides whether a worker can reserve capacity for a task.
#[async_trait]
pub trait WorkerAdmissionController: Send + Sync {
    /// Reserves capacity for `request` or returns an explicit rejection policy.
    async fn admit(
        &self,
        request: &WorkerAdmissionRequest,
    ) -> Result<WorkerAdmissionPermit, WorkerAdmissionRejection>;
}

pub(crate) struct AcceptAllAdmissionController;

#[async_trait]
impl WorkerAdmissionController for AcceptAllAdmissionController {
    async fn admit(
        &self,
        _request: &WorkerAdmissionRequest,
    ) -> Result<WorkerAdmissionPermit, WorkerAdmissionRejection> {
        Ok(WorkerAdmissionPermit::default())
    }
}

pub(crate) struct TaskReservation {
    request: WorkerAdmissionRequest,
    permit: WorkerAdmissionPermit,
}

impl TaskReservation {
    pub(crate) fn commit(
        self,
        request: &SetPlanRequest,
    ) -> Result<WorkerAdmissionPermit, DataFusionError> {
        if self.request.task_key != request.task_key {
            return Err(DataFusionError::Plan(format!(
                "SetPlanRequest task key {:?} does not match reserved task key {:?}",
                request.task_key, self.request.task_key
            )));
        }
        if self.request.attempt_id != request.attempt_id {
            return Err(DataFusionError::Plan(format!(
                "SetPlanRequest attempt {} does not match reserved attempt {}",
                request.attempt_id, self.request.attempt_id
            )));
        }
        if self.request.task_count != request.task_count {
            return Err(DataFusionError::Plan(format!(
                "SetPlanRequest task count {} does not match reserved task count {}",
                request.task_count, self.request.task_count
            )));
        }
        Ok(self.permit)
    }
}

impl Worker {
    pub(crate) async fn admit_task(
        &self,
        headers: HeaderMap,
        request: OpenTaskRequest,
    ) -> Result<TaskReservation, WorkerAdmissionRejection> {
        let request = WorkerAdmissionRequest::from_open_task(request, headers);
        let permit = self.admission_controller.admit(&request).await?;
        Ok(TaskReservation { request, permit })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MaybeEncoded;
    use datafusion::physical_plan::empty::EmptyExec;
    use std::sync::atomic::{AtomicBool, Ordering};
    use url::Url;

    #[test]
    fn matching_reservation_holds_permit_until_drop() {
        let dropped = Arc::new(AtomicBool::new(false));
        let key = TaskKey {
            query_id: Uuid::new_v4(),
            stage_id: 1,
            task_number: 2,
        };
        let attempt_id = Uuid::new_v4();
        let reservation = TaskReservation {
            request: WorkerAdmissionRequest {
                task_key: key,
                attempt_id,
                task_count: 3,
                headers: HeaderMap::new(),
            },
            permit: WorkerAdmissionPermit::new(DropSignal(Arc::clone(&dropped))),
        };
        let request = SetPlanRequest {
            task_key: key,
            attempt_id,
            task_count: 3,
            plan: MaybeEncoded::Decoded(Arc::new(EmptyExec::new(Arc::new(
                datafusion::arrow::datatypes::Schema::empty(),
            )))),
            work_unit_feed_declarations: vec![],
            target_worker_url: Url::parse("http://worker.test").unwrap(),
            query_start_time_ns: 0,
        };

        let permit = reservation.commit(&request).unwrap();
        assert!(!dropped.load(Ordering::SeqCst));
        drop(permit);
        assert!(dropped.load(Ordering::SeqCst));
    }

    struct DropSignal(Arc<AtomicBool>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
}
