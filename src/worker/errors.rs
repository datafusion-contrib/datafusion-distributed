use crate::TaskKey;
#[cfg(feature = "grpc")]
use datafusion::error::DataFusionError;
use std::error::Error;
use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub(super) struct UnregisteredTaskDuringDrainError {
    task_key: TaskKey,
}

impl UnregisteredTaskDuringDrainError {
    pub(super) fn new(task_key: TaskKey) -> Self {
        Self { task_key }
    }

    #[cfg(feature = "grpc")]
    fn to_tonic_status(&self) -> tonic::Status {
        tonic::Status::unavailable(self.to_string())
    }
}

impl Display for UnregisteredTaskDuringDrainError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "worker is draining and task key {:?} is not registered",
            self.task_key
        )
    }
}

impl Error for UnregisteredTaskDuringDrainError {}

#[cfg(feature = "grpc")]
pub(crate) fn execute_task_error_to_tonic_status(error: DataFusionError) -> tonic::Status {
    if let DataFusionError::External(source) = &error
        && let Some(error) = source.downcast_ref::<UnregisteredTaskDuringDrainError>()
    {
        return error.to_tonic_status();
    }

    crate::protocol::grpc::datafusion_error_to_tonic_status(error)
}

#[cfg(all(test, feature = "grpc"))]
mod tests {
    use super::*;
    use tonic::Code;
    use uuid::Uuid;

    #[test]
    fn unregistered_task_during_drain_maps_to_unavailable() {
        let error =
            DataFusionError::External(Box::new(UnregisteredTaskDuringDrainError::new(TaskKey {
                query_id: Uuid::nil(),
                stage_id: 0,
                task_number: 0,
            })));

        assert_eq!(
            execute_task_error_to_tonic_status(error).code(),
            Code::Unavailable
        );
    }
}
