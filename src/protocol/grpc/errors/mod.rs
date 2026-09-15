#![allow(clippy::upper_case_acronyms, clippy::vec_box)]

use arrow_flight::error::FlightError;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use prost::Message;
use std::borrow::Borrow;

use super::errors::datafusion_error::DataFusionErrorProto;
use crate::RetryTarget;
use crate::common::RetryOutcome;

const DATAFUSION_ERROR_STATUS: &str = "DataFusionError";
const RETRYABLE_DATAFUSION_ERROR_STATUS: &str = "RetryableDataFusionError";

#[derive(Clone, PartialEq, Message)]
struct RetryableDataFusionErrorProto {
    #[prost(message, optional, tag = "1")]
    error: Option<DataFusionErrorProto>,
    #[prost(enumeration = "RetryTargetProto", tag = "2")]
    retry_target: i32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, prost::Enumeration)]
enum RetryTargetProto {
    Unspecified = 0,
    SameWorker = 1,
    DifferentWorker = 2,
}

mod arrow_error;
mod datafusion_error;
mod io_error;
mod objectstore_error;
mod parquet_error;
mod parser_error;
mod schema_error;

/// Encodes a [DataFusionError] into a [tonic::Status] error. The produced error is suitable
/// to be sent over the wire and decoded by the receiving end, recovering the original
/// [DataFusionError] across a network boundary with [tonic_status_to_datafusion_error].
pub fn datafusion_error_to_tonic_status(err: impl Borrow<DataFusionError>) -> tonic::Status {
    let err = DataFusionErrorProto::from_datafusion_error(err.borrow());
    let err = err.encode_to_vec();

    tonic::Status::with_details(tonic::Code::Internal, DATAFUSION_ERROR_STATUS, err.into())
}

/// Encodes a worker error together with the worker's explicit retry decision.
pub(crate) fn datafusion_error_to_tonic_status_with_retry(
    err: impl Borrow<DataFusionError>,
    retry_target: RetryTarget,
) -> tonic::Status {
    let retry_target = match retry_target {
        RetryTarget::SameWorker => RetryTargetProto::SameWorker,
        RetryTarget::DifferentWorker => RetryTargetProto::DifferentWorker,
    };
    let err = RetryableDataFusionErrorProto {
        error: Some(DataFusionErrorProto::from_datafusion_error(err.borrow())),
        retry_target: retry_target as i32,
    }
    .encode_to_vec();

    tonic::Status::with_details(
        tonic::Code::Internal,
        RETRYABLE_DATAFUSION_ERROR_STATUS,
        err.into(),
    )
}

/// Decodes a [DataFusionError] from a [tonic::Status] error. If the provided [tonic::Status]
/// error was produced with [datafusion_error_to_tonic_status], this function will be able to
/// recover it even across a network boundary.
///
/// The provided [tonic::Status] error might also be something else, like an actual network
/// failure. This function returns `None` for those cases.
pub fn tonic_status_to_datafusion_error(
    status: impl Borrow<tonic::Status>,
) -> Option<DataFusionError> {
    let status = status.borrow();
    if status.code() != tonic::Code::Internal {
        return None;
    }

    match status.message() {
        DATAFUSION_ERROR_STATUS => match DataFusionErrorProto::decode(status.details()) {
            Ok(err_proto) => Some(err_proto.to_datafusion_err()),
            Err(err) => Some(internal_datafusion_err!(
                "Cannot decode DataFusionError: {err}"
            )),
        },
        RETRYABLE_DATAFUSION_ERROR_STATUS => {
            match RetryableDataFusionErrorProto::decode(status.details()) {
                Ok(proto) => {
                    let error = proto.error.ok_or_else(|| {
                        internal_datafusion_err!("RetryableDataFusionError is missing error")
                    });
                    let target = RetryTargetProto::try_from(proto.retry_target).map_err(|_| {
                        internal_datafusion_err!(
                            "RetryableDataFusionError has invalid retry target {}",
                            proto.retry_target
                        )
                    });
                    let target = target.and_then(|target| match target {
                        RetryTargetProto::Unspecified => Err(internal_datafusion_err!(
                            "RetryableDataFusionError is missing retry target"
                        )),
                        target => Ok(target),
                    });
                    Some(match (error, target) {
                        (Ok(error), Ok(RetryTargetProto::SameWorker)) => {
                            RetryOutcome::SameUrl.tag(error.to_datafusion_err())
                        }
                        (Ok(error), Ok(RetryTargetProto::DifferentWorker)) => {
                            RetryOutcome::OtherUrl.tag(error.to_datafusion_err())
                        }
                        (Ok(_), Ok(RetryTargetProto::Unspecified)) => {
                            internal_datafusion_err!(
                                "RetryableDataFusionError is missing retry target"
                            )
                        }
                        (Err(error), _) | (_, Err(error)) => error,
                    })
                }
                Err(err) => Some(internal_datafusion_err!(
                    "Cannot decode RetryableDataFusionError: {err}"
                )),
            }
        }
        _ => None,
    }
}

/// Same as [tonic_status_to_datafusion_error] but suitable to be used in `.map_err` calls that
/// accept a [tonic::Status] error.
pub fn map_status_to_datafusion_error(err: tonic::Status) -> DataFusionError {
    tonic_status_to_datafusion_error(&err)
        .unwrap_or_else(|| DataFusionError::External(Box::new(err)))
}

/// Same as [tonic_status_to_datafusion_error] but suitable to be used in `.map_err` calls that
/// accept a [FlightError] error.
pub fn map_flight_to_datafusion_error(err: FlightError) -> DataFusionError {
    match err {
        FlightError::Tonic(status) => map_status_to_datafusion_error(*status),
        err => DataFusionError::External(Box::new(err)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_target_round_trips_over_tonic_status() {
        for (target, expected) in [
            (RetryTarget::SameWorker, RetryOutcome::SameUrl),
            (RetryTarget::DifferentWorker, RetryOutcome::OtherUrl),
        ] {
            let status = datafusion_error_to_tonic_status_with_retry(
                DataFusionError::ResourcesExhausted("busy".to_string()),
                target,
            );
            let error = tonic_status_to_datafusion_error(status).unwrap();
            assert_eq!(RetryOutcome::try_from_err(&error), Some(expected));
            assert!(error.to_string().contains("busy"));
        }
    }

    #[test]
    fn ordinary_datafusion_error_is_not_retryable() {
        let status = datafusion_error_to_tonic_status(DataFusionError::Execution(
            "do not retry".to_string(),
        ));
        let error = tonic_status_to_datafusion_error(status).unwrap();
        assert_eq!(RetryOutcome::try_from_err(&error), None);
    }

    #[test]
    fn missing_retry_target_fails_closed() {
        let details = RetryableDataFusionErrorProto {
            error: Some(DataFusionErrorProto::from_datafusion_error(
                &DataFusionError::Execution("ambiguous".to_string()),
            )),
            retry_target: RetryTargetProto::Unspecified as i32,
        }
        .encode_to_vec();
        let status = tonic::Status::with_details(
            tonic::Code::Internal,
            RETRYABLE_DATAFUSION_ERROR_STATUS,
            details.into(),
        );

        let error = tonic_status_to_datafusion_error(status).unwrap();
        assert_eq!(RetryOutcome::try_from_err(&error), None);
        assert!(error.to_string().contains("missing retry target"));
    }
}
