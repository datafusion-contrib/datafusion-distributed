use crate::errors::datafusion_error::DataFusionErrorProto;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use prost::Message;

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
pub fn datafusion_error_to_tonic_status(err: &DataFusionError) -> tonic::Status {
    let err = DataFusionErrorProto::from_datafusion_error(err);
    let err = err.encode_to_vec();
    let status = tonic::Status::with_details(tonic::Code::Internal, "DataFusionError", err.into());
    status
}

/// Decodes a [DataFusionError] from a [tonic::Status] error. If the provided [tonic::Status]
/// error was produced with [datafusion_error_to_tonic_status], this function will be able to
/// recover it even across a network boundary.
///
/// The provided [tonic::Status] error might also be something else, like an actual network
/// failure. This function returns `None` for those cases.
pub fn tonic_status_to_datafusion_error(status: &tonic::Status) -> Option<DataFusionError> {
    if status.code() != tonic::Code::Internal {
        return None;
    }

    if status.message() != "DataFusionError" {
        return None;
    }

    match DataFusionErrorProto::decode(status.details()) {
        Ok(err_proto) => {
            dbg!(&err_proto);
            Some(err_proto.to_datafusion_err())
        }
        Err(err) => Some(internal_datafusion_err!(
            "Cannot decode DataFusionError: {err}"
        )),
    }
}
