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

pub fn datafusion_error_to_tonic_status(err: &DataFusionError) -> tonic::Status {
    let err = DataFusionErrorProto::from_datafusion_error(err);
    let err = err.encode_to_vec();
    let status = tonic::Status::with_details(tonic::Code::Internal, "DataFusionError", err.into());
    status
}

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
