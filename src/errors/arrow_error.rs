use crate::errors::io_error::IoErrorProto;
use datafusion::arrow::error::ArrowError;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowErrorProto {
    #[prost(string, optional, tag = "1")]
    pub ctx: Option<String>,
    #[prost(
        oneof = "ArrowErrorInnerProto",
        tags = "2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19"
    )]
    pub inner: Option<ArrowErrorInnerProto>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum ArrowErrorInnerProto {
    #[prost(string, tag = "2")]
    NotYetImplemented(String),
    #[prost(string, tag = "3")]
    ExternalError(String),
    #[prost(string, tag = "4")]
    CastError(String),
    #[prost(string, tag = "5")]
    MemoryError(String),
    #[prost(string, tag = "6")]
    ParseError(String),
    #[prost(string, tag = "7")]
    SchemaError(String),
    #[prost(string, tag = "8")]
    ComputeError(String),
    #[prost(bool, tag = "9")]
    DivideByZero(bool),
    #[prost(string, tag = "10")]
    ArithmeticOverflow(String),
    #[prost(string, tag = "11")]
    CsvError(String),
    #[prost(string, tag = "12")]
    JsonError(String),
    #[prost(message, tag = "13")]
    IoError(IoErrorProto),
    #[prost(message, tag = "14")]
    IpcError(String),
    #[prost(message, tag = "15")]
    InvalidArgumentError(String),
    #[prost(message, tag = "16")]
    ParquetError(String),
    #[prost(message, tag = "17")]
    CDataInterface(String),
    #[prost(bool, tag = "18")]
    DictionaryKeyOverflowError(bool),
    #[prost(bool, tag = "19")]
    RunEndIndexOverflowError(bool),
}

impl ArrowErrorProto {
    pub fn from_arrow_error(err: &ArrowError, ctx: Option<&String>) -> Self {
        match err {
            ArrowError::NotYetImplemented(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::NotYetImplemented(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::ExternalError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::ExternalError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::CastError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::CastError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::MemoryError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::MemoryError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::ParseError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::ParseError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::SchemaError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::SchemaError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::ComputeError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::ComputeError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::DivideByZero => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::DivideByZero(true)),
                ctx: ctx.cloned(),
            },
            ArrowError::ArithmeticOverflow(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::ArithmeticOverflow(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::CsvError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::CsvError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::JsonError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::JsonError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::IoError(msg, err) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::IoError(IoErrorProto::from_io_error(
                    msg, err,
                ))),
                ctx: ctx.cloned(),
            },
            ArrowError::IpcError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::IpcError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::InvalidArgumentError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::InvalidArgumentError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::ParquetError(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::ParquetError(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::CDataInterface(msg) => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::CDataInterface(msg.to_string())),
                ctx: ctx.cloned(),
            },
            ArrowError::DictionaryKeyOverflowError => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::DictionaryKeyOverflowError(true)),
                ctx: ctx.cloned(),
            },
            ArrowError::RunEndIndexOverflowError => ArrowErrorProto {
                inner: Some(ArrowErrorInnerProto::RunEndIndexOverflowError(true)),
                ctx: ctx.cloned(),
            },
        }
    }

    pub fn to_arrow_error(&self) -> (ArrowError, Option<String>) {
        let Some(ref inner) = self.inner else {
            return (
                ArrowError::ExternalError(Box::from("Malformed protobuf message".to_string())),
                None,
            );
        };
        let err = match inner {
            ArrowErrorInnerProto::NotYetImplemented(msg) => {
                ArrowError::NotYetImplemented(msg.to_string())
            }
            ArrowErrorInnerProto::ExternalError(msg) => {
                ArrowError::ExternalError(Box::from(msg.to_string()))
            }
            ArrowErrorInnerProto::CastError(msg) => ArrowError::CastError(msg.to_string()),
            ArrowErrorInnerProto::MemoryError(msg) => ArrowError::MemoryError(msg.to_string()),
            ArrowErrorInnerProto::ParseError(msg) => ArrowError::ParseError(msg.to_string()),
            ArrowErrorInnerProto::SchemaError(msg) => ArrowError::SchemaError(msg.to_string()),
            ArrowErrorInnerProto::ComputeError(msg) => ArrowError::ComputeError(msg.to_string()),
            ArrowErrorInnerProto::DivideByZero(_) => ArrowError::DivideByZero,
            ArrowErrorInnerProto::ArithmeticOverflow(msg) => {
                ArrowError::ArithmeticOverflow(msg.to_string())
            }
            ArrowErrorInnerProto::CsvError(msg) => ArrowError::CsvError(msg.to_string()),
            ArrowErrorInnerProto::JsonError(msg) => ArrowError::JsonError(msg.to_string()),
            ArrowErrorInnerProto::IoError(msg) => {
                let (msg, err) = msg.to_io_error();
                ArrowError::IoError(err, msg)
            }
            ArrowErrorInnerProto::IpcError(msg) => ArrowError::IpcError(msg.to_string()),
            ArrowErrorInnerProto::InvalidArgumentError(msg) => {
                ArrowError::InvalidArgumentError(msg.to_string())
            }
            ArrowErrorInnerProto::ParquetError(msg) => ArrowError::ParquetError(msg.to_string()),
            ArrowErrorInnerProto::CDataInterface(msg) => {
                ArrowError::CDataInterface(msg.to_string())
            }
            ArrowErrorInnerProto::DictionaryKeyOverflowError(_) => {
                ArrowError::DictionaryKeyOverflowError
            }
            ArrowErrorInnerProto::RunEndIndexOverflowError(_) => {
                ArrowError::RunEndIndexOverflowError
            }
        };
        (err, self.ctx.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost::Message;
    use std::io::{Error as IoError, ErrorKind};

    #[test]
    fn test_arrow_error_roundtrip() {
        let test_cases = vec![
            ArrowError::NotYetImplemented("test not implemented".to_string()),
            ArrowError::ExternalError(Box::new(std::io::Error::new(
                ErrorKind::Other,
                "external error",
            ))),
            ArrowError::CastError("cast error".to_string()),
            ArrowError::MemoryError("memory error".to_string()),
            ArrowError::ParseError("parse error".to_string()),
            ArrowError::SchemaError("schema error".to_string()),
            ArrowError::ComputeError("compute error".to_string()),
            ArrowError::DivideByZero,
            ArrowError::ArithmeticOverflow("overflow".to_string()),
            ArrowError::CsvError("csv error".to_string()),
            ArrowError::JsonError("json error".to_string()),
            ArrowError::IoError(
                "io message".to_string(),
                IoError::new(ErrorKind::NotFound, "file not found"),
            ),
            ArrowError::IpcError("ipc error".to_string()),
            ArrowError::InvalidArgumentError("invalid arg".to_string()),
            ArrowError::ParquetError("parquet error".to_string()),
            ArrowError::CDataInterface("cdata error".to_string()),
            ArrowError::DictionaryKeyOverflowError,
            ArrowError::RunEndIndexOverflowError,
        ];

        for original_error in test_cases {
            let proto = ArrowErrorProto::from_arrow_error(
                &original_error,
                Some(&"test context".to_string()),
            );
            let proto = ArrowErrorProto::decode(proto.encode_to_vec().as_ref()).unwrap();
            let (recovered_error, recovered_ctx) = proto.to_arrow_error();

            if original_error.to_string() != recovered_error.to_string() {
                println!("original error: {}", original_error.to_string());
                println!("recovered error: {}", recovered_error.to_string());
            }

            assert_eq!(original_error.to_string(), recovered_error.to_string());
            assert_eq!(recovered_ctx, Some("test context".to_string()));

            let proto_no_ctx = ArrowErrorProto::from_arrow_error(&original_error, None);
            let proto_no_ctx =
                ArrowErrorProto::decode(proto_no_ctx.encode_to_vec().as_ref()).unwrap();
            let (recovered_error_no_ctx, recovered_ctx_no_ctx) = proto_no_ctx.to_arrow_error();

            assert_eq!(
                original_error.to_string(),
                recovered_error_no_ctx.to_string()
            );
            assert_eq!(recovered_ctx_no_ctx, None);
        }
    }

    #[test]
    fn test_malformed_protobuf_message() {
        let malformed_proto = ArrowErrorProto {
            inner: None,
            ctx: None,
        };
        let (recovered_error, _) = malformed_proto.to_arrow_error();
        assert!(matches!(recovered_error, ArrowError::ExternalError(_)));
    }
}
