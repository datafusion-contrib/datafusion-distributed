use crate::errors::io_error::IoErrorProto;
use datafusion::arrow::error::ArrowError;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowErrorProto {
    #[prost(oneof = "ArrowErrorInnerProto", tags = "1")]
    pub inner: Option<ArrowErrorInnerProto>,
    #[prost(string, optional, tag = "2")]
    pub ctx: Option<String>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum ArrowErrorInnerProto {
    #[prost(string, tag = "1")]
    NotYetImplemented(String),
    #[prost(string, tag = "2")]
    ExternalError(String),
    #[prost(string, tag = "3")]
    CastError(String),
    #[prost(string, tag = "4")]
    MemoryError(String),
    #[prost(string, tag = "5")]
    ParseError(String),
    #[prost(string, tag = "6")]
    SchemaError(String),
    #[prost(string, tag = "7")]
    ComputeError(String),
    #[prost(bool, tag = "8")]
    DivideByZero(bool),
    #[prost(string, tag = "9")]
    ArithmeticOverflow(String),
    #[prost(string, tag = "10")]
    CsvError(String),
    #[prost(string, tag = "11")]
    JsonError(String),
    #[prost(message, tag = "12")]
    IoError(IoErrorProto),
    #[prost(message, tag = "13")]
    IpcError(String),
    #[prost(message, tag = "14")]
    InvalidArgumentError(String),
    #[prost(message, tag = "15")]
    ParquetError(String),
    #[prost(message, tag = "16")]
    CDataInterface(String),
    #[prost(bool, tag = "17")]
    DictionaryKeyOverflowError(bool),
    #[prost(bool, tag = "18")]
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
