use crate::errors::arrow_error::ArrowErrorProto;
use crate::errors::io_error::IoErrorProto;
use crate::errors::objectstore_error::ObjectStoreErrorProto;
use crate::errors::parquet_error::ParquetErrorProto;
use crate::errors::parser_error::ParserErrorProto;
use crate::errors::schema_error::SchemaErrorProto;
use datafusion::common::{DataFusionError, Diagnostic};
use datafusion::logical_expr::sqlparser::parser::ParserError;
use std::error::Error;
use std::sync::Arc;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataFusionErrorProto {
    #[prost(
        oneof = "DataFusionErrorInnerProto",
        tags = "1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19"
    )]
    pub inner: Option<DataFusionErrorInnerProto>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum DataFusionErrorInnerProto {
    #[prost(message, tag = "1")]
    ArrowError(ArrowErrorProto),
    #[prost(message, tag = "2")]
    ParquetError(ParquetErrorProto),
    #[prost(message, tag = "3")]
    ObjectStoreError(ObjectStoreErrorProto),
    #[prost(message, tag = "4")]
    IoError(IoErrorProto),
    #[prost(message, tag = "5")]
    SQL(DataFusionSqlErrorProto),
    #[prost(string, tag = "6")]
    NotImplemented(String),
    #[prost(string, tag = "7")]
    Internal(String),
    #[prost(string, tag = "8")]
    Plan(String),
    #[prost(string, tag = "9")]
    Configuration(String),
    #[prost(message, tag = "10")]
    Schema(SchemaErrorProto),
    #[prost(string, tag = "11")]
    Execution(String),
    #[prost(string, tag = "12")]
    ExecutionJoin(String),
    #[prost(string, tag = "13")]
    ResourceExhausted(String),
    #[prost(string, tag = "14")]
    External(String),
    #[prost(message, tag = "15")]
    Context(DataFusionContextErrorProto),
    #[prost(string, tag = "16")]
    Substrait(String),
    #[prost(message, boxed, tag = "17")]
    Diagnostic(Box<DataFusionErrorProto>),
    #[prost(message, tag = "18")]
    Collection(DataFusionCollectionErrorProto),
    #[prost(message, boxed, tag = "19")]
    Shared(Box<DataFusionErrorProto>),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataFusionSqlErrorProto {
    #[prost(message, tag = "1")]
    err: Option<ParserErrorProto>,
    #[prost(string, optional, tag = "2")]
    backtrace: Option<String>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataFusionContextErrorProto {
    #[prost(message, boxed, tag = "1")]
    err: Option<Box<DataFusionErrorProto>>,
    #[prost(string, tag = "2")]
    ctx: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DataFusionCollectionErrorProto {
    #[prost(message, repeated, boxed, tag = "1")]
    errs: Vec<Box<DataFusionErrorProto>>,
}

impl DataFusionErrorProto {
    pub fn from_datafusion_error(err: &DataFusionError) -> Self {
        match err {
            DataFusionError::ArrowError(err, msg) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::ArrowError(
                    ArrowErrorProto::from_arrow_error(err, msg.as_ref()),
                )),
            },
            DataFusionError::ParquetError(err) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::ParquetError(
                    ParquetErrorProto::from_parquet_error(err),
                )),
            },
            DataFusionError::ObjectStore(err) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::ObjectStoreError(
                    ObjectStoreErrorProto::from_object_store_error(err),
                )),
            },
            DataFusionError::IoError(err) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::IoError(
                    IoErrorProto::from_io_error("", err),
                )),
            },
            DataFusionError::SQL(err, msg) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::SQL(DataFusionSqlErrorProto {
                    err: Some(ParserErrorProto::from_parser_error(err)),
                    backtrace: msg.clone(),
                })),
            },
            DataFusionError::NotImplemented(msg) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::NotImplemented(msg.clone())),
            },
            DataFusionError::Internal(msg) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Internal(msg.clone())),
            },
            DataFusionError::Plan(msg) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Plan(msg.clone())),
            },
            DataFusionError::Configuration(msg) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Configuration(msg.clone())),
            },
            DataFusionError::SchemaError(err, backtrace) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Schema(
                    SchemaErrorProto::from_schema_error(err, backtrace.as_ref().as_ref()),
                )),
            },
            DataFusionError::Execution(msg) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Execution(msg.clone())),
            },
            DataFusionError::ExecutionJoin(err) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::ExecutionJoin(err.to_string())),
            },
            DataFusionError::ResourcesExhausted(msg) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::ResourceExhausted(msg.clone())),
            },
            DataFusionError::External(err) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::External(err.to_string())),
            },
            DataFusionError::Context(ctx, err) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Context(
                    DataFusionContextErrorProto {
                        ctx: ctx.to_string(),
                        err: Some(Box::new(DataFusionErrorProto::from_datafusion_error(err))),
                    },
                )),
            },
            DataFusionError::Substrait(err) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Substrait(err.to_string())),
            },
            // Diagnostics are trimmed out
            DataFusionError::Diagnostic(_, err) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Diagnostic(Box::new(
                    DataFusionErrorProto::from_datafusion_error(err),
                ))),
            },
            DataFusionError::Collection(errs) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Collection(
                    DataFusionCollectionErrorProto {
                        errs: errs
                            .iter()
                            .map(DataFusionErrorProto::from_datafusion_error)
                            .map(Box::new)
                            .collect(),
                    },
                )),
            },
            DataFusionError::Shared(err) => DataFusionErrorProto {
                inner: Some(DataFusionErrorInnerProto::Shared(Box::new(
                    DataFusionErrorProto::from_datafusion_error(err.as_ref()),
                ))),
            },
        }
    }

    pub fn to_datafusion_err(&self) -> DataFusionError {
        let Some(ref inner) = self.inner else {
            return DataFusionError::Internal("DataFusionError proto message is empty".to_string());
        };

        match inner {
            DataFusionErrorInnerProto::ArrowError(err) => {
                let (err, ctx) = err.to_arrow_error();
                DataFusionError::ArrowError(err, ctx)
            }
            DataFusionErrorInnerProto::ParquetError(err) => {
                DataFusionError::ParquetError(err.to_parquet_error())
            }
            DataFusionErrorInnerProto::ObjectStoreError(err) => {
                DataFusionError::ObjectStore(err.to_object_store_error())
            }
            DataFusionErrorInnerProto::IoError(err) => {
                let (err, _) = err.to_io_error();
                DataFusionError::IoError(err)
            }
            DataFusionErrorInnerProto::SQL(err) => {
                let backtrace = err.backtrace.clone();
                let err = err.err.as_ref().map(|err| err.to_parser_error());
                let err = err.unwrap_or(ParserError::ParserError("".to_string()));
                DataFusionError::SQL(err, backtrace)
            }
            DataFusionErrorInnerProto::NotImplemented(msg) => {
                DataFusionError::NotImplemented(msg.clone())
            }
            DataFusionErrorInnerProto::Internal(msg) => DataFusionError::Internal(msg.clone()),
            DataFusionErrorInnerProto::Plan(msg) => DataFusionError::Plan(msg.clone()),
            DataFusionErrorInnerProto::Configuration(msg) => {
                DataFusionError::Configuration(msg.clone())
            }
            DataFusionErrorInnerProto::Schema(err) => {
                let (err, backtrace) = err.to_schema_error();
                DataFusionError::SchemaError(err, Box::new(backtrace))
            }
            DataFusionErrorInnerProto::Execution(msg) => DataFusionError::Execution(msg.clone()),
            // We cannot build JoinErrors ourselves, so instead we map it to internal.
            DataFusionErrorInnerProto::ExecutionJoin(msg) => DataFusionError::Internal(msg.clone()),
            DataFusionErrorInnerProto::ResourceExhausted(msg) => {
                DataFusionError::ResourcesExhausted(msg.clone())
            }
            DataFusionErrorInnerProto::External(generic) => {
                DataFusionError::External(Box::new(DistributedDataFusionGenericError {
                    message: generic.clone(),
                }))
            }
            DataFusionErrorInnerProto::Context(err) => DataFusionError::Context(
                err.ctx.clone(),
                Box::new(err.err.as_ref().map(|v| v.to_datafusion_err()).unwrap_or(
                    DataFusionError::Internal(
                        "Missing DataFusionError protobuf message".to_string(),
                    ),
                )),
            ),
            DataFusionErrorInnerProto::Substrait(msg) => DataFusionError::Substrait(msg.clone()),
            DataFusionErrorInnerProto::Diagnostic(err) => {
                DataFusionError::Diagnostic(
                    // We lose diagnostic information because we are not encoding it.
                    Box::new(Diagnostic::new_error("", None)),
                    Box::new(err.to_datafusion_err()),
                )
            }
            DataFusionErrorInnerProto::Collection(errs) => DataFusionError::Collection(
                errs.errs
                    .iter()
                    .map(|err| err.to_datafusion_err())
                    .collect(),
            ),
            DataFusionErrorInnerProto::Shared(err) => {
                DataFusionError::Shared(Arc::new(err.to_datafusion_err()))
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct DistributedDataFusionGenericError {
    pub message: String,
}

impl std::fmt::Display for DistributedDataFusionGenericError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl Error for DistributedDataFusionGenericError {}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::error::ArrowError;
    use datafusion::common::{DataFusionError, SchemaError};
    use datafusion::logical_expr::sqlparser::parser::ParserError;
    use datafusion::parquet::errors::ParquetError;
    use object_store::Error as ObjectStoreError;
    use prost::Message;
    use std::io::{Error as IoError, ErrorKind};
    use std::sync::Arc;

    #[test]
    fn test_datafusion_error_roundtrip() {
        let test_cases = vec![
            DataFusionError::ArrowError(
                ArrowError::ComputeError("compute".to_string()),
                Some("arrow context".to_string()),
            ),
            DataFusionError::ParquetError(ParquetError::General("parquet error".to_string())),
            DataFusionError::ObjectStore(ObjectStoreError::NotFound {
                path: "test/path".to_string(),
                source: Box::new(std::io::Error::new(ErrorKind::NotFound, "not found")),
            }),
            DataFusionError::IoError(IoError::new(
                ErrorKind::PermissionDenied,
                "permission denied",
            )),
            DataFusionError::SQL(
                ParserError::ParserError("sql parse error".to_string()),
                Some("sql backtrace".to_string()),
            ),
            DataFusionError::NotImplemented("not implemented".to_string()),
            DataFusionError::Internal("internal error".to_string()),
            DataFusionError::Plan("plan error".to_string()),
            DataFusionError::Configuration("config error".to_string()),
            DataFusionError::SchemaError(
                SchemaError::AmbiguousReference {
                    field: datafusion::common::Column::new_unqualified("test_field"),
                },
                Box::new(None),
            ),
            DataFusionError::Execution("execution error".to_string()),
            DataFusionError::ResourcesExhausted("resources exhausted".to_string()),
            DataFusionError::External(Box::new(std::io::Error::new(ErrorKind::Other, "external"))),
            DataFusionError::Context(
                "context message".to_string(),
                Box::new(DataFusionError::Internal("nested".to_string())),
            ),
            DataFusionError::Substrait("substrait error".to_string()),
            DataFusionError::Collection(vec![
                DataFusionError::Internal("error 1".to_string()),
                DataFusionError::Internal("error 2".to_string()),
            ]),
            DataFusionError::Shared(Arc::new(DataFusionError::Internal(
                "shared error".to_string(),
            ))),
        ];

        for original_error in test_cases {
            let proto = DataFusionErrorProto::from_datafusion_error(&original_error);
            let proto = DataFusionErrorProto::decode(proto.encode_to_vec().as_ref()).unwrap();
            let recovered_error = proto.to_datafusion_err();

            assert_eq!(original_error.to_string(), recovered_error.to_string());
        }
    }

    #[test]
    fn test_malformed_protobuf_message() {
        let malformed_proto = DataFusionErrorProto { inner: None };
        let recovered_error = malformed_proto.to_datafusion_err();
        assert!(matches!(recovered_error, DataFusionError::Internal(_)));
    }

    #[test]
    fn test_nested_datafusion_errors() {
        let nested_error = DataFusionError::Context(
            "outer context".to_string(),
            Box::new(DataFusionError::Context(
                "inner context".to_string(),
                Box::new(DataFusionError::Internal("deepest error".to_string())),
            )),
        );

        let proto = DataFusionErrorProto::from_datafusion_error(&nested_error);
        let proto = DataFusionErrorProto::decode(proto.encode_to_vec().as_ref()).unwrap();
        let recovered_error = proto.to_datafusion_err();

        assert_eq!(nested_error.to_string(), recovered_error.to_string());
    }

    #[test]
    fn test_collection_errors() {
        let collection_error = DataFusionError::Collection(vec![
            DataFusionError::Internal("error 1".to_string()),
            DataFusionError::Plan("error 2".to_string()),
            DataFusionError::Execution("error 3".to_string()),
        ]);

        let proto = DataFusionErrorProto::from_datafusion_error(&collection_error);
        let proto = DataFusionErrorProto::decode(proto.encode_to_vec().as_ref()).unwrap();
        let recovered_error = proto.to_datafusion_err();

        assert_eq!(collection_error.to_string(), recovered_error.to_string());
    }

    #[test]
    fn test_sql_error_with_backtrace() {
        let sql_error = DataFusionError::SQL(
            ParserError::ParserError("syntax error".to_string()),
            Some("test backtrace".to_string()),
        );

        let proto = DataFusionErrorProto::from_datafusion_error(&sql_error);
        let proto = DataFusionErrorProto::decode(proto.encode_to_vec().as_ref()).unwrap();
        let recovered_error = proto.to_datafusion_err();

        if let DataFusionError::SQL(_, backtrace) = recovered_error {
            assert_eq!(backtrace, Some("test backtrace".to_string()));
        } else {
            panic!("Expected SQL error");
        }
    }

    #[test]
    fn test_distributed_generic_error() {
        let generic_error = DistributedDataFusionGenericError {
            message: "test message".to_string(),
        };

        assert_eq!(generic_error.to_string(), "test message");
        assert!(Error::source(&generic_error).is_none());
    }
}
