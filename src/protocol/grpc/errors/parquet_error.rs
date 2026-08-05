// Only the conversions touch `ParquetError`; the proto types below are always compiled so
// that the wire format does not depend on which features an endpoint was built with.
#[cfg(feature = "parquet")]
use datafusion::parquet::errors::ParquetError;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParquetErrorProto {
    #[prost(oneof = "ParquetErrorInnerProto", tags = "1,2,3,4,5,6,7")]
    pub inner: Option<ParquetErrorInnerProto>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum ParquetErrorInnerProto {
    #[prost(message, tag = "1")]
    General(String),
    #[prost(message, tag = "2")]
    NYI(String),
    #[prost(message, tag = "3")]
    EOF(String),
    #[prost(message, tag = "4")]
    ArrowError(String),
    #[prost(message, tag = "5")]
    IndexOutOfBound(IndexOutOfBoundProto),
    #[prost(message, tag = "6")]
    External(String),
    #[prost(uint64, tag = "7")]
    NeedMoreData(u64),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct IndexOutOfBoundProto {
    #[prost(uint64, tag = "1")]
    a: u64,
    #[prost(uint64, tag = "2")]
    b: u64,
}

/// Mirrors `ParquetError`'s `Display` without needing the type itself, so that endpoints built
/// without the `parquet` feature can still surface the message an error carried over the wire.
impl std::fmt::Display for ParquetErrorProto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Some(ref inner) = self.inner else {
            return write!(f, "External: Malformed protobuf message");
        };

        match inner {
            ParquetErrorInnerProto::General(msg) => write!(f, "Parquet error: {msg}"),
            ParquetErrorInnerProto::NYI(msg) => write!(f, "NYI: {msg}"),
            ParquetErrorInnerProto::EOF(msg) => write!(f, "EOF: {msg}"),
            ParquetErrorInnerProto::ArrowError(msg) => write!(f, "Arrow: {msg}"),
            ParquetErrorInnerProto::IndexOutOfBound(IndexOutOfBoundProto { a, b }) => {
                write!(f, "Index {a} out of bound: {b}")
            }
            ParquetErrorInnerProto::External(msg) => write!(f, "External: {msg}"),
            ParquetErrorInnerProto::NeedMoreData(n) => write!(f, "NeedMoreData: {n}"),
        }
    }
}

#[cfg(feature = "parquet")]
impl ParquetErrorProto {
    pub fn from_parquet_error(err: &ParquetError) -> Self {
        match err {
            ParquetError::General(msg) => ParquetErrorProto {
                inner: Some(ParquetErrorInnerProto::General(msg.to_string())),
            },
            ParquetError::NYI(msg) => ParquetErrorProto {
                inner: Some(ParquetErrorInnerProto::NYI(msg.to_string())),
            },
            ParquetError::EOF(msg) => ParquetErrorProto {
                inner: Some(ParquetErrorInnerProto::EOF(msg.to_string())),
            },
            ParquetError::ArrowError(msg) => ParquetErrorProto {
                inner: Some(ParquetErrorInnerProto::ArrowError(msg.to_string())),
            },
            ParquetError::IndexOutOfBound(a, b) => ParquetErrorProto {
                inner: Some(ParquetErrorInnerProto::IndexOutOfBound(
                    IndexOutOfBoundProto {
                        a: *a as u64,
                        b: *b as u64,
                    },
                )),
            },
            ParquetError::External(err) => ParquetErrorProto {
                inner: Some(ParquetErrorInnerProto::External(err.to_string())),
            },
            ParquetError::NeedMoreData(a) => ParquetErrorProto {
                inner: Some(ParquetErrorInnerProto::NeedMoreData(*a as u64)),
            },
            _ => ParquetErrorProto {
                inner: Some(ParquetErrorInnerProto::General(
                    "ParquetError could not be serialized into protobuf".to_string(),
                )),
            },
        }
    }

    pub fn to_parquet_error(&self) -> ParquetError {
        let Some(ref inner) = self.inner else {
            return ParquetError::External(Box::from("Malformed protobuf message".to_string()));
        };

        match inner {
            ParquetErrorInnerProto::General(msg) => ParquetError::General(msg.to_string()),
            ParquetErrorInnerProto::NYI(msg) => ParquetError::NYI(msg.to_string()),
            ParquetErrorInnerProto::EOF(msg) => ParquetError::EOF(msg.to_string()),
            ParquetErrorInnerProto::ArrowError(msg) => ParquetError::ArrowError(msg.to_string()),
            ParquetErrorInnerProto::IndexOutOfBound(IndexOutOfBoundProto { a, b }) => {
                ParquetError::IndexOutOfBound(*a as usize, *b as usize)
            }
            ParquetErrorInnerProto::External(msg) => {
                ParquetError::External(Box::from(msg.to_string()))
            }
            ParquetErrorInnerProto::NeedMoreData(n) => ParquetError::NeedMoreData(*n as usize),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::parquet::errors::ParquetError;
    use prost::Message;

    #[test]
    fn test_parquet_error_roundtrip() {
        let test_cases = vec![
            ParquetError::General("general error".to_string()),
            ParquetError::NYI("not yet implemented".to_string()),
            ParquetError::EOF("end of file".to_string()),
            ParquetError::ArrowError("arrow error".to_string()),
            ParquetError::IndexOutOfBound(42, 100),
            ParquetError::External(Box::new(std::io::Error::other("external error"))),
            ParquetError::NeedMoreData(1024),
        ];

        for original_error in test_cases {
            let proto = ParquetErrorProto::from_parquet_error(&original_error);
            let proto = ParquetErrorProto::decode(proto.encode_to_vec().as_ref()).unwrap();
            let recovered_error = proto.to_parquet_error();

            assert_eq!(original_error.to_string(), recovered_error.to_string());
            // The proto's own `Display` is what endpoints built without `parquet` fall back to,
            // so it must stay in sync with `ParquetError`'s.
            assert_eq!(original_error.to_string(), proto.to_string());
        }
    }

    #[test]
    fn test_malformed_protobuf_message() {
        let malformed_proto = ParquetErrorProto { inner: None };
        let recovered_error = malformed_proto.to_parquet_error();
        assert!(matches!(recovered_error, ParquetError::External(_)));
        assert_eq!(recovered_error.to_string(), malformed_proto.to_string());
    }
}
