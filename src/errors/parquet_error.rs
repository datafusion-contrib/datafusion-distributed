use datafusion::parquet::errors::ParquetError;

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ParquetErrorProto {
    #[prost(oneof = "ParquetErrorInnerProto", tags = "1")]
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
