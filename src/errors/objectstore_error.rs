#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectStoreErrorProto {
    #[prost(oneof = "ObjectStoreErrorInnerProto", tags = "1")]
    pub inner: Option<ObjectStoreErrorInnerProto>,
}

#[derive(Clone, PartialEq, prost::Oneof)]
pub enum ObjectStoreErrorInnerProto {
    #[prost(message, tag = "1")]
    Generic(ObjectStoreGenericErrorProto),
    #[prost(message, tag = "2")]
    NotFound(ObjectStoreSourcePathErrorProto),
    #[prost(message, tag = "3")]
    InvalidPath(ObjectStoreSourceErrorProto),
    #[prost(message, tag = "4")]
    JoinError(ObjectStoreSourceErrorProto),
    #[prost(message, tag = "5")]
    NotSupported(ObjectStoreSourceErrorProto),
    #[prost(message, tag = "6")]
    AlreadyExists(ObjectStoreSourcePathErrorProto),
    #[prost(message, tag = "7")]
    Precondition(ObjectStoreSourcePathErrorProto),
    #[prost(message, tag = "8")]
    NotModified(ObjectStoreSourcePathErrorProto),
    #[prost(message, tag = "9")]
    NotImplemented(bool),
    #[prost(message, tag = "10")]
    PermissionDenied(ObjectStoreSourcePathErrorProto),
    #[prost(message, tag = "11")]
    Unauthenticated(ObjectStoreSourcePathErrorProto),
    #[prost(message, tag = "12")]
    UnknownConfigurationKey(ObjectStoreConfigurationKeyErrorProto),
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectStoreGenericErrorProto {
    #[prost(string, tag = "1")]
    store: String,
    #[prost(string, tag = "2")]
    source: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectStoreSourceErrorProto {
    #[prost(string, tag = "1")]
    source: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectStoreSourcePathErrorProto {
    #[prost(string, tag = "1")]
    path: String,
    #[prost(string, tag = "2")]
    source: String,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ObjectStoreConfigurationKeyErrorProto {
    #[prost(string, tag = "1")]
    key: String,
    #[prost(string, tag = "2")]
    store: String,
}

impl ObjectStoreErrorProto {
    pub fn from_object_store_error(err: &object_store::Error) -> Self {
        match err {
            object_store::Error::Generic { store, source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::Generic(
                    ObjectStoreGenericErrorProto {
                        store: store.to_string(),
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::NotFound { path, source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::NotFound(
                    ObjectStoreSourcePathErrorProto {
                        path: path.to_string(),
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::InvalidPath { source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::InvalidPath(
                    ObjectStoreSourceErrorProto {
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::JoinError { source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::JoinError(
                    ObjectStoreSourceErrorProto {
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::NotSupported { source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::NotSupported(
                    ObjectStoreSourceErrorProto {
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::AlreadyExists { path, source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::AlreadyExists(
                    ObjectStoreSourcePathErrorProto {
                        path: path.to_string(),
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::Precondition { path, source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::Precondition(
                    ObjectStoreSourcePathErrorProto {
                        path: path.to_string(),
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::NotModified { path, source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::NotModified(
                    ObjectStoreSourcePathErrorProto {
                        path: path.to_string(),
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::NotImplemented => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::NotImplemented(true)),
            },
            object_store::Error::PermissionDenied { path, source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::PermissionDenied(
                    ObjectStoreSourcePathErrorProto {
                        path: path.to_string(),
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::Unauthenticated { path, source } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::Unauthenticated(
                    ObjectStoreSourcePathErrorProto {
                        path: path.to_string(),
                        source: source.to_string(),
                    },
                )),
            },
            object_store::Error::UnknownConfigurationKey { key, store } => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::UnknownConfigurationKey(
                    ObjectStoreConfigurationKeyErrorProto {
                        key: key.to_string(),
                        store: store.to_string(),
                    },
                )),
            },
            _ => ObjectStoreErrorProto {
                inner: Some(ObjectStoreErrorInnerProto::Generic(
                    ObjectStoreGenericErrorProto {
                        store: "Could not serialize ObjectStore error to proto".to_string(),
                        source: "Could not serialize ObjectStore error to proto".to_string(),
                    },
                )),
            },
        }
    }

    pub fn to_object_store_error(&self) -> object_store::Error {
        let Some(ref inner) = self.inner else {
            return object_store::Error::Generic {
                store: "unknown",
                source: "Could not deserialize ObjectStore error from proto".into(),
            };
        };

        match inner {
            ObjectStoreErrorInnerProto::Generic(msg) => object_store::Error::Generic {
                store: parse_store(&msg.store),
                source: msg.source.clone().into(),
            },
            ObjectStoreErrorInnerProto::NotFound(msg) => object_store::Error::NotFound {
                path: msg.path.clone(),
                source: msg.source.clone().into(),
            },
            ObjectStoreErrorInnerProto::InvalidPath(msg) => object_store::Error::Generic {
                // InvalidPath contains a full nested error, and my time has been wasted too
                // much with this already
                store: "unknown",
                source: format!("InvalidPath: {}", msg.source).into(),
            },
            ObjectStoreErrorInnerProto::JoinError(msg) => object_store::Error::Generic {
                // tokio::task::JoinError does not allow to be built
                store: "unknown",
                source: format!("JoinError: {}", msg.source).into(),
            },
            ObjectStoreErrorInnerProto::NotSupported(msg) => object_store::Error::NotSupported {
                source: msg.source.clone().into(),
            },
            ObjectStoreErrorInnerProto::AlreadyExists(msg) => object_store::Error::AlreadyExists {
                path: msg.path.clone(),
                source: msg.source.clone().into(),
            },
            ObjectStoreErrorInnerProto::Precondition(msg) => object_store::Error::Precondition {
                path: msg.path.clone(),
                source: msg.source.clone().into(),
            },
            ObjectStoreErrorInnerProto::NotModified(msg) => object_store::Error::NotModified {
                path: msg.path.clone(),
                source: msg.source.clone().into(),
            },
            ObjectStoreErrorInnerProto::NotImplemented(_) => object_store::Error::NotImplemented,
            ObjectStoreErrorInnerProto::PermissionDenied(msg) => {
                object_store::Error::PermissionDenied {
                    path: msg.path.clone(),
                    source: msg.source.clone().into(),
                }
            }
            ObjectStoreErrorInnerProto::Unauthenticated(msg) => {
                object_store::Error::Unauthenticated {
                    path: msg.path.clone(),
                    source: msg.source.clone().into(),
                }
            }
            ObjectStoreErrorInnerProto::UnknownConfigurationKey(msg) => {
                object_store::Error::UnknownConfigurationKey {
                    key: msg.key.clone(),
                    store: parse_store(&msg.store),
                }
            }
        }
    }
}

fn parse_store(store: &str) -> &'static str {
    // some appearances while looking at
    // https://github.com/search?q=repo%3Aapache%2Farrow-rs-object-store%20store%3A%20%22&type=code
    match store {
        "GCS" => "GCS",
        "MicrosoftAzure" => "MicrosoftAzure",
        "S3" => "S3",
        "Config" => "Config",
        "ChunkedStore" => "ChunkedStore",
        "LineDelimiter" => "LineDelimiter",
        "HTTP client" => "HTTP client",
        "HTTP" => "HTTP",
        "URL" => "URL",
        "InMemory" => "InMemory",
        "ObjectStoreRegistry" => "ObjectStoreRegistry",
        "Parts" => "Parts",
        "LocalFileSystem" => "LocalFileSystem",
        _ => "Unknown",
    }
}
