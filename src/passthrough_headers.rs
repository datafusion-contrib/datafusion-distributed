use crate::config_extension_ext::FLIGHT_METADATA_CONFIG_PREFIX;
use datafusion::common::DataFusionError;
use datafusion::prelude::SessionConfig;
use http::HeaderMap;
use std::sync::Arc;

/// Stores arbitrary HTTP headers that should be forwarded unchanged to worker nodes.
#[derive(Clone, Default)]
pub(crate) struct PassthroughHeaders(HeaderMap);

/// Sets passthrough headers on a SessionConfig as an extension.
///
/// Returns an error if any header name starts with the reserved prefix
/// `x-datafusion-distributed-config-`, which is used internally for config extensions.
pub(crate) fn set_passthrough_headers(
    cfg: &mut SessionConfig,
    headers: HeaderMap,
) -> Result<(), DataFusionError> {
    // Validate that no headers use the reserved internal prefix
    for name in headers.keys() {
        if name.as_str().starts_with(FLIGHT_METADATA_CONFIG_PREFIX) {
            return Err(DataFusionError::Configuration(format!(
                "Passthrough header '{name}' uses reserved prefix '{FLIGHT_METADATA_CONFIG_PREFIX}'. \
                 This prefix is reserved for internal config extension propagation."
            )));
        }
    }

    cfg.set_extension(Arc::new(PassthroughHeaders(headers)));
    Ok(())
}

/// Gets passthrough headers from a SessionConfig extension.
/// Returns an empty HeaderMap if none are set.
pub(crate) fn get_passthrough_headers(cfg: &SessionConfig) -> HeaderMap {
    cfg.get_extension::<PassthroughHeaders>()
        .map(|h| h.0.clone())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderName, HeaderValue};

    #[test]
    fn test_set_and_get_passthrough_headers() {
        let mut config = SessionConfig::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-custom-header"),
            HeaderValue::from_static("test-value"),
        );
        headers.insert(
            HeaderName::from_static("x-another-header"),
            HeaderValue::from_static("another-value"),
        );

        set_passthrough_headers(&mut config, headers.clone()).unwrap();

        let retrieved = get_passthrough_headers(&config);
        assert_eq!(retrieved.len(), 2);
        assert_eq!(retrieved.get("x-custom-header").unwrap(), "test-value");
        assert_eq!(retrieved.get("x-another-header").unwrap(), "another-value");
    }

    #[test]
    fn test_get_passthrough_headers_empty() {
        let config = SessionConfig::new();
        let retrieved = get_passthrough_headers(&config);
        assert!(retrieved.is_empty());
    }

    #[test]
    fn test_overwrite_passthrough_headers() {
        let mut config = SessionConfig::new();

        let mut headers1 = HeaderMap::new();
        headers1.insert(
            HeaderName::from_static("x-first"),
            HeaderValue::from_static("first-value"),
        );
        set_passthrough_headers(&mut config, headers1).unwrap();

        let mut headers2 = HeaderMap::new();
        headers2.insert(
            HeaderName::from_static("x-second"),
            HeaderValue::from_static("second-value"),
        );
        set_passthrough_headers(&mut config, headers2).unwrap();

        let retrieved = get_passthrough_headers(&config);
        assert_eq!(retrieved.len(), 1);
        assert!(retrieved.get("x-first").is_none());
        assert_eq!(retrieved.get("x-second").unwrap(), "second-value");
    }

    #[test]
    fn test_rejects_reserved_prefix() {
        let mut config = SessionConfig::new();
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-datafusion-distributed-config-custom.foo"),
            HeaderValue::from_static("should-fail"),
        );

        let result = set_passthrough_headers(&mut config, headers);
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert!(err.to_string().contains("reserved prefix"));
    }

    #[test]
    fn test_accepts_similar_but_different_prefix() {
        let mut config = SessionConfig::new();
        let mut headers = HeaderMap::new();
        // This is similar but doesn't match the exact prefix
        headers.insert(
            HeaderName::from_static("x-datafusion-distributed-other"),
            HeaderValue::from_static("should-succeed"),
        );

        let result = set_passthrough_headers(&mut config, headers);
        assert!(result.is_ok());
    }
}
