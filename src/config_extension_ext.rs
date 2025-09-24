use datafusion::common::{DataFusionError, internal_datafusion_err};
use datafusion::config::ConfigExtension;
use datafusion::execution::TaskContext;
use datafusion::prelude::SessionConfig;
use http::{HeaderMap, HeaderName};
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;

const FLIGHT_METADATA_CONFIG_PREFIX: &str = "x-datafusion-distributed-config-";

pub(crate) fn set_distributed_option_extension<T: ConfigExtension + Default>(
    cfg: &mut SessionConfig,
    t: T,
) -> Result<(), DataFusionError> {
    fn parse_err(err: impl Error) -> DataFusionError {
        DataFusionError::Internal(format!("Failed to add config extension: {err}"))
    }
    let mut meta = HeaderMap::new();

    for entry in t.entries() {
        if let Some(value) = entry.value {
            meta.insert(
                HeaderName::from_str(&format!(
                    "{}{}.{}",
                    FLIGHT_METADATA_CONFIG_PREFIX,
                    T::PREFIX,
                    entry.key
                ))
                .map_err(parse_err)?,
                value.parse().map_err(parse_err)?,
            );
        }
    }
    let flight_metadata = ContextGrpcMetadata(meta);
    match cfg.get_extension::<ContextGrpcMetadata>() {
        None => cfg.set_extension(Arc::new(flight_metadata)),
        Some(prev) => {
            let prev = prev.as_ref().clone();
            cfg.set_extension(Arc::new(prev.merge(flight_metadata)))
        }
    }
    cfg.options_mut().extensions.insert(t);
    Ok(())
}

pub(crate) fn set_distributed_option_extension_from_headers<T: ConfigExtension + Default>(
    cfg: &mut SessionConfig,
    headers: &HeaderMap,
) -> Result<(), DataFusionError> {
    let mut result = T::default();
    let mut found_some = false;
    for (k, v) in headers.iter() {
        let key = k.as_str().trim_start_matches(FLIGHT_METADATA_CONFIG_PREFIX);
        let prefix = format!("{}.", T::PREFIX);
        if key.starts_with(&prefix) {
            found_some = true;
            result.set(
                key.trim_start_matches(&prefix),
                v.to_str()
                    .map_err(|err| internal_datafusion_err!("Cannot parse header value: {err}"))?,
            )?;
        }
    }
    if !found_some {
        return Ok(());
    }
    cfg.options_mut().extensions.insert(result);
    Ok(())
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ContextGrpcMetadata(pub HeaderMap);

impl ContextGrpcMetadata {
    fn merge(mut self, other: Self) -> Self {
        for (k, v) in other.0.into_iter() {
            let Some(k) = k else { continue };
            self.0.insert(k, v);
        }
        self
    }

    pub fn headers_from_ctx(ctx: &Arc<TaskContext>) -> HeaderMap {
        ctx.session_config()
            .get_extension::<ContextGrpcMetadata>()
            .as_ref()
            .map(|v| v.as_ref().clone())
            .unwrap_or_default()
            .0
    }
}

#[cfg(test)]
mod tests {
    use crate::config_extension_ext::{
        ContextGrpcMetadata, set_distributed_option_extension,
        set_distributed_option_extension_from_headers,
    };
    use datafusion::common::extensions_options;
    use datafusion::config::ConfigExtension;
    use datafusion::prelude::SessionConfig;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use std::str::FromStr;

    #[test]
    fn test_propagation() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        let opt = CustomExtension {
            foo: "".to_string(),
            bar: 0,
            baz: false,
        };

        set_distributed_option_extension(&mut config, opt)?;
        let metadata = config.get_extension::<ContextGrpcMetadata>().unwrap();
        let mut new_config = SessionConfig::new();
        set_distributed_option_extension_from_headers::<CustomExtension>(
            &mut new_config,
            &metadata.0,
        )?;

        let opt = get_ext::<CustomExtension>(&config);
        let new_opt = get_ext::<CustomExtension>(&new_config);

        assert_eq!(new_opt.foo, opt.foo);
        assert_eq!(new_opt.bar, opt.bar);
        assert_eq!(new_opt.baz, opt.baz);

        Ok(())
    }

    #[test]
    fn test_add_extension_with_empty_values() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();
        let opt = CustomExtension::default();

        set_distributed_option_extension(&mut config, opt)?;

        let flight_metadata = config.get_extension::<ContextGrpcMetadata>();
        assert!(flight_metadata.is_some());

        let metadata = &flight_metadata.unwrap().0;
        assert!(metadata.contains_key("x-datafusion-distributed-config-custom.foo"));
        assert!(metadata.contains_key("x-datafusion-distributed-config-custom.bar"));
        assert!(metadata.contains_key("x-datafusion-distributed-config-custom.baz"));

        let get = |key: &str| metadata.get(key).unwrap().to_str().unwrap();
        assert_eq!(get("x-datafusion-distributed-config-custom.foo"), "");
        assert_eq!(get("x-datafusion-distributed-config-custom.bar"), "0");
        assert_eq!(get("x-datafusion-distributed-config-custom.baz"), "false");

        Ok(())
    }

    #[test]
    fn test_new_extension_overwrites_previous() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        let opt1 = CustomExtension {
            foo: "first".to_string(),
            ..Default::default()
        };
        set_distributed_option_extension(&mut config, opt1)?;

        let opt2 = CustomExtension {
            bar: 42,
            ..Default::default()
        };
        set_distributed_option_extension(&mut config, opt2)?;

        let flight_metadata = config.get_extension::<ContextGrpcMetadata>().unwrap();
        let metadata = &flight_metadata.0;

        let get = |key: &str| metadata.get(key).unwrap().to_str().unwrap();
        assert_eq!(get("x-datafusion-distributed-config-custom.foo"), "");
        assert_eq!(get("x-datafusion-distributed-config-custom.bar"), "42");
        assert_eq!(get("x-datafusion-distributed-config-custom.baz"), "false");

        Ok(())
    }

    #[test]
    fn test_propagate_no_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        set_distributed_option_extension_from_headers::<CustomExtension>(
            &mut config,
            &Default::default(),
        )?;

        let extension = config.options().extensions.get::<CustomExtension>();
        assert!(extension.is_none());

        Ok(())
    }

    #[test]
    fn test_propagate_no_matching_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();
        let mut header_map = HeaderMap::new();
        header_map.insert(
            HeaderName::from_str("x-datafusion-distributed-config-other.setting").unwrap(),
            HeaderValue::from_str("value").unwrap(),
        );

        set_distributed_option_extension_from_headers::<CustomExtension>(&mut config, &header_map)?;

        let extension = config.options().extensions.get::<CustomExtension>();
        assert!(extension.is_none());

        Ok(())
    }

    #[test]
    fn test_multiple_extensions_different_prefixes() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        let custom_opt = CustomExtension {
            foo: "custom_value".to_string(),
            bar: 123,
            ..Default::default()
        };

        let another_opt = AnotherExtension {
            setting1: "other".to_string(),
            setting2: 456,
            ..Default::default()
        };

        set_distributed_option_extension(&mut config, custom_opt)?;
        set_distributed_option_extension(&mut config, another_opt)?;

        let flight_metadata = config.get_extension::<ContextGrpcMetadata>().unwrap();
        let metadata = &flight_metadata.0;

        assert!(metadata.contains_key("x-datafusion-distributed-config-custom.foo"));
        assert!(metadata.contains_key("x-datafusion-distributed-config-custom.bar"));
        assert!(metadata.contains_key("x-datafusion-distributed-config-another.setting1"));
        assert!(metadata.contains_key("x-datafusion-distributed-config-another.setting2"));

        let get = |key: &str| metadata.get(key).unwrap().to_str().unwrap();

        assert_eq!(
            get("x-datafusion-distributed-config-custom.foo"),
            "custom_value"
        );
        assert_eq!(get("x-datafusion-distributed-config-custom.bar"), "123");
        assert_eq!(
            get("x-datafusion-distributed-config-another.setting1"),
            "other"
        );
        assert_eq!(
            get("x-datafusion-distributed-config-another.setting2"),
            "456"
        );

        let mut new_config = SessionConfig::new();
        set_distributed_option_extension_from_headers::<CustomExtension>(
            &mut new_config,
            metadata,
        )?;
        set_distributed_option_extension_from_headers::<AnotherExtension>(
            &mut new_config,
            metadata,
        )?;

        let propagated_custom = get_ext::<CustomExtension>(&new_config);
        let propagated_another = get_ext::<AnotherExtension>(&new_config);

        assert_eq!(propagated_custom.foo, "custom_value");
        assert_eq!(propagated_custom.bar, 123);
        assert_eq!(propagated_another.setting1, "other");
        assert_eq!(propagated_another.setting2, 456);

        Ok(())
    }

    #[test]
    fn test_invalid_header_name() {
        let mut config = SessionConfig::new();
        let extension = InvalidExtension::default();

        let result = set_distributed_option_extension(&mut config, extension);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_header_value() {
        let mut config = SessionConfig::new();
        let extension = InvalidValueExtension::default();

        let result = set_distributed_option_extension(&mut config, extension);
        assert!(result.is_err());
    }

    extensions_options! {
        pub struct CustomExtension {
            pub foo: String, default = "".to_string()
            pub bar: usize, default = 0
            pub baz: bool, default = false
        }
    }

    impl ConfigExtension for CustomExtension {
        const PREFIX: &'static str = "custom";
    }

    extensions_options! {
        pub struct AnotherExtension {
            pub setting1: String, default = "default1".to_string()
            pub setting2: usize, default = 42
        }
    }

    impl ConfigExtension for AnotherExtension {
        const PREFIX: &'static str = "another";
    }

    extensions_options! {
        pub struct InvalidExtension {
            pub key_with_spaces: String, default = "value".to_string()
        }
    }

    impl ConfigExtension for InvalidExtension {
        const PREFIX: &'static str = "invalid key with spaces";
    }

    extensions_options! {
        pub struct InvalidValueExtension {
            pub key: String, default = "\u{0000}invalid\u{0001}".to_string()
        }
    }

    impl ConfigExtension for InvalidValueExtension {
        const PREFIX: &'static str = "invalid_value";
    }

    fn get_ext<T: ConfigExtension>(cfg: &SessionConfig) -> &T {
        cfg.options().extensions.get::<T>().unwrap()
    }
}
