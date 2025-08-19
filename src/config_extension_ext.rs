use datafusion::common::{internal_datafusion_err, DataFusionError};
use datafusion::config::ConfigExtension;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::prelude::{SessionConfig, SessionContext};
use delegate::delegate;
use http::{HeaderMap, HeaderName};
use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;

const FLIGHT_METADATA_CONFIG_PREFIX: &str = "x-datafusion-distributed-config-";

/// Extension trait for `SessionConfig` to add support for propagating [ConfigExtension]s across
/// network calls.
pub trait ConfigExtensionExt {
    /// Adds the provided [ConfigExtension] to the distributed context. The [ConfigExtension] will
    /// be serialized using gRPC metadata and sent across tasks. Users are expected to call this
    /// method with their own extensions to be able to access them in any place in the
    /// plan.
    ///
    /// This method also adds the provided [ConfigExtension] to the current session option
    /// extensions, the same as calling [SessionConfig::with_option_extension].
    ///
    /// Example:
    ///
    /// ```rust
    /// # use async_trait::async_trait;
    /// # use datafusion::common::{extensions_options, DataFusionError};
    /// # use datafusion::config::ConfigExtension;
    /// # use datafusion::execution::SessionState;
    /// # use datafusion::prelude::SessionConfig;
    /// # use datafusion_distributed::{ConfigExtensionExt, SessionBuilder};
    ///
    /// extensions_options! {
    ///     pub struct CustomExtension {
    ///         pub foo: String, default = "".to_string()
    ///         pub bar: usize, default = 0
    ///         pub baz: bool, default = false
    ///     }
    /// }
    ///
    /// impl ConfigExtension for CustomExtension {
    ///     const PREFIX: &'static str = "custom";
    /// }
    ///
    /// let mut config = SessionConfig::new();
    /// let mut opt = CustomExtension::default();
    /// // Now, the CustomExtension will be able to cross network boundaries. Upon making an Arrow
    /// // Flight request, it will be sent through gRPC metadata.
    /// config.add_distributed_option_extension(opt).unwrap();
    ///
    /// struct MyCustomSessionBuilder;
    ///
    /// #[async_trait]
    /// impl SessionBuilder for MyCustomSessionBuilder {
    ///     async fn session_state(&self, mut state: SessionState) -> Result<SessionState, DataFusionError> {
    ///         // while providing this MyCustomSessionBuilder to an Arrow Flight endpoint, it will
    ///         // know how to deserialize the CustomExtension from the gRPC metadata.
    ///         state.retrieve_distributed_option_extension::<CustomExtension>()?;
    ///         Ok(state)
    ///     }
    /// }
    /// ```
    fn add_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
        t: T,
    ) -> Result<(), DataFusionError>;

    /// Gets the specified [ConfigExtension] from the distributed context and adds it to
    /// the [SessionConfig::options] extensions. The function will build a new [ConfigExtension]
    /// out of the Arrow Flight gRPC metadata present in the [SessionConfig] and will propagate it
    /// to the extension options.
    /// Example:
    ///
    /// ```rust
    /// # use async_trait::async_trait;
    /// # use datafusion::common::{extensions_options, DataFusionError};
    /// # use datafusion::config::ConfigExtension;
    /// # use datafusion::execution::SessionState;
    /// # use datafusion::prelude::SessionConfig;
    /// # use datafusion_distributed::{ConfigExtensionExt, SessionBuilder};
    ///
    /// extensions_options! {
    ///     pub struct CustomExtension {
    ///         pub foo: String, default = "".to_string()
    ///         pub bar: usize, default = 0
    ///         pub baz: bool, default = false
    ///     }
    /// }
    ///
    /// impl ConfigExtension for CustomExtension {
    ///     const PREFIX: &'static str = "custom";
    /// }
    ///
    /// let mut config = SessionConfig::new();
    /// let mut opt = CustomExtension::default();
    /// // Now, the CustomExtension will be able to cross network boundaries. Upon making an Arrow
    /// // Flight request, it will be sent through gRPC metadata.
    /// config.add_distributed_option_extension(opt).unwrap();
    ///
    /// struct MyCustomSessionBuilder;
    ///
    /// #[async_trait]
    /// impl SessionBuilder for MyCustomSessionBuilder {
    ///     async fn session_state(&self, mut state: SessionState) -> Result<SessionState, DataFusionError> {
    ///         // while providing this MyCustomSessionBuilder to an Arrow Flight endpoint, it will
    ///         // know how to deserialize the CustomExtension from the gRPC metadata.
    ///         state.retrieve_distributed_option_extension::<CustomExtension>()?;
    ///         Ok(state)
    ///     }
    /// }
    /// ```
    fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
    ) -> Result<(), DataFusionError>;
}

impl ConfigExtensionExt for SessionConfig {
    fn add_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
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
        match self.get_extension::<ContextGrpcMetadata>() {
            None => self.set_extension(Arc::new(flight_metadata)),
            Some(prev) => {
                let prev = prev.as_ref().clone();
                self.set_extension(Arc::new(prev.merge(flight_metadata)))
            }
        }
        self.options_mut().extensions.insert(t);
        Ok(())
    }

    fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(
        &mut self,
    ) -> Result<(), DataFusionError> {
        let Some(flight_metadata) = self.get_extension::<ContextGrpcMetadata>() else {
            return Ok(());
        };

        let mut result = T::default();
        let mut found_some = false;
        for (k, v) in flight_metadata.0.iter() {
            let key = k.as_str().trim_start_matches(FLIGHT_METADATA_CONFIG_PREFIX);
            if key.starts_with(T::PREFIX) {
                found_some = true;
                result.set(
                    &key.trim_start_matches(T::PREFIX).trim_start_matches("."),
                    v.to_str().map_err(|err| {
                        internal_datafusion_err!("Cannot parse header value: {err}")
                    })?,
                )?;
            }
        }
        if !found_some {
            return Ok(());
        }
        self.options_mut().extensions.insert(result);
        Ok(())
    }
}

impl ConfigExtensionExt for SessionStateBuilder {
    delegate! {
        to self.config().get_or_insert_default() {
            fn add_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) -> Result<(), DataFusionError>;
            fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(&mut self) -> Result<(), DataFusionError>;
        }
    }
}

impl ConfigExtensionExt for SessionState {
    delegate! {
        to self.config_mut() {
            fn add_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) -> Result<(), DataFusionError>;
            fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(&mut self) -> Result<(), DataFusionError>;
        }
    }
}

impl ConfigExtensionExt for SessionContext {
    delegate! {
        to self.state_ref().write().config_mut() {
            fn add_distributed_option_extension<T: ConfigExtension + Default>(&mut self, t: T) -> Result<(), DataFusionError>;
            fn retrieve_distributed_option_extension<T: ConfigExtension + Default>(&mut self) -> Result<(), DataFusionError>;
        }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct ContextGrpcMetadata(pub HeaderMap);

impl ContextGrpcMetadata {
    pub(crate) fn from_headers(metadata: HeaderMap) -> Self {
        let mut new = HeaderMap::new();
        for (k, v) in metadata.into_iter() {
            let Some(k) = k else { continue };
            if k.as_str().starts_with(FLIGHT_METADATA_CONFIG_PREFIX) {
                new.insert(k, v);
            }
        }
        Self(new)
    }

    fn merge(mut self, other: Self) -> Self {
        for (k, v) in other.0.into_iter() {
            let Some(k) = k else { continue };
            self.0.insert(k, v);
        }
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::config_extension_ext::ContextGrpcMetadata;
    use crate::ConfigExtensionExt;
    use datafusion::common::extensions_options;
    use datafusion::config::ConfigExtension;
    use datafusion::prelude::SessionConfig;
    use http::{HeaderMap, HeaderName, HeaderValue};
    use std::str::FromStr;

    #[test]
    fn test_propagation() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        let mut opt = CustomExtension::default();
        opt.foo = "foo".to_string();
        opt.bar = 1;
        opt.baz = true;

        config.add_distributed_option_extension(opt)?;

        let mut new_config = SessionConfig::new();
        new_config.set_extension(config.get_extension::<ContextGrpcMetadata>().unwrap());
        new_config.retrieve_distributed_option_extension::<CustomExtension>()?;

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

        config.add_distributed_option_extension(opt)?;

        let flight_metadata = config.get_extension::<ContextGrpcMetadata>();
        assert!(flight_metadata.is_some());

        let metadata = &flight_metadata.unwrap().0;
        assert!(metadata.contains_key("x-datafusion-distributed-custom.foo"));
        assert!(metadata.contains_key("x-datafusion-distributed-custom.bar"));
        assert!(metadata.contains_key("x-datafusion-distributed-custom.baz"));

        let get = |key: &str| metadata.get(key).unwrap().to_str().unwrap();
        assert_eq!(get("x-datafusion-distributed-custom.foo"), "");
        assert_eq!(get("x-datafusion-distributed-custom.bar"), "0");
        assert_eq!(get("x-datafusion-distributed-custom.baz"), "false");

        Ok(())
    }

    #[test]
    fn test_new_extension_overwrites_previous() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        let mut opt1 = CustomExtension::default();
        opt1.foo = "first".to_string();
        config.add_distributed_option_extension(opt1)?;

        let mut opt2 = CustomExtension::default();
        opt2.bar = 42;
        config.add_distributed_option_extension(opt2)?;

        let flight_metadata = config.get_extension::<ContextGrpcMetadata>().unwrap();
        let metadata = &flight_metadata.0;

        let get = |key: &str| metadata.get(key).unwrap().to_str().unwrap();
        assert_eq!(get("x-datafusion-distributed-custom.foo"), "");
        assert_eq!(get("x-datafusion-distributed-custom.bar"), "42");
        assert_eq!(get("x-datafusion-distributed-custom.baz"), "false");

        Ok(())
    }

    #[test]
    fn test_propagate_no_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        config.retrieve_distributed_option_extension::<CustomExtension>()?;

        let extension = config.options().extensions.get::<CustomExtension>();
        assert!(extension.is_none());

        Ok(())
    }

    #[test]
    fn test_propagate_no_matching_prefix() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();
        let mut header_map = HeaderMap::new();
        header_map.insert(
            HeaderName::from_str("x-datafusion-distributed-other.setting").unwrap(),
            HeaderValue::from_str("value").unwrap(),
        );

        let flight_metadata = ContextGrpcMetadata::from_headers(header_map);
        config.set_extension(std::sync::Arc::new(flight_metadata));
        config.retrieve_distributed_option_extension::<CustomExtension>()?;

        let extension = config.options().extensions.get::<CustomExtension>();
        assert!(extension.is_none());

        Ok(())
    }

    #[test]
    fn test_multiple_extensions_different_prefixes() -> Result<(), Box<dyn std::error::Error>> {
        let mut config = SessionConfig::new();

        let mut custom_opt = CustomExtension::default();
        custom_opt.foo = "custom_value".to_string();
        custom_opt.bar = 123;

        let mut another_opt = AnotherExtension::default();
        another_opt.setting1 = "other".to_string();
        another_opt.setting2 = 456;

        config.add_distributed_option_extension(custom_opt)?;
        config.add_distributed_option_extension(another_opt)?;

        let flight_metadata = config.get_extension::<ContextGrpcMetadata>().unwrap();
        let metadata = &flight_metadata.0;

        assert!(metadata.contains_key("x-datafusion-distributed-custom.foo"));
        assert!(metadata.contains_key("x-datafusion-distributed-custom.bar"));
        assert!(metadata.contains_key("x-datafusion-distributed-another.setting1"));
        assert!(metadata.contains_key("x-datafusion-distributed-another.setting2"));

        let get = |key: &str| metadata.get(key).unwrap().to_str().unwrap();

        assert_eq!(get("x-datafusion-distributed-custom.foo"), "custom_value");
        assert_eq!(get("x-datafusion-distributed-custom.bar"), "123");
        assert_eq!(get("x-datafusion-distributed-another.setting1"), "other");
        assert_eq!(get("x-datafusion-distributed-another.setting2"), "456");

        let mut new_config = SessionConfig::new();
        new_config.set_extension(flight_metadata);
        new_config.retrieve_distributed_option_extension::<CustomExtension>()?;
        new_config.retrieve_distributed_option_extension::<AnotherExtension>()?;

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

        let result = config.add_distributed_option_extension(extension);
        assert!(result.is_err());
    }

    #[test]
    fn test_invalid_header_value() {
        let mut config = SessionConfig::new();
        let extension = InvalidValueExtension::default();

        let result = config.add_distributed_option_extension(extension);
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
