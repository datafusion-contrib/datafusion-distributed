use datafusion::common::{DataFusionError, Result, config_datafusion_err};
use datafusion::prelude::SessionConfig;
use std::any::TypeId;
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// Serializes one typed [`SessionConfig`] extension for distributed execution.
///
/// Registering a codec on a coordinating session opts its [`Extension`](Self::Extension) into
/// propagation to workers. The codec controls the bytes sent over the wire; DataFusion Distributed
/// does not provide confidentiality for payloads that contain credentials or other sensitive data.
/// A decoder for the same [`TYPE_URL`](Self::TYPE_URL) must be registered on each worker.
pub trait SessionExtensionCodec: Send + Sync + 'static {
    /// The concrete [`SessionConfig`] extension encoded by this codec.
    type Extension: Send + Sync + 'static;

    /// Stable identifier for this encoding.
    ///
    /// Include a format version in this value when incompatible encodings need distinct decoders,
    /// for example `"example.com/my-session-extension/v1"`.
    const TYPE_URL: &'static str;

    /// Serializes `extension` into its transport representation.
    fn encode(&self, extension: &Self::Extension) -> Result<Vec<u8>>;

    /// Deserializes an extension previously produced by [`Self::encode`].
    fn decode(&self, payload: &[u8]) -> Result<Self::Extension>;
}

/// An encoded session extension sent from a coordinator to a worker.
#[derive(Clone)]
pub struct SessionExtensionPayload {
    /// Stable identifier of the codec required to decode [`Self::payload`].
    pub type_url: String,
    /// Opaque bytes produced by the registered codec.
    ///
    /// These bytes may contain sensitive data and are intentionally omitted from [`Debug`].
    pub payload: Vec<u8>,
}

impl Debug for SessionExtensionPayload {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SessionExtensionPayload")
            .field("type_url", &self.type_url)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

pub(crate) trait ErasedSessionExtensionCodec: Send + Sync {
    fn type_url(&self) -> &'static str;
    fn extension_type_id(&self) -> TypeId;
    fn encode_from_config(&self, config: &SessionConfig) -> Result<Option<Vec<u8>>>;
    fn decode_into_config(&self, payload: &[u8], config: &mut SessionConfig) -> Result<()>;
}

struct SessionExtensionCodecAdapter<C>(C);

impl<C> ErasedSessionExtensionCodec for SessionExtensionCodecAdapter<C>
where
    C: SessionExtensionCodec,
{
    fn type_url(&self) -> &'static str {
        C::TYPE_URL
    }

    fn extension_type_id(&self) -> TypeId {
        TypeId::of::<C::Extension>()
    }

    fn encode_from_config(&self, config: &SessionConfig) -> Result<Option<Vec<u8>>> {
        config
            .get_extension::<C::Extension>()
            .map(|extension| self.0.encode(extension.as_ref()))
            .transpose()
    }

    fn decode_into_config(&self, payload: &[u8], config: &mut SessionConfig) -> Result<()> {
        if config.get_extension::<C::Extension>().is_some() {
            return Err(config_datafusion_err!(
                "Cannot decode session extension '{}': an extension of that type is already present in the worker session",
                C::TYPE_URL
            ));
        }
        config.set_extension(Arc::new(self.0.decode(payload)?));
        Ok(())
    }
}

#[derive(Clone, Default)]
pub(crate) struct SessionExtensionCodecRegistry {
    codecs: Vec<Arc<dyn ErasedSessionExtensionCodec>>,
}

impl SessionExtensionCodecRegistry {
    pub(crate) fn push<C: SessionExtensionCodec>(&mut self, codec: C) {
        self.codecs
            .push(Arc::new(SessionExtensionCodecAdapter(codec)));
    }

    fn validate(&self) -> Result<()> {
        let mut type_urls = HashSet::with_capacity(self.codecs.len());
        let mut extension_types = HashSet::with_capacity(self.codecs.len());
        for codec in &self.codecs {
            if codec.type_url().is_empty() {
                return Err(config_datafusion_err!(
                    "Session extension codec TYPE_URL cannot be empty"
                ));
            }
            if !type_urls.insert(codec.type_url()) {
                return Err(config_datafusion_err!(
                    "Multiple session extension codecs use TYPE_URL '{}'",
                    codec.type_url()
                ));
            }
            if !extension_types.insert(codec.extension_type_id()) {
                return Err(config_datafusion_err!(
                    "Multiple session extension codecs encode the same Rust extension type"
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn encode(&self, config: &SessionConfig) -> Result<Vec<SessionExtensionPayload>> {
        self.validate()?;
        self.codecs
            .iter()
            .filter_map(|codec| {
                codec.encode_from_config(config).transpose().map(|result| {
                    result.map(|payload| SessionExtensionPayload {
                        type_url: codec.type_url().to_string(),
                        payload,
                    })
                })
            })
            .collect()
    }

    pub(crate) fn decode(
        &self,
        payloads: &[SessionExtensionPayload],
        config: &mut SessionConfig,
    ) -> Result<()> {
        self.validate()?;
        let mut decoded_type_urls = HashSet::with_capacity(payloads.len());
        for payload in payloads {
            if !decoded_type_urls.insert(payload.type_url.as_str()) {
                return Err(config_datafusion_err!(
                    "Received multiple session extension payloads for TYPE_URL '{}'",
                    payload.type_url
                ));
            }
            let codec = self
                .codecs
                .iter()
                .find(|codec| codec.type_url() == payload.type_url)
                .ok_or_else(|| {
                    DataFusionError::Configuration(format!(
                        "No worker session extension codec is registered for TYPE_URL '{}'",
                        payload.type_url
                    ))
                })?;
            codec.decode_into_config(&payload.payload, config)?;
        }
        Ok(())
    }
}

#[derive(Clone, Default)]
struct CoordinatorSessionExtensionCodecs(SessionExtensionCodecRegistry);

pub(crate) fn set_session_extension_codec<C: SessionExtensionCodec>(
    config: &mut SessionConfig,
    codec: C,
) {
    let mut codecs = config
        .get_extension::<CoordinatorSessionExtensionCodecs>()
        .map(|codecs| codecs.as_ref().clone())
        .unwrap_or_default();
    codecs.0.push(codec);
    config.set_extension(Arc::new(codecs));
}

pub(crate) fn encode_session_extensions(
    config: &SessionConfig,
) -> Result<Vec<SessionExtensionPayload>> {
    match config.get_extension::<CoordinatorSessionExtensionCodecs>() {
        Some(codecs) => codecs.0.encode(config),
        None => Ok(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct TestExtension(String);

    struct TestCodec;

    impl SessionExtensionCodec for TestCodec {
        type Extension = TestExtension;
        const TYPE_URL: &'static str = "test/extension/v1";

        fn encode(&self, extension: &Self::Extension) -> Result<Vec<u8>> {
            Ok(extension.0.as_bytes().to_vec())
        }

        fn decode(&self, payload: &[u8]) -> Result<Self::Extension> {
            Ok(TestExtension(String::from_utf8(payload.to_vec()).unwrap()))
        }
    }

    #[test]
    fn registered_extension_roundtrips() -> Result<()> {
        let mut coordinator = SessionConfig::new()
            .with_extension(Arc::new(TestExtension("request value".to_string())));
        set_session_extension_codec(&mut coordinator, TestCodec);
        let payloads = encode_session_extensions(&coordinator)?;

        let mut worker = SessionConfig::new();
        let mut codecs = SessionExtensionCodecRegistry::default();
        codecs.push(TestCodec);
        codecs.decode(&payloads, &mut worker)?;

        assert_eq!(
            worker.get_extension::<TestExtension>().as_deref(),
            Some(&TestExtension("request value".to_string()))
        );
        Ok(())
    }

    #[test]
    fn registered_codec_skips_absent_extension() -> Result<()> {
        let mut config = SessionConfig::new();
        set_session_extension_codec(&mut config, TestCodec);

        assert!(encode_session_extensions(&config)?.is_empty());
        Ok(())
    }

    #[test]
    fn decoding_does_not_replace_existing_worker_extension() {
        let payloads = vec![SessionExtensionPayload {
            type_url: TestCodec::TYPE_URL.to_string(),
            payload: b"request value".to_vec(),
        }];
        let mut worker = SessionConfig::new()
            .with_extension(Arc::new(TestExtension("worker value".to_string())));
        let mut codecs = SessionExtensionCodecRegistry::default();
        codecs.push(TestCodec);

        let err = codecs.decode(&payloads, &mut worker).unwrap_err();
        assert!(err.to_string().contains("already present"));
        assert_eq!(
            worker.get_extension::<TestExtension>().as_deref(),
            Some(&TestExtension("worker value".to_string()))
        );
    }
}
