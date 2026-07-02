use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use std::sync::Arc;

pub struct UserProvidedCodecs(Vec<Arc<dyn PhysicalExtensionCodec>>);

pub(crate) fn set_distributed_user_codec_arc(
    cfg: &mut SessionConfig,
    codec: Arc<dyn PhysicalExtensionCodec>,
) {
    let mut codecs = match cfg.get_extension::<UserProvidedCodecs>() {
        None => vec![],
        Some(prev) => prev.0.clone(),
    };
    codecs.push(codec);
    cfg.set_extension(Arc::new(UserProvidedCodecs(codecs)))
}

pub(crate) fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(
    cfg: &mut SessionConfig,
    codec: T,
) {
    set_distributed_user_codec_arc(cfg, Arc::new(codec))
}

pub(crate) fn get_distributed_user_codecs(
    cfg: &SessionConfig,
) -> Vec<Arc<dyn PhysicalExtensionCodec>> {
    match cfg.get_extension::<UserProvidedCodecs>() {
        None => vec![],
        Some(v) => v.0.clone(),
    }
}
