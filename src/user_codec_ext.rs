use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use std::sync::Arc;

pub struct UserProvidedCodec(Arc<dyn PhysicalExtensionCodec>);

pub(crate) fn set_distributed_user_codec<T: PhysicalExtensionCodec + 'static>(
    cfg: &mut SessionConfig,
    codec: T,
) {
    cfg.set_extension(Arc::new(UserProvidedCodec(Arc::new(codec))))
}

pub(crate) fn get_distributed_user_codec(
    cfg: &SessionConfig,
) -> Option<Arc<dyn PhysicalExtensionCodec>> {
    Some(Arc::clone(&cfg.get_extension::<UserProvidedCodec>()?.0))
}
