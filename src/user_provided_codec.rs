use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use std::sync::Arc;

pub struct UserProvidedCodec(Arc<dyn PhysicalExtensionCodec>);

#[allow(private_bounds)]
pub fn add_user_codec(
    transport: &mut impl UserCodecTransport,
    codec: impl PhysicalExtensionCodec + 'static,
) {
    transport.set(codec);
}

#[allow(private_bounds)]
pub fn with_user_codec<T: UserCodecTransport>(
    mut transport: T,
    codec: impl PhysicalExtensionCodec + 'static,
) -> T {
    transport.set(codec);
    transport
}

#[allow(private_bounds)]
pub fn get_user_codec(
    transport: &impl UserCodecTransport,
) -> Option<Arc<dyn PhysicalExtensionCodec>> {
    transport.get()
}

trait UserCodecTransport {
    fn set(&mut self, codec: impl PhysicalExtensionCodec + 'static);
    fn get(&self) -> Option<Arc<dyn PhysicalExtensionCodec>>;
}

impl UserCodecTransport for SessionConfig {
    fn set(&mut self, codec: impl PhysicalExtensionCodec + 'static) {
        self.set_extension(Arc::new(UserProvidedCodec(Arc::new(codec))));
    }

    fn get(&self) -> Option<Arc<dyn PhysicalExtensionCodec>> {
        Some(Arc::clone(&self.get_extension::<UserProvidedCodec>()?.0))
    }
}

impl UserCodecTransport for SessionContext {
    fn set(&mut self, codec: impl PhysicalExtensionCodec + 'static) {
        self.state_ref().write().config_mut().set(codec)
    }

    fn get(&self) -> Option<Arc<dyn PhysicalExtensionCodec>> {
        self.state_ref().read().config().get()
    }
}

impl UserCodecTransport for SessionStateBuilder {
    fn set(&mut self, codec: impl PhysicalExtensionCodec + 'static) {
        self.config().get_or_insert_default().set(codec);
    }

    fn get(&self) -> Option<Arc<dyn PhysicalExtensionCodec>> {
        // Nobody will never want to retriever a user codec from a SessionStateBuilder
        None
    }
}
