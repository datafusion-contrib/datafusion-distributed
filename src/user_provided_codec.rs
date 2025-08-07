use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use std::sync::Arc;

pub struct UserProvidedCodec(Arc<dyn PhysicalExtensionCodec>);

/// Injects a user-defined codec that is capable of encoding/decoding custom execution nodes.
/// It will inject the codec as a config extension in the provided [SessionConfig], [SessionContext]
/// or [SessionStateBuilder].
///
/// Example:
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion::execution::{SessionState, FunctionRegistry, SessionStateBuilder};
/// # use datafusion::physical_plan::ExecutionPlan;
/// # use datafusion_proto::physical_plan::PhysicalExtensionCodec;
/// # use datafusion_distributed::{add_user_codec};
///
/// #[derive(Debug)]
/// struct CustomExecCodec;
///
/// impl PhysicalExtensionCodec for CustomExecCodec {
///     fn try_decode(&self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>], registry: &dyn FunctionRegistry) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
///         todo!()
///     }
///
///     fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> datafusion::common::Result<()> {
///         todo!()
///     }
/// }
///
/// let builder = SessionStateBuilder::new();
/// let mut state = builder.build();
/// add_user_codec(state.config_mut(), CustomExecCodec);
/// ```
#[allow(private_bounds)]
pub fn add_user_codec(
    transport: &mut impl UserCodecTransport,
    codec: impl PhysicalExtensionCodec + 'static,
) {
    transport.set(codec);
}

/// Adds a user-defined codec that is capable of encoding/decoding custom execution nodes.
/// It returns the [SessionContext], [SessionConfig] or [SessionStateBuilder] passed on the first
/// argument with the user-defined codec already placed into the config extensions.
///
/// Example:
///
/// ```
/// # use std::sync::Arc;
/// # use datafusion::execution::{SessionState, FunctionRegistry, SessionStateBuilder};
/// # use datafusion::physical_plan::ExecutionPlan;
/// # use datafusion_proto::physical_plan::PhysicalExtensionCodec;
/// # use datafusion_distributed::with_user_codec;
///
/// #[derive(Debug)]
/// struct CustomExecCodec;
///
/// impl PhysicalExtensionCodec for CustomExecCodec {
///     fn try_decode(&self, buf: &[u8], inputs: &[Arc<dyn ExecutionPlan>], registry: &dyn FunctionRegistry) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
///         todo!()
///     }
///
///     fn try_encode(&self, node: Arc<dyn ExecutionPlan>, buf: &mut Vec<u8>) -> datafusion::common::Result<()> {
///         todo!()
///     }
/// }
///
/// let builder = SessionStateBuilder::new();
/// let builder = with_user_codec(builder, CustomExecCodec);
/// let state = builder.build();
/// ```
#[allow(private_bounds)]
pub fn with_user_codec<T: UserCodecTransport>(
    mut transport: T,
    codec: impl PhysicalExtensionCodec + 'static,
) -> T {
    transport.set(codec);
    transport
}

#[allow(private_bounds)]
pub(crate) fn get_user_codec(
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
