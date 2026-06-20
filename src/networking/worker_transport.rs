use crate::DistributedConfig;
use crate::config_extension_ext::set_distributed_option_extension;
use crate::worker::{FlightWorkerTransport, WorkerTransport};
use datafusion::prelude::SessionConfig;
use std::sync::Arc;

pub fn set_distributed_worker_transport(
    cfg: &mut SessionConfig,
    worker_transport: impl WorkerTransport + 'static,
) {
    let opts = cfg.options_mut();
    let worker_transport = WorkerTransportExtension(Arc::new(worker_transport));
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg.__private_worker_transport = worker_transport;
    } else {
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_worker_transport: worker_transport,
                ..Default::default()
            },
        )
    }
}

/// Returns the [WorkerTransport] in scope, defaulting to the Arrow-Flight gRPC transport. Network
/// boundaries call this at execute time to open connections and dispatch plans, so a custom
/// transport set via [crate::DistributedExt::with_distributed_worker_transport] takes over both the
/// read and write sides.
pub fn get_distributed_worker_transport(cfg: &SessionConfig) -> Arc<dyn WorkerTransport> {
    cfg.options()
        .extensions
        .get::<DistributedConfig>()
        .map(|distributed_cfg| Arc::clone(&distributed_cfg.__private_worker_transport.0))
        .unwrap_or_else(|| Arc::new(FlightWorkerTransport))
}

#[derive(Clone)]
pub(crate) struct WorkerTransportExtension(pub(crate) Arc<dyn WorkerTransport>);

impl Default for WorkerTransportExtension {
    fn default() -> Self {
        Self(Arc::new(FlightWorkerTransport))
    }
}
