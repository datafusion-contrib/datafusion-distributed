
use std::sync::Arc;

use async_trait::async_trait;
use arrow_flight::{decode::FlightRecordBatchStream, Action, Ticket};
use bytes::Bytes;
use futures::stream::BoxStream;
use crate::result::Result;
use std::collections::HashMap;
use parking_lot::RwLock;
use std::sync::OnceLock;

use crate::vocab::Host;


/// Global “cache” – **exactly one Arc per host** (backed by a Grpc/duplex channel etc.)
static REGISTRY: OnceLock<RwLock<HashMap<String, Arc<dyn WorkerTransport>>>> = OnceLock::new();

pub fn register(host: &Host, tx: Arc<dyn WorkerTransport>) {
    REGISTRY.write().insert(host.addr.clone(), tx);
}

pub fn get(host: &Host) -> anyhow::Result<Arc<dyn WorkerTransport>> {
    REGISTRY
        .read()
        .get(&host.addr)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("no transport registered for {}", host))
}

/// Poison the entry when gRPC tells us the channel is broken.
pub fn poison(host: &Host) {
    REGISTRY.write().remove(&host.addr);
}

#[async_trait]
pub trait WorkerTransport: Send + Sync {
    /// Execute a Flight `Action` (e.g. `add_plan`) and
    /// stream back the raw payloads.
    async fn do_action(
        &self,
        action: Action,
    ) -> Result<BoxStream<'static, Result<Bytes>>>;

    /// Fetch a stream of record batches identified by `Ticket`.
    async fn do_get(
        &self,
        ticket: Ticket,
    ) -> Result<FlightRecordBatchStream>;
}