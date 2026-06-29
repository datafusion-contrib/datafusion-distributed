//! The reference implementation of the worker protocol for a co-located worker.
//!
//! It implements [`WorkerChannel`] by calling a [`Worker`] that lives in the same process, with no
//! gRPC, no IPC, and no networking. Its purpose is twofold: it lets the crate run distributed
//! queries with the `grpc` feature off (the protocol abstraction is only real if something other
//! than gRPC implements it), and it is the shape a custom co-located transport (for example a
//! shared-memory mesh spanning sibling processes) follows: implement [`ChannelResolver`] to hand
//! out a channel for a URL, then route the three protocol methods to wherever the worker runs.

use crate::protocol::{ChannelResolver, WorkerChannel};
use crate::{
    CoordinatorToWorkerMsg, DefaultSessionBuilder, DistributedExt, ExecuteTaskRequest,
    GetWorkerInfoRequest, GetWorkerInfoResponse, MappedWorkerSessionBuilderExt, Worker,
    WorkerSessionBuilder, WorkerToCoordinatorMsg,
};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::{DataFusionError, Result, internal_datafusion_err};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use futures::stream::{BoxStream, StreamExt};
use http::HeaderMap;
use std::sync::{Arc, Weak};
use url::Url;

/// A [`ChannelResolver`] backed by a single co-located [`Worker`].
///
/// Every URL resolves to that one worker: with no network there is nothing to dial, so the URLs a
/// [`crate::WorkerResolver`] hands out only label the tasks the planner routes. One worker holds the
/// state for every task, keyed by [`crate::TaskKey`], the same way the gRPC worker does when several
/// tasks of a query land on it.
#[derive(Clone)]
pub struct InProcessChannelResolver {
    worker: Arc<Worker>,
}

impl InProcessChannelResolver {
    /// Builds the co-located worker from `session_builder`, registering this resolver into the
    /// worker's own per-query sessions so a worker reading a downstream stage stays in process
    /// rather than falling back to the gRPC default.
    pub fn from_session_builder(
        session_builder: impl WorkerSessionBuilder + Send + Sync + 'static,
    ) -> Self {
        // The worker and the resolver point at each other: the resolver runs tasks on the worker,
        // and the worker resolves its own nested reads back through the resolver. A `Weak` on the
        // worker's side breaks the cycle, so the returned `InProcessChannelResolver` owns the only
        // strong reference and dropping it frees the worker.
        let worker = Arc::new_cyclic(|weak: &Weak<Worker>| {
            let weak = weak.clone();
            Worker::from_session_builder(session_builder.map(move |builder| {
                Ok(builder
                    .with_distributed_channel_resolver(WeakInProcessChannelResolver(weak.clone()))
                    .build())
            }))
        });
        Self { worker }
    }
}

impl Default for InProcessChannelResolver {
    fn default() -> Self {
        Self::from_session_builder(DefaultSessionBuilder)
    }
}

#[async_trait]
impl ChannelResolver for InProcessChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        _url: &Url,
    ) -> Result<Box<dyn WorkerChannel>, DataFusionError> {
        Ok(Box::new(InProcessWorkerChannel {
            worker: Arc::clone(&self.worker),
        }))
    }
}

/// The resolver a worker installs in its own sessions. It upgrades a [`Weak`] reference to the
/// co-located worker so a read of a downstream stage routes back in process instead of dialing out.
struct WeakInProcessChannelResolver(Weak<Worker>);

#[async_trait]
impl ChannelResolver for WeakInProcessChannelResolver {
    async fn get_worker_client_for_url(
        &self,
        _url: &Url,
    ) -> Result<Box<dyn WorkerChannel>, DataFusionError> {
        let worker = self
            .0
            .upgrade()
            .ok_or_else(|| internal_datafusion_err!("the in-process worker has been dropped"))?;
        Ok(Box::new(InProcessWorkerChannel { worker }))
    }
}

/// A [`WorkerChannel`] that calls a co-located [`Worker`] directly.
struct InProcessWorkerChannel {
    worker: Arc<Worker>,
}

#[async_trait]
impl WorkerChannel for InProcessWorkerChannel {
    async fn coordinator_channel(
        &mut self,
        headers: HeaderMap,
        c2w_stream: BoxStream<'static, CoordinatorToWorkerMsg>,
    ) -> Result<BoxStream<'static, Result<WorkerToCoordinatorMsg>>> {
        // The worker reads a fallible stream so a wire transport can surface its decode errors.
        // Handing messages over in process has no such step, so each one is already an `Ok`.
        self.worker
            .coordinator_channel(headers, c2w_stream.map(Ok).boxed())
            .await
    }

    async fn execute_task(
        &mut self,
        _headers: HeaderMap,
        request: ExecuteTaskRequest,
        _metrics: ExecutionPlanMetricsSet,
        _task_ctx: &Arc<TaskContext>,
    ) -> Result<Vec<BoxStream<'static, Result<RecordBatch>>>> {
        // Reading a partition runs the producer in place: the returned streams are the worker's own
        // task output, so there is no IPC decode pass and no network metrics to record. The
        // consumer's `task_ctx` is the consumer side's; the producer runs under the worker's own.
        let (streams, _task_ctx) = self.worker.execute_task(request).await?;
        Ok(streams.into_iter().map(|stream| stream.boxed()).collect())
    }

    async fn get_worker_info(
        &mut self,
        _request: GetWorkerInfoRequest,
    ) -> Result<GetWorkerInfoResponse> {
        Ok(GetWorkerInfoResponse {
            version: self.worker.version().to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{SessionStateBuilderExt, WorkerResolver, display_plan_ascii};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{CsvReadOptions, SessionConfig, SessionContext};
    use std::io::Write;

    /// Hands out as many placeholder URLs as workers. With one co-located worker behind the
    /// transport, these only label the tasks the planner routes; nothing is dialed.
    struct InProcessWorkers(usize);

    impl WorkerResolver for InProcessWorkers {
        fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
            (0..self.0)
                .map(|i| Url::parse(&format!("http://worker-{i}")))
                .collect::<Result<_, _>>()
                .map_err(|err| DataFusionError::External(Box::new(err)))
        }
    }

    /// Drives a real distributed query end to end through the in-process transport. With the `grpc`
    /// feature off this is the only transport that can run it; `cargo check --no-default-features`
    /// covers the no-gRPC compile.
    #[tokio::test]
    async fn in_process_transport_runs_a_distributed_query() -> Result<()> {
        const N: usize = 4;

        // A file scan round-trips through `datafusion-proto`, so a worker can rebuild it from the
        // serialized stage plan. An in-memory table would not, hence a CSV on disk.
        let path = std::env::temp_dir().join(format!("dfd_in_process_{}.csv", std::process::id()));
        let mut file =
            std::fs::File::create(&path).map_err(|e| DataFusionError::External(Box::new(e)))?;
        writeln!(file, "k,v").unwrap();
        for i in 0..200 {
            writeln!(file, "{},{}", ["a", "b", "c", "d"][i % 4], i).unwrap();
        }
        drop(file);
        let path = path.to_str().unwrap().to_string();

        let query = "SELECT k, COUNT(*) AS c FROM t GROUP BY k ORDER BY k";

        // Single-node reference result.
        let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(N));
        ctx.register_csv("t", &path, CsvReadOptions::new()).await?;
        let expected = collect(
            ctx.sql(query).await?.create_physical_plan().await?,
            ctx.task_ctx(),
        )
        .await?;
        let expected = pretty_format_batches(&expected)?.to_string();

        // Distributed over the in-process transport.
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::new().with_target_partitions(N))
            .with_distributed_planner()
            .with_distributed_worker_resolver(InProcessWorkers(N))
            .with_distributed_channel_resolver(InProcessChannelResolver::default())
            .with_distributed_file_scan_config_bytes_per_partition(1)
            .unwrap()
            .build();
        let ctx_distributed = SessionContext::from(state);
        ctx_distributed
            .register_csv("t", &path, CsvReadOptions::new())
            .await?;

        let physical = ctx_distributed
            .sql(query)
            .await?
            .create_physical_plan()
            .await?;
        let rendered = display_plan_ascii(physical.as_ref(), false);
        assert!(
            rendered.contains("DistributedExec"),
            "plan was not distributed:\n{rendered}"
        );
        assert!(
            rendered.contains("NetworkShuffleExec"),
            "no shuffle boundary, so the transport never carried a cross-task stream:\n{rendered}"
        );

        let actual = collect(physical, ctx_distributed.task_ctx()).await?;
        let actual = pretty_format_batches(&actual)?.to_string();

        let _ = std::fs::remove_file(&path);
        assert_eq!(actual, expected);
        Ok(())
    }
}
