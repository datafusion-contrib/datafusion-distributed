# Shared-Memory Transport (`src/shm`)

This module provides a non-gRPC `ChannelResolver` (`src/protocol/worker_channel.rs`) and transport layer for co-located execution, where workers run as parallel tasks or processes sharing one machine and communicating over a shared-memory mesh rather than gRPC over TCP.

---

# Part 1: Consumer Guide & Public API

This section is intended for embedders (e.g. Postgres extensions like `pg_search`, co-located query engines, or custom test harnesses) integrating `datafusion-distributed` over shared memory.

## 1. Overview & Setup Concepts

Shared-memory execution replaces network connections with two coordinated communication mechanisms:
1. **Shared-Memory Ring Mesh**: Lock-free, cache-aligned MPSC ring buffers allocated in a shared memory region (POSIX `mmap`, Direct Shared Memory / DSM) carrying serialized Arrow IPC batches and control frames.
2. **IPC Signaling Mesh (`IpcMeshNotifier`)**: Cross-process signaling over Unix Domain Sockets (or in-memory channels for same-process testing). Notifiers provide asynchronous readiness notification (`DataReady`, `SpaceReady`, `StreamCancel`, `Detach`) with an atomic fast-path so zero IPC overhead is incurred during steady-state data streaming.

### Embedder Setup Flow
Every participant (leader and workers) attaches to the shared memory region and connects to the IPC signaling mesh:
- Leader allocates the memory region and sets up the listener via `leader_setup_ipc` (or `leader_setup` in `src/shm/setup.rs`).
- Workers attach to the region and connect via `worker_setup_ipc` (or `worker_setup` in `src/shm/setup.rs`).

Both setup functions return RAII session handles (`LeaderSession` and `WorkerSession` in `src/shm/setup.rs`) that own the underlying outbound channel handles and start background inbound reactors.

---

## 2. Consumer Code Examples

### Leader Query Execution

```rust
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use datafusion_distributed::shm::{
    leader_setup_ipc, QuerySocketScope, NoInterrupt, NoopWakeup,
};

pub async fn run_leader_query(
    base: *mut std::ffi::c_void,
    n_procs: u32,
    queue_bytes: usize,
    plan_bytes: &[u8],
    socket_dir: &std::path::Path,
    query_id: &str,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    // 1. RAII scope: creates <socket_dir>/df_dist_<query_id>/ and cleans it up on drop
    let _socket_scope = QuerySocketScope::new(socket_dir, query_id)?;

    // 2. Initialize DSM region and IPC signaling mesh in one step
    let session = leader_setup_ipc(
        base,
        n_procs,
        queue_bytes,
        plan_bytes,
        socket_dir,
        query_id,
        Arc::new(NoopWakeup),
        Arc::new(NoInterrupt),
    ).await?;

    // 3. Build DataFusion SessionContext using the ShmChannelResolver on session.mesh
    let ctx = SessionContext::new(); // configured with ShmChannelResolver(session.mesh.clone())
    let physical_plan = ctx.sql("SELECT ...").await?.create_physical_plan().await?;

    // 4. Execute distributed plan
    let stream = physical_plan.execute(0, ctx.task_ctx())?;
    let batches = datafusion::physical_plan::common::collect(stream).await?;

    Ok(batches)
    // 5. `session` and `_socket_scope` drop here at query completion
}
```

### Worker Execution Loop

```rust
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use datafusion_distributed::shm::{
    worker_setup_ipc, run_execute_task_loop, NoInterrupt, NoopWakeup,
};

pub async fn run_worker_process(
    base: *mut std::ffi::c_void,
    proc_idx: u32,
    socket_dir: &std::path::Path,
    query_id: &str,
    token: CancellationToken,
) -> Result<(), DataFusionError> {
    // 1. Attach to shared memory region and connect to IPC signaling mesh.
    //    Total region bytes and n_procs are automatically discovered from the DSM header at `base`.
    let session = worker_setup_ipc(
        base,
        proc_idx,
        socket_dir,
        query_id,
        Arc::new(NoopWakeup),
        proc_idx as u64, // receiver token
        Arc::new(NoInterrupt),
    ).await?;

    // 2. Run the demand-driven request loop.
    //    Listens for ExecuteTaskFrame requests, validates partition ranges, and spawns executions.
    run_execute_task_loop(
        &session.mesh,
        stage_id,
        task_number,
        n_partitions,
        token,
        |request, headers, partition_range| async move {
            // Evaluate physical fragment over partition_range and write to MppPartitionSink
            Ok(())
        },
    ).await?;

    Ok(())
    // 3. `session` drops on exit, error, or cancellation, cleanly notifying peer inboxes.
}
```

---

## 3. Public Extension Points for Embedders

Embedders customize behavior via the following interfaces:
- **Shared Memory Allocation**: Embedders calculate required region bytes using `dsm_region_bytes` or `region_total` (`src/shm/setup.rs`), allocate contiguous shared memory (POSIX shared memory or PostgreSQL DSM), and supply the base pointer.
- **Interruption Hook (`Interrupt` in `src/shm/transport.rs`)**: Embedders supply a cancellation checker (such as checking PostgreSQL query cancellation state) invoked during execution.
- **Wakeup Hook (`Wakeup` in `src/shm/mpsc_ring.rs`)**: Custom wakeup callback for environments using OS latches (e.g. Postgres `SetLatch`).
- **Worker Execution Loop (`run_execute_task_loop` in `src/shm/setup.rs`)**: Standardized demand-driven task loop handling range validation, duplicate partition protection, cancellation unwinding, and error propagation.

---

# Part 2: Internal Architecture & Implementation Details

This section documents internal mechanics for contributors to `datafusion-distributed`.

## 1. High-Level Flow & Control Topology

```text
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Coordinator                                    │
└──────────────────────────────────────┬──────────────────────────────────────┘
                                       │ SetPlan (Control Mesh)
                                       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Worker Process / Instance                          │
│                                                                             │
│  1. Receives SetPlan & registers plan fragment (TaskKey)                    │
│  2. Idles until ExecuteTaskFrame arrives from downstream consumer           │
│  3. ExecuteTaskFrame arrives specifying partition_range (start..end)        │
│  4. Opens per-task, per-partition sinks lazily for start..end               │
│  5. Evaluates run_worker_fragment(plan, sinks, ctx, start..end)             │
│  6. Streams batches out asynchronously via DSM ring buffers                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 2. Framing & Multiplexing

All data and control frames share a fixed 20-byte `MppFrameHeader` (`src/shm/transport.rs`):
- `magic` (2B) + `version` (1B) + `kind` (1B)
- `stage_id` (4B)
- `task_id` (4B)
- `partition` (4B)
- `sender_proc` (4B)

Frame kinds include `Batch`, `Eof`, `Cancel`, `Chunk`, `SetPlan`, `ExecuteTask`, `TaskMetrics`, `TaskError`, `WorkUnit`, and `FeedEof`.

Because one inbox ring per process multiplexes streams from all peers and sibling tasks, the 20-byte header ensures each frame routes to its specific `(sender_proc, stage_id, task_id, partition)` channel buffer.

## 3. Asynchronous Backpressure & Fast-Path Atomics

Rather than blocking threads or executing spin loops:
1. **Lock-Free Rings**: Senders attempt `try_send` into the DSM MPSC ring.
2. **Fast-Path Atomic Synchronization**:
   - `consumer_waiting`: Atomic boolean on the ring header. When the receiver is actively draining, this flag is false; senders push without emitting IPC socket signals. When the receiver empties the ring, it sets `consumer_waiting = true` and suspends. The next write swaps the flag to false and emits a single `DataReady` IPC signal.
   - `producers_waiting`: When a sender finds the ring full, it marks `producers_waiting = true` and suspends on `wait_for_space().await`. When the receiver frees ring space, it swaps `producers_waiting` to false and emits a single `SpaceReady` IPC signal to wake blocked producers.
3. **Chunking for Oversized Frames**: If a batch exceeds maximum frame capacity, `send_chunked` splits the message across multiple `Chunk` frames, streaming through the ring without head-of-line blocking.
4. **Inbound Reactor Task**: `DrainHandle::start_inbound_reactor` (`src/shm/transport.rs`) runs an async background task per mesh that reactively drains the inbox whenever data is available and pushes decoded batches into per-channel `DrainBuffer` (`src/shm/transport.rs`) instances.
