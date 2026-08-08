# Shared-Memory Transport (`src/shm`)

This module provides a non-gRPC [`ChannelResolver`](../protocol/worker_channel.rs) and transport layer for co-located execution, where workers run as parallel tasks or processes sharing one machine and communicating over a shared-memory mesh rather than gRPC over TCP.

---

## Architecture Overview

The shared-memory transport mirrors the canonical gRPC protocol's pull-based RPC design while replacing network serialization and TCP sockets with Direct Shared Memory (DSM) ring buffers and compact 20-byte frame headers.

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
│  6. Streams batches out via DSM ring buffers                                │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Control & Data Plane Design

### 1. Control Plane (`MppMesh`, `DrainHandle` & `run_execute_task_loop`)
- **Messages**: Frames carry a 20-byte [`MppFrameHeader`](./transport.rs): `kind`, `stage_id`, `task_id`, `partition`, and `sender_proc`. Control frames route a `(stage_id, task_number)` address; data frames (`Batch`, `EOF`, `Cancel`, and `Chunk`) route a `(stage_id, task_id, partition)` stream.
- **Demand-Driven Pull Execution**:
  - Downstream consumers issue `ExecuteTaskFrame` to upstream producer tasks via `MppMesh::send_execute_task`.
  - Upstream worker tasks wait on `MppMesh::take_execute_task_rx(stage_num, task_idx)` for incoming execution requests before spawning task fragments.
  - Workers run the demand loop via [`run_execute_task_loop`](./setup.rs), which validates requested partition ranges (`start..end`), guards against duplicate/overlapping partition claims, listens to `CancellationToken` for prompt cancellation unwinding, and periodically drives inbound ring draining.
- **Demuxing**: Incoming frames are demuxed cooperatively by [`DrainHandle`](./transport.rs) into per-`(stage_id, task_number)` request registries and per-`(sender_proc, stage_id, task_id, partition)` record-batch buffers.

### 2. Data Plane (DSM Ring Buffers)
- Output partitions write record batches into lock-free/cache-aligned DSM ring buffers (`mpsc_ring.rs`).
- Batches are serialized as Arrow IPC streams directly into ring slots.
- One shared inbox per process multiplexes data streams; the task-aware stream key keeps tasks on the same producer process independent.

---

## Lifecycle & Memory Safety

- **Session Handles (`LeaderSession` / `WorkerSession`)**: `leader_setup` and `worker_setup` hand back `LeaderSession` and `WorkerSession` handles owning the process's outbound data senders.
- **Scope Retention**: Internal sender fields are private to prevent premature destructuring at compile time. Embedders hold `session` as a local variable for the execution scope.
- **Teardown & Detachment**: When `session` drops (on normal return, `?` error, or panic/unwind), its outbound senders drop automatically, causing peer inboxes to observe `Detached` without requiring manual cleanup calls. Embedders invoke `MppMesh::mark_detached()` when unmapping shared-memory segments to clear liveness flags.

---

## Embedder Usage & Session Lifecycles

### Worker Execution Loop (`pg_search` / parallel workers)

```rust
pub fn run_mpp_worker(...) -> Result<()> {
    // 1. Attach to shared memory region and obtain the opaque WorkerSession handle.
    let session = unsafe {
        worker_setup(base, region_total, proc_idx, wakeup, token, interrupt)?
    };

    // 2. Access session.mesh and session.plan_bytes.
    let ctx = build_session(Arc::clone(&session.mesh));

    // 3. Drive worker execution loop.
    run_execute_task_loop(&session.mesh, ...).await?;

    Ok(())
    // 4. `session` drops here on normal exit, `?` error, or panic unwind,
    //    automatically detaching peer inboxes.
}
```

### Leader Query Execution

```rust
pub async fn run_leader_query(...) -> Result<Vec<RecordBatch>> {
    // 1. Initialize DSM region and obtain LeaderSession.
    let session = unsafe {
        leader_setup(base, n_procs, queue_bytes, plan_bytes, wakeup, token, interrupt, true)?
    };

    // 2. Execute plan over session.mesh.
    let results = execute_plan(&session.mesh).await?;

    Ok(results)
    // 3. `session` drops at query completion, releasing leader control senders.
}
```

---

## Extension Points for Embedders

Embedders (such as ParadeDB / Postgres backend engines or custom runners) consume the `src/shm` API:
- **Buffer Allocation**: Standard POSIX `mmap` or Postgres Shared Memory allocations (`dsm.rs`).
- **Worker Execution Loop**: [`run_execute_task_loop`](./setup.rs) for driving worker tasks, range validation, and cancellation unwinding.
- **Wakeup Primitives**: Custom signal/wakeup hooks (`NO_RECEIVER_TOKEN`, [`Wakeup`](./mpsc_ring.rs)).
