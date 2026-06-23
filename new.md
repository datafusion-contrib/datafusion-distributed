# What Is Going Wrong

What is going wrong is a lifecycle mismatch between distributed task ranges
and DataFusion `RepartitionExec`'s all-output-partitions state.

In the failing repro:

```text
Stage 2 task 0: owns producer partitions p0..p3
Stage 2 task 1: owns producer partitions p4..p7

Stage 1 producer task:
  RepartitionExec output partitions p0..p7
```

Rank 5 does this:

1. Stage 2 task 0 creates streams for `p0..p3`.
2. It drops them before any stream is polled.
3. Stage 2 task 1 fully drains `p4..p7`.
4. The query-side stream can complete.
5. Stage 1 producer task is still retained:
   `tasks_running=3, tracked drops=0/3`.

The bug is that the producer `TaskDataEntry` only cleans up when all output
partitions have dropped or completed. That happens in `execute_local_task`:
every requested partition stream gets an `on_drop_stream`, and only the final
drop invalidates the task entry:

```rust
// src/worker/impl_execute_task.rs
let stream = on_drop_stream(stream, move || {
    if num_partitions_remaining.fetch_sub(1, Ordering::SeqCst) == 1 {
        tokio::spawn(async move {
            task_data_entries.invalidate(&key).await;
        });
        task_data_metrics.mark_execution_finished();
        if send_metrics {
            send_metrics_via_channel(&metrics_tx, &plan, d_ctx, &task_data_metrics);
        }
    }
});
```

If an upper task dies before its range is truly consumed or dropped on the
producer, those partitions never decrement the producer's remaining-partitions
counter to zero. So the producer `TaskDataEntry` keeps the `final_plan`, which
keeps the `RepartitionExec`, which keeps the child plan and repartition state
alive.

The `RepartitionExec` detail is important. When any output partition is
executed, it initializes input streams and creates channels for all output
partitions, not just the requested one. It also spawns the input pull tasks and
stores their abort helpers in shared repartition state:

```rust
// datafusion-physical-plan/src/repartition/mod.rs
for (partition, (tx, rx)) in txs.into_iter().zip(rxs).enumerate() {
    channels.insert(
        partition,
        PartitionChannels {
            tx,
            rx,
            reservation,
            spill_readers,
            spill_writers,
            shared_coalescer,
        },
    );
}

let input_task = SpawnedTask::spawn(RepartitionExec::pull_from_input(
    stream,
    txs,
    partitioning.clone(),
    metrics,
    if preserve_order { 0 } else { i },
    num_input_partitions,
));

let wait_for_task =
    SpawnedTask::spawn(RepartitionExec::wait_for_task(input_task, senders));
spawned_tasks.push(wait_for_task);

*self = Self::ConsumingInputStreams(ConsumingInputStreamsState {
    channels,
    abort_helper: Arc::new(spawned_tasks),
});
```

So one sibling can finish `p4..p7`, but that does not imply `p0..p3` were ever
cleaned up.

`NetworkShuffleExec` splits the producer partitions by upper task using:

```rust
let off = output_partitions * task_index;
off..(off + output_partitions)
```

That is the distributed range split. The failing case is specifically the
sibling range split: one range is dropped before polling, the other range
completes.

`spawn_select_all` is involved, but it is not the whole bug. It spawns one task
per requested stream and keeps those tasks alive as long as the returned merged
stream lives:

```rust
// src/worker/spawn_select_all.rs
tasks.push(SpawnedTask::spawn(async move {
    loop {
        let msg = tokio::select! {
            biased;
            _ = in_tx.closed() => return,
            msg = t.next() => msg
        };
        let Some(msg) = msg else { return };

        if in_tx.send(msg).await.is_err() {
            return;
        };
    }
}));

futures::stream::select_all(in_rxs).map(move |msg| {
    let _ = &tasks;
    msg
})
```

Also the comment says "send timeout", but the code just awaits
`in_tx.send(msg)`; there is no timeout. That can make stalled streams worse,
but the reproduced leak is fundamentally that producer task cleanup is
partition-count based and no global query/stage cancellation invalidates the
producer when an upper sibling disappears before polling.

In short: a query can complete from the coordinator's perspective while a
lower-stage producer task is still retained because not all producer output
partitions were driven to drop or completion.

## Why Streams Can Be Created And Dropped Before Polling

Creating a DataFusion stream is not the same as polling it. Most of these APIs
return a lazy stream object. The stream object can allocate state, register drop
handlers, and sometimes initialize shared operator state, but the actual data
pull usually starts only when `poll_next` is called.

The distributed code has several places where it creates streams first and only
polls later.

In `NetworkShuffleExec::execute`, each output partition creates or reuses a
`RemoteWorkerConnection`, then calls `worker_connection.execute(...)`:

```rust
// src/execution_plans/network_shuffle.rs
let worker_connection = self.worker_connections.get_or_init_worker_connection(
    remote_stage,
    off..(off + self.properties.partitioning.partition_count()),
    input_task_index,
    self.producer_head(task_context.task_count),
    &context,
)?;

let stream = worker_connection.execute(off + partition)?;
streams.push(stream);
```

At that point the stream object exists. It may never be polled if the parent
operator builds several child streams and then errors, short-circuits, is
cancelled, or drops the plan before it starts driving those streams.

The same pattern exists inside the worker when a remote task request arrives.
`execute_local_task` eagerly creates every requested partition stream:

```rust
// src/worker/impl_execute_task.rs
for partition in body.target_partition_start..body.target_partition_end {
    let stream = plan.execute(partition as usize, Arc::clone(&task_ctx))?;
    let stream_schema = plan.schema();
    let stream = on_drop_stream(stream, move || {
        // cleanup accounting
    });
    streams.push(Box::pin(RecordBatchStreamAdapter::new(stream_schema, stream)) as _);
}
```

Then `execute_remote_task` wraps those streams as Flight streams and passes
them to `spawn_select_all`:

```rust
// src/worker/impl_execute_task.rs
for (partition, arrow_stream) in partition_range.zip(arrow_streams) {
    let flight_stream = build_flight_data_stream(arrow_stream, compression)?.map(move |msg| {
        msg.map(|v| v.with_app_metadata(flight_data.encode_to_vec()))
    });

    flight_streams.push(flight_stream);
}

let stream = spawn_select_all(flight_streams, memory_pool, RECORD_BATCH_BUFFER_SIZE);
```

So a worker can construct a whole set of per-partition streams before the
client actually polls the response stream.

There is also an explicit first-poll gate in `RemoteWorkerConnection`. The
background network task is held inactive until one returned partition stream is
first polled:

```rust
// src/worker/worker_connection_pool.rs
let first_poll_notify = Arc::new(Notify::new());

let stream = async move {
    first_poll_notify.notify_one();
    UnboundedReceiverStream::new(partition_receiver)
}
.flatten_stream();
```

The demux task waits here:

```rust
// src/worker/worker_connection_pool.rs
tokio::select! {
    biased;
    _ = cancel.cancelled() => {
        let _ = client.execute_task(request).await;
        return
    },
    _ = first_poll_notify_for_task.notified() => {}
}
```

This means a stream can be created and dropped before polling in normal Rust
control flow:

1. The parent calls `execute(...)` and receives a stream.
2. The stream is put in a `Vec`, a join set, a `select_all`, or an operator
   field.
3. Before `poll_next` happens, the parent hits an error, cancellation,
   early-return, short-circuit, timeout, or sibling branch completion.
4. Rust drops the stream object without ever polling it.

That is not inherently wrong. Dropping an unpolled stream is allowed. The
problem is that our producer-side cleanup assumes every producer output
partition stream will either be polled to EOF or dropped in a way that decrements
the producer task's remaining-partitions counter. The failing repro shows a
distributed sibling range can disappear before polling while another sibling
range completes, leaving the producer task retained.

## Non-Error Case: A Stream Exists But Is Not Polled

The non-error case is normal pull-based execution, not a panic or failed query.
DataFusion plans are demand-driven: `ExecutionPlan::execute(partition)` returns
a stream object, and actual work happens later when someone polls that stream.
It is valid for an operator to create multiple streams first and then poll them
later, or poll only the streams needed to satisfy its own output.

The distributed shuffle makes this more subtle because one upper-stage task owns
a contiguous range of lower-stage producer partitions.

For the production-shaped plan:

```text
Stage 1 producer tasks:
  each task exposes p0..p1739 after RepartitionExec

Stage 2:
  60 tasks
  each task has 29 local output partitions p0..p28

Mapping:
  Stage 2 task 0 reads Stage 1 producer p0..p28
  Stage 2 task 1 reads Stage 1 producer p29..p57
  Stage 2 task 2 reads Stage 1 producer p58..p86
  ...
  Stage 2 task 59 reads Stage 1 producer p1711..p1739
```

That mapping comes from `NetworkShuffleExec`:

```rust
// src/execution_plans/network_shuffle.rs
let off = self.properties.partitioning.partition_count() * task_context.task_index;

let worker_connection = self.worker_connections.get_or_init_worker_connection(
    remote_stage,
    off..(off + self.properties.partitioning.partition_count()),
    input_task_index,
    self.producer_head(task_context.task_count),
    &context,
)?;

let stream = worker_connection.execute(off + partition)?;
```

In a completely healthy run of this aggregate plan, every Stage 2 task should
eventually drive all 29 of its local partitions, because the final
`CoalescePartitionsExec` / `NetworkCoalesceExec` path needs all Stage 2 outputs
to compute the final result. If that happens, all Stage 1 producer partitions
`p0..p1739` are either polled to EOF or dropped, and the producer task can clean
up.

The leak requires a deviation from that healthy lifecycle, but not necessarily
an error visible to the user. The user-visible query can finish if some higher
level consumer decides it has enough output, or if a stage-level stream is
dropped as part of normal completion/cancellation, while another sibling range
successfully finishes.

The non-error shape looks like this:

```text
Stage 2 task 17 starts.
It owns Stage 1 producer partitions p493..p521.

The worker constructs stream objects for that range.
Those stream objects install drop handlers and can initialize repartition state.

Before those streams are first polled, the parent no longer needs task 17's
range. This can happen because the parent completed, short-circuited, or dropped
the response stream as part of normal shutdown.

Stage 2 task 18, a sibling, still runs normally and drains p522..p550.

From the outside, there may be no error. Some stage/query stream finished.
But the Stage 1 producer task still believes some output partitions are live or
unaccounted, so its `TaskDataEntry` and `RepartitionExec` stay alive.
```

This can happen without an error because "not polled yet" is not an error in
DataFusion. It is just a stream object waiting for demand. Examples of normal
non-error reasons demand may stop before a stream is polled:

1. A parent operator creates several child streams, then only polls a subset
   first. The others are alive but have not yet received `poll_next`.
2. A final consumer stops early because a plan has a semantic early-exit, such
   as a `LIMIT`, `FETCH`, `EXISTS`, or another operator that can produce its
   result without draining every input.
3. A distributed response stream is created for a whole partition range, but the
   downstream side stops polling the merged response before all per-partition
   producer tasks have made progress.
4. Backpressure parks a stream before first poll. This is normally temporary,
   but if the owning response stream is kept alive after the query is considered
   complete, it becomes a retained-task bug.

For the pasted aggregate plan, case 2 should not be the intended behavior: the
aggregate should need all Stage 2 outputs. Therefore, if production shows the
query completing successfully while stale Stage 1 `RepartitionExec` tasks remain,
the suspicious non-error path is not "the SQL did not need those partitions".
It is more likely that some distributed stage/task lifecycle marked the query or
parent stream complete without driving every expected Stage 2 task partition
range to either EOF or an explicit drop/cancel signal on the Stage 1 producer.

The important distinction:

```text
Normal transient:
  stream exists
  stream is not polled yet
  later it is polled or dropped
  producer cleanup eventually runs

Leak:
  stream exists or producer partition is accounted as live
  stream is not polled
  query/stage is considered complete
  no later poll/drop/cancel reaches the producer
  producer TaskDataEntry keeps final_plan alive
```

## The Specific Failure Shape

The concrete failure is:

```text
one upper task creates streams for its assigned producer partition range
that upper task drops those streams before first poll
another upper task drains its sibling producer partition range to completion
the coordinator-visible query stream completes
the lower producer task still has live TaskDataEntry/final_plan state
the RepartitionExec child is not dropped
```

That matches the repro result:

```text
rank 5 drop_consumer_before_poll_sibling_completes_hash_join
producer tasks_running=3, tracked drops=0/3
```

The important part is not that `SpawnedTask::drop` fails to abort. It does
abort when the owning streams/drop helpers are dropped. The problem is that the
producer task and repartition state are still owned through `TaskDataEntry` and
through live or unaccounted output partition lifecycle state, so the drop never
happens.
