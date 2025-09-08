# Distributed EXPLAIN ANALYZE Implementation Report

## Overview

This report outlines the implementation plan for distributed EXPLAIN ANALYZE functionality that displays per-task metrics for ExecutionPlan nodes across distributed workers. The system will capture execution metrics from each worker task and aggregate them for display in the coordinator's explain output.

## Architecture Overview

```ascii
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Coordinator   │    │     Worker       │    │  Explain Output │
│  ExecutionStage │    │  ExecutionPlan   │    │                 │
│                 │    │   with Metrics   │    │ ExecutionStage  │
│ ┌─────────────┐ │    │                  │    │ ├─ Task 0: 25ms │
│ │ArrowFlight  │◄┼────┼─ FlightData +    │    │ ├─ Task 1: 30ms │
│ │ReadExec     │ │    │   Embedded       │    │ ├─ Task 2: 22ms │
│ └─────────────┘ │    │   Metrics        │    │ └─ Task 3: 28ms │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

---

## Part 1: Storing and Displaying Per-Task Metrics in ExecutionStage

### Files and Types Involved
- **File**: `src/stage/execution_stage.rs`
  - **Type**: `ExecutionStage`
  - **Functions**: `store_task_metrics()`, `get_per_task_metrics()`, `metrics()`
- **File**: `src/stage/task_metrics.rs` (new)
  - **Type**: `TaskMetrics`, `TaskMetricsCollection`
- **File**: `src/stage/display.rs` (new)
  - **Functions**: `format_per_task_metrics()`

### Alternative 1: HashMap-Based Task Metrics Storage

```rust
// execution_stage.rs
pub struct ExecutionStage {
    // existing fields...
    per_task_metrics: Arc<Mutex<HashMap<usize, MetricsSet>>>,
    task_execution_status: Arc<Mutex<HashMap<usize, TaskStatus>>>,
}

#[derive(Clone, Debug)]
enum TaskStatus {
    Pending,
    Executing, 
    Completed(MetricsSet),
}

impl ExecutionStage {
    pub fn store_task_metrics(&self, task_index: usize, metrics: MetricsSet) {
        if let Ok(mut task_metrics) = self.per_task_metrics.lock() {
            task_metrics.insert(task_index, metrics.clone());
        }
        if let Ok(mut status) = self.task_execution_status.lock() {
            status.insert(task_index, TaskStatus::Completed(metrics));
        }
    }
}

impl DisplayAs for ExecutionStage {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "ExecutionStage: {} ({} tasks)", self.name(), self.tasks.len())?;
        
        if let Ok(task_metrics) = self.per_task_metrics.lock() {
            for (task_idx, metrics) in task_metrics.iter() {
                writeln!(f, "  Task {}: partitions {:?}", 
                    task_idx, self.tasks[*task_idx].partition_group)?;
                for metric in metrics.iter() {
                    writeln!(f, "    {}: {}", metric.name(), metric.value())?;
                }
            }
        }
        Ok(())
    }
}
```

**Explain Output:**
```
ExecutionStage: Stage 1 (4 tasks)
  Task 0: partitions [0, 1]
    output_rows: 1250
    elapsed_compute: 25ms
  Task 1: partitions [2, 3]  
    output_rows: 1100
    elapsed_compute: 30ms
```

### Alternative 2: Structured TaskMetrics Type

```rust
// task_metrics.rs
#[derive(Clone, Debug)]
pub struct TaskMetrics {
    pub task_index: usize,
    pub partition_group: Vec<u64>,
    pub worker_url: Option<String>,
    pub execution_metrics: MetricsSet,
    pub execution_time_ms: u64,
    pub rows_processed: usize,
}

#[derive(Clone, Debug)]
pub struct TaskMetricsCollection {
    metrics: HashMap<usize, TaskMetrics>,
    completion_status: HashMap<usize, bool>,
}

impl TaskMetricsCollection {
    pub fn add_task_metrics(&mut self, task_metrics: TaskMetrics) {
        self.completion_status.insert(task_metrics.task_index, true);
        self.metrics.insert(task_metrics.task_index, task_metrics);
    }
    
    pub fn is_all_tasks_complete(&self, total_tasks: usize) -> bool {
        self.completion_status.len() == total_tasks
    }
}

// execution_stage.rs  
impl ExecutionStage {
    pub fn get_task_metrics_display(&self) -> String {
        // Custom formatting logic for better display
    }
}
```

### Alternative 3: Real-time Metrics Updates with Observers

```rust
// stage/metrics_observer.rs
pub trait TaskMetricsObserver: Send + Sync {
    fn on_task_start(&self, task_index: usize, partition_group: &[u64]);
    fn on_task_progress(&self, task_index: usize, intermediate_metrics: &MetricsSet);
    fn on_task_complete(&self, task_index: usize, final_metrics: MetricsSet);
}

// execution_stage.rs
impl ExecutionStage {
    pub fn add_metrics_observer(&self, observer: Arc<dyn TaskMetricsObserver>) {
        self.metrics_observers.lock().unwrap().push(observer);
    }
}

impl DisplayAs for ExecutionStage {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        // Display with real-time status indicators
        for (idx, task) in self.tasks.iter().enumerate() {
            let status = self.get_task_status(idx);
            writeln!(f, "  Task {}: {} - partitions {:?}", 
                idx, status, task.partition_group)?;
        }
        Ok(())
    }
}
```

---

## Part 2: Sending ExecutionPlan from Worker When Stream Exhausted

### Files and Types Involved
- **File**: `src/flight_service/do_get.rs`
  - **Functions**: `get()` (modified)
- **File**: `src/flight_service/metrics_stream.rs` (new)
  - **Type**: `MetricsCapturingFlightStream`
  - **Functions**: `new()`, `poll_next()`
- **File**: `src/flight_service/protocol.rs` (new)
  - **Type**: `ExecutionPlanMessage`

### Alternative 1: Custom Stream Wrapper with Chain Pattern

```rust
// flight_service/metrics_stream.rs
pub struct MetricsCapturingFlightStream<S> {
    inner_stream: S,
    execution_plan: Arc<dyn ExecutionPlan>,
    task_metadata: TaskExecutionMetadata,
    metrics_sent: bool,
}

#[derive(Clone)]
pub struct TaskExecutionMetadata {
    pub task_index: usize,
    pub partition_group: Vec<u64>,
    pub stage_key: StageKey,
}

impl<S> Stream for MetricsCapturingFlightStream<S> 
where S: Stream<Item = Result<FlightData, Status>>
{
    type Item = Result<FlightData, Status>;
    
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.inner_stream.poll_next_unpin(cx) {
            Poll::Ready(None) if !self.metrics_sent => {
                self.metrics_sent = true;
                
                // Stream exhausted - capture metrics and send as FlightData
                let metrics_message = self.create_metrics_flight_data();
                Poll::Ready(Some(Ok(metrics_message)))
            }
            Poll::Ready(None) => Poll::Ready(None),
            other => other,
        }
    }
}

impl<S> MetricsCapturingFlightStream<S> {
    fn create_metrics_flight_data(&self) -> FlightData {
        let executed_plan_proto = self.serialize_executed_plan().unwrap();
        
        FlightData {
            flight_descriptor: Some(FlightDescriptor {
                type_: flight_descriptor::DescriptorType::Cmd as i32,
                cmd: b"TASK_EXECUTION_COMPLETE".to_vec().into(),
                path: vec![],
            }),
            data_header: executed_plan_proto.encode_to_vec().into(),
            app_metadata: self.task_metadata.encode_to_vec().into(),
            data_body: vec![].into(),
        }
    }
}

// flight_service/do_get.rs
impl ArrowFlightEndpoint {
    pub(super) async fn get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        // ... existing setup ...
        
        let metrics_stream = MetricsCapturingFlightStream::new(
            flight_data_stream,
            inner_plan.clone(),
            task_metadata,
        );
        
        Ok(Response::new(Box::pin(metrics_stream)))
    }
}
```

### Alternative 2: Event-Driven Completion Notification

```rust
// flight_service/completion_notifier.rs
pub struct StreamCompletionNotifier {
    completion_tx: mpsc::UnboundedSender<TaskCompletionEvent>,
}

#[derive(Debug, Clone)]
pub struct TaskCompletionEvent {
    pub stage_key: StageKey,
    pub task_index: usize,
    pub executed_plan: Arc<dyn ExecutionPlan>,
    pub execution_summary: ExecutionSummary,
}

// flight_service/do_get.rs
impl ArrowFlightEndpoint {
    async fn get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        let (completion_tx, completion_rx) = mpsc::unbounded_channel();
        
        let enhanced_stream = flight_data_stream
            .chain(futures::stream::once(async move {
                // Send completion event
                completion_tx.send(TaskCompletionEvent {
                    stage_key: stage_key.clone(),
                    task_index: task_number,
                    executed_plan: inner_plan.clone(),
                    execution_summary: ExecutionSummary::from_metrics(inner_plan.metrics()),
                }).ok();
                
                // Convert to FlightData
                self.completion_event_to_flight_data(completion_event).await
            }));
            
        Ok(Response::new(Box::pin(enhanced_stream)))
    }
}
```

### Alternative 3: Two-Phase Protocol with Separate Metrics RPC

```rust
// flight_service/metrics_service.rs  
#[async_trait]
pub trait TaskMetricsService {
    async fn get_task_metrics(
        &self,
        request: Request<GetTaskMetricsRequest>,
    ) -> Result<Response<GetTaskMetricsResponse>, Status>;
}

#[derive(prost::Message)]
pub struct GetTaskMetricsRequest {
    #[prost(message, optional, tag = "1")]
    pub stage_key: Option<StageKey>,
    #[prost(uint64, tag = "2")]
    pub task_index: u64,
}

#[derive(prost::Message)]
pub struct GetTaskMetricsResponse {
    #[prost(message, optional, tag = "1")]
    pub executed_plan_proto: Option<ExecutionStageProto>,
    #[prost(message, optional, tag = "2")]
    pub execution_summary: Option<ExecutionSummaryProto>,
}

// flight_service/do_get.rs
impl ArrowFlightEndpoint {
    async fn get(&self, request: Request<Ticket>) -> Result<Response<Self::DoGetStream>, Status> {
        // Normal stream processing
        let stream = create_normal_flight_stream(inner_plan, state)?;
        
        // Register completion callback
        self.register_task_completion(stage_key.clone(), task_number, inner_plan.clone()).await;
        
        Ok(Response::new(stream))
    }
    
    async fn register_task_completion(&self, stage_key: StageKey, task_index: u64, plan: Arc<dyn ExecutionPlan>) {
        // Store executed plan for later retrieval via get_task_metrics RPC
        self.completed_tasks.insert((stage_key, task_index), plan).await;
    }
}
```

---

## Part 3: Custom Decoder on Client Side

### Files and Types Involved
- **File**: `src/plan/arrow_flight_read.rs`
  - **Functions**: `execute()`, `stream_from_stage_task()` (modified)
- **File**: `src/plan/mixed_message_decoder.rs` (new)
  - **Type**: `MixedMessageFlightStream`
  - **Functions**: `new()`, `poll_next()`
- **File**: `src/plan/metrics_aggregator.rs` (new)
  - **Type**: `TaskMetricsAggregator`
  - **Functions**: `add_task_metrics()`, `get_aggregated_metrics()`

### Alternative 1: Mixed Message Stream with Filter/Decode Pattern

```rust
// plan/mixed_message_decoder.rs
pub struct MixedMessageFlightStream {
    flight_stream: Pin<Box<dyn Stream<Item = Result<FlightData, FlightError>> + Send>>,
    schema: Option<SchemaRef>,
    metrics_callback: Box<dyn Fn(TaskExecutionMetadata, Arc<ExecutionStage>) + Send>,
}

impl Stream for MixedMessageFlightStream {
    type Item = Result<RecordBatch, DataFusionError>;
    
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match self.flight_stream.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(flight_data))) => {
                    if self.is_metrics_message(&flight_data) {
                        self.process_metrics_message(flight_data);
                        continue; // Skip metrics messages in RecordBatch stream
                    }
                    
                    // Decode as normal RecordBatch
                    match self.decode_record_batch(flight_data) {
                        Ok(batch) => return Poll::Ready(Some(Ok(batch))),
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                Poll::Ready(Some(Err(e))) => {
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(e)))));
                }
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

impl MixedMessageFlightStream {
    fn is_metrics_message(&self, flight_data: &FlightData) -> bool {
        flight_data.flight_descriptor
            .as_ref()
            .map(|desc| desc.cmd == b"TASK_EXECUTION_COMPLETE".to_vec())
            .unwrap_or(false)
    }
    
    fn process_metrics_message(&mut self, flight_data: FlightData) {
        if let Ok(executed_plan_proto) = ExecutionStageProto::decode(flight_data.data_header.as_ref()) {
            if let Ok(task_metadata) = TaskExecutionMetadata::decode(flight_data.app_metadata.as_ref()) {
                let executed_stage = self.deserialize_execution_stage(executed_plan_proto);
                (self.metrics_callback)(task_metadata, executed_stage);
            }
        }
    }
}

// plan/arrow_flight_read.rs  
async fn stream_from_stage_task(...) -> Result<SendableRecordBatchStream, DataFusionError> {
    let response = client.do_get(ticket).await?;
    let flight_stream = response.into_inner();
    
    let metrics_callback = {
        let executed_plans = executed_plans.clone();
        let stage_key = stage_key.clone();
        
        move |task_metadata: TaskExecutionMetadata, executed_stage: Arc<ExecutionStage>| {
            let key = (stage_key.query_id.clone(), task_metadata.task_index as u64);
            if let Ok(mut plans) = executed_plans.lock() {
                plans.insert(key, executed_stage);
            }
        }
    };
    
    let mixed_stream = MixedMessageFlightStream::new(flight_stream, schema.clone(), Box::new(metrics_callback));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, mixed_stream)))
}
```

### Alternative 2: State Machine-Based Decoder

```rust
// plan/stateful_decoder.rs
#[derive(Debug)]
enum DecoderState {
    ReadingSchema,
    ReadingData,
    ReadingMetrics,
    Complete,
}

pub struct StatefulFlightDecoder {
    state: DecoderState,
    flight_stream: Pin<Box<dyn Stream<Item = Result<FlightData, FlightError>> + Send>>,
    schema: Option<SchemaRef>,
    pending_batches: VecDeque<RecordBatch>,
    metrics_collector: TaskMetricsCollector,
}

impl Stream for StatefulFlightDecoder {
    type Item = Result<RecordBatch, DataFusionError>;
    
    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Return any pending batches first
        if let Some(batch) = self.pending_batches.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }
        
        match self.flight_stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(flight_data))) => {
                match self.state {
                    DecoderState::ReadingData => {
                        if self.is_metrics_message(&flight_data) {
                            self.state = DecoderState::ReadingMetrics;
                            self.process_metrics(flight_data);
                            self.state = DecoderState::Complete;
                            return self.poll_next(cx); // Continue processing
                        }
                        // Process as RecordBatch
                        self.decode_and_queue_batch(flight_data)
                    }
                    _ => self.handle_state_transition(flight_data)
                }
            }
            other => self.handle_stream_event(other)
        }
    }
}
```

### Alternative 3: Reactive Streams with Message Routing

```rust
// plan/message_router.rs
pub struct FlightMessageRouter {
    data_sink: mpsc::UnboundedSender<FlightData>,
    metrics_sink: mpsc::UnboundedSender<TaskMetricsMessage>,
}

impl FlightMessageRouter {
    pub fn route_message(&self, flight_data: FlightData) {
        if self.is_metrics_message(&flight_data) {
            let metrics_msg = self.extract_metrics_message(flight_data);
            self.metrics_sink.send(metrics_msg).ok();
        } else {
            self.data_sink.send(flight_data).ok();
        }
    }
}

// plan/reactive_decoder.rs
pub struct ReactiveFlightDecoder {
    data_stream: ReceiverStream<FlightData>,
    metrics_stream: ReceiverStream<TaskMetricsMessage>,
    record_batch_decoder: FlightRecordBatchDecoder,
    metrics_aggregator: Arc<Mutex<TaskMetricsAggregator>>,
}

impl ReactiveFlightDecoder {
    pub fn new(flight_stream: impl Stream<Item = Result<FlightData, FlightError>> + Send + 'static) -> (Self, TaskMetricsReceiver) {
        let (data_tx, data_rx) = mpsc::unbounded_channel();
        let (metrics_tx, metrics_rx) = mpsc::unbounded_channel();
        
        let router = FlightMessageRouter { data_sink: data_tx, metrics_sink: metrics_tx };
        
        tokio::spawn(async move {
            flight_stream.for_each(|flight_data_result| async {
                if let Ok(flight_data) = flight_data_result {
                    router.route_message(flight_data);
                }
            }).await;
        });
        
        let decoder = Self {
            data_stream: ReceiverStream::new(data_rx),
            metrics_stream: ReceiverStream::new(metrics_rx),
            record_batch_decoder: FlightRecordBatchDecoder::new(),
            metrics_aggregator: Arc::new(Mutex::new(TaskMetricsAggregator::new())),
        };
        
        (decoder, TaskMetricsReceiver::new(decoder.metrics_aggregator.clone()))
    }
}
```

---

## Implementation Recommendation

### Recommended Approach

**Part 1**: Alternative 1 (HashMap-based) for simplicity and immediate implementation  
**Part 2**: Alternative 1 (Custom Stream Wrapper) for seamless integration  
**Part 3**: Alternative 1 (Mixed Message Stream) for clean separation of concerns  

### Implementation Order

1. **Phase 1**: Implement basic per-task metrics storage in ExecutionStage
2. **Phase 2**: Add metrics capture in ArrowFlightEndpoint stream completion
3. **Phase 3**: Implement mixed message decoder in ArrowFlightReadExec
4. **Phase 4**: Enhanced display formatting and aggregation
5. **Phase 5**: Performance optimization and error handling

### Key Benefits

- **Per-task visibility**: Debug performance issues at task granularity
- **Worker attribution**: See which workers are performing poorly  
- **Partition-level insights**: Understand data distribution effects
- **Backward compatibility**: Maintains existing DataFusion ExecutionPlan interface

```ascii
Final Output Example:
┌─────────────────────────────────────────────────────────────┐
│ EXPLAIN ANALYZE - Distributed Query Execution              │
├─────────────────────────────────────────────────────────────┤
│ ExecutionStage: Stage 1 (4 tasks) - 105ms total            │
│ ├─ Task 0: worker-1 partitions [0,1] - 25ms                │
│ │  ├─ output_rows: 1,250                                   │
│ │  ├─ elapsed_compute: 22ms                                │
│ │  └─ spill_count: 0                                       │
│ ├─ Task 1: worker-2 partitions [2,3] - 30ms                │
│ │  ├─ output_rows: 1,100                                   │
│ │  ├─ elapsed_compute: 28ms                                │
│ │  └─ spill_count: 0                                       │
│ └─ ExecutionStage: Stage 2 (2 tasks) - 45ms total          │
│    ├─ Task 0: worker-3 partitions [0] - 20ms               │
│    └─ Task 1: worker-4 partitions [1] - 25ms               │
└─────────────────────────────────────────────────────────────┘
```