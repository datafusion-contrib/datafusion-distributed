// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Transport layer for MPP shuffle.
//!
//! - [`MppFrameHeader`]: fixed 20-byte prefix tagging each wire message with
//!   `(stage_id, task_id, partition)`, so one queue carries frames for many logical channels.
//! - [`encode_frame_into`] / [`decode_frame`]: Arrow IPC serialize/deserialize with
//!   header prefix. Only codec entry points; tests round-trip through the same path.
//! - [`DrainBuffer`]: per-proc queue the drain writes into and the DataFusion consumer
//!   reads from. Decouples consumer-side from producer-side backpressure: the drain
//!   always makes forward progress on the inbound rings, so a stalled consumer can't
//!   propagate backpressure to remote producers and cause a peer-mesh stall.

use async_trait::async_trait;
use std::collections::VecDeque;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

use datafusion::common::{HashMap, HashSet};
use std::time::{Duration, Instant};

use datafusion::arrow::array::{
    ArrayRef, BinaryViewArray, RecordBatch, StringViewArray, UInt64Array,
};
use datafusion::arrow::compute::take;
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::ipc::writer::StreamWriter;
use datafusion::common::DataFusionError;
use prost::Message;

use crate::common::deserialize_uuid;
use crate::proto as pb;
use crate::work_unit_feed::RemoteWorkUnitFeedTxs;
use crate::{MaybeEncoded, WorkUnitMsg, set_received_time};

/// Magic bytes "MPPF" (MPP Frame) at the start of every wire message.
/// Lets receivers reject misrouted / corrupt frames before they hit Arrow IPC.
const MPP_FRAME_MAGIC: u32 = 0x4D505046;

/// Wire-format size of [`MppFrameHeader`] in bytes. Asserted at compile time
/// below via `const _: ()`.
const MPP_FRAME_HEADER_SIZE: usize = 20;

/// Kind of payload following [`MppFrameHeader`].
///
/// `Batch` is the common case. The header is followed by an Arrow IPC stream containing one
/// `RecordBatch`. `Eof` carries no payload. It signals the receiver that the named
/// `(stage_id, task_id, partition)` channel is done, even though the underlying shm_mq queue may still
/// carry frames for other channels.
///
/// The remaining kinds are the control plane riding the same rings. For them the header's
/// `partition` field carries the task number instead: a work unit already names its
/// `(feed id, partition)` inside the payload, and metrics describe a whole task.
/// `SetPlan` (leader -> worker) carries one task's [`pb::SetPlanRequest`], the same message
/// Flight ships over its coordinator stream, plus the propagation headers that ride gRPC
/// metadata there; `WorkUnit` (leader -> worker) carries one prost-encoded work unit for
/// `(stage, task)`; `FeedEof` closes that task's feed channels (the wire stand-in for Flight's
/// stream close); `TaskMetrics` (worker -> leader) carries the task's collected metrics.
#[repr(u32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MppFrameKind {
    Batch = 0,
    Eof = 1,
    WorkUnit = 2,
    FeedEof = 3,
    TaskMetrics = 4,
    SetPlan = 5,
    /// Consumer -> producer: stop producing the `(stage_id, task_id, partition)` stream, the consumer
    /// stopped reading it before EOF (a top-N `LIMIT`, an inner merge join exhausting a side, etc.).
    /// The producer ends that one stream cleanly. Scoped to the stream, not the connection, so the
    /// ring stays healthy for metrics and every other stream, the way gRPC closes one stream
    /// without dropping the channel.
    Cancel = 6,
    /// One piece of a frame too large for the channel's frame bound. The payload is a
    /// `[total_len: u64][offset: u64]` prefix followed by that byte range of the inner frame.
    /// The receiver reassembles per `(sender_proc, stage_id, task_id, partition)` and decodes the inner
    /// frame once `total_len` bytes have arrived, so no single ring-resident message ever
    /// exceeds the ring and frame size is never an error.
    Chunk = 7,
    ExecuteTask = 8,
    /// Carries a worker's unrecoverable error (e.g. a panic or `DataFusionError`) string
    /// to the consumer, so it sees the actual crash reason instead of a
    /// detached channel.
    TaskError = 9,
}

/// Payload of a `SetPlan` frame: the plan-delivery message a worker needs to run one task,
/// byte-compatible with what Flight sends.
///
/// `set_plan` is the exact [`pb::SetPlanRequest`] the Flight dispatcher would put on its gRPC
/// stream. The headers are the config-extension propagation headers that travel as gRPC metadata
/// there; the ring has no metadata side channel, so they ride inside the frame as parallel
/// key/value lists (parallel lists rather than a map so repeated header names survive).
#[derive(Clone, PartialEq, prost::Message)]
pub struct SetPlanFrame {
    #[prost(message, optional, tag = "1")]
    pub set_plan: Option<pb::SetPlanRequest>,
    #[prost(string, repeated, tag = "2")]
    pub header_keys: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    pub header_values: Vec<String>,
}

impl SetPlanFrame {
    /// Bundle a plan-delivery message with the headers Flight would carry as gRPC metadata.
    pub fn from_parts(
        set_plan: pb::SetPlanRequest,
        headers: &http::HeaderMap,
    ) -> Result<Self, DataFusionError> {
        let mut header_keys = Vec::with_capacity(headers.len());
        let mut header_values = Vec::with_capacity(headers.len());
        for (name, value) in headers {
            let value = value.to_str().map_err(|e| {
                DataFusionError::Internal(format!(
                    "mpp: non-ASCII header {name} cannot travel in a SetPlan frame: {e}"
                ))
            })?;
            header_keys.push(name.as_str().to_string());
            header_values.push(value.to_string());
        }
        Ok(Self {
            set_plan: Some(set_plan),
            header_keys,
            header_values,
        })
    }

    /// Split back into the plan-delivery message and the propagation headers.
    pub fn into_parts(self) -> Result<(pb::SetPlanRequest, http::HeaderMap), DataFusionError> {
        let set_plan = self.set_plan.ok_or_else(|| {
            DataFusionError::Internal("mpp: SetPlan frame carries no SetPlanRequest".to_string())
        })?;
        let mut headers = http::HeaderMap::with_capacity(self.header_keys.len());
        for (key, value) in self.header_keys.iter().zip(self.header_values.iter()) {
            let name = http::header::HeaderName::from_bytes(key.as_bytes()).map_err(|e| {
                DataFusionError::Internal(format!("mpp: SetPlan frame header name {key:?}: {e}"))
            })?;
            let value = http::header::HeaderValue::from_str(value).map_err(|e| {
                DataFusionError::Internal(format!("mpp: SetPlan frame header value for {key}: {e}"))
            })?;
            headers.append(name, value);
        }
        Ok((set_plan, headers))
    }
}

/// Payload of an `ExecuteTask` frame: the request a consumer uses to ask a producer
/// to evaluate a specific partition range.
/// Control frame wrapping a [`pb::ExecuteTaskRequest`] and pass-through HTTP headers sent over
/// the shared-memory control mesh.
#[derive(Clone, PartialEq, prost::Message)]
pub struct ExecuteTaskFrame {
    #[prost(message, optional, tag = "1")]
    pub request: Option<pb::ExecuteTaskRequest>,
    #[prost(string, repeated, tag = "2")]
    pub header_keys: Vec<String>,
    #[prost(string, repeated, tag = "3")]
    pub header_values: Vec<String>,
}

impl ExecuteTaskFrame {
    pub fn from_parts(
        request: pb::ExecuteTaskRequest,
        headers: &http::HeaderMap,
    ) -> Result<Self, DataFusionError> {
        let mut header_keys = Vec::with_capacity(headers.len());
        let mut header_values = Vec::with_capacity(headers.len());
        for (name, value) in headers {
            let value = value.to_str().map_err(|e| {
                DataFusionError::Internal(format!(
                    "mpp: non-ASCII header {name} cannot travel in a ExecuteTask frame: {e}"
                ))
            })?;
            header_keys.push(name.as_str().to_string());
            header_values.push(value.to_string());
        }
        Ok(Self {
            request: Some(request),
            header_keys,
            header_values,
        })
    }

    pub fn into_parts(self) -> Result<(pb::ExecuteTaskRequest, http::HeaderMap), DataFusionError> {
        let request = self.request.ok_or_else(|| {
            DataFusionError::Internal(
                "mpp: ExecuteTask frame carries no ExecuteTaskRequest".to_string(),
            )
        })?;
        let mut headers = http::HeaderMap::with_capacity(self.header_keys.len());
        for (key, value) in self.header_keys.iter().zip(self.header_values.iter()) {
            let name = http::header::HeaderName::from_bytes(key.as_bytes()).map_err(|e| {
                DataFusionError::Internal(format!(
                    "mpp: ExecuteTask frame header name {key:?}: {e}"
                ))
            })?;
            let value = http::header::HeaderValue::from_str(value).map_err(|e| {
                DataFusionError::Internal(format!(
                    "mpp: ExecuteTask frame header value for {key}: {e}"
                ))
            })?;
            headers.append(name, value);
        }
        Ok((request, headers))
    }
}

/// Identity of one task's output partition within a distributed stage.
///
/// Batches, EOF, cancellation, and data chunks must use this key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MppDataStreamKey {
    pub stage_id: u32,
    pub task_id: u32,
    pub partition: u32,
}

impl MppDataStreamKey {
    pub const fn new(stage_id: u32, task_id: u32, partition: u32) -> Self {
        Self {
            stage_id,
            task_id,
            partition,
        }
    }
}

/// One data stream as it appears on a receiver's shared inbox.
///
/// `sender_proc` distinguishes producers sharing an inbox.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct PhysicalStreamKey {
    sender_proc: u32,
    stream: MppDataStreamKey,
}

impl PhysicalStreamKey {
    const fn new(sender_proc: u32, stream: MppDataStreamKey) -> Self {
        Self {
            sender_proc,
            stream,
        }
    }
}

/// 20-byte prefix on every transport frame.
///
/// The fixed layout `[magic, flags, stage_id, task_id, partition]` (5×u32) is what
/// senders prepend before the Arrow IPC stream bytes and what receivers
/// parse before deciding which channel buffer the payload belongs to.
///
/// See the `flags` bit-layout block below for the encoding of the `flags` word.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MppFrameHeader {
    // Private so headers only come out of constructors: hand-built ones could bypass
    // `pack_flags`'s sender bound and the reserved-bits invariant, and the consumer would
    // reject them at decode, far from the producer.
    pub(super) magic: u32,
    pub(super) flags: u32,
    pub(super) stage_id: u32,
    /// Logical task that produced a data stream. Task-control frames leave this zero because
    /// their existing `partition` slot encodes the target task number.
    pub(super) task_id: u32,
    pub(super) partition: u32,
}

/// `flags` bit layout:
///   bits  0..8:  frame kind (Batch | Eof)
///   bits  8..16: reserved (must be 0)
///   bits 16..32: sender_proc (mesh peer that wrote the frame)
const FRAME_KIND_MASK: u32 = 0x0000_00FF;
const FRAME_RESERVED_MASK: u32 = 0x0000_FF00;
const FRAME_SENDER_SHIFT: u32 = 16;
/// Maximum `sender_proc` representable in the header. Asserted at construction time so an
/// overflow becomes a hard error in the producer rather than silent flag corruption on the wire.
pub const MPP_MAX_SENDER_PROC: u32 = 0xFFFF;

/// Spin bound for landing a control or metrics frame on a peer inbox that may be momentarily full.
/// A live peer drains its inbox within this many tries, so the frame lands well inside the bound;
/// it only runs out if the peer already exited, where the leader's wait-for-workers surfaces the
/// failure instead of hanging here.
const MAX_CONTROL_SEND_SPINS: usize = 10_000;

const _: () = {
    // shm_mq slot layout calculations depend on this being exact.
    assert!(std::mem::size_of::<MppFrameHeader>() == MPP_FRAME_HEADER_SIZE);
};

#[inline]
fn pack_flags(kind: MppFrameKind, sender_proc: u32) -> u32 {
    // fail_loud rather than debug_assert: in release builds the check would be compiled out and
    // an out-of-range value would silently truncate to `sender_proc & 0xFFFF`. Catching the case
    // where a refactor accidentally passes a task_id or partition here is the whole point.
    assert!(
        sender_proc <= MPP_MAX_SENDER_PROC,
        "mpp: sender_proc {sender_proc} > MPP_MAX_SENDER_PROC ({MPP_MAX_SENDER_PROC})"
    );
    (kind as u32) | (sender_proc << FRAME_SENDER_SHIFT)
}

impl MppFrameHeader {
    /// Build a `Batch` header for one data stream stamped with `sender_proc`.
    pub fn batch(stream: MppDataStreamKey, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::Batch, sender_proc),
            stage_id: stream.stage_id,
            task_id: stream.task_id,
            partition: stream.partition,
        }
    }

    /// Build an `Eof` header for the given data stream stamped with `sender_proc`.
    /// Carries no payload; receivers route it to the channel buffer's source-done counter.
    pub fn eof(stream: MppDataStreamKey, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::Eof, sender_proc),
            stage_id: stream.stage_id,
            task_id: stream.task_id,
            partition: stream.partition,
        }
    }

    /// Build a `WorkUnit` header addressed to `(stage_id, task_number)`. The `partition` slot
    /// carries the task number; the unit's own `(feed id, partition)` ride in the payload.
    pub fn work_unit(stage_id: u32, task_number: u32, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::WorkUnit, sender_proc),
            stage_id,
            task_id: 0,
            partition: task_number,
        }
    }

    /// Build a `FeedEof` header for `(stage_id, task_number)`: every feed of that task is done.
    pub fn feed_eof(stage_id: u32, task_number: u32, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::FeedEof, sender_proc),
            stage_id,
            task_id: 0,
            partition: task_number,
        }
    }

    /// Build a `TaskMetrics` header for `(stage_id, task_number)`.
    pub fn task_metrics(stage_id: u32, task_number: u32, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::TaskMetrics, sender_proc),
            stage_id,
            task_id: 0,
            partition: task_number,
        }
    }

    /// Build a `TaskError` header destined for the consumer of `(stage_id, task_number)`.
    pub fn task_error(stage_id: u32, partition: u32, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::TaskError, sender_proc),
            stage_id,
            task_id: 0,
            partition,
        }
    }

    /// Build a `SetPlan` header for `(stage_id, task_number)`: the frame delivers that task's
    /// plan to the proc hosting it.
    pub fn set_plan(stage_id: u32, task_number: u32, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::SetPlan, sender_proc),
            stage_id,
            task_id: 0,
            partition: task_number,
        }
    }

    /// Build an `ExecuteTask` header for `(stage_id, task_number)`.
    pub fn execute_task(stage_id: u32, task_number: u32, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::ExecuteTask, sender_proc),
            stage_id,
            task_id: 0,
            partition: task_number,
        }
    }

    /// Build a `Cancel` header for a data stream, stamped with the consumer's `sender_proc`.
    /// Carries no payload; the producer reads it as "stop sending this stream."
    pub fn cancel(stream: MppDataStreamKey, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::Cancel, sender_proc),
            stage_id: stream.stage_id,
            task_id: stream.task_id,
            partition: stream.partition,
        }
    }

    /// Build a `Chunk` header for one piece of an oversized frame. The data stream mirrors the
    /// inner frame's addressing: together with `sender_proc` they key the
    /// receiver's reassembly, so chunked frames from two streams of the same proc can
    /// interleave without corrupting each other.
    pub fn chunk(stream: MppDataStreamKey, sender_proc: u32) -> Self {
        Self {
            magic: MPP_FRAME_MAGIC,
            flags: pack_flags(MppFrameKind::Chunk, sender_proc),
            stage_id: stream.stage_id,
            task_id: stream.task_id,
            partition: stream.partition,
        }
    }

    /// Returns the data-stream address carried by a `Batch`, `Eof`, `Cancel`, or `Chunk` frame.
    /// Control frames are a distinct tagged layout and keep their task address in `partition`.
    pub(super) const fn data_stream(&self) -> MppDataStreamKey {
        MppDataStreamKey::new(self.stage_id, self.task_id, self.partition)
    }

    /// The mesh peer that wrote this frame. The drain demuxes incoming frames into the
    /// per-channel buffer registry by `(sender_proc, stage_id, task_id, partition)`.
    pub fn sender_proc(&self) -> u32 {
        (self.flags >> FRAME_SENDER_SHIFT) & 0xFFFF
    }

    /// Read the kind out of `flags`. Returns an error if the kind byte is
    /// unknown or if any reserved bit (bits 8..16) is set, which catches wire-format
    /// drift early. Sender_proc bits (16..32) are not validated here; readers extract
    /// them with `sender_proc()`.
    pub(super) fn kind(&self) -> Result<MppFrameKind, DataFusionError> {
        let reserved = self.flags & FRAME_RESERVED_MASK;
        if reserved != 0 {
            return Err(DataFusionError::Internal(format!(
                "mpp: reserved frame flag bits set ({reserved:#x})"
            )));
        }
        match self.flags & FRAME_KIND_MASK {
            0 => Ok(MppFrameKind::Batch),
            1 => Ok(MppFrameKind::Eof),
            2 => Ok(MppFrameKind::WorkUnit),
            3 => Ok(MppFrameKind::FeedEof),
            4 => Ok(MppFrameKind::TaskMetrics),
            5 => Ok(MppFrameKind::SetPlan),
            6 => Ok(MppFrameKind::Cancel),
            7 => Ok(MppFrameKind::Chunk),
            8 => Ok(MppFrameKind::ExecuteTask),
            9 => Ok(MppFrameKind::TaskError),
            other => Err(DataFusionError::Internal(format!(
                "mpp: unknown frame kind {other:#x}"
            ))),
        }
    }

    /// Serialize into the first `MPP_FRAME_HEADER_SIZE` bytes of `out`.
    /// `out.len()` must be `>= MPP_FRAME_HEADER_SIZE`.
    fn write_to(&self, out: &mut [u8]) {
        debug_assert!(out.len() >= MPP_FRAME_HEADER_SIZE);
        out[0..4].copy_from_slice(&self.magic.to_le_bytes());
        out[4..8].copy_from_slice(&self.flags.to_le_bytes());
        out[8..12].copy_from_slice(&self.stage_id.to_le_bytes());
        out[12..16].copy_from_slice(&self.task_id.to_le_bytes());
        out[16..20].copy_from_slice(&self.partition.to_le_bytes());
    }

    /// Parse from the first `MPP_FRAME_HEADER_SIZE` bytes of `bytes`. Returns
    /// `Err` if the slice is too short or the magic doesn't match.
    fn parse(bytes: &[u8]) -> Result<Self, DataFusionError> {
        if bytes.len() < MPP_FRAME_HEADER_SIZE {
            // No encoder in this file emits sub-header output, so a short frame means the
            // shm_mq stitched together payloads from different senders. Hex-dump the bytes
            // so the source is identifiable from log output without a debugger.
            let hex = bytes
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<Vec<_>>()
                .join(" ");
            return Err(DataFusionError::Internal(format!(
                "mpp: frame too short for header ({} < {}); bytes = [{hex}]",
                bytes.len(),
                MPP_FRAME_HEADER_SIZE
            )));
        }
        let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        if magic != MPP_FRAME_MAGIC {
            return Err(DataFusionError::Internal(format!(
                "mpp: bad frame magic {magic:#x} (expected {MPP_FRAME_MAGIC:#x})"
            )));
        }
        Ok(Self {
            magic,
            flags: u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
            stage_id: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
            task_id: u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            partition: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
        })
    }
}

/// Serialize `batch` into `buf` with a 20-byte [`MppFrameHeader`] prefix
/// addressing it to `(stage_id, task_id, partition)`. Wire format:
///
/// ```text
/// [ magic | flags | stage_id | task_id | partition ] [ Arrow IPC stream bytes ]
/// |--------------- 20 bytes ---------------|           |---- variable ----|
/// ```
///
/// `flags` encodes kind + sender_proc; see the bit-layout block near
/// `FRAME_KIND_MASK` for details.
///
/// Caller is expected to hold `buf` alive across many encodes so the peak-sized
/// allocation amortizes (~500 KB/batch on the 25M GROUP BY bench).
fn encode_frame_into(
    header: MppFrameHeader,
    batch: &RecordBatch,
    buf: &mut Vec<u8>,
) -> Result<(), DataFusionError> {
    buf.clear();
    buf.resize(MPP_FRAME_HEADER_SIZE, 0);
    header.write_to(&mut buf[..MPP_FRAME_HEADER_SIZE]);
    let mut writer = StreamWriter::try_new(&mut *buf, batch.schema_ref())?;
    writer.write(batch)?;
    writer.finish()?;
    Ok(())
}

/// Copy `len` rows starting at `offset` into fresh, tightly-packed arrays. A plain
/// `RecordBatch::slice` keeps the original backing buffers, and arrow-ipc writes a sliced
/// variable-length array's values buffer whole, which is exactly the ballooning the oversized
/// send path exists to undo. `take` materializes only the selected rows; view arrays
/// additionally need a `gc`, because `take` copies their views but keeps referencing the
/// shared data blocks, so the encoded size wouldn't shrink no matter how few rows remain.
///
/// The flight path applies the same collection idea in `garbage_collect_arrays`
/// (`protocol/grpc/worker_service.rs`), unconditionally and with dictionary gc on top.
/// They stay separate on purpose: everything outside `shm` follows upstream, and the
/// dictionary kernel lives in `arrow-select`, which only the `grpc` feature pulls in.
/// Dictionaries therefore ride uncollected here: `take` re-maps the keys but keeps the
/// whole values array, so an oversized dictionary frame streams in chunks instead of
/// shrinking.
fn compact_rows(
    batch: &RecordBatch,
    offset: usize,
    len: usize,
) -> Result<RecordBatch, DataFusionError> {
    let indices = UInt64Array::from_iter_values(offset as u64..(offset + len) as u64);
    let columns = batch
        .columns()
        .iter()
        .map(|column| {
            let taken = take(column.as_ref(), &indices, None)?;
            Ok(match taken.data_type() {
                DataType::Utf8View => Arc::new(
                    taken
                        .as_any()
                        .downcast_ref::<StringViewArray>()
                        .unwrap()
                        .gc(),
                ) as ArrayRef,
                DataType::BinaryView => Arc::new(
                    taken
                        .as_any()
                        .downcast_ref::<BinaryViewArray>()
                        .unwrap()
                        .gc(),
                ) as ArrayRef,
                _ => taken,
            })
        })
        .collect::<Result<Vec<_>, DataFusionError>>()?;
    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

/// Serialize a payload-less [`MppFrameKind::Eof`] frame for `(stage_id, task_id, partition)`
/// into `buf`. The shm_mq peer reads this as a 20-byte message and routes it to
/// the channel buffer's source-done counter without touching Arrow IPC.
/// Consumed by [`MppSender::send_eof_traced`] when a producer fragment's
/// per-partition stream exhausts, so the receiver's `(stage_id, task_id, partition)`
/// channel buffer transitions to `Eof` even though the multiplexed shm_mq queue
/// stays attached for other channels.
fn encode_eof_frame_into(
    stream: MppDataStreamKey,
    sender_proc: u32,
    buf: &mut Vec<u8>,
) -> Result<(), DataFusionError> {
    buf.clear();
    buf.resize(MPP_FRAME_HEADER_SIZE, 0);
    MppFrameHeader::eof(stream, sender_proc).write_to(&mut buf[..MPP_FRAME_HEADER_SIZE]);
    Ok(())
}

/// Byte length of the `[total_len: u64][offset: u64]` prefix on every `Chunk` payload.
const CHUNK_PREFIX_SIZE: usize = 16;

/// Serialize one piece of an oversized frame: chunk header, `[total_len, offset]` prefix, then
/// `data` (the inner frame's bytes at `offset`).
fn encode_chunk_frame_into(
    header: MppFrameHeader,
    total_len: u64,
    offset: u64,
    data: &[u8],
    buf: &mut Vec<u8>,
) {
    buf.clear();
    buf.resize(MPP_FRAME_HEADER_SIZE, 0);
    header.write_to(&mut buf[..MPP_FRAME_HEADER_SIZE]);
    buf.extend_from_slice(&total_len.to_le_bytes());
    buf.extend_from_slice(&offset.to_le_bytes());
    buf.extend_from_slice(data);
}

/// Parse a `Chunk` frame's payload into `(total_len, offset, data)`. The caller has already
/// parsed and validated the 20-byte frame header.
fn parse_chunk_frame(bytes: &[u8]) -> Result<(usize, usize, &[u8]), DataFusionError> {
    let payload = &bytes[MPP_FRAME_HEADER_SIZE..];
    if payload.len() < CHUNK_PREFIX_SIZE {
        return Err(DataFusionError::Internal(format!(
            "mpp: chunk frame too short for its prefix ({} < {CHUNK_PREFIX_SIZE})",
            payload.len()
        )));
    }
    let total_len = u64::from_le_bytes(payload[0..8].try_into().unwrap());
    let offset = u64::from_le_bytes(payload[8..16].try_into().unwrap());
    let (Ok(total_len), Ok(offset)) = (usize::try_from(total_len), usize::try_from(offset)) else {
        return Err(DataFusionError::Internal(format!(
            "mpp: chunk prefix out of range (total_len {total_len}, offset {offset})"
        )));
    };
    Ok((total_len, offset, &payload[CHUNK_PREFIX_SIZE..]))
}

/// Serialize a prost-encoded control payload (`WorkUnit` / `TaskMetrics`) behind `header`.
fn encode_prost_frame_into(
    header: MppFrameHeader,
    msg: &impl prost::Message,
    buf: &mut Vec<u8>,
) -> Result<(), DataFusionError> {
    buf.clear();
    buf.resize(MPP_FRAME_HEADER_SIZE, 0);
    header.write_to(&mut buf[..MPP_FRAME_HEADER_SIZE]);
    msg.encode(buf)
        .map_err(|e| DataFusionError::Internal(format!("mpp: prost frame encode: {e}")))?;
    Ok(())
}

/// A decoded frame payload, routed by the drain according to its kind.
#[derive(Debug)]
enum FrameBody {
    Batch(RecordBatch),
    Eof,
    WorkUnit(pb::WorkUnit),
    FeedEof,
    TaskMetrics(pb::TaskMetrics),
    SetPlan(SetPlanFrame),
    ExecuteTask(ExecuteTaskFrame),
    Cancel,
    TaskError(String),
}

/// Inverse of the frame encoders. Parses the 20-byte header and decodes the payload according
/// to the kind. Receivers branch on the body to decide routing.
fn decode_frame(bytes: &[u8]) -> Result<(MppFrameHeader, FrameBody), DataFusionError> {
    let header = MppFrameHeader::parse(bytes)?;
    let payload = &bytes[MPP_FRAME_HEADER_SIZE..];
    match header.kind()? {
        MppFrameKind::Eof | MppFrameKind::FeedEof | MppFrameKind::Cancel => {
            if bytes.len() != MPP_FRAME_HEADER_SIZE {
                return Err(DataFusionError::Internal(format!(
                    "mpp: payload-less frame carries payload ({} > {})",
                    bytes.len(),
                    MPP_FRAME_HEADER_SIZE
                )));
            }
            match header.kind()? {
                MppFrameKind::Eof => Ok((header, FrameBody::Eof)),
                MppFrameKind::Cancel => Ok((header, FrameBody::Cancel)),
                _ => Ok((header, FrameBody::FeedEof)),
            }
        }
        MppFrameKind::WorkUnit => {
            let unit = pb::WorkUnit::decode(payload)
                .map_err(|e| DataFusionError::Internal(format!("mpp: work unit decode: {e}")))?;
            Ok((header, FrameBody::WorkUnit(unit)))
        }
        MppFrameKind::TaskMetrics => {
            let metrics = pb::TaskMetrics::decode(payload)
                .map_err(|e| DataFusionError::Internal(format!("mpp: task metrics decode: {e}")))?;
            Ok((header, FrameBody::TaskMetrics(metrics)))
        }
        MppFrameKind::SetPlan => {
            let frame = SetPlanFrame::decode(payload)
                .map_err(|e| DataFusionError::Internal(format!("mpp: set-plan decode: {e}")))?;
            Ok((header, FrameBody::SetPlan(frame)))
        }
        MppFrameKind::ExecuteTask => {
            let frame = ExecuteTaskFrame::decode(payload)
                .map_err(|e| DataFusionError::Internal(format!("mpp: execute-task decode: {e}")))?;
            Ok((header, FrameBody::ExecuteTask(frame)))
        }
        MppFrameKind::Batch => {
            let mut reader = StreamReader::try_new(payload, None)?;
            let batch = reader.next().ok_or_else(|| {
                DataFusionError::Execution("mpp: empty arrow-ipc stream in decode_frame".into())
            })??;
            Ok((header, FrameBody::Batch(batch)))
        }
        MppFrameKind::Chunk => Err(DataFusionError::Internal(
            "mpp: chunk frame reached decode_frame; chunks reassemble in MppReceiver first"
                .to_string(),
        )),
        MppFrameKind::TaskError => {
            let msg = String::from_utf8_lossy(payload).into_owned();
            Ok((header, FrameBody::TaskError(msg)))
        }
    }
}

/// Local queue between a drain (either the cooperative `try_drain_pass` or the test-only thread
/// variant) and the consumer that pops batches.
///
/// In the cooperative path each `DrainBuffer` corresponds to one logical channel: one
/// `(stage_id, task_id, partition)` entry in the owning [`DrainHandle`]'s registry. `num_sources` is
/// always `1` there because a given drain serves a single sender_proc, which is the only producer
/// for any channel routed through it. The test-only thread path uses a single shared buffer with
/// `num_sources = N` over an N-sender setup.
///
/// Push side: callers append deserialized batches; on source detach (or per-channel `Eof` frame)
/// [`DrainBuffer::notify_source_done`] is called. Once `sources_done >= num_sources` AND the
/// queue is empty, `try_pop` returns [`DrainItem::Eof`].
///
/// Pop side: cooperative consumers loop on `try_pop` + `yield_now`. The test-only `pop_front`
/// blocks on the condvar.
#[derive(Debug)]
pub(super) struct DrainBuffer {
    inner: Mutex<DrainBufferInner>,
    cond: Condvar,
}

#[derive(Debug)]
struct DrainBufferInner {
    queue: VecDeque<RecordBatch>,
    num_sources: u32,
    sources_done: u32,
    /// Consumer-side cancel flag. When set (e.g., query cancelled or `DrainHandle` dropped),
    /// `try_pop`/`pop_front` returns `Eof` even if `sources_done` hasn't reached `num_sources`.
    cancelled: bool,
    /// Set when the receiver feeding this channel detached (or errored) before the channel's
    /// `Eof` frame arrived. Distinct from `cancelled`: cancellation is a clean teardown and
    /// yields `Eof`, while a lost source must surface as an error or the consumer would treat
    /// truncated output as complete.
    failed: Option<String>,
}

/// Yielded by [`DrainBuffer::pop_front`].
#[derive(Debug)]
pub(super) enum DrainItem {
    /// A batch produced by one of the inbound shm_mqs.
    Batch(RecordBatch),
    /// All source queues have detached and the local queue is drained.
    Eof,
    /// The receiver feeding this channel went away before the channel's `Eof` frame.
    Failed(String),
}

impl DrainBuffer {
    /// Create a drain buffer expecting `num_sources` inbound queues. For a
    /// proc in an N-proc mesh, `num_sources == N - 1` (all peers
    /// excluding self — the self-partition bypasses the buffer).
    pub fn new(num_sources: u32) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(DrainBufferInner {
                queue: VecDeque::new(),
                num_sources,
                sources_done: 0,
                cancelled: false,
                failed: None,
            }),
            cond: Condvar::new(),
        })
    }

    /// Push a freshly-received batch into the local queue.
    pub fn push_batch(&self, batch: RecordBatch) {
        let mut guard = self.inner.lock().expect("DrainBuffer mutex poisoned");
        guard.queue.push_back(batch);
        self.cond.notify_one();
    }

    /// Mark one source queue as detached. Safe to call from the drain thread
    /// after observing `SHM_MQ_DETACHED` on a given inbound queue.
    pub fn notify_source_done(&self) {
        let mut guard = self.inner.lock().expect("DrainBuffer mutex poisoned");
        guard.sources_done = guard.sources_done.saturating_add(1);
        if guard.sources_done >= guard.num_sources {
            self.cond.notify_all();
        }
    }

    /// Mark the channel as fed by a dead receiver, unless it already completed (its `Eof`
    /// arrived), was cancelled, or already failed. Consumers then see an error instead of
    /// hanging on a channel nothing will ever fill.
    pub fn fail_pending(&self, msg: &str) {
        let mut guard = self.inner.lock().expect("DrainBuffer mutex poisoned");
        if guard.sources_done >= guard.num_sources || guard.cancelled || guard.failed.is_some() {
            return;
        }
        guard.failed = Some(msg.to_string());
        self.cond.notify_all();
    }

    /// Cancel all further pushes and wake all consumers with EOF.
    pub fn cancel(&self) {
        let mut guard = self.inner.lock().expect("DrainBuffer mutex poisoned");
        guard.cancelled = true;
        self.cond.notify_all();
    }

    /// Whether this buffer has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.inner
            .lock()
            .expect("DrainBuffer mutex poisoned")
            .cancelled
    }

    /// Non-blocking variant. Returns the front item, or `DrainItem::Eof` if
    /// all sources have detached and the queue is drained, or `None` if more
    /// data may yet arrive. Cooperative consumers loop on
    /// `try_drain_pass` + `try_pop`, yielding to the executor between
    /// iterations.
    pub fn try_pop(&self) -> Option<DrainItem> {
        let mut guard = self.inner.lock().expect("DrainBuffer mutex poisoned");
        Self::try_pop_locked(&mut guard)
    }

    /// Shared body of [`try_pop`] and the test-only [`Self::pop_front`].
    /// Returns `Some(Batch)` if the queue has data, `Some(Eof)` if all
    /// sources have detached or the buffer is cancelled, and `None`
    /// otherwise. Lets the two entry points stay in lockstep on the
    /// "buffered data wins over cancellation/EOF" invariant locked in by
    /// the `drain_buffer_drains_buffered_before_eof` test.
    fn try_pop_locked(guard: &mut MutexGuard<'_, DrainBufferInner>) -> Option<DrainItem> {
        if let Some(batch) = guard.queue.pop_front() {
            return Some(DrainItem::Batch(batch));
        }
        if let Some(msg) = &guard.failed {
            return Some(DrainItem::Failed(msg.clone()));
        }
        if guard.cancelled || guard.sources_done >= guard.num_sources {
            return Some(DrainItem::Eof);
        }
        None
    }
}

/// Outcome of a single non-blocking receive attempt.
#[derive(Debug)]
pub(super) enum RecvOutcome {
    /// One serialized Arrow IPC message ready to decode.
    Bytes(Vec<u8>),
    /// No data currently available but the peer is still attached.
    Empty,
    /// The peer has detached; no more bytes will ever arrive on this channel.
    Detached,
}

/// Non-blocking byte channel receiver. Implementations: `DsmInboxReceiver` (production),
/// `std::sync::mpsc` (tests). Must be `Send` because the drain thread takes ownership.
pub(super) trait BatchChannelReceiver: Send + Sync {
    fn try_recv(&self) -> RecvOutcome;
}

/// Byte channel sender paired with [`BatchChannelReceiver`]. `send` blocks when
/// the channel is full. Dropping the sender signals EOF to the receiver.
///
/// `Send` is required because unit tests and future producer-pump threads move
/// senders across thread boundaries.
pub(crate) trait BatchChannelSender: Send + Sync {
    fn send_bytes(&self, bytes: &[u8]) -> Result<(), DataFusionError>;

    /// Non-blocking variant. Returns `Ok(true)` on success, `Ok(false)`
    /// when the channel is full (caller should retry), `Err` on detach /
    /// transport error. Default falls back to the blocking send — safe
    /// for in-proc channels used by tests where "full" doesn't arise.
    fn try_send_bytes(&self, bytes: &[u8]) -> Result<bool, DataFusionError> {
        self.send_bytes(bytes).map(|()| true)
    }

    /// Async lock the send paths hold across the cooperative-drain spin so two tasks can't
    /// interleave partial writes on the same handle. PG's `shm_mq_send` requires the same
    /// `(nbytes, data)` on retry after `SHM_MQ_WOULD_BLOCK`. Multiple [`MppSender`] clones
    /// multiplex onto one channel, and the spin's `yield_now().await` would otherwise let a
    /// sibling task land a different payload mid-message and corrupt the queue. In-proc
    /// channels return a per-instance mutex too, just to keep the call sites uniform.
    fn send_lock(&self) -> &tokio::sync::Mutex<()>;

    /// Largest single frame the channel accepts, when bounded. The batch send path splits
    /// oversized batches to fit under it. `None` (the default, for in-proc channels) disables
    /// splitting.
    fn max_frame_bytes(&self) -> Option<usize> {
        None
    }
}

/// Pluggable "drain everything inbound" hook for [`MppSender`]'s cooperative send spin. The
/// peer-mesh deadlock-breaking pattern needs the producer to pump ALL inbound queues (not just
/// one) while waiting for a full outbound, so the implementation typically delegates to
/// `MppMesh::drain_all_inbound()` which iterates every per-sender-proc drain.
pub trait CooperativeDrainSet: Send + Sync {
    fn try_drain_pass(&self) -> Result<(), DataFusionError>;

    /// Checked in the send spin alongside `try_drain_pass`: returns `Err` (or aborts) if the
    /// query should stop. Default is a no-op, for embedders with no external interrupt source; a
    /// Postgres embedder overrides this to run `check_for_interrupts!`, which longjmps on cancel.
    fn check_interrupt(&self) -> Result<(), DataFusionError> {
        Ok(())
    }

    /// Whether a consumer cancelled the `(stage_id, task_id, partition)` stream (a `Cancel` frame arrived on
    /// this proc's inbox). The send spin ends the producer's stream cleanly when it's set. Default
    /// `false` for drains that don't carry inbound control frames (in-proc test channels).
    fn stream_cancelled(&self, _stream: MppDataStreamKey) -> bool {
        false
    }
}

/// Cancellation extension point, checked at the transport's block points (the send spin and the consumer
/// pull loop). An in-process embedder uses the default no-op or a cancellation token; a Postgres
/// embedder runs `check_for_interrupts!`, which longjmps out of the backend on cancel.
pub trait Interrupt: Send + Sync {
    fn check(&self) -> Result<(), DataFusionError>;
}

/// No-op interrupt for embedders that have no external cancellation source.
pub struct NoInterrupt;
impl Interrupt for NoInterrupt {
    fn check(&self) -> Result<(), DataFusionError> {
        Ok(())
    }
}

impl CooperativeDrainSet for DrainHandle {
    fn try_drain_pass(&self) -> Result<(), DataFusionError> {
        DrainHandle::try_drain_pass(self)
    }

    fn stream_cancelled(&self, stream: MppDataStreamKey) -> bool {
        DrainHandle::stream_cancelled(self, stream)
    }
}

/// High-level sender: encodes a `RecordBatch` then pushes bytes through the underlying channel.
///
/// With `cooperative_drain` set, `send_batch` breaks the symmetric-send deadlock on a
/// single-threaded tokio runtime by interleaving send-retries with
/// `CooperativeDrainSet::try_drain_pass` on the same mesh's inbound side. Each proc's
/// sender doing the same guarantees mutual progress: our drain pulls peer-shipped rows out of
/// our inbound queues, which frees peers' outbound-to-us send space, which lets their sends
/// un-stall.
pub struct MppSender {
    /// Underlying byte channel. Held behind `Arc` so multiple `MppSender`s can share one
    /// `shm_mq` queue while tagging frames with different `(stage_id, task_id, partition)` headers, which
    /// is the multiplexed path's natural pattern. Clone the Arc, build a new `MppSender` with a
    /// different header, both write into the same queue.
    pub(super) channel: Arc<dyn BatchChannelSender>,
    cooperative_drain: Option<Arc<dyn CooperativeDrainSet>>,
    /// Frame header prepended to every outgoing batch. Identifies the logical
    /// `(stage_id, task_id, partition)` channel the receiver demultiplexes on. Per-sender rather than
    /// per-call: each partition gets its own `MppSender` via `clone_with_header`, all sharing
    /// the underlying `Arc<dyn BatchChannelSender>` of a single shm_mq queue.
    pub(super) header: MppFrameHeader,
    /// Scratch buffer reused across every `encode_frame_into` on this sender. Sized by the
    /// first batch; subsequent batches clear and re-fill without reallocating. Interior
    /// mutability lets the caller keep the `&self` signature (each producer fragment holds
    /// its `MppSender` clones behind shared borrows for the duration of
    /// `worker::run_worker_fragment`).
    scratch: std::cell::RefCell<Vec<u8>>,
}

// SAFETY: only `scratch: RefCell<Vec<u8>>` and the trait-object `Arc`s are `!Sync`. Callers
// compose `send_*_traced` futures via `tokio::spawn` / `join_all`, which makes the compiler
// require `&Self: Send` and therefore `Self: Sync`. The shared-memory model runs those futures on
// a current-thread runtime (see the module docs), so the cell is never observed from two
// threads; a multi-thread embedder would additionally be serialized by `send_lock` across
// every send path that touches `scratch`.
unsafe impl Sync for MppSender {}

impl MppSender {
    pub(super) fn with_header(
        channel: Arc<dyn BatchChannelSender>,
        header: MppFrameHeader,
    ) -> Self {
        Self {
            channel,
            cooperative_drain: None,
            header,
            scratch: std::cell::RefCell::new(Vec::new()),
        }
    }

    /// Build a new `MppSender` that shares this sender's underlying channel
    /// but tags every frame with `header` instead. Used by callers that know
    /// the physical plan's output partition count and need one sender per
    /// partition, all multiplexed over the same shm_mq queue.
    pub fn clone_with_header(&self, header: MppFrameHeader) -> Self {
        Self {
            channel: Arc::clone(&self.channel),
            cooperative_drain: self.cooperative_drain.as_ref().map(Arc::clone),
            header,
            scratch: std::cell::RefCell::new(Vec::new()),
        }
    }

    /// Whether the consumer of this sender's `(stage, task, partition)` stream cancelled it. Read by the
    /// produce loop to stop pulling its input, not just skip the send. `false` without a drain
    /// (in-proc test channels carry no inbound cancel).
    pub(super) fn stream_cancelled(&self) -> bool {
        // A data-stream cancel can numerically match a control address; control traffic is never
        // cancelled by a stream-level cancel.
        if !matches!(self.header.kind(), Ok(MppFrameKind::Batch)) {
            return false;
        }
        self.cooperative_drain
            .as_ref()
            .is_some_and(|d| d.stream_cancelled(self.header.data_stream()))
    }

    /// Attach a [`CooperativeDrainSet`] so `Self::send_batch_traced`'s spin
    /// can drain inbound peer traffic while waiting for outbound space.
    /// Required for peer-mesh fragments where every worker is both sender and
    /// consumer; without it, symmetric full-queue stalls deadlock the
    /// single-threaded Tokio runtime.
    pub fn with_cooperative_drain(mut self, drain: Arc<dyn CooperativeDrainSet>) -> Self {
        self.cooperative_drain = Some(drain);
        self
    }

    /// `send_batch` variant that accumulates per-call timings and spin counts into `stats`.
    /// Callers that report at EOF (e.g. `ShuffleStream`) use this to diagnose where time
    /// goes when the outbound queue is full.
    ///
    /// Async because the spin awaits the per-handle send lock and yields between
    /// `try_send_bytes` retries; see `send_with_scratch`.
    pub(super) async fn send_batch_traced(
        &self,
        batch: &RecordBatch,
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        // Take the scratch buffer out of the `RefCell` rather than
        // holding a `RefMut` across the spin below. The spin contains
        // the embedder's `Interrupt::check`, which may unwind or `longjmp` through
        // Rust frames; a `longjmp` does not run `Drop`, so a `RefMut`
        // held across it would leave the cell perpetually borrowed and
        // panic the next caller. `replace` is atomic — the cell is
        // never observed in a borrowed state — and we put the buffer
        // back at the end so its heap allocation survives across calls.
        // If the spin longjmps anyway, the cell holds the default empty
        // `Vec` and the next call simply re-allocates.
        let mut scratch = self.scratch.replace(Vec::new());
        let result = self.send_with_scratch(batch, &mut scratch, stats).await;
        self.scratch.replace(scratch);
        result
    }

    /// Send a payload-less [`MppFrameKind::Eof`] frame so the receiver's `(stage_id, task_id, partition)`
    /// channel buffer transitions to `Eof` and the consumer's pull loop terminates cleanly.
    ///
    /// Producer fragments must call this exactly once per `(stage_id, task_id, partition)` channel after
    /// the local stream exhausts. Without it the multiplexed shm_mq queue stays attached (other
    /// channels still flow) and the consumer channel buffer never reaches `sources_done == 1`. The
    /// receive-side [`DrainHandle::try_drain_pass`] decodes the frame and calls
    /// `notify_source_done` on the matching channel buffer.
    ///
    /// Uses the same cooperative-spin path as [`Self::send_batch_traced`] so a full outbound
    /// queue doesn't deadlock the EOF send. `stats.spin_iters` / `send_wait` capture any
    /// contention.
    ///
    /// Symmetric-EOF safety: when every peer reaches EOF simultaneously with full outbound
    /// queues, each peer's cooperative [`CooperativeDrainSet::try_drain_pass`] inside the spin
    /// pulls peer-sent frames out of its own inbound queues, freeing space the peers are blocked
    /// on. Progress is monotone: at least one `try_send_bytes` succeeds per spin iteration
    /// somewhere in the mesh, so symmetric stalls resolve within a few iterations rather than
    /// deadlocking.
    pub(super) async fn send_eof_traced(
        &self,
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        let mut scratch = self.scratch.replace(Vec::new());
        let result = self.send_eof_with_scratch(&mut scratch, stats).await;
        self.scratch.replace(scratch);
        result
    }

    /// Bounded synchronous send of a `Cancel` frame for the `(stage_id, task_id, partition)` stream. The
    /// consumer calls it on the sender to the producing proc when it abandons that stream.
    /// Synchronous so it can run from the consumer stream's drop, where `await` isn't available.
    ///
    /// The bound doesn't risk a stuck producer. A live producer drains its own inbox in its send
    /// spin, so a slot frees and the frame lands well inside the bound even when the inbox is
    /// backed up. The bound only runs out if the producer already exited or died, and a dead worker
    /// makes the leader's wait-for-workers error out rather than hang.
    pub fn try_send_cancel(&self, stream: MppDataStreamKey) {
        let header = MppFrameHeader::cancel(stream, self.header.sender_proc());
        let mut buf = [0u8; MPP_FRAME_HEADER_SIZE];
        header.write_to(&mut buf);
        for _ in 0..MAX_CONTROL_SEND_SPINS {
            match self.channel.try_send_bytes(&buf) {
                Ok(true) => return,
                Ok(false) => std::thread::yield_now(),
                Err(_) => return, // the producer's inbox is gone; nothing left to cancel
            }
        }
    }

    async fn send_eof_with_scratch(
        &self,
        scratch: &mut Vec<u8>,
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        encode_eof_frame_into(
            self.header.data_stream(),
            self.header.sender_proc(),
            scratch,
        )?;
        self.spin_send_frame(scratch, stats).await.map(|_| ())
    }

    /// Spin-loop helper: call `channel.try_send_bytes(scratch)`.
    async fn spin_try_send_bytes(&self, scratch: &[u8]) -> Result<bool, DataFusionError> {
        self.channel.try_send_bytes(scratch)
    }

    /// Spin-loop helper: call `drain.try_drain_pass()`.
    async fn spin_try_drain_pass(
        &self,
        drain: &Arc<dyn CooperativeDrainSet>,
    ) -> Result<(), DataFusionError> {
        drain.try_drain_pass()
    }

    async fn send_with_scratch(
        &self,
        batch: &RecordBatch,
        scratch: &mut Vec<u8>,
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        let t_enc = Instant::now();
        encode_frame_into(self.header, batch, scratch)?;
        stats.encode += t_enc.elapsed();
        if self
            .channel
            .max_frame_bytes()
            .is_none_or(|cap| scratch.len() <= cap)
        {
            return self.spin_send_scratch(scratch, stats).await;
        }
        // An over-cap frame usually means the batch carries offset slices of an upstream
        // operator's accumulated state: arrow-ipc writes a sliced variable-length array's
        // whole values buffer, so the frame balloons to the state's size no matter the row
        // count. Compacting into fresh arrays drops the shared buffers. It's an
        // optimization, not a bound: whatever stays over the cap streams through the ring
        // in chunks.
        let compacted = compact_rows(batch, 0, batch.num_rows())?;
        let t_enc = Instant::now();
        encode_frame_into(self.header, &compacted, scratch)?;
        stats.encode += t_enc.elapsed();
        self.spin_send_scratch(scratch, stats).await
    }

    /// Push an already-encoded frame through the channel, splitting it into `Chunk` frames
    /// first when it exceeds the channel's frame bound. Shared by every frame kind's send
    /// path, so an oversized `SetPlan` streams the same way an oversized batch does.
    async fn spin_send_scratch(
        &self,
        scratch: &[u8],
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        match self.channel.max_frame_bytes() {
            Some(cap) if scratch.len() > cap => self.send_chunked(scratch, cap, stats).await,
            _ => self.spin_send_frame(scratch, stats).await.map(|_| ()),
        }
    }

    /// Stream one oversized frame through the bounded channel as a sequence of `Chunk`
    /// frames, each within the channel's bound. The whole frame exists only in this
    /// sender's scratch and the receiver's reassembly buffer, never in the ring, so the
    /// ring bounds backpressure granularity rather than message size. A cancel observed
    /// between chunks abandons the remainder; the receiver's reassembly rules (offset-0
    /// restart, cleanup on the stream's `Eof`) tolerate the orphaned prefix.
    async fn send_chunked(
        &self,
        frame: &[u8],
        cap: usize,
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        let Some(chunk_data_max) = cap
            .checked_sub(MPP_FRAME_HEADER_SIZE + CHUNK_PREFIX_SIZE)
            .filter(|n| *n > 0)
        else {
            return Err(DataFusionError::Internal(format!(
                "mpp: channel frame bound {cap} cannot fit a chunk header"
            )));
        };
        let header = MppFrameHeader::chunk(self.header.data_stream(), self.header.sender_proc());
        let total_len = frame.len() as u64;
        let mut buf =
            Vec::with_capacity(MPP_FRAME_HEADER_SIZE + CHUNK_PREFIX_SIZE + chunk_data_max);
        let mut offset = 0usize;
        while offset < frame.len() {
            let end = usize::min(offset + chunk_data_max, frame.len());
            encode_chunk_frame_into(
                header,
                total_len,
                offset as u64,
                &frame[offset..end],
                &mut buf,
            );
            if !self.spin_send_frame(&buf, stats).await? {
                // The consumer cancelled the stream mid-frame; drop the rest.
                return Ok(());
            }
            offset = end;
        }
        Ok(())
    }

    /// Push one channel-bounded frame through the cooperative-drain spin (or the blocking
    /// fallback when no drain is attached). Returns `false` when the spin ended because the
    /// consumer cancelled this sender's stream, so multi-frame callers can stop early.
    async fn spin_send_frame(
        &self,
        scratch: &[u8],
        stats: &mut SendBatchStats,
    ) -> Result<bool, DataFusionError> {
        let Some(drain) = self.cooperative_drain.as_ref() else {
            // No drain attached (unit tests, in-proc channels): fall
            // back to the blocking send path.
            return self.channel.send_bytes(scratch).map(|()| true);
        };
        // Lock the channel before the spin so a sibling task can't interleave a different
        // partial write through the shared shm_mq handle. See `BatchChannelSender::send_lock`.
        // Long-term, switching shm_mq for an async-friendly ring buffer (cf. #4184) drops the
        // partial-send invariant entirely and removes the need for this lock.
        //
        // Latent under the current-thread runtime: today every fragment owns its own
        // `Arc<dyn BatchChannelSender>` (one sender per `Arc`), so the FIFO Mutex below
        // is uncontended. A future multi-thread runtime that shares a sender across
        // sibling fragment tasks (multi-partition fan-out) would let one task starve
        // another for the duration of a large shuffle; the fix at that point is to move
        // the entire spin off the compute thread.
        let _send_guard = self.channel.send_lock().lock().await;
        let mut first_try = true;
        let t_wait_start = Instant::now();
        // The spin runs inside a tokio task on the backend thread's current-thread runtime
        // (DataFusion needs one to drive `Stream`s). The deadlock we're breaking is
        // *cross-proc*: two peers each blocked on a full outbound. `try_drain_pass` pulls
        // peer batches off our inbound on the same OS thread, freeing their slots so their
        // sends advance. `yield_now().await` between iterations hands the runtime back to
        // siblings if any are ready, mostly a no-op under today's linear MPP topology.
        loop {
            drain.check_interrupt()?;
            // The consumer cancelled this stream, so end the send cleanly and let the producer
            // fragment complete. Checked before the send so a cancel that landed mid-spin stops
            // the next iteration.
            if self.stream_cancelled() {
                return Ok(false);
            }
            if self.spin_try_send_bytes(scratch).await? {
                if !first_try {
                    stats.send_wait += t_wait_start.elapsed();
                }
                return Ok(true);
            }
            first_try = false;
            stats.spin_iters += 1;
            // Would-block. Pull from our inbound so peers' outbound-to-us frees up and their
            // sends to us unblock; without this, symmetric full-queue sends deadlock. Errors
            // propagate so a peer detaching mid-spin doesn't leave us spinning on a closed
            // mesh.
            let t_drain = Instant::now();
            self.spin_try_drain_pass(drain).await?;
            stats.coop_drain_in_spin += t_drain.elapsed();
            tokio::task::yield_now().await;
        }
    }

    /// Send one work unit for the task this sender's header names. The unit's hop stamps are the
    /// caller's job; the payload travels prost-encoded, never through Arrow IPC.
    pub async fn send_work_unit_traced(
        &self,
        unit: &pb::WorkUnit,
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        let mut scratch = self.scratch.replace(Vec::new());
        let result = async {
            encode_prost_frame_into(self.header, unit, &mut scratch)?;
            self.spin_send_scratch(&scratch, stats).await
        }
        .await;
        self.scratch.replace(scratch);
        result
    }

    /// Ship one task's plan as a `SetPlan` frame: the wire stand-in for Flight's
    /// `SetPlanRequest` over its coordinator stream.
    pub async fn send_set_plan_traced(
        &self,
        frame: &SetPlanFrame,
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        let mut scratch = self.scratch.replace(Vec::new());
        let result = async {
            encode_prost_frame_into(self.header, frame, &mut scratch)?;
            self.spin_send_scratch(&scratch, stats).await
        }
        .await;
        self.scratch.replace(scratch);
        result
    }

    /// Ship a task's execute-task request as an `ExecuteTask` frame.
    pub async fn send_execute_task_traced(
        &self,
        frame: &ExecuteTaskFrame,
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        let mut scratch = self.scratch.replace(Vec::new());
        let result = async {
            encode_prost_frame_into(self.header, frame, &mut scratch)?;
            self.spin_send_scratch(&scratch, stats).await
        }
        .await;
        self.scratch.replace(scratch);
        result
    }

    /// Close the feed channels of the task this sender's header names: the wire stand-in for
    /// Flight closing its coordinator stream, after which the worker-side feed streams end.
    pub async fn send_feed_eof_traced(
        &self,
        stats: &mut SendBatchStats,
    ) -> Result<(), DataFusionError> {
        let mut scratch = self.scratch.replace(Vec::new());
        let result = async {
            scratch.clear();
            scratch.resize(MPP_FRAME_HEADER_SIZE, 0);
            MppFrameHeader::feed_eof(
                self.header.stage_id,
                self.header.partition,
                self.header.sender_proc(),
            )
            .write_to(&mut scratch[..MPP_FRAME_HEADER_SIZE]);
            self.spin_send_scratch(&scratch, stats).await
        }
        .await;
        self.scratch.replace(scratch);
        result
    }

    /// Send the task's collected metrics without consulting the interrupt: this runs after the
    /// query's cancellation token already fired (it fires on normal completion too), so the
    /// cooperative spin would abort exactly when delivery matters. Best-effort like Flight's
    /// metrics sends: a detached leader drops them. Retrying on a full ring is safe because the
    /// receiving side keeps draining until every producer reported in.
    pub async fn send_task_metrics_best_effort(
        &self,
        metrics: &pb::TaskMetrics,
    ) -> Result<(), DataFusionError> {
        let mut scratch = self.scratch.replace(Vec::new());
        let result = async {
            encode_prost_frame_into(self.header, metrics, &mut scratch)?;
            let _send_guard = self.channel.send_lock().lock().await;
            // The consumer stops reading the data path early but keeps draining at teardown to
            // collect these metrics, so the frame lands once the data the cancel stopped drains out
            // and a slot frees. Bounded so a leader that really went away can't wedge the worker on
            // a full ring.
            for _ in 0..MAX_CONTROL_SEND_SPINS {
                match self.channel.try_send_bytes(&scratch) {
                    Ok(true) => return Ok(()),
                    Ok(false) => tokio::task::yield_now().await,
                    Err(_) => return Ok(()), // receiver gone; metrics are best-effort
                }
            }
            Ok(())
        }
        .await;
        self.scratch.replace(scratch);
        result
    }

    /// Like `send_task_metrics_best_effort`, but for sending a task's fatal error message.
    /// Sent explicitly by the producer (via `run_execute_task_loop`) on a panic.
    pub(crate) async fn send_task_error_best_effort(
        &self,
        msg: &str,
    ) -> Result<(), DataFusionError> {
        let mut scratch = self.scratch.replace(Vec::new());
        let result = async {
            scratch.clear();
            scratch.resize(MPP_FRAME_HEADER_SIZE, 0);
            let error_header = MppFrameHeader::task_error(
                self.header.stage_id,
                self.header.partition,
                self.header.sender_proc(),
            );
            error_header.write_to(&mut scratch[..MPP_FRAME_HEADER_SIZE]);

            let mut msg_bytes = msg.as_bytes();
            if let Some(cap) = self.channel.max_frame_bytes() {
                let max_payload_len = cap.saturating_sub(MPP_FRAME_HEADER_SIZE);
                if msg_bytes.len() > max_payload_len {
                    msg_bytes = &msg_bytes[..max_payload_len];
                }
            }
            scratch.extend_from_slice(msg_bytes);

            let _send_guard = self.channel.send_lock().lock().await;
            for _ in 0..MAX_CONTROL_SEND_SPINS {
                match self.channel.try_send_bytes(&scratch) {
                    Ok(true) => return Ok(()),
                    Ok(false) => tokio::task::yield_now().await,
                    Err(_) => return Ok(()),
                }
            }
            Ok(())
        }
        .await;
        self.scratch.replace(scratch);
        result
    }
}

impl Clone for MppSender {
    fn clone(&self) -> Self {
        self.clone_with_header(self.header)
    }
}

/// Per-call timing + spin metrics for [`MppSender::send_batch_traced`].
/// All fields accumulate; callers zero or reuse as needed.
#[derive(Default, Debug, Clone)]
pub struct SendBatchStats {
    /// Cumulative time spent inside `encode_frame_into` (header + Arrow IPC serialization).
    pub encode: Duration,
    /// Cumulative wall time in the send-retry spin after the first failed
    /// `try_send_bytes`. Zero if the first try succeeded.
    pub send_wait: Duration,
    /// Cumulative time spent in `try_drain_pass` while spinning on a
    /// full outbound. A subset of `send_wait`; the remainder is the
    /// `tokio::task::yield_now()` await + the (small) cost of
    /// `try_send_bytes` itself.
    pub coop_drain_in_spin: Duration,
    /// Count of `try_send_bytes` calls that returned `Ok(false)` (full).
    pub spin_iters: u64,
}

/// A [`crate::PartitionSink`] over one [`MppSender`]: the produce loop's per-partition send end.
/// `send` runs the cooperative-drain spin and `finish` flushes the channel EOF the same way, so a
/// non-Flight embedder (the in-process harness, pg_search's worker loop) wraps each routed sender
/// in one of these and pushes batches through the trait instead of touching `MppSender` directly.
pub struct MppPartitionSink {
    sender: MppSender,
    stats: SendBatchStats,
}

impl MppPartitionSink {
    pub fn new(sender: MppSender) -> Self {
        Self {
            sender,
            stats: SendBatchStats::default(),
        }
    }

    /// Per-channel send counters, for an embedder that traces throughput. Read them before
    /// `finish`, which consumes the sink.
    pub fn stats(&self) -> &SendBatchStats {
        &self.stats
    }
}

#[async_trait]
impl crate::PartitionSink for MppPartitionSink {
    async fn send(&mut self, batch: &RecordBatch) -> datafusion::common::Result<()> {
        self.sender.send_batch_traced(batch, &mut self.stats).await
    }

    async fn finish(mut self: Box<Self>) -> datafusion::common::Result<()> {
        self.sender.send_eof_traced(&mut self.stats).await
    }

    fn cancelled(&self) -> bool {
        self.sender.stream_cancelled()
    }
}

/// A [`PartitionSink`] that pushes batches directly into a local in-memory [`DrainBuffer`].
/// Used for self-loops on a worker process when a fragment's output partition is consumed
/// by another fragment on the same worker process.
pub struct LocalDrainPartitionSink {
    buffer: Arc<DrainBuffer>,
}

impl LocalDrainPartitionSink {
    pub(super) fn new(buffer: Arc<DrainBuffer>) -> Self {
        Self { buffer }
    }
}

#[async_trait]
impl crate::PartitionSink for LocalDrainPartitionSink {
    async fn send(&mut self, batch: &RecordBatch) -> datafusion::common::Result<()> {
        self.buffer.push_batch(batch.clone());
        Ok(())
    }

    async fn finish(self: Box<Self>) -> datafusion::common::Result<()> {
        self.buffer.notify_source_done();
        Ok(())
    }

    fn cancelled(&self) -> bool {
        self.buffer.is_cancelled()
    }
}

/// High-level receiver: pulls bytes via the underlying channel and decodes them
/// into `RecordBatch`. Used by the drain thread.
pub(super) struct MppReceiver {
    channel: Box<dyn BatchChannelReceiver>,
    /// In-progress oversized frames, keyed by the chunk headers' `(sender_proc, stage_id,
    /// task_id, partition)`. One entry per stream: a sender's frames for one stream are sequential,
    /// so its chunks never interleave with each other, while chunked frames of two streams
    /// from the same proc may. A prefix abandoned mid-frame (the producer observed the
    /// stream's cancel) sits here until the stream's `Eof` clears it or a fresh frame for
    /// the stream restarts at offset 0.
    assemblies: Mutex<HashMap<PhysicalStreamKey, FrameAssembly>>,
}

/// One stream's partially reassembled oversized frame.
struct FrameAssembly {
    total_len: usize,
    buf: Vec<u8>,
}

impl MppReceiver {
    pub fn new(channel: Box<dyn BatchChannelReceiver>) -> Self {
        Self {
            channel,
            assemblies: Mutex::new(HashMap::default()),
        }
    }

    pub(super) fn try_recv_batch(&self) -> RecvBatchOutcome {
        loop {
            match self.channel.try_recv() {
                RecvOutcome::Bytes(bytes) => {
                    let complete = match self.ingest(bytes) {
                        Ok(Some(frame)) => frame,
                        // Absorbed one chunk of a larger frame; pull the next message.
                        Ok(None) => continue,
                        Err(e) => return RecvBatchOutcome::TransportError(e),
                    };
                    return match decode_frame(&complete) {
                        Ok((header, FrameBody::Batch(batch))) => {
                            RecvBatchOutcome::Batch { header, batch }
                        }
                        Ok((header, FrameBody::Eof)) => RecvBatchOutcome::Eof { header },
                        Ok((header, FrameBody::WorkUnit(unit))) => {
                            RecvBatchOutcome::WorkUnit { header, unit }
                        }
                        Ok((header, FrameBody::FeedEof)) => RecvBatchOutcome::FeedEof { header },
                        Ok((header, FrameBody::TaskMetrics(metrics))) => {
                            RecvBatchOutcome::TaskMetrics { header, metrics }
                        }
                        Ok((header, FrameBody::SetPlan(frame))) => {
                            RecvBatchOutcome::SetPlan { header, frame }
                        }
                        Ok((header, FrameBody::ExecuteTask(frame))) => {
                            RecvBatchOutcome::ExecuteTask { header, frame }
                        }
                        Ok((header, FrameBody::Cancel)) => RecvBatchOutcome::Cancel { header },
                        Ok((header, FrameBody::TaskError(msg))) => {
                            RecvBatchOutcome::TaskError { header, msg }
                        }
                        Err(e) => RecvBatchOutcome::TransportError(e),
                    };
                }
                RecvOutcome::Empty => return RecvBatchOutcome::Empty,
                RecvOutcome::Detached => return RecvBatchOutcome::Detached,
            }
        }
    }

    /// Route one raw ring message through reassembly. Complete frames pass through untouched;
    /// `Chunk` frames accumulate per stream until `total_len` bytes arrived, at which point the
    /// reassembled inner frame returns for normal decoding.
    fn ingest(&self, bytes: Vec<u8>) -> Result<Option<Vec<u8>>, DataFusionError> {
        let header = MppFrameHeader::parse(&bytes)?;
        let kind = header.kind()?;
        if kind != MppFrameKind::Chunk {
            if kind == MppFrameKind::Eof {
                // The stream is over; drop any prefix its producer abandoned mid-frame.
                self.assemblies
                    .lock()
                    .unwrap()
                    .remove(&PhysicalStreamKey::new(
                        header.sender_proc(),
                        header.data_stream(),
                    ));
            }
            return Ok(Some(bytes));
        }
        let (total_len, offset, data) = parse_chunk_frame(&bytes)?;
        if total_len < MPP_FRAME_HEADER_SIZE
            || offset
                .checked_add(data.len())
                .is_none_or(|end| end > total_len)
        {
            return Err(DataFusionError::Internal(format!(
                "mpp: chunk bounds exceed the frame ({offset} + {} > {total_len})",
                data.len()
            )));
        }
        let key = PhysicalStreamKey::new(header.sender_proc(), header.data_stream());
        let mut assemblies = self.assemblies.lock().unwrap();
        if offset == 0 {
            // Start of a new frame; replaces any abandoned predecessor for the same stream.
            let mut buf = Vec::new();
            // `try_reserve` so a corrupt total_len surfaces as a query error, not an abort.
            buf.try_reserve_exact(total_len).map_err(|e| {
                DataFusionError::ResourcesExhausted(format!(
                    "mpp: cannot buffer a {total_len}-byte reassembled frame: {e}"
                ))
            })?;
            buf.extend_from_slice(data);
            if buf.len() == total_len {
                return Ok(Some(buf));
            }
            assemblies.insert(key, FrameAssembly { total_len, buf });
            return Ok(None);
        }
        let Some(assembly) = assemblies.get_mut(&key) else {
            return Err(DataFusionError::Internal(format!(
                "mpp: continuation chunk (offset {offset}) with no first chunk for \
                 sender {} stage {} task {} partition {}",
                key.sender_proc, key.stream.stage_id, key.stream.task_id, key.stream.partition,
            )));
        };
        if assembly.total_len != total_len || assembly.buf.len() != offset {
            return Err(DataFusionError::Internal(format!(
                "mpp: chunk out of sequence (have {} of {}, got offset {offset} of {total_len})",
                assembly.buf.len(),
                assembly.total_len
            )));
        }
        assembly.buf.extend_from_slice(data);
        if assembly.buf.len() == total_len {
            let assembly = assemblies.remove(&key).expect("assembly present");
            return Ok(Some(assembly.buf));
        }
        Ok(None)
    }
}

/// Decoded result of an [`MppReceiver::try_recv_batch`]. Carries the
/// parsed [`MppFrameHeader`] so the drain thread can route the payload to
/// the right `(stage_id, task_id, partition)` channel buffer.
#[derive(Debug)]
pub(super) enum RecvBatchOutcome {
    Batch {
        header: MppFrameHeader,
        batch: RecordBatch,
    },
    /// A payload-less `Eof` frame for `header.(stage_id, task_id, partition)`. The
    /// underlying shm_mq queue is still attached. The sender is just
    /// signalling that this logical channel is done, so we can EOF
    /// per-channel without dropping the whole queue.
    Eof {
        header: MppFrameHeader,
    },
    /// One work unit for the task named by `header.(stage_id, partition=task)`.
    WorkUnit {
        header: MppFrameHeader,
        unit: pb::WorkUnit,
    },
    /// Every feed of the task named by the header is done; its channels close.
    FeedEof {
        header: MppFrameHeader,
    },
    /// The plan for the task named by `header.(stage_id, partition=task)`.
    SetPlan {
        header: MppFrameHeader,
        frame: SetPlanFrame,
    },
    /// A task execution request named by `header.(stage_id, partition=task)`.
    ExecuteTask {
        header: MppFrameHeader,
        frame: ExecuteTaskFrame,
    },
    /// The collected metrics of the task named by the header.
    TaskMetrics {
        header: MppFrameHeader,
        metrics: pb::TaskMetrics,
    },
    /// Consumer abandoned the `header.(stage_id, task_id, partition)` stream; its producer stops sending it.
    Cancel {
        header: MppFrameHeader,
    },
    Empty,
    Detached,
    /// An explicitly transmitted error from a remote worker for a specific task (e.g. panic or query error).
    /// The transport channel itself is perfectly healthy and may continue to yield frames.
    TaskError {
        header: MppFrameHeader,
        msg: String,
    },
    /// A fatal structural or protocol error on the local side (e.g. corrupt bytes, IPC
    /// decode failure). The transport channel is unusable.
    TransportError(DataFusionError),
}

/// Per-`(sender_proc, stage_id, task_id, partition)` channel buffer registry owned by a cooperative
/// [`DrainHandle`]. Demultiplexes frames by the [`MppFrameHeader`] prefix into `map`.
/// `try_drain_pass` looks up the right channel buffer on every frame and pushes the payload into
/// it. Consumers waiting on a given key only see frames matching that key.
///
/// Each entry is a `DrainBuffer::new(1)`: exactly one sender_proc emits frames for any given
/// channel. Per-channel EOF flows via the `Eof` frame demuxed onto the matching buffer; query-
/// teardown unblock flows via [`DrainHandle::cancel_channel_buffers`] from the handle's `Drop`.
#[derive(Default)]
struct ChannelBufferRegistry {
    /// Keyed by `(sender_proc, stage_id, task_id, partition)`. The unified inbox carries frames
    /// from every peer, so each `(stage, task, partition)` consumer gets its own per-sender
    /// buffer. This preserves the implicit "one stream per sender" semantics that
    /// `WorkerConnection::execute` consumers rely on.
    map: HashMap<PhysicalStreamKey, Arc<DrainBuffer>>,
    /// Whether the receiver detached (or errored) before draining cleanly. Channels
    /// fail at registration time too, so a consumer that registers after the detach
    /// does not wait on a channel nothing will ever fill.
    dead_inbox: bool,
}

/// Per-sender-proc drain: stashes the receivers and polls them inline from the cooperative spin
/// (no background thread), demuxing each frame into a per-`(stage_id, task_id, partition)` channel buffer.
///
/// Inline polling is the production requirement: pgrx's `check_active_thread` guard panics on any
/// pg FFI call (including `shm_mq_receive`) from a non-backend thread, so the drain work has to
/// run on the backend thread. Tests that need a true thread-backed drain use
/// [`ThreadedDrainHandle`] instead.
///
/// On drop, the handle cancels every channel buffer so any consumer blocked on `try_pop` unblocks
/// with `Eof` — the drain can therefore never outlive its query, even on a panicked teardown.
pub struct DrainHandle {
    /// Per-(stage_id, task_id, partition) channel buffer registry. Populated lazily on first frame for a
    /// channel, or up-front by callers (e.g. `WorkerConnection::execute`) that need a
    /// buffer to wait on before any frame arrives.
    channel_buffers: Mutex<ChannelBufferRegistry>,
    /// Receivers owned by the handle and polled inline from `DrainGatherStream::poll_next` via
    /// [`Self::try_drain_pass`]. The `Mutex` is for interior mutability: `try_drain_pass(&self)`
    /// marks each slot as `None` after observing `Detached` so subsequent passes skip the dead
    /// receiver. `BatchChannelReceiver: Send + Sync` makes `Vec<Option<MppReceiver>>: Sync`
    /// already, so the lock is no longer doubling as the `Sync` provider — replacing it with a
    /// non-locking primitive would need either an atomic per-slot detached flag or accepting
    /// that detached receivers get polled once per pass (fast-returning `Detached`). The lock
    /// is uncontended in production (single backend thread) so the marginal cost is in the
    /// type system, not the runtime.
    coop_receivers: Mutex<Vec<Option<MppReceiver>>>,
    /// Worker-side destination of `WorkUnit` frames, keyed `(stage_id, task_number)`. Frames
    /// arriving before the embedder registers a task's channels buffer in `Pending`; `FeedEof`
    /// drops the senders so the consuming feed streams end, the wire analog of Flight closing
    /// its coordinator stream.
    feed_registry: Mutex<FeedRegistry>,
    /// Leader-side destination of `TaskMetrics` frames: `(stage_id, task_number, metrics)`.
    task_metrics_tx: tokio::sync::mpsc::UnboundedSender<(u32, u32, pb::TaskMetrics)>,
    task_metrics_rx:
        Mutex<Option<tokio::sync::mpsc::UnboundedReceiver<(u32, u32, pb::TaskMetrics)>>>,
    /// Worker-side destination of `SetPlan` frames, keyed `(stage_id, task_number)`. Same
    /// pending-or-waiting shape as the feed registry: a frame arriving before the task asks
    /// buffers in `Pending`; a task asking first parks a oneshot the drain fulfills.
    set_plan_registry: Mutex<SetPlanRegistry>,
    /// Worker-side destination of `ExecuteTask` frames, keyed `(stage_id, task_number)`.
    execute_task_registry: Mutex<ExecuteTaskRegistry>,
    /// `(stage_id, task_id, partition)` streams this proc's consumers abandoned, learned from inbound
    /// `Cancel` frames. A producer blocked on a full outbound checks it in its send spin and ends
    /// that stream cleanly, so a consumer that stopped reading early doesn't leave it spinning to
    /// the statement timeout.
    cancelled_streams: Mutex<HashSet<MppDataStreamKey>>,
}

#[derive(Default)]
struct FeedRegistry {
    map: HashMap<(u32, u32), FeedSlot>,
    /// Set when the inbox scope died: feeds come from a peer proc, so a dead inbox means no
    /// further units or `FeedEof` can arrive. Registered channels get the failure pushed in;
    /// later registrations fail immediately.
    dead: Option<String>,
}

enum FeedSlot {
    /// Frames that arrived before the embedder registered the task's channels.
    Pending {
        units: Vec<pb::WorkUnit>,
        done: bool,
    },
    Active(RemoteWorkUnitFeedTxs),
}

/// Push one decoded unit into the channel its `(feed id, partition)` names. A missing channel is
/// not an error: the same tolerance the Flight worker applies to its stream (a feed the plan
/// does not declare is dropped).
fn forward_unit(senders: &RemoteWorkUnitFeedTxs, unit: pb::WorkUnit) {
    let Ok(id) = deserialize_uuid(&unit.id) else {
        return;
    };
    let Some(tx) = senders.get(&(id, unit.partition as usize)) else {
        return;
    };
    // The feed channels carry the protocol's `WorkUnitMsg`, so the decoded proto frame is mapped
    // into it the same way the gRPC worker's `decode_work_unit` does.
    let _ = tx.send(Ok(WorkUnitMsg {
        id,
        partition: unit.partition as usize,
        body: MaybeEncoded::Encoded(unit.body),
        created_timestamp_unix_nanos: unit.created_timestamp_unix_nanos as usize,
        sent_timestamp_unix_nanos: unit.sent_timestamp_unix_nanos as usize,
        received_timestamp_unix_nanos: unit.received_timestamp_unix_nanos as usize,
        processed_timestamp_unix_nanos: unit.processed_timestamp_unix_nanos as usize,
    }));
}

fn fail_feed_senders(senders: &RemoteWorkUnitFeedTxs, reason: &str) {
    for tx in senders.values() {
        let _ = tx.send(Err(DataFusionError::Execution(reason.to_string())));
    }
}

#[derive(Default)]
struct SetPlanRegistry {
    map: HashMap<(u32, u32), SetPlanSlot>,
    /// Set when the inbox scope died: plans come from the leader's proc, so a dead inbox means
    /// no plan can arrive. Parked takers get the failure; later takers fail immediately.
    dead: Option<String>,
}

enum SetPlanSlot {
    /// A frame that arrived before the task asked for it.
    Pending(SetPlanFrame),
    /// A task that asked before its frame arrived.
    Waiting(tokio::sync::oneshot::Sender<Result<SetPlanFrame, DataFusionError>>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct IncomingExecuteTaskRequest {
    pub sender_proc: u32,
    pub frame: ExecuteTaskFrame,
}

pub type ExecuteTaskRx =
    tokio::sync::mpsc::UnboundedReceiver<Result<IncomingExecuteTaskRequest, DataFusionError>>;

#[derive(Default)]
struct ExecuteTaskRegistry {
    map: HashMap<(u32, u32), ExecuteTaskSlot>,
    dead: Option<String>,
}

enum ExecuteTaskSlot {
    Pending(Vec<Result<IncomingExecuteTaskRequest, DataFusionError>>),
    Active(tokio::sync::mpsc::UnboundedSender<Result<IncomingExecuteTaskRequest, DataFusionError>>),
}

impl DrainHandle {
    /// Construct a cooperative drain handle. Channel buffers are populated lazily by
    /// [`Self::try_drain_pass`] when a frame arrives, or up-front by [`Self::register_channel`]
    /// when a consumer needs a buffer to wait on before any frame has come in.
    pub(super) fn cooperative(receivers: Vec<MppReceiver>) -> Self {
        let wrapped = receivers.into_iter().map(Some).collect();
        let (task_metrics_tx, task_metrics_rx) = tokio::sync::mpsc::unbounded_channel();
        Self {
            channel_buffers: Mutex::new(ChannelBufferRegistry::default()),
            coop_receivers: Mutex::new(wrapped),
            feed_registry: Mutex::new(FeedRegistry::default()),
            task_metrics_tx,
            task_metrics_rx: Mutex::new(Some(task_metrics_rx)),
            set_plan_registry: Mutex::new(SetPlanRegistry::default()),
            execute_task_registry: Mutex::new(ExecuteTaskRegistry::default()),
            cancelled_streams: Mutex::new(HashSet::default()),
        }
    }

    /// Record a `Cancel` frame: the consumer abandoned this data stream, so this proc's producer
    /// of it stops. Idempotent.
    fn note_cancel(&self, stream: MppDataStreamKey) {
        self.cancelled_streams.lock().unwrap().insert(stream);
    }

    /// Whether a consumer cancelled this data stream. Read by the send spin to end a producer's
    /// stream cleanly when the consumer stopped reading.
    pub(super) fn stream_cancelled(&self, stream: MppDataStreamKey) -> bool {
        self.cancelled_streams.lock().unwrap().contains(&stream)
    }

    /// Take the receiving end of the `TaskMetrics` frame stream. The embedder (the leader)
    /// drains it into its metrics store; the first caller gets it, later calls get `None`.
    pub(super) fn take_task_metrics_receiver(
        &self,
    ) -> Option<tokio::sync::mpsc::UnboundedReceiver<(u32, u32, pb::TaskMetrics)>> {
        self.task_metrics_rx.lock().unwrap().take()
    }

    /// Install the senders of one task's feed channels, flushing any units that arrived first.
    /// If the task's `FeedEof` (or the inbox death) already came through, the senders drop (or
    /// fail) immediately so the consuming streams terminate instead of waiting forever.
    pub(super) fn register_work_unit_senders(
        &self,
        stage_id: u32,
        task_number: u32,
        senders: RemoteWorkUnitFeedTxs,
    ) {
        let mut registry = self.feed_registry.lock().unwrap();
        if let Some(reason) = &registry.dead {
            fail_feed_senders(&senders, reason);
            return;
        }
        match registry.map.remove(&(stage_id, task_number)) {
            Some(FeedSlot::Pending { units, done }) => {
                for unit in units {
                    forward_unit(&senders, unit);
                }
                if !done {
                    registry
                        .map
                        .insert((stage_id, task_number), FeedSlot::Active(senders));
                }
            }
            Some(FeedSlot::Active(_)) | None => {
                registry
                    .map
                    .insert((stage_id, task_number), FeedSlot::Active(senders));
            }
        }
    }

    fn route_work_unit(&self, stage_id: u32, task_number: u32, unit: pb::WorkUnit) {
        let mut registry = self.feed_registry.lock().unwrap();
        if registry.dead.is_some() {
            return;
        }
        match registry.map.get_mut(&(stage_id, task_number)) {
            Some(FeedSlot::Active(senders)) => forward_unit(senders, unit),
            Some(FeedSlot::Pending { units, .. }) => units.push(unit),
            None => {
                registry.map.insert(
                    (stage_id, task_number),
                    FeedSlot::Pending {
                        units: vec![unit],
                        done: false,
                    },
                );
            }
        }
    }

    fn close_feeds(&self, stage_id: u32, task_number: u32) {
        let mut registry = self.feed_registry.lock().unwrap();
        match registry.map.get_mut(&(stage_id, task_number)) {
            Some(FeedSlot::Active(_)) => {
                // Dropping the senders is the close: the consuming streams see end-of-input.
                registry.map.remove(&(stage_id, task_number));
            }
            Some(FeedSlot::Pending { done, .. }) => *done = true,
            None => {
                registry.map.insert(
                    (stage_id, task_number),
                    FeedSlot::Pending {
                        units: Vec::new(),
                        done: true,
                    },
                );
            }
        }
    }

    /// Route one decoded `SetPlan` frame to whoever asked for `(stage_id, task_number)`, or
    /// buffer it until they do. A duplicate for an already-buffered slot keeps the first frame.
    pub(super) fn route_set_plan(&self, stage_id: u32, task_number: u32, frame: SetPlanFrame) {
        let mut registry = self.set_plan_registry.lock().unwrap();
        match registry.map.remove(&(stage_id, task_number)) {
            Some(SetPlanSlot::Waiting(tx)) => {
                let _ = tx.send(Ok(frame));
            }
            Some(pending @ SetPlanSlot::Pending(_)) => {
                log::debug!(
                    "mpp: duplicate SetPlan frame for stage {stage_id} task {task_number}; \
                     keeping the first"
                );
                registry.map.insert((stage_id, task_number), pending);
            }
            None => {
                registry
                    .map
                    .insert((stage_id, task_number), SetPlanSlot::Pending(frame));
            }
        }
    }

    /// Take the plan delivered for `(stage_id, task_number)`, waiting for its `SetPlan` frame if
    /// it has not arrived yet. Something on this proc must keep draining (a pump or a
    /// cooperative send spin) or the wait starves; same contract as the feed channels.
    pub(super) async fn take_set_plan(
        &self,
        stage_id: u32,
        task_number: u32,
    ) -> Result<SetPlanFrame, DataFusionError> {
        let rx = {
            let mut registry = self.set_plan_registry.lock().unwrap();
            if let Some(reason) = &registry.dead {
                return Err(DataFusionError::Execution(reason.clone()));
            }
            match registry.map.remove(&(stage_id, task_number)) {
                Some(SetPlanSlot::Pending(frame)) => return Ok(frame),
                Some(SetPlanSlot::Waiting(_)) => {
                    return Err(DataFusionError::Internal(format!(
                        "mpp: two takers for the SetPlan frame of stage {stage_id} task \
                         {task_number}"
                    )));
                }
                None => {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    registry
                        .map
                        .insert((stage_id, task_number), SetPlanSlot::Waiting(tx));
                    rx
                }
            }
        };
        rx.await.map_err(|_| {
            DataFusionError::Execution(
                "mpp: transport torn down before this task's plan arrived".to_string(),
            )
        })?
    }

    /// Mark the data plane as departed: fail every registered channel still waiting on
    /// its `Eof`, and remember that the inbox is dead so later data registrations fail immediately.
    /// Channels whose `Eof` already arrived are untouched: a detach after a clean drain is the
    /// normal end of life for a ring. Idempotent.
    ///
    /// The feed, plan, and request registries stay live. Their frames come from control
    /// senders, which never join a ring's `sender_count`, so "the last data sender dropped"
    /// says nothing about them: a peer that completed normally has EOF'd everything it served,
    /// while this proc may still owe the leader work it has not been asked for yet.
    fn detach_data_scope(&self, reason: &str) {
        let to_fail = {
            let mut guard = self
                .channel_buffers
                .lock()
                .expect("DrainHandle channel_buffers mutex poisoned");
            if guard.dead_inbox {
                return;
            }
            guard.dead_inbox = true;
            guard.map.values().cloned().collect::<Vec<_>>()
        };
        for buf in to_fail {
            buf.fail_pending(reason);
        }
    }

    /// Hard scope death: [`Self::detach_data_scope`] plus the control-plane registries. For
    /// ring corruption and embedder-driven teardown, where no further frame of any kind can be
    /// trusted to arrive.
    fn fail_scope(&self, reason: &str) {
        self.detach_data_scope(reason);
        let mut registry = self.feed_registry.lock().unwrap();
        registry.dead = Some(reason.to_string());
        for (_, slot) in registry.map.drain() {
            if let FeedSlot::Active(senders) = slot {
                fail_feed_senders(&senders, reason);
            }
        }
        drop(registry);
        let mut plans = self.set_plan_registry.lock().unwrap();
        plans.dead = Some(reason.to_string());
        for (_, slot) in plans.map.drain() {
            if let SetPlanSlot::Waiting(tx) = slot {
                let _ = tx.send(Err(DataFusionError::Execution(reason.to_string())));
            }
        }
        let mut execs = self.execute_task_registry.lock().unwrap();
        execs.dead = Some(reason.to_string());
        for (_, slot) in execs.map.drain() {
            if let ExecuteTaskSlot::Active(tx) = slot {
                let _ = tx.send(Err(DataFusionError::Execution(reason.to_string())));
            }
        }
    }

    pub(super) fn route_execute_task(
        &self,
        stage_id: u32,
        task_number: u32,
        sender_proc: u32,
        frame: ExecuteTaskFrame,
    ) {
        let mut guard = self.execute_task_registry.lock().unwrap();
        if guard.dead.is_some() {
            return;
        }
        let slot = guard
            .map
            .entry((stage_id, task_number))
            .or_insert_with(|| ExecuteTaskSlot::Pending(Vec::new()));
        let item = Ok(IncomingExecuteTaskRequest { sender_proc, frame });
        match slot {
            ExecuteTaskSlot::Pending(queue) => queue.push(item),
            ExecuteTaskSlot::Active(tx) => {
                let _ = tx.send(item);
            }
        }
    }

    pub(super) fn take_execute_task_rx(
        &self,
        stage_id: u32,
        task_number: u32,
    ) -> Result<ExecuteTaskRx, DataFusionError> {
        let mut guard = self.execute_task_registry.lock().unwrap();
        if let Some(msg) = &guard.dead {
            return Err(DataFusionError::Execution(msg.clone()));
        }
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        if let Some(slot) = guard.map.get_mut(&(stage_id, task_number)) {
            match slot {
                ExecuteTaskSlot::Active(_) => {
                    return Err(DataFusionError::Execution(format!(
                        "take_execute_task_rx: execute_task_rx for stage {stage_id} task {task_number} already taken"
                    )));
                }
                ExecuteTaskSlot::Pending(queue) => {
                    for item in queue.drain(..) {
                        let _ = tx.send(item);
                    }
                    *slot = ExecuteTaskSlot::Active(tx);
                }
            }
        } else {
            guard
                .map
                .insert((stage_id, task_number), ExecuteTaskSlot::Active(tx));
        }
        Ok(rx)
    }

    /// Register (or look up) the channel buffer for one producer's data stream.
    /// The returned `Arc<DrainBuffer>` is the canonical destination for frames matching
    /// that key: `try_drain_pass` pushes into the same entry on every `Batch { header, .. }`
    /// whose `header.sender_proc()` / `stage_id` / `partition` matches.
    pub(super) fn register_data_channel(
        &self,
        sender_proc: u32,
        stream: MppDataStreamKey,
    ) -> Arc<DrainBuffer> {
        let mut guard = self
            .channel_buffers
            .lock()
            .expect("DrainHandle channel_buffers mutex poisoned");
        let scope_dead = guard.dead_inbox;
        let buf = guard
            .map
            .entry(PhysicalStreamKey::new(sender_proc, stream))
            .or_insert_with(|| {
                // num_sources stays 1: each (sender_proc, stage, task, partition) tuple has
                // exactly one upstream (the named sender), even though the underlying
                // inbox is shared across all senders.
                DrainBuffer::new(1)
            })
            .clone();
        drop(guard);
        if scope_dead {
            buf.fail_pending(
                "transport receiver detached before this channel's EOF; the producer went away",
            );
        }
        buf
    }

    /// Cancel every registered channel buffer. Called from `Drop` to unblock any consumer waiting on
    /// a channel buffer when the handle goes away mid-query.
    ///
    /// Collects buffer handles under the registry lock, then notifies after releasing
    /// it. Notifying inline would block any concurrent [`Self::register_channel`] for
    /// as long as it takes to acquire `DrainBuffer::inner` N times. Fine today (single
    /// backend thread), but cheap insurance against the multi-thread variant landing
    /// later.
    fn cancel_channel_buffers(&self) {
        let to_cancel = {
            let guard = self
                .channel_buffers
                .lock()
                .expect("DrainHandle channel_buffers mutex poisoned");
            guard.map.values().cloned().collect::<Vec<_>>()
        };
        for buf in to_cancel {
            buf.cancel();
        }
    }

    /// Pull batches from each live receiver and demux them into the per-`(stage_id, task_id, partition)`
    /// channel buffer registry. Called from `DrainGatherStream::poll_next` and from
    /// `MppSender::send_batch`'s cooperative spin. Drain work happens on the backend thread
    /// (pgrx-safe). No-op for thread-backed handles.
    ///
    /// Each pass drains *every available* batch from each receiver (up to a safety cap). Pulling
    /// only one batch per source per call would mean that under steady producer pressure the
    /// cooperative sender's spin-loop can't keep up: we'd fall N:1 behind peers' sends and the
    /// mesh would stall once any queue fills. Draining until the receiver reports `Empty` bounds
    /// each pass by queue depth rather than by spin-loop iteration count.
    ///
    /// Returns `Ok(())` once every cooperative receiver has been pulled until `Empty` (or
    /// detached). Errors propagate as `Err` so a transport-level failure surfaces at the call
    /// site rather than getting silently dropped.
    ///
    /// Routing rules per outcome:
    /// - `Batch { header, batch }`: look up (or lazily create) the
    ///   `(header.sender_proc(), header.stage_id, header.task_id, header.partition)` channel
    ///   buffer and push `batch`.
    /// - `Eof { header }`: per-channel EOF. Resolve the channel buffer and call
    ///   `notify_source_done`. Other channels on the same queue keep flowing,
    ///   so the receiver slot stays live.
    /// - `Detached`: the receiver's data senders are gone. Fail the channels still
    ///   waiting on their `Eof`; the slot stays live for control frames.
    /// - `Error`: queue-wide shutdown. Notify every registered channel buffer and
    ///   the control registries, mark the handle detached, and drop the slot.
    pub fn try_drain_pass(&self) -> Result<(), DataFusionError> {
        // Bound per-source pulls per call. The upper limit exists to give
        // the caller a chance to re-try its own send between drains —
        // otherwise a proc with a very fast peer could drain
        // indefinitely on one source and starve its own outbound.
        const MAX_BATCHES_PER_SOURCE_PER_PASS: usize = 256;

        let mut slots = self.coop_receivers.lock().unwrap();
        for slot in slots.iter_mut() {
            let Some(rx) = slot.as_mut() else {
                continue;
            };
            for _ in 0..MAX_BATCHES_PER_SOURCE_PER_PASS {
                match rx.try_recv_batch() {
                    RecvBatchOutcome::Batch { header, batch } => {
                        let buf =
                            self.register_data_channel(header.sender_proc(), header.data_stream());
                        buf.push_batch(batch);
                    }
                    RecvBatchOutcome::Eof { header } => {
                        let buf =
                            self.register_data_channel(header.sender_proc(), header.data_stream());
                        buf.notify_source_done();
                        // Other channels may still flow on this queue, so the receiver slot
                        // stays live.
                    }
                    RecvBatchOutcome::WorkUnit { header, mut unit } => {
                        set_received_time(&mut unit);
                        self.route_work_unit(header.stage_id, header.partition, unit);
                    }
                    RecvBatchOutcome::FeedEof { header } => {
                        self.close_feeds(header.stage_id, header.partition);
                    }
                    RecvBatchOutcome::TaskMetrics { header, metrics } => {
                        // The embedder may have dropped the receiver; metrics are best-effort.
                        let _ =
                            self.task_metrics_tx
                                .send((header.stage_id, header.partition, metrics));
                    }
                    RecvBatchOutcome::SetPlan { header, frame } => {
                        self.route_set_plan(header.stage_id, header.partition, frame);
                    }
                    RecvBatchOutcome::ExecuteTask { header, frame } => {
                        self.route_execute_task(
                            header.stage_id,
                            header.partition,
                            header.sender_proc(),
                            frame,
                        );
                    }
                    RecvBatchOutcome::Cancel { header } => {
                        self.note_cancel(header.data_stream());
                    }
                    RecvBatchOutcome::TaskError { header, msg } => {
                        // The remote worker sent a crash message, but the transport is intact.
                        // Fail the scope to unblock waiters, but leave this receiver in the slot:
                        // the worker will likely close the channel next, yielding `Detached`.
                        self.fail_scope(&format!(
                            "remote task {}.{} failed: {}",
                            header.stage_id, header.partition, msg
                        ));
                    }
                    RecvBatchOutcome::Empty => break,
                    RecvBatchOutcome::Detached => {
                        // Every data sender to this receiver is gone; that is also how a
                        // peer's normal completion looks. Its EOFs were drained ahead of the
                        // detach (the ring is FIFO), so failing only the channels still
                        // waiting on their `Eof` hits exactly the work that depended on a
                        // departed producer, which would otherwise spin on
                        // `try_pop -> None` forever. Control senders stay out of the ring's
                        // sender count and can still publish, so the slot stays live for the
                        // leader-origin frames a fragment may still be waiting on (requests,
                        // plans, feed units); `detach_data_scope` is idempotent, so the
                        // passes that keep observing the latch cost one failed recv each.
                        self.detach_data_scope(
                            "transport receiver detached before this channel's EOF; the \
                             producer went away",
                        );
                        break;
                    }
                    RecvBatchOutcome::TransportError(e) => {
                        // The channel is corrupted or broken. Polling it again is dangerous.
                        // We evict this receiver (`*slot = None`) to prevent future polls.
                        // The error also propagates to this caller directly.
                        *slot = None;
                        self.fail_scope(&format!("transport receiver failed: {e}"));
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }
}

impl Drop for DrainHandle {
    fn drop(&mut self) {
        // Unblock any consumer blocked on a channel buffer when the handle is torn down before EOF
        // flows naturally (e.g. a query error en route to ExecEndCustomScan).
        self.cancel_channel_buffers();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{
        Int32Array, Int64Array, StringArray, StringViewArray, UInt64Array,
    };
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc as StdArc;
    use std::thread;

    use std::thread::JoinHandle;

    /// SPSC channel pair used in unit tests (bounded capacity, exercising backpressure).
    pub(super) fn in_proc_channel(capacity: usize) -> (InProcSender, InProcReceiver) {
        let (tx, rx) = std::sync::mpsc::sync_channel::<Vec<u8>>(capacity);
        (
            InProcSender {
                tx,
                send_lock: tokio::sync::Mutex::new(()),
            },
            InProcReceiver { rx: Mutex::new(rx) },
        )
    }

    pub(super) struct InProcSender {
        tx: std::sync::mpsc::SyncSender<Vec<u8>>,
        send_lock: tokio::sync::Mutex<()>,
    }

    pub(super) struct InProcReceiver {
        rx: Mutex<std::sync::mpsc::Receiver<Vec<u8>>>,
    }

    impl BatchChannelSender for InProcSender {
        fn send_bytes(&self, bytes: &[u8]) -> Result<(), DataFusionError> {
            self.tx.send(bytes.to_vec()).map_err(|_| {
                DataFusionError::Execution("mpp: in-proc channel detached during send".into())
            })
        }

        fn try_send_bytes(&self, bytes: &[u8]) -> Result<bool, DataFusionError> {
            match self.tx.try_send(bytes.to_vec()) {
                Ok(()) => Ok(true),
                Err(std::sync::mpsc::TrySendError::Full(_)) => Ok(false),
                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                    Err(DataFusionError::Execution(
                        "mpp: in-proc channel detached during try_send".into(),
                    ))
                }
            }
        }

        fn send_lock(&self) -> &tokio::sync::Mutex<()> {
            &self.send_lock
        }
    }

    impl BatchChannelReceiver for InProcReceiver {
        fn try_recv(&self) -> RecvOutcome {
            let rx = self.rx.lock().expect("InProcReceiver mutex poisoned");
            match rx.try_recv() {
                Ok(bytes) => RecvOutcome::Bytes(bytes),
                Err(std::sync::mpsc::TryRecvError::Empty) => RecvOutcome::Empty,
                Err(std::sync::mpsc::TryRecvError::Disconnected) => RecvOutcome::Detached,
            }
        }
    }

    #[test]
    fn take_execute_task_rx_second_take_errors() {
        let drain = DrainHandle::cooperative(vec![]);
        let rx1 = drain.take_execute_task_rx(1, 0);
        assert!(rx1.is_ok());
        let rx2 = drain.take_execute_task_rx(1, 0);
        assert!(rx2.is_err());
        assert!(rx2.unwrap_err().to_string().contains("already taken"));
    }

    #[tokio::test]
    async fn send_task_error_best_effort_fails_drain_scope() {
        let (tx, rx) = in_proc_channel(8);
        let header = MppFrameHeader::task_metrics(1, 0, 1);
        let sender = MppSender::new(Arc::new(tx)).clone_with_header(header);
        let receiver = MppReceiver::new(Box::new(rx));
        let drain = DrainHandle::cooperative(vec![receiver]);

        let mut task_rx = drain.take_execute_task_rx(1, 0).unwrap();

        sender
            .send_task_error_best_effort("worker failed: out of memory")
            .await
            .unwrap();

        drain.try_drain_pass().unwrap();

        let recv_item = task_rx.recv().await;
        assert!(recv_item.is_some());
        let err = recv_item.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("worker failed: out of memory"),
            "expected error string, got: {err}"
        );
    }

    impl DrainBuffer {
        /// Block until a batch is available, EOF is reached, or the buffer is cancelled.
        ///
        /// INVARIANT: any already-buffered batch is returned *before* honoring either
        /// cancellation or all-sources-done. Reordering the queue pop ahead of the cancel/eof
        /// check would silently drop buffered data on an otherwise-clean shutdown; the
        /// `drain_buffer_drains_buffered_before_eof` test locks this in.
        fn pop_front(&self) -> DrainItem {
            let mut guard = self.inner.lock().expect("DrainBuffer mutex poisoned");
            loop {
                if let Some(batch) = guard.queue.pop_front() {
                    return DrainItem::Batch(batch);
                }
                if let Some(msg) = &guard.failed {
                    return DrainItem::Failed(msg.clone());
                }
                if guard.cancelled || guard.sources_done >= guard.num_sources {
                    return DrainItem::Eof;
                }
                guard = self.cond.wait(guard).expect("DrainBuffer mutex poisoned");
            }
        }
    }

    impl MppSender {
        /// Construct a sender with the default `(stage_id=0, task_id=0, partition=0)` header. Used where
        /// the header carries no actionable routing info.
        fn new(channel: Arc<dyn BatchChannelSender>) -> Self {
            Self::with_header(
                channel,
                MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 0), 0),
            )
        }

        /// Stats-less wrapper around `send_batch_traced`. Production call sites
        /// (`ShuffleStream::process_batch`) always pass a `SendBatchStats` so per-peer
        /// wall-time shows up in the EOF trace. Wraps the async send in a tiny current-thread
        /// Tokio runtime so `#[test]` functions don't have to be `#[tokio::test]` and the
        /// OS-thread-spawning test harnesses don't have to plumb an async runtime themselves.
        fn send_batch(&self, batch: &RecordBatch) -> Result<(), DataFusionError> {
            let mut stats = SendBatchStats::default();
            let rt = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("test tokio runtime build");
            rt.block_on(self.send_batch_traced(batch, &mut stats))
        }
    }

    /// Configuration for `spawn_drain_thread`. pgrx panics on any pg FFI call (including
    /// `shm_mq_receive`) from a non-backend thread, so production never spawns a drain thread —
    /// see [`DrainHandle::cooperative`] for the cooperative path.
    struct DrainConfig {
        /// Receivers to drain. Ownership moves into the spawned thread.
        receivers: Vec<MppReceiver>,
        /// Destination buffer.
        buffer: Arc<DrainBuffer>,
        /// How long to sleep when every receiver is empty but some are still attached. Tuning:
        /// small values reduce end-of-batch latency but raise CPU; 1 ms is a safe default until
        /// we integrate with WaitLatch.
        idle_sleep: Duration,
    }

    impl DrainConfig {
        fn new(receivers: Vec<MppReceiver>, buffer: Arc<DrainBuffer>) -> Self {
            Self {
                receivers,
                buffer,
                idle_sleep: Duration::from_millis(1),
            }
        }
    }

    /// Spawn the dedicated drain thread. The thread round-robins through every receiver with
    /// non-blocking `try_recv`, pushes decoded batches into `buffer`, and marks each source done
    /// as soon as it observes a detach or decode error. When every source is done, the thread
    /// exits.
    fn spawn_drain_thread(config: DrainConfig) -> JoinHandle<Result<(), DataFusionError>> {
        thread::spawn(move || drain_loop(config))
    }

    /// RAII wrapper: owns the drain thread's `JoinHandle` and the buffer it writes into.
    /// `Drop` cancels the buffer (unblocking the consumer) and joins the thread, so the thread
    /// can never outlive the test scope even on a panic.
    struct ThreadedDrainHandle {
        buffer: Arc<DrainBuffer>,
        join: Mutex<Option<JoinHandle<Result<(), DataFusionError>>>>,
    }

    impl ThreadedDrainHandle {
        fn spawn(config: DrainConfig) -> Self {
            let buffer = Arc::clone(&config.buffer);
            let join = spawn_drain_thread(config);
            Self {
                buffer,
                join: Mutex::new(Some(join)),
            }
        }
    }

    impl Drop for ThreadedDrainHandle {
        fn drop(&mut self) {
            self.buffer.cancel();
            if let Some(join) = self.join.lock().unwrap().take() {
                let _ = join.join();
            }
        }
    }

    /// Test-only thread-backed drain. Writes every observed frame into a single shared
    /// [`DrainBuffer`] with `num_sources = N`. Per-channel `Eof` frames are treated as "this source
    /// is done" rather than "this logical channel within the source is done"; sufficient for unit
    /// tests that don't exercise per-channel demux. Production drains route through
    /// [`DrainHandle::try_drain_pass`] (cooperative variant), which keys on the frame header. Tests
    /// that need to validate production demux must use [`DrainHandle::cooperative`] and call
    /// `try_drain_pass` directly.
    fn drain_loop(config: DrainConfig) -> Result<(), DataFusionError> {
        let DrainConfig {
            receivers,
            buffer,
            idle_sleep,
        } = config;

        let mut done = vec![false; receivers.len()];
        loop {
            // Observe cancellation before each pass so a `DrainHandle::drop` with
            // live peer senders tears down cleanly. Without this check, the drain
            // thread would spin `try_recv` forever because no source has detached.
            if buffer.is_cancelled() {
                return Ok(());
            }

            let mut got_any = false;
            let mut all_done = true;
            for (i, rx) in receivers.iter().enumerate() {
                if done[i] {
                    continue;
                }
                all_done = false;
                match rx.try_recv_batch() {
                    RecvBatchOutcome::Batch { header: _, batch } => {
                        got_any = true;
                        buffer.push_batch(batch);
                    }
                    RecvBatchOutcome::Eof { header: _ } => {
                        // Per-channel Eof frame: single-channel positional design
                        // treats it as a source-done signal. See `try_drain_pass`.
                        done[i] = true;
                        buffer.notify_source_done();
                    }
                    // Control frames have no place in the test-only single-buffer path.
                    RecvBatchOutcome::WorkUnit { .. }
                    | RecvBatchOutcome::FeedEof { .. }
                    | RecvBatchOutcome::TaskMetrics { .. }
                    | RecvBatchOutcome::SetPlan { .. }
                    | RecvBatchOutcome::ExecuteTask { .. }
                    | RecvBatchOutcome::Cancel { .. }
                    | RecvBatchOutcome::TaskError { .. } => {}
                    RecvBatchOutcome::Empty => {}
                    RecvBatchOutcome::Detached => {
                        done[i] = true;
                        buffer.notify_source_done();
                    }
                    RecvBatchOutcome::TransportError(e) => {
                        // Treat a decode error as a fatal detach for this source
                        // so the consumer can observe EOF and abort the query.
                        done[i] = true;
                        buffer.notify_source_done();
                        return Err(e);
                    }
                }
            }

            if all_done {
                return Ok(());
            }
            if !got_any {
                thread::sleep(idle_sleep);
            }
        }
    }

    fn sample_batch(rows: i32) -> RecordBatch {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let ids = Int32Array::from_iter_values(0..rows);
        let names = StringArray::from_iter_values((0..rows).map(|i| format!("n{i}")));
        RecordBatch::try_new(schema, vec![StdArc::new(ids), StdArc::new(names)]).unwrap()
    }

    #[test]
    fn frame_round_trips_a_batch_with_header() {
        let orig = sample_batch(64);
        let header = MppFrameHeader::batch(MppDataStreamKey::new(7, 0, 3), 0);
        let mut buf = Vec::with_capacity(1024);
        encode_frame_into(header, &orig, &mut buf).expect("encode_frame");

        let (parsed, body) = decode_frame(&buf).expect("decode_frame");
        assert_eq!(parsed, header);
        assert_eq!(parsed.kind().unwrap(), MppFrameKind::Batch);
        let FrameBody::Batch(decoded) = body else {
            panic!("Batch frame must carry a batch payload");
        };
        assert_eq!(decoded.num_rows(), 64);
        assert_eq!(decoded.schema(), orig.schema());
        assert_eq!(decoded.num_columns(), orig.num_columns());
        for col in 0..orig.num_columns() {
            assert_eq!(orig.column(col).as_ref(), decoded.column(col).as_ref());
        }
    }

    #[test]
    fn frame_round_trips_eof() {
        let mut buf = Vec::new();
        encode_eof_frame_into(MppDataStreamKey::new(2, 0, 5), 0, &mut buf).expect("encode_eof");
        assert_eq!(buf.len(), MPP_FRAME_HEADER_SIZE);

        let (header, body) = decode_frame(&buf).expect("decode_frame");
        assert_eq!(
            header,
            MppFrameHeader::eof(MppDataStreamKey::new(2, 0, 5), 0)
        );
        assert_eq!(header.kind().unwrap(), MppFrameKind::Eof);
        assert!(matches!(body, FrameBody::Eof));
    }

    #[test]
    fn frame_round_trips_cancel() {
        let header = MppFrameHeader::cancel(MppDataStreamKey::new(4, 0, 2), 0);
        let mut buf = vec![0u8; MPP_FRAME_HEADER_SIZE];
        header.write_to(&mut buf);

        let (parsed, body) = decode_frame(&buf).expect("decode_frame");
        assert_eq!(parsed, header);
        assert_eq!(parsed.kind().unwrap(), MppFrameKind::Cancel);
        assert_eq!(parsed.stage_id, 4);
        assert_eq!(parsed.partition, 2);
        assert!(matches!(body, FrameBody::Cancel));
    }

    #[test]
    fn drain_records_cancel_scoped_to_its_stream() {
        let drain = DrainHandle::cooperative(Vec::new());
        let cancelled = MppDataStreamKey::new(7, 0, 1);
        assert!(!drain.stream_cancelled(cancelled));
        drain.note_cancel(cancelled);
        assert!(drain.stream_cancelled(cancelled));
        assert!(!drain.stream_cancelled(MppDataStreamKey::new(7, 3, 1)));
        assert!(!drain.stream_cancelled(MppDataStreamKey::new(8, 0, 1)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn control_sender_ignores_matching_data_stream_cancel() {
        let drain = Arc::new(DrainHandle::cooperative(Vec::new()));
        // Control frames encode their target task in `partition`, so task 1 overlaps data
        // `(stage 7, task 0, partition 1)`.
        drain.note_cancel(MppDataStreamKey::new(7, 0, 1));
        let (tx, _rx) = in_proc_channel(1);
        let sender = MppSender::with_header(Arc::new(tx), MppFrameHeader::task_metrics(7, 1, 0))
            .with_cooperative_drain(drain as Arc<dyn CooperativeDrainSet>);

        let mut stats = SendBatchStats::default();
        assert!(
            sender
                .spin_send_frame(&[0xCA, 0xFE], &mut stats)
                .await
                .expect("control frame must not observe data cancellation")
        );
    }

    /// The producer-side half of the early-termination fix: a producer blocked on a full outbound
    /// ends its send cleanly once a `Cancel` for its `(stage, task, partition)` stream lands on its inbox.
    /// The test hangs if the spin doesn't observe the cancel.
    #[tokio::test(flavor = "current_thread")]
    async fn producer_send_ends_when_consumer_cancels_the_stream() {
        // Outbound to a consumer that never drains: capacity 1, so the second send finds it full.
        let (out_tx, _out_rx) = in_proc_channel(1);
        // The producer's inbox, where the consumer's `Cancel` frame lands.
        let (inbox_tx, inbox_rx) = in_proc_channel(4);
        let drain = Arc::new(DrainHandle::cooperative(vec![MppReceiver::new(Box::new(
            inbox_rx,
        ))]));
        // Producer of the `(stage 7, partition 0)` stream.
        let sender = MppSender::with_header(
            Arc::new(out_tx),
            MppFrameHeader::batch(MppDataStreamKey::new(7, 0, 0), 1),
        )
        .with_cooperative_drain(Arc::clone(&drain) as Arc<dyn CooperativeDrainSet>);

        // The consumer abandons that stream (it stopped reading before EOF): a `Cancel` reaches the
        // inbox.
        let mut buf = vec![0u8; MPP_FRAME_HEADER_SIZE];
        MppFrameHeader::cancel(MppDataStreamKey::new(7, 0, 0), 0).write_to(&mut buf);
        inbox_tx.send_bytes(&buf).unwrap();

        let mut stats = SendBatchStats::default();
        // First batch fills the one slot.
        sender
            .send_batch_traced(&sample_batch(1), &mut stats)
            .await
            .unwrap();
        // The second batch would block forever on the full slot. Instead the spin drains the inbox,
        // sees the cancel, and ends the send cleanly.
        sender
            .send_batch_traced(&sample_batch(1), &mut stats)
            .await
            .unwrap();
    }

    /// Tasks 0 and 3 can share one worker and output partition.
    #[tokio::test(flavor = "current_thread")]
    async fn cancelling_one_task_does_not_stop_sibling_sender() {
        let (out_tx, out_rx) = in_proc_channel(4);
        let out_tx: Arc<dyn BatchChannelSender> = Arc::new(out_tx);
        let (inbox_tx, inbox_rx) = in_proc_channel(4);
        let drain = Arc::new(DrainHandle::cooperative(vec![MppReceiver::new(Box::new(
            inbox_rx,
        ))]));
        let task_0_stream = MppDataStreamKey::new(7, 0, 0);
        let task_3_stream = MppDataStreamKey::new(7, 3, 0);
        let task_0 =
            MppSender::with_header(Arc::clone(&out_tx), MppFrameHeader::batch(task_0_stream, 1))
                .with_cooperative_drain(Arc::clone(&drain) as Arc<dyn CooperativeDrainSet>);
        let task_3 =
            MppSender::with_header(Arc::clone(&out_tx), MppFrameHeader::batch(task_3_stream, 1))
                .with_cooperative_drain(Arc::clone(&drain) as Arc<dyn CooperativeDrainSet>);

        let mut cancel = vec![0u8; MPP_FRAME_HEADER_SIZE];
        MppFrameHeader::cancel(task_0_stream, 0).write_to(&mut cancel);
        inbox_tx.send_bytes(&cancel).unwrap();
        drain.try_drain_pass().unwrap();
        assert!(task_0.stream_cancelled());
        assert!(!task_3.stream_cancelled());

        let mut stats = SendBatchStats::default();
        task_0
            .send_batch_traced(&sample_batch(2), &mut stats)
            .await
            .unwrap();
        task_3
            .send_batch_traced(&sample_batch(3), &mut stats)
            .await
            .unwrap();
        task_3.send_eof_traced(&mut stats).await.unwrap();

        let receiver = MppReceiver::new(Box::new(out_rx));
        let RecvBatchOutcome::Batch { header, batch } = receiver.try_recv_batch() else {
            panic!("task 3 batch must be delivered");
        };
        assert_eq!(header.data_stream(), task_3_stream);
        assert_eq!(batch.num_rows(), 3);
        let RecvBatchOutcome::Eof { header } = receiver.try_recv_batch() else {
            panic!("task 3 EOF must be delivered");
        };
        assert_eq!(header.data_stream(), task_3_stream);
        assert!(matches!(receiver.try_recv_batch(), RecvBatchOutcome::Empty));
    }

    #[test]
    fn frame_round_trips_a_work_unit_and_feed_eof() {
        let unit = pb::WorkUnit {
            id: vec![7; 16],
            partition: 4,
            body: vec![1, 2, 3],
            created_timestamp_unix_nanos: 11,
            sent_timestamp_unix_nanos: 22,
            received_timestamp_unix_nanos: 0,
            processed_timestamp_unix_nanos: 0,
        };
        let header = MppFrameHeader::work_unit(3, 1, 0);
        let mut buf = Vec::new();
        encode_prost_frame_into(header, &unit, &mut buf).expect("encode work unit");
        let (parsed, body) = decode_frame(&buf).expect("decode work unit");
        assert_eq!(parsed, header);
        let FrameBody::WorkUnit(decoded) = body else {
            panic!("WorkUnit frame must carry a unit");
        };
        assert_eq!(decoded, unit);

        let mut buf = vec![0u8; MPP_FRAME_HEADER_SIZE];
        MppFrameHeader::feed_eof(3, 1, 0).write_to(&mut buf);
        let (parsed, body) = decode_frame(&buf).expect("decode feed eof");
        assert_eq!(parsed.kind().unwrap(), MppFrameKind::FeedEof);
        assert!(matches!(body, FrameBody::FeedEof));
    }

    #[test]
    fn frame_round_trips_task_metrics() {
        let metrics = pb::TaskMetrics {
            pre_order_plan_metrics: vec![],
            task_metrics: None,
        };
        let header = MppFrameHeader::task_metrics(2, 0, 1);
        let mut buf = Vec::new();
        encode_prost_frame_into(header, &metrics, &mut buf).expect("encode metrics");
        let (parsed, body) = decode_frame(&buf).expect("decode metrics");
        assert_eq!(parsed, header);
        assert!(matches!(body, FrameBody::TaskMetrics(m) if m == metrics));
    }

    #[test]
    fn work_units_buffer_until_registration_and_feed_eof_closes() {
        fn unit(partition: u64) -> pb::WorkUnit {
            pb::WorkUnit {
                id: vec![9; 16],
                partition,
                body: vec![],
                created_timestamp_unix_nanos: 0,
                sent_timestamp_unix_nanos: 0,
                received_timestamp_unix_nanos: 0,
                processed_timestamp_unix_nanos: 0,
            }
        }
        let drain = DrainHandle::cooperative(vec![]);

        // Units arriving before registration must buffer, not drop.
        drain.route_work_unit(5, 0, unit(0));
        drain.route_work_unit(5, 0, unit(0));

        let id = crate::common::deserialize_uuid(&[9; 16]).unwrap();
        let mut channels = crate::work_unit_feed::RemoteWorkUnitFeedRegistry::default();
        channels.add(id, 1);
        let mut rx = channels
            .receivers
            .get(&(id, 0))
            .unwrap()
            .lock()
            .unwrap()
            .take()
            .unwrap();
        drain.register_work_unit_senders(5, 0, channels.senders);

        assert!(rx.try_recv().unwrap().is_ok());
        assert!(rx.try_recv().unwrap().is_ok());
        // Still open: the producer may send more units.
        assert!(rx.try_recv().is_err());

        // FeedEof drops the senders, which ends the stream.
        drain.route_work_unit(5, 0, unit(0));
        drain.close_feeds(5, 0);
        assert!(rx.try_recv().unwrap().is_ok());
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn frame_round_trips_a_set_plan_with_headers() {
        let set_plan = pb::SetPlanRequest {
            plan_proto: vec![1, 2, 3, 4],
            task_count: 2,
            task_key: Some(pb::TaskKey {
                query_id: vec![5; 16],
                stage_id: 3,
                task_number: 1,
            }),
            work_unit_feed_declarations: vec![],
            target_worker_url: "inprocess://worker/1".to_string(),
            query_start_time_ns: 42,
        };
        let mut headers = http::HeaderMap::new();
        headers.insert("x-datafusion-distributed-config", "abc".parse().unwrap());
        headers.append("x-repeated", "one".parse().unwrap());
        headers.append("x-repeated", "two".parse().unwrap());

        let frame = SetPlanFrame::from_parts(set_plan.clone(), &headers).expect("from_parts");
        let header = MppFrameHeader::set_plan(3, 1, 0);
        let mut buf = Vec::new();
        encode_prost_frame_into(header, &frame, &mut buf).expect("encode set plan");
        let (parsed, body) = decode_frame(&buf).expect("decode set plan");
        assert_eq!(parsed, header);
        assert_eq!(parsed.kind().unwrap(), MppFrameKind::SetPlan);
        let FrameBody::SetPlan(decoded) = body else {
            panic!("SetPlan frame must carry a SetPlanFrame");
        };
        let (decoded_plan, decoded_headers) = decoded.into_parts().expect("into_parts");
        assert_eq!(decoded_plan, set_plan);
        assert_eq!(decoded_headers, headers);
    }

    fn sample_set_plan_frame(plan_proto: Vec<u8>) -> SetPlanFrame {
        SetPlanFrame {
            set_plan: Some(pb::SetPlanRequest {
                plan_proto,
                task_count: 1,
                task_key: None,
                work_unit_feed_declarations: vec![],
                target_worker_url: String::new(),
                query_start_time_ns: 0,
            }),
            header_keys: vec![],
            header_values: vec![],
        }
    }

    #[tokio::test]
    async fn set_plan_serves_taker_in_either_arrival_order() {
        let drain = DrainHandle::cooperative(vec![]);

        // Frame first: the take resolves from the pending slot.
        drain.route_set_plan(7, 0, sample_set_plan_frame(vec![1]));
        let frame = drain.take_set_plan(7, 0).await.expect("pending take");
        assert_eq!(frame.set_plan.unwrap().plan_proto, vec![1]);

        // Taker first: the frame fulfills the parked oneshot.
        let take = drain.take_set_plan(7, 1);
        futures::pin_mut!(take);
        assert!(futures::poll!(take.as_mut()).is_pending());
        drain.route_set_plan(7, 1, sample_set_plan_frame(vec![2]));
        let frame = take.await.expect("waiting take");
        assert_eq!(frame.set_plan.unwrap().plan_proto, vec![2]);
    }

    #[tokio::test]
    async fn set_plan_take_fails_when_the_inbox_dies() {
        let drain = DrainHandle::cooperative(vec![]);
        let take = drain.take_set_plan(9, 0);
        futures::pin_mut!(take);
        assert!(futures::poll!(take.as_mut()).is_pending());
        drain.fail_scope("producer went away");
        let err = take.await.expect_err("dead inbox must fail the take");
        assert!(format!("{err}").contains("producer went away"));
        // Later takers fail immediately.
        let err = drain.take_set_plan(9, 1).await.expect_err("dead registry");
        assert!(format!("{err}").contains("producer went away"));
    }

    /// Ring-header-aligned heap region standing in for the DSM segment. Returned first from
    /// [`detached_test_inbox`] so it outlives every ring handle minted over it.
    struct AlignedRingRegion {
        ptr: *mut u8,
        layout: std::alloc::Layout,
    }

    impl AlignedRingRegion {
        fn new(bytes: usize) -> Self {
            let align = std::mem::align_of::<super::super::mpsc_ring::DsmMpscRingHeader>();
            let layout = std::alloc::Layout::from_size_align(bytes, align).expect("layout");
            let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
            assert!(!ptr.is_null());
            Self { ptr, layout }
        }
    }

    impl Drop for AlignedRingRegion {
        fn drop(&mut self) {
            unsafe { std::alloc::dealloc(self.ptr, self.layout) };
        }
    }

    use crate::shm::Wakeup;

    struct NoWake;
    impl Wakeup for NoWake {
        fn wake(&self, _token: u64) {}
    }

    /// One DSM test ring playing a worker's inbox: `peer` holds the ring's only data sender,
    /// `leader` a control sender that stays out of `sender_count`. Dropping `peer` latches the
    /// ring detached while `leader` can still publish, the exact state a peer's normal
    /// completion leaves behind in a leader + two-worker mesh.
    fn detached_test_inbox() -> (
        AlignedRingRegion,
        super::super::mpsc_ring::DsmMpscSender,
        super::super::mpsc_ring::DsmMpscSender,
        DrainHandle,
    ) {
        use super::super::mesh::DsmInboxReceiver;
        use super::super::mpsc_ring::{self, DsmMpscReceiver, DsmMpscRingHeader, DsmMpscSender};

        let (ring_slots, slot_capacity) = (64u32, 1024u32);
        let region =
            AlignedRingRegion::new(DsmMpscRingHeader::region_bytes(ring_slots, slot_capacity));
        let header = unsafe { mpsc_ring::create_at(region.ptr, ring_slots, slot_capacity) };
        let nn = std::ptr::NonNull::new(header).expect("create_at returned null");
        let alive: super::super::AliveFlag = StdArc::new(std::sync::atomic::AtomicBool::new(true));
        let wakeup: StdArc<dyn Wakeup> = StdArc::new(NoWake);
        let peer = unsafe { DsmMpscSender::new(nn, StdArc::clone(&wakeup), StdArc::clone(&alive)) };
        let leader = unsafe {
            DsmMpscSender::new_control(nn, StdArc::clone(&wakeup), StdArc::clone(&alive))
        };
        let receiver = unsafe { DsmMpscReceiver::new(nn, alive) };
        let drain = DrainHandle::cooperative(vec![MppReceiver::new(Box::new(
            DsmInboxReceiver::new(receiver),
        ))]);
        (region, peer, leader, drain)
    }

    /// A peer's normal completion drops the ring's last data sender. Channels the peer EOF'd
    /// stay clean, parked request takers keep waiting, and a control frame published after
    /// the detach still reaches its registry: this proc may still owe the leader work.
    #[test]
    fn peer_completion_detach_keeps_leader_control_plane_alive() {
        let (_region, peer, leader, drain) = detached_test_inbox();

        // A fragment mid-query holds a parked request taker.
        let mut requests = drain.take_execute_task_rx(3, 1).expect("take rx");

        // The peer serves its one stream to completion and departs.
        let mut eof = Vec::new();
        encode_eof_frame_into(MppDataStreamKey::new(2, 1, 0), 1, &mut eof).expect("eof frame");
        peer.try_send(&eof).expect("eof send");
        drop(peer);

        drain.try_drain_pass().expect("drain over the detach");

        // The served stream completed; the departure did not fail it.
        let served = drain.register_data_channel(1, MppDataStreamKey::new(2, 1, 0));
        assert!(matches!(served.try_pop(), Some(DrainItem::Eof)));

        // The request taker is still parked, not failed.
        assert!(matches!(
            requests.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));

        // A request the leader publishes after the detach still lands.
        let request = ExecuteTaskFrame::from_parts(
            pb::ExecuteTaskRequest {
                task_key: Some(pb::TaskKey {
                    query_id: vec![7; 16],
                    stage_id: 3,
                    task_number: 1,
                }),
                target_partition_start: 0,
                target_partition_end: 1,
                producer_head: Some(pb::execute_task_request::ProducerHead::None(
                    pb::NoneHead {},
                )),
            },
            &http::HeaderMap::new(),
        )
        .expect("request frame");
        let mut frame = Vec::new();
        encode_prost_frame_into(MppFrameHeader::execute_task(3, 1, 0), &request, &mut frame)
            .expect("encode request");
        leader.try_send(&frame).expect("post-detach control send");
        drain.try_drain_pass().expect("drain the request");

        let got = requests
            .try_recv()
            .expect("request frame must be delivered")
            .expect("delivery must not be a failure");
        let (decoded, _headers) = got.frame.into_parts().expect("into_parts");
        assert_eq!(decoded.task_key.unwrap().task_number, 1);
    }

    /// A producer that departs without a channel's `Eof` still fails that channel: the
    /// consumer depends on data that will never arrive. Registrations after the detach fail
    /// the same way.
    #[test]
    fn producer_loss_fails_channels_still_awaiting_eof() {
        let (_region, peer, _leader, drain) = detached_test_inbox();

        let dependent = drain.register_data_channel(1, MppDataStreamKey::new(2, 1, 0));
        drop(peer);
        drain.try_drain_pass().expect("drain over the detach");

        assert!(matches!(dependent.try_pop(), Some(DrainItem::Failed(_))));
        let late = drain.register_data_channel(1, MppDataStreamKey::new(2, 1, 1));
        assert!(matches!(late.try_pop(), Some(DrainItem::Failed(_))));
    }

    #[test]
    fn frame_rejects_short_message() {
        let too_short = vec![0u8; MPP_FRAME_HEADER_SIZE - 1];
        let err = decode_frame(&too_short).expect_err("short frame must fail");
        assert!(format!("{err}").contains("too short"));
    }

    #[test]
    fn frame_rejects_bad_magic() {
        // Explicit non-zero, non-magic prefix. Don't rely on the
        // happenstance that 0u32 != MPP_FRAME_MAGIC.
        let mut bad = vec![0u8; MPP_FRAME_HEADER_SIZE];
        bad[0..4].copy_from_slice(&0xCAFEBABE_u32.to_le_bytes());
        let err = decode_frame(&bad).expect_err("bad magic must fail");
        assert!(format!("{err}").contains("bad frame magic"));
        bad[0..4].copy_from_slice(&0xDEADBEEF_u32.to_le_bytes());
        let err = decode_frame(&bad).expect_err("bad magic must fail");
        assert!(format!("{err}").contains("bad frame magic"));
    }

    #[test]
    fn frame_rejects_unknown_kind() {
        let header = MppFrameHeader {
            magic: MPP_FRAME_MAGIC,
            flags: 0x42, // unknown kind byte, no reserved bits set
            stage_id: 0,
            task_id: 0,
            partition: 0,
        };
        let mut buf = vec![0u8; MPP_FRAME_HEADER_SIZE];
        header.write_to(&mut buf);
        let err = decode_frame(&buf).expect_err("unknown kind must fail");
        assert!(format!("{err}").contains("unknown frame kind"));
    }

    #[test]
    fn frame_rejects_reserved_flag_bits() {
        // Reserved range is bits 8..16. Bits 16..32 are sender_proc and must NOT trip the
        // reserved check. Cover both boundaries of the reserved range.
        for bit in [0x0000_0100u32, 0x0000_8000u32] {
            let header = MppFrameHeader {
                magic: MPP_FRAME_MAGIC,
                flags: bit, // kind byte 0 (Batch), reserved bit set, no sender_proc
                stage_id: 0,
                task_id: 0,
                partition: 0,
            };
            let mut buf = vec![0u8; MPP_FRAME_HEADER_SIZE];
            header.write_to(&mut buf);
            let err = decode_frame(&buf).expect_err(&format!("reserved bit {bit:#x} must fail"));
            assert!(
                format!("{err}").contains("reserved frame flag bits"),
                "bit {bit:#x}: {err}"
            );
        }
    }

    #[test]
    fn frame_kind_coexists_with_max_sender_proc() {
        // Negative-space companion to frame_rejects_reserved_flag_bits: setting every bit in
        // 16..32 (= max sender_proc) along with kind=Eof in bit 0 must parse cleanly without
        // tripping the reserved-bits check, and sender_proc()/kind() must read both back.
        let header = MppFrameHeader {
            magic: MPP_FRAME_MAGIC,
            flags: 0xFFFF_0001, // Eof in low byte, max sender_proc in high half, reserved=0
            stage_id: 0,
            task_id: 0,
            partition: 0,
        };
        assert_eq!(header.kind().unwrap(), MppFrameKind::Eof);
        assert_eq!(header.sender_proc(), MPP_MAX_SENDER_PROC);
    }

    #[test]
    fn frame_sender_proc_round_trip() {
        // sender_proc lives in flags bits 16..32 and shouldn't collide with kind or reserved.
        for &sp in &[0u32, 1, 7, 255, 256, 1023, 65534, MPP_MAX_SENDER_PROC] {
            let stream = MppDataStreamKey::new(11, 0, 5);
            let header = MppFrameHeader::batch(stream, sp);
            assert_eq!(header.sender_proc(), sp, "batch round-trip sp={sp}");
            assert_eq!(header.kind().unwrap(), MppFrameKind::Batch);

            let mut buf = Vec::with_capacity(MPP_FRAME_HEADER_SIZE);
            let payload = sample_batch(8);
            encode_frame_into(header, &payload, &mut buf).expect("encode batch");
            let (parsed, _) = decode_frame(&buf).expect("decode batch");
            assert_eq!(parsed.sender_proc(), sp, "decoded batch sender_proc");

            let mut eof_buf = Vec::new();
            encode_eof_frame_into(stream, sp, &mut eof_buf).expect("encode eof");
            let (parsed_eof, _) = decode_frame(&eof_buf).expect("decode eof");
            assert_eq!(parsed_eof.sender_proc(), sp, "decoded eof sender_proc");
            assert_eq!(parsed_eof.kind().unwrap(), MppFrameKind::Eof);
        }
    }

    #[test]
    fn frame_eof_with_payload_is_rejected() {
        let mut buf = Vec::with_capacity(32);
        encode_eof_frame_into(MppDataStreamKey::new(0, 0, 0), 0, &mut buf).expect("encode_eof");
        buf.push(0xAB); // smuggle a payload byte after the Eof header
        let err = decode_frame(&buf).expect_err("Eof+payload must fail");
        assert!(format!("{err}").contains("payload-less frame carries payload"));
    }

    #[test]
    fn codec_round_trips_many_batch_sizes() {
        let mut buf = Vec::with_capacity(1024);
        for rows in [0, 1, 7, 64, 1024] {
            let orig = sample_batch(rows);
            encode_frame_into(
                MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 0), 0),
                &orig,
                &mut buf,
            )
            .expect("encode");
            let (_header, body) = decode_frame(&buf).expect("decode");
            let FrameBody::Batch(decoded) = body else {
                panic!("Batch frame must carry a batch payload");
            };
            assert_eq!(orig.num_rows(), decoded.num_rows());
        }
    }

    /// Bounded in-proc sink standing in for a DSM ring: rejects frames over `cap` the way
    /// `try_send` does, records every accepted frame, and advertises the bound so the send
    /// path's chunker engages.
    struct BoundedSink {
        cap: usize,
        frames: Mutex<Vec<Vec<u8>>>,
        send_lock: tokio::sync::Mutex<()>,
    }

    impl BoundedSink {
        fn new(cap: usize) -> Arc<Self> {
            Arc::new(Self {
                cap,
                frames: Mutex::new(Vec::new()),
                send_lock: tokio::sync::Mutex::new(()),
            })
        }
    }

    impl BatchChannelSender for BoundedSink {
        fn send_bytes(&self, bytes: &[u8]) -> Result<(), DataFusionError> {
            if bytes.len() > self.cap {
                return Err(DataFusionError::Execution(
                    "mpp: DSM MPSC frame exceeds the entire ring capacity \
                     (raise the embedder's queue-size knob)"
                        .into(),
                ));
            }
            self.frames.lock().unwrap().push(bytes.to_vec());
            Ok(())
        }

        fn send_lock(&self) -> &tokio::sync::Mutex<()> {
            &self.send_lock
        }

        fn max_frame_bytes(&self) -> Option<usize> {
            Some(self.cap)
        }
    }

    /// `rows` of ~1 KiB unique strings, so slices drag a large values buffer through
    /// arrow-ipc.
    fn wide_batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("s", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let strings = StringArray::from_iter_values(
            (0..rows).map(|i| format!("{i:06}-{}", "x".repeat(1024))),
        );
        let vals = Int64Array::from_iter_values(0..rows as i64);
        RecordBatch::try_new(schema, vec![Arc::new(strings), Arc::new(vals)]).unwrap()
    }

    /// Replays recorded frames as a `BatchChannelReceiver`, standing in for the ring's recv
    /// side so reassembly can be exercised without a DSM region.
    struct ReplayChannel {
        frames: Mutex<VecDeque<Vec<u8>>>,
    }

    impl ReplayChannel {
        fn over(frames: Vec<Vec<u8>>) -> Box<Self> {
            Box::new(Self {
                frames: Mutex::new(frames.into_iter().collect()),
            })
        }
    }

    impl BatchChannelReceiver for ReplayChannel {
        fn try_recv(&self) -> RecvOutcome {
            match self.frames.lock().unwrap().pop_front() {
                Some(f) => RecvOutcome::Bytes(f),
                None => RecvOutcome::Empty,
            }
        }
    }

    /// Pull one decoded batch out of a receiver fed by `frames`, asserting nothing else is
    /// queued behind it.
    fn reassemble_single_batch(frames: Vec<Vec<u8>>) -> (MppFrameHeader, RecordBatch) {
        let receiver = MppReceiver::new(ReplayChannel::over(frames));
        let RecvBatchOutcome::Batch { header, batch } = receiver.try_recv_batch() else {
            panic!("expected a reassembled batch");
        };
        assert!(matches!(receiver.try_recv_batch(), RecvBatchOutcome::Empty));
        (header, batch)
    }

    #[test]
    fn oversized_sliced_batch_compacts_then_chunks() {
        let cap = 64 * 1024;
        let base = wide_batch(512);
        // 128 rows, but the slice keeps the whole ~512 KiB values buffer alive.
        let sliced = base.slice(64, 128);

        // Premise: the sliced encode really does balloon past the cap. If arrow-ipc ever
        // learns to truncate sliced buffers, this assert flags the test (and the compact
        // step's main motivation) for review.
        let mut probe = Vec::new();
        encode_frame_into(
            MppFrameHeader::batch(MppDataStreamKey::new(3, 0, 7), 0),
            &sliced,
            &mut probe,
        )
        .expect("probe");
        assert!(
            probe.len() > cap,
            "premise: sliced encode ({} bytes) must exceed cap ({cap})",
            probe.len()
        );

        let sink = BoundedSink::new(cap);
        let sender = MppSender::with_header(
            Arc::clone(&sink) as Arc<dyn BatchChannelSender>,
            MppFrameHeader::batch(MppDataStreamKey::new(3, 0, 7), 0),
        );
        sender.send_batch(&sliced).expect("chunked send");

        // The compacted 128 rows (~135 KiB of strings) still exceed the cap, so the frame
        // streams as chunks, every ring message within the cap.
        let frames = sink.frames.lock().unwrap().clone();
        assert!(
            frames.len() > 1,
            "expected the oversized frame to chunk, got {} frame(s)",
            frames.len()
        );
        for frame in &frames {
            assert!(
                frame.len() <= cap,
                "frame of {} bytes over cap",
                frame.len()
            );
            let header = MppFrameHeader::parse(frame).expect("chunk header");
            assert_eq!(header.kind().expect("kind"), MppFrameKind::Chunk);
            assert_eq!((header.stage_id, header.partition), (3, 7));
        }
        let (header, got) = reassemble_single_batch(frames);
        assert_eq!((header.stage_id, header.partition), (3, 7));
        // Row order and content must survive; compare against a compacted copy (`take` of
        // every row) since the decoded side never sees the original buffers.
        let want = compact_rows(&sliced, 0, sliced.num_rows()).expect("compact reference");
        assert_eq!(got, want);
    }

    /// Same as above but through `Utf8View`: `take` copies the views yet keeps the shared
    /// data blocks, so without the compact-time `gc` every frame would drag the full data
    /// blocks through the ring no matter how it is chunked.
    #[test]
    fn oversized_view_array_batch_compacts_then_chunks() {
        let cap = 64 * 1024;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let strings = StringViewArray::from_iter_values(
            (0..512).map(|i| format!("{i:06}-{}", "x".repeat(1024))),
        );
        let base = RecordBatch::try_new(schema, vec![Arc::new(strings)]).unwrap();
        let sliced = base.slice(64, 128);

        let sink = BoundedSink::new(cap);
        let sender = MppSender::with_header(
            Arc::clone(&sink) as Arc<dyn BatchChannelSender>,
            MppFrameHeader::batch(MppDataStreamKey::new(1, 0, 1), 0),
        );
        sender.send_batch(&sliced).expect("view-array chunked send");

        let frames = sink.frames.lock().unwrap().clone();
        for frame in &frames {
            assert!(
                frame.len() <= cap,
                "frame of {} bytes over cap",
                frame.len()
            );
        }
        let (_, got) = reassemble_single_batch(frames);
        assert_eq!(got.num_rows(), 128);
    }

    /// A single row bigger than the whole ring streams through in chunks; frame size is
    /// never an error.
    #[test]
    fn single_oversized_row_streams_through() {
        let cap = 64 * 1024;
        let value = "y".repeat(128 * 1024);
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from_iter_values([value.as_str()]))],
        )
        .unwrap();

        let sink = BoundedSink::new(cap);
        let sender = MppSender::with_header(
            Arc::clone(&sink) as Arc<dyn BatchChannelSender>,
            MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 0), 0),
        );
        sender.send_batch(&batch).expect("giant row streams");

        let frames = sink.frames.lock().unwrap().clone();
        assert!(
            frames.len() > 2,
            "a 128 KiB row needs several 64 KiB chunks"
        );
        let (_, got) = reassemble_single_batch(frames);
        assert_eq!(got.num_rows(), 1);
        let strings = got
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string column");
        assert_eq!(strings.value(0), value);
    }

    /// Chunked frames from two tasks on one proc may interleave on the shared inbox. Their stage
    /// and output partition deliberately match, so only task identity keeps their reassembly
    /// buffers apart.
    #[test]
    fn chunk_reassembly_keys_by_stream() {
        let sender_proc = 2;
        let stream_a = MppDataStreamKey::new(1, 0, 1);
        let stream_b = MppDataStreamKey::new(1, 3, 1);
        let inner_a = {
            let mut buf = Vec::new();
            encode_frame_into(
                MppFrameHeader::batch(stream_a, sender_proc),
                &sample_batch(4),
                &mut buf,
            )
            .expect("encode inner A");
            buf
        };
        let inner_b = {
            let mut buf = Vec::new();
            encode_frame_into(
                MppFrameHeader::batch(stream_b, sender_proc),
                &sample_batch(7),
                &mut buf,
            )
            .expect("encode inner B");
            buf
        };
        let a_split = inner_a.len() / 2;
        let b_split = inner_b.len() / 2;
        let mut a0 = Vec::new();
        encode_chunk_frame_into(
            MppFrameHeader::chunk(stream_a, sender_proc),
            inner_a.len() as u64,
            0,
            &inner_a[..a_split],
            &mut a0,
        );
        let mut a1 = Vec::new();
        encode_chunk_frame_into(
            MppFrameHeader::chunk(stream_a, sender_proc),
            inner_a.len() as u64,
            a_split as u64,
            &inner_a[a_split..],
            &mut a1,
        );
        let mut b0 = Vec::new();
        encode_chunk_frame_into(
            MppFrameHeader::chunk(stream_b, sender_proc),
            inner_b.len() as u64,
            0,
            &inner_b[..b_split],
            &mut b0,
        );
        let mut b1 = Vec::new();
        encode_chunk_frame_into(
            MppFrameHeader::chunk(stream_b, sender_proc),
            inner_b.len() as u64,
            b_split as u64,
            &inner_b[b_split..],
            &mut b1,
        );

        let receiver = MppReceiver::new(ReplayChannel::over(vec![a0, b0, a1, b1]));
        let RecvBatchOutcome::Batch { header, batch } = receiver.try_recv_batch() else {
            panic!("expected reassembled task 0 batch");
        };
        assert_eq!(header.data_stream(), stream_a);
        assert_eq!(batch.num_rows(), 4);
        let RecvBatchOutcome::Batch { header, batch } = receiver.try_recv_batch() else {
            panic!("expected reassembled task 3 batch");
        };
        assert_eq!(header.data_stream(), stream_b);
        assert_eq!(batch.num_rows(), 7);
        assert!(matches!(receiver.try_recv_batch(), RecvBatchOutcome::Empty));
    }

    /// A producer that observes its stream's cancel stops mid-frame; the stream's `Eof`
    /// clears the orphaned prefix, and a later frame for the stream restarts at offset 0.
    #[test]
    fn eof_clears_abandoned_chunk_prefix() {
        let sender_proc = 3;
        let mut inner = Vec::new();
        encode_frame_into(
            MppFrameHeader::batch(MppDataStreamKey::new(5, 0, 0), sender_proc),
            &sample_batch(4),
            &mut inner,
        )
        .expect("encode inner");
        let chunk_header = MppFrameHeader::chunk(MppDataStreamKey::new(5, 0, 0), sender_proc);
        let mut partial = Vec::new();
        encode_chunk_frame_into(
            chunk_header,
            inner.len() as u64,
            0,
            &inner[..inner.len() / 2],
            &mut partial,
        );
        let mut eof = Vec::new();
        encode_eof_frame_into(MppDataStreamKey::new(5, 0, 0), sender_proc, &mut eof)
            .expect("encode eof");
        // A fresh frame for the stream after the abandonment restarts cleanly at offset 0.
        let mut whole = Vec::new();
        encode_chunk_frame_into(chunk_header, inner.len() as u64, 0, &inner, &mut whole);

        let receiver = MppReceiver::new(ReplayChannel::over(vec![partial, eof, whole]));
        let RecvBatchOutcome::Eof { header } = receiver.try_recv_batch() else {
            panic!("expected the Eof to pass through");
        };
        assert_eq!((header.stage_id, header.partition), (5, 0));
        assert!(
            receiver.assemblies.lock().unwrap().is_empty(),
            "the Eof must clear the abandoned prefix"
        );
        assert!(matches!(
            receiver.try_recv_batch(),
            RecvBatchOutcome::Batch { .. }
        ));
    }

    /// Reassembly rejects a continuation with no first chunk and an offset that doesn't
    /// line up: both mean the per-sender FIFO the protocol rides on was violated.
    #[test]
    fn chunk_out_of_sequence_errors() {
        let sender_proc = 4;
        let chunk_header = MppFrameHeader::chunk(MppDataStreamKey::new(1, 0, 0), sender_proc);
        let mut orphan = Vec::new();
        encode_chunk_frame_into(chunk_header, 1000, 500, &[0u8; 16], &mut orphan);
        let receiver = MppReceiver::new(ReplayChannel::over(vec![orphan]));
        assert!(matches!(
            receiver.try_recv_batch(),
            RecvBatchOutcome::TransportError(_)
        ));

        let mut first = Vec::new();
        encode_chunk_frame_into(chunk_header, 1000, 0, &[0u8; 100], &mut first);
        let mut skipped = Vec::new();
        encode_chunk_frame_into(chunk_header, 1000, 200, &[0u8; 100], &mut skipped);
        let receiver = MppReceiver::new(ReplayChannel::over(vec![first, skipped]));
        assert!(matches!(
            receiver.try_recv_batch(),
            RecvBatchOutcome::TransportError(_)
        ));
    }

    #[test]
    fn drain_buffer_pop_returns_pushed_batches_in_order() {
        let buf = DrainBuffer::new(1);
        buf.push_batch(sample_batch(3));
        buf.push_batch(sample_batch(5));
        buf.notify_source_done();

        match buf.pop_front() {
            DrainItem::Batch(b) => assert_eq!(b.num_rows(), 3),
            DrainItem::Eof => panic!("expected batch"),
            DrainItem::Failed(msg) => panic!("unexpected failure: {msg}"),
        }
        match buf.pop_front() {
            DrainItem::Batch(b) => assert_eq!(b.num_rows(), 5),
            DrainItem::Eof => panic!("expected batch"),
            DrainItem::Failed(msg) => panic!("unexpected failure: {msg}"),
        }
        matches!(buf.pop_front(), DrainItem::Eof);
    }

    #[test]
    fn drain_buffer_pop_blocks_until_push_then_eof() {
        let buf = DrainBuffer::new(2);
        let producer = StdArc::clone(&buf);
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            producer.push_batch(sample_batch(2));
            producer.notify_source_done();
            thread::sleep(Duration::from_millis(20));
            producer.notify_source_done();
        });

        match buf.pop_front() {
            DrainItem::Batch(b) => assert_eq!(b.num_rows(), 2),
            DrainItem::Eof => panic!("expected batch first"),
            DrainItem::Failed(msg) => panic!("unexpected failure: {msg}"),
        }
        assert!(matches!(buf.pop_front(), DrainItem::Eof));
        handle.join().unwrap();
    }

    #[test]
    fn drain_buffer_cancel_unblocks_waiter() {
        let buf = DrainBuffer::new(1);
        let canceller = StdArc::clone(&buf);
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(20));
            canceller.cancel();
        });
        assert!(matches!(buf.pop_front(), DrainItem::Eof));
        handle.join().unwrap();
    }

    #[test]
    fn in_proc_channel_round_trips_through_mpp_sender_receiver() {
        let (tx, rx) = in_proc_channel(8);
        let sender = MppSender::new(Arc::new(tx));
        let receiver = MppReceiver::new(Box::new(rx));

        sender.send_batch(&sample_batch(4)).unwrap();
        std::mem::drop(sender);

        match receiver.try_recv_batch() {
            RecvBatchOutcome::Batch { header: _, batch } => assert_eq!(batch.num_rows(), 4),
            other => panic!("expected batch, got {other:?}"),
        }
        assert!(matches!(
            receiver.try_recv_batch(),
            RecvBatchOutcome::Detached
        ));
    }

    #[test]
    fn drain_thread_drains_single_source() {
        let (tx, rx) = in_proc_channel(4);
        let sender = MppSender::new(Arc::new(tx));
        let receiver = MppReceiver::new(Box::new(rx));
        let buffer = DrainBuffer::new(1);

        let join = spawn_drain_thread(DrainConfig::new(vec![receiver], StdArc::clone(&buffer)));

        thread::spawn(move || {
            for rows in [1, 2, 3, 4, 5] {
                sender.send_batch(&sample_batch(rows)).unwrap();
            }
            // Drop sender to signal EOF
        })
        .join()
        .unwrap();

        let mut received = Vec::new();
        while let DrainItem::Batch(b) = buffer.pop_front() {
            received.push(b.num_rows());
        }
        assert_eq!(received, vec![1, 2, 3, 4, 5]);
        join.join().unwrap().unwrap();
    }

    #[test]
    fn drain_handle_shutdown_joins_cleanly() {
        let (tx, rx) = in_proc_channel(4);
        let sender = MppSender::new(Arc::new(tx));
        let receiver = MppReceiver::new(Box::new(rx));
        let buffer = DrainBuffer::new(1);
        let handle =
            ThreadedDrainHandle::spawn(DrainConfig::new(vec![receiver], StdArc::clone(&buffer)));

        sender.send_batch(&sample_batch(2)).unwrap();
        std::mem::drop(sender); // detach
        // Pop the one batch
        assert!(matches!(buffer.pop_front(), DrainItem::Batch(_)));
        assert!(matches!(buffer.pop_front(), DrainItem::Eof));
        // Drop drives production teardown (cancel + join). Test passes if
        // this returns without hanging.
        std::mem::drop(handle);
    }

    #[test]
    fn drain_handle_drop_cancels_and_joins() {
        // Build a drain that never detaches (we keep the sender alive), then
        // drop the handle. The Drop impl must cancel the buffer and join the
        // thread without hanging.
        let (tx, rx) = in_proc_channel(4);
        let _sender_kept_alive = MppSender::new(Arc::new(tx));
        let receiver = MppReceiver::new(Box::new(rx));
        let buffer = DrainBuffer::new(1);
        let handle =
            ThreadedDrainHandle::spawn(DrainConfig::new(vec![receiver], StdArc::clone(&buffer)));

        // Simulate consumer path error: drop the handle without calling
        // shutdown(). The drain thread must exit before drop returns.
        let start = Instant::now();
        drop(handle);
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_secs(2),
            "ThreadedDrainHandle::drop took too long: {elapsed:?}"
        );
        // Consumer observes EOF because cancel was called.
        assert!(matches!(buffer.pop_front(), DrainItem::Eof));
    }

    #[test]
    fn drain_thread_drains_n2_mesh_100k_batches() {
        // Simulates a 2-proc mesh under load. Each of two producers
        // pushes 50_000 small batches through a bounded channel; the drain
        // thread interleaves and the consumer reads EOF exactly after
        // receiving all 100_000 batches. Exercises backpressure (bounded
        // capacity = 16) without deadlock.
        const PER_SOURCE: usize = 50_000;
        let (tx0, rx0) = in_proc_channel(16);
        let (tx1, rx1) = in_proc_channel(16);
        let receivers = vec![
            MppReceiver::new(Box::new(rx0)),
            MppReceiver::new(Box::new(rx1)),
        ];
        let buffer = DrainBuffer::new(2);
        let drain_join = spawn_drain_thread(DrainConfig::new(receivers, StdArc::clone(&buffer)));

        let tx0_send = MppSender::new(Arc::new(tx0));
        let tx1_send = MppSender::new(Arc::new(tx1));
        let batch_template = sample_batch(1);

        let p0 = {
            let b = batch_template.clone();
            thread::spawn(move || {
                for _ in 0..PER_SOURCE {
                    tx0_send.send_batch(&b).unwrap();
                }
            })
        };
        let p1 = {
            let b = batch_template.clone();
            thread::spawn(move || {
                for _ in 0..PER_SOURCE {
                    tx1_send.send_batch(&b).unwrap();
                }
            })
        };

        let mut total = 0usize;
        while let DrainItem::Batch(_) = buffer.pop_front() {
            total += 1;
        }
        assert_eq!(total, 2 * PER_SOURCE);
        p0.join().unwrap();
        p1.join().unwrap();
        drain_join.join().unwrap().unwrap();
    }

    #[test]
    fn drain_buffer_drains_buffered_before_eof() {
        // Even if all sources have finished and cancel fires, any already-
        // buffered batches must be observed before Eof.
        let buf = DrainBuffer::new(1);
        buf.push_batch(sample_batch(1));
        buf.push_batch(sample_batch(1));
        buf.notify_source_done();
        buf.cancel();

        assert!(matches!(buf.pop_front(), DrainItem::Batch(_)));
        assert!(matches!(buf.pop_front(), DrainItem::Batch(_)));
        assert!(matches!(buf.pop_front(), DrainItem::Eof));
    }

    // ---------------------------------------------------------------------
    // Throughput microbenches.
    //
    // These are `#[ignore]` by default because they spin for seconds and spam stdout. Run with:
    //
    //   cargo test --package pg_search --release \
    //       postgres::customscan::mpp::transport::tests::throughput \
    //       -- --ignored --nocapture
    //
    // They help us bound the transport layer's cost independently of DataFusion/Tantivy. All use
    // the `in_proc_channel` backend (same `MppSender`/`MppReceiver` trait boundary as the shm_mq
    // one), so numbers here are an optimistic ceiling. shm_mq adds the ring-buffer copy +
    // cross-process notification cost on top. If these numbers are already below the row rate
    // the real query needs, we know IPC encode + channel handoff is the bottleneck without
    // needing CI data.
    // ---------------------------------------------------------------------

    /// Row shape matching the post-Partial shuffle in
    /// `aggregate_join_groupby`: a grouping key (title string) plus two
    /// partial-aggregate accumulators (COUNT u64, SUM i64).
    fn postagg_shape_batch(rows: usize) -> RecordBatch {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("title", DataType::Utf8, false),
            Field::new("count_partial", DataType::UInt64, false),
            Field::new("sum_partial", DataType::Int64, false),
        ]));
        // Titles averaging ~30 bytes, typical for the docs dataset.
        let titles = StringArray::from_iter_values(
            (0..rows).map(|i| format!("file_{i:012}_title_with_some_length")),
        );
        let counts = UInt64Array::from_iter_values((0..rows as u64).map(|i| i % 64 + 1));
        let sums = Int64Array::from_iter_values((0..rows as i64).map(|i| i * 1024));
        RecordBatch::try_new(
            schema,
            vec![StdArc::new(titles), StdArc::new(counts), StdArc::new(sums)],
        )
        .unwrap()
    }

    /// Row shape matching the probe-side shuffle in the same query:
    /// `pages.fileId` (u64) plus `pages.sizeInBytes` (i64).
    fn probe_shape_batch(rows: usize) -> RecordBatch {
        let schema = StdArc::new(Schema::new(vec![
            Field::new("fileId", DataType::UInt64, false),
            Field::new("sizeInBytes", DataType::Int64, false),
        ]));
        let ids =
            UInt64Array::from_iter_values((0..rows as u64).map(|i| i.wrapping_mul(2654435761)));
        let sizes = Int64Array::from_iter_values((0..rows as i64).map(|i| i * 37));
        RecordBatch::try_new(schema, vec![StdArc::new(ids), StdArc::new(sizes)]).unwrap()
    }

    fn bench_throughput(
        label: &str,
        make_batch: fn(usize) -> RecordBatch,
        batch_rows: usize,
        total_rows: usize,
    ) {
        let batches = total_rows.div_ceil(batch_rows);
        let template = make_batch(batch_rows);
        // Encode once up front so we also report pure-encode throughput
        // separately. Real queries encode inside the hot path per batch.
        let enc_start = Instant::now();
        let mut enc_bytes = 0usize;
        let mut enc_buf = Vec::with_capacity(1024);
        for _ in 0..batches {
            encode_frame_into(
                MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 0), 0),
                &template,
                &mut enc_buf,
            )
            .expect("encode");
            enc_bytes += enc_buf.len();
        }
        let enc_elapsed = enc_start.elapsed();

        // N=2 mesh: two senders, one drain thread, one consumer. Matches
        // the gb_postagg / gb_right topology in the real query.
        let (tx0, rx0) = in_proc_channel(16);
        let (tx1, rx1) = in_proc_channel(16);
        let receivers = vec![
            MppReceiver::new(Box::new(rx0)),
            MppReceiver::new(Box::new(rx1)),
        ];
        let buffer = DrainBuffer::new(2);
        let drain_join = spawn_drain_thread(DrainConfig::new(receivers, StdArc::clone(&buffer)));
        let tx0_send = MppSender::new(Arc::new(tx0));
        let tx1_send = MppSender::new(Arc::new(tx1));

        let per_source = batches / 2;
        let round_trip_start = Instant::now();
        let p0 = {
            let b = template.clone();
            thread::spawn(move || {
                for _ in 0..per_source {
                    tx0_send.send_batch(&b).unwrap();
                }
            })
        };
        let p1 = {
            let b = template.clone();
            thread::spawn(move || {
                for _ in 0..per_source {
                    tx1_send.send_batch(&b).unwrap();
                }
            })
        };

        let mut got_rows = 0usize;
        let mut got_batches = 0usize;
        while let DrainItem::Batch(b) = buffer.pop_front() {
            got_rows += b.num_rows();
            got_batches += 1;
        }
        p0.join().unwrap();
        p1.join().unwrap();
        drain_join.join().unwrap().unwrap();
        let rt_elapsed = round_trip_start.elapsed();

        let enc_mb_per_s = (enc_bytes as f64 / (1024.0 * 1024.0)) / enc_elapsed.as_secs_f64();
        let enc_rows_per_s = (batches * batch_rows) as f64 / enc_elapsed.as_secs_f64();
        let rt_rows_per_s = got_rows as f64 / rt_elapsed.as_secs_f64();
        let rt_bytes_total_mb = enc_bytes as f64 / (1024.0 * 1024.0);
        let rt_mb_per_s = rt_bytes_total_mb / rt_elapsed.as_secs_f64();
        let per_batch_us = rt_elapsed.as_micros() as f64 / got_batches as f64;

        println!(
            "[throughput] {label:<18} batch_rows={batch_rows:<5} batches={got_batches:<6} rows={got_rows} \
             encode_only: {enc_rows_per_s:>11.0} rows/s {enc_mb_per_s:>7.1} MB/s | \
             round_trip: {rt_rows_per_s:>11.0} rows/s {rt_mb_per_s:>7.1} MB/s ({per_batch_us:.1}us/batch)"
        );
    }

    #[test]
    #[ignore]
    fn throughput_postagg_shape() {
        // Sweeps batch size to show per-batch fixed cost vs per-row cost.
        // 1.25M total rows ≈ what one proc ships through gb_postagg at
        // 25M scale. 625K per proc × 2 = 1.25M.
        for batch_rows in [128, 512, 2048, 8192, 32_768] {
            bench_throughput("postagg", postagg_shape_batch, batch_rows, 1_250_000);
        }
    }

    #[test]
    #[ignore]
    fn throughput_probe_shape() {
        // 12.5M total rows ≈ what one proc ships through gb_right at 25M.
        for batch_rows in [128, 512, 2048, 8192, 32_768] {
            bench_throughput("probe", probe_shape_batch, batch_rows, 12_500_000);
        }
    }

    // ---------------------------------------------------------------------
    // Per-`(stage_id, task_id, partition)` channel buffer registry on the cooperative `DrainHandle`.
    //
    // Producers stamp `MppFrameHeader::batch(MppDataStreamKey::new(stage_id, task_id,
    // partition), sender)` on
    // every outgoing frame, and
    // the receiver-side cooperative drain demuxes by header into a channel buffer per
    // `(stage_id, task_id, partition)`. These tests use the `in_proc_channel` backend to drive
    // `try_drain_pass` from the test thread. That mirrors how the production path runs the drain
    // inline from `DrainGatherStream::poll_next` on the backend thread.
    // ---------------------------------------------------------------------

    /// Drain a `DrainHandle::cooperative` to completion: poll until every receiver returns
    /// `Empty`. With the `in_proc_channel` test backend the drain observes `Detached` once the
    /// producer drops its sender, so a bounded loop of `try_drain_pass` calls is enough to flush
    /// everything the producer wrote.
    fn drain_until_detached(handle: &DrainHandle) {
        for _ in 0..64 {
            handle.try_drain_pass().expect("try_drain_pass");
        }
    }

    #[test]
    fn drain_handle_demuxes_frames_by_header() {
        // One queue carrying two channels: `(0, 0)` and `(0, 1)`. Each
        // channel buffer receives only its own batches. Per-channel EOF is out of scope
        // here. See `drain_handle_eof_frame_closes_one_channel` for explicit-Eof routing
        // and `drain_handle_drop_cancels_registered_channel_buffers` for the
        // teardown-EOF contract.
        let (tx, rx) = in_proc_channel(8);
        let base = MppSender::new(Arc::new(tx));
        let s00 = base.clone_with_header(MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 0), 0));
        let s01 = base.clone_with_header(MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 1), 0));
        let receiver = MppReceiver::new(Box::new(rx));
        let handle = DrainHandle::cooperative(vec![receiver]);

        s00.send_batch(&sample_batch(2)).unwrap();
        s01.send_batch(&sample_batch(7)).unwrap();
        s00.send_batch(&sample_batch(3)).unwrap();
        drop(s00);
        drop(s01);
        drop(base);

        let buf00 = handle.register_data_channel(0, MppDataStreamKey::new(0, 0, 0));
        let buf01 = handle.register_data_channel(0, MppDataStreamKey::new(0, 0, 1));

        drain_until_detached(&handle);

        let mut p0_rows = Vec::new();
        while let Some(DrainItem::Batch(b)) = buf00.try_pop() {
            p0_rows.push(b.num_rows());
        }
        let mut p1_rows = Vec::new();
        while let Some(DrainItem::Batch(b)) = buf01.try_pop() {
            p1_rows.push(b.num_rows());
        }
        assert_eq!(p0_rows, vec![2, 3]);
        assert_eq!(p1_rows, vec![7]);
    }

    #[test]
    fn drain_handle_eof_frame_closes_one_channel() {
        // An `Eof` frame on `(0, 0)` closes that channel buffer while frames on
        // `(0, 1)` continue to flow on the same queue. `Detached` doesn't broadcast a
        // registry-wide EOF, so `(0, 1)` surfaces EOF only when the handle's `Drop`
        // runs `cancel_channel_buffers`.
        let (tx, rx) = in_proc_channel(8);
        let tx_arc: Arc<dyn BatchChannelSender> = Arc::new(tx);
        let s00 = MppSender::with_header(
            Arc::clone(&tx_arc),
            MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 0), 0),
        );
        let s01 = MppSender::with_header(
            Arc::clone(&tx_arc),
            MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 1), 0),
        );
        let receiver = MppReceiver::new(Box::new(rx));
        let handle = DrainHandle::cooperative(vec![receiver]);

        s00.send_batch(&sample_batch(4)).unwrap();
        let mut eof_buf = Vec::new();
        encode_eof_frame_into(MppDataStreamKey::new(0, 0, 0), 0, &mut eof_buf).unwrap();
        tx_arc.send_bytes(&eof_buf).unwrap();
        s01.send_batch(&sample_batch(6)).unwrap();

        let buf00 = handle.register_data_channel(0, MppDataStreamKey::new(0, 0, 0));
        let buf01 = handle.register_data_channel(0, MppDataStreamKey::new(0, 0, 1));

        drop(s00);
        drop(s01);
        drop(tx_arc);
        drain_until_detached(&handle);

        match buf00.try_pop() {
            Some(DrainItem::Batch(b)) => assert_eq!(b.num_rows(), 4),
            other => panic!("expected (0,0) batch, got {other:?}"),
        }
        assert!(matches!(buf00.try_pop(), Some(DrainItem::Eof)));

        match buf01.try_pop() {
            Some(DrainItem::Batch(b)) => assert_eq!(b.num_rows(), 6),
            other => panic!("expected (0,1) batch, got {other:?}"),
        }
        assert!(matches!(buf01.try_pop(), Some(DrainItem::Failed(_))));
    }

    #[test]
    fn drain_handle_keeps_same_worker_tasks_separate_after_one_eof() {
        // A short PostgreSQL launch can place task 0 and task 3 on the same producer proc.
        // They may both emit stage 7 / output partition 0, but task 0's EOF must not close task
        // 3's stream. This is the exact failure mode that task_id in the frame key prevents.
        let (tx, rx) = in_proc_channel(8);
        let tx_arc: Arc<dyn BatchChannelSender> = Arc::new(tx);
        let task_0 = MppSender::with_header(
            Arc::clone(&tx_arc),
            MppFrameHeader::batch(MppDataStreamKey::new(7, 0, 0), 1),
        );
        let task_3 = MppSender::with_header(
            Arc::clone(&tx_arc),
            MppFrameHeader::batch(MppDataStreamKey::new(7, 3, 0), 1),
        );
        let receiver = MppReceiver::new(Box::new(rx));
        let handle = DrainHandle::cooperative(vec![receiver]);

        task_0.send_batch(&sample_batch(2)).unwrap();
        task_3.send_batch(&sample_batch(3)).unwrap();
        let mut eof = Vec::new();
        encode_eof_frame_into(MppDataStreamKey::new(7, 0, 0), 1, &mut eof).unwrap();
        tx_arc.send_bytes(&eof).unwrap();
        task_3.send_batch(&sample_batch(5)).unwrap();
        encode_eof_frame_into(MppDataStreamKey::new(7, 3, 0), 1, &mut eof).unwrap();
        tx_arc.send_bytes(&eof).unwrap();

        let task_0_buf = handle.register_data_channel(1, MppDataStreamKey::new(7, 0, 0));
        let task_3_buf = handle.register_data_channel(1, MppDataStreamKey::new(7, 3, 0));
        drain_until_detached(&handle);

        match task_0_buf.try_pop() {
            Some(DrainItem::Batch(batch)) => assert_eq!(batch.num_rows(), 2),
            other => panic!("expected task 0 batch, got {other:?}"),
        }
        assert!(matches!(task_0_buf.try_pop(), Some(DrainItem::Eof)));

        let mut task_3_rows = Vec::new();
        while let Some(DrainItem::Batch(batch)) = task_3_buf.try_pop() {
            task_3_rows.push(batch.num_rows());
        }
        assert_eq!(task_3_rows, vec![3, 5]);
        assert!(matches!(task_3_buf.try_pop(), Some(DrainItem::Eof)));
    }

    #[test]
    fn drain_handle_register_channel_is_idempotent() {
        // Two calls for the same key return Arcs pointing to the same
        // DrainBuffer instance.
        let (_tx, rx) = in_proc_channel(8);
        let receiver = MppReceiver::new(Box::new(rx));
        let handle = DrainHandle::cooperative(vec![receiver]);

        let stream = MppDataStreamKey::new(2, 0, 3);
        let first = handle.register_data_channel(0, stream);
        let second = handle.register_data_channel(0, stream);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn drain_handle_demuxes_frames_by_stage_id() {
        // Same partition (0) for two different stage ids on the same queue.
        // The registry's compound key keeps them on separate channel buffers.
        let (tx, rx) = in_proc_channel(8);
        let tx_arc: Arc<dyn BatchChannelSender> = Arc::new(tx);
        let s_stage0 = MppSender::with_header(
            Arc::clone(&tx_arc),
            MppFrameHeader::batch(MppDataStreamKey::new(0, 0, 0), 0),
        );
        let s_stage1 = MppSender::with_header(
            Arc::clone(&tx_arc),
            MppFrameHeader::batch(MppDataStreamKey::new(1, 0, 0), 0),
        );
        let receiver = MppReceiver::new(Box::new(rx));
        let handle = DrainHandle::cooperative(vec![receiver]);

        s_stage0.send_batch(&sample_batch(2)).unwrap();
        s_stage1.send_batch(&sample_batch(9)).unwrap();
        s_stage0.send_batch(&sample_batch(4)).unwrap();
        drop(s_stage0);
        drop(s_stage1);
        drop(tx_arc);

        let buf0 = handle.register_data_channel(0, MppDataStreamKey::new(0, 0, 0));
        let buf1 = handle.register_data_channel(0, MppDataStreamKey::new(1, 0, 0));

        drain_until_detached(&handle);

        let mut stage0_rows = Vec::new();
        while let Some(DrainItem::Batch(b)) = buf0.try_pop() {
            stage0_rows.push(b.num_rows());
        }
        let mut stage1_rows = Vec::new();
        while let Some(DrainItem::Batch(b)) = buf1.try_pop() {
            stage1_rows.push(b.num_rows());
        }
        assert_eq!(stage0_rows, vec![2, 4]);
        assert_eq!(stage1_rows, vec![9]);
    }

    #[test]
    fn drain_handle_drop_cancels_registered_channel_buffers() {
        // Dropping a cooperative DrainHandle must wake any consumer holding an Arc<DrainBuffer>
        // from `register_channel`. Otherwise a query error path that tears down the mesh would
        // leave a consumer blocked on a buffer that will never see EOF.
        let (_tx, rx) = in_proc_channel(8);
        let receiver = MppReceiver::new(Box::new(rx));
        let handle = DrainHandle::cooperative(vec![receiver]);

        let buf_a = handle.register_data_channel(0, MppDataStreamKey::new(0, 0, 0));
        let buf_b = handle.register_data_channel(0, MppDataStreamKey::new(7, 0, 3));
        // No data ever flows; the handle is just dropped.
        drop(handle);

        assert!(matches!(buf_a.try_pop(), Some(DrainItem::Eof)));
        assert!(matches!(buf_b.try_pop(), Some(DrainItem::Eof)));
    }
}
