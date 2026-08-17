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

//! Ring-level async readiness notifiers and fast-path atomics.
//!
//! Bridges lock-free DSM ring buffers with `IpcMeshNotifier` signaling.
//! Implements the atomic fast path to avoid IPC signaling overhead during continuous
//! streaming.

use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use datafusion::common::{DataFusionError, Result};

use super::AliveFlag;
use super::ipc::IpcMeshNotifier;
use super::mpsc_ring::DsmMpscRingHeader;
use super::transport::MppDataStreamKey;

/// Sender-side notifier for a DSM MPSC ring.
#[allow(dead_code)]
pub struct RingSenderNotifier {
    mesh_notifier: Arc<IpcMeshNotifier>,
    target_proc: u32,
    ring: NonNull<DsmMpscRingHeader>,
    alive: AliveFlag,
}

unsafe impl Send for RingSenderNotifier {}
unsafe impl Sync for RingSenderNotifier {}

#[allow(dead_code)]
impl RingSenderNotifier {
    /// Create a new sender notifier targeting `target_proc`'s inbox ring.
    pub fn new(
        mesh_notifier: Arc<IpcMeshNotifier>,
        target_proc: u32,
        ring: NonNull<DsmMpscRingHeader>,
        alive: AliveFlag,
    ) -> Self {
        Self {
            mesh_notifier,
            target_proc,
            ring,
            alive,
        }
    }

    /// Notify the receiver that data is ready in its inbound ring.
    ///
    /// Fast path: If `consumer_waiting` is false (consumer is actively draining),
    /// no IPC signal is emitted. If true, resets the flag and sends `IpcSignal::DataReady`.
    #[inline]
    pub fn notify_data_ready(&self) {
        if !self.alive.load(Ordering::Acquire) {
            return;
        }
        let header = unsafe { self.ring.as_ref() };
        if header.consumer_waiting.swap(false, Ordering::SeqCst) {
            self.mesh_notifier.notify_data_ready(self.target_proc);
        }
    }

    /// Mark that this producer is waiting for space and wait asynchronously for a `SpaceReady` signal.
    pub async fn wait_for_space(&self) -> Result<()> {
        if !self.alive.load(Ordering::Acquire) {
            return Err(DataFusionError::Execution(format!(
                "target proc {} DSM detached",
                self.target_proc
            )));
        }
        let header = unsafe { self.ring.as_ref() };
        if header.detached.load(Ordering::Acquire) {
            return Err(DataFusionError::Execution(format!(
                "target proc {} detached",
                self.target_proc
            )));
        }
        let notified = self.mesh_notifier.space_ready_notified(self.target_proc);
        header.producers_waiting.store(true, Ordering::SeqCst);
        if let Some(fut) = notified {
            let _ = tokio::time::timeout(Duration::from_millis(50), fut).await;
            if !self.alive.load(Ordering::Acquire) {
                return Err(DataFusionError::Execution(format!(
                    "target proc {} DSM detached",
                    self.target_proc
                )));
            }
            header.producers_waiting.store(false, Ordering::SeqCst);
            if header.detached.load(Ordering::Acquire) {
                return Err(DataFusionError::Execution(format!(
                    "target proc {} detached while waiting for space",
                    self.target_proc
                )));
            }
            Ok(())
        } else {
            if self.alive.load(Ordering::Acquire) {
                header.producers_waiting.store(false, Ordering::SeqCst);
            }
            Err(DataFusionError::Internal(format!(
                "invalid target proc index: {}",
                self.target_proc
            )))
        }
    }

    /// Check if the stream has been cancelled by its consumer.
    #[inline]
    pub fn is_stream_cancelled(&self, stream: &MppDataStreamKey) -> bool {
        self.mesh_notifier.is_stream_cancelled(stream)
    }

    /// Target process ID for this sender's ring.
    pub fn target_proc(&self) -> u32 {
        self.target_proc
    }
}

/// Receiver-side notifier for a DSM MPSC ring.
#[allow(dead_code)]
pub struct RingReceiverNotifier {
    mesh_notifier: Arc<IpcMeshNotifier>,
    ring: NonNull<DsmMpscRingHeader>,
    alive: AliveFlag,
}

unsafe impl Send for RingReceiverNotifier {}
unsafe impl Sync for RingReceiverNotifier {}

impl Drop for RingReceiverNotifier {
    fn drop(&mut self) {
        self.mark_detached();
    }
}

#[allow(dead_code)]
impl RingReceiverNotifier {
    /// Create a new receiver notifier for this process's inbound ring.
    pub(super) fn new(
        mesh_notifier: Arc<IpcMeshNotifier>,
        ring: NonNull<DsmMpscRingHeader>,
        alive: AliveFlag,
    ) -> Self {
        Self {
            mesh_notifier,
            ring,
            alive,
        }
    }

    /// Mark the inbound ring as detached and broadcast space-ready to wake all blocked producers.
    pub fn mark_detached(&self) {
        if !self.alive.load(Ordering::Acquire) {
            return;
        }
        let header = unsafe { self.ring.as_ref() };
        header.detached.store(true, Ordering::Release);
        for proc in 0..self.mesh_notifier.n_procs() {
            if proc != self.mesh_notifier.this_proc() {
                self.mesh_notifier.notify_peer(proc);
            }
        }
    }

    /// Access the underlying `IpcMeshNotifier`.
    #[inline]
    pub fn mesh_notifier(&self) -> &Arc<IpcMeshNotifier> {
        &self.mesh_notifier
    }

    /// Update the consumer waiting flag.
    #[inline]
    pub fn set_consumer_waiting(&self, waiting: bool) {
        if !self.alive.load(Ordering::Acquire) {
            return;
        }
        let header = unsafe { self.ring.as_ref() };
        header.consumer_waiting.store(waiting, Ordering::SeqCst);
    }

    /// Notify waiting producers that space has been freed in the ring.
    ///
    /// Fast path: If `producers_waiting` is false, no IPC signal is emitted.
    /// If true, resets the flag and sends `IpcSignal::SpaceReady` to all peer processes.
    #[inline]
    pub fn notify_space_ready(&self) {
        if !self.alive.load(Ordering::Acquire) {
            return;
        }
        let header = unsafe { self.ring.as_ref() };
        if header.producers_waiting.swap(false, Ordering::SeqCst) {
            for proc in 0..self.mesh_notifier.n_procs() {
                if proc != self.mesh_notifier.this_proc() {
                    self.mesh_notifier.notify_space_ready(proc);
                }
            }
        }
    }

    /// Mark that the consumer is waiting for data and wait asynchronously for an incoming `DataReady` signal.
    pub async fn wait_for_data(&self) {
        let header = unsafe { self.ring.as_ref() };
        let notified = self.mesh_notifier.data_ready_notified();
        header.consumer_waiting.store(true, Ordering::SeqCst);
        let _ = tokio::time::timeout(Duration::from_millis(50), notified).await;
        header.consumer_waiting.store(false, Ordering::SeqCst);
    }

    /// Broadcast stream cancellation to a producer process.
    #[inline]
    pub fn notify_stream_cancel(&self, target_proc: u32, stream: MppDataStreamKey) {
        self.mesh_notifier.notify_stream_cancel(target_proc, stream);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shm::mpsc_ring::{
        DsmMpscReceiver, DsmMpscSender, RecvOutcome, SendError, Wakeup, create_at,
    };
    use datafusion::common::runtime::SpawnedTask;
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;

    struct TestWakeup;
    impl Wakeup for TestWakeup {
        fn wake(&self, _token: u64) {}
    }

    fn allocate_test_ring(
        ring_size: u32,
        slot_capacity: u32,
    ) -> (Vec<u8>, NonNull<DsmMpscRingHeader>) {
        let region_bytes = DsmMpscRingHeader::region_bytes(ring_size, slot_capacity);
        let mut region = vec![0u8; region_bytes + 64];
        let align_offset = region.as_ptr().align_offset(64);
        let base = unsafe { region.as_mut_ptr().add(align_offset) };
        let ptr = unsafe { create_at(base, ring_size, slot_capacity) };
        (region, NonNull::new(ptr).unwrap())
    }

    #[tokio::test]
    async fn test_notifier_fast_path_zero_ipc_signals() {
        let (_mem, ring) = allocate_test_ring(4, 64);
        let mesh = IpcMeshNotifier::in_memory_mesh(2);
        let alive = Arc::new(AtomicBool::new(true));
        let tx_notif = RingSenderNotifier::new(Arc::clone(&mesh[0]), 1, ring, Arc::clone(&alive));
        let rx_notif = RingReceiverNotifier::new(Arc::clone(&mesh[1]), ring, alive);

        let header = unsafe { ring.as_ref() };
        // Initial state: neither is waiting
        assert!(!header.consumer_waiting.load(Ordering::Acquire));
        assert!(!header.producers_waiting.load(Ordering::Acquire));

        // Calling notify_data_ready when consumer is NOT waiting does not emit signal
        tx_notif.notify_data_ready();
        assert!(!header.consumer_waiting.load(Ordering::Acquire));

        // Calling notify_space_ready when producers are NOT waiting does not emit signal
        rx_notif.notify_space_ready();
        assert!(!header.producers_waiting.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn test_async_ring_consumer_wakeup_on_empty() {
        let (_mem, ring) = allocate_test_ring(4, 64);
        let mesh = IpcMeshNotifier::in_memory_mesh(2);
        let alive = Arc::new(AtomicBool::new(true));

        let sender = unsafe { DsmMpscSender::new(ring, Arc::new(TestWakeup), Arc::clone(&alive)) };
        let receiver = unsafe { DsmMpscReceiver::new(ring, Arc::clone(&alive)) };

        let tx_notif = RingSenderNotifier::new(Arc::clone(&mesh[0]), 1, ring, Arc::clone(&alive));
        let rx_notif = RingReceiverNotifier::new(Arc::clone(&mesh[1]), ring, Arc::clone(&alive));

        // Consumer task attempts to receive from empty ring
        let consumer_task = SpawnedTask::spawn(async move {
            let mut receiver = receiver;
            let mut out = Vec::new();
            let outcome = receiver.recv(&rx_notif, &mut out).await;
            (outcome, out)
        });

        // Small delay to ensure consumer has called wait_for_data and set consumer_waiting = true
        tokio::time::sleep(Duration::from_millis(20)).await;

        let payload = b"hello_async_shm";
        sender.send(&tx_notif, payload).await.unwrap();

        let res = tokio::time::timeout(Duration::from_secs(1), consumer_task.join()).await;
        assert!(res.is_ok(), "consumer timed out waiting for data");
        let (outcome, out) = res.unwrap().unwrap();
        assert_eq!(outcome, RecvOutcome::Bytes);
        assert_eq!(&out, payload);
    }

    #[tokio::test]
    async fn test_async_ring_producer_wakeup_on_full() {
        // Small ring: 2 slots
        let (_mem, ring) = allocate_test_ring(2, 64);
        let mesh = IpcMeshNotifier::in_memory_mesh(2);
        let alive = Arc::new(AtomicBool::new(true));

        let sender = unsafe { DsmMpscSender::new(ring, Arc::new(TestWakeup), Arc::clone(&alive)) };
        let mut receiver = unsafe { DsmMpscReceiver::new(ring, Arc::clone(&alive)) };

        let rx_notif = RingReceiverNotifier::new(Arc::clone(&mesh[1]), ring, Arc::clone(&alive));

        // Fill both slots in the ring
        sender.try_send(b"batch_1").unwrap();
        sender.try_send(b"batch_2").unwrap();

        // Third send should block waiting for space
        let tx_notif_clone =
            RingSenderNotifier::new(Arc::clone(&mesh[0]), 1, ring, Arc::clone(&alive));
        let producer_task = SpawnedTask::spawn(async move {
            sender.send(&tx_notif_clone, b"batch_3").await.unwrap();
            true
        });

        tokio::time::sleep(Duration::from_millis(20)).await;

        // Drain one batch to free space
        let mut out = Vec::new();
        let outcome = receiver.recv(&rx_notif, &mut out).await;
        assert_eq!(outcome, RecvOutcome::Bytes);
        assert_eq!(&out, b"batch_1");

        let res = tokio::time::timeout(Duration::from_secs(1), producer_task.join()).await;
        assert!(res.is_ok(), "producer timed out waiting for space");
        assert!(res.unwrap().unwrap());
    }

    #[tokio::test]
    async fn test_async_ring_concurrent_producers() {
        let (_mem, ring) = allocate_test_ring(16, 64);
        let mesh = IpcMeshNotifier::in_memory_mesh(3);
        let alive = Arc::new(AtomicBool::new(true));

        let sender1 = unsafe { DsmMpscSender::new(ring, Arc::new(TestWakeup), Arc::clone(&alive)) };
        let sender2 = unsafe { DsmMpscSender::new(ring, Arc::new(TestWakeup), Arc::clone(&alive)) };
        let receiver = unsafe { DsmMpscReceiver::new(ring, Arc::clone(&alive)) };

        let tx_notif1 = RingSenderNotifier::new(Arc::clone(&mesh[0]), 2, ring, Arc::clone(&alive));
        let tx_notif2 = RingSenderNotifier::new(Arc::clone(&mesh[1]), 2, ring, Arc::clone(&alive));
        let rx_notif = RingReceiverNotifier::new(Arc::clone(&mesh[2]), ring, Arc::clone(&alive));

        let n_messages = 50;

        let p1 = SpawnedTask::spawn(async move {
            for i in 0..n_messages {
                let msg = format!("p1_{i}");
                sender1.send(&tx_notif1, msg.as_bytes()).await.unwrap();
            }
        });

        let p2 = SpawnedTask::spawn(async move {
            for i in 0..n_messages {
                let msg = format!("p2_{i}");
                sender2.send(&tx_notif2, msg.as_bytes()).await.unwrap();
            }
        });

        let consumer = SpawnedTask::spawn(async move {
            let mut receiver = receiver;
            let mut count = 0;
            let mut out = Vec::new();
            while count < n_messages * 2 {
                let outcome = receiver.recv(&rx_notif, &mut out).await;
                if outcome == RecvOutcome::Bytes {
                    count += 1;
                }
            }
            count
        });

        p1.join().await.unwrap();
        p2.join().await.unwrap();
        let total = tokio::time::timeout(Duration::from_secs(2), consumer.join())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(total, n_messages * 2);
    }

    #[tokio::test]
    async fn test_receiver_drop_unblocks_producer_waiting_for_space() {
        let (_mem, ring) = allocate_test_ring(2, 64);
        let mesh = IpcMeshNotifier::in_memory_mesh(2);
        let alive = Arc::new(AtomicBool::new(true));

        let sender = unsafe { DsmMpscSender::new(ring, Arc::new(TestWakeup), Arc::clone(&alive)) };
        let receiver = unsafe { DsmMpscReceiver::new(ring, Arc::clone(&alive)) };

        let tx_notif = RingSenderNotifier::new(Arc::clone(&mesh[0]), 1, ring, Arc::clone(&alive));
        let rx_notif = RingReceiverNotifier::new(Arc::clone(&mesh[1]), ring, Arc::clone(&alive));

        // Fill ring to capacity (2 slots)
        sender.try_send(b"batch_1").unwrap();
        sender.try_send(b"batch_2").unwrap();

        // Spawn producer task attempting to send into full ring
        let producer_task =
            SpawnedTask::spawn(async move { sender.send(&tx_notif, b"batch_3").await });

        // Let producer task run and suspend on wait_for_space
        tokio::time::sleep(Duration::from_millis(20)).await;

        // Drop receiver and rx_notif
        drop(receiver);
        drop(rx_notif);

        // Producer task must immediately unblock with SendError::Detached
        let res = tokio::time::timeout(Duration::from_secs(1), producer_task.join()).await;
        assert!(res.is_ok(), "producer did not unblock on receiver drop");
        assert_eq!(res.unwrap().unwrap(), Err(SendError::Detached));
    }
}
