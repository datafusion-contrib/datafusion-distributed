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

//! Cross-process asynchronous IPC signaling mesh using `interprocess::local_socket`.
//!
//! Provides lightweight, edge-triggered event notifications between leader and worker processes
//! sharing Direct Shared Memory (DSM) ring buffers. All data, cancellation, EOF, and detachment
//! state are maintained in-band inside DSM; this notifier acts purely as an edge-triggered waker.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use datafusion::common::runtime::SpawnedTask;
use datafusion::common::{DataFusionError, Result};
use interprocess::local_socket::traits::Stream as _;
use interprocess::local_socket::traits::tokio::Listener as _;
use interprocess::local_socket::{
    GenericFilePath, ListenerOptions, Stream as SyncStream, ToFsName,
    tokio::{Listener, Stream},
};
use tokio::io::AsyncReadExt;
use tokio::sync::Notify;

use super::transport::MppDataStreamKey;

/// Resource guard that removes the entire query socket directory on drop.
pub struct QuerySocketScope {
    dir: PathBuf,
}

impl QuerySocketScope {
    pub fn new(base_dir: &Path, query_id: &str) -> std::io::Result<Self> {
        let dir = base_dir.join(format!("df_dist_{query_id}"));
        std::fs::create_dir_all(&dir)?;
        Ok(Self { dir })
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

impl Drop for QuerySocketScope {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

enum PeerSender {
    Socket(Arc<Mutex<SyncStream>>),
    Direct(Arc<IpcMeshNotifier>),
}

pub type DetachmentChecker = Arc<dyn Fn(u32) -> bool + Send + Sync>;

/// Mesh notifier managing async readiness signaling for one process.
#[allow(dead_code)]
pub struct IpcMeshNotifier {
    this_proc: u32,
    n_procs: u32,
    wake_notify: Arc<Notify>,
    peer_senders: Mutex<Vec<Option<PeerSender>>>,
    background_tasks: Mutex<Vec<SpawnedTask<()>>>,
    detachment_checker: Mutex<Option<DetachmentChecker>>,
}

#[allow(dead_code)]
impl IpcMeshNotifier {
    /// Recursively remove the query socket directory if it exists.
    pub fn cleanup_query_sockets(base_dir: &Path, query_id: &str) {
        let dir = base_dir.join(format!("df_dist_{query_id}"));
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Format a filesystem socket path or namespace for `(query_id, proc_idx)`.
    pub fn socket_name_for_proc(base_dir: &Path, query_id: &str, proc_idx: u32) -> PathBuf {
        if query_id.is_empty() {
            base_dir.join(format!("p{proc_idx}.sock"))
        } else {
            let dir = base_dir.join(format!("df_dist_{query_id}"));
            let _ = std::fs::create_dir_all(&dir);
            dir.join(format!("proc_{proc_idx}.sock"))
        }
    }

    /// Construct an in-memory mesh for testing without OS socket allocations.
    pub fn in_memory_mesh(n_procs: u32) -> Vec<Arc<Self>> {
        let mut notifiers = Vec::with_capacity(n_procs as usize);

        for this_proc in 0..n_procs {
            notifiers.push(Arc::new(Self {
                this_proc,
                n_procs,
                wake_notify: Arc::new(Notify::new()),
                peer_senders: Mutex::new((0..n_procs).map(|_| None).collect()),
                background_tasks: Mutex::new(Vec::new()),
                detachment_checker: Mutex::new(None),
            }));
        }

        // Direct in-memory cross-connection without background threads
        for (i, notif) in notifiers.iter().enumerate() {
            let mut senders = notif.peer_senders.lock().unwrap();
            for (j, peer_notif) in notifiers.iter().enumerate() {
                if i != j {
                    senders[j] = Some(PeerSender::Direct(Arc::clone(peer_notif)));
                }
            }
        }

        notifiers
    }

    /// Bind a local socket listener and return an `IpcMeshNotifier` instance.
    pub async fn bind(
        base_dir: &Path,
        query_id: &str,
        this_proc: u32,
        n_procs: u32,
    ) -> Result<(Arc<Self>, Listener)> {
        let socket_path = Self::socket_name_for_proc(base_dir, query_id, this_proc);
        let _ = std::fs::remove_file(&socket_path);

        let name = socket_path
            .as_path()
            .to_fs_name::<GenericFilePath>()
            .map_err(|e| DataFusionError::Internal(format!("invalid socket name: {e}")))?;

        let listener = ListenerOptions::new()
            .name(name)
            .create_tokio()
            .map_err(|e| DataFusionError::Internal(format!("failed to bind local socket: {e}")))?;

        let notifier = Arc::new(Self {
            this_proc,
            n_procs,
            wake_notify: Arc::new(Notify::new()),
            peer_senders: Mutex::new((0..n_procs).map(|_| None).collect()),
            background_tasks: Mutex::new(Vec::new()),
            detachment_checker: Mutex::new(None),
        });

        Ok((notifier, listener))
    }

    pub fn set_detachment_checker(&self, checker: DetachmentChecker) {
        *self.detachment_checker.lock().unwrap() = Some(checker);
    }

    pub fn is_peer_detached(&self, proc_id: u32) -> bool {
        if let Some(checker) = self.detachment_checker.lock().unwrap().as_ref() {
            checker(proc_id)
        } else {
            false
        }
    }

    /// Establish connections to all peer local sockets with retry.
    pub async fn connect_peers(
        self: &Arc<Self>,
        base_dir: &Path,
        query_id: &str,
        timeout: Duration,
    ) -> Result<()> {
        let query_dir = base_dir.join(format!("df_dist_{query_id}"));
        let deadline = tokio::time::Instant::now() + timeout;
        for target_proc in 0..self.n_procs {
            if target_proc == self.this_proc {
                continue;
            }
            let peer_socket_path = Self::socket_name_for_proc(base_dir, query_id, target_proc);
            let name = peer_socket_path
                .as_path()
                .to_fs_name::<GenericFilePath>()
                .map_err(|e| DataFusionError::Internal(format!("invalid peer socket name: {e}")))?;

            let sync_stream_opt = loop {
                if !query_dir.exists() || self.is_peer_detached(target_proc) {
                    // Peer process or entire query has already detached/finished
                    break None;
                }
                match SyncStream::connect(name.borrow()) {
                    Ok(s) => break Some(s),
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                        // Peer process already finished and shut down its listener
                        break None;
                    }
                    Err(e)
                        if e.kind() == std::io::ErrorKind::NotFound
                            && self.is_peer_detached(target_proc) =>
                    {
                        // Peer process already finished, exited, and unlinked its listener socket
                        break None;
                    }
                    Err(_) if tokio::time::Instant::now() < deadline => {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                    Err(e) => {
                        if !query_dir.exists() || self.is_peer_detached(target_proc) {
                            break None;
                        }
                        return Err(DataFusionError::Internal(format!(
                            "timed out connecting to peer proc {target_proc} socket {peer_socket_path:?}: {e}"
                        )));
                    }
                }
            };

            if let Some(mut sync_stream) = sync_stream_opt {
                // Write 4-byte handshake so receiver knows which proc connected
                if let Err(e) = sync_stream.write_all(&self.this_proc.to_le_bytes()) {
                    return Err(DataFusionError::Internal(format!(
                        "failed to send handshake to peer proc {target_proc}: {e}"
                    )));
                }
                let _ = sync_stream.flush();
                let _ = sync_stream.set_nonblocking(true);

                self.peer_senders.lock().unwrap()[target_proc as usize] =
                    Some(PeerSender::Socket(Arc::new(Mutex::new(sync_stream))));
            }
        }
        Ok(())
    }

    /// Accept incoming peer socket connections on the listener and spawn reader dispatchers.
    pub fn start_listener_loop(self: &Arc<Self>, listener: Listener) {
        let notifier = Arc::clone(self);
        let task = SpawnedTask::spawn(async move {
            while let Ok(stream) = listener.accept().await {
                let notif = Arc::clone(&notifier);
                let reader_task = SpawnedTask::spawn(Self::run_socket_reader(notif, stream));
                notifier.background_tasks.lock().unwrap().push(reader_task);
            }
        });
        self.background_tasks.lock().unwrap().push(task);
    }

    /// Wake all local waiters.
    pub fn handle_wake(&self) {
        self.wake_notify.notify_waiters();
    }

    async fn run_socket_reader(notifier: Arc<Self>, mut stream: Stream) {
        let mut handshake = [0u8; 4];
        if stream.read_exact(&mut handshake).await.is_err() {
            return;
        }
        let mut buf = [0u8; 1];
        while stream.read_exact(&mut buf).await.is_ok() {
            notifier.handle_wake();
        }
        notifier.handle_wake();
    }

    /// Send a wake notification byte to `target_proc`.
    pub fn notify_peer(&self, target_proc: u32) {
        let senders = self.peer_senders.lock().unwrap();
        if let Some(Some(peer)) = senders.get(target_proc as usize) {
            match peer {
                PeerSender::Socket(stream) => {
                    if let Ok(mut guard) = stream.lock() {
                        let _ = guard.write_all(&[1_u8]);
                        let _ = guard.flush();
                    }
                }
                PeerSender::Direct(target) => {
                    target.handle_wake();
                }
            }
        }
    }

    /// Notify `target_proc` that data is ready in its inbound ring.
    #[inline]
    pub fn notify_data_ready(&self, target_proc: u32) {
        self.notify_peer(target_proc);
    }

    /// Notify `target_proc` that space is ready in this proc's inbound ring.
    #[inline]
    pub fn notify_space_ready(&self, target_proc: u32) {
        self.notify_peer(target_proc);
    }

    /// Notify `target_proc` that stream `stream` was cancelled.
    #[inline]
    pub fn notify_stream_cancel(&self, target_proc: u32, _stream: MppDataStreamKey) {
        self.notify_peer(target_proc);
    }

    /// Broadcast that this proc is detaching from the mesh.
    pub fn notify_detach(&self) {
        for proc in 0..self.n_procs {
            if proc != self.this_proc {
                self.notify_peer(proc);
            }
        }
    }

    /// Returns a future that resolves when a wake signal is received.
    #[inline]
    pub fn data_ready_notified(&self) -> impl std::future::Future<Output = ()> + '_ {
        self.wake_notify.notified()
    }

    /// Obtain a notification future for space ready on `target_proc`'s inbound ring.
    #[inline]
    pub fn space_ready_notified(
        &self,
        _target_proc: u32,
    ) -> Option<impl std::future::Future<Output = ()> + '_> {
        Some(self.wake_notify.notified())
    }

    /// Wait asynchronously until woken.
    pub async fn wait_for_data(&self) {
        self.wake_notify.notified().await;
    }

    /// Wait asynchronously until woken.
    pub async fn wait_for_space(&self, _target_proc: u32) -> Result<()> {
        self.wake_notify.notified().await;
        Ok(())
    }

    /// Check if stream `stream` was cancelled by its consumer (handled in-band in DSM ring).
    #[inline]
    pub fn is_stream_cancelled(&self, _stream: &MppDataStreamKey) -> bool {
        false
    }

    /// This process's proc index.
    pub fn this_proc(&self) -> u32 {
        self.this_proc
    }

    /// Total number of processes in the mesh.
    pub fn n_procs(&self) -> u32 {
        self.n_procs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn in_memory_mesh_signaling() {
        let n_procs = 3;
        let mesh = IpcMeshNotifier::in_memory_mesh(n_procs);
        assert_eq!(mesh.len(), 3);
        assert_eq!(mesh[0].this_proc(), 0);
        assert_eq!(mesh[0].n_procs(), 3);
        assert_eq!(mesh[1].this_proc(), 1);

        let proc0 = Arc::clone(&mesh[0]);
        let proc1 = Arc::clone(&mesh[1]);

        // Test DataReady: proc0 notifies proc1
        let wait_task = SpawnedTask::spawn(async move {
            proc1.wait_for_data().await;
            true
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        proc0.notify_data_ready(1);

        let result = tokio::time::timeout(Duration::from_secs(1), wait_task.join()).await;
        assert!(result.is_ok());
        assert!(result.unwrap().unwrap());

        // Test SpaceReady: proc1 notifies proc0
        let proc0_clone = Arc::clone(&mesh[0]);
        let space_task = SpawnedTask::spawn(async move {
            proc0_clone.wait_for_space(1).await.unwrap();
            true
        });

        tokio::time::sleep(Duration::from_millis(10)).await;
        mesh[1].notify_space_ready(0);

        let result = tokio::time::timeout(Duration::from_secs(1), space_task.join()).await;
        assert!(result.is_ok());
        assert!(result.unwrap().unwrap());
    }

    #[tokio::test]
    async fn socket_mesh_real_handshake() {
        let temp_dir = tempfile::tempdir().unwrap();
        let query_id = "test_q1";
        let n_procs = 2;

        // Bind listeners
        let (proc0, listener0) = IpcMeshNotifier::bind(temp_dir.path(), query_id, 0, n_procs)
            .await
            .unwrap();
        let (proc1, listener1) = IpcMeshNotifier::bind(temp_dir.path(), query_id, 1, n_procs)
            .await
            .unwrap();

        proc0.start_listener_loop(listener0);
        proc1.start_listener_loop(listener1);

        // Connect peers
        let connect0 = {
            let p0 = Arc::clone(&proc0);
            let dir = temp_dir.path().to_path_buf();
            SpawnedTask::spawn(async move {
                p0.connect_peers(&dir, query_id, Duration::from_secs(2))
                    .await
            })
        };
        let connect1 = {
            let p1 = Arc::clone(&proc1);
            let dir = temp_dir.path().to_path_buf();
            SpawnedTask::spawn(async move {
                p1.connect_peers(&dir, query_id, Duration::from_secs(2))
                    .await
            })
        };

        connect0.join().await.unwrap().unwrap();
        connect1.join().await.unwrap().unwrap();

        // Send DataReady from proc0 to proc1 over real local socket
        let p1_clone = Arc::clone(&proc1);
        let wait_task = SpawnedTask::spawn(async move {
            p1_clone.wait_for_data().await;
            true
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        proc0.notify_data_ready(1);

        let res = tokio::time::timeout(Duration::from_secs(2), wait_task.join()).await;
        assert!(res.is_ok(), "timed out waiting for socket signal");
        assert!(res.unwrap().unwrap());
    }
}
