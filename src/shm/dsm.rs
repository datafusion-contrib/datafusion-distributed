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

//! Mesh-multiplexed DSM layout: one MPSC inbox per receiver process.
//!
//! Each MPP query allocates a single DSM region:
//!
//! ```text
//!   +--- MppDsmHeader (repr C, MAXALIGN-padded) -------------+
//!   |    magic, version, n_procs, queue_bytes               |
//!   |    plan_offset, plan_len, queues_offset, region_total |
//!   +-------------------------------------------------------+
//!   | plan bytes (bincode-serialized worker fragment)       |
//!   +-------------------------------------------------------+
//!   | padding to MAXALIGN                                   |
//!   +-------------------------------------------------------+
//!   | DsmMpscRing inbox array: n_procs inboxes              |
//!   |   inbox(receiver) = queues_offset                     |
//!   |     + receiver * queue_bytes                          |
//!   +-------------------------------------------------------+
//! ```
//!
//! - `n_procs` = 1 leader + N parallel workers. Leader is `proc_idx = 0`; workers are
//!   `proc_idx = ParallelWorkerNumber + 1`.
//! - Each process attaches as receiver to its own inbox (one MPSC ring) and as sender
//!   to each of N-1 peer inboxes. Senders stamp `MppFrameHeader::sender_proc` on every
//!   frame so the receiver demuxes by source.
//! - Self-loop frames (proc → itself) ride an in-proc channel installed by
//!   `worker_setup`, not a DSM slot: each `DsmMpscReceiver` is owned by a single
//!   receiver process, so there is no DSM slot for the self-loop pair.

use std::ffi::c_void;
use std::mem::size_of;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::AliveFlag;
use super::mesh::{align_up_maxalign_checked, aligned_queue_bytes};
use super::mpsc_ring::{self, DsmMpscReceiver, DsmMpscRingHeader, DsmMpscSender, Wakeup};

/// Number of slots in each per-receiver MPSC ring. With operator-visible
/// The embedder's queue-size knob divided across `RING_SLOTS` slots, each slot holds
/// up to `(queue_bytes / RING_SLOTS) - SLOT_HEADER` bytes of payload. That's enough
/// for typical Arrow batches at the bench scales we measured. Fixed at compile time
/// for now; a future GUC could expose it if bench data points at a different sweet
/// spot.
const RING_SLOTS: u32 = 8;

/// Floor `ring_dims_for` applies to the per-slot payload capacity. The layout minimum check
/// uses the same floor so the constructed ring always fits its inbox region.
const MIN_SLOT_CAPACITY: u32 = 64;

/// Cache-line alignment for the per-inbox DsmMpscRing header. The ring itself is plain
/// `#[repr(C)]` (the base address is only MAXALIGN-guaranteed), but spacing the per-inbox
/// offsets to 64 keeps hot headers on separate cache lines when the base cooperates;
/// both `queues_offset` and per-inbox `queue_bytes` are aligned up to this so the
/// computed `inbox_offset(r) = queues_offset + r * queue_bytes` lands at a 64-aligned
/// address for every `r`.
const RING_ALIGN: usize = 64;

#[inline]
fn align_up_ring(n: usize) -> Option<usize> {
    let mask = RING_ALIGN - 1;
    n.checked_add(mask).map(|x| x & !mask)
}

const MPP_DSM_MAGIC: u32 = 0x4D50_5052; // "MPPR" (RPC variant)
/// Bump on any wire-incompatible change to the DSM header layout or to the inbox-offset
/// math, so attaching workers reject mismatched leaders loudly rather than reading
/// garbage. Validated in [`MppDsmHeader::validate`].
const MPP_DSM_HEADER_VERSION: u32 = 4;

/// Absolute cap on DSM region size. 16 GiB is two orders of magnitude beyond
/// any realistic workload; the cap fails early on a pathologically oversized
/// request rather than asking PG for ~`usize::MAX` bytes.
const MPP_DSM_MAX_BYTES: usize = 16 * 1024 * 1024 * 1024;

/// C-repr header at offset 0 of the DSM region.
///
/// Layout: three `u32`s + padding, then five `u64`s.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MppDsmHeader {
    pub(super) magic: u32,
    pub(super) header_version: u32,
    /// Total proc count. Leader is `proc_idx = 0`; workers are
    /// `proc_idx = ParallelWorkerNumber + 1`. The DSM region holds `n_procs`
    /// per-receiver MPSC inboxes laid out contiguously after the plan bytes.
    pub n_procs: u32,
    pub(super) _pad: u32,
    pub(super) queue_bytes: u64,
    pub(super) plan_offset: u64,
    pub(super) plan_len: u64,
    pub(super) queues_offset: u64,
    pub region_total: u64,
}

impl MppDsmHeader {
    fn from_layout(layout: &DsmLayout) -> Self {
        Self {
            magic: MPP_DSM_MAGIC,
            header_version: MPP_DSM_HEADER_VERSION,
            n_procs: layout.n_procs,
            _pad: 0,
            queue_bytes: layout.queue_bytes as u64,
            plan_offset: layout.plan_offset as u64,
            plan_len: layout.plan_len as u64,
            queues_offset: layout.queues_offset as u64,
            region_total: layout.region_total as u64,
        }
    }

    pub(super) fn validate(&self, region_total: u64) -> Result<(), &'static str> {
        if self.magic != MPP_DSM_MAGIC {
            return Err("mpp: DSM header magic mismatch");
        }
        if self.header_version != MPP_DSM_HEADER_VERSION {
            return Err("mpp: DSM header version mismatch");
        }
        if self.n_procs == 0 {
            return Err("mpp: header n_procs must be > 0");
        }
        if self.region_total != region_total {
            return Err("mpp: DSM region_total in header disagrees with attached size");
        }
        match self.plan_offset.checked_add(self.plan_len) {
            None => return Err("mpp: plan_offset + plan_len overflow"),
            Some(end) if end > self.queues_offset => {
                return Err("mpp: plan would overlap queues area");
            }
            _ => {}
        }
        if self.queues_offset > region_total {
            return Err("mpp: queues_offset past end of region");
        }
        Ok(())
    }

    /// Byte offset (relative to DSM base) of `receiver_proc`'s MPSC inbox.
    ///
    /// The owner attaches as receiver (`DsmMpscReceiver`); every other process attaches
    /// as sender (`DsmMpscSender`) to the same inbox and stamps its identity into the
    /// frame header (`MppFrameHeader::sender_proc`).
    pub(super) fn inbox_offset(&self, receiver_proc: u32) -> u64 {
        debug_assert!(receiver_proc < self.n_procs);
        self.queues_offset + (receiver_proc as u64) * self.queue_bytes
    }
}

/// Pure-math layout for [`compute_dsm_layout`].
#[derive(Debug, Clone, Copy)]
pub(super) struct DsmLayout {
    pub n_procs: u32,
    pub queue_bytes: usize,
    pub plan_offset: usize,
    pub plan_len: usize,
    pub queues_offset: usize,
    pub region_total: usize,
}

/// Compute the DSM region size and field offsets for one MPP query.
///
/// `n_procs` is the total proc count (1 leader + N workers); the region holds one
/// `DsmMpscRing` inbox per process. Every other process attaches as sender to that
/// inbox and the receiver demultiplexes by `MppFrameHeader::sender_proc`.
///
/// `queue_bytes` is the per-inbox total (ring header + `RING_SLOTS` slots).
/// The embedder's operator-facing queue-size knob controls this value.
pub(super) fn compute_dsm_layout(
    n_procs: u32,
    queue_bytes: usize,
    plan_len: usize,
) -> Result<DsmLayout, &'static str> {
    if n_procs < 2 {
        return Err("mpp: n_procs must be >= 2 (leader + at least one worker)");
    }
    // MAXALIGN-round-down first (operator-friendly), then round up to the ring's
    // 64-byte alignment requirement. Doing it in this order means each per-inbox
    // region is both MAXALIGN-aligned (PG DSM convention) AND cache-line aligned,
    // which keeps the rings' hot header fields on separate cache lines.
    let queue_bytes = aligned_queue_bytes(queue_bytes);
    if queue_bytes == 0 {
        return Err("mpp: queue_bytes too small after alignment");
    }
    let queue_bytes = align_up_ring(queue_bytes).ok_or("mpp: queue_bytes alignment overflow")?;
    // Each inbox must have room for the ring `ring_dims_for` will actually build:
    // `RING_SLOTS` slots with the 64-byte capacity floor. Checking against a smaller
    // ring here would let `create_at` write past the inbox region for tiny
    // `queue_bytes`, overlapping the next inbox.
    if queue_bytes < DsmMpscRingHeader::region_bytes(RING_SLOTS, MIN_SLOT_CAPACITY) {
        return Err("mpp: queue_bytes too small for ring header + min slot capacity");
    }
    let header_end = align_up_maxalign_checked(size_of::<MppDsmHeader>())
        .ok_or("mpp: header alignment overflow")?;
    let plan_offset = header_end;
    let plan_end = plan_offset
        .checked_add(plan_len)
        .ok_or("mpp: plan offset+len overflow")?;
    // Round queues_offset up to RING_ALIGN so the first inbox starts at a 64-aligned
    // address; subsequent inboxes are queue_bytes apart and queue_bytes is RING_ALIGN-
    // aligned, so they all land on cache-line boundaries.
    let queues_offset = align_up_ring(plan_end).ok_or("mpp: queues alignment overflow")?;
    let queues_bytes = (n_procs as usize)
        .checked_mul(queue_bytes)
        .ok_or("mpp: queues bytes overflow")?;
    let region_total = queues_offset
        .checked_add(queues_bytes)
        .ok_or("mpp: region total overflow")?;
    if region_total > MPP_DSM_MAX_BYTES {
        return Err("mpp: DSM region exceeds MPP_DSM_MAX_BYTES");
    }
    Ok(DsmLayout {
        n_procs,
        queue_bytes,
        plan_offset,
        plan_len,
        queues_offset,
        region_total,
    })
}

/// Derive `(ring_slots, slot_capacity)` for a per-inbox region of `queue_bytes`.
/// Total ring region size = `DsmMpscRingHeader::region_bytes(ring_slots, slot_capacity)`
/// which fits within `queue_bytes` by construction.
fn ring_dims_for(queue_bytes: usize) -> (u32, u32) {
    let header = std::mem::size_of::<DsmMpscRingHeader>();
    // queue_bytes >= ring_dims minimum is enforced in compute_dsm_layout; we recompute
    // here without re-validating.
    let slot_total_bytes = queue_bytes.saturating_sub(header);
    let slot_capacity = (slot_total_bytes / RING_SLOTS as usize).max(MIN_SLOT_CAPACITY as usize);
    // Cap slot_capacity at u32::MAX (DsmMpscRing's field type) to avoid casting wrap.
    let slot_capacity = slot_capacity.min(u32::MAX as usize) as u32;
    (RING_SLOTS, slot_capacity)
}

/// Per-proc return from `attach_proc`: N-1 outbound senders (one per peer inbox) plus a
/// single inbound receiver (this proc's own inbox).
///
/// The own-inbox is the multiplexed entry point: every peer attaches to it as a sender
/// (each `DsmMpscSender` increments the ring's `sender_count`) and stamps its identity
/// into `MppFrameHeader::sender_proc` on every frame. The receiver side pulls frames
/// off that single ring and routes them to per-`(sender_proc, stage_id, partition)`
/// channel buffers via [`DrainHandle`].
pub(super) struct ProcAttach {
    /// `outbound_senders[i]` writes to peer `peer_proc_for_index(this_proc, i)`'s inbox.
    /// `peer_proc(i) = i if i < this_proc else i + 1` skips the self-loop entry.
    pub(super) outbound_senders: Vec<DsmMpscSender>,
    /// This process's own MPSC inbox receiver. Drained inline by `DrainHandle`.
    pub(super) inbound_receiver: DsmMpscReceiver,
    pub(super) alive: AliveFlag,
}

/// Read `region_total` out of the header at the start of an initialized region.
///
/// # Safety
/// `base` must point at the start of a region a leader wrote via [`leader_init`].
pub(super) unsafe fn read_region_total(base: *const c_void) -> u64 {
    unsafe { std::ptr::read(base as *const MppDsmHeader).region_total }
}

/// Translate a peer index (`0..n_procs - 1`) into a process index
/// (`0..n_procs`) by skipping the self-loop slot.
#[inline]
pub(super) fn peer_proc_for_index(this_proc: u32, peer_idx: u32) -> u32 {
    if peer_idx < this_proc {
        peer_idx
    } else {
        peer_idx + 1
    }
}

/// Initialize the DSM region as the leader (`proc_idx = 0`). Writes the MppDsmHeader,
/// copies the plan bytes, initializes the N MPSC inboxes via `DsmMpscRing::create_at`,
/// and attaches the leader as receiver to its own inbox, plus (when `attach_senders`) as
/// sender to each peer for the control plane (work-unit frames).
///
/// `attach_senders` is a commitment: a ring latches `detached` when its sender count falls to
/// zero, so a leader that attaches and then drops its senders before a worker attached poisons
/// that worker's inbox. Attach only when the senders outlive the query.
///
/// # Safety
/// - `coordinate` must point to the start of a DSM region of size `>= layout.region_total`.
/// - The region must be uninitialized (the leader is the first writer).
pub(super) unsafe fn leader_init(
    coordinate: *mut c_void,
    layout: &DsmLayout,
    plan_bytes: &[u8],
    wakeup: Arc<dyn Wakeup>,
    attach_senders: bool,
) -> Result<ProcAttach, String> {
    if coordinate.is_null() {
        return Err("mpp: leader_init given null coordinate".into());
    }
    if plan_bytes.len() != layout.plan_len {
        return Err(format!(
            "mpp: plan_bytes.len()={} != layout.plan_len={}",
            plan_bytes.len(),
            layout.plan_len
        ));
    }

    let base = coordinate as *mut u8;

    // Header.
    unsafe {
        std::ptr::write(
            base.cast::<MppDsmHeader>(),
            MppDsmHeader::from_layout(layout),
        );
    }
    // Plan bytes.
    unsafe {
        std::ptr::copy_nonoverlapping(
            plan_bytes.as_ptr(),
            base.add(layout.plan_offset),
            plan_bytes.len(),
        );
    }

    // Initialize the N MPSC inboxes. Workers can't do this (the region is uninitialized
    // at their attach time), so the leader runs DsmMpscRing::create_at for every receiver.
    let header = MppDsmHeader::from_layout(layout);
    let n_procs = layout.n_procs;
    let (ring_slots, slot_capacity) = ring_dims_for(layout.queue_bytes);
    for r in 0..n_procs {
        let off = header.inbox_offset(r) as usize;
        let inbox_addr = unsafe { base.add(off) };
        unsafe { mpsc_ring::create_at(inbox_addr, ring_slots, slot_capacity) };
    }

    let attach = unsafe { attach_proc(base, &header, 0, attach_senders, wakeup) };
    Ok(attach)
}

/// Attach to the leader-initialized DSM region as `proc_idx` (`0 = leader`, `1..N` =
/// parallel workers). Attach as receiver to this proc's own MPSC inbox, and (if
/// `attach_senders` is true) as sender to every peer's inbox.
///
/// `attach_senders = false` is for the leader (consumer-only). If the leader attached
/// as sender it would bump every peer inbox's `sender_count` and decrement it on `Drop`;
/// if it dropped before any worker bumped, the 1 → 0 transition would flip `detached`
/// on every peer inbox and every later worker send would fail `SendError::Detached`.
/// Skipping keeps `sender_count` honest (only producers ever increment).
///
/// # Safety
/// - `base` must point to a DSM region whose header has been validated.
/// - `header.inbox_offset(r)` must point at a ring already initialized by
///   `DsmMpscRing::create_at` (the leader does this in `leader_init`).
unsafe fn attach_proc(
    base: *mut u8,
    header: &MppDsmHeader,
    this_proc: u32,
    attach_senders: bool,
    wakeup: Arc<dyn Wakeup>,
) -> ProcAttach {
    let n_procs = header.n_procs;
    let peer_count = (n_procs - 1) as usize;
    let mut outbound_senders = Vec::with_capacity(if attach_senders { peer_count } else { 0 });

    let (ring_slots, slot_capacity) = ring_dims_for(header.queue_bytes as usize);

    // One liveness flag shared by every handle this attach mints.
    let alive = Arc::new(AtomicBool::new(true));

    if attach_senders {
        for peer_idx in 0..(n_procs - 1) {
            let r = peer_proc_for_index(this_proc, peer_idx);
            let off = header.inbox_offset(r) as usize;
            let inbox_addr = unsafe { base.add(off) };
            let nn = unsafe { mpsc_ring::attach_at(inbox_addr, ring_slots, slot_capacity) }
                .expect("DsmMpscRing attach_at: leader-initialized region must validate");
            outbound_senders
                .push(unsafe { DsmMpscSender::new(nn, Arc::clone(&wakeup), Arc::clone(&alive)) });
        }
    }

    // Inbound: this proc's own inbox. Single receiver per ring (MPSC contract).
    let own_off = header.inbox_offset(this_proc) as usize;
    let own_inbox_addr = unsafe { base.add(own_off) };
    let own_nn = unsafe { mpsc_ring::attach_at(own_inbox_addr, ring_slots, slot_capacity) }
        .expect("DsmMpscRing attach_at: own inbox must validate");
    let inbound_receiver = unsafe { DsmMpscReceiver::new(own_nn, Arc::clone(&alive)) };

    ProcAttach {
        outbound_senders,
        inbound_receiver,
        alive,
    }
}

/// Attach to the leader-initialized DSM region as `proc_idx` (1-based for
/// workers: PG's `ParallelWorkerNumber + 1`).
///
/// # Safety
/// - `coordinate` must be the DSM region pointer the leader initialized.
/// - `region_total` must match the DSM's attached size.
pub(super) unsafe fn worker_attach(
    coordinate: *mut c_void,
    region_total: u64,
    proc_idx: u32,
    wakeup: Arc<dyn Wakeup>,
) -> Result<(MppDsmHeader, Vec<u8>, ProcAttach), String> {
    if coordinate.is_null() {
        return Err("mpp: worker_attach given null coordinate".into());
    }
    let base = coordinate as *mut u8;
    let header = unsafe { std::ptr::read(base.cast::<MppDsmHeader>()) };
    header
        .validate(region_total)
        .map_err(|e| format!("mpp: worker DSM validate: {e}"))?;
    if proc_idx == 0 {
        return Err(
            "mpp: worker_attach must be called with proc_idx >= 1 (proc 0 is leader)".into(),
        );
    }
    if proc_idx >= header.n_procs {
        return Err(format!(
            "mpp: proc_idx {proc_idx} not in 1..{}",
            header.n_procs
        ));
    }

    // Copy plan bytes out of DSM so the caller has an owned buffer.
    let plan_bytes = unsafe {
        std::slice::from_raw_parts(
            base.add(header.plan_offset as usize),
            header.plan_len as usize,
        )
        .to_vec()
    };

    let attach = unsafe {
        attach_proc(
            base, &header, proc_idx, /* attach_senders */ true, wakeup,
        )
    };
    Ok((header, plan_bytes, attach))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compute_dsm_layout_works() {
        let l = compute_dsm_layout(4, 64 * 1024, 1024).unwrap();
        assert_eq!(l.n_procs, 4);
        // 4 procs => 4 inboxes laid out contiguously after the plan bytes.
        let aligned = aligned_queue_bytes(64 * 1024);
        let queues_size = 4 * aligned;
        assert_eq!(l.region_total, l.queues_offset + queues_size);
    }

    #[test]
    fn compute_dsm_layout_rejects_zero_procs() {
        assert!(compute_dsm_layout(0, 64 * 1024, 0).is_err());
    }

    #[test]
    fn compute_dsm_layout_rejects_oversize() {
        assert!(compute_dsm_layout(u32::MAX, 64 * 1024, 0).is_err());
    }

    #[test]
    fn compute_dsm_layout_scales_linearly_in_n_procs() {
        // Total queue area must grow as O(N) in proc count. Pinning the math here so a
        // regression that grows queues per-pair fails this test at compile time rather
        // than at the N=24 wall-time cliff in production.
        let queue_bytes = 64 * 1024;
        let aligned = aligned_queue_bytes(queue_bytes);
        for n in [2u32, 4, 8, 16, 24] {
            let l = compute_dsm_layout(n, queue_bytes, 0).unwrap();
            let expected = (n as usize) * aligned;
            assert_eq!(
                l.region_total - l.queues_offset,
                expected,
                "n={n}: expected {expected} inbox bytes ({n} inboxes × {aligned})"
            );
        }
    }

    #[test]
    fn header_inbox_offset_is_per_receiver() {
        // 4 procs => 4 inboxes, contiguous, sized by queue_bytes each.
        let l = compute_dsm_layout(4, 64 * 1024, 0).unwrap();
        let h = MppDsmHeader::from_layout(&l);
        let aligned = h.queue_bytes;
        assert_eq!(h.inbox_offset(0), h.queues_offset);
        assert_eq!(h.inbox_offset(1), h.queues_offset + aligned);
        assert_eq!(h.inbox_offset(2), h.queues_offset + 2 * aligned);
        assert_eq!(h.inbox_offset(3), h.queues_offset + 3 * aligned);
    }

    #[test]
    fn header_validate_accepts_self() {
        let l = compute_dsm_layout(2, 64 * 1024, 0).unwrap();
        let h = MppDsmHeader::from_layout(&l);
        assert!(h.validate(l.region_total as u64).is_ok());
    }

    #[test]
    fn header_validate_rejects_wrong_version() {
        let l = compute_dsm_layout(2, 64 * 1024, 0).unwrap();
        let mut h = MppDsmHeader::from_layout(&l);
        h.header_version = MPP_DSM_HEADER_VERSION.wrapping_sub(1);
        let err = h
            .validate(l.region_total as u64)
            .expect_err("wrong version must fail");
        assert!(err.contains("DSM header version mismatch"), "got: {err}");
    }

    #[test]
    fn header_validate_rejects_size_mismatch() {
        let l = compute_dsm_layout(2, 64 * 1024, 0).unwrap();
        let h = MppDsmHeader::from_layout(&l);
        assert!(h.validate(l.region_total as u64 + 1).is_err());
    }

    /// Pins the coupling between `compute_dsm_layout`'s minimum check and `ring_dims_for`'s
    /// slot-capacity floor: a queue sized below the floored ring must be rejected, or
    /// `create_at` would write past its inbox region.
    #[test]
    fn layout_rejects_queue_smaller_than_floored_ring() {
        let floored = DsmMpscRingHeader::region_bytes(RING_SLOTS, MIN_SLOT_CAPACITY);
        let too_small = DsmMpscRingHeader::region_bytes(RING_SLOTS, 1);
        assert!(too_small < floored);
        assert!(compute_dsm_layout(2, too_small, 0).is_err());
        assert!(compute_dsm_layout(2, floored.next_multiple_of(64), 0).is_ok());
    }
}
