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

use std::env;

use crate::logging::info;

mod proto;
pub use proto::generated::protobuf;

pub mod codec;
pub mod explain;
pub mod flight;
pub mod friendly;
pub mod isolator;
pub mod k8s;
pub mod logging;
pub mod max_rows;
pub mod physical;
pub mod planning;
pub mod processor_service;
pub mod proxy_service;
pub mod result;
pub mod stage;
pub mod stage_reader;
pub mod util;
pub mod vocab;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

pub fn setup() {
    setup_logging();

    // Setup memory logging if not on MSVC (Windows)
    setup_memory_logging();
}

fn setup_logging() {
    let dfr_env = env::var("DATAFUSION_RAY_LOG_LEVEL").unwrap_or("WARN".to_string());
    let rust_log_env = env::var("RUST_LOG").unwrap_or("WARN".to_string());

    let combined_env = format!("{rust_log_env},distributed_datafusion={dfr_env}");

    env_logger::Builder::new()
        .parse_filters(&combined_env)
        .init();
}

fn setup_memory_logging() {
    #[cfg(not(target_env = "msvc"))]
    {
        let mut mem_tracker = MemTracker::new();
        std::thread::spawn(move || {
            loop {
                let dt = 0.5;
                std::thread::sleep(std::time::Duration::from_millis((dt * 1000.0) as u64));
                mem_tracker.update(dt);
                if mem_tracker.rate_above_reporting_threshold() {
                    info!("{}", mem_tracker.status());
                }
            }
        });
    }
}

#[cfg(not(target_env = "msvc"))]
struct MemTracker {
    /// Resident memory in bytes
    pub resident: usize,
    /// Resident memory rate of change in bytes per second
    pub resident_rate: f64,
    /// Active memory in bytes
    pub active: usize,
    /// Active memory rate of change in bytes per second
    pub active_rate: f64,
    /// Allocated memory in bytes
    pub allocated: usize,
    /// Allocated memory rate of change in bytes per second
    pub allocated_rate: f64,
    /// Threshold for reporting memory usage in bytes
    pub threshold: usize,
    /// Threshold rate for reporting memory usage in bytes per second
    pub threshold_rate: f64,
}

#[cfg(not(target_env = "msvc"))]
impl MemTracker {
    pub fn new() -> Self {
        // Initialize the memory tracker from the environment or if not present, then
        // defaults
        let threshold = env::var("MEM_THRESHOLD")
            .unwrap_or_else(|_| "1_000_000_000".to_string())
            .parse::<usize>()
            .unwrap_or(1_000_000_000);
        let threshold_rate = env::var("MEM_THRESHOLD_RATE")
            .unwrap_or_else(|_| "100_000_000".to_string())
            .parse::<f64>()
            .unwrap_or(100_000_000.0);

        Self {
            resident: 0,
            resident_rate: 0.0,
            active: 0,
            active_rate: 0.0,
            allocated: 0,
            allocated_rate: 0.0,
            threshold,
            threshold_rate,
        }
    }

    /// Get the current memory usage and rate of change as a string.
    pub fn status(&self) -> String {
        let resident_str = format!("{} MiB", self.resident / 1024 / 1024);
        let resident_rate_str = format!("{:.2} MiB/s", self.resident_rate / 1024.0 / 1024.0);
        let active_str = format!("{} MiB", self.active / 1024 / 1024);
        let active_rate_str = format!("{:.2} MiB/s", self.active_rate / 1024.0 / 1024.0);
        let allocated_str = format!("{} MiB", self.allocated / 1024 / 1024);
        let allocated_rate_str = format!("{:.2} MiB/s", self.allocated_rate / 1024.0 / 1024.0);
        format!(
            "jemalloc resident: {}({}) active: {}({}) allocated: {}({})",
            resident_str,
            resident_rate_str,
            active_str,
            active_rate_str,
            allocated_str,
            allocated_rate_str
        )
    }

    pub fn rate_above_reporting_threshold(&self) -> bool {
        self.resident_rate.abs() > self.threshold_rate
            || self.active_rate.abs() > self.threshold_rate
            || self.allocated_rate.abs() > self.threshold_rate
            || self.resident > self.threshold
            || self.active > self.threshold
            || self.allocated > self.threshold
    }

    /// Update the memory tracker with the current memory usage.
    pub fn update(&mut self, dt: f64) {
        use tikv_jemalloc_ctl::{epoch, stats};
        let active = stats::active::mib().expect("can get active mib");
        let allocated = stats::allocated::mib().expect("can get allocated mib");
        let resident = stats::resident::mib().expect("can get resident mib");
        let e = epoch::mib().expect("can get epoch mib");

        let resident_bytes = resident.read().expect("can read resident mem");
        let active_bytes = active.read().expect("can read active mem");
        let allocated_bytes = allocated.read().expect("can read allocated mem");

        self.resident_rate = (resident_bytes as f64 - self.resident as f64) / dt;
        self.active_rate = (active_bytes as f64 - self.active as f64) / dt;
        self.allocated_rate = (allocated_bytes as f64 - self.allocated as f64) / dt;

        self.resident = resident_bytes;
        self.active = active_bytes;
        self.allocated = allocated_bytes;

        e.advance().expect("can advance epoch");
    }
}
