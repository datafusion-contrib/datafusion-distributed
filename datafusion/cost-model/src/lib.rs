//! A CPU, memory and network cost model for DataFusion physical plans.
//!
//! Costs are measured in bytes processed, held in memory or moved across the network, and are
//! derived from each node's [Complexity] evaluated against the plan's [Statistics].
//!
//! The model knows about the nodes shipped by upstream DataFusion. Crates that bring their own
//! [ExecutionPlan] implementations describe them through a [CostModelExtension].
//!
//! [Statistics]: datafusion::common::Statistics
//! [ExecutionPlan]: datafusion::physical_plan::ExecutionPlan

mod complexity;
mod complexity_cpu;
mod complexity_memory;
mod cost;
mod default_bytes_for_datatype;
mod extension;
mod plan_statistics;
#[cfg(test)]
mod test_utils;

pub use complexity::{Complexity, LinearComplexity};
pub use cost::{CalculateCostOptions, Cost, calculate_cost};
pub use extension::CostModelExtension;
