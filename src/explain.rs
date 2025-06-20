use std::sync::Arc;

use anyhow::Context;
use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    logical_expr::LogicalPlan,
    physical_plan::{displayable, ExecutionPlan},
    prelude::SessionContext,
};
use datafusion_proto::physical_plan::PhysicalExtensionCodec;

use crate::{result::Result, util::bytes_to_physical_plan, vocab::DDTask};

pub fn format_distributed_tasks(
    tasks: &[DDTask],
    codec: &dyn PhysicalExtensionCodec,
) -> Result<String> {
    let mut result = String::new();
    for (i, task) in tasks.iter().enumerate() {
        let plan = bytes_to_physical_plan(&SessionContext::new(), &task.plan_bytes, codec)
            .context("unable to decode task plan for formatted output")?;

        result.push_str(&format!(
            "Task: Stage {}, Partitions {:?}\n",
            task.stage_id, task.partition_group
        ));
        result.push_str(&format!("  Full Partitions: {}\n", task.full_partitions));
        result.push_str("  Plan:\n");
        let plan_display = format!("{}", displayable(plan.as_ref()).indent(true));
        for line in plan_display.lines() {
            result.push_str(&format!("    {}\n", line));
        }
        if i < tasks.len() - 1 {
            result.push('\n');
        }
    }
    if result.is_empty() {
        result.push_str("No distributed tasks generated");
    }
    Ok(result)
}

/// Builds a single RecordBatch with two columns, the first column with the type of plan and
/// the second column containing the formatted logical plan, physical plan, distributed plan,
/// and distributed tasks.
pub fn build_explain_batch(
    logical_plan: &LogicalPlan,
    physical_plan: &Arc<dyn ExecutionPlan>,
    distributed_plan: &Arc<dyn ExecutionPlan>,
    distributed_tasks: &[DDTask],
    codec: &dyn PhysicalExtensionCodec,
) -> Result<RecordBatch> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("plan_type", DataType::Utf8, false),
        Field::new("plan", DataType::Utf8, false),
    ]));

    // Create the result data with our 4 plan types
    let plan_types = StringArray::from(vec![
        "logical_plan",
        "physical_plan",
        "distributed_plan",
        "distributed_tasks",
    ]);
    let plans = StringArray::from(vec![
        logical_plan.display_indent().to_string(),
        displayable(physical_plan.as_ref()).indent(true).to_string(),
        displayable(distributed_plan.as_ref())
            .indent(true)
            .to_string(),
        format_distributed_tasks(distributed_tasks, codec)?,
    ]);

    let batch = RecordBatch::try_new(schema, vec![Arc::new(plan_types), Arc::new(plans)])?;

    Ok(batch)
}
