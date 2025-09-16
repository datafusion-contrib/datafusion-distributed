use datafusion::error::Result;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{displayable, ExecutionPlan, ExecutionPlanProperties};

use std::fmt::Write;

pub fn display_plan_with_partition_in_out(plan: &dyn ExecutionPlan) -> Result<String> {
    let mut f = String::new();

    fn visit(plan: &dyn ExecutionPlan, indent: usize, f: &mut String) -> Result<()> {
        let output_partitions = plan.output_partitioning().partition_count();
        let input_partitions = plan
            .children()
            .first()
            .map(|child| child.output_partitioning().partition_count());

        write!(
            f,
            "partitions [out:{:<3}{}]{} {}",
            output_partitions,
            input_partitions
                .map(|p| format!("<-- in:{:<3}", p))
                .unwrap_or("          ".to_string()),
            " ".repeat(indent),
            displayable(plan).one_line()
        )?;

        plan.children()
            .iter()
            .try_for_each(|input| visit(input.as_ref(), indent + 2, f))?;

        Ok(())
    }

    visit(plan, 0, &mut f)?;
    Ok(f)
}

pub fn display_plan_with_partition_in_out_metrics(plan: &dyn ExecutionPlan) -> Result<String> {
    let mut f = String::new();

    fn visit(plan: &dyn ExecutionPlan, indent: usize, f: &mut String) -> Result<()> {
        let output_partitions = plan.output_partitioning().partition_count();
        let input_partitions = plan
            .children()
            .first()
            .map(|child| child.output_partitioning().partition_count());

        // DEBUG: Check what metrics this plan has
        // println!("🖥️  Display: visiting node '{}' at indent {}", plan.name(), indent);
        // match plan.metrics() {
        //     Some(metrics) => {
        //         println!("     Node has {} metrics", metrics.iter().count());
        //         for metric in metrics.iter() {
        //             println!("       - {}: {:?}", metric.value().name(), metric.value());
        //         }
        //     }
        //     None => {
        //         println!("     Node has NO metrics");
        //     }
        // }

        let display_line = DisplayableExecutionPlan::with_metrics(plan).one_line();
        // println!("     DisplayableExecutionPlan result: {}", display_line);

        write!(
            f,
            "partitions [out:{:<3}{}]{} {}",
            output_partitions,
            input_partitions
                .map(|p| format!("<-- in:{:<3}", p))
                .unwrap_or("          ".to_string()),
            " ".repeat(indent),
            display_line
        )?;

        plan.children()
            .iter()
            .try_for_each(|input| visit(input.as_ref(), indent + 2, f))?;

        Ok(())
    }

    visit(plan, 0, &mut f)?;
    Ok(f)
}
