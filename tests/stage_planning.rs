mod common;
mod tpch;

#[cfg(test)]
mod tests {
    use crate::tpch::tpch_query;
    use crate::{assert_snapshot, tpch};
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion::physical_plan::{displayable, execute_stream};
    use datafusion::prelude::{SessionConfig, SessionContext};
    use datafusion_distributed::physical_optimizer::DistributedPhysicalOptimizerRule;
    use datafusion_distributed::stage::{display_stage_graphviz, ExecutionStage};
    use futures::TryStreamExt;
    use std::error::Error;
    use std::sync::Arc;

    // FIXME: ignored out until we figure out how to integrate best with tpch
    #[tokio::test]
    #[ignore]
    async fn stage_planning() -> Result<(), Box<dyn Error>> {
        let config = SessionConfig::new().with_target_partitions(3);

        let rule = DistributedPhysicalOptimizerRule::default().with_maximum_partitions_per_task(4);

        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(config)
            .with_physical_optimizer_rule(Arc::new(rule))
            .build();

        let ctx = SessionContext::new_with_state(state);

        for table_name in [
            "lineitem", "orders", "part", "partsupp", "customer", "nation", "region", "supplier",
        ] {
            let query_path = format!("testdata/tpch/{}.parquet", table_name);
            ctx.register_parquet(
                table_name,
                query_path,
                datafusion::prelude::ParquetReadOptions::default(),
            )
            .await?;
        }

        let sql = tpch_query(2);
        //let sql = "select 1;";
        println!("SQL Query:\n{}", sql);

        let df = ctx.sql(&sql).await?;

        let physical = df.create_physical_plan().await?;

        let physical_str = displayable(physical.as_ref()).tree_render();
        println!("\n\nPhysical Plan:\n{}", physical_str);

        let physical_str = displayable(physical.as_ref()).indent(false);
        println!("\n\nPhysical Plan:\n{}", physical_str);

        let physical_str = displayable(physical.as_ref()).indent(true);
        println!("\n\nPhysical Plan:\n{}", physical_str);

        let physical_str =
            display_stage_graphviz(physical.as_any().downcast_ref::<ExecutionStage>().unwrap())?;
        println!("\n\nPhysical Plan:\n{}", physical_str);

        assert_snapshot!(physical_str,
            @r"
        ",
        );

        /*let batches = pretty_format_batches(
            &execute_stream(physical, ctx.task_ctx())?
                .try_collect::<Vec<_>>()
                .await?,
        )?;

        assert_snapshot!(batches, @r"
        +----------+-----------+
        | count(*) | RainToday |
        +----------+-----------+
        | 66       | Yes       |
        | 300      | No        |
        +----------+-----------+
        ");*/

        Ok(())
    }
}
