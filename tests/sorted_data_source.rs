#[cfg(all(feature = "integration", test))]
mod tests {
    use datafusion::arrow::util::pretty::pretty_format_batches;
    use datafusion::common::Result;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{CsvReadOptions, SessionContext, col};
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_in_memory_context;
    use datafusion_distributed::test_utils::insta::insta::allow_duplicates;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, assert_snapshot,
    };
    use std::fs;
    use std::path::PathBuf;
    use test_case::test_case;
    use uuid::Uuid;

    const MULTILINE_CSV: &str = r#"value
"0first
0middle
0last"
"1first
1middle
1last"
"2first
2middle
2last"
"3first
3middle
3last"
"4first
4middle
4last"
"5first
5middle
5last"
"6first
6middle
6last"
"#;

    // Each file is sorted, but their value ranges run opposite to file-name order.
    const SORTED_CSV: &[&str] = &[
        "value
8
8
9
9
10
10
11
11
12
12
13
13
14
14
15
15
",
        "value
0
0
1
1
2
2
3
3
4
4
5
5
6
6
7
7
",
    ];

    /// Byte boundaries inside quoted values must not invent extra CSV records.
    #[test_case(false; "static_execution")]
    #[test_case(true; "adaptive")]
    #[tokio::test]
    async fn sorted_csv_with_embedded_newlines(adaptive: bool) -> Result<()> {
        let result = query_csv(
            "SELECT count(*) AS n FROM records",
            &[MULTILINE_CSV],
            true,
            adaptive,
        )
        .await?;
        allow_duplicates! {
            assert_snapshot!(result, @"
            +---+
            | n |
            +---+
            | 7 |
            +---+
            ");
        }
        Ok(())
    }

    /// A sorted shuffle must keep its producer's ordering when task counts scale up.
    #[test_case(false; "static_execution")]
    #[test_case(true; "adaptive")]
    #[tokio::test]
    async fn sorted_csv_aggregation(adaptive: bool) -> Result<()> {
        let result = query_csv(
            "SELECT value, count(*) AS n FROM records GROUP BY value ORDER BY value",
            SORTED_CSV,
            false,
            adaptive,
        )
        .await?;
        allow_duplicates! {
            assert_snapshot!(result, @"
            +-------+---+
            | value | n |
            +-------+---+
            | 0     | 2 |
            | 1     | 2 |
            | 2     | 2 |
            | 3     | 2 |
            | 4     | 2 |
            | 5     | 2 |
            | 6     | 2 |
            | 7     | 2 |
            | 8     | 2 |
            | 9     | 2 |
            | 10    | 2 |
            | 11    | 2 |
            | 12    | 2 |
            | 13    | 2 |
            | 14    | 2 |
            | 15    | 2 |
            +-------+---+
            ");
        }
        Ok(())
    }

    async fn query_csv(
        sql: &str,
        bodies: &[&str],
        newlines_in_values: bool,
        adaptive: bool,
    ) -> Result<String> {
        let (ctx, dir) = csv_context(bodies, newlines_in_values, adaptive).await?;
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        assert!(plan.is::<DistributedExec>());
        let batches = collect(plan, ctx.task_ctx()).await?;
        fs::remove_dir_all(dir)?;
        Ok(pretty_format_batches(&batches)?.to_string())
    }

    async fn csv_context(
        bodies: &[&str],
        newlines_in_values: bool,
        adaptive: bool,
    ) -> Result<(SessionContext, PathBuf)> {
        let mut ctx = start_in_memory_context(2, DefaultSessionBuilder)
            .await
            .with_distributed_file_scan_config_bytes_per_partition(1)?;
        ctx.set_distributed_dynamic_task_count(adaptive)?;
        ctx.sql("SET datafusion.execution.target_partitions = 2")
            .await?;
        // Exercise merging across batches, while letting consumers trust scan ordering.
        ctx.sql("SET datafusion.execution.batch_size = 2").await?;
        ctx.sql("SET datafusion.optimizer.prefer_existing_sort = true")
            .await?;

        let dir = std::env::temp_dir().join(format!("sorted_csv_{}", Uuid::new_v4()));
        fs::create_dir_all(&dir)?;
        for (i, body) in bodies.iter().enumerate() {
            fs::write(dir.join(format!("{i}.csv")), body)?;
        }
        ctx.register_csv(
            "records",
            dir.to_str().unwrap(),
            CsvReadOptions::new()
                .has_header(true)
                .newlines_in_values(newlines_in_values)
                .file_sort_order(vec![vec![col("value").sort(true, false)]]),
        )
        .await?;
        Ok((ctx, dir))
    }
}
