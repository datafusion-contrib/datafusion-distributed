#[cfg(all(feature = "integration", test))]
mod tests {
    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
        util::pretty::pretty_format_batches,
    };
    use datafusion::{
        common::{ScalarValue, SplitPoint},
        datasource::{
            file_format::parquet::ParquetFormat,
            listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl},
        },
        error::Result,
        logical_expr::{Partitioning as LogicalPartitioning, RangePartitioning, col},
        physical_plan::collect,
        prelude::SessionContext,
    };
    use datafusion_distributed::{
        DefaultSessionBuilder, assert_snapshot, display_plan_ascii,
        test_utils::localhost::start_localhost_context,
    };
    use parquet::arrow::ArrowWriter;
    use std::fs::{self, File};
    use std::path::PathBuf;
    use std::sync::Arc;
    use uuid::Uuid;

    /// 8 range partitions with `target_partitions=2`. DataFusion must keep the
    /// declared range partition count rather than collapsing to 2 file groups.
    const RANGE_PARTITIONS: usize = 8;
    const TARGET_PARTITIONS: usize = 2;

    #[tokio::test]
    async fn test_range_scan_keeps_declared_partition_count()
    -> Result<(), Box<dyn std::error::Error>> {
        let table_dir = write_range_table(RANGE_PARTITIONS, 1)?;
        struct Cleanup(PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        let _cleanup = Cleanup(table_dir.clone());
        let (mut ctx, _guard, _) = start_localhost_context(2, DefaultSessionBuilder).await;
        set_target_partitions(&mut ctx, TARGET_PARTITIONS);
        register_range_table(&ctx, "t", &table_dir, RANGE_PARTITIONS)?;

        let (plan, results) = execute_query(
            &ctx,
            "SELECT range_key, SUM(value) AS total FROM t GROUP BY range_key ORDER BY range_key",
        )
        .await?;

        assert_snapshot!(&plan, @r#"
        ┌───── DistributedExec
        │ SortPreservingMergeExec: [range_key@0 ASC NULLS LAST]
        │   [Stage 1] => NetworkCoalesceExec: output_partitions=16, input_tasks=2
        └──────────────────────────────────────────────────
          ┌───── Stage 1 ── tasks=2, partitions=8
          │ ProjectionExec: expr=[range_key@0 as range_key, sum(t.value)@1 as total]
          │   SortExec: expr=[range_key@0 ASC NULLS LAST], preserve_partitioning=[true]
          │     AggregateExec: mode=SinglePartitioned, gby=[range_key@0 as range_key], aggr=[sum(t.value)]
          │       DistributedLeafExec:
          │         t0: DataSourceExec: file_groups={8 groups: [[/dfd-range-UUID/part-0.parquet], [], [/dfd-range-UUID/part-2.parquet], [], [/dfd-range-UUID/part-4.parquet], ...]}, projection=[range_key, value], output_partitioning=Range([range_key@0 ASC], [(10), (20), (30), (40), (50), (60), (70)], 8), file_type=parquet
          │         t1: DataSourceExec: file_groups={8 groups: [[], [/dfd-range-UUID/part-1.parquet], [], [/dfd-range-UUID/part-3.parquet], [], ...]}, projection=[range_key, value], output_partitioning=Range([range_key@0 ASC], [(10), (20), (30), (40), (50), (60), (70)], 8), file_type=parquet
          └──────────────────────────────────────────────────
        "#);

        let pretty_results = pretty_format_batches(&results)?;
        assert_snapshot!(pretty_results, @"
        +-----------+-------+
        | range_key | total |
        +-----------+-------+
        | 0         | 0     |
        | 10        | 10    |
        | 20        | 20    |
        | 30        | 30    |
        | 40        | 40    |
        | 50        | 50    |
        | 60        | 60    |
        | 70        | 70    |
        +-----------+-------+
        ");

        Ok(())
    }

    fn set_target_partitions(ctx: &mut SessionContext, target_partitions: usize) {
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = target_partitions;
    }

    fn range_partitioning(range_partitions: usize) -> Result<LogicalPartitioning> {
        let split_points = (1..range_partitions)
            .map(|i| SplitPoint::new(vec![ScalarValue::Int32(Some(i as i32 * 10))]))
            .collect();
        Ok(LogicalPartitioning::Range(RangePartitioning::try_new(
            vec![col("range_key").sort(true, true)],
            split_points,
        )?))
    }

    fn register_range_table(
        ctx: &SessionContext,
        name: &str,
        table_dir: &PathBuf,
        range_partitions: usize,
    ) -> Result<()> {
        let table_path = ListingTableUrl::parse(format!(
            "{}/",
            table_dir.to_str().expect("table path should be utf8")
        ))?;
        let options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_output_partitioning(Some(range_partitioning(range_partitions)?));
        let schema = range_schema();
        let table = ListingTable::try_new(
            ListingTableConfig::new(table_path)
                .with_listing_options(options)
                .with_schema(schema),
        )?;
        ctx.register_table(name, Arc::new(table))?;
        Ok(())
    }

    async fn execute_query(
        ctx: &SessionContext,
        query: &str,
    ) -> Result<(String, Vec<RecordBatch>)> {
        let df = ctx.sql(query).await?;
        let (state, logical_plan) = df.into_parts();
        let physical_plan = state.create_physical_plan(&logical_plan).await?;
        let distributed_plan = display_plan_ascii(physical_plan.as_ref(), false);
        let results = collect(physical_plan, state.task_ctx()).await?;
        Ok((distributed_plan, results))
    }

    fn range_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("range_key", DataType::Int32, false),
            Field::new("value", DataType::Int32, false),
        ]))
    }

    fn write_range_table(range_partitions: usize, files_per_partition: usize) -> Result<PathBuf> {
        let table_dir =
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(format!("dfd-range-{}", Uuid::new_v4()));
        fs::create_dir_all(&table_dir)?;
        let schema = range_schema();
        let mut file_idx = 0;
        for part in 0..range_partitions {
            let key = part as i32 * 10;
            for _ in 0..files_per_partition {
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(Int32Array::from(vec![key])),
                        Arc::new(Int32Array::from(vec![key])),
                    ],
                )?;
                let path = table_dir.join(format!("part-{file_idx}.parquet"));
                let file = File::create(&path)?;
                let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None)?;
                writer.write(&batch)?;
                writer.close()?;
                file_idx += 1;
            }
        }
        Ok(table_dir)
    }
}
