#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::sync::Arc;

    use datafusion::common::stats::Precision;
    use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion::datasource::source::DataSourceExec;
    use datafusion::physical_plan::statistics::{StatisticsArgs, StatisticsContext};
    use datafusion_distributed_iceberg::test_utils::{FIXTURE_URI, IcebergTestHarness};
    use datafusion_distributed_iceberg::{IcebergConfig, IcebergExt};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, Datum, FormatVersion, ListType,
        NestedField, PartitionSpec, PrimitiveType, Schema, SortOrder, Struct, StructType,
        TableMetadataBuilder, Type,
    };

    #[tokio::test]
    async fn selected_nested_columns_sum_leaves_and_keep_missing_leaves_unknown()
    -> Result<(), Box<dyn Error>> {
        let harness = nested_fixture().await?;
        for (id, expected) in [(1, Precision::Inexact(300)), (2, Precision::Absent)] {
            let plan = harness
                .physical_plan(&format!("SELECT details FROM taxi WHERE id = {id}"))
                .await?;
            let mut checked = false;
            plan.apply(|node| {
                if node.is::<DataSourceExec>() {
                    let stats =
                        StatisticsContext::new().compute(node.as_ref(), &StatisticsArgs::new())?;
                    let column = node.schema().index_of("details")?;
                    assert_eq!(stats.column_statistics[column].byte_size, expected);
                    assert_eq!(
                        stats.column_statistics[column].null_count,
                        Precision::Absent
                    );
                    assert_eq!(stats.column_statistics[column].min_value, Precision::Absent);
                    assert_eq!(stats.column_statistics[column].max_value, Precision::Absent);
                    assert_eq!(stats.num_rows, Precision::Inexact(10));
                    checked = true;
                }
                Ok(TreeNodeRecursion::Continue)
            })?;
            assert!(checked);
        }
        Ok(())
    }

    async fn nested_fixture() -> Result<IcebergTestHarness, Box<dyn Error>> {
        let schema = Schema::builder()
            .with_fields([
                Arc::new(NestedField::required(
                    1,
                    "id",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "details",
                    Type::Struct(StructType::new(vec![
                        Arc::new(NestedField::optional(
                            3,
                            "name",
                            Type::Primitive(PrimitiveType::String),
                        )),
                        Arc::new(NestedField::optional(
                            4,
                            "values",
                            Type::List(ListType::new(Arc::new(NestedField::optional(
                                5,
                                "element",
                                Type::Primitive(PrimitiveType::Long),
                            )))),
                        )),
                    ])),
                )),
            ])
            .build()?;
        let spec = PartitionSpec::builder(Arc::new(schema.clone())).build()?;
        let metadata = TableMetadataBuilder::new(
            schema,
            spec.into_unbound(),
            SortOrder::unsorted_order(),
            FIXTURE_URI.to_owned(),
            FormatVersion::V2,
            Default::default(),
        )?
        .build()?
        .metadata;
        let mut files = Vec::new();
        for id in [1, 2] {
            let sizes = if id == 1 {
                [(1, 40), (3, 100), (5, 200)].into_iter().collect()
            } else {
                [(1, 40), (3, 900)].into_iter().collect()
            };
            files.push(
                DataFileBuilder::default()
                    .content(DataContentType::Data)
                    .file_format(DataFileFormat::Parquet)
                    .file_path(format!("{FIXTURE_URI}/data/unopened-nested-{id}.parquet"))
                    .partition(Struct::empty())
                    .record_count(10)
                    .file_size_in_bytes(1000)
                    .column_sizes(sizes)
                    .lower_bounds([(1, Datum::int(id))].into())
                    .upper_bounds([(1, Datum::int(id))].into())
                    .build()?,
            );
        }
        Ok(IcebergTestHarness::builder()
            .with_table_metadata(metadata)
            .with_data_files(&format!("{FIXTURE_URI}/metadata/nested.avro"), files)
            .await?
            .configure_session(|mut state| {
                let mut config = IcebergConfig::default();
                config.eager_file_planning = true;
                state
                    .config()
                    .get_or_insert_default()
                    .options_mut()
                    .extensions
                    .insert(config);
                Ok(state.with_iceberg_column_stats_enabled(true))
            })?
            .build()
            .await?)
    }
}
