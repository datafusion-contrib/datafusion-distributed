use crate::column_statistics::compute_column_stats;
use crate::common::df_err;
use crate::scan::ScanConfig;
use crate::{IcebergConfig, IcebergDataSource, IcebergWorkUnitFeed};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_distributed::WorkUnitFeed;
use iceberg::table::Table;
use iceberg_datafusion::read::convert_filters_to_predicate;
use std::sync::Arc;

/// Optional fields for building an [IcebergDataSource](crate::IcebergDataSource).
#[derive(Default, Clone)]
pub(crate) struct IcebergDataSourceOptions<'a> {
    pub(crate) snapshot_id: Option<i64>,
    pub(crate) projection: Option<&'a Vec<usize>>,
    pub(crate) fetch: Option<usize>,
    pub(crate) filters: &'a [Expr],
    pub(crate) iceberg_runtime: Option<iceberg::Runtime>,
}

pub(crate) async fn plan_scan(
    table: Table,
    schema: SchemaRef,
    opts: IcebergDataSourceOptions<'_>,
    context: Arc<TaskContext>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let output_schema = match opts.projection {
        None => schema.clone(),
        Some(projection) => Arc::new(schema.project(projection).unwrap()),
    };
    let projection = opts.projection.map(|v| {
        v.iter()
            .map(|p| schema.field(*p).name().clone())
            .collect::<Vec<String>>()
    });
    // Necessary for time-travel queries
    let table_snapshot = match opts.snapshot_id {
        Some(snapshot_id) => table.metadata().snapshot_by_id(snapshot_id),
        None => table.metadata().current_snapshot(),
    }
    .cloned();
    let predicates = convert_filters_to_predicate(opts.filters);
    let partitioning =
        Partitioning::UnknownPartitioning(context.session_config().target_partitions());
    let scan = ScanConfig {
        schema: output_schema,
        iceberg_file_io: table.file_io().clone(),
        partitioning: partitioning.clone(),
        fetch: opts.fetch,
        metrics: Default::default(),
        iceberg_runtime: opts
            .iceberg_runtime
            .unwrap_or_else(iceberg::Runtime::current),
    };
    let column_stats = if IcebergConfig::from_task_context(&context).column_stats_enabled {
        let schema = match &table_snapshot {
            Some(snap) => snap.schema(table.metadata()).map_err(df_err)?,
            // empty table
            None => table.metadata().current_schema().clone(),
        };
        let fields = schema.as_struct().fields().to_vec();
        let field_ids: Vec<i32> = match opts.projection {
            Some(projection) => projection.iter().map(|&idx| fields[idx].id).collect(),
            None => fields.iter().map(|f| f.id).collect(),
        };
        Some(compute_column_stats(table.clone(), field_ids, table_snapshot.clone()).await?)
    } else {
        None
    };
    let feed = WorkUnitFeed::new(IcebergWorkUnitFeed {
        iceberg_table: table,
        snapshot_id: opts.snapshot_id,
        projection,
        predicates,
        partitioning,
        sync_manager: Default::default(),
    });
    Ok(DataSourceExec::from_data_source(IcebergDataSource {
        scan,
        feed,
        table_snapshot,
        column_stats,
    }))
}
