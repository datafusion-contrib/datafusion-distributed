use crate::common::assign_files;
use crate::common::df_err;
use crate::common::scan_config::{ScanConfig, apply_column_statistics};
use crate::eager::planned_files::PlannedFiles;
use crate::table_provider::column_statistics::{compute_column_stats, selected_column_statistics};
use crate::{EagerIcebergDataSource, IcebergConfig, IcebergWorkUnitFeed, LazyIcebergDataSource};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{Result, config_err, exec_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::Expr;
use datafusion_distributed::WorkUnitFeed;
use iceberg::table::Table;
use iceberg_datafusion::read::convert_filters_to_predicate;
use std::sync::Arc;
use tokio::sync::oneshot;

/// Optional fields for planning an eager or lazy Iceberg scan.
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
        Some(projection) => Arc::new(schema.project(projection)?),
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
    let planned = if IcebergConfig::from_task_context(&context).eager_file_planning {
        if partitioning.partition_count() == 0 {
            return config_err!("Iceberg scan requires at least one output partition");
        }
        let mut builder = table.scan();
        if let Some(snapshot_id) = opts.snapshot_id {
            builder = builder.snapshot_id(snapshot_id);
        }
        if let Some(columns) = &projection {
            builder = builder.select(columns);
        }
        if let Some(predicates) = &predicates {
            builder = builder.with_filter(predicates.clone());
        }
        let table_scan = builder.build().map_err(df_err)?;
        // File discovery runs on Iceberg's I/O runtime and is canceled if planning is dropped.
        let (_cancel_on_drop, canceled) = oneshot::channel::<()>();
        let planning_context = Arc::clone(&context);
        let task = scan.iceberg_runtime.io().spawn(async move {
            tokio::select! {
                _ = canceled => exec_err!("Iceberg scan planning canceled"),
                planned = PlannedFiles::collect(table_scan, &planning_context) => planned,
            }
        });
        Some(Arc::new(task.await.map_err(df_err)??))
    } else {
        None
    };
    let may_filter_rows = predicates.is_some()
        || planned
            .as_ref()
            .is_some_and(|files| files.tasks().iter().any(|task| !task.deletes().is_empty()));
    let column_stats = if IcebergConfig::from_task_context(&context).column_stats_enabled {
        let schema = match &table_snapshot {
            Some(snap) => snap.schema(table.metadata()).map_err(df_err)?,
            // empty table
            None => table.metadata().current_schema().clone(),
        };
        let fields = schema.as_struct().fields();
        let fields: Vec<_> = match opts.projection {
            Some(projection) => projection
                .iter()
                .map(|&idx| Arc::clone(&fields[idx]))
                .collect(),
            None => fields.to_vec(),
        };
        let stats_table = table.clone();
        let stats_snapshot = table_snapshot.clone();
        let stats_files = planned.clone();
        let (_cancel_on_drop, canceled) = oneshot::channel::<()>();
        let task = scan.iceberg_runtime.io().spawn(async move {
            tokio::select! {
                _ = canceled => exec_err!("Iceberg column statistics canceled"),
                stats = async {
                    if let Some(files) = stats_files
                        && let Some(snapshot) = &stats_snapshot
                    {
                        return selected_column_statistics(
                            &stats_table, snapshot, &fields, files.tasks(), may_filter_rows,
                        ).await;
                    }
                    let field_ids = fields.iter().map(|field| field.id).collect();
                    compute_column_stats(stats_table, field_ids, stats_snapshot).await
                } => stats,
            }
        });
        Some(task.await.map_err(df_err)??)
    } else {
        None
    };
    if let Some(planned) = planned {
        let mut statistics = planned.statistics(&scan.schema, may_filter_rows);
        if let Some(column_stats) = column_stats {
            apply_column_statistics(&mut statistics, column_stats);
        }
        let partitions = assign_files(planned.tasks().iter(), 1, partitioning.partition_count())?
            .pop()
            .expect("one task was assigned");
        return Ok(DataSourceExec::from_data_source(EagerIcebergDataSource {
            scan,
            statistics: Arc::new(statistics),
            files: planned,
            partitions: partitions.into(),
        }));
    }
    let feed = WorkUnitFeed::new(IcebergWorkUnitFeed {
        iceberg_table: table,
        snapshot_id: opts.snapshot_id,
        projection,
        predicates,
        partitioning,
        sync_manager: Default::default(),
    });
    Ok(DataSourceExec::from_data_source(LazyIcebergDataSource {
        scan,
        feed,
        table_snapshot,
        column_stats,
    }))
}
