#[cfg(test)]
mod tests {
    use arrow::array::Int64Array;
    use arrow::record_batch::RecordBatch;
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
    use datafusion::common::{
        HashMap, HashSet, Result, Statistics, internal_datafusion_err, internal_err,
    };
    use datafusion::config::ConfigOptions;
    use datafusion::datasource::physical_plan::FileScanConfig;
    use datafusion::datasource::source::{DataSource, DataSourceExec, OpenArgs};
    use datafusion::execution::{SendableRecordBatchStream, TaskContext};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, DynamicFilterPhysicalExpr, lit,
    };
    use datafusion::physical_expr::projection::ProjectionExprs;
    use datafusion::physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
    use datafusion::physical_plan::execution_plan::SchedulingType;
    use datafusion::physical_plan::filter_pushdown::FilterPushdownPropagation;
    use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
    use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricValue, MetricsSet};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan, collect};
    use datafusion::prelude::{ParquetReadOptions, SessionContext};
    use datafusion_distributed::test_utils::in_memory_channel_resolver::start_configured_in_memory_context;
    use datafusion_distributed::{
        DefaultSessionBuilder, DistributedExec, DistributedExt, DistributedMetricsFormat,
        DistributedTaskContext, MappedWorkerSessionBuilderExt, TaskFinishedEvent,
        TaskFinishedEventResponse, TaskFinishedHandler, WorkerPlanRewriteEvent,
        WorkerPlanRewriteEventResponse, rewrite_distributed_plan_with_metrics,
    };
    use delegate::delegate;
    use futures::{TryStreamExt, stream};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::{Compression, Encoding};
    use parquet::file::properties::WriterProperties;
    use std::any::Any;
    use std::fmt::{Formatter, Result as FmtResult};
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::time::timeout;

    const TOPK: &str = "SELECT rank_key AS key, secondary AS tie, row_id FROM topk_data
        ORDER BY key DESC NULLS LAST, tie ASC NULLS FIRST, row_id DESC LIMIT 10";

    #[tokio::test]
    async fn sibling_bound_prunes_parquet_before_the_receiving_task_has_a_local_bound() -> Result<()>
    {
        let observations = ScanObservations::default();
        let workers = DefaultSessionBuilder.map({
            let observations = observations.clone();
            move |builder| {
                Ok(builder
                    .with_distributed_worker_plan_rewrite_handler(gate_second_task_scan)
                    .with_distributed_task_finished_handler(observations.recorder())
                    .build())
            }
        });
        let ctx = start_configured_in_memory_context(2, workers, |worker| worker).await;
        ctx.sql("SET distributed.enable_sibling_topk_filter_pushdown = true")
            .await?;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 2;
        // Two equal files have high keys in their first row group and low keys afterwards.
        // Ordinary allocation gives the tasks complementary byte ranges, possibly with
        // an extra fragment at a file boundary; the executed assignments are checked below.
        let fixture = TopKFixture::register(&ctx, 512).await?;

        let plan = ctx.sql(TOPK).await?.create_physical_plan().await?;
        assert!(plan.is::<DistributedExec>());
        let actual = timeout(Duration::from_secs(30), collect(plan, ctx.task_ctx()))
            .await
            .map_err(|error| {
                internal_datafusion_err!("gated Top-K query did not finish: {error}")
            })??;
        let expected = fixture.local_query(TOPK).await?;
        assert_eq!(pretty_format_batches(&actual)?.to_string(), expected);

        let scans = observations.wait_for(2).await?;
        assert_eq!(scans.len(), 2);
        let low = scans.iter().find(|scan| scan.task == 1).unwrap();
        let high = scans.iter().find(|scan| scan.task == 0).unwrap();
        let paths = high
            .files
            .iter()
            .map(|file| &file.path)
            .collect::<HashSet<_>>();
        assert_eq!(paths.len(), 2);
        assert_eq!(
            paths,
            low.files
                .iter()
                .map(|file| &file.path)
                .collect::<HashSet<_>>()
        );
        let file_size = std::fs::metadata(fixture.0.join("left.parquet"))?.len();
        for path in paths {
            let mut ranges = scans
                .iter()
                .flat_map(|scan| &scan.files)
                .filter(|file| &file.path == path)
                .map(|file| file.range.unwrap())
                .collect::<Vec<_>>();
            ranges.sort_unstable();
            let mut end = 0;
            for (start, next) in ranges {
                assert_eq!(start, end, "no missing or duplicate file bytes: {scans:?}");
                assert!(start < next);
                end = next;
            }
            assert_eq!(end, i64::try_from(file_size).unwrap());
        }
        assert_eq!(low.local.len(), 1);
        assert!(
            low.local[0].initial,
            "the receiving task never earned a local cutoff"
        );
        assert_eq!(low.remote.len(), 1);
        assert!(
            !low.remote[0].initial,
            "a distinct remote bound reached the scan"
        );
        assert_ne!(low.local[0].id, low.remote[0].id);
        assert_eq!(low.output_rows, 0);
        assert!(
            low.pruned_row_groups > 0,
            "real Parquet pruning consumed the remote bound"
        );
        assert!(high.output_rows >= 10);
        assert!(high.local.iter().any(|filter| !filter.initial));
        Ok(())
    }

    #[tokio::test]
    async fn sharing_is_disabled_by_default() -> Result<()> {
        let observations = ScanObservations::default();
        let workers = DefaultSessionBuilder.map({
            let observations = observations.clone();
            move |builder| {
                Ok(builder
                    .with_distributed_task_finished_handler(observations.recorder())
                    .build())
            }
        });
        let ctx = start_configured_in_memory_context(2, workers, |worker| worker).await;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 2;
        let fixture = TopKFixture::register(&ctx, 128).await?;
        let actual = ctx.sql(TOPK).await?.collect().await?;
        assert_eq!(
            pretty_format_batches(&actual)?.to_string(),
            fixture.local_query(TOPK).await?
        );
        let scans = observations.wait_for(2).await?;
        assert!(
            scans
                .iter()
                .all(|scan| scan.remote.is_empty() && scan.local.len() == 1)
        );
        Ok(())
    }

    #[tokio::test]
    async fn enabling_topk_sharing_preserves_partitioned_join_results() -> Result<()> {
        let executed_joins = Arc::new(AtomicUsize::new(0));
        let workers = DefaultSessionBuilder.map({
            let executed_joins = Arc::clone(&executed_joins);
            move |builder| {
                let executed_joins = Arc::clone(&executed_joins);
                Ok(builder
                    .with_distributed_task_finished_handler(move |event: TaskFinishedEvent<'_>| {
                        event.plan.apply(|node| {
                            if node.downcast_ref::<HashJoinExec>().is_some_and(|join| {
                                matches!(join.partition_mode(), PartitionMode::Partitioned)
                            }) {
                                executed_joins.fetch_add(1, Ordering::Relaxed);
                            }
                            Ok(TreeNodeRecursion::Continue)
                        })?;
                        Ok(TaskFinishedEventResponse::new(MetricsSet::new()))
                    })
                    .build())
            }
        });
        let ctx = start_configured_in_memory_context(2, workers, |worker| worker)
            .await
            .with_distributed_broadcast_joins(false)?;
        ctx.sql("SET distributed.enable_sibling_topk_filter_pushdown = true")
            .await?;
        {
            let state = ctx.state_ref();
            let mut state = state.write();
            let options = state.config_mut().options_mut();
            options.execution.target_partitions = 2;
            options.optimizer.hash_join_single_partition_threshold = 0;
            options.optimizer.hash_join_single_partition_threshold_rows = 0;
        }
        let fixture = TopKFixture::register(&ctx, 128).await?;
        let sql = "SELECT a.rank_key, count(*) AS matches FROM topk_data a
            JOIN topk_data b ON a.rank_key = b.rank_key GROUP BY a.rank_key ORDER BY a.rank_key";
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        assert!(plan.is::<DistributedExec>());
        let actual = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        assert_eq!(
            pretty_format_batches(&actual)?.to_string(),
            fixture.local_query(sql).await?
        );
        rewrite_distributed_plan_with_metrics(Arc::clone(&plan), DistributedMetricsFormat::PerTask)
            .await?;
        assert!(executed_joins.load(Ordering::Relaxed) >= 2);
        let updates = plan
            .metrics()
            .unwrap()
            .sum(|metric| metric.value().name() == "dynamic_filter_updates_received")
            .unwrap()
            .as_usize();
        assert!(
            updates > 0,
            "the partitioned join still publishes its existing remote filters"
        );
        Ok(())
    }

    #[tokio::test]
    async fn tasks_with_fewer_than_k_rows_do_not_establish_a_pruning_bound() -> Result<()> {
        let observations = ScanObservations::default();
        let workers = DefaultSessionBuilder.map({
            let observations = observations.clone();
            move |builder| {
                Ok(builder
                    .with_distributed_task_finished_handler(observations.recorder())
                    .build())
            }
        });
        let ctx = start_configured_in_memory_context(2, workers, |worker| worker).await;
        ctx.sql("SET distributed.enable_sibling_topk_filter_pushdown = true")
            .await?;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 2;
        let fixture = TopKFixture::register(&ctx, 3).await?;
        let actual = ctx.sql(TOPK).await?.collect().await?;
        assert_eq!(actual.iter().map(RecordBatch::num_rows).sum::<usize>(), 6);
        assert_eq!(
            pretty_format_batches(&actual)?.to_string(),
            fixture.local_query(TOPK).await?
        );
        let scans = observations.wait_for(2).await?;
        assert!(
            scans
                .iter()
                .all(|scan| scan.local.len() == 1 && scan.remote.len() == 1)
        );
        assert!(
            scans
                .iter()
                .flat_map(|scan| scan.local.iter().chain(&scan.remote))
                .all(|filter| filter.initial)
        );
        assert!(scans.iter().all(|scan| scan.output_rows < 10));
        assert_eq!(scans.iter().map(|scan| scan.output_rows).sum::<usize>(), 6);
        assert!(scans.iter().all(|scan| scan.pruned_row_groups == 0));
        Ok(())
    }

    #[tokio::test]
    async fn an_outer_topk_bound_does_not_cross_an_inner_fetch() -> Result<()> {
        let observations = ScanObservations::default();
        let workers = DefaultSessionBuilder.map({
            let observations = observations.clone();
            move |builder| {
                Ok(builder
                    .with_distributed_task_finished_handler(observations.recorder())
                    .build())
            }
        });
        let ctx = start_configured_in_memory_context(2, workers, |worker| worker).await;
        ctx.sql("SET distributed.enable_sibling_topk_filter_pushdown = true")
            .await?;
        ctx.state_ref()
            .write()
            .config_mut()
            .options_mut()
            .execution
            .target_partitions = 2;
        let fixture = TopKFixture::register(&ctx, 128).await?;
        let sql = "SELECT * FROM (SELECT * FROM topk_data ORDER BY row_id ASC LIMIT 20)
            ORDER BY rank_key DESC NULLS LAST, secondary ASC NULLS FIRST, row_id DESC LIMIT 5";
        let actual = ctx.sql(sql).await?.collect().await?;
        assert_eq!(
            pretty_format_batches(&actual)?.to_string(),
            fixture.local_query(sql).await?
        );
        let scans = observations.wait_for(2).await?;
        assert!(scans.iter().all(|scan| scan.remote.len() == 1));
        assert!(
            scans
                .iter()
                .flat_map(|scan| &scan.remote)
                .all(|filter| filter.children == ["row_id@3"])
        );
        Ok(())
    }

    struct TopKFixture(PathBuf);

    impl TopKFixture {
        async fn register(ctx: &SessionContext, rows_per_file: i64) -> Result<Self> {
            let directory =
                std::env::temp_dir().join(format!("sibling_topk_{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&directory)?;
            let fixture = Self(directory);
            for (name, offset) in [("left", 100_i64), ("right", 0_i64)] {
                let key = Int64Array::from_iter((0..rows_per_file).map(|row| {
                    (row % 17 != 0).then_some(if row < 64 { 100 + row % 4 } else { row % 4 })
                }));
                let tie = Int64Array::from_iter(
                    (0..rows_per_file).map(|row| (row % 3 != 0).then_some(row % 5)),
                );
                let id = Int64Array::from_iter_values(
                    (0..rows_per_file).map(|row| offset * 1_000 + row),
                );
                let batch = RecordBatch::try_from_iter(vec![
                    // Top-K omits this leading column, so source and table indices differ.
                    (
                        "unused",
                        Arc::new(Int64Array::from_iter_values(0..rows_per_file)) as _,
                    ),
                    ("rank_key", Arc::new(key) as _),
                    ("secondary", Arc::new(tie) as _),
                    ("row_id", Arc::new(id) as _),
                ])?;
                // Equal-sized files split into complementary byte ranges under normal task allocation.
                let properties = WriterProperties::builder()
                    .set_compression(Compression::UNCOMPRESSED)
                    .set_dictionary_enabled(false)
                    .set_encoding(Encoding::PLAIN)
                    .set_max_row_group_row_count(Some(64))
                    .build();
                let file = std::fs::File::create(fixture.0.join(format!("{name}.parquet")))?;
                let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(properties))?;
                writer.write(&batch)?;
                writer.close()?;
            }
            assert_eq!(
                std::fs::metadata(fixture.0.join("left.parquet"))?.len(),
                std::fs::metadata(fixture.0.join("right.parquet"))?.len()
            );
            ctx.register_parquet(
                "topk_data",
                fixture.0.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
            Ok(fixture)
        }

        async fn local_query(&self, sql: &str) -> Result<String> {
            let ctx = SessionContext::new();
            ctx.register_parquet(
                "topk_data",
                self.0.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
            Ok(pretty_format_batches(&ctx.sql(sql).await?.collect().await?)?.to_string())
        }
    }

    impl Drop for TopKFixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Debug)]
    struct FilterObservation {
        id: u64,
        initial: bool,
        children: Vec<String>,
    }

    #[derive(Clone, Debug)]
    struct ScanObservation {
        task: usize,
        files: Vec<ScanFile>,
        local: Vec<FilterObservation>,
        remote: Vec<FilterObservation>,
        output_rows: usize,
        pruned_row_groups: usize,
    }

    #[derive(Clone, Default)]
    struct ScanObservations {
        scans: Arc<Mutex<Vec<ScanObservation>>>,
        changed: Arc<Notify>,
    }

    impl ScanObservations {
        fn recorder(&self) -> impl TaskFinishedHandler {
            let observations = self.clone();
            move |event: TaskFinishedEvent<'_>| {
                let producer_ids = producer_ids(event.plan)?;
                event.plan.apply(|node| {
                    let Some(files) = scan_files(node) else {
                        return Ok(TreeNodeRecursion::Continue);
                    };
                    let mut local = vec![];
                    let mut remote = vec![];
                    let source = node.downcast_ref::<DataSourceExec>().unwrap();
                    for (id, expression) in scan_filters(source.data_source().as_ref())? {
                        let filter = expression
                            .downcast_ref::<DynamicFilterPhysicalExpr>()
                            .unwrap();
                        let observed = FilterObservation {
                            id,
                            initial: is_initial_bound(&filter.current()?),
                            children: expression
                                .children()
                                .iter()
                                .map(ToString::to_string)
                                .collect(),
                        };
                        if producer_ids.contains(&id) {
                            local.push(observed);
                        } else {
                            remote.push(observed);
                        }
                    }
                    let metrics = node.metrics().unwrap_or_default();
                    let pruned_row_groups = metrics
                        .iter()
                        .filter_map(|metric| match metric.value() {
                            MetricValue::PruningMetrics {
                                name,
                                pruning_metrics,
                            } if name == "row_groups_pruned_statistics" => {
                                Some(pruning_metrics.pruned())
                            }
                            _ => None,
                        })
                        .sum();
                    observations.scans.lock().unwrap().push(ScanObservation {
                        task: event.task_key.task_number,
                        files,
                        local,
                        remote,
                        output_rows: metrics.output_rows().unwrap_or_default(),
                        pruned_row_groups,
                    });
                    Ok(TreeNodeRecursion::Continue)
                })?;
                observations.changed.notify_one();
                Ok(TaskFinishedEventResponse::new(MetricsSet::new()))
            }
        }

        async fn wait_for(&self, count: usize) -> Result<Vec<ScanObservation>> {
            timeout(Duration::from_secs(10), async {
                loop {
                    let notified = self.changed.notified();
                    let scans = self.scans.lock().unwrap().clone();
                    if scans.len() >= count {
                        return scans;
                    }
                    notified.await;
                }
            })
            .await
            .map_err(|error| {
                internal_datafusion_err!("waiting for worker scan observations: {error}")
            })
        }
    }

    fn producer_ids(plan: &Arc<dyn ExecutionPlan>) -> Result<HashSet<u64>> {
        let mut ids = HashSet::new();
        plan.apply(|node| {
            ids.extend(
                node.dynamic_expressions_produced()
                    .iter()
                    .filter_map(|expression| expression.expression_id()),
            );
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(ids)
    }

    fn is_initial_bound(predicate: &Arc<dyn PhysicalExpr>) -> bool {
        predicate.as_ref() == lit(true).as_ref()
            || predicate
                .downcast_ref::<BinaryExpr>()
                .is_some_and(|binary| {
                    binary.op() == &Operator::And
                        && is_initial_bound(binary.left())
                        && is_initial_bound(binary.right())
                })
    }

    fn scan_filters(source: &dyn DataSource) -> Result<HashMap<u64, Arc<dyn PhysicalExpr>>> {
        let mut filters = HashMap::new();
        source.apply_expressions(&mut |expression| {
            expression.apply(|expression| {
                if expression.is::<DynamicFilterPhysicalExpr>() {
                    filters.insert(expression.expression_id().unwrap(), Arc::clone(expression));
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(filters)
    }

    #[derive(Clone, Debug)]
    struct ScanFile {
        path: String,
        range: Option<(i64, i64)>,
    }

    fn scan_files(plan: &Arc<dyn ExecutionPlan>) -> Option<Vec<ScanFile>> {
        let source = plan.downcast_ref::<DataSourceExec>()?;
        let source = source.data_source();
        let inner = source
            .downcast_ref::<WaitForSiblingSource>()
            .map_or(source, |source| &source.inner);
        let config = inner.downcast_ref::<FileScanConfig>()?;
        let mut files = config
            .file_groups
            .iter()
            .flat_map(|group| group.iter())
            .map(|file| ScanFile {
                path: file.object_meta.location.to_string(),
                range: file.range.as_ref().map(|range| (range.start, range.end)),
            })
            .collect::<Vec<_>>();
        files.sort_by(|left, right| left.path.cmp(&right.path));
        Some(files)
    }

    fn gate_second_task_scan(
        event: WorkerPlanRewriteEvent<'_>,
    ) -> Result<WorkerPlanRewriteEventResponse> {
        let task = event
            .session_config
            .get_extension::<DistributedTaskContext>()
            .unwrap();
        if task.task_index != 1 {
            return Ok(WorkerPlanRewriteEventResponse::new(event.plan));
        }
        let local_ids = producer_ids(&event.plan)?;
        let plan = event
            .plan
            .transform_up(|node| {
                if let Some(source) = node.downcast_ref::<DataSourceExec>() {
                    let gated = WaitForSiblingSource {
                        inner: Arc::clone(source.data_source()),
                        local_ids: local_ids.clone(),
                    };
                    return Ok(Transformed::yes(Arc::new(
                        source.clone().with_data_source(Arc::new(gated)),
                    ) as _));
                }
                Ok(Transformed::no(node))
            })?
            .data;
        Ok(WorkerPlanRewriteEventResponse::new(plan))
    }

    /// A custom data source that waits for sibling delivery before opening Parquet.
    /// It changes scheduling only; Parquet evaluates and prunes with the real predicate.
    #[derive(Clone, Debug)]
    struct WaitForSiblingSource {
        inner: Arc<dyn DataSource>,
        local_ids: HashSet<u64>,
    }

    impl DataSource for WaitForSiblingSource {
        delegate! {
            to self.inner {
                fn fmt_as(&self, format: DisplayFormatType, formatter: &mut Formatter) -> FmtResult;
                fn output_partitioning(&self) -> Partitioning;
                fn eq_properties(&self) -> EquivalenceProperties;
                fn scheduling_type(&self) -> SchedulingType;
                fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>>;
                fn fetch(&self) -> Option<usize>;
                fn metrics(&self) -> ExecutionPlanMetricsSet;
                fn apply_expressions(&self, f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion>;
                fn create_sibling_state(&self, config: &ConfigOptions) -> Option<Arc<dyn Any + Send + Sync>>;
            }
        }

        fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn DataSource>> {
            self.inner.with_fetch(limit).map(|inner| {
                Arc::new(Self {
                    inner,
                    ..self.clone()
                }) as Arc<dyn DataSource>
            })
        }

        fn try_swapping_with_projection(
            &self,
            projection: &ProjectionExprs,
        ) -> Result<Option<Arc<dyn DataSource>>> {
            Ok(self
                .inner
                .try_swapping_with_projection(projection)?
                .map(|inner| {
                    Arc::new(Self {
                        inner,
                        ..self.clone()
                    }) as Arc<dyn DataSource>
                }))
        }

        fn try_pushdown_filters(
            &self,
            filters: Vec<Arc<dyn PhysicalExpr>>,
            config: &ConfigOptions,
        ) -> Result<FilterPushdownPropagation<Arc<dyn DataSource>>> {
            let properties = self.eq_properties();
            let schema = properties.schema();
            assert_eq!(
                schema.fields().len(),
                3,
                "the unused column was projected out"
            );
            for filter in &filters {
                filter.apply(|expression| {
                    if let Some(column) = expression.downcast_ref::<Column>() {
                        assert_eq!(schema.field(column.index()).name(), column.name());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })?;
            }
            let mut result = self.inner.try_pushdown_filters(filters, config)?;
            result.updated_node = result.updated_node.map(|inner| {
                Arc::new(Self {
                    inner,
                    ..self.clone()
                }) as Arc<dyn DataSource>
            });
            Ok(result)
        }

        fn open(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            self.open_with_args(OpenArgs::new(partition, context))
        }

        fn open_with_args(&self, args: OpenArgs) -> Result<SendableRecordBatchStream> {
            let remote = scan_filters(self.inner.as_ref())?
                .into_iter()
                .filter(|(id, _)| !self.local_ids.contains(id))
                .map(|(_, expression)| expression)
                .collect::<Vec<_>>();
            let [remote] = remote.as_slice() else {
                return internal_err!("gated scan requires exactly one attached sibling filter");
            };
            let remote = Arc::clone(remote);
            let inner = Arc::clone(&self.inner);
            let pending = stream::once(async move {
                let filter = remote.downcast_ref::<DynamicFilterPhysicalExpr>().unwrap();
                timeout(Duration::from_secs(10), async {
                    loop {
                        let changed = filter.wait_update();
                        futures::pin_mut!(changed);
                        let ready = futures::poll!(&mut changed).is_ready();
                        if !is_initial_bound(&filter.current()?) {
                            break Ok::<_, datafusion::error::DataFusionError>(());
                        }
                        if !ready {
                            changed.await;
                        }
                    }
                })
                .await
                .map_err(|error| {
                    internal_datafusion_err!(
                        "no useful sibling bound reached the gated scan: {error}"
                    )
                })??;
                inner.open_with_args(args)
            });
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(self.eq_properties().schema()),
                pending.try_flatten(),
            )))
        }
    }
}
