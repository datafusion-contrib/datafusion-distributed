//! Benchmark encoding and decoding an eager Iceberg scan with many assigned files.
//!
//! ```shell
//! cargo bench -p datafusion-distributed-iceberg-benchmarks --bench eager_scan_codec -- --save-baseline main
//! # Run the same benchmark on the candidate revision:
//! cargo bench -p datafusion-distributed-iceberg-benchmarks --bench eager_scan_codec -- --baseline main
//! ```
//!
//! Each assigned file is encoded with iceberg-rust's `FileScanTask` serialization, which embeds
//! the table's schema, name mapping, partition spec and the scan predicate in every file. Plan
//! size and codec time therefore grow with files × table width. The coordinator pays this when
//! it sends a task, and again when dynamic filtering copies a task plan; workers pay the decode.
//!
//! The cases cover a narrow and a wide table at increasing file counts in one scan, and a plan of
//! many small scans, as a query over a union of tables produces. `drop` measures freeing a
//! decoded plan. Before timing, every case checks that decoding returns the files it encoded.

#[path = "wide_table/mod.rs"]
mod wide_table;

use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Result, exec_datafusion_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::union::UnionExec;
use datafusion_distributed_iceberg::iceberg::arrow::schema_to_arrow_schema;
use datafusion_distributed_iceberg::iceberg::expr::{Bind, BoundPredicate, Reference};
use datafusion_distributed_iceberg::iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
use datafusion_distributed_iceberg::iceberg::scan::FileScanTask;
use datafusion_distributed_iceberg::iceberg::spec::{
    DataFileFormat, Literal, NameMapping, PartitionSpec, Schema, Struct,
};
use datafusion_distributed_iceberg::{EagerIcebergCodec, EagerIcebergDataSource, iceberg};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::sync::Arc;
use tokio::runtime::Builder as RuntimeBuilder;
use wide_table::{FILTER_COLUMN, iceberg_schema, name_mapping, partition_spec};

/// Local output partitions of each scan, as with the default `target_partitions` of 16.
const PARTITIONS: usize = 16;
/// Distinct values of the partition column across files.
const PARTITION_VALUES: usize = 30;

struct Case {
    name: &'static str,
    columns: usize,
    scans: usize,
    files_per_scan: usize,
}

const fn case(name: &'static str, columns: usize, scans: usize, files_per_scan: usize) -> Case {
    Case {
        name,
        columns,
        scans,
        files_per_scan,
    }
}

const CASES: [Case; 7] = [
    case("narrow", 16, 1, 100),
    case("narrow", 16, 1, 1_000),
    case("narrow", 16, 1, 2_200),
    case("wide", 100, 1, 100),
    case("wide", 100, 1, 1_000),
    case("wide", 100, 1, 2_200),
    // About as many files as the largest single scan, spread over many small scans.
    case("wide", 100, 200, 10),
];

fn eager_scan_codec(c: &mut Criterion) {
    let rt = RuntimeBuilder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let _guard = rt.enter();
    let task_ctx = TaskContext::default();
    let codec =
        EagerIcebergCodec::new(Arc::new(LocalFsStorageFactory), iceberg::Runtime::current());

    let mut group = c.benchmark_group("eager_scan_codec");
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    for case in &CASES {
        let table = Table::new(case.columns).expect("table");
        let plan = table
            .plan(case.scans, case.files_per_scan, &task_ctx)
            .expect("eager scan plan");
        let bytes = encode(&plan, &codec).expect("encode");
        let decoded = decode(&bytes, &codec, &task_ctx).expect("decode");
        assert!(
            planned_files(&decoded) == planned_files(&plan),
            "decoding must return the encoded files"
        );

        let files = case.scans * case.files_per_scan;
        let id = match case.scans {
            1 => format!("{}/{files}_files", case.name),
            scans => format!("{}/{scans}_scans_{}_files", case.name, case.files_per_scan),
        };
        println!(
            "eager_scan_codec/{id}: {} bytes, {} bytes per file",
            bytes.len(),
            bytes.len() / files
        );

        group.throughput(Throughput::Elements(files as u64));
        group.bench_function(BenchmarkId::new("encode", &id), |b| {
            b.iter(|| encode(&plan, &codec).unwrap())
        });
        group.bench_function(BenchmarkId::new("decode", &id), |b| {
            b.iter(|| decode(&bytes, &codec, &task_ctx).unwrap())
        });
        group.bench_function(BenchmarkId::new("drop", &id), |b| {
            b.iter_batched(
                || decode(&bytes, &codec, &task_ctx).unwrap(),
                drop,
                BatchSize::PerIteration,
            )
        });
    }

    group.finish();
}

fn encode(plan: &Arc<dyn ExecutionPlan>, codec: &EagerIcebergCodec) -> Result<Vec<u8>> {
    let proto = PhysicalPlanNode::try_from_physical_plan(Arc::clone(plan), codec)?;
    Ok(proto.encode_to_vec())
}

fn decode(
    bytes: &[u8],
    codec: &EagerIcebergCodec,
    task_ctx: &TaskContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let proto = PhysicalPlanNode::decode(bytes)
        .map_err(|error| exec_datafusion_err!("cannot decode plan: {error}"))?;
    proto.try_into_physical_plan(task_ctx, codec)
}

/// The files of every eager Iceberg scan in `plan`, in plan order.
fn planned_files(plan: &Arc<dyn ExecutionPlan>) -> Vec<FileScanTask> {
    let mut files = vec![];
    plan.apply(|node| {
        if let Some(source) = node
            .downcast_ref::<DataSourceExec>()
            .and_then(|exec| exec.data_source().downcast_ref::<EagerIcebergDataSource>())
        {
            files.extend(source.planned_files().cloned());
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("visit plan");
    files
}

/// The values every file planned from one table snapshot shares.
struct Table {
    schema: Arc<Schema>,
    field_ids: Vec<i32>,
    predicate: BoundPredicate,
    partition_spec: Arc<PartitionSpec>,
    name_mapping: Arc<NameMapping>,
}

impl Table {
    fn new(columns: usize) -> Result<Self> {
        let schema = Arc::new(iceberg_schema(columns)?);
        let field_ids = schema
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.id)
            .collect();
        let predicate = Reference::new(FILTER_COLUMN)
            .is_null()
            .bind(Arc::clone(&schema), true)
            .map_err(|error| exec_datafusion_err!("cannot bind predicate: {error}"))?;
        Ok(Self {
            partition_spec: Arc::new(partition_spec(&schema)?),
            name_mapping: Arc::new(name_mapping(&schema)),
            schema,
            field_ids,
            predicate,
        })
    }

    /// A plan of `scans` eager scans, unioned if more than one, each over `files_per_scan`
    /// synthetic data files spread across [`PARTITIONS`] partitions.
    fn plan(
        &self,
        scans: usize,
        files_per_scan: usize,
        task_ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut inputs = (0..scans)
            .map(|scan| self.scan(scan, files_per_scan, task_ctx))
            .collect::<Result<Vec<_>>>()?;
        if inputs.len() == 1 {
            return Ok(inputs.remove(0));
        }
        UnionExec::try_new(inputs)
    }

    fn scan(
        &self,
        scan: usize,
        files: usize,
        task_ctx: &TaskContext,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let mut groups = vec![Vec::new(); PARTITIONS.min(files)];
        for file in 0..files {
            groups[file % PARTITIONS.min(files)].push(self.file(scan, file)?);
        }
        let arrow_schema = Arc::new(
            schema_to_arrow_schema(&self.schema)
                .map_err(|error| exec_datafusion_err!("cannot convert schema: {error}"))?,
        );
        let file_io = FileIOBuilder::new(Arc::new(LocalFsStorageFactory)).build();
        let source = EagerIcebergDataSource::from_file_groups(
            arrow_schema,
            groups,
            file_io,
            iceberg::Runtime::current(),
            None,
            task_ctx,
        )?;
        Ok(DataSourceExec::from_data_source(source))
    }

    /// A synthetic data file holding the table's shared values, as planned files do.
    fn file(&self, scan: usize, file: usize) -> Result<FileScanTask> {
        let partition_value = Literal::long((file % PARTITION_VALUES) as i64);
        FileScanTask::builder()
            .with_file_size_in_bytes(64 * 1024 * 1024)
            .with_start(0)
            .with_length(64 * 1024 * 1024)
            .with_record_count(Some(100_000))
            .with_data_file_path(format!("s3://example/data/{scan:04}/{file:06}.parquet"))
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(Arc::clone(&self.schema))
            .with_project_field_ids(self.field_ids.clone())
            .with_predicate(Some(self.predicate.clone()))
            .with_partition(Some(Struct::from_iter([Some(partition_value)])))
            .with_partition_spec(Some(Arc::clone(&self.partition_spec)))
            .with_name_mapping(Some(Arc::clone(&self.name_mapping)))
            .with_case_sensitive(true)
            .build()
            .map_err(|error| exec_datafusion_err!("cannot build file scan task: {error}"))
    }
}

criterion_group!(benches, eager_scan_codec);
criterion_main!(benches);
