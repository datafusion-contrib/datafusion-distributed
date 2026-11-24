//! Benchmark encoding and decoding an eager Iceberg scan with many assigned files.
//!
//! Each assigned file is encoded with iceberg-rust's `FileScanTask` serialization, which embeds the
//! table schema in every file. Plan size and codec time therefore grow with files × schema width.
//! The coordinator pays this when it sends a task, and again when dynamic filtering copies a task
//! plan; workers pay the decode. The cases cover a narrow and a wide schema at increasing file
//! counts. `drop` measures freeing a decoded scan, whose files each own a separate schema.

use criterion::{
    BatchSize, BenchmarkId, Criterion, SamplingMode, Throughput, criterion_group, criterion_main,
};
use datafusion::common::{Result, exec_datafusion_err};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_distributed_iceberg::iceberg::arrow::schema_to_arrow_schema;
use datafusion_distributed_iceberg::iceberg::io::{FileIOBuilder, LocalFsStorageFactory};
use datafusion_distributed_iceberg::iceberg::scan::FileScanTask;
use datafusion_distributed_iceberg::iceberg::spec::{
    DataFileFormat, NestedField, PrimitiveType, Schema, StructType, Type,
};
use datafusion_distributed_iceberg::{EagerIcebergCodec, EagerIcebergDataSource, iceberg};
use datafusion_proto::physical_plan::AsExecutionPlan;
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::sync::Arc;
use tokio::runtime::Builder as RuntimeBuilder;

/// Local output partitions of the scan, as with the default `target_partitions` of 16.
const PARTITIONS: usize = 16;

struct SchemaShape {
    name: &'static str,
    columns: usize,
}

const SHAPES: [SchemaShape; 2] = [
    SchemaShape {
        name: "narrow",
        columns: 16,
    },
    SchemaShape {
        name: "wide",
        columns: 100,
    },
];

const FILE_COUNTS: [usize; 3] = [100, 1_000, 2_200];

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

    for shape in &SHAPES {
        let schema = Arc::new(iceberg_schema(shape.columns).expect("iceberg schema"));
        for files in FILE_COUNTS {
            let plan = eager_scan(&schema, files, &task_ctx).expect("eager scan");
            let bytes = encode(&plan, &codec).expect("encode");
            let id = format!("{}/{files}_files", shape.name);
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

/// An eager scan over `files` synthetic data files spread across [`PARTITIONS`] partitions.
/// Every file shares one `Arc<Schema>`, as files planned from one table snapshot do.
fn eager_scan(
    schema: &Arc<Schema>,
    files: usize,
    task_ctx: &TaskContext,
) -> Result<Arc<dyn ExecutionPlan>> {
    let field_ids = schema
        .as_struct()
        .fields()
        .iter()
        .map(|field| field.id)
        .collect::<Vec<_>>();
    let mut groups = vec![Vec::new(); PARTITIONS];
    for file in 0..files {
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(64 * 1024 * 1024)
            .with_start(0)
            .with_length(64 * 1024 * 1024)
            .with_record_count(Some(100_000))
            .with_data_file_path(format!("s3://example/data/{file:06}.parquet"))
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(Arc::clone(schema))
            .with_project_field_ids(field_ids.clone())
            .with_case_sensitive(true)
            .build()
            .map_err(|error| exec_datafusion_err!("cannot build file scan task: {error}"))?;
        groups[file % PARTITIONS].push(task);
    }
    let arrow_schema = Arc::new(
        schema_to_arrow_schema(schema)
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

/// A table schema of `columns` optional columns. Every eighth column is a struct of four fields,
/// standing in for the nested columns common in wide tables.
fn iceberg_schema(columns: usize) -> Result<Schema> {
    // Top-level columns take ids 1..=columns; nested fields take the ids after them.
    let fields = (0..columns).map(|column| {
        let field_type = if column % 8 == 7 {
            let first_child_id = columns + 1 + (column / 8) * 4;
            let children = (0..4)
                .map(|child| {
                    Arc::new(NestedField::optional(
                        (first_child_id + child) as i32,
                        format!("nested_attribute_{child}"),
                        Type::Primitive(PrimitiveType::String),
                    ))
                })
                .collect::<Vec<_>>();
            Type::Struct(StructType::new(children))
        } else if column % 3 == 0 {
            Type::Primitive(PrimitiveType::Long)
        } else {
            Type::Primitive(PrimitiveType::String)
        };
        Arc::new(NestedField::optional(
            (column + 1) as i32,
            format!("resource_attribute_column_{column:04}"),
            field_type,
        ))
    });
    Schema::builder()
        .with_fields(fields)
        .build()
        .map_err(|error| exec_datafusion_err!("cannot build schema: {error}"))
}

criterion_group!(benches, eager_scan_codec);
criterion_main!(benches);
