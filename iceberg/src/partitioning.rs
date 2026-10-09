use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::common::hash_utils::create_hashes;
use datafusion::common::{Result, exec_datafusion_err, exec_err, not_impl_err};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{Partitioning, PhysicalExpr};
use datafusion::physical_plan::repartition::REPARTITION_RANDOM_STATE;
use datafusion::scalar::ScalarValue;
use iceberg::arrow::type_to_arrow_type;
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    Literal, PartitionSpec, PrimitiveType, Schema, TableMetadata, Transform, Type,
};
use iceberg::table::Table;

use crate::common::{df_err, primitive_to_scalar};

/// Returns the output partitioning that a scan producing `output_schema` can declare.
///
/// Only one scheme can be satisfied from Iceberg metadata alone: [Partitioning::Hash] over
/// identity-transformed partition columns. All the rows of a data file share the same value for
/// those columns, and that value is known from the file's partition metadata, so
/// [FileScanTaskRouter] can place each file in the partition where a hash repartition would have
/// placed its rows.
///
/// Any other Iceberg transform does not qualify. `bucket[N]` in particular hashes with murmur3,
/// which is not the hash DataFusion uses, so declaring it as [Partitioning::Hash] would break
/// partitioned joins against a side that went through a hash repartition.
///
/// A column only qualifies if it is identity-partitioned in every partition spec of the table,
/// as files written under an older spec keep that spec's partition values.
pub(crate) fn scan_partitioning(
    table: &Table,
    snapshot_id: Option<i64>,
    output_schema: &SchemaRef,
    partitions: usize,
    hash_partitioning_enabled: bool,
) -> Partitioning {
    let unknown = Partitioning::UnknownPartitioning(partitions);
    if !hash_partitioning_enabled {
        return unknown;
    }
    let Ok(table_schema) = scan_schema(table, snapshot_id) else {
        return unknown;
    };

    let mut exprs: Vec<Arc<dyn PhysicalExpr>> = vec![];
    for source_id in identity_partition_source_ids(table.metadata()) {
        // Only top-level columns can be referenced from the output schema.
        let fields = table_schema.as_struct().fields();
        let Some(field) = fields.iter().find(|field| field.id == source_id) else {
            continue;
        };
        let Type::Primitive(primitive_type) = field.field_type.as_ref() else {
            continue;
        };
        if !is_hashable(primitive_type) {
            continue;
        }
        if let Ok(idx) = output_schema.index_of(&field.name) {
            exprs.push(Arc::new(Column::new(&field.name, idx)));
        }
    }

    if exprs.is_empty() {
        return unknown;
    }
    Partitioning::Hash(exprs, partitions)
}

/// Decides to which output channel each [FileScanTask] goes so that the scan adheres to its
/// declared [Partitioning].
#[derive(Debug)]
pub(crate) enum FileScanTaskRouter {
    RoundRobin { next: usize },
    Hash { keys: Vec<HashKey> },
}

#[derive(Debug)]
pub(crate) struct HashKey {
    source_id: i32,
    iceberg_type: PrimitiveType,
    arrow_type: DataType,
}

impl FileScanTaskRouter {
    pub(crate) fn try_new(
        partitioning: &Partitioning,
        table: &Table,
        snapshot_id: Option<i64>,
    ) -> Result<Self> {
        let exprs = match partitioning {
            Partitioning::UnknownPartitioning(_) | Partitioning::RoundRobinBatch(_) => {
                return Ok(Self::RoundRobin { next: 0 });
            }
            Partitioning::Hash(exprs, _) => exprs,
            Partitioning::Range(_) => {
                return not_impl_err!("Iceberg scans do not support {partitioning} partitioning");
            }
        };

        let table_schema = scan_schema(table, snapshot_id)?;
        let identity_source_ids = identity_partition_source_ids(table.metadata());
        let mut keys = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let Some(column) = expr.downcast_ref::<Column>() else {
                return not_impl_err!("Iceberg scans cannot be hash partitioned by {expr}");
            };
            let field = table_schema
                .as_struct()
                .fields()
                .iter()
                .find(|field| field.name == column.name())
                .filter(|field| identity_source_ids.contains(&field.id));
            let Some(field) = field else {
                return not_impl_err!(
                    "Iceberg scans can only be hash partitioned by identity partition columns, got {expr}"
                );
            };
            let Type::Primitive(iceberg_type) = field.field_type.as_ref() else {
                return not_impl_err!("Iceberg scans cannot be hash partitioned by {expr}");
            };
            keys.push(HashKey {
                source_id: field.id,
                iceberg_type: iceberg_type.clone(),
                arrow_type: type_to_arrow_type(&field.field_type).map_err(df_err)?,
            });
        }
        Ok(Self::Hash { keys })
    }

    /// Returns the index of the output channel, out of `channels`, the task should be sent to.
    pub(crate) fn route(&mut self, task: &FileScanTask, channels: usize) -> Result<usize> {
        let keys = match self {
            Self::RoundRobin { next } => {
                let channel = *next % channels;
                *next = channel + 1;
                return Ok(channel);
            }
            Self::Hash { keys } => keys,
        };

        let (Some(spec), Some(partition)) = (task.partition_spec(), task.partition()) else {
            return exec_err!(
                "Iceberg file scan task for {} has no partition values",
                task.data_file_path()
            );
        };
        let mut arrays = Vec::with_capacity(keys.len());
        for key in keys {
            let value = spec
                .fields()
                .iter()
                .position(|f| f.source_id == key.source_id && f.transform == Transform::Identity)
                .and_then(|idx| partition.fields().get(idx))
                .ok_or_else(|| {
                    exec_datafusion_err!(
                        "Iceberg file scan task for {} has no identity partition value for field id {}",
                        task.data_file_path(),
                        key.source_id
                    )
                })?;
            let scalar = match value {
                None => ScalarValue::try_from(&key.arrow_type)?,
                Some(Literal::Primitive(literal)) => {
                    primitive_to_scalar(&key.iceberg_type, literal)
                        .ok_or_else(|| {
                            exec_datafusion_err!(
                                "unsupported Iceberg partition value {literal:?} of type {}",
                                key.iceberg_type
                            )
                        })?
                        // The hash depends on the physical Arrow type, which needs to be the one
                        // of the column a hash repartition would have seen.
                        .cast_to(&key.arrow_type)?
                }
                Some(literal) => {
                    return exec_err!("unsupported Iceberg partition value {literal:?}");
                }
            };
            arrays.push(scalar.to_array()?);
        }

        // Same hashing as RepartitionExec, so that this scan is co-partitioned with any other
        // input that was hash repartitioned by the same keys.
        let mut hashes = vec![0; 1];
        create_hashes(
            &arrays,
            REPARTITION_RANDOM_STATE.random_state(),
            &mut hashes,
        )?;
        Ok((hashes[0] % channels as u64) as usize)
    }
}

/// Schema of the snapshot that gets scanned.
fn scan_schema(table: &Table, snapshot_id: Option<i64>) -> Result<Arc<Schema>> {
    let metadata = table.metadata();
    let snapshot = match snapshot_id {
        Some(snapshot_id) => metadata.snapshot_by_id(snapshot_id),
        None => metadata.current_snapshot(),
    };
    match snapshot {
        Some(snapshot) => snapshot.schema(metadata).map_err(df_err),
        // empty table
        None => Ok(Arc::clone(metadata.current_schema())),
    }
}

/// Ids of the source columns that are identity-partitioned in every partition spec of the table,
/// in the order of the default partition spec.
fn identity_partition_source_ids(metadata: &TableMetadata) -> Vec<i32> {
    let is_identity_in = |spec: &PartitionSpec, source_id: i32| {
        spec.fields()
            .iter()
            .any(|f| f.source_id == source_id && f.transform == Transform::Identity)
    };
    metadata
        .default_partition_spec()
        .fields()
        .iter()
        .filter(|f| f.transform == Transform::Identity)
        .map(|f| f.source_id)
        .filter(|id| {
            metadata
                .partition_specs_iter()
                .all(|spec| is_identity_in(spec, *id))
        })
        .collect()
}

/// Whether equal values of this type are guaranteed to hash equally. Floats are left out because
/// `0.0` and `-0.0` are equal but hash differently.
fn is_hashable(primitive_type: &PrimitiveType) -> bool {
    matches!(
        primitive_type,
        PrimitiveType::Boolean
            | PrimitiveType::Int
            | PrimitiveType::Long
            | PrimitiveType::String
            | PrimitiveType::Date
            | PrimitiveType::Timestamp
            | PrimitiveType::Timestamptz
            | PrimitiveType::Decimal { .. }
    )
}
