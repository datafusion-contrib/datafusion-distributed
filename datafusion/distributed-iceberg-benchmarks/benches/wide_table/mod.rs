//! The table shape shared by the Iceberg scan benchmarks.
//!
//! Tables have `columns` optional columns, and every eighth one is a struct of four fields,
//! standing in for the nested columns common in wide tables. They are partitioned by the
//! identity of their first column and carry a default name mapping, as tables written by other
//! engines often do. Planned files then hold the schema, the name mapping and the partition spec,
//! as well as their own partition value.

// Each benchmark uses only part of this module.
#![allow(dead_code)]

use datafusion::common::{Result, exec_datafusion_err};
use datafusion_distributed_iceberg::iceberg::spec::{
    MappedField, NameMapping, NestedField, NestedFieldRef, PartitionSpec, PrimitiveType, Schema,
    StructType, Transform, Type,
};
use std::sync::Arc;

/// The first column, which tables are partitioned by.
pub const PARTITION_COLUMN: &str = "resource_attribute_column_0000";
/// A string column the end-to-end queries filter on.
pub const FILTER_COLUMN: &str = "resource_attribute_column_0001";
/// The table property iceberg-rust reads the default name mapping from.
pub const NAME_MAPPING_PROPERTY: &str = "schema.name-mapping.default";

/// A table schema of `columns` optional columns.
pub fn iceberg_schema(columns: usize) -> Result<Schema> {
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

/// Maps every column of `schema`, nested ones included, from its name to its field id.
pub fn name_mapping(schema: &Schema) -> NameMapping {
    fn mapped(fields: &[NestedFieldRef]) -> Vec<MappedField> {
        fields
            .iter()
            .map(|field| {
                let children = match field.field_type.as_ref() {
                    Type::Struct(nested) => mapped(nested.fields()),
                    _ => vec![],
                };
                MappedField::new(Some(field.id), vec![field.name.clone()], children)
            })
            .collect()
    }
    NameMapping::new(mapped(schema.as_struct().fields()))
}

/// Partitions by the identity of [`PARTITION_COLUMN`].
pub fn partition_spec(schema: &Arc<Schema>) -> Result<PartitionSpec> {
    PartitionSpec::builder(Arc::clone(schema))
        .add_partition_field(PARTITION_COLUMN, PARTITION_COLUMN, Transform::Identity)
        .and_then(|builder| builder.build())
        .map_err(|error| exec_datafusion_err!("cannot build partition spec: {error}"))
}
