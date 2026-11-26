//! Adds a filter to a `FileScanTask`.
//!
//! iceberg-rust has no API for this. A task's filter can't be changed, and iceberg-rust has no
//! public way to AND two bound filters. So we turn the task's filter back into an unbound one,
//! AND ours onto it, bind the result, and build a new task with it.
//!
//! If iceberg-rust adds an API for this, delete this file and use the API.

use iceberg::Result;
use iceberg::expr::{
    BinaryExpression, Bind, BoundPredicate, BoundReference, Predicate, Reference, SetExpression,
    UnaryExpression,
};
use iceberg::scan::FileScanTask;
use iceberg::spec::Schema;

/// Returns a copy of `task` whose filter is its existing filter AND `predicate`. Returns `None`
/// if that fails.
pub(crate) fn and_predicate(task: &FileScanTask, predicate: Predicate) -> Option<FileScanTask> {
    let combined = match task.predicate() {
        Some(existing) => unbind(existing, task.schema())?.and(predicate),
        None => predicate,
    };
    let combined = combined
        .bind(task.schema_ref(), task.case_sensitive())
        .ok()?;
    with_predicate(task, combined).ok()
}

/// Turns a bound predicate back into an unbound one, so we can AND it with another predicate and
/// bind it again. Each column gets its full name from `schema`, like `location.lat`. Returns
/// `None` if a column isn't in `schema`.
fn unbind(predicate: &BoundPredicate, schema: &Schema) -> Option<Predicate> {
    let reference = |column: &BoundReference| {
        schema
            .name_by_field_id(column.field().id)
            .map(Reference::new)
    };
    Some(match predicate {
        BoundPredicate::AlwaysTrue => Predicate::AlwaysTrue,
        BoundPredicate::AlwaysFalse => Predicate::AlwaysFalse,
        BoundPredicate::And(expr) => {
            let [left, right] = expr.inputs();
            unbind(left, schema)?.and(unbind(right, schema)?)
        }
        BoundPredicate::Or(expr) => {
            let [left, right] = expr.inputs();
            unbind(left, schema)?.or(unbind(right, schema)?)
        }
        BoundPredicate::Not(expr) => {
            let [inner] = expr.inputs();
            !unbind(inner, schema)?
        }
        BoundPredicate::Unary(expr) => {
            Predicate::Unary(UnaryExpression::new(expr.op(), reference(expr.term())?))
        }
        BoundPredicate::Binary(expr) => Predicate::Binary(BinaryExpression::new(
            expr.op(),
            reference(expr.term())?,
            expr.literal().clone(),
        )),
        BoundPredicate::Set(expr) => Predicate::Set(SetExpression::new(
            expr.op(),
            reference(expr.term())?,
            expr.literals().clone(),
        )),
    })
}

/// Returns a copy of `task` with `predicate` as its filter. Every other field is copied as is.
/// This is only required because iceberg-rust doesn't provide a way to add predicates to an existing FileScanTask
fn with_predicate(task: &FileScanTask, predicate: BoundPredicate) -> Result<FileScanTask> {
    FileScanTask::builder()
        .with_file_size_in_bytes(task.file_size_in_bytes())
        .with_start(task.start())
        .with_length(task.length())
        .with_record_count(task.record_count())
        .with_first_row_id(task.first_row_id())
        .with_data_sequence_number(task.data_sequence_number())
        .with_data_file_path(task.data_file_path().to_string())
        .with_data_file_format(task.data_file_format())
        .with_schema(task.schema_ref())
        .with_project_field_ids(task.project_field_ids().to_vec())
        .with_predicate(Some(predicate))
        .with_deletes(task.deletes().to_vec())
        .with_partition(task.partition().cloned())
        .with_partition_spec(task.partition_spec().cloned())
        .with_name_mapping(task.name_mapping().cloned())
        .with_unified_partition_type(task.unified_partition_type().cloned())
        .with_sort_order_id(task.sort_order_id())
        .with_sort_order(task.sort_order().cloned())
        .with_case_sensitive(task.case_sensitive())
        .with_key_metadata(task.key_metadata().map(Box::from))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::scan::FileScanTaskDeleteFile;
    use iceberg::spec::{
        DataContentType, DataFileFormat, Datum, Literal, MappedField, NameMapping, NestedField,
        PartitionSpec, PrimitiveType, SortOrder, Struct, StructType, Transform, Type,
    };
    use std::error::Error;
    use std::sync::Arc;

    #[test]
    fn unbinding_and_binding_again_gives_the_same_predicate()
    -> std::result::Result<(), Box<dyn Error>> {
        let location = StructType::new(vec![
            NestedField::optional(3, "lat", Type::Primitive(PrimitiveType::Int)).into(),
        ]);
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                    NestedField::optional(2, "name", Type::Primitive(PrimitiveType::String)).into(),
                    NestedField::optional(4, "location", Type::Struct(location)).into(),
                ])
                .build()?,
        );
        let id = || Reference::new("id");
        let predicate = id()
            .less_than(Datum::long(5))
            .and(!id().is_in([Datum::long(1), Datum::long(2)]))
            .or(Reference::new("name").starts_with(Datum::string("a")))
            .or(Reference::new("location.lat").is_null())
            .bind(Arc::clone(&schema), true)?;

        let unbound = unbind(&predicate, &schema).ok_or("a column isn't in the schema")?;

        assert_eq!(unbound.bind(schema, true)?, predicate);
        Ok(())
    }

    /// `with_predicate` copies a task one field at a time. Every field of the task below differs
    /// from its default, so the `assert_eq!` fails if the copy leaves a field out. If iceberg-rust
    /// adds a field, the copy would silently leave it out. The snapshot lists the task's fields,
    /// so it fails when a field is added.
    #[test]
    fn the_copy_of_a_task_keeps_every_field() -> std::result::Result<(), Box<dyn Error>> {
        let schema = Arc::new(
            Schema::builder()
                .with_fields(vec![
                    NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()?,
        );
        let spec = PartitionSpec::builder(Arc::clone(&schema))
            .add_partition_field("id", "id", Transform::Identity)?
            .build()?;
        let deletes = FileScanTaskDeleteFile::builder()
            .with_file_path("deletes.parquet".to_string())
            .with_file_size_in_bytes(1)
            .with_file_type(DataContentType::PositionDeletes)
            .with_file_format(DataFileFormat::Parquet)
            .with_partition_spec_id(0)
            .build()?;
        let name_mapping = NameMapping::new(vec![MappedField::new(
            Some(1),
            vec!["id".to_string()],
            vec![],
        )]);
        let task = FileScanTask::builder()
            .with_file_size_in_bytes(1)
            .with_start(2)
            .with_length(3)
            .with_record_count(Some(4))
            .with_first_row_id(Some(5))
            .with_data_sequence_number(Some(6))
            .with_data_file_path("data.parquet".to_string())
            .with_data_file_format(DataFileFormat::Parquet)
            .with_schema(Arc::clone(&schema))
            .with_project_field_ids(vec![1])
            .with_predicate(Some(
                Reference::new("id")
                    .is_null()
                    .bind(Arc::clone(&schema), true)?,
            ))
            .with_deletes(vec![deletes])
            .with_partition(Some(Struct::from_iter([Some(Literal::long(7))])))
            .with_unified_partition_type(Some(Arc::new(spec.partition_type(&schema)?)))
            .with_partition_spec(Some(Arc::new(spec)))
            .with_name_mapping(Some(Arc::new(name_mapping)))
            .with_sort_order_id(Some(8))
            .with_sort_order(Some(Arc::new(SortOrder::unsorted_order())))
            .with_case_sensitive(true)
            .with_key_metadata(Some(Box::from(b"key".as_slice())))
            .build()?;
        let predicate = task.predicate().ok_or("the task has no filter")?;

        let copy = with_predicate(&task, predicate.clone())?;

        assert_eq!(copy, task);
        insta::assert_snapshot!(field_names(&task).join("\n"), @"
        file_size_in_bytes
        start
        length
        record_count
        first_row_id
        data_sequence_number
        data_file_path
        data_file_format
        schema
        project_field_ids
        predicate
        deletes
        partition
        partition_spec
        name_mapping
        unified_partition_type
        sort_order_id
        sort_order
        case_sensitive
        key_metadata
        ");
        Ok(())
    }

    /// Reads a struct's field names from its pretty-printed `Debug` output. The top-level fields
    /// are the lines indented by exactly four spaces.
    fn field_names(value: &impl std::fmt::Debug) -> Vec<String> {
        format!("{value:#?}")
            .lines()
            .filter_map(|line| line.strip_prefix("    "))
            .filter(|line| !line.starts_with(' '))
            .filter_map(|line| line.split_once(':'))
            .map(|(name, _)| name.to_string())
            .collect()
    }
}
