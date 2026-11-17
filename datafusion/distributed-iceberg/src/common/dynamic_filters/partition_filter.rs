//! A copy of the code iceberg-rust uses to skip data files by their partition values. That
//! code isn't part of iceberg-rust's public API yet. When it becomes public, this file should
//! be deleted and its callers should use the public API.
//! Ref: https://github.com/apache/iceberg-rust/issues/3675
//!
//! Each part of this file copies one part of iceberg-rust:
//!
//! | This file                    | iceberg-rust                                                 |
//! |------------------------------|--------------------------------------------------------------|
//! | `PartitionFilter::try_new`   | `TableScanBuilder::with_filter`, `PartitionFilterCache::get` |
//! | `PartitionFilter::can_match` | `ExpressionEvaluator::eval`                                  |
//! | `project`, `project_column`  | `InclusiveProjection`                                        |
//! | `evaluate`                   | `ExpressionEvaluatorVisitor`                                 |
//! | `partition_value`, `datum`   | `StructAccessor::get`                                        |
//!
//! The copy works the same way as iceberg-rust, except for one case in `datum` that's
//! explained there.

use std::sync::Arc;

use iceberg::expr::{Bind, BoundPredicate, BoundReference, Predicate, PredicateOperator};
use iceberg::spec::{
    Datum, Literal, PartitionSpec, PartitionSpecRef, PrimitiveLiteral, PrimitiveType, Schema,
    SchemaRef, Struct, Type,
};
use iceberg::{Error, ErrorKind, Result};

/// Checks whether a data file's partition can hold rows that match a filter, as a table scan
/// does when it plans files.
pub(crate) struct PartitionFilter {
    /// The filter projected onto the spec's partition columns, bound to its partition schema.
    predicate: BoundPredicate,
    spec: PartitionSpecRef,
}

impl PartitionFilter {
    /// Creates a partition filter for `filter` on data files written with `partition_spec`.
    ///
    /// If `partition_spec` uses a column that `schema` no longer has, every partition matches.
    /// Returns an error if `filter` doesn't bind to `schema`, or if it can't be projected onto
    /// `partition_spec`.
    pub(crate) fn try_new(
        filter: Predicate,
        schema: SchemaRef,
        partition_spec: &PartitionSpecRef,
        case_sensitive: bool,
    ) -> Result<Self> {
        let filter = filter
            .rewrite_not()
            .bind(Arc::clone(&schema), case_sensitive)?;
        let has_dropped_source_column = partition_spec
            .fields()
            .iter()
            .any(|field| schema.field_by_id(field.source_id).is_none());
        let predicate = if has_dropped_source_column {
            BoundPredicate::AlwaysTrue
        } else {
            let partition_type = partition_spec.partition_type(&schema)?;
            let partition_schema = Arc::new(
                Schema::builder()
                    .with_schema_id(partition_spec.spec_id())
                    .with_fields(partition_type.fields().to_owned())
                    .build()?,
            );
            project(&filter, partition_spec)?
                .rewrite_not()
                .bind(partition_schema, case_sensitive)?
        };
        Ok(Self {
            predicate,
            spec: Arc::clone(partition_spec),
        })
    }

    /// Returns whether a data file with `partition` can hold rows that match the filter.
    /// Returns an error if `partition` doesn't match the partition spec's fields.
    pub(crate) fn can_match(&self, partition: &Struct) -> Result<bool> {
        evaluate(&self.predicate, &self.spec, partition)
    }
}

/// Rewrites a predicate on table columns into one on the spec's partition columns. The result
/// matches every partition that holds a matching row, and can match more.
fn project(predicate: &BoundPredicate, spec: &PartitionSpec) -> Result<Predicate> {
    Ok(match predicate {
        BoundPredicate::AlwaysTrue => Predicate::AlwaysTrue,
        BoundPredicate::AlwaysFalse => Predicate::AlwaysFalse,
        BoundPredicate::And(expr) => {
            let [left, right] = expr.inputs();
            project(left, spec)?.and(project(right, spec)?)
        }
        BoundPredicate::Or(expr) => {
            let [left, right] = expr.inputs();
            project(left, spec)?.or(project(right, spec)?)
        }
        // `PartitionFilter::try_new` removes every NOT first, so none should reach here. If one
        // does, this returns an error, as iceberg-rust does.
        BoundPredicate::Not(_) => {
            return Err(Error::new(
                ErrorKind::Unexpected,
                "found a NOT that `PartitionFilter::try_new` should have removed",
            ));
        }
        BoundPredicate::Unary(expr) => project_column(expr.term(), predicate, spec)?,
        BoundPredicate::Binary(expr) => project_column(expr.term(), predicate, spec)?,
        BoundPredicate::Set(expr) => project_column(expr.term(), predicate, spec)?,
    })
}

/// Projects a predicate on one column through every partition field built from that column.
/// A column with no partition field gives `AlwaysTrue`.
fn project_column(
    column: &BoundReference,
    predicate: &BoundPredicate,
    spec: &PartitionSpec,
) -> Result<Predicate> {
    spec.fields()
        .iter()
        .filter(|field| field.source_id == column.field().id)
        .try_fold(Predicate::AlwaysTrue, |projected, field| {
            // `None` means the transform can't express the predicate, so it keeps every file.
            Ok(match field.transform.project(&field.name, predicate)? {
                Some(part) => projected.and(part),
                None => projected,
            })
        })
}

/// Checks whether one data file's partition values pass a filter on the partition columns.
fn evaluate(predicate: &BoundPredicate, spec: &PartitionSpec, partition: &Struct) -> Result<bool> {
    match predicate {
        BoundPredicate::AlwaysTrue => Ok(true),
        BoundPredicate::AlwaysFalse => Ok(false),
        BoundPredicate::And(expr) => {
            let [left, right] = expr.inputs();
            let (left, right) = (
                evaluate(left, spec, partition)?,
                evaluate(right, spec, partition)?,
            );
            Ok(left && right)
        }
        BoundPredicate::Or(expr) => {
            let [left, right] = expr.inputs();
            let (left, right) = (
                evaluate(left, spec, partition)?,
                evaluate(right, spec, partition)?,
            );
            Ok(left || right)
        }
        BoundPredicate::Not(_) => Err(Error::new(
            ErrorKind::Unexpected,
            "found a NOT that `PartitionFilter::try_new` should have removed",
        )),
        BoundPredicate::Unary(expr) => {
            let value = partition_value(expr.term(), spec, partition)?;
            let is_nan = value.as_ref().is_some_and(Datum::is_nan);
            Ok(match expr.op() {
                PredicateOperator::IsNull => value.is_none(),
                PredicateOperator::NotNull => value.is_some(),
                PredicateOperator::IsNan => is_nan,
                PredicateOperator::NotNan => !is_nan,
                _ => true,
            })
        }
        BoundPredicate::Binary(expr) => {
            let Some(value) = partition_value(expr.term(), spec, partition)? else {
                return Ok(matches!(
                    expr.op(),
                    PredicateOperator::NotEq | PredicateOperator::NotStartsWith
                ));
            };
            let literal = expr.literal();
            Ok(match expr.op() {
                PredicateOperator::StartsWith => starts_with(&value, literal),
                PredicateOperator::NotStartsWith => !starts_with(&value, literal),
                PredicateOperator::LessThan => &value < literal,
                PredicateOperator::LessThanOrEq => &value <= literal,
                PredicateOperator::GreaterThan => &value > literal,
                PredicateOperator::GreaterThanOrEq => &value >= literal,
                PredicateOperator::Eq => &value == literal,
                PredicateOperator::NotEq => &value != literal,
                _ => true,
            })
        }
        BoundPredicate::Set(expr) => {
            let Some(value) = partition_value(expr.term(), spec, partition)? else {
                return Ok(matches!(expr.op(), PredicateOperator::NotIn));
            };
            let contains = expr.literals().contains(&value);
            Ok(match expr.op() {
                PredicateOperator::In => contains,
                PredicateOperator::NotIn => !contains,
                _ => true,
            })
        }
    }
}

/// Returns the partition value that `column` points to, with the partition column's type.
/// Returns `None` when the value is null.
///
/// iceberg-rust reads this value with `StructAccessor::get`, which isn't public. So this finds
/// the value's position by looking up its partition field ID in the spec.
fn partition_value(
    column: &BoundReference,
    spec: &PartitionSpec,
    partition: &Struct,
) -> Result<Option<Datum>> {
    let field = column.field();
    let position = spec
        .fields()
        .iter()
        .position(|partition_field| partition_field.field_id == field.id)
        .ok_or_else(|| {
            Error::new(
                ErrorKind::DataInvalid,
                format!("partition field {} is not in the spec", field.id),
            )
        })?;
    let Some(value) = partition.fields().get(position) else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("the partition has no value at {position}"),
        ));
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let (Literal::Primitive(literal), Type::Primitive(r#type)) = (value, field.field_type.as_ref())
    else {
        return Err(Error::new(
            ErrorKind::DataInvalid,
            format!("partition field {} is not a primitive", field.id),
        ));
    };
    datum(r#type, literal).map(Some)
}

/// Builds a `Datum` of type `r#type` from a stored partition value. iceberg-rust uses
/// `Datum::new` for this, which isn't public, so this uses the public constructors.
fn datum(r#type: &PrimitiveType, literal: &PrimitiveLiteral) -> Result<Datum> {
    Ok(match (r#type, literal) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(value)) => Datum::bool(*value),
        (PrimitiveType::Int, PrimitiveLiteral::Int(value)) => Datum::int(*value),
        (PrimitiveType::Long, PrimitiveLiteral::Long(value)) => Datum::long(*value),
        (PrimitiveType::Float, PrimitiveLiteral::Float(value)) => Datum::float(value.0),
        (PrimitiveType::Double, PrimitiveLiteral::Double(value)) => Datum::double(value.0),
        (PrimitiveType::Date, PrimitiveLiteral::Int(value)) => Datum::date(*value),
        (PrimitiveType::Time, PrimitiveLiteral::Long(value)) => Datum::time_micros(*value)?,
        (PrimitiveType::Timestamp, PrimitiveLiteral::Long(value)) => {
            Datum::timestamp_micros(*value)
        }
        (PrimitiveType::Timestamptz, PrimitiveLiteral::Long(value)) => {
            Datum::timestamptz_micros(*value)
        }
        (PrimitiveType::TimestampNs, PrimitiveLiteral::Long(value)) => {
            Datum::timestamp_nanos(*value)
        }
        (PrimitiveType::TimestamptzNs, PrimitiveLiteral::Long(value)) => {
            Datum::timestamptz_nanos(*value)
        }
        (PrimitiveType::String, PrimitiveLiteral::String(value)) => Datum::string(value),
        (PrimitiveType::Binary, PrimitiveLiteral::Binary(value)) => {
            Datum::binary(value.iter().copied())
        }
        (PrimitiveType::Fixed(_), PrimitiveLiteral::Binary(value)) => {
            Datum::fixed(value.iter().copied())
        }
        // Decimal and uuid have no public constructor that takes the raw value, so these go
        // through their byte encoding.
        (PrimitiveType::Decimal { .. }, PrimitiveLiteral::Int128(value)) => {
            Datum::try_from_bytes(&value.to_be_bytes(), r#type.clone())?
        }
        (PrimitiveType::Uuid, PrimitiveLiteral::UInt128(value)) => {
            Datum::try_from_bytes(&value.to_be_bytes(), PrimitiveType::Uuid)?
        }
        // The stored value doesn't fit its column's type. This happens in files written before
        // a column was widened, for example from int to long. iceberg-rust builds the value
        // anyway. None of its comparisons then match, so it skips the file. The public
        // constructors can't build a value like that, so we return an error and don't skip
        // the file.
        _ => {
            return Err(Error::new(
                ErrorKind::DataInvalid,
                format!("can't read a {type} partition value"),
            ));
        }
    })
}

/// Returns whether `value` starts with `prefix`
fn starts_with(value: &Datum, prefix: &Datum) -> bool {
    match (value.literal(), prefix.literal()) {
        (PrimitiveLiteral::String(value), PrimitiveLiteral::String(prefix)) => {
            value.starts_with(prefix.as_str())
        }
        _ => false,
    }
}
