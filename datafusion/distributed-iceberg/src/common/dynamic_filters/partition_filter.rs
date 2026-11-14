//! A copy of iceberg-rust's `PartitionFilter`, which the iceberg-rust version this crate uses
//! doesn't have. It has the same API, so moving to iceberg-rust's version means deleting this
//! file and changing the import.
//!
//! The projection mirrors iceberg-rust's `InclusiveProjection`, and the evaluation mirrors its
//! `ExpressionEvaluator`. Both are private in this version.

use std::cmp::Ordering;
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
        // `PartitionFilter::try_new` rewrites every NOT away first, so none reach here. Keep
        // the file if one shows up anyway.
        BoundPredicate::Not(_) => Predicate::AlwaysTrue,
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

/// Evaluates a predicate bound to the partition schema against one partition value. Null values
/// follow iceberg-rust's `ExpressionEvaluator`.
fn evaluate(predicate: &BoundPredicate, spec: &PartitionSpec, partition: &Struct) -> Result<bool> {
    match predicate {
        BoundPredicate::AlwaysTrue => Ok(true),
        BoundPredicate::AlwaysFalse => Ok(false),
        BoundPredicate::And(expr) => {
            let [left, right] = expr.inputs();
            Ok(evaluate(left, spec, partition)? && evaluate(right, spec, partition)?)
        }
        BoundPredicate::Or(expr) => {
            let [left, right] = expr.inputs();
            Ok(evaluate(left, spec, partition)? || evaluate(right, spec, partition)?)
        }
        // A projection never makes a NOT. Keep the file if one shows up anyway.
        BoundPredicate::Not(_) => Ok(true),
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
                PredicateOperator::StartsWith => starts_with(&value, literal)?,
                PredicateOperator::NotStartsWith => !starts_with(&value, literal)?,
                PredicateOperator::LessThan => compare(&value, literal)?.is_lt(),
                PredicateOperator::LessThanOrEq => compare(&value, literal)?.is_le(),
                PredicateOperator::GreaterThan => compare(&value, literal)?.is_gt(),
                PredicateOperator::GreaterThanOrEq => compare(&value, literal)?.is_ge(),
                PredicateOperator::Eq => compare(&value, literal)?.is_eq(),
                PredicateOperator::NotEq => compare(&value, literal)?.is_ne(),
                _ => true,
            })
        }
        BoundPredicate::Set(expr) => {
            let Some(value) = partition_value(expr.term(), spec, partition)? else {
                return Ok(matches!(expr.op(), PredicateOperator::NotIn));
            };
            // Binding gives every value in the set the column's type, so checking one is enough.
            if let Some(literal) = expr.literals().iter().next()
                && literal.data_type() != value.data_type()
            {
                return Err(type_mismatch(&value, literal));
            }
            let contains = expr.literals().contains(&value);
            Ok(match expr.op() {
                PredicateOperator::In => contains,
                PredicateOperator::NotIn => !contains,
                _ => true,
            })
        }
    }
}

/// The partition value for a reference bound to the partition schema, with the partition
/// column's type. `None` is a null value.
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
        .ok_or_else(|| invalid(format!("partition field {} is not in the spec", field.id)))?;
    let Some(value) = partition.fields().get(position) else {
        return Err(invalid(format!("the partition has no value at {position}")));
    };
    let Some(value) = value else {
        return Ok(None);
    };
    let (Literal::Primitive(literal), Type::Primitive(r#type)) = (value, field.field_type.as_ref())
    else {
        return Err(invalid(format!(
            "partition field {} is not a primitive",
            field.id
        )));
    };
    datum(r#type, literal).map(Some)
}

/// Builds a `Datum` of type `r#type`. Also widens values written before a column's type was
/// promoted, such as an int partition value of a column that is now a long.
fn datum(r#type: &PrimitiveType, literal: &PrimitiveLiteral) -> Result<Datum> {
    Ok(match (r#type, literal) {
        (PrimitiveType::Boolean, PrimitiveLiteral::Boolean(value)) => Datum::bool(*value),
        (PrimitiveType::Int, PrimitiveLiteral::Int(value)) => Datum::int(*value),
        (PrimitiveType::Long, PrimitiveLiteral::Int(value)) => Datum::long(*value),
        (PrimitiveType::Long, PrimitiveLiteral::Long(value)) => Datum::long(*value),
        (PrimitiveType::Float, PrimitiveLiteral::Float(value)) => Datum::float(value.0),
        (PrimitiveType::Double, PrimitiveLiteral::Float(value)) => Datum::double(value.0),
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
        // A value that doesn't fit its column's type. Keep the file.
        _ => return Err(invalid(format!("can't read a {type} partition value"))),
    })
}

/// Orders two values of the same type. Values of different types are an error, so the file is
/// kept.
fn compare(value: &Datum, literal: &Datum) -> Result<Ordering> {
    if value.data_type() != literal.data_type() {
        return Err(type_mismatch(value, literal));
    }
    value
        .partial_cmp(literal)
        .ok_or_else(|| type_mismatch(value, literal))
}

fn starts_with(value: &Datum, prefix: &Datum) -> Result<bool> {
    match (value.literal(), prefix.literal()) {
        (PrimitiveLiteral::String(value), PrimitiveLiteral::String(prefix)) => {
            Ok(value.starts_with(prefix.as_str()))
        }
        _ => Err(type_mismatch(value, prefix)),
    }
}

fn type_mismatch(value: &Datum, literal: &Datum) -> Error {
    invalid(format!(
        "can't compare a {} partition value with a {} value",
        value.data_type(),
        literal.data_type()
    ))
}

fn invalid(message: String) -> Error {
    Error::new(ErrorKind::DataInvalid, message)
}
