use std::cmp::Ordering;
use std::sync::Arc;

use datafusion::common::HashMap;
use datafusion::physical_expr::{PhysicalExpr, conjunction};
use datafusion::physical_expr_common::physical_expr::{
    snapshot_generation, snapshot_physical_expr,
};
use iceberg::expr::{Bind, BoundPredicate, BoundReference, Predicate, PredicateOperator};
use iceberg::scan::FileScanTask;
use iceberg::spec::{
    Datum, Literal, PartitionSpec, PrimitiveLiteral, PrimitiveType, Schema, Struct, Type,
};
use iceberg::{Error, ErrorKind};

use crate::table_provider::physical_predicate::convert_physical_filter_to_predicate;

/// Skips files whose partition value can't match the scan's dynamic filters.
pub(crate) struct PartitionSkipper {
    /// The dynamic filters that will be used to evaluate whether a task can be skipped based on it's partition keys
    dynamic_filters: Arc<dyn PhysicalExpr>,

    /// The generation of `dynamic_filters` that `predicate` was built from.
    cached_snapshot_generation: u64,

    /// The filters as an Iceberg predicate bound to the table schema.
    predicate: Option<BoundPredicate>,

    /// `predicate` projected onto each partition spec and bound to its partition schema, by spec
    /// id. `None` keeps every file written with that spec. Cleared when `predicate` changes.
    partition_predicates: HashMap<i32, Option<BoundPredicate>>,
}

impl PartitionSkipper {
    pub(crate) fn new(dynamic_filters: Vec<Arc<dyn PhysicalExpr>>) -> Self {
        Self {
            dynamic_filters: conjunction(dynamic_filters),
            cached_snapshot_generation: 0,
            predicate: None,
            partition_predicates: HashMap::new(),
        }
    }

    /// Whether no row in the task's partition can match the filters. Any error keeps the file.
    pub(crate) fn should_skip_task(&mut self, task: &FileScanTask) -> bool {
        self.refresh(task);
        let Some(predicate) = &self.predicate else {
            return false;
        };
        let (Some(spec), Some(partition)) = (task.partition_spec(), task.partition()) else {
            return false;
        };
        if spec.is_unpartitioned() {
            return false;
        }
        // Every task in a scan has the same table schema, so the spec id is enough as a key.
        let partition_predicate = self
            .partition_predicates
            .entry(spec.spec_id())
            .or_insert_with(|| project_onto_spec(predicate, spec, task.schema()).ok());
        let Some(partition_predicate) = partition_predicate else {
            return false;
        };
        !evaluate(partition_predicate, spec, partition).unwrap_or(true)
    }

    /// Rebuilds the predicate when any of the filters has changed.
    fn refresh(&mut self, task: &FileScanTask) {
        // Read the generation before taking the snapshot. If a filter changes in between, the
        // next file sees a newer generation and rebuilds.
        let generation = snapshot_generation(&self.dynamic_filters);
        if self.cached_snapshot_generation == generation {
            return;
        }
        self.cached_snapshot_generation = generation;
        self.partition_predicates.clear();
        self.predicate = snapshot_physical_expr(Arc::clone(&self.dynamic_filters))
            .ok()
            .and_then(|snapshot| convert_physical_filter_to_predicate(&snapshot))
            .and_then(|predicate| {
                predicate
                    .bind(task.schema_ref(), task.case_sensitive())
                    .ok()
            });
    }
}

/// Projects a predicate on table columns onto a partition spec, then binds it to the spec's
/// partition schema. Binding gives its values the partition columns' types, so they compare
/// with the partition values.
fn project_onto_spec(
    predicate: &BoundPredicate,
    spec: &PartitionSpec,
    table_schema: &Schema,
) -> iceberg::Result<BoundPredicate> {
    let partition_type = spec.partition_type(table_schema)?;
    let partition_schema = Schema::builder()
        .with_fields(partition_type.fields().iter().cloned())
        .build()?;
    project(predicate, spec)?.bind(Arc::new(partition_schema), true)
}

/// Rewrites a predicate on table columns into one on the spec's partition columns. The result
/// matches every partition that holds a matching row, and can match more.
///
/// Mirrors iceberg-rust's `InclusiveProjection`, which is private.
fn project(predicate: &BoundPredicate, spec: &PartitionSpec) -> iceberg::Result<Predicate> {
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
        // `refresh` removes every NOT before binding. Keep the file if one shows up anyway.
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
) -> iceberg::Result<Predicate> {
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
fn evaluate(
    predicate: &BoundPredicate,
    spec: &PartitionSpec,
    partition: &Struct,
) -> iceberg::Result<bool> {
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
            if let Some(literal) = expr
                .literals()
                .iter()
                .find(|literal| literal.data_type() != value.data_type())
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
) -> iceberg::Result<Option<Datum>> {
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
fn datum(r#type: &PrimitiveType, literal: &PrimitiveLiteral) -> iceberg::Result<Datum> {
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
fn compare(value: &Datum, literal: &Datum) -> iceberg::Result<Ordering> {
    if value.data_type() != literal.data_type() {
        return Err(type_mismatch(value, literal));
    }
    value
        .partial_cmp(literal)
        .ok_or_else(|| type_mismatch(value, literal))
}

fn starts_with(value: &Datum, prefix: &Datum) -> iceberg::Result<bool> {
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
