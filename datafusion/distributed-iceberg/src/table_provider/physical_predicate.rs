use std::sync::Arc;

use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, LikeExpr, Literal, NotExpr,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType};

/// Turns a DataFusion filter into an iceberg predicate. The predicate matches every row that the
/// filter would match, and *can* match more if the DF expression cannot be translated into an Iceberg predicate completely.
/// Returns `None` when no part of the filter can be converted.
pub(crate) fn convert_physical_filter_to_predicate(
    filter: &Arc<dyn PhysicalExpr>,
) -> Option<Predicate> {
    convert(filter).map(|converted| converted.predicate)
}

/// An iceberg predicate that matches every row the DataFusion filter matches.
/// It can also match extra rows, so the caller must still apply the original filter.
struct Converted {
    predicate: Predicate,
    /// True if the predicate is false wherever the filter is false.
    /// Only an exact predicate can go under a NOT.
    exact: bool,
}

impl Converted {
    fn exact(predicate: Predicate) -> Self {
        Self {
            predicate,
            exact: true,
        }
    }

    fn wider(predicate: Predicate) -> Self {
        Self {
            predicate,
            exact: false,
        }
    }

    fn and(self, other: Self) -> Self {
        Self {
            predicate: self.predicate.and(other.predicate),
            exact: self.exact && other.exact,
        }
    }

    fn or(self, other: Self) -> Self {
        Self {
            predicate: self.predicate.or(other.predicate),
            exact: self.exact && other.exact,
        }
    }
}

fn convert(expr: &Arc<dyn PhysicalExpr>) -> Option<Converted> {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        return convert_binary(binary);
    }
    if let Some(not) = expr.downcast_ref::<NotExpr>() {
        return convert_not(not);
    }
    if let Some(case) = expr.downcast_ref::<CaseExpr>() {
        return convert_case(case);
    }
    if let Some(in_list) = expr.downcast_ref::<InListExpr>() {
        return convert_in_list(in_list);
    }
    if let Some(is_null) = expr.downcast_ref::<IsNullExpr>() {
        let column = column_name(is_null.arg())?;
        return Some(Converted::exact(Reference::new(column).is_null()));
    }
    if let Some(is_not_null) = expr.downcast_ref::<IsNotNullExpr>() {
        let column = column_name(is_not_null.arg())?;
        return Some(Converted::exact(Reference::new(column).is_not_null()));
    }
    if let Some(like) = expr.downcast_ref::<LikeExpr>() {
        return convert_like(like).map(Converted::exact);
    }
    if let Some(function) = expr.downcast_ref::<ScalarFunctionExpr>() {
        return convert_function(function).map(Converted::exact);
    }
    // A bare boolean column like `WHERE flag` becomes `flag = true`.
    if let Some(column) = expr.downcast_ref::<Column>() {
        let predicate = Reference::new(column.name()).equal_to(Datum::bool(true));
        return Some(Converted::exact(predicate));
    }
    // A bare `true`, `false` or NULL. NULL matches nothing.
    match literal_value(expr)? {
        ScalarValue::Boolean(Some(true)) => Some(Converted::exact(Predicate::AlwaysTrue)),
        ScalarValue::Boolean(_) | ScalarValue::Null => {
            Some(Converted::exact(Predicate::AlwaysFalse))
        }
        _ => None,
    }
}

fn convert_binary(binary: &BinaryExpr) -> Option<Converted> {
    let (left, right) = (binary.left(), binary.right());
    match binary.op() {
        Operator::And => both(convert(left), convert(right)),
        // An OR needs both sides. Turning `a = 1 OR f(b)` into `a = 1` would skip rows where
        // only `f(b)` is true.
        Operator::Or => Some(convert(left)?.or(convert(right)?)),
        op => compare(left, *op, right).map(Converted::exact),
    }
}

/// ANDs two converted sides. An AND can drop a side it can't convert:
/// `a = 1 AND f(b)` becomes `a = 1`. The result matches extra rows, which is fine.
fn both(left: Option<Converted>, right: Option<Converted>) -> Option<Converted> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.and(right)),
        (Some(side), None) | (None, Some(side)) => Some(Converted::wider(side.predicate)),
        (None, None) => None,
    }
}

/// A NOT needs an exact predicate. In `NOT (a = 1 AND f(b))`, the inside becomes `a = 1`,
/// and `NOT (a = 1)` would skip rows where `a = 1` but `f(b)` is false.
fn convert_not(not: &NotExpr) -> Option<Converted> {
    convert(not.arg())
        .filter(|inner| inner.exact)
        .map(|inner| Converted::exact(inner.predicate.negate()))
}

/// In a CASE, each WHEN picks which THEN decides if a row is kept. So every row the CASE
/// keeps passed some WHEN and its THEN, and this matches it:
/// `CASE WHEN c1 THEN x WHEN c2 THEN y ELSE z END` becomes `(c1 AND x) OR (c2 AND y) OR z`.
/// It ignores that the second branch only applies when `c1` is false, so it can match
/// extra rows. That's fine here, but a NOT over it could skip rows the filter keeps.
///
/// `CASE a WHEN 1 THEN x ...` works the same way, with `a = 1` as the WHEN.
///
/// Partitioned hash joins build filters like this, with one branch per partition:
/// `CASE hash(a) % 2 WHEN 0 THEN a IN (1, 2) WHEN 1 THEN a IN (3) ELSE false END`.
/// Iceberg can't compute the hash, so the WHEN conditions are dropped, leaving
/// `a IN (1, 2) OR a IN (3)`.
///
/// A missing ELSE means NULL, which matches nothing.
fn convert_case(case: &CaseExpr) -> Option<Converted> {
    let convert_when = |when| match case.expr() {
        Some(value) => compare(value, Operator::Eq, when).map(Converted::exact),
        None => convert(when),
    };
    let predicate = case
        .when_then_expr()
        .iter()
        .map(|(when, then)| both(convert_when(when), convert(then)))
        .chain(case.else_expr().map(convert))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        .map(|branch| branch.predicate)
        .reduce(Predicate::or)?;
    Some(Converted::wider(predicate))
}

/// Converts a column compared with a value, like `a < 5`.
/// The value can come first too: `5 > a` becomes `a < 5`.
fn compare(
    left: &Arc<dyn PhysicalExpr>,
    op: Operator,
    right: &Arc<dyn PhysicalExpr>,
) -> Option<Predicate> {
    let (column, op, datum) = match (column_name(left), literal_datum(right)) {
        (Some(column), Some(datum)) => (column, op, datum),
        _ => (column_name(right)?, op.swap()?, literal_datum(left)?),
    };
    let column = Reference::new(column);
    match op {
        Operator::Eq => Some(column.equal_to(datum)),
        Operator::NotEq => Some(column.not_equal_to(datum)),
        Operator::Lt => Some(column.less_than(datum)),
        Operator::LtEq => Some(column.less_than_or_equal_to(datum)),
        Operator::Gt => Some(column.greater_than(datum)),
        Operator::GtEq => Some(column.greater_than_or_equal_to(datum)),
        _ => None,
    }
}

fn convert_in_list(in_list: &InListExpr) -> Option<Converted> {
    // Something like `struct(a, b) IN (...)`, which checks several columns at once.
    let Some(column) = column_name(in_list.expr()) else {
        return convert_struct_in_list(in_list).map(Converted::wider);
    };
    let column = Reference::new(column);
    let values = in_list
        .list()
        .iter()
        .map(literal_value)
        .collect::<Option<Vec<_>>>()?;
    let (nulls, values): (Vec<_>, Vec<_>) = values.into_iter().partition(|value| value.is_null());
    let datums = values
        .into_iter()
        .map(scalar_value_to_datum)
        .collect::<Option<Vec<_>>>()?;
    let predicate = match (in_list.negated(), nulls.is_empty()) {
        // `x IN (1, NULL)` matches the same rows as `x IN (1)`.
        (false, _) => column.is_in(datums),
        (true, true) => column.is_not_in(datums),
        // `x NOT IN (1, NULL)` never matches anything. It's rare, so we don't convert it.
        (true, false) => return None,
    };
    Some(Converted::exact(predicate))
}

/// Hash joins on two or more columns build filters like `struct(a, b) IN ({1, 3}, {2, 4})`.
/// Iceberg can't check pairs, so each column is checked on its own:
/// `a IN (1, 2) AND b IN (3, 4)`. This also matches pairs like `{1, 4}`, which is fine.
/// A column with a NULL value is left out, because the join decides if NULL matches NULL.
fn convert_struct_in_list(in_list: &InListExpr) -> Option<Predicate> {
    if in_list.negated() {
        return None;
    }
    let columns = in_list
        .expr()
        .downcast_ref::<ScalarFunctionExpr>()
        .filter(|function| function.name() == "struct")?
        .args();
    let items = in_list
        .list()
        .iter()
        .map(|item| match literal_value(item)? {
            ScalarValue::Struct(item) => Some(item),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;
    columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let datums = items
                .iter()
                .map(|item| {
                    let value = ScalarValue::try_from_array(item.columns().get(index)?, 0).ok()?;
                    scalar_value_to_datum(&value)
                })
                .collect::<Option<Vec<_>>>()?;
            Some(Reference::new(column_name(column)?).is_in(datums))
        })
        .reduce(Predicate::and)
}

/// Only simple prefix patterns: `a LIKE 'abc%'` becomes `a STARTS WITH 'abc'`.
/// ILIKE can't be converted, because iceberg's STARTS WITH is case-sensitive.
fn convert_like(like: &LikeExpr) -> Option<Predicate> {
    if like.case_insensitive() {
        return None;
    }
    let column = Reference::new(column_name(like.expr())?);
    let prefix = string_literal(like.pattern())?.strip_suffix('%')?;
    // Any other `%` or `_`, or a `\`, means it's more than a simple prefix.
    if prefix.contains(['%', '_', '\\']) {
        return None;
    }
    let prefix = prefix_datum(prefix)?;
    Some(if like.negated() {
        column.not_starts_with(prefix)
    } else {
        column.starts_with(prefix)
    })
}

/// Functions are matched by name: `isnan(a)` and `starts_with(a, 'abc')`.
fn convert_function(function: &ScalarFunctionExpr) -> Option<Predicate> {
    match (function.name(), function.args()) {
        ("isnan", [arg]) => Some(Reference::new(column_name(arg)?).is_nan()),
        ("starts_with", [arg, prefix]) => {
            let prefix = prefix_datum(string_literal(prefix)?)?;
            Some(Reference::new(column_name(arg)?).starts_with(prefix))
        }
        _ => None,
    }
}

/// Iceberg counts bytes where it should count characters when it checks STARTS WITH against
/// a `truncate` partition. With `truncate(2)`, the prefix `é` is 2 bytes, so iceberg only
/// looks in the partition `é` and skips the partition `éa`, which also has matching rows.
/// ASCII text has one byte per character, so only ASCII prefixes are converted.
fn prefix_datum(prefix: &str) -> Option<Datum> {
    prefix.is_ascii().then(|| Datum::string(prefix))
}

fn column_name(expr: &Arc<dyn PhysicalExpr>) -> Option<&str> {
    expr.downcast_ref::<Column>().map(Column::name)
}

fn literal_value(expr: &Arc<dyn PhysicalExpr>) -> Option<&ScalarValue> {
    expr.downcast_ref::<Literal>().map(Literal::value)
}

fn literal_datum(expr: &Arc<dyn PhysicalExpr>) -> Option<Datum> {
    scalar_value_to_datum(literal_value(expr)?)
}

fn string_literal(expr: &Arc<dyn PhysicalExpr>) -> Option<&str> {
    literal_value(expr)?.try_as_str().flatten()
}

/// Converts a DataFusion value to an iceberg value, for the types iceberg columns use.
///
/// Floats aren't converted. DataFusion sorts NaN above every number and -0.0 below 0.0.
/// Iceberg doesn't, so a float filter like `a > 1.0` could skip rows DataFusion keeps.
///
/// Times aren't converted either, because iceberg-rust's file reader can't compare them yet.
fn scalar_value_to_datum(value: &ScalarValue) -> Option<Datum> {
    match value {
        ScalarValue::Boolean(Some(v)) => Some(Datum::bool(*v)),
        ScalarValue::Int32(Some(v)) => Some(Datum::int(*v)),
        ScalarValue::Int64(Some(v)) => Some(Datum::long(*v)),
        ScalarValue::Decimal128(Some(v), precision, scale) => {
            let data_type = PrimitiveType::Decimal {
                precision: u32::from(*precision),
                scale: u32::try_from(*scale).ok()?,
            };
            Datum::try_from_bytes(&v.to_be_bytes(), data_type).ok()
        }
        ScalarValue::Date32(Some(v)) => Some(Datum::date(*v)),
        ScalarValue::TimestampMicrosecond(Some(v), None) => Some(Datum::timestamp_micros(*v)),
        ScalarValue::TimestampMicrosecond(Some(v), Some(_)) => Some(Datum::timestamptz_micros(*v)),
        ScalarValue::TimestampNanosecond(Some(v), None) => Some(Datum::timestamp_nanos(*v)),
        ScalarValue::TimestampNanosecond(Some(v), Some(_)) => Some(Datum::timestamptz_nanos(*v)),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => Some(Datum::string(v)),
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v)) => Some(Datum::binary(v.clone())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Int64Array, StructArray};
    use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema};
    use datafusion::common::{DFSchema, Result};
    use datafusion::config::ConfigOptions;
    use datafusion::functions::core::r#struct;
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{
        BinaryExpr, Column, InListExpr, NotExpr, col, in_list, lit,
    };
    use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
    use datafusion::prelude::SessionContext;
    use datafusion::scalar::ScalarValue;
    use insta::assert_snapshot;

    use super::convert_physical_filter_to_predicate;

    // The filters below use the columns from `schema()`: `a` and `b` are integers, `s` is a
    // string, `f` is a float and `flag` is a boolean.

    #[test]
    fn converts_null_checks() {
        assert_snapshot!(convert_sql("a IS NULL"), @"a IS NULL");
        assert_snapshot!(convert_sql("a IS NOT NULL"), @"a IS NOT NULL");
    }

    #[test]
    fn converts_comparisons_with_a_literal() {
        assert_snapshot!(convert_sql("a = 5"), @"a = 5");
        assert_snapshot!(convert_sql("a != 5"), @"a != 5");
        assert_snapshot!(convert_sql("a < 5"), @"a < 5");
        assert_snapshot!(convert_sql("a <= 5"), @"a <= 5");
        assert_snapshot!(convert_sql("a > 5"), @"a > 5");
        assert_snapshot!(convert_sql("a >= 5"), @"a >= 5");
    }

    #[test]
    fn flips_comparisons_with_the_literal_on_the_left() {
        assert_snapshot!(convert_sql("5 < a"), @"a > 5");
        assert_snapshot!(convert_sql("5 >= a"), @"a <= 5");
    }

    #[test]
    fn leaves_comparisons_between_columns_unconverted() {
        assert_snapshot!(convert_sql("a < b"), @"None");
    }

    #[test]
    fn leaves_float_comparisons_unconverted() {
        assert_snapshot!(convert_sql("f > 1.5"), @"None");
        assert_snapshot!(convert_sql("a = 1 AND f > 1.5"), @"a = 1");
    }

    #[test]
    fn converts_in_lists() {
        assert_snapshot!(convert_sql("a IN (1, 2)"), @"a IN (1, 2)");
        assert_snapshot!(convert_sql("a NOT IN (1, 2)"), @"a NOT IN (1, 2)");
    }

    #[test]
    fn drops_nulls_from_an_in_list() -> Result<()> {
        // Built by hand, the way a hash join builds it. SQL would wrap the NULL in a cast.
        let schema = schema();
        let null = lit(ScalarValue::Int64(None));
        let values = vec![lit(1i64), Arc::clone(&null)];
        let one_or_null = in_list(col("a", &schema)?, values, &false, &schema)?;
        let only_null = in_list(col("a", &schema)?, vec![null], &false, &schema)?;
        assert_snapshot!(convert_filter(&one_or_null), @"a IN (1)");
        // `a IN (NULL)` matches nothing.
        assert_snapshot!(convert_filter(&only_null), @"a IN ()");
        Ok(())
    }

    #[test]
    fn leaves_not_in_with_a_null_unconverted() -> Result<()> {
        // Built by hand, the way a hash join builds it. SQL would wrap the NULL in a cast.
        let schema = schema();
        let values = vec![lit(1i64), lit(ScalarValue::Int64(None))];
        let filter = in_list(col("a", &schema)?, values, &true, &schema)?;
        assert_snapshot!(convert_filter(&filter), @"None");
        Ok(())
    }

    #[test]
    fn leaves_in_lists_with_a_column_item_unconverted() {
        assert_snapshot!(convert_sql("a IN (b, 1)"), @"None");
    }

    #[test]
    fn leaves_an_in_list_over_an_expression_unconverted() {
        assert_snapshot!(convert_sql("a + 1 IN (1, 2)"), @"None");
    }

    #[test]
    fn converts_like_prefixes() {
        assert_snapshot!(convert_sql("s LIKE 'ab%'"), @r#"s STARTS WITH "ab""#);
        assert_snapshot!(convert_sql("s NOT LIKE 'ab%'"), @r#"s NOT STARTS WITH "ab""#);
    }

    #[test]
    fn leaves_other_like_patterns_unconverted() {
        assert_snapshot!(convert_sql("s LIKE 'a_b%'"), @"None");
        assert_snapshot!(convert_sql("s LIKE 'a%b%'"), @"None");
        assert_snapshot!(convert_sql(r"s LIKE 'a\b%'"), @"None");
        assert_snapshot!(convert_sql("s LIKE 'ab'"), @"None");
        assert_snapshot!(convert_sql("s ILIKE 'ab%'"), @"None");
    }

    #[test]
    fn leaves_non_ascii_prefixes_unconverted() {
        assert_snapshot!(convert_sql("s LIKE 'éa%'"), @"None");
        assert_snapshot!(convert_sql("starts_with(s, 'éa')"), @"None");
    }

    #[test]
    fn converts_isnan_and_starts_with() {
        assert_snapshot!(convert_sql("isnan(f)"), @"f IS NAN");
        assert_snapshot!(convert_sql("starts_with(s, 'ab')"), @r#"s STARTS WITH "ab""#);
    }

    #[test]
    fn converts_a_boolean_column() {
        assert_snapshot!(convert_sql("flag"), @"flag = true");
    }

    #[test]
    fn converts_true_false_and_null() {
        assert_snapshot!(convert_sql("true"), @"TRUE");
        assert_snapshot!(convert_sql("false"), @"FALSE");
        // A NULL filter keeps no rows.
        assert_snapshot!(convert_sql("NULL"), @"FALSE");
    }

    #[test]
    fn converts_an_and() {
        assert_snapshot!(convert_sql("a = 1 AND b = 2"), @"(a = 1) AND (b = 2)");
    }

    #[test]
    fn drops_the_unconverted_side_of_an_and() {
        assert_snapshot!(convert_sql("a = 1 AND a < b"), @"a = 1");
    }

    #[test]
    fn converts_an_or_only_when_both_sides_convert() {
        // Hash joins build this when rows with a NULL key can still match.
        assert_snapshot!(
            convert_sql("a IS NULL OR a IN (1, 2)"),
            @"(a IS NULL) OR (a IN (1, 2))"
        );
        assert_snapshot!(convert_sql("a = 1 OR a < b"), @"None");
    }

    #[test]
    fn negates_exact_predicates() {
        assert_snapshot!(convert_sql("NOT (a < 5)"), @"a >= 5");
        assert_snapshot!(convert_sql("NOT (a IS NULL)"), @"a IS NOT NULL");
        // DataFusion turns `NOT (a IN (1, 2))` into `a NOT IN (1, 2)` itself, and does the
        // same for LIKE. Putting them inside an OR keeps the NOT.
        assert_snapshot!(
            convert_sql("NOT (a IN (1, 2) OR b = 2)"),
            @"(a NOT IN (1, 2)) AND (b != 2)"
        );
        assert_snapshot!(
            convert_sql("NOT (s LIKE 'ab%' OR b = 2)"),
            @r#"(s NOT STARTS WITH "ab") AND (b != 2)"#
        );
        assert_snapshot!(convert_sql("NOT starts_with(s, 'ab')"), @r#"s NOT STARTS WITH "ab""#);
        assert_snapshot!(convert_sql("NOT isnan(f)"), @"f IS NOT NAN");
        assert_snapshot!(convert_sql("NOT flag"), @"flag != true");
        assert_snapshot!(convert_sql("NOT (a = 1 AND b = 2)"), @"(a != 1) OR (b != 2)");
        assert_snapshot!(convert_sql("NOT (a = 1 OR b = 2)"), @"(a != 1) AND (b != 2)");
    }

    #[test]
    fn leaves_not_over_a_partly_converted_filter_unconverted() {
        assert_snapshot!(convert_sql("NOT (a = 1 AND a < b)"), @"None");
        assert_snapshot!(convert_sql("NOT (a = 1 AND (b = 2 AND a < b))"), @"None");
        assert_snapshot!(convert_sql("NOT (a = 1 OR (b = 2 AND a < b))"), @"None");
    }

    #[test]
    fn converts_a_case_to_each_when_and_its_then_ored_together() {
        assert_snapshot!(
            convert_sql("CASE WHEN a = 1 THEN b = 2 WHEN a = 2 THEN b = 3 ELSE b = 4 END"),
            @"(((a = 1) AND (b = 2)) OR ((a = 2) AND (b = 3))) OR (b = 4)"
        );
    }

    #[test]
    fn converts_case_value_whens_to_equalities() {
        assert_snapshot!(
            convert_sql("CASE a WHEN 1 THEN b = 2 WHEN 2 THEN b = 3 END"),
            @"((a = 1) AND (b = 2)) OR ((a = 2) AND (b = 3))"
        );
    }

    #[test]
    fn keeps_only_the_thens_when_the_whens_cant_be_converted() {
        // `a % 2` stands in for the `hash(a) % N` a partitioned hash join uses.
        assert_snapshot!(
            convert_sql("CASE a % 2 WHEN 0 THEN b IN (1, 2) WHEN 1 THEN b = 3 ELSE false END"),
            @"(b IN (1, 2)) OR (b = 3)"
        );
    }

    #[test]
    fn keeps_the_when_if_its_then_cant_be_converted() {
        assert_snapshot!(convert_sql("CASE WHEN a = 1 THEN a < b ELSE false END"), @"a = 1");
    }

    #[test]
    fn converts_a_case_with_an_always_true_branch_to_true() {
        // Hash joins use `true` for a partition whose build side was canceled.
        assert_snapshot!(convert_sql("CASE a % 2 WHEN 0 THEN b = 1 ELSE true END"), @"TRUE");
    }

    #[test]
    fn leaves_a_case_with_an_unconverted_branch_unconverted() {
        assert_snapshot!(
            convert_sql("CASE a % 2 WHEN 0 THEN b IN (1, 2) WHEN 1 THEN a < b ELSE false END"),
            @"None"
        );
        assert_snapshot!(convert_sql("CASE WHEN a = 1 THEN b = 2 ELSE a < b END"), @"None");
    }

    #[test]
    fn leaves_not_over_a_case_unconverted() {
        assert_snapshot!(convert_sql("NOT (CASE WHEN a = 1 THEN b = 2 ELSE false END)"), @"None");
    }

    #[test]
    fn converts_a_multi_column_join_filter_to_an_in_list_per_column() -> Result<()> {
        let filter = struct_in(vec![Some(1), Some(2)], vec![Some(5), Some(6)])?;
        assert_snapshot!(convert_filter(&filter), @"(a IN (1, 2)) AND (b IN (5, 6))");
        Ok(())
    }

    #[test]
    fn drops_join_filter_columns_with_a_null() -> Result<()> {
        let filter = struct_in(vec![Some(1), Some(2)], vec![Some(3), None])?;
        assert_snapshot!(convert_filter(&filter), @"a IN (1, 2)");
        Ok(())
    }

    #[test]
    fn leaves_a_multi_column_not_in_unconverted() -> Result<()> {
        let filter = struct_not_in(vec![Some(1), Some(2)], vec![Some(3), Some(4)])?;
        assert_snapshot!(convert_filter(&filter), @"None");
        Ok(())
    }

    #[test]
    fn leaves_not_over_a_multi_column_join_filter_unconverted() -> Result<()> {
        let filter = struct_in(vec![Some(1), Some(2)], vec![Some(3), Some(4)])?;
        let filter: Arc<dyn PhysicalExpr> = Arc::new(NotExpr::new(filter));
        assert_snapshot!(convert_filter(&filter), @"None");
        Ok(())
    }

    #[test]
    fn converts_literals_to_iceberg_values() {
        // Each value is compared with `a`. The translator doesn't check that the types match.
        let values = [
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Int32(Some(1)),
            ScalarValue::Int64(Some(1)),
            ScalarValue::Decimal128(Some(12345), 10, 2),
            ScalarValue::Decimal128(Some(-12345), 10, 2),
            ScalarValue::Date32(Some(19723)),
            ScalarValue::TimestampMicrosecond(Some(1_704_067_200_000_000), None),
            ScalarValue::TimestampMicrosecond(Some(1_704_067_200_000_000), Some("+00:00".into())),
            ScalarValue::TimestampNanosecond(Some(1_704_067_200_000_000_000), None),
            ScalarValue::TimestampNanosecond(
                Some(1_704_067_200_000_000_000),
                Some("+00:00".into()),
            ),
            ScalarValue::Utf8(Some("ab".into())),
            ScalarValue::LargeUtf8(Some("ab".into())),
            ScalarValue::Utf8View(Some("ab".into())),
            ScalarValue::Binary(Some(vec![1, 2])),
            ScalarValue::LargeBinary(Some(vec![1, 2])),
            ScalarValue::BinaryView(Some(vec![1, 2])),
            ScalarValue::Float32(Some(1.5)),
            ScalarValue::Float64(Some(1.5)),
            ScalarValue::Time64Microsecond(Some(3_600_000_000)),
            ScalarValue::Int64(None),
        ];
        let converted = values
            .into_iter()
            .map(|value| {
                let column = Arc::new(Column::new("a", 0));
                let filter: Arc<dyn PhysicalExpr> =
                    Arc::new(BinaryExpr::new(column, Operator::Eq, lit(value.clone())));
                format!("{value:?} => {}", convert_filter(&filter))
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert_snapshot!(converted, @r#"
        Boolean(true) => a = true
        Int32(1) => a = 1
        Int64(1) => a = 1
        Decimal128(123.45,10,2) => a = 123.45
        Decimal128(-123.45,10,2) => a = -123.45
        Date32("2024-01-01") => a = 2024-01-01
        TimestampMicrosecond(1704067200000000, None) => a = 2024-01-01 00:00:00
        TimestampMicrosecond(1704067200000000, Some("+00:00")) => a = 2024-01-01 00:00:00 UTC
        TimestampNanosecond(1704067200000000000, None) => a = 2024-01-01 00:00:00
        TimestampNanosecond(1704067200000000000, Some("+00:00")) => a = 2024-01-01 00:00:00 UTC
        Utf8("ab") => a = "ab"
        LargeUtf8("ab") => a = "ab"
        Utf8View("ab") => a = "ab"
        Binary("1,2") => a = 0102
        LargeBinary("1,2") => a = 0102
        BinaryView("1,2") => a = 0102
        Float32(1.5) => None
        Float64(1.5) => None
        Time64Microsecond("3600000000") => None
        Int64(NULL) => None
        "#);
    }

    fn schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
            Field::new("flag", DataType::Boolean, true),
        ])
    }

    /// Turns SQL like `a < 5` into a DataFusion filter over [`schema`], then converts it.
    /// DataFusion's simplify step is skipped, so `NOT (a < 5)` stays a NOT.
    fn convert_sql(filter: &str) -> String {
        let schema = DFSchema::try_from(schema()).unwrap();
        let ctx = SessionContext::new();
        let expr = ctx.parse_sql_expr(filter, &schema).unwrap();
        let expr = ctx.state().create_physical_expr(expr, &schema).unwrap();
        convert_filter(&expr)
    }

    /// Converts the filter and prints the predicate, or `None`.
    fn convert_filter(filter: &Arc<dyn PhysicalExpr>) -> String {
        match convert_physical_filter_to_predicate(filter) {
            Some(predicate) => predicate.to_string(),
            None => "None".to_string(),
        }
    }

    /// Builds the filter a hash join on `a` and `b` makes: `struct(a, b) IN (...)`.
    /// Each pair takes the values at the same position in `a` and `b`.
    fn struct_in(a: Vec<Option<i64>>, b: Vec<Option<i64>>) -> Result<Arc<dyn PhysicalExpr>> {
        struct_in_list(a, b, false)
    }

    /// Like [`struct_in`], but `NOT IN`. Hash joins never build this.
    fn struct_not_in(a: Vec<Option<i64>>, b: Vec<Option<i64>>) -> Result<Arc<dyn PhysicalExpr>> {
        struct_in_list(a, b, true)
    }

    fn struct_in_list(
        a: Vec<Option<i64>>,
        b: Vec<Option<i64>>,
        negated: bool,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let schema = schema();
        let fields = Fields::from(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);
        let columns = ScalarFunctionExpr::new(
            "struct",
            r#struct(),
            vec![col("a", &schema)?, col("b", &schema)?],
            Arc::new(Field::new("struct", DataType::Struct(fields.clone()), true)),
            Arc::new(ConfigOptions::default()),
        );
        let values = StructArray::new(
            fields,
            vec![
                Arc::new(Int64Array::from(a)) as ArrayRef,
                Arc::new(Int64Array::from(b)) as ArrayRef,
            ],
            None,
        );
        Ok(Arc::new(InListExpr::try_new_from_array(
            Arc::new(columns),
            Arc::new(values),
            negated,
            &schema,
        )?))
    }
}
