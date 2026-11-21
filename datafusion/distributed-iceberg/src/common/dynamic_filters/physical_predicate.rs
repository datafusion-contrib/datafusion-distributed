use std::sync::Arc;

use datafusion::arrow::array::StructArray;
use datafusion::functions::core::r#struct::StructFunc;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::expressions::{
    BinaryExpr, CaseExpr, Column, InListExpr, IsNotNullExpr, IsNullExpr, Literal,
};
use datafusion::physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use datafusion::scalar::ScalarValue;
use iceberg::expr::{Predicate, Reference};
use iceberg::spec::{Datum, PrimitiveType};

/// Turns a DataFusion filter into an iceberg predicate. The predicate matches every row that the
/// filter would match, and *can* match more if the DF expression cannot be translated into an
/// Iceberg predicate completely.
///
/// Returns `None` when no part of the filter can be converted.
pub(crate) fn convert_physical_filter_to_predicate(
    filter: &Arc<dyn PhysicalExpr>,
) -> Option<Predicate> {
    convert(filter)
}

/// Converts a given `PhysicalExpr` into an `iceberg::Predicate`
///
/// We only convert the expressions DataFusion's dynamic filters use: comparisons, IN lists,
/// IS NULL, IS NOT NULL, AND, OR and CASE.
///
/// Returns None if the expression could not be converted
fn convert(expr: &Arc<dyn PhysicalExpr>) -> Option<Predicate> {
    if let Some(binary) = expr.downcast_ref::<BinaryExpr>() {
        return convert_binary(binary);
    }
    if let Some(case) = expr.downcast_ref::<CaseExpr>() {
        return convert_case(case);
    }
    if let Some(in_list) = expr.downcast_ref::<InListExpr>() {
        return convert_in_list(in_list);
    }
    if let Some(is_null) = expr.downcast_ref::<IsNullExpr>() {
        let column = column_name(is_null.arg())?;
        return Some(Reference::new(column).is_null());
    }
    if let Some(is_not_null) = expr.downcast_ref::<IsNotNullExpr>() {
        let column = column_name(is_not_null.arg())?;
        return Some(Reference::new(column).is_not_null());
    }
    // A bare `true`, `false` or NULL. NULL matches nothing.
    match literal_value(expr)? {
        ScalarValue::Boolean(Some(true)) => Some(Predicate::AlwaysTrue),
        ScalarValue::Boolean(_) | ScalarValue::Null => Some(Predicate::AlwaysFalse),
        _ => None,
    }
}

/// Converts a binary expression into an `iceberg::Predicate`
fn convert_binary(binary: &BinaryExpr) -> Option<Predicate> {
    let (left, right) = (binary.left(), binary.right());
    match binary.op() {
        // An AND expression can be converted if *either* side can be converted
        // If one of the sides are dropped, iceberg would return extra rows
        // which would then be filtered out by DataFusion
        Operator::And => both(convert(left), convert(right)),
        // An OR expression can only be converted if *both* sides can be converted
        // We cannot drop one of the sides, as that would lead to data being skipped incorrectly
        Operator::Or => Some(convert(left)?.or(convert(right)?)),
        op => compare(left, *op, right),
    }
}

/// ANDs two converted sides. An AND can drop a side it can't convert:
/// `a = 1 AND f(b)` becomes `a = 1`. The result matches extra rows, which is fine.
fn both(left: Option<Predicate>, right: Option<Predicate>) -> Option<Predicate> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.and(right)),
        (Some(side), None) | (None, Some(side)) => Some(side),
        (None, None) => None,
    }
}

/// Converts a CaseExpr into an `iceberg::Predicate`
///
/// In a CASE statement, each WHEN/THEN pair applies a filter over the rows.
///
/// `CASE WHEN c1 THEN x WHEN c2 THEN y ELSE z END` becomes `(c1 AND x) OR (c2 AND y) OR z`.
/// It ignores that the second branch only applies when `c1` is false, so it can match
/// extra rows.
///
/// `CASE a WHEN 1 THEN x ...` works the same way, with `a = 1` as the WHEN.
///
/// A missing ELSE means NULL, which matches nothing.
fn convert_case(case: &CaseExpr) -> Option<Predicate> {
    // This handles the two variants for CASE
    // 1. CASE WHEN a = 1 THEN ...
    // In this variant, `case.expr()` is `None` because the expression is imbued in the WHEN clause.
    // We pass the WHEN clause to our general predicate converter for this.
    //
    // 2. CASE a WHEN 0 THEN ...
    // In this variant, `case.expr()` is `Some(a)`, and each WHEN clause compares its own
    // expression with this expression.
    // To handle this, we convert this into a comparison predicate.
    // Iceberg predicates can't always represent a given expression; if that is the case, we drop
    // the predicate for the WHEN clause.
    let convert_when = |when| match case.expr() {
        Some(value) => compare(value, Operator::Eq, when),
        None => convert(when),
    };

    case.when_then_expr()
        .iter()
        // Converts each individual WHEN/THEN pair into a conjunctive pair
        // WHEN x > 5 THEN y < 3 becomes (x > 5 AND y < 3)
        // If either side is dropped because it can't be converted, the filter is still valid
        // and will just match more rows
        .map(|(when, then)| both(convert_when(when), convert(then)))
        // If there is an ELSE at the end, convert it into a predicate directly
        .chain(case.else_expr().map(convert))
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        // Combines all predicates in a disjunction, i.e, OR
        // Multiple WHEN/THEN statements are joined together with ORs
        // E.g:
        // WHEN x > 5 THEN y ELSE
        // WHEN x > 10 THEN z ELSE ...
        // becomes
        // (x > 5 AND y) OR (x > 10 AND z)
        .reduce(Predicate::or)
}

/// Converts a column <-> value comparison into an `iceberg::Predicate`
fn compare(
    left: &Arc<dyn PhysicalExpr>,
    op: Operator,
    right: &Arc<dyn PhysicalExpr>,
) -> Option<Predicate> {
    // Try to match `<COLUMN> <op> <LITERAL>` first
    // If unsuccessful, swap the order of the two inputs and check again
    // If unsuccessful again, we can't convert this expression and we bail early
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

/// Converts an InList into an `iceberg::Predicate`
fn convert_in_list(in_list: &InListExpr) -> Option<Predicate> {
    // Dynamic filters only build IN lists, so we don't convert NOT IN.
    if in_list.negated() {
        return None;
    }

    // Something like `struct(a, b) IN (...)`, which checks several columns at once.
    let Some(column) = column_name(in_list.expr()) else {
        return convert_struct_in_list(in_list);
    };

    let literals = in_list
        .list()
        .iter()
        // We expect every value in the list to be a literal, i.e, x IN (1, 2, 3)
        // If it isn't, we can't convert this list, and bail early
        .map(literal_value)
        .collect::<Option<Vec<_>>>()?
        .into_iter()
        // NULL never equals anything, so `x IN (1, NULL)` matches the same rows as `x IN (1)`
        .filter(|value| !value.is_null())
        .map(scalar_value_to_datum)
        .collect::<Option<Vec<_>>>()?;

    Some(Reference::new(column).is_in(literals))
}

/// Converts a multi-column InList into an `iceberg::Predicate`
///
/// Hash joins on two or more columns build filters like `struct(a, b) IN ({1, 3}, {2, 4})`.
///
/// Iceberg can't check pairs, so each column is checked on its own:
/// `a IN (1, 2) AND b IN (3, 4)`. This can result in extra rows from  pairs like `{1, 4}`,
/// which is okay.
///
/// A column with a NULL value is left out, because the join decides if NULL matches NULL.
fn convert_struct_in_list(in_list: &InListExpr) -> Option<Predicate> {
    let columns =
        ScalarFunctionExpr::try_downcast_func::<StructFunc>(in_list.expr().as_ref())?.args();

    let items = in_list
        .list()
        .iter()
        // If any of the items in the list aren't struct literals, we can't convert this
        // and we bail early
        .map(|item| match literal_value(item)? {
            ScalarValue::Struct(item) => Some(item),
            _ => None,
        })
        .collect::<Option<Vec<_>>>()?;

    // One IN list per plain column. A column left out only makes the filter match more rows.
    columns
        .iter()
        .enumerate()
        .filter_map(|(index, column)| {
            let column = column_name(column)?;
            let values = field_values(&items, index)?;
            Some(Reference::new(column).is_in(values))
        })
        .reduce(Predicate::and)
}

/// Field `index` of each struct as an Iceberg value, or `None` if any of them can't be converted.
/// Each struct is a one-row array, so its value is in row 0.
fn field_values(items: &[&Arc<StructArray>], index: usize) -> Option<Vec<Datum>> {
    items
        .iter()
        .map(|item| {
            let field = item.columns().get(index)?;
            scalar_value_to_datum(&ScalarValue::try_from_array(field, 0).ok()?)
        })
        .collect()
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
        BinaryExpr, Column, InListExpr, col, in_list, lit,
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
    }

    #[test]
    fn leaves_not_in_lists_unconverted() {
        assert_snapshot!(convert_sql("a NOT IN (1, 2)"), @"None");
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
    fn leaves_in_lists_with_a_column_item_unconverted() {
        assert_snapshot!(convert_sql("a IN (b, 1)"), @"None");
    }

    #[test]
    fn leaves_an_in_list_over_an_expression_unconverted() {
        assert_snapshot!(convert_sql("a + 1 IN (1, 2)"), @"None");
    }

    #[test]
    fn leaves_filters_that_dynamic_filters_dont_use_unconverted() {
        assert_snapshot!(convert_sql("NOT (a < 5)"), @"None");
        assert_snapshot!(convert_sql("s LIKE 'ab%'"), @"None");
        assert_snapshot!(convert_sql("flag"), @"None");
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
            false,
            &schema,
        )?))
    }
}
