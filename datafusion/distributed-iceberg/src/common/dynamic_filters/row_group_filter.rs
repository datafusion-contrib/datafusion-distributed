use iceberg::expr::Predicate;

/// The maximum number of values we give iceberg-rust's `ArrowReader` across all the IN lists in the filter.
///
/// `ArrowReader` checks an IN list by comparing each row with every value in it, one value at a
/// time. So the more values there are, the slower every row gets. A join's filter can have
/// thousands of values. DataFusion keeps each IN list to at most 150 values, but a partitioned
/// join has one list per partition, and the filter combines them with OR.
///
/// When the filter has more values than this, we leave out all of its IN lists. `ArrowReader`
/// still gets the join's min and max values, so it can still skip row groups outside that range.
/// We use 200 because Iceberg's own row group checks ignore any IN list longer than that. This
/// limit can be removed once `ArrowReader` checks IN lists more optimally (eg. with a hash set)
const MAX_IN_LIST_VALUES: usize = 200;

/// Returns the dynamic filters to give iceberg-rust's `ArrowReader`. `ArrowReader` uses them to
/// skip row groups, and to drop the rows that don't match.
///
/// `predicate` is the dynamic filters, converted to Iceberg's filter type.
///
/// If the filters have too many IN list values, we leave out every IN list. See
/// [`MAX_IN_LIST_VALUES`].
pub(crate) fn reader_filter(predicate: &Predicate) -> Predicate {
    if count_in_list_values(predicate) <= MAX_IN_LIST_VALUES {
        return predicate.clone();
    }
    keep_filters(predicate, &|filter| !matches!(filter, Predicate::Set(_)))
}

/// Walks the whole filter and adds up the values in every IN and NOT IN list it finds.
fn count_in_list_values(predicate: &Predicate) -> usize {
    match predicate {
        Predicate::And(expr) | Predicate::Or(expr) => {
            let [left, right] = expr.inputs();
            count_in_list_values(left) + count_in_list_values(right)
        }
        Predicate::Not(expr) => {
            let [inner] = expr.inputs();
            count_in_list_values(inner)
        }
        Predicate::Set(expr) => expr.literals().len(),
        _ => 0,
    }
}

/// Returns a copy of `predicate` with only the filters that `keep` accepts. We replace every
/// other filter with "always true".
///
/// This is safe because it can only make `predicate` match more rows. So iceberg-rust's
/// `ArrowReader` may read some rows it didn't need to, but it never drops a row that matches.
///
/// A NOT is the exception. Inside a NOT, "always true" turns into "always false", which would
/// drop rows that match. So we replace the whole NOT with "always true". Our filter converter
/// never produces a NOT, so this doesn't come up in practice.
fn keep_filters(predicate: &Predicate, keep: &dyn Fn(&Predicate) -> bool) -> Predicate {
    match predicate {
        Predicate::And(expr) => {
            let [left, right] = expr.inputs();
            keep_filters(left, keep).and(keep_filters(right, keep))
        }
        Predicate::Or(expr) => {
            let [left, right] = expr.inputs();
            keep_filters(left, keep).or(keep_filters(right, keep))
        }
        Predicate::Not(_) => Predicate::AlwaysTrue,
        filter if keep(filter) => filter.clone(),
        _ => Predicate::AlwaysTrue,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::expr::Reference;
    use iceberg::spec::Datum;

    #[test]
    fn leaves_out_long_lists_and_keeps_their_bounds() {
        let values = (0..=MAX_IN_LIST_VALUES as i64).map(Datum::long);
        let predicate = id()
            .greater_than_or_equal_to(Datum::long(0))
            .and(id().less_than_or_equal_to(Datum::long(1000)))
            .and(id().is_in(values));

        insta::assert_snapshot!(reader_filter(&predicate), @"(id >= 0) AND (id <= 1000)");
    }

    #[test]
    fn leaves_out_a_not_around_a_long_list() {
        let values = (0..=MAX_IN_LIST_VALUES as i64).map(Datum::long);
        let predicate = id()
            .greater_than_or_equal_to(Datum::long(0))
            .and(!id().is_in(values));

        insta::assert_snapshot!(reader_filter(&predicate), @"id >= 0");
    }

    fn id() -> Reference {
        Reference::new("id")
    }
}
