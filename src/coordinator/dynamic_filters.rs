use datafusion::common::{HashMap, Result, internal_err};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::BinaryExpr;
use std::sync::{Arc, Mutex};

/// Identifies all instances of the same dynamic filter within a query.
///
/// Equivalent to [`PhysicalExpr::expression_id`].
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct ExpressionId(pub(crate) u64);

/// Collection of completed dynamic filter expressions received from producers.
pub(super) struct DynamicFilterStore {
    expressions: Mutex<HashMap<ExpressionId, Vec<Arc<dyn PhysicalExpr>>>>,
}

impl DynamicFilterStore {
    /// Creates an empty entry for every dynamic filter known to the query.
    pub(super) fn new(ids: impl IntoIterator<Item = ExpressionId>) -> Self {
        Self {
            expressions: Mutex::new(ids.into_iter().map(|id| (id, vec![])).collect()),
        }
    }

    /// Adds an expression and returns the number received so far for the given id.
    pub(super) fn add(&self, id: ExpressionId, expression: Arc<dyn PhysicalExpr>) -> Result<usize> {
        let mut expressions = self.expressions.lock().expect("poisoned lock");
        let Some(expressions) = expressions.get_mut(&id) else {
            return internal_err!("Unknown dynamic filter id: {}", id.0);
        };
        expressions.push(expression);
        Ok(expressions.len())
    }

    /// Returns the number of producer expressions received for a given id.
    pub(super) fn count(&self, id: ExpressionId) -> Result<usize> {
        let expressions = self.expressions.lock().expect("poisoned lock");
        let Some(expressions) = expressions.get(&id) else {
            return internal_err!("Unknown dynamic filter id: {}", id.0);
        };
        Ok(expressions.len())
    }

    /// Returns a snapshot of all expressions for `id`, combined with an OR binary expression.
    ///
    /// DataFusion may eventually merge dynamic filter expressions natively. See
    /// [apache/datafusion#23817](https://github.com/apache/datafusion/issues/23817).
    pub(super) fn merge(&self, id: ExpressionId) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let expressions = {
            let all_expressions = self.expressions.lock().expect("poisoned lock");
            let Some(expressions) = all_expressions.get(&id) else {
                return internal_err!("Unknown dynamic filter id: {}", id.0);
            };
            expressions.clone()
        };

        Ok(merge_with_or(&expressions))
    }
}

/// Merges the provided expressions by ORing them together. The expression tree is balanced
/// to avoid creating a deep left- or right-associated tree.
fn merge_with_or(expressions: &[Arc<dyn PhysicalExpr>]) -> Option<Arc<dyn PhysicalExpr>> {
    match expressions {
        [] => None,
        [expression] => Some(Arc::clone(expression)),
        expressions => {
            let middle = expressions.len() / 2;
            let left = merge_with_or(&expressions[..middle])?;
            let right = merge_with_or(&expressions[middle..])?;
            Some(Arc::new(BinaryExpr::new(left, Operator::Or, right)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_expr::expressions::{CaseExpr, Column, lit};

    const FILTER_ID: ExpressionId = ExpressionId(1);
    const OTHER_FILTER_ID: ExpressionId = ExpressionId(2);

    #[test]
    fn new_deduplicates_filter_ids() -> Result<()> {
        let store = DynamicFilterStore::new([FILTER_ID, FILTER_ID]);

        assert_eq!(store.count(FILTER_ID)?, 0);
        assert!(store.add(FILTER_ID, lit(true)).is_ok());
        assert_eq!(store.count(FILTER_ID)?, 1);
        Ok(())
    }

    #[test]
    fn unknown_filter_ids_are_rejected() {
        let store = DynamicFilterStore::new([FILTER_ID]);

        let add_error = store.add(OTHER_FILTER_ID, lit(true)).unwrap_err();
        let count_error = store.count(OTHER_FILTER_ID).unwrap_err();
        let merge_error = store.merge(OTHER_FILTER_ID).unwrap_err();

        assert!(
            add_error
                .to_string()
                .contains("Unknown dynamic filter id: 2")
        );
        assert!(
            count_error
                .to_string()
                .contains("Unknown dynamic filter id: 2")
        );
        assert!(
            merge_error
                .to_string()
                .contains("Unknown dynamic filter id: 2")
        );
    }

    #[test]
    fn merge_empty_filter_returns_none() -> Result<()> {
        let store = DynamicFilterStore::new([FILTER_ID]);

        assert!(store.merge(FILTER_ID)?.is_none());
        Ok(())
    }

    #[test]
    fn merge_single_filter_preserves_expression() -> Result<()> {
        let store = DynamicFilterStore::new([FILTER_ID]);
        let expression = lit(true);
        store.add(FILTER_ID, Arc::clone(&expression))?;

        let merged = store.merge(FILTER_ID)?.unwrap();

        assert!(Arc::ptr_eq(&expression, &merged));
        Ok(())
    }

    #[test]
    fn merge_multiple_filters_is_balanced_and_deterministic() -> Result<()> {
        let store = DynamicFilterStore::new([FILTER_ID]);
        for value in [true, false, true, false] {
            store.add(FILTER_ID, lit(value))?;
        }

        let merged = store.merge(FILTER_ID)?.unwrap();

        assert_eq!(merged.to_string(), "true OR false OR true OR false");
        let root = merged.downcast_ref::<BinaryExpr>().unwrap();
        assert!(root.left().is::<BinaryExpr>());
        assert!(root.right().is::<BinaryExpr>());
        Ok(())
    }

    #[test]
    fn merge_ors_case_expressions() -> Result<()> {
        let store = DynamicFilterStore::new([FILTER_ID]);
        store.add(FILTER_ID, task_case(0)?)?;
        store.add(FILTER_ID, task_case(1)?)?;

        let merged = store.merge(FILTER_ID)?.unwrap();

        assert_eq!(
            merged.to_string(),
            "CASE WHEN task@0 = 0 THEN true ELSE false END OR CASE WHEN task@0 = 1 THEN true ELSE false END"
        );
        Ok(())
    }

    fn task_case(task: i32) -> Result<Arc<dyn PhysicalExpr>> {
        let matches_task = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("task", 0)),
            Operator::Eq,
            lit(task),
        ));
        Ok(Arc::new(CaseExpr::try_new(
            None,
            vec![(matches_task, lit(true))],
            Some(lit(false)),
        )?))
    }
}
