use datafusion::common::{HashMap, Result, internal_err};
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::BinaryExpr;
use std::sync::{Arc, Mutex};

/// Identifies all instances of the same dynamic filter within a query.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DynamicFilterId(pub(crate) u64);

/// Query-scoped collection of completed dynamic filter expressions received from producers.
///
/// The number of expected producers is deliberately not tracked here yet: under dynamic
/// planning it is not known until the producer stage has been finalized. Until then, [`Self::len`]
/// only reports how many expressions have arrived; it does not indicate completion.
pub(super) struct DynamicFilterStore {
    expressions: Mutex<HashMap<DynamicFilterId, Vec<Arc<dyn PhysicalExpr>>>>,
}

impl DynamicFilterStore {
    /// Creates an empty entry for every dynamic filter known to the query.
    pub(super) fn new(ids: impl IntoIterator<Item = DynamicFilterId>) -> Self {
        Self {
            expressions: Mutex::new(ids.into_iter().map(|id| (id, vec![])).collect()),
        }
    }

    /// Adds a completed producer expression and returns the number received for its filter.
    pub(super) fn add(
        &self,
        id: DynamicFilterId,
        expression: Arc<dyn PhysicalExpr>,
    ) -> Result<usize> {
        let mut expressions = self.expressions.lock().expect("poisoned lock");
        let Some(expressions) = expressions.get_mut(&id) else {
            return internal_err!("Unknown dynamic filter id: {}", id.0);
        };
        expressions.push(expression);
        Ok(expressions.len())
    }

    /// Returns the number of producer expressions received for a filter.
    pub(super) fn len(&self, id: DynamicFilterId) -> Result<usize> {
        let expressions = self.expressions.lock().expect("poisoned lock");
        let Some(expressions) = expressions.get(&id) else {
            return internal_err!("Unknown dynamic filter id: {}", id.0);
        };
        Ok(expressions.len())
    }

    /// Returns a snapshot of all expressions for `id`, combined with boolean OR.
    ///
    /// The merge is non-destructive: subsequent calls see the same expressions plus any that have
    /// arrived since the previous call. The expression tree is balanced to avoid creating a deep
    /// left- or right-associated tree when a filter has many producers.
    ///
    /// DataFusion may eventually merge dynamic filter expressions natively; see
    /// [apache/datafusion#23817](https://github.com/apache/datafusion/issues/23817).
    pub(super) fn merge(&self, id: DynamicFilterId) -> Result<Option<Arc<dyn PhysicalExpr>>> {
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

    const FILTER_ID: DynamicFilterId = DynamicFilterId(1);
    const OTHER_FILTER_ID: DynamicFilterId = DynamicFilterId(2);

    #[test]
    fn new_deduplicates_filter_ids() -> Result<()> {
        let store = DynamicFilterStore::new([FILTER_ID, FILTER_ID]);

        assert_eq!(store.len(FILTER_ID)?, 0);
        assert!(store.add(FILTER_ID, lit(true)).is_ok());
        assert_eq!(store.len(FILTER_ID)?, 1);
        Ok(())
    }

    #[test]
    fn unknown_filter_ids_are_rejected() {
        let store = DynamicFilterStore::new([FILTER_ID]);

        let add_error = store.add(OTHER_FILTER_ID, lit(true)).unwrap_err();
        let len_error = store.len(OTHER_FILTER_ID).unwrap_err();
        let merge_error = store.merge(OTHER_FILTER_ID).unwrap_err();

        assert!(
            add_error
                .to_string()
                .contains("Unknown dynamic filter id: 2")
        );
        assert!(
            len_error
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

    #[test]
    fn merge_is_non_destructive_and_includes_later_additions() -> Result<()> {
        let store = DynamicFilterStore::new([FILTER_ID]);
        store.add(FILTER_ID, lit(true))?;

        assert_eq!(store.merge(FILTER_ID)?.unwrap().to_string(), "true");
        assert_eq!(store.len(FILTER_ID)?, 1);

        store.add(FILTER_ID, lit(false))?;

        assert_eq!(
            store.merge(FILTER_ID)?.unwrap().to_string(),
            "true OR false"
        );
        assert_eq!(store.len(FILTER_ID)?, 2);
        Ok(())
    }

    #[test]
    fn concurrent_adds_are_counted() -> Result<()> {
        let store = Arc::new(DynamicFilterStore::new([FILTER_ID]));
        let threads = (0..8)
            .map(|_| {
                let store = Arc::clone(&store);
                std::thread::spawn(move || store.add(FILTER_ID, lit(true)))
            })
            .collect::<Vec<_>>();

        for thread in threads {
            thread.join().unwrap()?;
        }

        assert_eq!(store.len(FILTER_ID)?, 8);
        assert!(store.merge(FILTER_ID)?.is_some());
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
