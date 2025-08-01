use datafusion::common::tree_node::{
    Transformed, TreeNode, TreeNodeContainer, TreeNodeRecursion, TreeNodeVisitor,
};
use datafusion::error::Result;

use super::stage::ExecutionStage;

impl<'a> TreeNodeContainer<'a, Self> for ExecutionStage {
    fn apply_elements<F: FnMut(&'a Self) -> Result<TreeNodeRecursion>>(
        &'a self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        f(self)
    }

    fn map_elements<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        f(self)
    }
}

impl TreeNode for ExecutionStage {
    fn apply_children<'n, F: FnMut(&'n Self) -> Result<TreeNodeRecursion>>(
        &'n self,
        f: F,
    ) -> Result<TreeNodeRecursion> {
        self.inputs.apply_elements(f)
    }

    fn map_children<F: FnMut(Self) -> Result<Transformed<Self>>>(
        self,
        f: F,
    ) -> Result<Transformed<Self>> {
        self.inputs
            .map_elements(f)?
            .map_data(|data| Ok(ExecutionStage::new(self.num, self.plan, data)))
    }
}

mod tests {
    use std::sync::Arc;

    use crate::stage::ExecutionStage;

    use datafusion::{
        arrow::datatypes::Schema,
        common::tree_node::{TreeNode, TreeNodeRecursion},
        physical_plan::empty::EmptyExec,
    };

    fn build_stage() -> ExecutionStage {
        let schema = Arc::new(Schema::empty());
        let empty = Arc::new(EmptyExec::new(schema.clone()));
        let stage0 = Arc::new(ExecutionStage::new(0, empty.clone(), vec![]));
        let stage1 = Arc::new(ExecutionStage::new(1, empty.clone(), vec![]));
        let stage2 = Arc::new(ExecutionStage::new(2, empty.clone(), vec![stage0, stage1]));
        ExecutionStage::new(3, empty, vec![stage2])
    }

    #[test]
    fn test_traversal() {
        let task = build_stage();
        let mut visited = vec![];
        task.apply(|node| {
            visited.push(node.clone());
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();

        assert_eq!(visited.len(), 4);
        assert!(matches!(visited[0], ExecutionStage { .. }));
        assert!(matches!(visited[1], ExecutionStage { .. }));
        assert!(matches!(visited[2], ExecutionStage { .. }));
        assert!(matches!(visited[3], ExecutionStage { .. }));
    }
}
