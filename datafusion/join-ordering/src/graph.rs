use std::sync::Arc;

use crate::common::get_join;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{JoinType, Result, plan_err};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::repartition::RepartitionExec;

#[derive(Copy, Clone)]
pub(crate) struct Rel(pub(crate) u64);

impl Rel {
    pub(crate) fn first() -> Self {
        Self(1)
    }

    pub(crate) fn next(self) -> Self {
        Self(self.0 << 1)
    }
}

#[derive(Copy, Clone, Default)]
pub(crate) struct RelSet(pub(crate) u64);

impl RelSet {
    pub(crate) fn add_rel(&mut self, rel: &Rel) {
        self.0 |= rel.0;
    }
}

pub(crate) struct RelationVertex {
    pub(crate) rel: Rel,

    // the arbitrary plan to which this vertex points to
    pub(crate) plan: Arc<dyn ExecutionPlan>,
}

pub(crate) struct JoinHyperEdge {
    pub(crate) join_type: JoinType,
    pub(crate) ses: RelSet,

    // The join node to which this hyper edge points to.
    pub(crate) plan: Arc<dyn ExecutionPlan>,
}

pub(crate) struct Graph {
    pub(crate) vertices: Vec<RelationVertex>,
    pub(crate) hyper_edges: Vec<JoinHyperEdge>,
}

pub(crate) fn parse_graph(plan: &Arc<dyn ExecutionPlan>) -> Result<Option<Graph>> {
    if get_join(plan).is_none() {
        return Ok(None);
    }

    let mut graph_parsing_context = GraphParsingContext {
        next_rel: Rel::first(),
        vertices: vec![],
        hyper_edges: vec![],
    };
    _parse_graph(plan, &mut graph_parsing_context)?;
    Ok(Some(graph_parsing_context.into_graph()))
}

fn _parse_graph(
    plan: &Arc<dyn ExecutionPlan>,
    ctx: &mut GraphParsingContext,
) -> Result<RelationScope> {
    if let Some(join) = get_join(plan) {
        let left_scope = _parse_graph(join.left(), ctx)?;
        let right_scope = _parse_graph(join.right(), ctx)?;

        let mut ses = RelSet::default();
        for (left_expr, right_expr) in join.exprs() {
            left_expr.apply(|expr| {
                let Some(col) = expr.downcast_ref::<Column>() else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                let Some(rel_id) = left_scope.col_idx_to_rel_id.get(col.index()) else {
                    return plan_err!(
                        "Invalid column index {} for a left scope with length {}",
                        col.index(),
                        left_scope.col_idx_to_rel_id.len()
                    );
                };
                ses.add_rel(rel_id);
                Ok(TreeNodeRecursion::Continue)
            })?;

            right_expr.apply(|expr| {
                let Some(col) = expr.downcast_ref::<Column>() else {
                    return Ok(TreeNodeRecursion::Continue);
                };
                let Some(rel_id) = right_scope.col_idx_to_rel_id.get(col.index()) else {
                    return plan_err!(
                        "Invalid column index {} for a right scope with length {}",
                        col.index(),
                        right_scope.col_idx_to_rel_id.len()
                    );
                };
                ses.add_rel(rel_id);
                Ok(TreeNodeRecursion::Continue)
            })?;
        }

        let mut unprojected_columns = vec![];
        match join.join_type() {
            // produces columns from both sides
            JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
                unprojected_columns.extend(left_scope.col_idx_to_rel_id);
                unprojected_columns.extend(right_scope.col_idx_to_rel_id);
            }
            // produces columns only from the left side
            // TODO: Mark joins add an extra column that does not point to any relation, so is it
            //  fine to just omit it here?
            JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
                unprojected_columns.extend(left_scope.col_idx_to_rel_id);
            }
            // produces columns only from the right side
            // TODO: Mark joins add an extra column that does not point to any relation, so is it
            //  fine to just omit it here?
            JoinType::RightSemi | JoinType::RightAnti | JoinType::RightMark => {
                unprojected_columns.extend(right_scope.col_idx_to_rel_id);
            }
        };

        let edge = JoinHyperEdge {
            join_type: join.join_type(),
            ses,
            // TODO: can we avoid cloning Arcs here?
            plan: Arc::clone(plan),
        };
        ctx.hyper_edges.push(edge);

        Ok(if let Some(projection) = join.projection() {
            let mut projected_columns = Vec::with_capacity(projection.len());
            for projection_idx in projection {
                if let Some(col) = unprojected_columns.get(*projection_idx) {
                    projected_columns.push(*col)
                }
            }
            RelationScope {
                col_idx_to_rel_id: projected_columns,
            }
        } else {
            RelationScope {
                col_idx_to_rel_id: unprojected_columns,
            }
        })
    } else if let Some(r_exec) = plan.downcast_ref::<RepartitionExec>() {
        _parse_graph(r_exec.input(), ctx)
    } else if let Some(cp_exec) = plan.downcast_ref::<CoalescePartitionsExec>() {
        _parse_graph(cp_exec.input(), ctx)
    } else {
        let vertex = RelationVertex {
            rel: ctx.next_rel(),
            plan: Arc::clone(plan),
        };
        let n_cols = plan.schema().fields().len();

        let scope = RelationScope {
            col_idx_to_rel_id: vec![vertex.rel; n_cols],
        };
        ctx.vertices.push(vertex);
        Ok(scope)
    }
}

struct GraphParsingContext {
    next_rel: Rel,
    vertices: Vec<RelationVertex>,
    hyper_edges: Vec<JoinHyperEdge>,
}

impl GraphParsingContext {
    fn next_rel(&mut self) -> Rel {
        let current = self.next_rel;
        self.next_rel = self.next_rel.next();
        current
    }

    fn into_graph(self) -> Graph {
        Graph {
            vertices: self.vertices,
            hyper_edges: self.hyper_edges,
        }
    }
}

struct RelationScope {
    // Map from column index to the [BitMask] that identifies the relation to which the column
    // ultimately belongs to.
    col_idx_to_rel_id: Vec<Rel>,
}

#[cfg(test)]
mod tests {
    use crate::assert_snapshot;
    use crate::test_utils::TestPlanBuilder;

    #[tokio::test]
    async fn three_way_inner_join_chain() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r"SELECT c_name, o_orderdate, l_quantity
                 FROM customer
                 JOIN orders ON c_custkey = o_custkey
                 JOIN lineitem ON o_orderkey = l_orderkey",
            )
            .await;

        assert_snapshot!(plan.display(), @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@1, l_orderkey@0)], projection=[c_name@0, o_orderdate@2, l_quantity@4]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c_custkey@0, o_custkey@1)], projection=[c_name@1, o_orderkey@2, o_orderdate@4]
            DataSourceExec: projection=[c_custkey, c_name], file_type=parquet
            DataSourceExec: projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          DataSourceExec: projection=[l_orderkey, l_quantity], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");

        let graph = plan.into_graph();
        assert_snapshot!(graph.display(), @r"
        Relations:
          R0: DataSourceExec [c_custkey, c_name]
          R1: DataSourceExec [o_orderkey, o_custkey, o_orderdate]
          R2: DataSourceExec [l_orderkey, l_quantity]
        Hyper-edges:
          {R0, R1} Inner on c_custkey = o_custkey (HashJoinExec)
          {R1, R2} Inner on o_orderkey = l_orderkey (HashJoinExec)
        ");
    }

    #[tokio::test]
    async fn three_way_inner_join_chain_look_through_repartitions() {
        let plan = TestPlanBuilder::new()
            .target_partitions(2)
            .physical_plan(
                r"SELECT c_name, o_orderdate, l_quantity
                 FROM customer
                 JOIN orders ON c_custkey = o_custkey
                 JOIN lineitem ON o_orderkey = l_orderkey",
            )
            .await;

        assert_snapshot!(plan.display(), @r"
        HashJoinExec: mode=Partitioned, join_type=Inner, on=[(o_orderkey@1, l_orderkey@0)], projection=[c_name@0, o_orderdate@2, l_quantity@4]
          RepartitionExec: partitioning=Hash([o_orderkey@1], 2), input_partitions=2
            HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c_custkey@0, o_custkey@1)], projection=[c_name@1, o_orderkey@2, o_orderdate@4]
              CoalescePartitionsExec
                DataSourceExec: projection=[c_custkey, c_name], file_type=parquet
              DataSourceExec: projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          RepartitionExec: partitioning=Hash([l_orderkey@0], 2), input_partitions=2
            DataSourceExec: projection=[l_orderkey, l_quantity], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");

        let graph = plan.into_graph();
        assert_snapshot!(graph.display(), @r"
        Relations:
          R0: DataSourceExec [c_custkey, c_name]
          R1: DataSourceExec [o_orderkey, o_custkey, o_orderdate]
          R2: DataSourceExec [l_orderkey, l_quantity]
        Hyper-edges:
          {R0, R1} Inner on c_custkey = o_custkey (HashJoinExec)
          {R1, R2} Inner on o_orderkey = l_orderkey (HashJoinExec)
        ");
    }

    #[tokio::test]
    async fn six_way_join_with_three_relation_hyper_edge() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r"SELECT n_name, l_extendedprice
                 FROM customer
                 JOIN orders ON c_custkey = o_custkey
                 JOIN lineitem ON l_orderkey = o_orderkey
                 JOIN supplier ON l_suppkey = s_suppkey AND c_nationkey = s_nationkey
                 JOIN nation ON s_nationkey = n_nationkey
                 JOIN region ON n_regionkey = r_regionkey
                 WHERE r_name = 'ASIA'",
            )
            .await;

        assert_snapshot!(plan.display(), @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(r_regionkey@0, n_regionkey@2)], projection=[n_name@2, l_extendedprice@1]
          FilterExec: r_name@1 = ASIA, projection=[r_regionkey@0]
            DataSourceExec: projection=[r_regionkey, r_name], file_type=parquet, predicate=r_name@1 = ASIA, pruning_predicate=r_name_null_count@2 != row_count@3 AND r_name_min@0 <= ASIA AND ASIA <= r_name_max@1, required_guarantees=[r_name in (ASIA)]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, s_nationkey@1)], projection=[l_extendedprice@3, n_name@1, n_regionkey@2]
            DataSourceExec: projection=[n_nationkey, n_name, n_regionkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
            HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(s_suppkey@0, l_suppkey@1), (s_nationkey@1, c_nationkey@0)], projection=[l_extendedprice@4, s_nationkey@1]
              DataSourceExec: projection=[s_suppkey, s_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
              HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@1, l_orderkey@0)], projection=[c_nationkey@0, l_suppkey@3, l_extendedprice@4]
                HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c_custkey@0, o_custkey@1)], projection=[c_nationkey@1, o_orderkey@2]
                  DataSourceExec: projection=[c_custkey, c_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
                  DataSourceExec: projection=[o_orderkey, o_custkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
                DataSourceExec: projection=[l_orderkey, l_suppkey, l_extendedprice], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");

        let graph = plan.into_graph();
        assert_snapshot!(graph.display(), @r"
        Relations:
          R0: FilterExec [r_regionkey]
          R1: DataSourceExec [n_nationkey, n_name, n_regionkey]
          R2: DataSourceExec [s_suppkey, s_nationkey]
          R3: DataSourceExec [c_custkey, c_nationkey]
          R4: DataSourceExec [o_orderkey, o_custkey]
          R5: DataSourceExec [l_orderkey, l_suppkey, l_extendedprice]
        Hyper-edges:
          {R3, R4} Inner on c_custkey = o_custkey (HashJoinExec)
          {R4, R5} Inner on o_orderkey = l_orderkey (HashJoinExec)
          {R2, R3, R5} Inner on s_suppkey = l_suppkey AND s_nationkey = c_nationkey (HashJoinExec)
          {R1, R2} Inner on n_nationkey = s_nationkey (HashJoinExec)
          {R0, R1} Inner on r_regionkey = n_regionkey (HashJoinExec)
        ");
    }

    #[tokio::test]
    async fn bushy_join_of_two_joined_subqueries() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r"SELECT c_name, p_name
                 FROM (SELECT c_name, o_orderkey FROM customer JOIN orders ON c_custkey = o_custkey) co
                 JOIN (SELECT l_orderkey, p_name FROM lineitem JOIN part ON l_partkey = p_partkey) lp
                   ON co.o_orderkey = lp.l_orderkey",
            )
            .await;

        assert_snapshot!(plan.display(), @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@1, l_orderkey@0)], projection=[c_name@0, p_name@3]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c_custkey@0, o_custkey@1)], projection=[c_name@1, o_orderkey@2]
            DataSourceExec: projection=[c_custkey, c_name], file_type=parquet
            DataSourceExec: projection=[o_orderkey, o_custkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(p_partkey@0, l_partkey@1)], projection=[l_orderkey@2, p_name@1]
            DataSourceExec: projection=[p_partkey, p_name], file_type=parquet
            DataSourceExec: projection=[l_orderkey, l_partkey], file_type=parquet, predicate=DynamicFilter [ empty ] AND DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");

        let graph = plan.into_graph();
        assert_snapshot!(graph.display(), @r"
        Relations:
          R0: DataSourceExec [c_custkey, c_name]
          R1: DataSourceExec [o_orderkey, o_custkey]
          R2: DataSourceExec [p_partkey, p_name]
          R3: DataSourceExec [l_orderkey, l_partkey]
        Hyper-edges:
          {R0, R1} Inner on c_custkey = o_custkey (HashJoinExec)
          {R2, R3} Inner on p_partkey = l_partkey (HashJoinExec)
          {R1, R3} Inner on o_orderkey = l_orderkey (HashJoinExec)
        ");
    }

    #[tokio::test]
    async fn left_semi_join_from_exists_subquery() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r"SELECT c_name, n_name
                 FROM customer
                 JOIN nation ON c_nationkey = n_nationkey
                 WHERE EXISTS (SELECT 1 FROM orders WHERE o_custkey = c_custkey)",
            )
            .await;

        assert_snapshot!(plan.display(), @r"
        HashJoinExec: mode=CollectLeft, join_type=LeftSemi, on=[(c_custkey@0, o_custkey@0)], projection=[c_name@1, n_name@2]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(n_nationkey@0, c_nationkey@2)], projection=[c_custkey@2, c_name@3, n_name@1]
            DataSourceExec: projection=[n_nationkey, n_name], file_type=parquet
            DataSourceExec: projection=[c_custkey, c_name, c_nationkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          DataSourceExec: projection=[o_custkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");

        let graph = plan.into_graph();
        assert_snapshot!(graph.display(), @r"
        Relations:
          R0: DataSourceExec [n_nationkey, n_name]
          R1: DataSourceExec [c_custkey, c_name, c_nationkey]
          R2: DataSourceExec [o_custkey]
        Hyper-edges:
          {R0, R1} Inner on n_nationkey = c_nationkey (HashJoinExec)
          {R1, R2} LeftSemi on c_custkey = o_custkey (HashJoinExec)
        ");
    }

    #[tokio::test]
    async fn left_outer_join_chain() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r"SELECT c_name, o_orderdate, l_quantity
                 FROM customer
                 LEFT JOIN orders ON c_custkey = o_custkey
                 LEFT JOIN lineitem ON o_orderkey = l_orderkey",
            )
            .await;

        assert_snapshot!(plan.display(), @r"
        HashJoinExec: mode=CollectLeft, join_type=Left, on=[(o_orderkey@1, l_orderkey@0)], projection=[c_name@0, o_orderdate@2, l_quantity@4]
          HashJoinExec: mode=CollectLeft, join_type=Left, on=[(c_custkey@0, o_custkey@1)], projection=[c_name@1, o_orderkey@2, o_orderdate@4]
            DataSourceExec: projection=[c_custkey, c_name], file_type=parquet
            DataSourceExec: projection=[o_orderkey, o_custkey, o_orderdate], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          DataSourceExec: projection=[l_orderkey, l_quantity], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");

        let graph = plan.into_graph();
        assert_snapshot!(graph.display(), @r"
        Relations:
          R0: DataSourceExec [c_custkey, c_name]
          R1: DataSourceExec [o_orderkey, o_custkey, o_orderdate]
          R2: DataSourceExec [l_orderkey, l_quantity]
        Hyper-edges:
          {R0, R1} Left on c_custkey = o_custkey (HashJoinExec)
          {R1, R2} Left on o_orderkey = l_orderkey (HashJoinExec)
        ");
    }

    #[tokio::test]
    async fn aggregate_subquery_is_a_single_relation() {
        let plan = TestPlanBuilder::new()
            .target_partitions(1)
            .physical_plan(
                r"SELECT c_name, total
                 FROM customer
                 JOIN orders ON c_custkey = o_custkey
                 JOIN (SELECT l_orderkey, sum(l_quantity) AS total FROM lineitem GROUP BY l_orderkey) l
                   ON o_orderkey = l.l_orderkey",
            )
            .await;

        assert_snapshot!(plan.display(), @r"
        HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(o_orderkey@1, l_orderkey@0)], projection=[c_name@0, total@3]
          HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(c_custkey@0, o_custkey@1)], projection=[c_name@1, o_orderkey@2]
            DataSourceExec: projection=[c_custkey, c_name], file_type=parquet
            DataSourceExec: projection=[o_orderkey, o_custkey], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
          ProjectionExec: expr=[l_orderkey@0 as l_orderkey, sum(lineitem.l_quantity)@1 as total]
            AggregateExec: mode=Single, gby=[l_orderkey@0 as l_orderkey], aggr=[sum(lineitem.l_quantity)]
              DataSourceExec: projection=[l_orderkey, l_quantity], file_type=parquet, predicate=DynamicFilter [ empty ], dynamic_rg_pruning=eligible
        ");

        let graph = plan.into_graph();
        assert_snapshot!(graph.display(), @r"
        Relations:
          R0: DataSourceExec [c_custkey, c_name]
          R1: DataSourceExec [o_orderkey, o_custkey]
          R2: ProjectionExec [l_orderkey, total]
        Hyper-edges:
          {R0, R1} Inner on c_custkey = o_custkey (HashJoinExec)
          {R1, R2} Inner on o_orderkey = l_orderkey (HashJoinExec)
        ");
    }
}
