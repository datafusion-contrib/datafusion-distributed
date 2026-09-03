use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::{DistributedTaskContext, NetworkBoundaryExt};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::joins::{CrossJoinExec, HashJoinExec, NestedLoopJoinExec};
use futures::future::{BoxFuture, try_join_all};
use std::cell::RefCell;
use std::future::Future;
use std::sync::Arc;

pub(crate) trait TreeNodeExt {
    /// Recursively rewrite the node using `f` in a bottom-up (post-order)
    /// fashion, awaiting `f` for each node.
    ///
    /// `f` is applied to the node's children first, and then to the node itself.
    /// Sibling subtrees are transformed concurrently.
    ///
    /// The returned [`TreeNodeRecursion`] steers the traversal as in
    /// [`TreeNode::transform_up`]: both [`TreeNodeRecursion::Jump`] and
    /// [`TreeNodeRecursion::Stop`] prevent `f` from being applied to *any* ancestor of the
    /// node that returned them.
    ///
    /// Because sibling subtrees run concurrently, [`TreeNodeRecursion::Stop`] cannot prune
    /// them: unlike [`TreeNode::transform_up`], `f` has already been applied to the sibling
    /// subtrees by the time the stop is observed, and their rewrites are then discarded. The
    /// resulting plan is identical to [`TreeNode::transform_up`]'s, but `f` must not rely on
    /// `Stop` to suppress its own side effects.
    ///
    /// Note that every child subtree is polled concurrently with no fan-out limit, so an `f`
    /// that performs I/O will issue up to one request per node of the widest tree level at a
    /// time.
    async fn transform_up_async<F, Fut>(self, f: F) -> Result<Transformed<Self>>
    where
        Self: Sized,
        F: Fn(Self) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Transformed<Self>>> + Send;

    /// Applies `f` to the node then each of its children, recursively (a
    /// top-down, pre-order traversal), propagating the [DistributedTaskContext] correctly
    /// across nodes that mutate this context, and ignoring nodes that do not belong to
    /// the passed [DistributedTaskContext].
    ///
    /// For example, the presence of [ChildrenIsolatorUnionExec] will make this function
    /// not recurse into nodes that would be ignored because of the contextual
    /// [DistributedTaskContext], and while recursing into its children, a different
    /// [DistributedTaskContext] will be passed.
    ///
    /// The return [`TreeNodeRecursion`] controls the recursion and can cause an early return.
    ///
    /// This function does not recurse into the input of network boundaries.
    fn apply_with_dt_ctx<F: FnMut(&Self, DistributedTaskContext) -> Result<TreeNodeRecursion>>(
        &self,
        ctx: DistributedTaskContext,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Applies `f` top-down (pre-order) to the nodes on the plan's *driver path*: the operators
    /// whose ongoing data production feeds, batch for batch, into the root's output. The
    /// [`TreeNodeRecursion`] returned by `f` steers it as in [`TreeNode::apply`].
    ///
    /// It differs from a full traversal only at the pipeline-breaking joins ([`HashJoinExec`],
    /// [`NestedLoopJoinExec`], [`CrossJoinExec`]): it follows the probe side (`right`) and skips the
    /// build side (`left`), since a join fully materializes its build side before emitting any
    /// output row, so build-side rows are setup work rather than output progress. Every other
    /// operator is transparent.
    ///
    /// Used to estimate a running stage's completion: the total rows to pull come from the leaves
    /// on the driver path (see `estimated_driver_path_leaf_rows`).
    ///
    /// ```text
    ///        ┌────────────┐
    ///        │ HashJoin   │  visited
    ///        └─────┬──────┘
    ///        build │ probe
    ///      (left)  │  │ (right)
    ///     ┌────────┘  └────────┐
    ///     ▼                    ▼
    /// ┌────────┐          ┌────────┐
    /// │ Scan B │ SKIPPED  │ Scan P │  visited
    /// └────────┘          └────────┘
    /// ```
    fn apply_driver_path<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        f: F,
    ) -> Result<TreeNodeRecursion>;

    /// Recursively rewrite the tree using `f` in a top-down (pre-order) fashion, propagating
    /// the appropriate [DistributedTaskContext] based on the presence of nodes that can isolate
    /// tasks, like [ChildrenIsolatorUnionExec].
    ///
    /// `f` is applied to the node first, and then its children.
    fn transform_down_with_dt_ctx<
        F: FnMut(Self, DistributedTaskContext) -> Result<Transformed<Self>>,
    >(
        self,
        dt_ctx: DistributedTaskContext,
        f: F,
    ) -> Result<Transformed<Self>>
    where
        Self: Sized;

    /// Recursively rewrite the tree using `f` in a bottom-up (post-order) fashion, propagating
    /// the appropriate task count based on the presence of nodes that can isolate tasks (e.g.,
    /// [ChildrenIsolatorUnionExec]) and the presence of network boundaries that change the task
    /// count.
    ///
    /// `f` is applied to the node's children first, and then to the node itself.
    fn transform_up_with_task_count<F: FnMut(Self, usize) -> Result<Transformed<Self>>>(
        self,
        task_count: usize,
        f: F,
    ) -> Result<Transformed<Self>>
    where
        Self: Sized;

    /// Recursively rewrite the tree using `f` in a top-down (pre-order) fashion, propagating
    /// the appropriate task count based on the presence of nodes that can isolate tasks (e.g.,
    /// [ChildrenIsolatorUnionExec]) and the presence of network boundaries that change the task
    /// count.
    ///
    /// `f` is applied to the node first, and then its children.
    fn transform_down_with_task_count<F: FnMut(Self, usize) -> Result<Transformed<Self>>>(
        self,
        task_count: usize,
        f: F,
    ) -> Result<Transformed<Self>>
    where
        Self: Sized;
}

impl TreeNodeExt for Arc<dyn ExecutionPlan> {
    async fn transform_up_async<F, Fut>(self, f: F) -> Result<Transformed<Self>>
    where
        F: Fn(Self) -> Fut + Send + Sync,
        Fut: Future<Output = Result<Transformed<Self>>> + Send,
    {
        fn transform_up_async_impl<'a, F, Fut>(
            node: Arc<dyn ExecutionPlan>,
            f: &'a F,
        ) -> BoxFuture<'a, Result<Transformed<Arc<dyn ExecutionPlan>>>>
        where
            F: Fn(Arc<dyn ExecutionPlan>) -> Fut + Send + Sync + 'a,
            Fut: Future<Output = Result<Transformed<Arc<dyn ExecutionPlan>>>> + Send + 'a,
        {
            Box::pin(async move {
                let children = try_join_all(
                    node.children()
                        .into_iter()
                        .map(|child| transform_up_async_impl(Arc::clone(child), f)),
                )
                .await?;
                let mut children = children.into_iter();
                let transformed = node.map_children(|_| {
                    Ok(children
                        .next()
                        .expect("each child has a transformation result"))
                })?;

                match transformed.tnr {
                    TreeNodeRecursion::Continue => {
                        let children_transformed = transformed.transformed;
                        let mut transformed = f(transformed.data).await?;
                        transformed.transformed |= children_transformed;
                        Ok(transformed)
                    }
                    TreeNodeRecursion::Jump | TreeNodeRecursion::Stop => Ok(transformed),
                }
            })
        }

        transform_up_async_impl(self, &f).await
    }

    fn apply_with_dt_ctx<F: FnMut(&Self, DistributedTaskContext) -> Result<TreeNodeRecursion>>(
        &self,
        ctx: DistributedTaskContext,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        fn recurse<
            F: FnMut(&Arc<dyn ExecutionPlan>, DistributedTaskContext) -> Result<TreeNodeRecursion>,
        >(
            plan: &Arc<dyn ExecutionPlan>,
            ctx: DistributedTaskContext,
            f: &mut F,
        ) -> Result<TreeNodeRecursion> {
            f(plan, ctx)?.visit_children(|| {
                if let Some(ciu) = plan.downcast_ref::<ChildrenIsolatorUnionExec>() {
                    // Just recurse to children that will actually get executed by this
                    // ChildrenIsolatorUnionExec.
                    ciu.task_idx_map[ctx.task_index].iter().apply_until_stop(
                        |(child_i, child_ctx)| recurse(&ciu.children[*child_i], *child_ctx, f),
                    )
                } else if plan.is_network_boundary() {
                    Ok(TreeNodeRecursion::Continue)
                } else {
                    plan.children()
                        .into_iter()
                        .apply_until_stop(|child| recurse(child, ctx, f))
                }
            })
        }
        recurse(self, ctx, &mut f)
    }

    fn apply_driver_path<F: FnMut(&Self) -> Result<TreeNodeRecursion>>(
        &self,
        mut f: F,
    ) -> Result<TreeNodeRecursion> {
        fn recurse<F: FnMut(&Arc<dyn ExecutionPlan>) -> Result<TreeNodeRecursion>>(
            plan: &Arc<dyn ExecutionPlan>,
            f: &mut F,
        ) -> Result<TreeNodeRecursion> {
            f(plan)?.visit_children(|| {
                if let Some(hash_join) = plan.downcast_ref::<HashJoinExec>() {
                    recurse(hash_join.right(), f)
                } else if let Some(nested_loop_join) = plan.downcast_ref::<NestedLoopJoinExec>() {
                    recurse(nested_loop_join.right(), f)
                } else if let Some(cross_join) = plan.downcast_ref::<CrossJoinExec>() {
                    recurse(cross_join.right(), f)
                } else {
                    plan.children()
                        .into_iter()
                        .apply_until_stop(|child| recurse(child, f))
                }
            })
        }

        recurse(self, &mut f)
    }

    fn transform_down_with_dt_ctx<
        F: FnMut(Self, DistributedTaskContext) -> Result<Transformed<Self>>,
    >(
        self,
        dt_ctx: DistributedTaskContext,
        mut f: F,
    ) -> Result<Transformed<Self>>
    where
        Self: Sized,
    {
        // None = skip this subtree (irrelevant CIU child for our task index).
        let mut stack = vec![Some(dt_ctx)];
        self.transform_down(|node| {
            let Some(dt_ctx) = stack.pop().unwrap() else {
                return Ok(Transformed {
                    data: node,
                    transformed: false,
                    tnr: TreeNodeRecursion::Jump,
                });
            };
            let transformed = f(node, dt_ctx)?;
            if transformed.tnr == TreeNodeRecursion::Stop {
                return Ok(transformed);
            }
            if transformed.tnr != TreeNodeRecursion::Continue
                || transformed.data.is_network_boundary()
            {
                return Ok(Transformed {
                    tnr: TreeNodeRecursion::Jump,
                    ..transformed
                });
            }
            let node = &transformed.data;
            if let Some(ciu) = node.downcast_ref::<ChildrenIsolatorUnionExec>() {
                let mut child_ctxs = vec![None; ciu.children.len()];
                for (child_idx, child_ctx) in &ciu.task_idx_map[dt_ctx.task_index] {
                    child_ctxs[*child_idx] = Some(*child_ctx);
                }
                stack.extend(child_ctxs.into_iter().rev());
            } else {
                stack.extend(node.children().iter().map(|_| Some(dt_ctx)).rev());
            }
            Ok(transformed)
        })
    }

    fn transform_up_with_task_count<F: FnMut(Self, usize) -> Result<Transformed<Self>>>(
        self,
        task_count: usize,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        let stack = RefCell::new(vec![task_count]);
        self.transform_down_up(
            |node| {
                let cur = *stack.borrow().last().unwrap();
                let child_tcs = if let Some(ciu) = node.downcast_ref::<ChildrenIsolatorUnionExec>()
                {
                    ciu.child_task_counts()
                } else if let Some(nb) = node.as_network_boundary() {
                    vec![nb.input_stage().task_count(); node.children().len()]
                } else {
                    vec![cur; node.children().len()]
                };
                stack.borrow_mut().extend(child_tcs.into_iter().rev());
                Ok(Transformed::no(node))
            },
            |node| {
                let tc = stack.borrow_mut().pop().unwrap();
                f(node, tc)
            },
        )
    }

    fn transform_down_with_task_count<F: FnMut(Self, usize) -> Result<Transformed<Self>>>(
        self,
        task_count: usize,
        mut f: F,
    ) -> Result<Transformed<Self>> {
        let stack = RefCell::new(vec![task_count]);
        self.transform_down_up(
            |node| {
                let tc = stack.borrow_mut().pop().unwrap();
                let transformed = f(node, tc)?;
                if transformed.tnr != TreeNodeRecursion::Continue {
                    return Ok(transformed);
                }
                let child_tcs = if let Some(ciu) =
                    transformed.data.downcast_ref::<ChildrenIsolatorUnionExec>()
                {
                    ciu.child_task_counts()
                } else if let Some(nb) = transformed.data.as_network_boundary() {
                    vec![nb.input_stage().task_count(); transformed.data.children().len()]
                } else {
                    vec![tc; transformed.data.children().len()]
                };
                stack.borrow_mut().extend(child_tcs.into_iter().rev());
                Ok(transformed)
            },
            |node| Ok(Transformed::no(node)),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::ChildWeight;
    use crate::stage::RemoteStage;
    use crate::{NetworkCoalesceExec, Stage};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::{JoinType, NullEquality, exec_err};
    use datafusion::physical_expr::PhysicalExpr;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::joins::PartitionMode;
    use datafusion::physical_plan::union::UnionExec;
    use insta::assert_snapshot;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::Barrier;
    use tokio::time::timeout;

    // ── transform_up_async ───────────────────────────────────────────────────

    #[tokio::test]
    async fn transform_up_async_bottom_up_order() {
        assert_snapshot!(trace_async_up(single(leaf())).await, @r"
        Leaf
        Single
        ");
    }

    #[tokio::test]
    async fn transform_up_async_rewrites_children() {
        assert_snapshot!(trace_async_up_rewrite(single(leaf())).await, @r"
        Single
        Single
        Leaf
        ");
    }

    #[tokio::test]
    async fn transform_up_async_processes_siblings_concurrently() {
        let plan = union(vec![leaf(), leaf()]);
        assert_snapshot!(trace_async_up_concurrently(plan, 2).await, @r"
        Leaf
        Leaf
        Union
        ");
    }

    #[tokio::test]
    async fn transform_up_async_jump_skips_parent() {
        let child = leaf();
        let plan = single(Arc::clone(&child));
        assert_snapshot!(trace_async_up_with(plan, |p| {
            if Arc::ptr_eq(p, &child) { TreeNodeRecursion::Jump } else { TreeNodeRecursion::Continue }
        }).await, @"Leaf [->jump]");
    }

    #[tokio::test]
    async fn transform_up_async_stop_skips_parent() {
        assert_snapshot!(
            trace_async_up_with(single(leaf()), |_| TreeNodeRecursion::Stop).await,
            @"Leaf [->stop]",
        );
    }

    #[tokio::test]
    async fn transform_up_async_deep_bottom_up_order() {
        assert_snapshot!(trace_async_up(single(single(single(leaf())))).await, @r"
        Leaf
        Single
        Single
        Single
        ");
    }

    #[tokio::test]
    async fn transform_up_async_jump_skips_every_ancestor() {
        // `Jump` returned bottom-up prunes the whole ancestor chain, not just the
        // immediate parent, matching `TreeNode::transform_up`.
        let child = leaf();
        let plan = single(single(Arc::clone(&child)));
        assert_snapshot!(trace_async_up_with(plan, |p| {
            if Arc::ptr_eq(p, &child) {
                TreeNodeRecursion::Jump
            } else {
                TreeNodeRecursion::Continue
            }
        })
        .await, @"Leaf [->jump]");
    }

    #[tokio::test]
    async fn transform_up_async_sees_rewritten_children() {
        // `f` must observe the children that its own recursive invocations produced.
        let observed = Mutex::new(vec![]);
        single(leaf())
            .transform_up_async(async |plan| {
                observed.lock().unwrap().push(format!(
                    "{} <- [{}]",
                    plan_label(&plan),
                    plan.children()
                        .iter()
                        .map(|c| plan_label(c))
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
                if plan.is::<EmptyExec>() {
                    Ok(Transformed::yes(union(vec![Arc::clone(&plan), plan])))
                } else {
                    Ok(Transformed::no(plan))
                }
            })
            .await
            .unwrap();
        assert_snapshot!(observed.into_inner().unwrap().join("\n"), @r"
        Leaf <- []
        Single <- [Union]
        ");
    }

    #[tokio::test]
    async fn transform_up_async_propagates_children_transformed_flag() {
        // The root's `f` reports no change, but a descendant did change, so the
        // overall result must still be flagged as transformed.
        let transformed = single(leaf())
            .transform_up_async(async |plan| {
                if plan.is::<EmptyExec>() {
                    Ok(Transformed::yes(single(plan)))
                } else {
                    Ok(Transformed::no(plan))
                }
            })
            .await
            .unwrap();
        assert!(transformed.transformed);
    }

    #[tokio::test]
    async fn transform_up_async_no_change_keeps_flag_unset() {
        let transformed = single(union(vec![leaf(), leaf()]))
            .transform_up_async(async |plan| Ok(Transformed::no(plan)))
            .await
            .unwrap();
        assert!(!transformed.transformed);
    }

    #[tokio::test]
    async fn transform_up_async_propagates_error() {
        let err = single(leaf())
            .transform_up_async(async |plan| {
                if plan.is::<EmptyExec>() {
                    return exec_err!("boom");
                }
                Ok(Transformed::no(plan))
            })
            .await
            .unwrap_err();
        assert_snapshot!(err.strip_backtrace(), @"Execution error: boom");
    }

    #[tokio::test]
    async fn transform_up_async_error_in_one_sibling_aborts() {
        // A failing subtree must surface its error rather than hang waiting on the
        // sibling subtrees that are running concurrently.
        let err = timeout(
            Duration::from_secs(5),
            union(vec![leaf(), single(leaf())]).transform_up_async(async |plan| {
                if plan.is::<UnionExec>() {
                    panic!("the root must not be reached when a child fails");
                }
                if plan.is::<EmptyExec>() {
                    return exec_err!("boom");
                }
                Ok(Transformed::no(plan))
            }),
        )
        .await
        .expect("a failing subtree should not hang the traversal")
        .unwrap_err();
        assert_snapshot!(err.strip_backtrace(), @"Execution error: boom");
    }

    #[tokio::test]
    async fn transform_up_async_stop_does_not_prune_concurrent_siblings() {
        // Documents the one divergence from `TreeNode::transform_up`: siblings run
        // concurrently, so a `Stop` cannot suppress `f` on the sibling subtrees. Only their
        // rewrites are discarded (see `transform_up_async_matches_sync_when_a_sibling_stops`).
        let stopper = leaf();
        let plan = union(vec![Arc::clone(&stopper), single(leaf())]);
        let mut visited = trace_async_up_with(plan, move |p| {
            if Arc::ptr_eq(p, &stopper) {
                TreeNodeRecursion::Stop
            } else {
                TreeNodeRecursion::Continue
            }
        })
        .await
        .lines()
        .map(str::to_string)
        .collect::<Vec<_>>();
        // Concurrent siblings complete in a non-deterministic order.
        visited.sort();
        assert_snapshot!(visited.join("\n"), @r"
        Leaf
        Leaf [->stop]
        Single
        ");
    }

    #[tokio::test]
    async fn transform_up_async_matches_sync_continue() {
        assert_snapshot!(
            assert_matches_sync(
                single(union(vec![leaf(), single(leaf())])),
                Box::new(|_| TreeNodeRecursion::Continue),
            )
            .await,
            @r"
        transformed=true tnr=Continue
        CoalescePartitionsExec
          CoalescePartitionsExec
            CoalescePartitionsExec
              UnionExec
                CoalescePartitionsExec
                  EmptyExec
                CoalescePartitionsExec
                  CoalescePartitionsExec
                    CoalescePartitionsExec
                      EmptyExec
        "
        );
    }

    #[tokio::test]
    async fn transform_up_async_matches_sync_when_a_sibling_stops() {
        // The first branch stops, so the second branch's rewrite must be discarded and
        // the ancestors left untouched, exactly as `TreeNode::transform_up` does.
        let stopper = leaf();
        let plan = union(vec![single(Arc::clone(&stopper)), single(leaf())]);
        assert_snapshot!(
            assert_matches_sync(
                plan,
                Box::new(move |p| {
                    if Arc::ptr_eq(p, &stopper) {
                        TreeNodeRecursion::Stop
                    } else {
                        TreeNodeRecursion::Continue
                    }
                }),
            )
            .await,
            @r"
        transformed=true tnr=Stop
        UnionExec
          CoalescePartitionsExec
            CoalescePartitionsExec
              EmptyExec
          CoalescePartitionsExec
            EmptyExec
        "
        );
    }

    #[tokio::test]
    async fn transform_up_async_matches_sync_when_a_sibling_jumps() {
        let jumper = leaf();
        let plan = union(vec![single(Arc::clone(&jumper)), single(leaf())]);
        assert_snapshot!(
            assert_matches_sync(
                plan,
                Box::new(move |p| {
                    if Arc::ptr_eq(p, &jumper) {
                        TreeNodeRecursion::Jump
                    } else {
                        TreeNodeRecursion::Continue
                    }
                }),
            )
            .await,
            @r"
        transformed=true tnr=Continue
        CoalescePartitionsExec
          UnionExec
            CoalescePartitionsExec
              CoalescePartitionsExec
                EmptyExec
            CoalescePartitionsExec
              CoalescePartitionsExec
                CoalescePartitionsExec
                  EmptyExec
        "
        );
    }

    // ── apply_with_dt_ctx ────────────────────────────────────────────────────────

    #[test]
    fn apply_leaf() {
        let plan = leaf();
        assert_snapshot!(trace_apply(&plan, ctx(0, 1)), @"Leaf [ctx=0/1]");
    }

    #[test]
    fn apply_top_down_order() {
        let plan = union(vec![leaf(), leaf()]);
        assert_snapshot!(trace_apply(&plan, ctx(0, 1)), @r"
        Union [ctx=0/1]
        Leaf [ctx=0/1]
        Leaf [ctx=0/1]
        ");
    }

    #[test]
    fn apply_deep_tree() {
        let plan = single(single(leaf()));
        assert_snapshot!(trace_apply(&plan, ctx(0, 1)), @r"
        Single [ctx=0/1]
        Single [ctx=0/1]
        Leaf [ctx=0/1]
        ");
    }

    #[test]
    fn apply_stop() {
        let plan = single(leaf());
        assert_snapshot!(
            trace_apply_with(&plan, ctx(0, 1), |_| TreeNodeRecursion::Stop),
            @"Single [ctx=0/1] [->stop]",
        );
    }

    #[test]
    fn apply_jump_skips_subtree() {
        let child = single(leaf());
        let plan = single(Arc::clone(&child));
        assert_snapshot!(
            trace_apply_with(&plan, ctx(0, 1), |p| {
                if Arc::ptr_eq(p, &child) { TreeNodeRecursion::Jump } else { TreeNodeRecursion::Continue }
            }),
            @r"
        Single [ctx=0/1]
        Single [ctx=0/1] [->jump]
        ");
    }

    #[test]
    fn apply_network_boundary() {
        let plan = network_boundary(leaf(), 2);
        assert_snapshot!(trace_apply(&plan, ctx(0, 1)), @"Network [ctx=0/1]");
    }

    #[test]
    fn apply_ciu_routing() {
        let plan = ciu(vec![leaf(), leaf()], vec![1, 1], 2).unwrap();
        assert_snapshot!(trace_apply(&plan, ctx(0, 2)), @r"
        CIU [ctx=0/2]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_apply(&plan, ctx(1, 2)), @r"
        CIU [ctx=1/2]
        Leaf [ctx=0/1]
        ");
    }

    #[test]
    fn apply_ciu_context_remapping() {
        let plan = ciu(vec![leaf(), leaf(), leaf()], vec![1, 1, 1], 3).unwrap();
        assert_snapshot!(trace_apply(&plan, ctx(0, 3)), @r"
        CIU [ctx=0/3]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_apply(&plan, ctx(1, 3)), @r"
        CIU [ctx=1/3]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_apply(&plan, ctx(2, 3)), @r"
        CIU [ctx=2/3]
        Leaf [ctx=0/1]
        ");
    }

    #[test]
    fn apply_nested_ciu() {
        let inner0 = ciu(vec![leaf(), leaf()], vec![1, 1], 2).unwrap();
        let inner1 = ciu(vec![leaf(), leaf()], vec![1, 1], 2).unwrap();
        let plan = ciu(vec![inner0, inner1], vec![2, 2], 4).unwrap();
        assert_snapshot!(trace_apply(&plan, ctx(0, 4)), @r"
        CIU [ctx=0/4]
        CIU [ctx=0/2]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_apply(&plan, ctx(1, 4)), @r"
        CIU [ctx=1/4]
        CIU [ctx=1/2]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_apply(&plan, ctx(2, 4)), @r"
        CIU [ctx=2/4]
        CIU [ctx=0/2]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_apply(&plan, ctx(3, 4)), @r"
        CIU [ctx=3/4]
        CIU [ctx=1/2]
        Leaf [ctx=0/1]
        ");
    }

    #[test]
    fn apply_ciu_multi_children_per_task() {
        // 4 children split across 2 tasks → each task sees 2 children
        let plan = ciu(vec![leaf(), leaf(), leaf(), leaf()], vec![1, 1, 1, 1], 2).unwrap();
        assert_snapshot!(trace_apply(&plan, ctx(0, 2)), @r"
        CIU [ctx=0/2]
        Leaf [ctx=0/1]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_apply(&plan, ctx(1, 2)), @r"
        CIU [ctx=1/2]
        Leaf [ctx=0/1]
        Leaf [ctx=0/1]
        ");
    }

    // ── apply_driver_path ────────────────────────────────────────────────────

    #[test]
    fn driver_path_leaf() {
        let plan = leaf();
        assert_snapshot!(trace_driver_path(&plan), @"Leaf");
    }

    #[test]
    fn driver_path_top_down_order() {
        let plan = union(vec![single(leaf()), leaf()]);
        assert_snapshot!(trace_driver_path(&plan), @r"
        Union
        Single
        Leaf
        Leaf
        ");
    }

    #[test]
    fn driver_path_jump_skips_subtree() {
        let child = single(leaf());
        let plan = single(Arc::clone(&child));
        assert_snapshot!(
            trace_driver_path_with(&plan, |p| {
                if Arc::ptr_eq(p, &child) { TreeNodeRecursion::Jump } else { TreeNodeRecursion::Continue }
            }),
            @r"
        Single
        Single [->jump]
        ");
    }

    #[test]
    fn driver_path_hash_join_uses_probe_side() {
        let plan = hash_join(
            single(leaf_i32("l")),
            union(vec![leaf_i32("r"), leaf_i32("r")]),
        );
        assert_snapshot!(trace_driver_path(&plan), @r"
        HashJoin
        Union
        Leaf
        Leaf
        ");
    }

    #[test]
    fn driver_path_nested_loop_join_uses_probe_side() {
        let plan = nested_loop_join(
            single(leaf_i32("l")),
            union(vec![leaf_i32("r"), leaf_i32("r")]),
        );
        assert_snapshot!(trace_driver_path(&plan), @r"
        NestedLoopJoin
        Union
        Leaf
        Leaf
        ");
    }

    #[test]
    fn driver_path_cross_join_uses_probe_side() {
        let plan = cross_join(
            single(leaf_i32("l")),
            union(vec![leaf_i32("r"), leaf_i32("r")]),
        );
        assert_snapshot!(trace_driver_path(&plan), @r"
        CrossJoin
        Union
        Leaf
        Leaf
        ");
    }

    // ── transform_down_with_dt_ctx ────────────────────────────────────────────

    #[test]
    fn dt_ctx_down_leaf() {
        let plan = leaf();
        assert_snapshot!(trace_dt_ctx_down(plan, ctx(2, 4)), @"Leaf [ctx=2/4]");
    }

    #[test]
    fn dt_ctx_down_top_down_order() {
        let plan = single(leaf());
        assert_snapshot!(trace_dt_ctx_down(plan, ctx(0, 1)), @r"
        Single [ctx=0/1]
        Leaf [ctx=0/1]
        ");
    }

    #[test]
    fn dt_ctx_down_ctx_propagation() {
        let plan = union(vec![leaf(), leaf()]);
        assert_snapshot!(trace_dt_ctx_down(plan, ctx(1, 3)), @r"
        Union [ctx=1/3]
        Leaf [ctx=1/3]
        Leaf [ctx=1/3]
        ");
    }

    #[test]
    fn dt_ctx_down_network_boundary() {
        let plan = network_boundary(leaf(), 2);
        assert_snapshot!(trace_dt_ctx_down(plan, ctx(0, 1)), @"Network [ctx=0/1]");
    }

    #[test]
    fn dt_ctx_down_ciu_routing() {
        let plan = ciu(vec![leaf(), leaf()], vec![1, 1], 2).unwrap();
        assert_snapshot!(trace_dt_ctx_down(Arc::clone(&plan), ctx(0, 2)), @r"
        CIU [ctx=0/2]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_dt_ctx_down(plan, ctx(1, 2)), @r"
        CIU [ctx=1/2]
        Leaf [ctx=0/1]
        ");
    }

    #[test]
    fn dt_ctx_down_nested_ciu() {
        let inner0 = ciu(vec![leaf(), leaf()], vec![1, 1], 2).unwrap();
        let inner1 = ciu(vec![leaf(), leaf()], vec![1, 1], 2).unwrap();
        let plan = ciu(vec![inner0, inner1], vec![2, 2], 4).unwrap();
        assert_snapshot!(trace_dt_ctx_down(Arc::clone(&plan), ctx(0, 4)), @r"
        CIU [ctx=0/4]
        CIU [ctx=0/2]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_dt_ctx_down(Arc::clone(&plan), ctx(1, 4)), @r"
        CIU [ctx=1/4]
        CIU [ctx=1/2]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_dt_ctx_down(Arc::clone(&plan), ctx(2, 4)), @r"
        CIU [ctx=2/4]
        CIU [ctx=0/2]
        Leaf [ctx=0/1]
        ");
        assert_snapshot!(trace_dt_ctx_down(Arc::clone(&plan), ctx(3, 4)), @r"
        CIU [ctx=3/4]
        CIU [ctx=1/2]
        Leaf [ctx=0/1]
        ");
    }

    #[test]
    fn dt_ctx_down_jump_skips_subtree() {
        let child = single(leaf());
        let root = single(Arc::clone(&child));
        assert_snapshot!(trace_dt_ctx_down_with(root, ctx(0, 1), |p| {
            if Arc::ptr_eq(p, &child) { TreeNodeRecursion::Jump } else { TreeNodeRecursion::Continue }
        }), @r"
        Single [ctx=0/1]
        Single [ctx=0/1] [->jump]
        ");
    }

    // ── transform_up_with_task_count ──────────────────────────────────────────

    #[test]
    fn tc_up_leaf() {
        let plan = leaf();
        assert_snapshot!(trace_tc_up(plan, 7), @"Leaf [tc=7]");
    }

    #[test]
    fn tc_up_bottom_up_order() {
        let plan = single(leaf());
        assert_snapshot!(trace_tc_up(plan, 1), @r"
        Leaf [tc=1]
        Single [tc=1]
        ");
    }

    #[test]
    fn tc_up_uniform_task_count() {
        let plan = union(vec![leaf(), leaf()]);
        assert_snapshot!(trace_tc_up(plan, 5), @r"
        Leaf [tc=5]
        Leaf [tc=5]
        Union [tc=5]
        ");
    }

    #[test]
    fn tc_up_ciu_per_child_task_counts() {
        let plan = ciu(vec![leaf(), leaf()], vec![2, 3], 5).unwrap();
        assert_snapshot!(trace_tc_up(plan, 5), @r"
        Leaf [tc=2]
        Leaf [tc=3]
        CIU [tc=5]
        ");
    }

    #[test]
    fn tc_up_network_boundary_changes_tc() {
        // Nodes inside the NB run at the producer task count (2), not the outer count (5)
        let plan = single(network_boundary(leaf(), 2));
        assert_snapshot!(trace_tc_up(plan, 5), @r"
        Leaf [tc=2]
        Network [tc=5]
        Single [tc=5]
        ");
    }

    #[test]
    fn tc_up_remote_nb_has_no_subtree() {
        let plan = union(vec![
            single(network_boundary(leaf(), 2)),
            single(remote_network_boundary()),
        ]);
        assert_snapshot!(trace_tc_up(plan, 5), @r"
        Leaf [tc=2]
        Network [tc=5]
        Single [tc=5]
        Network [tc=5]
        Single [tc=5]
        Union [tc=5]
        ");
    }

    // ── transform_down_with_task_count ────────────────────────────────────────

    #[test]
    fn tc_down_leaf() {
        let plan = leaf();
        assert_snapshot!(trace_tc_down(plan, 7), @"Leaf [tc=7]");
    }

    #[test]
    fn tc_down_top_down_order() {
        let plan = single(leaf());
        assert_snapshot!(trace_tc_down(plan, 1), @r"
        Single [tc=1]
        Leaf [tc=1]
        ");
    }

    #[test]
    fn tc_down_uniform_task_count() {
        let plan = union(vec![leaf(), leaf()]);
        assert_snapshot!(trace_tc_down(plan, 5), @r"
        Union [tc=5]
        Leaf [tc=5]
        Leaf [tc=5]
        ");
    }

    #[test]
    fn tc_down_ciu_per_child_task_counts() {
        let plan = ciu(vec![leaf(), leaf()], vec![2, 3], 5).unwrap();
        assert_snapshot!(trace_tc_down(plan, 5), @r"
        CIU [tc=5]
        Leaf [tc=2]
        Leaf [tc=3]
        ");
    }

    #[test]
    fn tc_down_network_boundary_changes_tc() {
        let plan = single(network_boundary(leaf(), 2));
        assert_snapshot!(trace_tc_down(plan, 5), @r"
        Single [tc=5]
        Network [tc=5]
        Leaf [tc=2]
        ");
    }

    #[test]
    fn tc_down_remote_nb_has_no_subtree() {
        let plan = union(vec![
            single(network_boundary(leaf(), 2)),
            single(remote_network_boundary()),
        ]);
        assert_snapshot!(trace_tc_down(plan, 5), @r"
        Union [tc=5]
        Single [tc=5]
        Network [tc=5]
        Leaf [tc=2]
        Single [tc=5]
        Network [tc=5]
        ");
    }

    // ── helpers: plan builders ────────────────────────────────────────────────

    fn leaf() -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::empty())))
    }

    fn leaf_i32(name: &str) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
            name,
            DataType::Int32,
            true,
        )]))))
    }

    fn single(child: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(CoalescePartitionsExec::new(child))
    }

    fn union(children: Vec<Arc<dyn ExecutionPlan>>) -> Arc<dyn ExecutionPlan> {
        UnionExec::try_new(children).unwrap()
    }

    fn network_boundary(
        input: Arc<dyn ExecutionPlan>,
        producer_tasks: usize,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(NetworkCoalesceExec::try_new(input, producer_tasks, 1).unwrap())
    }

    fn hash_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(
            HashJoinExec::try_new(
                left,
                right,
                join_on(),
                None,
                &JoinType::Inner,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNothing,
                false,
            )
            .unwrap(),
        )
    }

    fn nested_loop_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(NestedLoopJoinExec::try_new(left, right, None, &JoinType::Inner, None).unwrap())
    }

    fn cross_join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
    ) -> Arc<dyn ExecutionPlan> {
        Arc::new(CrossJoinExec::new(left, right))
    }

    fn join_on() -> Vec<(Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>)> {
        vec![(
            Arc::new(Column::new("l", 0)) as Arc<dyn PhysicalExpr>,
            Arc::new(Column::new("r", 0)) as Arc<dyn PhysicalExpr>,
        )]
    }

    fn remote_network_boundary() -> Arc<dyn ExecutionPlan> {
        network_boundary(leaf(), 1)
            .as_network_boundary()
            .unwrap()
            .with_input_stage(Stage::Remote(RemoteStage {
                query_id: uuid::Uuid::nil(),
                num: 0,
                workers: vec![],
                runtime_stats: None,
            }))
            .unwrap()
    }

    fn ciu(
        children: Vec<Arc<dyn ExecutionPlan>>,
        child_task_counts: Vec<usize>,
        task_count: usize,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            ChildrenIsolatorUnionExec::from_children_and_weights(
                children,
                child_task_counts
                    .iter()
                    .map(|v| ChildWeight::desired(*v as f64)),
                task_count,
            )?,
        ))
    }

    fn ctx(task_index: usize, task_count: usize) -> DistributedTaskContext {
        DistributedTaskContext {
            task_index,
            task_count,
        }
    }

    // ── helpers: trace renderers ──────────────────────────────────────────────

    fn plan_label(p: &Arc<dyn ExecutionPlan>) -> &'static str {
        if p.is::<EmptyExec>() {
            "Leaf"
        } else if p.is::<CoalescePartitionsExec>() {
            "Single"
        } else if p.is::<UnionExec>() {
            "Union"
        } else if p.is::<ChildrenIsolatorUnionExec>() {
            "CIU"
        } else if p.is::<NetworkCoalesceExec>() {
            "Network"
        } else if p.is::<HashJoinExec>() {
            "HashJoin"
        } else if p.is::<NestedLoopJoinExec>() {
            "NestedLoopJoin"
        } else if p.is::<CrossJoinExec>() {
            "CrossJoin"
        } else {
            "?"
        }
    }

    async fn trace_async_up(root: Arc<dyn ExecutionPlan>) -> String {
        trace_async_up_with(root, |_| TreeNodeRecursion::Continue).await
    }

    async fn trace_async_up_rewrite(root: Arc<dyn ExecutionPlan>) -> String {
        let transformed = root
            .transform_up_async(async |plan| {
                if plan.is::<EmptyExec>() {
                    Ok(Transformed::yes(single(plan)))
                } else {
                    Ok(Transformed::no(plan))
                }
            })
            .await
            .unwrap();
        let mut visited = vec![];
        transformed
            .data
            .apply(|plan| {
                visited.push(plan_label(plan));
                Ok(TreeNodeRecursion::Continue)
            })
            .unwrap();
        visited.join("\n")
    }

    async fn trace_async_up_concurrently(
        root: Arc<dyn ExecutionPlan>,
        concurrent_leaves: usize,
    ) -> String {
        let barrier = Barrier::new(concurrent_leaves);
        let visited = Mutex::new(vec![]);
        timeout(
            Duration::from_secs(1),
            root.transform_up_async(async |plan| {
                if plan.is::<EmptyExec>() {
                    barrier.wait().await;
                }
                visited.lock().unwrap().push(plan_label(&plan));
                Ok(Transformed::no(plan))
            }),
        )
        .await
        .expect("sibling callbacks should reach the barrier concurrently")
        .unwrap();
        visited.into_inner().unwrap().join("\n")
    }

    async fn trace_async_up_with<
        F: Fn(&Arc<dyn ExecutionPlan>) -> TreeNodeRecursion + Send + Sync,
    >(
        root: Arc<dyn ExecutionPlan>,
        decide: F,
    ) -> String {
        let visited = Mutex::new(vec![]);
        root.transform_up_async(async |plan| {
            tokio::task::yield_now().await;
            let rec = decide(&plan);
            let suffix = match rec {
                TreeNodeRecursion::Continue => "",
                TreeNodeRecursion::Jump => " [->jump]",
                TreeNodeRecursion::Stop => " [->stop]",
            };
            visited
                .lock()
                .unwrap()
                .push(format!("{}{suffix}", plan_label(&plan)));
            Ok(Transformed::new(plan, false, rec))
        })
        .await
        .unwrap();
        visited.into_inner().unwrap().join("\n")
    }

    type Decide = Box<dyn Fn(&Arc<dyn ExecutionPlan>) -> TreeNodeRecursion + Send + Sync>;

    /// Runs `transform_up_async` and `TreeNode::transform_up` with the same rewrite over the
    /// same plan and asserts that both produce the same plan, `transformed` flag and
    /// [`TreeNodeRecursion`], returning that rendering for snapshotting.
    async fn assert_matches_sync(root: Arc<dyn ExecutionPlan>, decide: Decide) -> String {
        let expected = Arc::clone(&root)
            .transform_up(|plan| Ok(rewrite_and_decide(plan, &*decide)))
            .map(render_transformed)
            .unwrap();
        let actual = root
            .transform_up_async(async |plan| Ok(rewrite_and_decide(plan, &*decide)))
            .await
            .map(render_transformed)
            .unwrap();
        assert_eq!(
            expected, actual,
            "transform_up_async diverged from TreeNode::transform_up"
        );
        actual
    }

    /// Wraps every visited node in a [`CoalescePartitionsExec`] so that a discarded rewrite is
    /// visible in the rendered plan, and returns the [`TreeNodeRecursion`] `decide` asks for.
    fn rewrite_and_decide<F: Fn(&Arc<dyn ExecutionPlan>) -> TreeNodeRecursion + ?Sized>(
        plan: Arc<dyn ExecutionPlan>,
        decide: &F,
    ) -> Transformed<Arc<dyn ExecutionPlan>> {
        let tnr = decide(&plan);
        Transformed::new(single(plan), true, tnr)
    }

    fn render_transformed(transformed: Transformed<Arc<dyn ExecutionPlan>>) -> String {
        format!(
            "transformed={} tnr={:?}\n{}",
            transformed.transformed,
            transformed.tnr,
            displayable(transformed.data.as_ref()).indent(false),
        )
    }

    fn trace_apply(root: &Arc<dyn ExecutionPlan>, dt_ctx: DistributedTaskContext) -> String {
        trace_apply_with(root, dt_ctx, |_| TreeNodeRecursion::Continue)
    }

    fn trace_apply_with<F: FnMut(&Arc<dyn ExecutionPlan>) -> TreeNodeRecursion>(
        root: &Arc<dyn ExecutionPlan>,
        dt_ctx: DistributedTaskContext,
        mut decide: F,
    ) -> String {
        let mut lines = vec![];
        root.apply_with_dt_ctx(dt_ctx, |p, c| {
            let rec = decide(p);
            let suffix = match rec {
                TreeNodeRecursion::Continue => "",
                TreeNodeRecursion::Jump => " [->jump]",
                TreeNodeRecursion::Stop => " [->stop]",
            };
            lines.push(format!(
                "{} [ctx={}/{}]{suffix}",
                plan_label(p),
                c.task_index,
                c.task_count,
            ));
            Ok(rec)
        })
        .unwrap();
        lines.join("\n")
    }

    fn trace_driver_path(root: &Arc<dyn ExecutionPlan>) -> String {
        trace_driver_path_with(root, |_| TreeNodeRecursion::Continue)
    }

    fn trace_driver_path_with<F: FnMut(&Arc<dyn ExecutionPlan>) -> TreeNodeRecursion>(
        root: &Arc<dyn ExecutionPlan>,
        mut decide: F,
    ) -> String {
        let mut lines = vec![];
        root.apply_driver_path(|p| {
            let rec = decide(p);
            let suffix = match rec {
                TreeNodeRecursion::Continue => "",
                TreeNodeRecursion::Jump => " [->jump]",
                TreeNodeRecursion::Stop => " [->stop]",
            };
            lines.push(format!("{}{suffix}", plan_label(p)));
            Ok(rec)
        })
        .unwrap();
        lines.join("\n")
    }

    fn trace_dt_ctx_down(root: Arc<dyn ExecutionPlan>, dt_ctx: DistributedTaskContext) -> String {
        trace_dt_ctx_down_with(root, dt_ctx, |_| TreeNodeRecursion::Continue)
    }

    fn trace_dt_ctx_down_with<F: FnMut(&Arc<dyn ExecutionPlan>) -> TreeNodeRecursion>(
        root: Arc<dyn ExecutionPlan>,
        dt_ctx: DistributedTaskContext,
        mut decide: F,
    ) -> String {
        let mut lines = vec![];
        root.transform_down_with_dt_ctx(dt_ctx, |p, c| {
            let rec = decide(&p);
            let suffix = match rec {
                TreeNodeRecursion::Continue => "",
                TreeNodeRecursion::Jump => " [->jump]",
                TreeNodeRecursion::Stop => " [->stop]",
            };
            lines.push(format!(
                "{} [ctx={}/{}]{suffix}",
                plan_label(&p),
                c.task_index,
                c.task_count,
            ));
            Ok(Transformed {
                data: p,
                transformed: false,
                tnr: rec,
            })
        })
        .unwrap();
        lines.join("\n")
    }

    fn trace_tc_up(root: Arc<dyn ExecutionPlan>, tc: usize) -> String {
        let mut lines = vec![];
        root.transform_up_with_task_count(tc, |p, tc| {
            lines.push(format!("{} [tc={tc}]", plan_label(&p)));
            Ok(Transformed::no(p))
        })
        .unwrap();
        lines.join("\n")
    }

    fn trace_tc_down(root: Arc<dyn ExecutionPlan>, tc: usize) -> String {
        let mut lines = vec![];
        root.transform_down_with_task_count(tc, |p, tc| {
            lines.push(format!("{} [tc={tc}]", plan_label(&p)));
            Ok(Transformed::no(p))
        })
        .unwrap();
        lines.join("\n")
    }
}
