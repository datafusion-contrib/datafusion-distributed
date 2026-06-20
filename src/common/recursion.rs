use crate::execution_plans::ChildrenIsolatorUnionExec;
use crate::{DistributedTaskContext, NetworkBoundaryExt};
use datafusion::common::Result;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeIterator, TreeNodeRecursion};
use datafusion::physical_plan::ExecutionPlan;
use std::cell::RefCell;
use std::sync::Arc;

pub trait TreeNodeExt {
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

    /// Recursively rewrite the tree using `f` in a top-down (pre-order) fashion, propagating
    /// the appropriate [DistributedTaskContext] based on the presence of nodes that can isolate
    /// tasks, like [ChildrenIsolatorUnionExec].
    ///
    /// `f` is applied to the node first, and then its children.
    #[allow(dead_code)] // Used in follow up work.
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
    #[allow(dead_code)] // Used in follow up work.
    fn transform_down_with_task_count<F: FnMut(Self, usize) -> Result<Transformed<Self>>>(
        self,
        task_count: usize,
        f: F,
    ) -> Result<Transformed<Self>>
    where
        Self: Sized;
}

impl TreeNodeExt for Arc<dyn ExecutionPlan> {
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
    use datafusion::arrow::datatypes::Schema;
    use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::union::UnionExec;
    use insta::assert_snapshot;
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

    fn remote_network_boundary() -> Arc<dyn ExecutionPlan> {
        network_boundary(leaf(), 1)
            .as_network_boundary()
            .unwrap()
            .with_input_stage(Stage::Remote(RemoteStage {
                query_id: uuid::Uuid::nil(),
                num: 0,
                workers: vec![],
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
        } else {
            "?"
        }
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
