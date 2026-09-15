use crate::common::TreeNodeExt;
use crate::stage::LocalStage;
use crate::{NetworkBoundaryExt, NetworkShuffleExec, Stage};
use datafusion::common::Result;
use datafusion::common::tree_node::Transformed;
use datafusion::physical_expr::Partitioning;
use datafusion::physical_plan::{ExecutionPlan, PlanProperties};
use std::sync::Arc;
use uuid::Uuid;

/// Prepares every [NetworkBoundary] in the plan for distributed execution:
/// - Elides ones whose producer and consumer sides both run on a single task
/// - Scales the producer-stage head of the survivors to feed all consumer tasks
/// - Stamps each surviving stage with a unique `(query_id, num)` identifier.
pub(crate) fn prepare_network_boundaries(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut stage_id = 1;
    let query_id = Uuid::new_v4();

    let transformed = plan.transform_up_with_task_count(1, |plan, task_count| {
        let Some(nb) = plan.as_network_boundary() else {
            return Ok(Transformed::no(plan));
        };
        // If the input stage is already remote, it was already sent over the network, so nothing else
        // we can do here.
        let Stage::Local(input_stage) = nb.input_stage() else {
            return Ok(Transformed::no(plan));
        };

        // 1) If there are both 1 producer and consumer tasks, optimize the network boundary out.
        if task_count == 1 && input_stage.tasks == 1 {
            return Ok(Transformed::yes(Arc::clone(&input_stage.plan)));
        }

        // 2) Scale up the head node of the input stage in order to account for the amount of partition
        //    and consumer count above it.
        let input_plan = nb
            .producer_head(task_count)?
            .insert(Arc::clone(&input_stage.plan))?;

        // 3) Make sure the input stage can be uniquely identified with a stage index and query id.
        //    If there were already some `query_id` and `num` that's fine.
        let mut nb = nb.with_input_stage(Stage::Local(LocalStage {
            query_id,
            num: stage_id,
            plan: input_plan.clone(),
            tasks: input_stage.tasks,
            metrics_set: Default::default(),
        }))?;

        // When a range shuffle feeds consumer tasks (1 partition per task),
        // scale the boundary's consumer-stage partition count down to 1 so its properties align
        // with the per-task execution in the consumer stage.
        if let Some(shuffle) = plan.downcast_ref::<NetworkShuffleExec>()
            && let Partitioning::Range(_) = &shuffle.properties.partitioning
        {
            let mut shuffle_clone = shuffle.clone();
            shuffle_clone.worker_connections =
                crate::worker::WorkerConnectionPool::new(input_stage.tasks);
            shuffle_clone.input_stage = Stage::Local(LocalStage {
                query_id,
                num: stage_id,
                plan: input_plan,
                tasks: input_stage.tasks,
                metrics_set: Default::default(),
            });
            shuffle_clone.properties = Arc::new(
                PlanProperties::clone(&shuffle.properties)
                    .with_partitioning(Partitioning::UnknownPartitioning(1)),
            );
            nb = Arc::new(shuffle_clone);
        }

        stage_id += 1;
        Ok(Transformed::yes(nb))
    })?;

    Ok(transformed.data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plans::ChildrenIsolatorUnionExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::ScalarValue;
    use datafusion::physical_expr::expressions::Column;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, RangePartitioning, SplitPoint};
    use datafusion::physical_plan::empty::EmptyExec;
    use datafusion::physical_plan::repartition::RepartitionExec;

    #[test]
    fn test_prepare_network_boundaries_scales_range_shuffle_properties() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            Default::default(),
        )])
        .unwrap();
        let splits = vec![
            SplitPoint::new(vec![ScalarValue::Int64(Some(10))]),
            SplitPoint::new(vec![ScalarValue::Int64(Some(20))]),
        ];
        let range = RangePartitioning::try_new(ordering, splits).unwrap();
        let empty = Arc::new(EmptyExec::new(schema));
        let repart = Arc::new(RepartitionExec::try_new(empty, Partitioning::Range(range)).unwrap());
        let shuffle = Arc::new(NetworkShuffleExec::try_new(repart, 3).unwrap());

        use crate::execution_plans::ChildWeight;

        // Wrap in ChildrenIsolatorUnionExec with 3 tasks
        let ciu = Arc::new(
            ChildrenIsolatorUnionExec::from_children_and_weights(
                vec![Arc::clone(&shuffle) as Arc<dyn ExecutionPlan>],
                vec![ChildWeight::desired(1.0)],
                3,
            )
            .unwrap(),
        );

        let prepared = prepare_network_boundaries(ciu).unwrap();
        let ciu_prepared = prepared
            .downcast_ref::<ChildrenIsolatorUnionExec>()
            .unwrap();
        let child = &ciu_prepared.children()[0];
        let shuffle_prepared = child.downcast_ref::<NetworkShuffleExec>().unwrap();
        assert_eq!(
            shuffle_prepared
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
        assert!(matches!(
            shuffle_prepared.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(1)
        ));
    }

    #[test]
    fn test_prepare_network_boundaries_scales_range_shuffle_properties_with_fewer_tasks() {
        use crate::execution_plans::ChildWeight;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            Arc::new(Column::new("a", 0)),
            Default::default(),
        )])
        .unwrap();
        let splits = vec![
            SplitPoint::new(vec![ScalarValue::Int64(Some(10))]),
            SplitPoint::new(vec![ScalarValue::Int64(Some(20))]),
        ];
        let range = RangePartitioning::try_new(ordering, splits).unwrap();
        let empty = Arc::new(EmptyExec::new(schema));
        let repart = Arc::new(RepartitionExec::try_new(empty, Partitioning::Range(range)).unwrap());
        let shuffle = Arc::new(NetworkShuffleExec::try_new(repart, 3).unwrap());

        // Wrap in ChildrenIsolatorUnionExec with 2 tasks (scaling 3 range partitions down to 2 consumer tasks)
        let ciu = Arc::new(
            ChildrenIsolatorUnionExec::from_children_and_weights(
                vec![Arc::clone(&shuffle) as Arc<dyn ExecutionPlan>],
                vec![ChildWeight::desired(1.0)],
                2,
            )
            .unwrap(),
        );

        let prepared = prepare_network_boundaries(ciu).unwrap();
        let ciu_prepared = prepared
            .downcast_ref::<ChildrenIsolatorUnionExec>()
            .unwrap();
        let child = &ciu_prepared.children()[0];
        let shuffle_prepared = child.downcast_ref::<NetworkShuffleExec>().unwrap();
        assert_eq!(
            shuffle_prepared
                .properties()
                .output_partitioning()
                .partition_count(),
            1
        );
        assert!(matches!(
            shuffle_prepared.properties().output_partitioning(),
            Partitioning::UnknownPartitioning(1)
        ));

        // The input stage plan should have been scaled to 2 partitions
        let Stage::Local(local_stage) = &shuffle_prepared.input_stage else {
            panic!("expected Local stage");
        };
        assert_eq!(
            local_stage
                .plan
                .properties()
                .output_partitioning()
                .partition_count(),
            2
        );
    }
}
