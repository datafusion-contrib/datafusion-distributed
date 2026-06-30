use crate::config_extension_ext::set_distributed_option_extension;
use crate::{DistributedConfig, DistributedTaskContext};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::memory::DataSourceExec;
use datafusion::common::tree_node::TreeNode;
use datafusion::common::{HashMap, HashSet, Result, internal_datafusion_err};
use datafusion::datasource::physical_plan::FileScanConfig;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::prelude::SessionConfig;
use std::any::Any;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::sync::Arc;

/// A filter expression and the schema required to decode its serialized form.
#[derive(Clone, Debug)]
pub struct FilterExpression {
    pub expr: Arc<dyn PhysicalExpr>,
    pub input_schema: SchemaRef,
}

impl FilterExpression {
    pub fn new(expr: Arc<dyn PhysicalExpr>, input_schema: SchemaRef) -> Self {
        Self { expr, input_schema }
    }
}

/// Extension trait for plan nodes that expose filter expressions.
pub trait PlanFilterExpressions: ExecutionPlan {
    fn get_filter_expressions(&self) -> Vec<FilterExpression>;
}

trait PlanFilterExpressionExtractor: Send + Sync {
    fn get_filter_expressions(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        dt_ctx: DistributedTaskContext,
    ) -> Vec<FilterExpression>;
}

struct TypedExtractor<T>(PhantomData<T>);

impl<T> PlanFilterExpressionExtractor for TypedExtractor<T>
where
    T: PlanFilterExpressions + 'static,
{
    fn get_filter_expressions(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        _dt_ctx: DistributedTaskContext,
    ) -> Vec<FilterExpression> {
        plan.downcast_ref::<T>()
            .map(PlanFilterExpressions::get_filter_expressions)
            .unwrap_or_default()
    }
}

/// Registry of plan node types that can expose filter expressions.
#[derive(Clone)]
pub struct PlanFilterExpressionRegistry {
    extractors: Vec<Arc<dyn PlanFilterExpressionExtractor>>,
}

impl Default for PlanFilterExpressionRegistry {
    fn default() -> Self {
        let mut registry = Self {
            extractors: Vec::new(),
        };
        registry.register::<FilterExec>();
        registry.register::<HashJoinExec>();
        registry.register::<AggregateExec>();
        registry.register::<SortExec>();
        registry.register::<DataSourceExec>();
        registry
    }
}

impl Debug for PlanFilterExpressionRegistry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "PlanFilterExpressionRegistry")
    }
}

impl PlanFilterExpressionRegistry {
    pub fn register<T>(&mut self)
    where
        T: PlanFilterExpressions + 'static,
    {
        self.extractors
            .push(Arc::new(TypedExtractor::<T>(PhantomData)));
    }

    pub fn get_filter_expressions(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
        dt_ctx: DistributedTaskContext,
    ) -> Vec<FilterExpression> {
        let mut out = Vec::new();
        for extractor in &self.extractors {
            out.extend(extractor.get_filter_expressions(plan, dt_ctx));
        }
        out
    }
}

impl PlanFilterExpressions for FilterExec {
    fn get_filter_expressions(&self) -> Vec<FilterExpression> {
        vec![FilterExpression::new(
            Arc::clone(self.predicate()),
            self.input().schema(),
        )]
    }
}

impl PlanFilterExpressions for HashJoinExec {
    fn get_filter_expressions(&self) -> Vec<FilterExpression> {
        self.dynamic_filter_expr()
            .map(|expr| {
                let expr: Arc<dyn PhysicalExpr> = expr.clone();
                FilterExpression::new(expr, self.right().schema())
            })
            .into_iter()
            .collect()
    }
}

impl PlanFilterExpressions for AggregateExec {
    fn get_filter_expressions(&self) -> Vec<FilterExpression> {
        self.dynamic_filter_expr()
            .map(|expr| {
                let expr: Arc<dyn PhysicalExpr> = expr.clone();
                FilterExpression::new(expr, self.input().schema())
            })
            .into_iter()
            .collect()
    }
}

impl PlanFilterExpressions for SortExec {
    fn get_filter_expressions(&self) -> Vec<FilterExpression> {
        self.dynamic_filter_expr()
            .map(|expr| {
                let expr: Arc<dyn PhysicalExpr> = expr;
                FilterExpression::new(expr, self.input().schema())
            })
            .into_iter()
            .collect()
    }
}

impl PlanFilterExpressions for DataSourceExec {
    fn get_filter_expressions(&self) -> Vec<FilterExpression> {
        let Some(file_scan) = self.data_source().downcast_ref::<FileScanConfig>() else {
            return Vec::new();
        };
        let table_schema = Arc::clone(file_scan.file_source().table_schema().table_schema());
        file_scan
            .file_source()
            .filter()
            .map(|expr| FilterExpression::new(expr, table_schema))
            .into_iter()
            .collect()
    }
}

pub(crate) fn collect_dynamic_filters(
    filters: impl IntoIterator<Item = FilterExpression>,
) -> Result<Vec<Arc<DynamicFilterPhysicalExpr>>> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for filter in filters {
        filter.expr.apply(|expr| {
            if expr.downcast_ref::<DynamicFilterPhysicalExpr>().is_some()
                && let Some(id) = expr.expression_id()
                && seen.insert(id)
            {
                out.push(downcast_dynamic_filter(Arc::clone(expr))?);
            }
            Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
        })?;
    }

    Ok(out)
}

pub(crate) fn collect_dynamic_filters_by_id(
    filters: impl IntoIterator<Item = FilterExpression>,
) -> Result<HashMap<u64, Arc<DynamicFilterPhysicalExpr>>> {
    let mut out = HashMap::new();
    for filter in collect_dynamic_filters(filters)? {
        let Some(id) = filter.expression_id() else {
            continue;
        };
        out.insert(id, filter);
    }
    Ok(out)
}

pub(crate) fn downcast_dynamic_filter(
    expr: Arc<dyn PhysicalExpr>,
) -> Result<Arc<DynamicFilterPhysicalExpr>> {
    let expr = expr as Arc<dyn Any + Send + Sync>;
    expr.downcast::<DynamicFilterPhysicalExpr>().map_err(|_| {
        internal_datafusion_err!("PhysicalExpr did not downcast to DynamicFilterPhysicalExpr")
    })
}

pub(crate) fn set_distributed_filter_expressions<T>(cfg: &mut SessionConfig)
where
    T: PlanFilterExpressions + 'static,
{
    let opts = cfg.options_mut();
    if let Some(distributed_cfg) = opts.extensions.get_mut::<DistributedConfig>() {
        distributed_cfg
            .__private_filter_expression_registry
            .register::<T>();
    } else {
        let mut registry = PlanFilterExpressionRegistry::default();
        registry.register::<T>();
        set_distributed_option_extension(
            cfg,
            DistributedConfig {
                __private_filter_expression_registry: registry,
                ..Default::default()
            },
        )
    }
}
