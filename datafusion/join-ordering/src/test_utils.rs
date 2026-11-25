use datafusion::physical_plan::display::DisplayableExecutionPlan;
use std::fmt::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::OnceCell;

use crate::common::get_join;
use crate::graph::{Graph, parse_graph};
use datafusion::physical_expr_common::physical_expr::fmt_sql;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_datasets::output::TESTDATA_ROOT;
use datafusion_datasets::parquet_layout::ParquetLayout;
use datafusion_datasets::{register_tables, tpch};

/// Same as `insta::assert_snapshot!`, but with the repository root stripped from file paths.
#[macro_export]
macro_rules! assert_snapshot {
    ($($arg:tt)*) => {
        $crate::test_utils::insta_settings().bind(|| {
            insta::assert_snapshot!($($arg)*);
        })
    };
}

pub(crate) fn insta_settings() -> insta::Settings {
    let root = workspace_root().canonicalize().unwrap();
    let mut settings = insta::Settings::clone_current();
    settings.add_filter(root.to_str().unwrap().trim_start_matches("/"), "");
    // File groups are not relevant for join ordering and make plans hard to read.
    settings.add_filter(r"file_groups=\{[^}]*\}, ", "");
    settings
}

/// Plans SQL queries with upstream DataFusion over the `weather` parquet table.
pub(crate) struct TestPlanBuilder {
    config: SessionConfig,
}

impl TestPlanBuilder {
    pub(crate) fn new() -> Self {
        Self {
            config: SessionConfig::new(),
        }
    }

    pub(crate) fn target_partitions(mut self, target_partitions: usize) -> Self {
        self.config = self.config.with_target_partitions(target_partitions);
        self
    }

    pub(crate) async fn physical_plan(&self, query: &str) -> TestPlan {
        let ctx = SessionContext::new_with_config(self.config.clone());
        // register weather parquet test data.
        let weather = workspace_root()
            .join("testdata/weather")
            .canonicalize()
            .unwrap();
        ctx.register_parquet(
            "weather",
            weather.to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();
        // register tpch test data.
        register_tables(&ctx, &tpch_data().await).await.unwrap();
        let plan = ctx
            .sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        TestPlan { plan }
    }
}

pub(crate) struct TestPlan {
    plan: Arc<dyn ExecutionPlan>,
}

impl TestPlan {
    pub(crate) fn display(&self) -> String {
        DisplayableExecutionPlan::new(self.plan.as_ref())
            .indent(true)
            .to_string()
    }

    pub(crate) fn into_graph(self) -> TestGraph {
        let graph = parse_graph(&self.plan).unwrap().unwrap();
        TestGraph { graph }
    }
}

pub(crate) struct TestGraph {
    graph: Graph,
}

impl TestGraph {
    /// Renders the graph as one line per relation followed by one line per hyper-edge. Relations
    /// are named `R<n>` after their bit position, so the first parsed relation is `R0`:
    ///
    /// ```text
    /// Relations:
    ///   R0: <plan name> [<output columns>]
    /// Hyper-edges:
    ///   {R0, R1} <join type> on <left> = <right> AND ... (<join plan name>)
    /// ```
    pub(crate) fn display(&self) -> String {
        let mut out = String::new();
        writeln!(out, "Relations:").unwrap();
        for vertex in &self.graph.vertices {
            let columns = vertex
                .plan
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().as_str())
                .collect::<Vec<_>>()
                .join(", ");
            let id = rel_name(vertex.rel.0);
            let name = vertex.plan.name();
            writeln!(out, "  {id}: {name} [{columns}]").unwrap();
        }
        writeln!(out, "Hyper-edges:").unwrap();
        for edge in &self.graph.hyper_edges {
            let join = get_join(&edge.plan).expect("hyper-edge must point to a join");
            let on = join
                .exprs()
                .iter()
                .map(|(l, r)| format!("{} = {}", fmt_sql(l.as_ref()), fmt_sql(r.as_ref())))
                .collect::<Vec<_>>()
                .join(" AND ");
            let ses = rel_set_name(edge.ses.0);
            write!(out, "  {ses} {:?}", edge.join_type).unwrap();
            if !on.is_empty() {
                write!(out, " on {on}").unwrap();
            }
            writeln!(out, " ({})", edge.plan.name()).unwrap();
        }
        out
    }
}

/// `R<n>`, where `n` is the position of the single bit set in a [crate::graph::Rel].
fn rel_name(bits: u64) -> String {
    format!("R{}", bits.trailing_zeros())
}

/// `{R<n>, R<m>, ...}` for every bit set in a [crate::graph::RelSet].
fn rel_set_name(bits: u64) -> String {
    let rels = (0..u64::BITS)
        .filter(|i| bits & (1 << i) != 0)
        .map(|i| rel_name(1 << i))
        .collect::<Vec<_>>()
        .join(", ");
    format!("{{{rels}}}")
}

fn workspace_root() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}

// OnceCell to ensure TPCH tables are generated only once for tests
static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

async fn tpch_data() -> std::path::PathBuf {
    let spec = tpch::TpchSpec {
        location: TESTDATA_ROOT.to_string(),
        scale_factor: 0.1,
        layout: ParquetLayout::default(),
    };
    let data_dir = std::path::PathBuf::from(spec.dataset());
    INIT_TEST_TPCH_TABLES
        .get_or_init(|| async {
            tpch::generate_data(&spec)
                .await
                .expect("Failed to generate TPC-H data");
        })
        .await;
    data_dir
}
