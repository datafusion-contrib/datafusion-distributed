use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use std::path::Path;
use std::sync::Arc;

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

    pub(crate) fn information_schema(mut self, enabled: bool) -> Self {
        self.config = self.config.with_information_schema(enabled);
        self
    }

    pub(crate) fn prefer_hash_joins(mut self, prefer_hash_joins: bool) -> Self {
        self.config.options_mut().optimizer.prefer_hash_join = prefer_hash_joins;
        self
    }

    pub(crate) fn hash_join_single_partition_threshold(mut self, v: usize) -> Self {
        self.config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold = v;
        self
    }

    pub(crate) fn hash_join_single_partition_threshold_rows(mut self, v: usize) -> Self {
        self.config
            .options_mut()
            .optimizer
            .hash_join_single_partition_threshold_rows = v;
        self
    }

    pub(crate) async fn physical_plan(&self, query: &str) -> Arc<dyn ExecutionPlan> {
        let ctx = SessionContext::new_with_config(self.config.clone());
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
        ctx.sql(query)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap()
    }
}

fn workspace_root() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
}
