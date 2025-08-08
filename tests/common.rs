use datafusion_distributed::test_utils::tpch;

use tokio::sync::OnceCell;

pub fn get_test_data_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tpch/data")
}

pub fn get_test_queries_dir() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata/tpch/queries")
}

pub fn get_test_tpch_query(num: u8) -> String {
    let queries_dir = get_test_queries_dir();
    tpch::tpch_query_from_dir(&queries_dir, num)
}

// OnceCell to ensure TPCH tables are generated only once for tests
static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

// ensure_tpch_data initializes the TPCH data on disk.
pub async fn ensure_tpch_data() {
    INIT_TEST_TPCH_TABLES
        .get_or_init(|| async {
            tpch::generate_tpch_data(&get_test_data_dir());
        })
        .await;
}
