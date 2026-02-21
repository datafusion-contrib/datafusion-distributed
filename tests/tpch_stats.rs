#[cfg(all(feature = "integration", feature = "tpch", test))]
mod tests {
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::assert_snapshot;
    use datafusion_distributed::test_utils::stats_vs_metrics::{
        StatsVsMetricsDisplayOptions, stats_vs_metrics_display,
    };
    use datafusion_distributed::test_utils::{benchmarks_common, tpch};
    use std::error::Error;
    use std::fs;
    use std::path::Path;
    use tokio::sync::OnceCell;

    const TPCH_SCALE_FACTOR: f64 = 1.0;
    const TPCH_DATA_PARTS: i32 = 16;

    #[tokio::test]
    async fn test_tpch_1() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q1").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=1200243 vs 4 (99%)
          SortExec: output_rows=1200243 vs 4 (99%)
            ProjectionExec: output_rows=1200243 vs 4 (99%)
              AggregateExec: output_rows=1200243 vs 4 (99%)
                RepartitionExec: output_rows=1200243 vs 48 (99%)
                  AggregateExec: output_rows=1200243 vs 48 (99%)
                    ProjectionExec: output_rows=1200243 vs 5916591 (79%)
                      FilterExec: output_rows=1200243 vs 5916591 (79%)
                        DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_2() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q2").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=0 vs 117760 (100%)
          SortExec: output_rows=0 vs 117760 (100%)
            ProjectionExec: output_rows=0 vs 117760 (100%)
              HashJoinExec: output_rows=0 vs 117760 (100%)
                RepartitionExec: output_rows=0 vs 164352 (100%)
                  HashJoinExec: output_rows=0 vs 164352 (100%)
                    CoalescePartitionsExec: output_rows=16 vs 16 (0%)
                      FilterExec: output_rows=16 vs 16 (0%)
                        DataSourceExec: output_rows=80 vs 80 (0%)
                    ProjectionExec: output_rows=20 vs 47808 (99%)
                      HashJoinExec: output_rows=20 vs 47808 (99%)
                        CoalescePartitionsExec: output_rows=400 vs 400 (0%)
                          DataSourceExec: output_rows=400 vs 400 (0%)
                        ProjectionExec: output_rows=500 vs 2988 (83%)
                          HashJoinExec: output_rows=500 vs 2988 (83%)
                            CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                              DataSourceExec: output_rows=10000 vs 10000 (0%)
                            HashJoinExec: output_rows=40000 vs 2988 (92%)
                              CoalescePartitionsExec: output_rows=40000 vs 747 (98%)
                                FilterExec: output_rows=40000 vs 747 (98%)
                                  DataSourceExec: output_rows=200000 vs 200000 (0%)
                              DataSourceExec: output_rows=800000 vs 800000 (0%)
                RepartitionExec: output_rows=16 vs 117422 (99%)
                  ProjectionExec: output_rows=16 vs 117422 (99%)
                    AggregateExec: output_rows=16 vs 117422 (99%)
                      RepartitionExec: output_rows=16 vs 117422 (99%)
                        AggregateExec: output_rows=16 vs 117422 (99%)
                          HashJoinExec: output_rows=16 vs 40693760 (99%)
                            CoalescePartitionsExec: output_rows=16 vs 16 (0%)
                              FilterExec: output_rows=16 vs 16 (0%)
                                DataSourceExec: output_rows=80 vs 80 (0%)
                            ProjectionExec: output_rows=400 vs 12800000 (99%)
                              HashJoinExec: output_rows=400 vs 12800000 (99%)
                                CoalescePartitionsExec: output_rows=400 vs 400 (0%)
                                  DataSourceExec: output_rows=400 vs 400 (0%)
                                ProjectionExec: output_rows=10000 vs 800000 (98%)
                                  HashJoinExec: output_rows=10000 vs 800000 (98%)
                                    CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                                      DataSourceExec: output_rows=10000 vs 10000 (0%)
                                    DataSourceExec: output_rows=800000 vs 800000 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_3() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q3").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=1200 vs 11620 (89%)
          SortExec: output_rows=1200 vs 11620 (89%)
            ProjectionExec: output_rows=1200 vs 11620 (89%)
              AggregateExec: output_rows=1200 vs 11620 (89%)
                RepartitionExec: output_rows=1200 vs 11620 (89%)
                  AggregateExec: output_rows=1200 vs 11620 (89%)
                    HashJoinExec: output_rows=1200 vs 30519 (96%)
                      CoalescePartitionsExec: output_rows=6000 vs 147126 (95%)
                        HashJoinExec: output_rows=6000 vs 147126 (95%)
                          CoalescePartitionsExec: output_rows=30000 vs 30142 (0%)
                            FilterExec: output_rows=30000 vs 30142 (0%)
                              DataSourceExec: output_rows=150000 vs 150000 (0%)
                          FilterExec: output_rows=300000 vs 727305 (58%)
                            DataSourceExec: output_rows=1500000 vs 1500000 (0%)
                      FilterExec: output_rows=1200243 vs 3241776 (62%)
                        DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_4() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q4").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=300000 vs 5 (99%)
          SortExec: output_rows=300000 vs 5 (99%)
            ProjectionExec: output_rows=300000 vs 5 (99%)
              AggregateExec: output_rows=300000 vs 5 (99%)
                RepartitionExec: output_rows=300000 vs 80 (99%)
                  AggregateExec: output_rows=300000 vs 80 (99%)
                    HashJoinExec: output_rows=300000 vs 52523 (82%)
                      RepartitionExec: output_rows=300000 vs 57218 (80%)
                        FilterExec: output_rows=300000 vs 57218 (80%)
                          DataSourceExec: output_rows=1500000 vs 1500000 (0%)
                      RepartitionExec: output_rows=1200243 vs 3793296 (68%)
                        FilterExec: output_rows=1200243 vs 3793296 (68%)
                          DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_5() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q5").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=3 vs 5 (40%)
          SortExec: output_rows=3 vs 5 (40%)
            ProjectionExec: output_rows=3 vs 5 (40%)
              AggregateExec: output_rows=3 vs 5 (40%)
                RepartitionExec: output_rows=3 vs 80 (96%)
                  AggregateExec: output_rows=3 vs 80 (96%)
                    HashJoinExec: output_rows=3 vs 1854208 (99%)
                      CoalescePartitionsExec: output_rows=16 vs 16 (0%)
                        FilterExec: output_rows=16 vs 16 (0%)
                          DataSourceExec: output_rows=80 vs 80 (0%)
                      ProjectionExec: output_rows=80 vs 579776 (99%)
                        HashJoinExec: output_rows=80 vs 579776 (99%)
                          CoalescePartitionsExec: output_rows=400 vs 400 (0%)
                            DataSourceExec: output_rows=400 vs 400 (0%)
                          ProjectionExec: output_rows=2000 vs 36236 (94%)
                            HashJoinExec: output_rows=2000 vs 36236 (94%)
                              CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                                DataSourceExec: output_rows=10000 vs 10000 (0%)
                              HashJoinExec: output_rows=30000 vs 910519 (96%)
                                RepartitionExec: output_rows=30000 vs 227597 (86%)
                                  HashJoinExec: output_rows=30000 vs 227597 (86%)
                                    RepartitionExec: output_rows=150000 vs 150000 (0%)
                                      DataSourceExec: output_rows=150000 vs 150000 (0%)
                                    RepartitionExec: output_rows=300000 vs 227597 (24%)
                                      FilterExec: output_rows=300000 vs 227597 (24%)
                                        DataSourceExec: output_rows=1500000 vs 1500000 (0%)
                                RepartitionExec: output_rows=6001215 vs 6001215 (0%)
                                  DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_6() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q6").await?;
        assert_snapshot!(display, @r"
        ProjectionExec: output_rows=1 vs 1 (0%)
          AggregateExec: output_rows=1 vs 1 (0%)
            CoalescePartitionsExec: output_rows=1200243 vs 16 (99%)
              AggregateExec: output_rows=1200243 vs 16 (99%)
                FilterExec: output_rows=1200243 vs 114160 (90%)
                  DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_7() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q7").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=0 vs 4 (100%)
          SortExec: output_rows=0 vs 4 (100%)
            ProjectionExec: output_rows=0 vs 4 (100%)
              AggregateExec: output_rows=0 vs 4 (100%)
                RepartitionExec: output_rows=0 vs 64 (100%)
                  AggregateExec: output_rows=0 vs 64 (100%)
                    ProjectionExec: output_rows=0 vs 1516544 (100%)
                      HashJoinExec: output_rows=0 vs 1516544 (100%)
                        CoalescePartitionsExec: output_rows=80 vs 32 (60%)
                          FilterExec: output_rows=80 vs 32 (60%)
                            DataSourceExec: output_rows=400 vs 400 (0%)
                        ProjectionExec: output_rows=0 vs 2331248 (100%)
                          HashJoinExec: output_rows=0 vs 2331248 (100%)
                            CoalescePartitionsExec: output_rows=80 vs 32 (60%)
                              FilterExec: output_rows=80 vs 32 (60%)
                                DataSourceExec: output_rows=400 vs 400 (0%)
                            ProjectionExec: output_rows=49 vs 1828450 (99%)
                              HashJoinExec: output_rows=49 vs 1828450 (99%)
                                RepartitionExec: output_rows=150000 vs 150000 (0%)
                                  DataSourceExec: output_rows=150000 vs 150000 (0%)
                                RepartitionExec: output_rows=499 vs 1828450 (99%)
                                  ProjectionExec: output_rows=499 vs 1828450 (99%)
                                    HashJoinExec: output_rows=499 vs 1828450 (99%)
                                      RepartitionExec: output_rows=1500000 vs 1500000 (0%)
                                        DataSourceExec: output_rows=1500000 vs 1500000 (0%)
                                      RepartitionExec: output_rows=2000 vs 1828450 (99%)
                                        HashJoinExec: output_rows=2000 vs 1828450 (99%)
                                          CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                                            DataSourceExec: output_rows=10000 vs 10000 (0%)
                                          FilterExec: output_rows=1200243 vs 1828450 (34%)
                                            DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_8() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q8").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=0 vs 2 (100%)
          SortExec: output_rows=0 vs 2 (100%)
            ProjectionExec: output_rows=0 vs 2 (100%)
              AggregateExec: output_rows=0 vs 2 (100%)
                RepartitionExec: output_rows=0 vs 32 (100%)
                  AggregateExec: output_rows=0 vs 32 (100%)
                    ProjectionExec: output_rows=0 vs 10661888 (100%)
                      HashJoinExec: output_rows=0 vs 10661888 (100%)
                        CoalescePartitionsExec: output_rows=16 vs 16 (0%)
                          FilterExec: output_rows=16 vs 16 (0%)
                            DataSourceExec: output_rows=80 vs 80 (0%)
                        ProjectionExec: output_rows=0 vs 3427584 (100%)
                          HashJoinExec: output_rows=0 vs 3427584 (100%)
                            CoalescePartitionsExec: output_rows=400 vs 400 (0%)
                              DataSourceExec: output_rows=400 vs 400 (0%)
                            ProjectionExec: output_rows=0 vs 214224 (100%)
                              HashJoinExec: output_rows=0 vs 214224 (100%)
                                CoalescePartitionsExec: output_rows=400 vs 400 (0%)
                                  DataSourceExec: output_rows=400 vs 400 (0%)
                                ProjectionExec: output_rows=0 vs 13389 (100%)
                                  HashJoinExec: output_rows=0 vs 13389 (100%)
                                    RepartitionExec: output_rows=150000 vs 150000 (0%)
                                      DataSourceExec: output_rows=150000 vs 150000 (0%)
                                    RepartitionExec: output_rows=3 vs 13389 (99%)
                                      ProjectionExec: output_rows=3 vs 13389 (99%)
                                        HashJoinExec: output_rows=3 vs 13389 (99%)
                                          RepartitionExec: output_rows=300000 vs 457263 (34%)
                                            FilterExec: output_rows=300000 vs 457263 (34%)
                                              DataSourceExec: output_rows=1500000 vs 1500000 (0%)
                                          RepartitionExec: output_rows=66 vs 43693 (99%)
                                            ProjectionExec: output_rows=66 vs 43693 (99%)
                                              HashJoinExec: output_rows=66 vs 43693 (99%)
                                                CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                                                  DataSourceExec: output_rows=10000 vs 10000 (0%)
                                                HashJoinExec: output_rows=40000 vs 43693 (8%)
                                                  CoalescePartitionsExec: output_rows=40000 vs 1451 (96%)
                                                    FilterExec: output_rows=40000 vs 1451 (96%)
                                                      DataSourceExec: output_rows=200000 vs 200000 (0%)
                                                  DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_9() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q9").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=0 vs 175 (100%)
          SortExec: output_rows=0 vs 175 (100%)
            ProjectionExec: output_rows=0 vs 175 (100%)
              AggregateExec: output_rows=0 vs 175 (100%)
                RepartitionExec: output_rows=0 vs 2800 (100%)
                  AggregateExec: output_rows=0 vs 2800 (100%)
                    ProjectionExec: output_rows=0 vs 5110464 (100%)
                      HashJoinExec: output_rows=0 vs 5110464 (100%)
                        CoalescePartitionsExec: output_rows=400 vs 400 (0%)
                          DataSourceExec: output_rows=400 vs 400 (0%)
                        ProjectionExec: output_rows=1 vs 319404 (99%)
                          HashJoinExec: output_rows=1 vs 319404 (99%)
                            RepartitionExec: output_rows=1500000 vs 1500000 (0%)
                              DataSourceExec: output_rows=1500000 vs 1500000 (0%)
                            RepartitionExec: output_rows=8 vs 319404 (99%)
                              ProjectionExec: output_rows=8 vs 319404 (99%)
                                HashJoinExec: output_rows=8 vs 319404 (99%)
                                  RepartitionExec: output_rows=800000 vs 800000 (0%)
                                    DataSourceExec: output_rows=800000 vs 800000 (0%)
                                  RepartitionExec: output_rows=66 vs 319404 (99%)
                                    ProjectionExec: output_rows=66 vs 319404 (99%)
                                      HashJoinExec: output_rows=66 vs 319404 (99%)
                                        CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                                          DataSourceExec: output_rows=10000 vs 10000 (0%)
                                        HashJoinExec: output_rows=40000 vs 319404 (87%)
                                          CoalescePartitionsExec: output_rows=40000 vs 10664 (73%)
                                            FilterExec: output_rows=40000 vs 10664 (73%)
                                              DataSourceExec: output_rows=200000 vs 200000 (0%)
                                          DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_10() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q10").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=16 vs 37967 (99%)
          SortExec: output_rows=16 vs 37967 (99%)
            ProjectionExec: output_rows=16 vs 37967 (99%)
              AggregateExec: output_rows=16 vs 37967 (99%)
                RepartitionExec: output_rows=16 vs 48177 (99%)
                  AggregateExec: output_rows=16 vs 48177 (99%)
                    ProjectionExec: output_rows=16 vs 1835280 (99%)
                      HashJoinExec: output_rows=16 vs 1835280 (99%)
                        CoalescePartitionsExec: output_rows=400 vs 400 (0%)
                          DataSourceExec: output_rows=400 vs 400 (0%)
                        HashJoinExec: output_rows=6000 vs 114705 (94%)
                          RepartitionExec: output_rows=30000 vs 57069 (47%)
                            HashJoinExec: output_rows=30000 vs 57069 (47%)
                              RepartitionExec: output_rows=150000 vs 150000 (0%)
                                DataSourceExec: output_rows=150000 vs 150000 (0%)
                              RepartitionExec: output_rows=300000 vs 57069 (80%)
                                FilterExec: output_rows=300000 vs 57069 (80%)
                                  DataSourceExec: output_rows=1500000 vs 1500000 (0%)
                          RepartitionExec: output_rows=1200243 vs 1478870 (18%)
                            FilterExec: output_rows=1200243 vs 1478870 (18%)
                              DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_11() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q11").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=81 vs 1048 (92%)
          SortExec: output_rows=81 vs 1048 (92%)
            ProjectionExec: output_rows=81 vs 1048 (92%)
              NestedLoopJoinExec: output_rows=81 vs 1048 (92%)
                ProjectionExec: output_rows=1 vs 1 (0%)
                  AggregateExec: output_rows=1 vs 1 (0%)
                    CoalescePartitionsExec: output_rows=80 vs 16 (80%)
                      AggregateExec: output_rows=80 vs 16 (80%)
                        HashJoinExec: output_rows=80 vs 506880 (99%)
                          CoalescePartitionsExec: output_rows=80 vs 16 (80%)
                            FilterExec: output_rows=80 vs 16 (80%)
                              DataSourceExec: output_rows=400 vs 400 (0%)
                          ProjectionExec: output_rows=10000 vs 800000 (98%)
                            HashJoinExec: output_rows=10000 vs 800000 (98%)
                              CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                                DataSourceExec: output_rows=10000 vs 10000 (0%)
                              DataSourceExec: output_rows=800000 vs 800000 (0%)
                ProjectionExec: output_rows=80 vs 29818 (99%)
                  AggregateExec: output_rows=80 vs 29818 (99%)
                    RepartitionExec: output_rows=80 vs 29818 (99%)
                      AggregateExec: output_rows=80 vs 29818 (99%)
                        HashJoinExec: output_rows=80 vs 506880 (99%)
                          CoalescePartitionsExec: output_rows=80 vs 16 (80%)
                            FilterExec: output_rows=80 vs 16 (80%)
                              DataSourceExec: output_rows=400 vs 400 (0%)
                          ProjectionExec: output_rows=10000 vs 800000 (98%)
                            HashJoinExec: output_rows=10000 vs 800000 (98%)
                              CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                                DataSourceExec: output_rows=10000 vs 10000 (0%)
                              DataSourceExec: output_rows=800000 vs 800000 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_12() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q12").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=300000 vs 2 (99%)
          SortExec: output_rows=300000 vs 2 (99%)
            ProjectionExec: output_rows=300000 vs 2 (99%)
              AggregateExec: output_rows=300000 vs 2 (99%)
                RepartitionExec: output_rows=300000 vs 32 (99%)
                  AggregateExec: output_rows=300000 vs 32 (99%)
                    HashJoinExec: output_rows=300000 vs 30988 (89%)
                      RepartitionExec: output_rows=1200243 vs 30988 (97%)
                        FilterExec: output_rows=1200243 vs 30988 (97%)
                          DataSourceExec: output_rows=6001215 vs 6001215 (0%)
                      RepartitionExec: output_rows=1500000 vs 1500000 (0%)
                        DataSourceExec: output_rows=1500000 vs 1500000 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_13() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q13").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=150000 vs 42 (99%)
          SortExec: output_rows=150000 vs 42 (99%)
            ProjectionExec: output_rows=150000 vs 42 (99%)
              AggregateExec: output_rows=150000 vs 42 (99%)
                RepartitionExec: output_rows=150000 vs 596 (99%)
                  AggregateExec: output_rows=150000 vs 596 (99%)
                    ProjectionExec: output_rows=150000 vs 150000 (0%)
                      AggregateExec: output_rows=150000 vs 150000 (0%)
                        HashJoinExec: output_rows=150000 vs 1533923 (90%)
                          RepartitionExec: output_rows=150000 vs 150000 (0%)
                            DataSourceExec: output_rows=150000 vs 150000 (0%)
                          RepartitionExec: output_rows=300000 vs 1483918 (79%)
                            FilterExec: output_rows=300000 vs 1483918 (79%)
                              DataSourceExec: output_rows=1500000 vs 1500000 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_14() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q14").await?;
        assert_snapshot!(display, @r"
        ProjectionExec: output_rows=1 vs 1 (0%)
          AggregateExec: output_rows=1 vs 1 (0%)
            CoalescePartitionsExec: output_rows=40000 vs 16 (99%)
              AggregateExec: output_rows=40000 vs 16 (99%)
                ProjectionExec: output_rows=40000 vs 75983 (47%)
                  HashJoinExec: output_rows=40000 vs 75983 (47%)
                    RepartitionExec: output_rows=200000 vs 200000 (0%)
                      DataSourceExec: output_rows=200000 vs 200000 (0%)
                    RepartitionExec: output_rows=1200243 vs 75983 (93%)
                      FilterExec: output_rows=1200243 vs 75983 (93%)
                        DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_15() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q15").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=0 vs 1 (100%)
          SortExec: output_rows=0 vs 1 (100%)
            HashJoinExec: output_rows=0 vs 1 (100%)
              AggregateExec: output_rows=1 vs 1 (0%)
                CoalescePartitionsExec: output_rows=1200243 vs 16 (99%)
                  AggregateExec: output_rows=1200243 vs 16 (99%)
                    ProjectionExec: output_rows=1200243 vs 10000 (99%)
                      AggregateExec: output_rows=1200243 vs 10000 (99%)
                        RepartitionExec: output_rows=1200243 vs 98336 (91%)
                          AggregateExec: output_rows=1200243 vs 98336 (91%)
                            FilterExec: output_rows=1200243 vs 225954 (81%)
                              DataSourceExec: output_rows=6001215 vs 6001215 (0%)
              HashJoinExec: output_rows=10000 vs 10000 (0%)
                CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                  DataSourceExec: output_rows=10000 vs 10000 (0%)
                ProjectionExec: output_rows=1200243 vs 10000 (99%)
                  AggregateExec: output_rows=1200243 vs 10000 (99%)
                    RepartitionExec: output_rows=1200243 vs 98336 (91%)
                      AggregateExec: output_rows=1200243 vs 98336 (91%)
                        FilterExec: output_rows=1200243 vs 225954 (81%)
                          DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_16() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q16").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=40000 vs 18314 (54%)
          SortExec: output_rows=40000 vs 18314 (54%)
            ProjectionExec: output_rows=40000 vs 18314 (54%)
              AggregateExec: output_rows=40000 vs 18314 (54%)
                RepartitionExec: output_rows=40000 vs 95811 (58%)
                  AggregateExec: output_rows=40000 vs 95811 (58%)
                    AggregateExec: output_rows=40000 vs 118250 (66%)
                      RepartitionExec: output_rows=40000 vs 118271 (66%)
                        AggregateExec: output_rows=40000 vs 118271 (66%)
                          HashJoinExec: output_rows=40000 vs 118274 (66%)
                            CoalescePartitionsExec: output_rows=2000 vs 4 (99%)
                              FilterExec: output_rows=2000 vs 4 (99%)
                                DataSourceExec: output_rows=10000 vs 10000 (0%)
                            ProjectionExec: output_rows=40000 vs 118324 (66%)
                              HashJoinExec: output_rows=40000 vs 118324 (66%)
                                CoalescePartitionsExec: output_rows=40000 vs 29581 (26%)
                                  FilterExec: output_rows=40000 vs 29581 (26%)
                                    DataSourceExec: output_rows=200000 vs 200000 (0%)
                                DataSourceExec: output_rows=800000 vs 800000 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_17() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q17").await?;
        assert_snapshot!(display, @r"
        ProjectionExec: output_rows=1 vs 1 (0%)
          AggregateExec: output_rows=1 vs 1 (0%)
            CoalescePartitionsExec: output_rows=40000 vs 16 (99%)
              AggregateExec: output_rows=40000 vs 16 (99%)
                HashJoinExec: output_rows=40000 vs 587 (98%)
                  RepartitionExec: output_rows=40000 vs 6088 (84%)
                    ProjectionExec: output_rows=40000 vs 6088 (84%)
                      HashJoinExec: output_rows=40000 vs 6088 (84%)
                        CoalescePartitionsExec: output_rows=40000 vs 204 (99%)
                          FilterExec: output_rows=40000 vs 204 (99%)
                            DataSourceExec: output_rows=200000 vs 200000 (0%)
                        DataSourceExec: output_rows=6001215 vs 6001215 (0%)
                  ProjectionExec: output_rows=6001215 vs 200000 (96%)
                    AggregateExec: output_rows=6001215 vs 200000 (96%)
                      RepartitionExec: output_rows=6001215 vs 2136476 (64%)
                        AggregateExec: output_rows=6001215 vs 2136476 (64%)
                          DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_18() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q18").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=150000 vs 57 (99%)
          SortExec: output_rows=150000 vs 57 (99%)
            AggregateExec: output_rows=150000 vs 57 (99%)
              HashJoinExec: output_rows=150000 vs 399 (99%)
                FilterExec: output_rows=1200243 vs 57 (99%)
                  AggregateExec: output_rows=6001215 vs 1500000 (75%)
                    RepartitionExec: output_rows=6001215 vs 1500000 (75%)
                      AggregateExec: output_rows=6001215 vs 1500000 (75%)
                        DataSourceExec: output_rows=6001215 vs 6001215 (0%)
                HashJoinExec: output_rows=150000 vs 6001215 (97%)
                  RepartitionExec: output_rows=150000 vs 1500000 (90%)
                    HashJoinExec: output_rows=150000 vs 1500000 (90%)
                      RepartitionExec: output_rows=150000 vs 150000 (0%)
                        DataSourceExec: output_rows=150000 vs 150000 (0%)
                      RepartitionExec: output_rows=1500000 vs 1500000 (0%)
                        DataSourceExec: output_rows=1500000 vs 1500000 (0%)
                  RepartitionExec: output_rows=6001215 vs 6001215 (0%)
                    DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_19() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q19").await?;
        assert_snapshot!(display, @r"
        ProjectionExec: output_rows=1 vs 1 (0%)
          AggregateExec: output_rows=1 vs 1 (0%)
            CoalescePartitionsExec: output_rows=8000 vs 16 (99%)
              AggregateExec: output_rows=8000 vs 16 (99%)
                HashJoinExec: output_rows=8000 vs 121 (98%)
                  CoalescePartitionsExec: output_rows=40000 vs 485 (98%)
                    FilterExec: output_rows=40000 vs 485 (98%)
                      DataSourceExec: output_rows=200000 vs 200000 (0%)
                  FilterExec: output_rows=1200243 vs 128371 (89%)
                    DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_20() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q20").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=80 vs 2976 (97%)
          SortExec: output_rows=80 vs 2976 (97%)
            HashJoinExec: output_rows=80 vs 2976 (97%)
              CoalescePartitionsExec: output_rows=80 vs 6592 (98%)
                HashJoinExec: output_rows=80 vs 6592 (98%)
                  CoalescePartitionsExec: output_rows=80 vs 16 (80%)
                    FilterExec: output_rows=80 vs 16 (80%)
                      DataSourceExec: output_rows=400 vs 400 (0%)
                  DataSourceExec: output_rows=10000 vs 10000 (0%)
              HashJoinExec: output_rows=800000 vs 5833 (99%)
                RepartitionExec: output_rows=800000 vs 8508 (98%)
                  HashJoinExec: output_rows=800000 vs 8508 (98%)
                    CoalescePartitionsExec: output_rows=40000 vs 2127 (94%)
                      FilterExec: output_rows=40000 vs 2127 (94%)
                        DataSourceExec: output_rows=200000 vs 200000 (0%)
                    DataSourceExec: output_rows=800000 vs 800000 (0%)
                ProjectionExec: output_rows=1200243 vs 543210 (54%)
                  AggregateExec: output_rows=1200243 vs 543210 (54%)
                    RepartitionExec: output_rows=1200243 vs 866468 (27%)
                      AggregateExec: output_rows=1200243 vs 866468 (27%)
                        FilterExec: output_rows=1200243 vs 909455 (24%)
                          DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_21() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q21").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=0 vs 411 (100%)
          SortExec: output_rows=0 vs 411 (100%)
            ProjectionExec: output_rows=0 vs 411 (100%)
              AggregateExec: output_rows=0 vs 411 (100%)
                RepartitionExec: output_rows=0 vs 3032 (100%)
                  AggregateExec: output_rows=0 vs 3032 (100%)
                    HashJoinExec: output_rows=0 vs 66256 (100%)
                      HashJoinExec: output_rows=0 vs 1169424 (100%)
                        HashJoinExec: output_rows=0 vs 1213936 (100%)
                          CoalescePartitionsExec: output_rows=80 vs 16 (80%)
                            FilterExec: output_rows=80 vs 16 (80%)
                              DataSourceExec: output_rows=400 vs 400 (0%)
                          HashJoinExec: output_rows=99 vs 1828911 (99%)
                            RepartitionExec: output_rows=300000 vs 729413 (58%)
                              FilterExec: output_rows=300000 vs 729413 (58%)
                                DataSourceExec: output_rows=1500000 vs 1500000 (0%)
                            RepartitionExec: output_rows=2000 vs 3793296 (99%)
                              HashJoinExec: output_rows=2000 vs 3793296 (99%)
                                CoalescePartitionsExec: output_rows=10000 vs 10000 (0%)
                                  DataSourceExec: output_rows=10000 vs 10000 (0%)
                                FilterExec: output_rows=1200243 vs 3793296 (68%)
                                  DataSourceExec: output_rows=6001215 vs 6001215 (0%)
                        RepartitionExec: output_rows=6001215 vs 6001215 (0%)
                          DataSourceExec: output_rows=6001215 vs 6001215 (0%)
                      RepartitionExec: output_rows=1200243 vs 3793296 (68%)
                        FilterExec: output_rows=1200243 vs 3793296 (68%)
                          DataSourceExec: output_rows=6001215 vs 6001215 (0%)
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_tpch_22() -> Result<(), Box<dyn Error>> {
        let display = stats_vs_metrics("q22").await?;
        assert_snapshot!(display, @r"
        SortPreservingMergeExec: output_rows=30001 vs 7 (99%)
          SortExec: output_rows=30001 vs 7 (99%)
            ProjectionExec: output_rows=30001 vs 7 (99%)
              AggregateExec: output_rows=30001 vs 7 (99%)
                RepartitionExec: output_rows=30001 vs 7 (99%)
                  AggregateExec: output_rows=30001 vs 7 (99%)
                    ProjectionExec: output_rows=30001 vs 6384 (78%)
                      NestedLoopJoinExec: output_rows=30001 vs 6384 (78%)
                        AggregateExec: output_rows=1 vs 1 (0%)
                          CoalescePartitionsExec: output_rows=30000 vs 16 (99%)
                            AggregateExec: output_rows=30000 vs 16 (99%)
                              FilterExec: output_rows=30000 vs 38120 (21%)
                                DataSourceExec: output_rows=150000 vs 150000 (0%)
                        ProjectionExec: output_rows=30000 vs 14086 (53%)
                          HashJoinExec: output_rows=30000 vs 14086 (53%)
                            CoalescePartitionsExec: output_rows=30000 vs 42015 (28%)
                              FilterExec: output_rows=30000 vs 42015 (28%)
                                DataSourceExec: output_rows=150000 vs 150000 (0%)
                            DataSourceExec: output_rows=1500000 vs 1500000 (0%)
        ");
        Ok(())
    }

    async fn stats_vs_metrics(query_id: &str) -> Result<String, Box<dyn Error>> {
        let ctx = SessionContext::new();
        let data_dir = ensure_tpch_data(TPCH_SCALE_FACTOR, TPCH_DATA_PARTS).await;
        let sql = tpch::get_query(query_id)?;

        benchmarks_common::register_tables(&ctx, &data_dir).await?;

        // Query 15 has three queries in it, one creating the view, the second
        // executing, which we want to capture the output of, and the third
        // tearing down the view
        let plan = if query_id == "q15" {
            let queries: Vec<&str> = sql
                .split(';')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .collect();

            ctx.sql(queries[0]).await?.collect().await?;
            let df = ctx.sql(queries[1]).await?;
            let plan = df.create_physical_plan().await?;
            ctx.sql(queries[2]).await?.collect().await?;
            plan
        } else {
            let df = ctx.sql(&sql).await?;
            df.create_physical_plan().await?
        };

        collect(plan.clone(), ctx.task_ctx()).await?;
        Ok(stats_vs_metrics_display(
            plan,
            StatsVsMetricsDisplayOptions::default().with_display_output_rows(),
        )?)
    }

    // OnceCell to ensure TPCH tables are generated only once for tests
    static INIT_TEST_TPCH_TABLES: OnceCell<()> = OnceCell::const_new();

    // ensure_tpch_data initializes the TPCH data on disk.
    pub async fn ensure_tpch_data(sf: f64, parts: i32) -> std::path::PathBuf {
        let data_dir =
            Path::new(env!("CARGO_MANIFEST_DIR")).join(format!("testdata/tpch/plan_sf{sf}"));
        INIT_TEST_TPCH_TABLES
            .get_or_init(|| async {
                if !fs::exists(&data_dir).unwrap() {
                    tpch::generate_tpch_data(&data_dir, sf, parts);
                }
            })
            .await;
        data_dir
    }
}
