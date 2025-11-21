#[cfg(all(feature = "integration", feature = "tpcds", test))]
mod tests {
    use datafusion::common::runtime::JoinSet;
    use datafusion::error::Result;
    use datafusion::prelude::SessionContext;
    use datafusion_distributed::test_utils::{
        localhost::start_localhost_context,
        property_based::{compare_ordering, compare_result_set},
        rand::rng,
        tpcds::{generate_tpcds_data, queries, register_tables},
    };
    use datafusion_distributed::{DefaultSessionBuilder, DistributedExt};
    use rand::Rng;
    use std::env;

    async fn setup() -> Result<(SessionContext, SessionContext, JoinSet<()>)> {
        let (mut rng, seed_b64) = rng()?;
        eprintln!("Seed: {}", seed_b64);

        let num_workers = rng.gen_range(3..=8);
        let files_per_task = rng.gen_range(2..=4);
        let cardinality_task_count_factor = rng.gen_range(1.1..=3.0);

        eprintln!(
            "workers: {}, files_per_task: {}, cardinality_task_count_factor: {}",
            num_workers, files_per_task, cardinality_task_count_factor
        );

        // Make distributed localhost context to run queries
        let (mut distributed_ctx, worker_tasks) =
            start_localhost_context(num_workers, DefaultSessionBuilder).await;
        distributed_ctx.set_distributed_files_per_task(files_per_task)?;
        distributed_ctx
            .set_distributed_cardinality_effect_task_scale_factor(cardinality_task_count_factor)?;
        register_tables(&distributed_ctx).await?;

        // Create single node context to compare results to.
        let single_node_ctx = SessionContext::new();
        register_tables(&single_node_ctx).await?;

        Ok((distributed_ctx, single_node_ctx, worker_tasks))
    }

    #[tokio::test]
    async fn test_tpcds_randomized() -> Result<()> {
        // Get scale factor from environment variable, defaulting to 0.01
        let scale_factor = env::var("SCALE_FACTOR").unwrap_or_else(|_| "0.01".to_string());
        let skip_data_gen = env::var("SKIP_GEN").is_ok();

        if !skip_data_gen {
            eprintln!("Generating TPC-DS data with scale factor {}", scale_factor);
            generate_tpcds_data(&scale_factor)?;
        }

        let queries = queries()?;

        // Create randomized fuzz config
        let (d_ctx, s_ctx, _handles) = setup().await?;

        let mut successful = 0;
        let mut failed = 0;
        let mut invalid = 0;

        for (query_name, query_sql) in queries {
            eprintln!("Executing query: {}", query_name);
            let df = s_ctx.sql(&query_sql).await?;
            let results = df.collect().await;

            let compare_result = tokio::try_join!(
                compare_result_set(&s_ctx, &d_ctx, &query_sql, &results),
                compare_ordering(&s_ctx, &d_ctx, &query_sql, &results),
            );

            match compare_result {
                Ok(_) => match results.ok() {
                    Some(_batches) => {
                        successful += 1;
                        eprintln!("✅ {} completed", query_name);
                    }
                    None => {
                        eprintln!("No results (query errored expectedly)");
                        invalid += 1;
                    }
                },
                Err(e) => {
                    failed += 1;
                    eprintln!("❌ {} failed: {}", query_name, e);
                    eprintln!("{}", query_sql.trim());
                }
            }
        }

        eprintln!(
            "Test summary - Success: {} Invalid: {} Failed: {} Valid %: {:.2}%",
            successful,
            invalid,
            failed,
            if successful + invalid > 0 {
                (successful as f64 / (successful + invalid) as f64) * 100.0
            } else {
                0.0
            }
        );

        assert_eq!(
            failed,
            0,
            "{} out of {} queries failed",
            failed,
            successful + failed
        );

        Ok(())
    }
}
