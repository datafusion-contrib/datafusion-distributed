import { Command } from "commander";
import { z } from 'zod';
import { BenchmarkRunner, ExecuteQueryResult, runBenchmark, TableSpec } from "./@bench-common";
import { execSync } from "child_process";

// Remember to port-forward a worker with
// aws ssm start-session --target {host-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"

async function main() {
    const program = new Command();

    program
        .requiredOption('--dataset <string>', 'Dataset to run queries on')
        .option('-i, --iterations <number>', 'Number of iterations', '3')
        .option('--files-per-task <number>', 'Files per task', '8')
        .option('--cardinality-task-sf <number>', 'Cardinality task scale factor', '1')
        .option('--batch-size <number>', 'Standard Batch coalescing size (number of rows)', '32768')
        .option('--shuffle-batch-size <number>', 'Shuffle batch coalescing size (number of rows)', '32768')
        .option('--children-isolator-unions <number>', 'Use children isolator unions', 'true')
        .option('--broadcast-joins <boolean>', 'Use broadcast joins', 'true')
        .option('--collect-metrics <boolean>', 'Propagates metric collection', 'true')
        .option('--compression <string>', 'Compression algo to use within workers (lz4, zstd, none)', 'lz4')
        .option('--max-tasks-per-stage <number>', 'Max tasks per stage', '0')
        .option('--repartition-file-min-size <number>', 'repartition_file_min_size DF option', '10485760' /* upstream default */)
        .option('--target-partitions <number>', 'target_partitions DF option', '8')
        .option('--grpc-connect-timeout-ms <number>', 'Total timeout for establishing Flight channels', '5000')
        .option('--grpc-request-timeout-ms <number>', 'Total do_get RPC deadline', '30000')
        .option('--wait-plan-timeout-ms <number>', 'Worker-side wait for task data before do_get fails', '10000')
        .option('--grpc-tcp-keepalive-ms <number>', 'TCP keepalive for Flight channels', '60000')
        .option('--queries <string>', 'Specific queries to run', undefined)
        .option('--debug <boolean>', 'Print the generated plans to stdout')
        .option('--warmup <boolean>', 'Perform a warmup query before the benchmarks', 'true')
        .parse(process.argv);

    const options = program.opts();

    const dataset: string = options.dataset
    const iterations = parseInt(options.iterations);
    const filesPerTask = parseInt(options.filesPerTask);
    const cardinalityTaskSf = parseInt(options.cardinalityTaskSf);
    const batchSize = parseInt(options.batchSize);
    const shuffleBatchSize = parseInt(options.shuffleBatchSize);
    const compression = options.compression;
    const maxTasksPerStage = parseInt(options.maxTasksPerStage);
    const repartitionFileMinSize = parseInt(options.repartitionFileMinSize)
    const targetPartitions = parseInt(options.targetPartitions);
    const grpcConnectTimeoutMs = parseInt(options.grpcConnectTimeoutMs);
    const grpcRequestTimeoutMs = parseInt(options.grpcRequestTimeoutMs);
    const waitPlanTimeoutMs = parseInt(options.waitPlanTimeoutMs);
    const grpcTcpKeepaliveMs = parseInt(options.grpcTcpKeepaliveMs);
    const queries = options.queries?.split(",") ?? []
    const collectMetrics = options.collectMetrics === 'true' || options.collectMetrics === 1
    const childrenIsolatorUnions = options.childrenIsolatorUnions === 'true' || options.childrenIsolatorUnions === 1
    const broadcastJoins = options.broadcastJoins === 'true' || options.broadcastJoins === 1
    const debug = options.debug === true || options.debug === 'true' || options.debug === 1
    const warmup = options.warmup === true || options.warmup === 'true' || options.warmup === 1

    const runner = new DataFusionRunner({
        filesPerTask,
        cardinalityTaskSf,
        batchSize,
        shuffleBatchSize,
        collectMetrics,
        childrenIsolatorUnions,
        compression,
        broadcastJoins,
        maxTasksPerStage,
        repartitionFileMinSize,
        targetPartitions,
        grpcConnectTimeoutMs,
        grpcRequestTimeoutMs,
        waitPlanTimeoutMs,
        grpcTcpKeepaliveMs
    });

    // Fail fast on dead port-forward/unhealthy worker before doing table setup and benchmark work.
    await runner.assertReachable();

    await runBenchmark(runner, {
        dataset,
        engine: `datafusion-distributed-${getCurrentBranch()}`,
        iterations,
        queries,
        debug,
        warmup
    });
}

const QueryResponse = z.object({
    count: z.number(),
    plan: z.string(),
    elapsed_ms: z.number(),
})
type QueryResponse = z.infer<typeof QueryResponse>

const WorkerInfo = z.object({
    worker_urls: z.string().array(),
    git_commit_hash: z.string(),
    build_time_utc: z.string(),
    errors: z.string().array(),
    transport_config: z.object({
        grpc_connect_timeout_ms: z.number(),
        grpc_request_timeout_ms: z.number(),
        wait_plan_timeout_ms: z.number(),
        grpc_tcp_keepalive_ms: z.number(),
    }).optional(),
})
type WorkerInfo = z.infer<typeof WorkerInfo>

class DataFusionRunner implements BenchmarkRunner {
    private url = 'http://localhost:9000';

    constructor(private readonly options: {
        filesPerTask: number;
        cardinalityTaskSf: number;
        batchSize: number;
        shuffleBatchSize: number;
        collectMetrics: boolean;
        compression: string;
        childrenIsolatorUnions: boolean;
        broadcastJoins: boolean;
        maxTasksPerStage: number;
        repartitionFileMinSize: number;
        targetPartitions: number;
        grpcConnectTimeoutMs: number;
        grpcRequestTimeoutMs: number;
        waitPlanTimeoutMs: number;
        grpcTcpKeepaliveMs: number;
    }) {
    }

    async assertReachable(): Promise<void> {
        await this.info()
    }

    private async info(): Promise<WorkerInfo> {
        // `/info` is a lightweight health endpoint; timeout avoids hanging when the local tunnel is stale.
        const infoUrl = `${this.url}/info`
        const controller = new AbortController()
        const timeout = setTimeout(() => controller.abort(), 5_000)
        try {
            const response = await fetch(infoUrl, {signal: controller.signal})
            if (!response.ok) {
                const msg = await response.text()
                throw new Error(`Worker health check failed: ${response.status} ${msg}`)
            }
            const unparsed = await response.json()
            return WorkerInfo.parse(unparsed)
        } catch (e: any) {
            throw this.decorateConnectionError(e, infoUrl)
        } finally {
            clearTimeout(timeout)
        }
    }

    async executeQuery(sql: string): Promise<ExecuteQueryResult> {
        let response
        if (sql.includes("create view")) {
            // This is query 15
            let [createView, query, dropView] = sql.split(";")
            await this.query(createView);
            response = await this.query(query)
            await this.query(dropView);
        } else {
            response = await this.query(sql)
        }

        return { rowCount: response.count, plan: response.plan, elapsed: response.elapsed_ms };
    }

    private async query(sql: string): Promise<QueryResponse> {
        const url = new URL(this.url);
        url.searchParams.set('sql', sql);

        let response
        try {
            response = await fetch(url.toString());
        } catch (e: any) {
            throw this.decorateConnectionError(e, url.toString())
        }

        if (!response.ok) {
            const msg = await response.text();
            throw new Error(`Query failed: ${response.status} ${msg}`);
        }

        const unparsed = await response.json();
        return QueryResponse.parse(unparsed);
    }

    async createTables(tables: TableSpec[]): Promise<void> {
        let stmt = '';
        for (const table of tables) {
            // language=SQL format=false
            stmt += `
    DROP TABLE IF EXISTS ${table.name};
    CREATE EXTERNAL TABLE IF NOT EXISTS ${table.name} STORED AS PARQUET LOCATION '${table.s3Path}';
 `;
        }
        await this.query(stmt);
        await this.query(`
      SET distributed.files_per_task=${this.options.filesPerTask};
      SET distributed.cardinality_task_count_factor=${this.options.cardinalityTaskSf};
      SET datafusion.execution.batch_size=${this.options.batchSize};
      SET distributed.shuffle_batch_size=${this.options.shuffleBatchSize};
      SET distributed.collect_metrics=${this.options.collectMetrics};
      SET distributed.compression=${this.options.compression};
      SET distributed.children_isolator_unions=${this.options.childrenIsolatorUnions};
      SET distributed.broadcast_joins=${this.options.broadcastJoins};
      SET distributed.max_tasks_per_stage=${this.options.maxTasksPerStage};
      SET datafusion.optimizer.repartition_file_min_size=${this.options.repartitionFileMinSize};
      SET datafusion.execution.target_partitions=${this.options.targetPartitions};
      SET distributed.grpc_connect_timeout_ms=${this.options.grpcConnectTimeoutMs};
      SET distributed.grpc_request_timeout_ms=${this.options.grpcRequestTimeoutMs};
      SET distributed.wait_plan_timeout_ms=${this.options.waitPlanTimeoutMs};
      SET distributed.grpc_tcp_keepalive_ms=${this.options.grpcTcpKeepaliveMs};
    `);

        const info = await this.info()
        console.log("Remote worker build:", info.git_commit_hash, info.build_time_utc)
        if (info.transport_config) {
            console.log("Active transport config:", info.transport_config)
        } else {
            console.log(
                "Active transport config: unavailable from /info; redeploy the remote worker to verify the applied timeout settings explicitly."
            )
        }
    }

    private decorateConnectionError(err: any, url: string): Error {
        const code = err?.cause?.code
        const timeout = err?.name === "AbortError"
        if (timeout || code === "ECONNREFUSED" || code === "ENOTFOUND" || code === "EHOSTUNREACH") {
            return new Error(
                `Could not connect to ${url}. Ensure the SSM port-forward is running (remote 9000 -> local 9000) and worker.service is active on the target instance.`
            )
        }
        if (err instanceof Error) {
            return err
        }
        return new Error(String(err))
    }
}

function getCurrentBranch(): string {
    try {
        // Try to get current git branch. For branches with a slash prefix, keep the last entry.
        return execSync('git rev-parse --abbrev-ref HEAD', { encoding: 'utf-8' }).trim().split("/").slice(-1)[0];
    } catch {
        // Fallback if git command fails
        return 'unknown';
    }
}

main()
    .catch(err => {
        console.error(err)
        process.exit(1)
    })
