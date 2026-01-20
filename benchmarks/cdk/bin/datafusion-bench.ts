import path from "path";
import {Command} from "commander";
import {z} from 'zod';
import {BenchmarkRunner, ROOT, runBenchmark, TableSpec} from "./@bench-common";

// Remember to port-forward a worker with
// aws ssm start-session --target {host-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"

async function main() {
    const program = new Command();

    program
        .option('--dataset <string>', 'Dataset to run queries on')
        .option('-i, --iterations <number>', 'Number of iterations', '3')
        .option('--files-per-task <number>', 'Files per task', '4')
        .option('--cardinality-task-sf <number>', 'Cardinality task scale factor', '2')
        .option('--shuffle-batch-size <number>', 'Shuffle batch coalescing size (number of rows)', '8192')
        .option('--collect-metrics <boolean>', 'Propagates metric collection', 'true')
        .option('--compression <string>', 'Compression algo to use within workers (lz4, zstd, none)', 'lz4')
        .option('--queries <string>', 'Specific queries to run', undefined)
        .parse(process.argv);

    const options = program.opts();

    const dataset: string = options.dataset
    const iterations = parseInt(options.iterations);
    const filesPerTask = parseInt(options.filesPerTask);
    const cardinalityTaskSf = parseInt(options.cardinalityTaskSf);
    const shuffleBatchSize = parseInt(options.shuffleBatchSize);
    const compression = options.compression;
    const queries = options.queries?.split(",") ?? []
    const collectMetrics = options.collectMetrics === 'true' || options.collectMetrics === 1

    const runner = new DataFusionRunner({
        filesPerTask,
        cardinalityTaskSf,
        shuffleBatchSize,
        collectMetrics,
        compression
    });

    const datasetPath = path.join(ROOT, "benchmarks", "data", dataset);
    const outputPath = path.join(datasetPath, "remote-results.json")

    await runBenchmark(runner, {
        dataset,
        iterations,
        queries,
        outputPath,
    });
}

const QueryResponse = z.object({
    count: z.number(),
    plan: z.string()
})
type QueryResponse = z.infer<typeof QueryResponse>

class DataFusionRunner implements BenchmarkRunner {
    private url = 'http://localhost:9000';

    constructor(private readonly options: {
        filesPerTask: number;
        cardinalityTaskSf: number;
        shuffleBatchSize: number;
        collectMetrics: boolean;
        compression: string
    }) {
    }

    async executeQuery(sql: string): Promise<{ rowCount: number }> {
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

        return { rowCount: response.count };
    }

    private async query(sql: string): Promise<QueryResponse> {
        const url = new URL(this.url);
        url.searchParams.set('sql', sql);

        const response = await fetch(url.toString());

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
      SET distributed.shuffle_batch_size=${this.options.shuffleBatchSize};
      SET distributed.collect_metrics=${this.options.collectMetrics};
      SET distributed.compression=${this.options.compression};
    `);
    }

}

main()
    .catch(err => {
        console.error(err)
        process.exit(1)
    })
