import {Command} from "commander";
import {z} from 'zod';
import {BenchmarkRunner, runBenchmark, TableSpec} from "./@bench-common";
import {execSync} from "child_process";

// Remember to port-forward a worker with
// aws ssm start-session --target {host-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"

async function main() {
    const program = new Command();

    program
        .requiredOption('--dataset <string>', 'Dataset to run queries on')
        .option('-i, --iterations <number>', 'Number of iterations', '3')
        .option('--files-per-task <number>', 'Files per task', '8')
        .option('--cardinality-task-sf <number>', 'Cardinality task scale factor', '1')
        .option('--batch-size <number>', 'Standard Batch coalescing size (number of rows)', '8192')
        .option('--shuffle-batch-size <number>', 'Shuffle batch coalescing size (number of rows)', '8192')
        .option('--children-isolator-unions <number>', 'Use children isolator unions', 'true')
        .option('--collect-metrics <boolean>', 'Propagates metric collection', 'true')
        .option('--queries <string>', 'Specific queries to run', undefined)
        .parse(process.argv);

    const options = program.opts();

    const dataset: string = options.dataset
    const iterations = parseInt(options.iterations);
    const filesPerTask = parseInt(options.filesPerTask);
    const cardinalityTaskSf = parseInt(options.cardinalityTaskSf);
    const batchSize = parseInt(options.batchSize);
    const shuffleBatchSize = parseInt(options.shuffleBatchSize);
    const queries = options.queries?.split(",") ?? []
    const collectMetrics = options.collectMetrics === 'true' || options.collectMetrics === 1
    const childrenIsolatorUnions = options.childrenIsolatorUnions === 'true' || options.childrenIsolatorUnions === 1

    const runner = new DataFusionRunner({
        filesPerTask,
        cardinalityTaskSf,
        batchSize,
        shuffleBatchSize,
        collectMetrics,
        childrenIsolatorUnions
    });

    await runBenchmark(runner, {
        dataset,
        engine: `datafusion-distributed-${getCurrentBranch()}`,
        iterations,
        queries,
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
        batchSize: number;
        shuffleBatchSize: number;
        collectMetrics: boolean;
        childrenIsolatorUnions: boolean
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
      SET datafusion.execution.batch_size=${this.options.batchSize};
      SET distributed.shuffle_batch_size=${this.options.shuffleBatchSize};
      SET distributed.collect_metrics=${this.options.collectMetrics};
      SET distributed.children_isolator_unions=${this.options.childrenIsolatorUnions};
    `);
    }
}

function getCurrentBranch(): string {
    try {
        // Try to get current git branch
        return execSync('git rev-parse --abbrev-ref HEAD', { encoding: 'utf-8' }).trim();
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
