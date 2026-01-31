import { Command } from "commander";
import { z } from 'zod';
import { BenchmarkRunner, runBenchmark, TableSpec } from "./@bench-common";

// Remember to port-forward the Spark HTTP server with
// aws ssm start-session --target {host-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=9003,localPortNumber=9003"

async function main() {
    const program = new Command();

    program
        .requiredOption('--dataset <string>', 'Dataset to run queries on')
        .option('-i, --iterations <number>', 'Number of iterations', '3')
        .option('--queries <string>', 'Specific queries to run', undefined)
        .option('--debug <boolean>', 'Print the generated plans to stdout')
        .option('--warmup <boolean>', 'Perform a warmup query before the benchmarks')
        .parse(process.argv);

    const options = program.opts();

    const dataset: string = options.dataset
    const iterations = parseInt(options.iterations);
    const queries = options.queries?.split(",") ?? []
    const debug = options.debug === 'true' || options.debug === 1
    const warmup = options.warmup === 'true' || options.debug === 1

    const runner = new SparkRunner({});

    await runBenchmark(runner, {
        dataset,
        engine: 'spark',
        iterations,
        queries,
        debug,
        warmup
    });
}

const QueryResponse = z.object({
    count: z.number()
})
type QueryResponse = z.infer<typeof QueryResponse>

class SparkRunner implements BenchmarkRunner {
    private url = 'http://localhost:9003';

    constructor(private readonly options: {}) {
    }

    async executeQuery(sql: string): Promise<{ rowCount: number, plan: string }> {
        // Fix TPCH query 4: Add DATE prefix to date literals
        sql = sql.replace(/(?<!date\s)('[\d]{4}-[\d]{2}-[\d]{2}')/gi, 'DATE $1');

        // Fix ClickBench queries: Spark uses from_unixtime
        sql = sql.replace(/to_timestamp_seconds\(/gi, 'from_unixtime(');

        let response
        if (sql.includes("create view")) {
            // Query 15
            let [createView, query, dropView] = sql.split(";")
            await this.query(createView);
            response = await this.query(query)
            await this.query(dropView);
        } else {
            response = await this.query(sql)
        }

        return { rowCount: response.count, plan: "" }; // plans not yet supported in Spark.
    }

    private async query(sql: string): Promise<QueryResponse> {
        const response = await fetch(`${this.url}/query`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                query: sql.trim().replace(/;+$/, '')
            })
        });

        if (!response.ok) {
            const msg = await response.text();
            throw new Error(`Query failed: ${response.status} ${msg}`);
        }

        return QueryResponse.parse(await response.json());
    }

    async createTables(tables: TableSpec[]): Promise<void> {
        for (const table of tables) {
            // Spark requires s3a:// protocol, not s3://
            const s3aPath = table.s3Path.replace('s3://', 's3a://');

            // Create temporary view from Parquet files
            const createViewStmt = `
                CREATE OR REPLACE TEMPORARY VIEW ${table.name}
                USING parquet
                OPTIONS (path '${s3aPath}')
            `;
            await this.query(createViewStmt);
        }
    }

}

main()
    .catch(err => {
        console.error(err)
        process.exit(1)
    })
