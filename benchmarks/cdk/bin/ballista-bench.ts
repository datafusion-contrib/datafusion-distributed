import path from "path";
import {Command} from "commander";
import {z} from 'zod';
import {BenchmarkRunner, ROOT, runBenchmark, TableSpec} from "./@bench-common";

// Remember to port-forward the ballista HTTP server with
// aws ssm start-session --target {host-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=9002,localPortNumber=9002"

async function main() {
    const program = new Command();

    program
        .option('--dataset <string>', 'Dataset to run queries on')
        .option('-i, --iterations <number>', 'Number of iterations', '3')
        .option('--queries <string>', 'Specific queries to run', undefined)
        .parse(process.argv);

    const options = program.opts();

    const dataset: string = options.dataset
    const iterations = parseInt(options.iterations);
    const queries = options.queries?.split(",") ?? []

    const runner = new BallistaRunner({});

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

class BallistaRunner implements BenchmarkRunner {
    private url = 'http://localhost:9002';

    constructor(private readonly options: {}) {
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
    }

}

main()
    .catch(err => {
        console.error(err)
        process.exit(1)
    })
