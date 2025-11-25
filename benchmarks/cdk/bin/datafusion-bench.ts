import path from "path";
import {Command} from "commander";
import {z} from 'zod';
import {BenchmarkRunner, ROOT, runBenchmark} from "./@bench-common";

// Remember to port-forward a worker with
// aws ssm start-session --target {host-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"

async function main() {
    const program = new Command();

    program
        .option('--sf <number>', 'Scale factor', '1')
        .option('-i, --iterations <number>', 'Number of iterations', '3')
        .option('--files-per-task <number>', 'Files per task', '4')
        .option('--cardinality-task-sf <number>', 'Cardinality task scale factor', '2')
        .option('--query <number>', 'A specific query to run', undefined)
        .parse(process.argv);

    const options = program.opts();

    const sf = parseInt(options.sf);
    const iterations = parseInt(options.iterations);
    const filesPerTask = parseInt(options.filesPerTask);
    const cardinalityTaskSf = parseInt(options.cardinalityTaskSf);
    const specificQuery = options.query ? parseInt(options.query) : undefined;

    const runner = new DataFusionRunner({
        filesPerTask,
        cardinalityTaskSf,
    });

    const outputPath = path.join(ROOT, "benchmarks", "data", `tpch_sf${sf}`, "remote-results.json");

    await runBenchmark(runner, {
        sf,
        iterations,
        specificQuery,
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
    }) {
    }

    async executeQuery(sql: string): Promise<{ rowCount: number }> {
        const response = await this.query(sql);
        return {rowCount: response.count};
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

    async createTables(sf: number): Promise<void> {
        let stmt = '';
        for (const tbl of [
            "lineitem",
            "orders",
            "part",
            "partsupp",
            "customer",
            "nation",
            "region",
            "supplier",
        ]) {
            // language=SQL format=false
            stmt += `
    DROP TABLE IF EXISTS ${tbl};
    CREATE EXTERNAL TABLE IF NOT EXISTS ${tbl} STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/${tbl}/';
 `;
        }
        await this.query(stmt);
        await this.query(`
      SET distributed.files_per_task=${this.options.filesPerTask};
      SET distributed.cardinality_task_count_factor=${this.options.cardinalityTaskSf}
    `);
    }

}

main()
    .catch(err => {
        console.error(err)
        process.exit(1)
    })
