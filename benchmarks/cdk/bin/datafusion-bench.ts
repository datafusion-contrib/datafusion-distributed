import path from "path";
import fs from "fs/promises";
import { Command } from "commander";
import { z } from 'zod';

const ROOT = path.join(__dirname, '../../..')

// Remember to port-forward a worker with
// aws ssm start-session --target {host-id} --document-name AWS-StartPortForwardingSession --parameters "portNumber=9000,localPortNumber=9000"
async function main () {
  const program = new Command();

  program
    .option('--sf <number>', 'Scale factor', '1')
    .option('-i, --iterations <number>', 'Number of iterations', '3')
    .option('--files-per-task <number>', 'Files per task', '4')
    .option('--cardinality-task-sf <number>', 'Cardinality task scale factor', '2')
    .option('--shuffle-batch-size <number>', 'Shuffle batch coalescing size (number of rows)', '8192')
    .option('--query <number>', 'A specific query to run', undefined)
    .parse(process.argv);

  const options = program.opts();

  const sf = parseInt(options.sf);
  const iterations = parseInt(options.iterations);
  const filesPerTask = parseInt(options.filesPerTask);
  const cardinalityTaskSf = parseInt(options.cardinalityTaskSf);
  const shuffleBatchSize = parseInt(options.shuffleBatchSize);

  // Compare with previous results first
  const results: BenchmarkResults = { queries: [] };
  const queriesPath = path.join(ROOT, "testdata", "tpch", "queries")

  console.log("Creating tables...")
  await query(createTablesSql(sf))
  await query(`
    SET distributed.files_per_task=${filesPerTask};
    SET distributed.cardinality_task_count_factor=${cardinalityTaskSf};
    SET distributed.shuffle_batch_size=${shuffleBatchSize}
  `)

  for (let id of IDS) {
    if (options.query && parseInt(options.query) !== id) {
      continue
    }

    const queryId = `q${id}`;
    const filePath = path.join(queriesPath, `${queryId}.sql`)
    const content = await fs.readFile(filePath, 'utf-8')

    const queryResult: QueryResult = {
      query: queryId,
      iterations: []
    };

    for (let i = 0; i < iterations; i++) {
      const start = new Date()
      const response = await query(content)
      const elapsed = Math.round(new Date().getTime() - start.getTime())

      queryResult.iterations.push({
        elapsed,
        row_count: response.count
      });

      console.log(
        `Query ${id} iteration ${i} took ${elapsed} ms and returned ${response.count} rows`
      );
    }

    const avg = Math.round(
      queryResult.iterations.reduce((a, b) => a + b.elapsed, 0) / queryResult.iterations.length
    );
    console.log(`Query ${id} avg time: ${avg} ms`);

    results.queries.push(queryResult);
  }

  // Write results and compare
  const outputPath = path.join(ROOT, "benchmarks", "data", `tpch_sf${sf}`, "remote-results.json");
  await compareWithPrevious(results, outputPath);
  await writeJson(results, outputPath);
}

// Simple data structures
type QueryResult = {
  query: string;
  iterations: { elapsed: number; row_count: number }[];
}

type BenchmarkResults = {
  queries: QueryResult[];
}

const BenchmarkResults = z.object({
  queries: z.array(z.object({
    query: z.string(),
    iterations: z.array(z.object({
      elapsed: z.number(),
      row_count: z.number()
    }))
  }))
})

async function writeJson (results: BenchmarkResults, outputPath?: string) {
  if (!outputPath) return;
  await fs.writeFile(outputPath, JSON.stringify(results, null, 2));
}

async function compareWithPrevious (results: BenchmarkResults, outputPath: string) {
  let prevResults: BenchmarkResults;
  try {
    const prevContent = await fs.readFile(outputPath, 'utf-8');
    prevResults = BenchmarkResults.parse(JSON.parse(prevContent));
  } catch {
    return; // No previous results to compare
  }

  console.log('\n==== Comparison with previous run ====');

  for (const query of results.queries) {
    const prevQuery = prevResults.queries.find(q => q.query === query.query);
    if (!prevQuery || prevQuery.iterations.length === 0 || query.iterations.length === 0) {
      continue;
    }

    const avgPrev = Math.round(
      prevQuery.iterations.reduce((sum, i) => sum + i.elapsed, 0) / prevQuery.iterations.length
    );
    const avg = Math.round(
      query.iterations.reduce((sum, i) => sum + i.elapsed, 0) / query.iterations.length
    );

    const factor = avg < avgPrev ? avgPrev / avg : avg / avgPrev;
    const tag = avg < avgPrev ? "faster" : "slower";
    const emoji = factor > 1.2 ? (avg < avgPrev ? "✅" : "❌") : (avg < avgPrev ? "✔" : "✖");

    console.log(
      `${query.query.padStart(8)}: prev=${avgPrev.toString().padStart(4)} ms, new=${avg.toString().padStart(4)} ms, ${factor.toFixed(2)}x ${tag} ${emoji}`
    );
  }
}


function createTablesSql (sf: number): string {
  let stmt = ''
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
 `
  }
  return stmt
}

const IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]

const QueryResponse = z.object({
  count: z.number(),
  plan: z.string()
})
type QueryResponse = z.infer<typeof QueryResponse>

async function query (sql: string): Promise<QueryResponse> {
  const url = new URL('http://localhost:9000')
  url.searchParams.set('sql', sql)

  const response = await fetch(url.toString())

  if (!response.ok) {
    const msg = await response.text()
    throw new Error(`Query failed: ${response.status} ${msg}`)
  }

  const unparsed = await response.json()
  return QueryResponse.parse(unparsed)
}

main()
  .catch(err => {
    console.error(err)
    process.exit(1)
  })
