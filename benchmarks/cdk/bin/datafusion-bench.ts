import path from "path";
import fs from "fs/promises";
import { Command } from "commander";
import { z } from 'zod';

const ROOT = path.join(__dirname, '../../..')

async function main () {
  const program = new Command();

  program
    .option('--sf <number>', 'Scale factor', '1')
    .option('-i, --iterations <number>', 'Number of iterations', '3')
    .option('--files-per-task <number>', 'Files per task', '4') // workers have 4 CPUs
    .option('--cardinality-task-sf <number>', 'Cardinality task scale factor', '2')
    .parse(process.argv);

  const options = program.opts();

  const sf = parseInt(options.sf);
  const iterations = parseInt(options.iterations);
  const filesPerTask = parseInt(options.filesPerTask);
  const cardinalityTaskSf = parseInt(options.cardinalityTaskSf);

  const queriesPath = path.join(ROOT, "testdata", "tpch", "queries")

  console.log("Creating tables...")
  await query(createTablesSql(sf))
  await query(`
    SET distributed.files_per_task=${filesPerTask};
    SET distributed.cardinality_task_sf=${cardinalityTaskSf}
  `)

  for (let id of IDS) {
    const filePath = path.join(queriesPath, `q${id}.sql`)
    const content = await fs.readFile(filePath, 'utf-8')

    const queryResults: QueryIter[] = []
    for (let i = 0; i < iterations; i++) {
      const start = new Date()
      const response = await query(content)
      const elapsed = Math.round(new Date().getTime() - start.getTime())
      queryResults.push({
        row_count: response.count,
        elapsed
      })
      console.log(
        `Query ${id} iteration ${i} took ${elapsed} ms and returned ${response.count} rows`
      );
    }
    const avg = Math.round(queryResults.reduce((a, b) => a + b.elapsed, 0) / queryResults.length)
    console.log(`Query ${id} avg time: ${avg} ms`);
  }
}

function createTablesSql (sf: number): string {
  let stmt = ''
  for (const tbl in [
    "lineitem",
    "orders",
    "part",
    "partsupp",
    "customer",
    "nation",
    "region",
    "supplier",
  ]) {
    stmt += `
        DROP TABLE IF EXISTS ${tbl}

        CREATE
        EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/${tbl}/';
    `
  }
  return stmt
}

const IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]

const QueryIter = z.object({
  row_count: z.number(),
  elapsed: z.number()
})
type QueryIter = z.infer<typeof QueryIter>

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
    throw new Error(`Query failed: ${response.status} ${response.statusText}`)
  }

  const unparsed = await response.json()
  return QueryResponse.parse(unparsed)
}

main()
  .catch(err => {
    console.error(err)
    process.exit(1)
  })
