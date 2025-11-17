import path from "path";
import fs from "fs/promises";
import minimist from "minimist";
import { z } from 'zod';

const ROOT = path.join(__dirname, '../../..')

async function main () {
  const argv = minimist(process.argv.slice(2), {
    default: {
      sf: 1,
      iterations: 3,
      ['files-per-task']: 0,
      ['cardinality-task-sf']: 0
    },
    alias: {
      iterations: ['i']
    }
  })

  const queries_path = path.join(ROOT, "testdata", "tpch", "queries")

  console.log("Creating tables...")
  await query(createTablesSql(argv.sf))

  if (argv['files-per-task'] > 0) {
    await query(`SET distributed.files_per_task=${argv['files-per-task']}`)
  }
  if (argv['cardinality-task-sf'] > 0) {
    await query(`SET distributed.cardinality_task_sf=${argv['cardinality-task-sf']}`)
  }

  for (let id of IDS) {
    const filePath = path.join(queries_path, `q${id}.sql`)
    const content = await fs.readFile(filePath, 'utf-8')

    const queryResults: QueryIter[] = []
    for (let i = 0; i < argv.iterations; i++) {
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

function createTablesSql (sf: number) {
  return `
      DROP TABLE IF EXISTS lineitem;

      CREATE
      EXTERNAL TABLE lineitem STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/lineitem/';

      DROP TABLE IF EXISTS orders;

      CREATE
      EXTERNAL TABLE orders STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/orders/';

      DROP TABLE IF EXISTS part;

      CREATE
      EXTERNAL TABLE part STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/part/';

      DROP TABLE IF EXISTS partsupp;

      CREATE
      EXTERNAL TABLE partsupp STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/partsupp/';

      DROP TABLE IF EXISTS customer;

      CREATE
      EXTERNAL TABLE customer STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/customer/';

      DROP TABLE IF EXISTS nation;

      CREATE
      EXTERNAL TABLE nation STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/nation/';

      DROP TABLE IF EXISTS region;

      CREATE
      EXTERNAL TABLE region STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/region/';

      DROP TABLE IF EXISTS supplier;

      CREATE
      EXTERNAL TABLE supplier STORED AS PARQUET LOCATION 's3://datafusion-distributed-benchmarks/tpch_sf${sf}/supplier/';
  `
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
