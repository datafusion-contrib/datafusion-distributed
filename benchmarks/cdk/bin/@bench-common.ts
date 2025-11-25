import path from "path";
import fs from "fs/promises";
import { z } from 'zod';

export const ROOT = path.join(__dirname, '../../..')

// Simple data structures
export type QueryResult = {
  query: string;
  iterations: { elapsed: number; row_count: number }[];
}

export type BenchmarkResults = {
  queries: QueryResult[];
}

export const BenchmarkResults = z.object({
  queries: z.array(z.object({
    query: z.string(),
    iterations: z.array(z.object({
      elapsed: z.number(),
      row_count: z.number()
    }))
  }))
})

export const IDS = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]

export async function writeJson(results: BenchmarkResults, outputPath?: string) {
  if (!outputPath) return;
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, JSON.stringify(results, null, 2));
}

export async function compareWithPrevious(results: BenchmarkResults, outputPath: string) {
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

export interface BenchmarkRunner {
  createTables(sf: number): Promise<void>;

  executeQuery(query: string): Promise<{ rowCount: number }>;
}

export async function runBenchmark(
  runner: BenchmarkRunner,
  options: {
    sf: number;
    iterations: number;
    specificQuery?: number;
    outputPath: string;
  }
) {
  const { sf, iterations, specificQuery, outputPath } = options;

  const results: BenchmarkResults = { queries: [] };
  const queriesPath = path.join(ROOT, "testdata", "tpch", "queries")

  console.log("Creating tables...");
  await runner.createTables(sf);

  for (let id of IDS) {
    if (specificQuery && specificQuery !== id) {
      continue;
    }

    const queryId = `q${id}`;
    const filePath = path.join(queriesPath, `${queryId}.sql`)
    const queryToExecute = await fs.readFile(filePath, 'utf-8')

    const queryResult: QueryResult = {
      query: queryId,
      iterations: []
    };

    for (let i = 0; i < iterations; i++) {
      const start = new Date()
      const response = await runner.executeQuery(queryToExecute);
      const elapsed = Math.round(new Date().getTime() - start.getTime())

      queryResult.iterations.push({
        elapsed,
        row_count: response.rowCount
      });

      console.log(
        `Query ${id} iteration ${i} took ${elapsed} ms and returned ${response.rowCount} rows`
      );
    }

    const avg = Math.round(
      queryResult.iterations.reduce((a, b) => a + b.elapsed, 0) / queryResult.iterations.length
    );
    console.log(`Query ${id} avg time: ${avg} ms`);

    results.queries.push(queryResult);
  }

  // Write results and compare
  await compareWithPrevious(results, outputPath);
  await writeJson(results, outputPath);
}
