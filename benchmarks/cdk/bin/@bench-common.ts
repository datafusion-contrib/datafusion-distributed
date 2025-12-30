import path from "path";
import fs from "fs/promises";
import {z} from 'zod';

export const ROOT = path.join(__dirname, '../../..')
export const BUCKET = 's3://datafusion-distributed-benchmarks' // hardcoded in CDK code

// Simple data structures
export type QueryResult = {
    query: string;
    iterations: { elapsed: number; row_count: number }[];
    failure?: string
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
        })),
        failed: z.string().optional()
    }))
})

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

export interface TableSpec {
    schema: string
    name: string
    s3Path: string
}

export interface BenchmarkRunner {
    createTables(s3Paths: TableSpec[]): Promise<void>;

    executeQuery(query: string): Promise<{ rowCount: number }>;
}

async function tablePathsForDataset(dataset: string): Promise<TableSpec[]> {
    const datasetPath = path.join(ROOT, "benchmarks", "data", dataset)

    const result: TableSpec[] = []
    for (const entryName of await fs.readdir(datasetPath)) {
        const dir = path.join(datasetPath, entryName)
        if (await isDirWithAllParquetFiles(dir)) {
            result.push({
                name: entryName,
                schema: dataset,
                s3Path: `${BUCKET}/${dataset}/${entryName}/`
            })
        }
    }
    return result
}

async function isDirWithAllParquetFiles(dir: string): Promise<boolean> {
    let readDir
    try {
        readDir = await fs.readdir(dir)
    } catch (e) {
        return false
    }
    for (const file of readDir) {
        if (!file.endsWith(".parquet")) {
            return false
        }
    }
    return true
}

async function queriesForDataset(dataset: string): Promise<[string, string][]> {
    const datasetSuffix = dataset.split("_")[0]
    const queriesPath = path.join(ROOT, "testdata", datasetSuffix, "queries")

    const queries: [string, string][] = []
    for (const queryName of await fs.readdir(queriesPath)) {
        const sql = await fs.readFile(path.join(queriesPath, queryName), 'utf-8');
        queries.push([queryName, sql])
    }
    queries.sort(([name1], [name2]) => numericId(name1) > numericId(name2) ? 1 : -1)
    return queries
}

function numericId(queryName: string): number {
    return parseInt([...queryName.matchAll(/(\d+)/g)][0][0])
}

export async function runBenchmark(
    runner: BenchmarkRunner,
    options: {
        dataset: string
        iterations: number;
        queries: number[];
        outputPath: string;
    }
) {
    const { dataset, iterations, queries, outputPath } = options;

    const results: BenchmarkResults = { queries: [] };

    console.log("Creating tables...");
    const s3Paths = await tablePathsForDataset(dataset)
    await runner.createTables(s3Paths);

    for (const [queryName, sql] of await queriesForDataset(dataset)) {
        const id = numericId(queryName)

        if (queries.length > 0 && !queries.includes(id)) {
            continue;
        }

        const queryResult: QueryResult = {
            query: queryName,
            iterations: [],
        };

        console.log(`Warming up query ${id}...`)
        try {
            await runner.executeQuery(sql);
        } catch (e: any) {
            queryResult.failure = e.toString();
            console.error(`Query ${queryResult.query} failed: ${queryResult.failure}`)
            continue
        }

        for (let i = 0; i < iterations; i++) {
            const start = new Date()
            let response
            try {
                response = await runner.executeQuery(sql);
            } catch (e: any) {
                queryResult.failure = e.toString();
                break
            }
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

        if (queryResult.failure) {
            console.error(`Query ${queryResult.query} failed: ${queryResult.failure}`)
        }
        results.queries.push(queryResult);
    }

    // Write results and compare
    await compareWithPrevious(results, outputPath);
    await writeJson(results, outputPath);
}
