import path from "path";
import fs from "fs/promises";
import * as fsSync from "fs";
import {BenchmarkRun, BenchResult} from "./@results";
import {datasetParts, datasetPath, ROOT} from "./@paths";

const PULUMI_OUTPUT_FILE = path.join(ROOT, "benchmarks-remote", "pulumi", ".pulumi-outputs.json")

function normalizeBucketUri(bucket: string): string {
    const withoutProtocol = bucket.replace(/^s3:\/\//, "").replace(/\/+$/, "")
    return `s3://${withoutProtocol}`
}

function getBucketFromLocalOutputs(): string | undefined {
    try {
        const raw = fsSync.readFileSync(PULUMI_OUTPUT_FILE, "utf-8")
        const outputs = JSON.parse(raw) as Record<string, unknown>
        const value = outputs["datasetBucketName"]
        if (typeof value === "string" && value.trim() !== "") {
            return normalizeBucketUri(value)
        }
    } catch {
        // Ignore read/parse issues and fall back to env-based configuration.
    }
    return undefined
}

function resolveBucketUri(): string {
    // Keep resolution local-first to avoid live AWS calls during benchmark script startup.
    // Resolution order:
    // 1) explicit env override
    // 2) local Pulumi outputs produced by `npm run foundation-deploy`
    const fromEnv = process.env.BENCHMARK_BUCKET
    if (fromEnv) {
        return normalizeBucketUri(fromEnv)
    }

    const fromLocalOutput = getBucketFromLocalOutputs()
    if (fromLocalOutput) {
        return fromLocalOutput
    }

    throw new Error(
        "Could not resolve benchmark bucket. Set BENCHMARK_BUCKET or run npm run foundation-deploy from benchmarks-remote."
    )
}

let resolvedBucketUri: string | undefined
export function getBucketUri(): string {
    // Resolve once per process; bucket does not change during a single benchmark invocation.
    if (!resolvedBucketUri) {
        resolvedBucketUri = resolveBucketUri()
    }
    return resolvedBucketUri
}

export interface TableSpec {
    suite: string
    schema: string
    name: string
    s3Path: string
}

export interface ExecuteQueryResult {
    rowCount: number,
    plan: string
    elapsed: number
    tasks: number
    statsQErrorP50?: number
    statsQErrorP95?: number
}

export interface BenchmarkRunner {
    createTables(s3Paths: TableSpec[]): Promise<void>;

    executeQuery(query: string): Promise<ExecuteQueryResult>;
}

async function tablePathsForDataset(dataset: string): Promise<TableSpec[]> {
    const localDatasetPath = datasetPath(dataset)
    const bucketUri = getBucketUri()
    const [suite] = datasetParts(dataset)
    const schema = dataset.replaceAll("/", "_")

    const result: TableSpec[] = []
    for (const entryName of await fs.readdir(localDatasetPath)) {
        const dir = path.join(localDatasetPath, entryName)
        if (await isDirWithAllParquetFiles(dir)) {
            result.push({
                suite,
                name: entryName,
                schema,
                s3Path: `${bucketUri}/${dataset}/${entryName}/`
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

async function queriesForDataset(dataset: string): Promise<{ id: string, sql: string }[]> {
    const [suite] = datasetParts(dataset)
    const queriesPath = path.join(ROOT, "testdata", suite, "queries")

    const queries = []
    for (const fileName of await fs.readdir(queriesPath)) {
        const sql = await fs.readFile(path.join(queriesPath, fileName), 'utf-8');
        queries.push({ id: fileName.replace(".sql", ""), sql })
    }
    queries.sort((a, b) => numericId(a.id) > numericId(b.id) ? 1 : -1)
    return queries
}

function numericId(queryName: string): number {
    return parseInt([...queryName.matchAll(/(\d+)/g)][0][0])
}

export async function runBenchmark(
    runner: BenchmarkRunner,
    options: {
        dataset: string
        engine: string,
        iterations: number;
        queries: string[];
        debug: boolean;
        warmup: boolean;
    }
) {
    const { dataset, engine, iterations, queries, warmup, debug } = options;

    const benchmarkRun = new BenchmarkRun(dataset, engine)

    console.log("Creating tables...");
    const s3Paths = await tablePathsForDataset(dataset)
    await runner.createTables(s3Paths);

    for (const { id, sql } of await queriesForDataset(dataset)) {
        if (queries.length > 0 && !queries.includes(id)) {
            continue;
        }

        const result = new BenchResult(dataset, engine, id)

        if (warmup) {
            console.log(`Warming up query ${id}...`)
            try {
                await runner.executeQuery(sql);
            } catch (e: any) {
                result.iterations.push({
                    elapsed: 0,
                    rowCount: 0,
                    error: e.toString(),
                    plan: "",
                    tasks: 0
                })
                console.error(`Query ${id} failed: ${e.toString()}`)
                continue
            }
        }

        for (let i = 0; i < iterations; i++) {
            let response
            try {
                response = await runner.executeQuery(sql);
            } catch (e: any) {
                result.iterations.push({
                    elapsed: 0,
                    rowCount: 0,
                    error: e.toString(),
                    plan: "",
                    tasks: 0
                })
                console.error(`Query ${id} failed: ${e.toString()}`)
                break
            }

            if (debug) {
                console.log(response.plan)
            }
            result.iterations.push({
                elapsed: response.elapsed,
                rowCount: response.rowCount,
                plan: response.plan,
                tasks: response.tasks,
                statsQErrorP50: response.statsQErrorP50,
                statsQErrorP95: response.statsQErrorP95,
            })

            if (response.statsQErrorP50 !== undefined && response.statsQErrorP95 !== undefined) {
                console.log(
                    `Query ${id} iteration ${i} took ${Math.round(response.elapsed)} ms, stats q-error P50 ${response.statsQErrorP50.toFixed(2)}x, P95 ${response.statsQErrorP95.toFixed(2)}x and returned ${response.rowCount} rows`
                );
            } else {
                console.log(
                    `Query ${id} iteration ${i} took ${Math.round(response.elapsed)} ms and returned ${response.rowCount} rows`
                );
            }
        }

        console.log(`Query ${id} p50 time: ${result.p50()} ms`);

        benchmarkRun.results.push(result)
    }

    // Write results and compare
    benchmarkRun.compareWithPrevious()
    benchmarkRun.store()
}
