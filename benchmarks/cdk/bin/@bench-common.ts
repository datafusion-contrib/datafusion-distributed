import path from "path";
import fs from "fs/promises";
import * as fsSync from "fs";
import {BenchmarkRun, BenchResult} from "./@results";

export const ROOT = path.join(__dirname, '../../..')
const STACK_NAME = "DataFusionDistributedBenchmarks"
const CDK_OUTPUT_FILE = path.join(__dirname, "..", ".cdk-outputs.json")

function normalizeBucketUri(bucket: string): string {
    const withoutProtocol = bucket.replace(/^s3:\/\//, "").replace(/\/+$/, "")
    return `s3://${withoutProtocol}`
}

function getBucketFromLocalOutputs(): string | undefined {
    // Prefer local deploy metadata over live AWS lookups to keep benchmark scripts deterministic
    // and independent from shell/profile/region drift.
    try {
        const raw = fsSync.readFileSync(CDK_OUTPUT_FILE, "utf-8")
        const outputs = JSON.parse(raw) as Record<string, Record<string, unknown>>
        const value = outputs[STACK_NAME]?.["BenchmarkBucketName"]
        if (typeof value === "string" && value.trim() !== "") {
            return normalizeBucketUri(value)
        }
    } catch {
        // Ignore read/parse issues and fall back to env-based configuration.
    }
    return undefined
}

function resolveBucketUri(): string {
    // Resolution order:
    // 1) explicit env override
    // 2) local CDK outputs produced by `npm run deploy -- --outputs-file ...`
    const fromEnv = process.env.BENCHMARK_BUCKET
    if (fromEnv) {
        return normalizeBucketUri(fromEnv)
    }

    const fromLocalOutput = getBucketFromLocalOutputs()
    if (fromLocalOutput) {
        return fromLocalOutput
    }

    throw new Error(
        "Could not resolve benchmark bucket. Set BENCHMARK_BUCKET or deploy DataFusionDistributedBenchmarks with --outputs-file .cdk-outputs.json."
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
    schema: string
    name: string
    s3Path: string
}

export interface ExecuteQueryResult {
    rowCount: number,
    plan: string
    elapsed: number
}

export interface BenchmarkRunner {
    createTables(s3Paths: TableSpec[]): Promise<void>;

    executeQuery(query: string): Promise<ExecuteQueryResult>;
}

async function tablePathsForDataset(dataset: string): Promise<TableSpec[]> {
    const datasetPath = path.join(ROOT, "benchmarks", "data", dataset)
    const bucketUri = getBucketUri()

    const result: TableSpec[] = []
    for (const entryName of await fs.readdir(datasetPath)) {
        const dir = path.join(datasetPath, entryName)
        if (await isDirWithAllParquetFiles(dir)) {
            result.push({
                name: entryName,
                schema: dataset,
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
    const datasetSuffix = dataset.split("_")[0]
    const queriesPath = path.join(ROOT, "testdata", datasetSuffix, "queries")

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
                    plan: ""
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
                    plan: ""
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
                plan: response.plan
            })

            console.log(
                `Query ${id} iteration ${i} took ${Math.round(response.elapsed)} ms and returned ${response.rowCount} rows`
            );
        }

        console.log(`Query ${id} avg time: ${result.avg()} ms`);

        benchmarkRun.results.push(result)
    }

    // Write results and compare
    benchmarkRun.compareWithPrevious()
    benchmarkRun.store()
}
