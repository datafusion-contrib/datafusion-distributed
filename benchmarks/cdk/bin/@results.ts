import * as fs from 'fs';
import * as path from 'path';
import { z } from "zod";

// Assuming DATA_PATH is defined elsewhere or passed as parameter
export const DATA_PATH = path.join(__dirname, '../../data');
export const RESULTS_DIR = ".results-remote"

// Interface for a single iteration of a benchmark query
export interface QueryIter {
    plan: string;
    rowCount: number;
    elapsed: number; // Duration in milliseconds
    tasks: number;
    error?: string;
}

// Class for collecting benchmark run data
export class BenchmarkRun {
    startTime: number;
    dataset: string;
    engine: string;
    results: BenchResult[];

    constructor(dataset: string, engine: string) {
        this.dataset = dataset;
        this.engine = engine;
        this.startTime = Math.floor(Date.now() / 1000); // Unix timestamp in seconds
        this.results = [];
    }

    loadPrevious(): BenchmarkRun | null {
        const previousPath = path.join(DATA_PATH, this.dataset, `previous-remote.json`);

        try {
            const prevData = fs.readFileSync(previousPath, 'utf-8');
            const prevOutput = JSON.parse(prevData) as BenchmarkRun;

            // Create new instance and load results
            const instance = new BenchmarkRun(prevOutput.dataset, prevOutput.engine);
            instance.startTime = prevOutput.startTime;
            instance.loadResults();

            return instance;
        } catch {
            return null;
        }
    }

    loadResults(): void {
        this.results = BenchResult.loadMany(this.dataset, this.engine);
    }

    // Write data as JSON into output path if it exists
    store(): void {
        const outputPath = path.join(DATA_PATH, this.dataset, `previous-remote.json`);

        // Ensure directory exists
        const dir = path.dirname(outputPath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        // Custom serialization to handle results
        const toSerialize = {
            ...this,
            results: [] // Empty array for serialization
        };

        const json = JSON.stringify(toSerialize, null, 2);
        fs.writeFileSync(outputPath, json);

        // Store individual results
        for (const result of this.results) {
            result.store();
        }
    }

    compare(other: BenchmarkRun): void {
        console.log(`=== Comparing ${this.dataset} results from engine '${other.engine}' [prev] with '${this.engine}' [new] ===`);
        let totalTimePrev = 0
        let totalTimeNew = 0
        for (const query of this.results) {
            const prevQuery = other.results.find(v => v.id === query.id);
            if (!prevQuery) {
                continue;
            }
            const timePrev = prevQuery.representativeTime()
            const timeNew = query.representativeTime()
            if (timePrev && timeNew) {
                totalTimePrev += timePrev
                totalTimeNew += timeNew
            }

            query.compare(prevQuery);
        }

        let f, tag, emoji
        if (totalTimeNew < totalTimePrev) {
            f = totalTimePrev / totalTimeNew;
            tag = "faster";
            emoji = f > 1.2 ? "✅" : "✔";
        } else {
            f = totalTimeNew / totalTimePrev;
            tag = "slower";
            emoji = f > 1.2 ? "❌" : "✖";
        }
        console.log(
            `${"TOTAL".padStart(8)}: prev=${totalTimePrev.toString()} ms, new=${totalTimeNew.toString()} ms, diff=${f.toFixed(2)} ${tag} ${emoji}`
        );
    }

    compareWithPrevious(): void {
        const previous = this.loadPrevious();
        if (!previous) {
            return;
        }
        this.compare(previous)
    }
}

// Class for a single benchmark case
export class BenchResult {
    id: string;
    dataset: string;
    engine: string;
    iterations: QueryIter[];

    constructor(dataset: string, engine: string, id: string) {
        this.dataset = dataset;
        this.engine = engine;
        this.id = id;
        this.iterations = [];
    }

    // Median (p50) of the successful iteration latencies in ms. The median is robust to
    // warmup/GC/noise outliers, and — unlike a mean or a sum — it does not grow with the number
    // of iterations, so runs done with different `-i` values stay comparable.
    p50(): number {
        const xs = this.iterations
            .filter(iter => !iter.error)
            .map(iter => iter.elapsed)
            .sort((a, b) => a - b);
        if (xs.length === 0) {
            return 0;
        }
        const mid = Math.floor(xs.length / 2);
        const median = xs.length % 2 ? xs[mid] : (xs[mid - 1] + xs[mid]) / 2;
        return Math.round(median);
    }

    // Representative single-run time used to aggregate a suite TOTAL. Returns undefined when any
    // iteration errored, so the query is dropped from the total. Because it is a per-query p50
    // (not a sum of all iterations), the TOTAL is independent of the iteration count: a run with
    // `-i 3` and a run with `-i 5` produce comparable totals.
    representativeTime(): undefined | number {
        if (this.iterations.some(iter => iter.error)) {
            return undefined;
        }
        return this.p50();
    }

    compare(prevQuery: BenchResult): void {
        const prevErr = prevQuery.iterations.find(v => v.error)?.error;
        const newErr = this.iterations.find(v => v.error)?.error;

        if (prevErr && !newErr) {
            console.log(`${this.id}: Previously failed, but now succeeded 🟠`);
            return;
        }
        if (!prevErr && newErr) {
            console.log(`${this.id}: Previously succeeded, but now failed ❌`);
            return;
        }
        if (prevErr && newErr) {
            console.log(`${this.id}: Previously failed, and now also failed ❌`);
            return;
        }

        const p50Prev = prevQuery.p50();
        const p50 = this.p50();

        let f: number;
        let tag: string;
        let emoji: string;

        if (p50 < p50Prev) {
            f = p50Prev / p50;
            tag = "faster";
            emoji = f > 1.2 ? "✅" : "✔";
        } else {
            f = p50 / p50Prev;
            tag = "slower";
            emoji = f > 1.2 ? "❌" : "✖";
        }

        console.log(
            `${this.id.padStart(8)}: prev=${p50Prev.toString().padStart(4)} ms, new=${p50.toString().padStart(4)} ms, diff=${f.toFixed(2)} ${tag} ${emoji}`
        );
    }

    store(): void {
        const filePath = path.join(
            DATA_PATH,
            this.dataset,
            RESULTS_DIR,
            this.engine,
            `${this.id}.json`
        );

        // Ensure directory exists
        const dir = path.dirname(filePath);
        if (!fs.existsSync(dir)) {
            fs.mkdirSync(dir, { recursive: true });
        }

        const json = JSON.stringify(this, null, 2);
        fs.writeFileSync(filePath, json);
    }

    static load(dataset: string, engine: string, id: string): BenchResult | null {
        const filePath = path.join(
            DATA_PATH,
            dataset,
            RESULTS_DIR,
            engine,
            `${id}.json`
        );

        try {
            const parser = z.object({
                dataset: z.string(),
                engine: z.string(),
                id: z.string(),
                iterations: z.object({
                    rowCount: z.number(),
                    elapsed: z.number(),
                    error: z.string().optional(),
                    plan: z.string(),
                    tasks: z.number().default(0),
                }).array(),
            })
            const data = fs.readFileSync(filePath, 'utf-8');
            const parsed = parser.parse(JSON.parse(data))
            const result = new BenchResult(
                parsed.dataset,
                parsed.engine,
                parsed.id
            )
            result.iterations = parsed.iterations
            return result;
        } catch {
            return null;
        }
    }

    static loadMany(dataset: string, engine: string): BenchResult[] {
        const resultsDir = path.join(DATA_PATH, dataset, RESULTS_DIR, engine);
        const results: BenchResult[] = [];

        try {
            const files = fs.readdirSync(resultsDir);

            for (const fileName of files) {
                if (!fileName.endsWith('.json')) {
                    continue;
                }

                const id = fileName.slice(0, -5); // Remove .json extension
                const result = BenchResult.load(dataset, engine, id);

                if (result) {
                    results.push(result);
                }
            }
        } catch {
            // Directory doesn't exist or can't be read
            return results;
        }

        results.sort((a, b) => numericId(a.id) > numericId(b.id) ? 1 : -1)
        return results;
    }
}

function numericId(queryName: string): number {
    return parseInt([...queryName.matchAll(/(\d+)/g)][0][0])
}
