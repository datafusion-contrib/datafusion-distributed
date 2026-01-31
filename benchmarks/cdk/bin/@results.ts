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

    private loadResults(): void {
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

    compareWithPrevious(): void {
        const previous = this.loadPrevious();
        if (!previous) {
            return;
        }

        console.log(`=== Comparing ${this.dataset} results from engine '${previous.engine}' [prev] with '${this.engine}' [new] ===`);
        for (const query of this.results) {
            const prevQuery = previous.results.find(v => v.id === query.id);
            if (!prevQuery) {
                continue;
            }

            query.compare(prevQuery);
        }
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

    avg(): number {
        if (this.iterations.length === 0) {
            return 0;
        }

        const sum = this.iterations.reduce((acc, iter) => acc + iter.elapsed, 0);
        return Math.floor(sum / this.iterations.length);
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

        const avgPrev = prevQuery.avg();
        const avg = this.avg();

        let f: number;
        let tag: string;
        let emoji: string;

        if (avg < avgPrev) {
            f = avgPrev / avg;
            tag = "faster";
            emoji = f > 1.2 ? "✅" : "✔";
        } else {
            f = avg / avgPrev;
            tag = "slower";
            emoji = f > 1.2 ? "❌" : "✖";
        }

        console.log(
            `${this.id.padStart(8)}: prev=${avgPrev.toString().padStart(4)} ms, new=${avg.toString().padStart(4)} ms, diff=${f.toFixed(2)} ${tag} ${emoji}`
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
                    plan: z.string()
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
