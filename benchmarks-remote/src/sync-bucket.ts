import {execFileSync} from "child_process";
import {Command} from "commander";
import fs from "fs";
import path from "path";
import {getBucketUri} from "./@bench-common";
import {datasetParts, testdataRoots} from "./@paths";

export interface Dataset {
    name: string;
    source: string;
}

export function containsParquetFiles(directory: string): boolean {
    for (const entry of fs.readdirSync(directory, {withFileTypes: true})) {
        const entryPath = path.join(directory, entry.name);
        if (entry.isFile() && entry.name.endsWith(".parquet")) {
            return true;
        }
        if (isDirectory(entryPath, entry) && containsParquetFiles(entryPath)) {
            return true;
        }
    }
    return false;
}

function isDirectory(directory: string, entry: fs.Dirent): boolean {
    if (entry.isDirectory()) {
        return true;
    }
    if (!entry.isSymbolicLink()) {
        return false;
    }
    try {
        return fs.statSync(directory).isDirectory();
    } catch {
        return false;
    }
}

export function discoverDatasets(testdataPaths: string | string[]): Dataset[] {
    const datasets: Dataset[] = [];
    const discovered = new Set<string>();
    for (const testdataPath of typeof testdataPaths === "string" ? [testdataPaths] : testdataPaths) {
        if (!fs.existsSync(testdataPath)) {
            continue;
        }
        for (const suiteEntry of fs.readdirSync(testdataPath, {withFileTypes: true})) {
            const suitePath = path.join(testdataPath, suiteEntry.name);
            if (!isDirectory(suitePath, suiteEntry) || !fs.existsSync(path.join(suitePath, "queries"))) {
                continue;
            }
            for (const entry of fs.readdirSync(suitePath, {withFileTypes: true})) {
                const source = path.join(suitePath, entry.name);
                if (entry.name === "queries" || !isDirectory(source, entry)) {
                    continue;
                }
                const name = `${suiteEntry.name}/${entry.name}`;
                if (discovered.has(name)) {
                    continue;
                }
                discovered.add(name);
                datasets.push({
                    name,
                    source,
                });
            }
        }
    }
    return datasets;
}

export function selectDatasets(datasets: Dataset[], requested: string[]): Dataset[] {
    if (requested.length === 0) {
        return datasets;
    }

    for (const dataset of requested) {
        datasetParts(dataset);
    }
    const byName = new Map(datasets.map(dataset => [dataset.name, dataset]));
    const unknown = requested.filter(name => !byName.has(name));
    if (unknown.length > 0) {
        const available = datasets.map(dataset => dataset.name).join(", ") || "none";
        throw new Error(`Unknown dataset(s): ${unknown.join(", ")}. Available datasets: ${available}`);
    }

    return [...new Set(requested)].map(name => byName.get(name)!);
}

function main() {
    const program = new Command()
        .option("--list", "List locally available datasets without syncing")
        .option("-d, --dataset <dataset...>", "Sync only the selected dataset(s)")
        .parse(process.argv);
    const options = program.opts<{list?: boolean; dataset?: string[]}>();
    const datasets = discoverDatasets(testdataRoots());

    if (options.list) {
        for (const dataset of datasets) {
            console.log(dataset.name);
        }
        return;
    }

    const selected = selectDatasets(datasets, options.dataset ?? []);
    const empty = selected.filter(dataset => !containsParquetFiles(dataset.source));
    if (empty.length > 0) {
        throw new Error(`Dataset(s) contain no Parquet files: ${empty.map(dataset => dataset.name).join(", ")}`);
    }
    const target = getBucketUri().replace(/\/+$/, "");
    for (const dataset of selected) {
        const destination = `${target}/${dataset.name}`;
        console.log(`Syncing local dataset '${dataset.source}' to '${destination}'...`);
        execFileSync(
            "aws",
            [
                "s3",
                "sync",
                dataset.source,
                destination,
                "--delete",
                "--exclude",
                "*",
                "--include",
                "*.parquet",
            ],
            {stdio: "inherit"},
        );
    }
}

if (require.main === module) {
    try {
        main();
    } catch (error) {
        console.error(error instanceof Error ? error.message : error);
        process.exit(1);
    }
}
