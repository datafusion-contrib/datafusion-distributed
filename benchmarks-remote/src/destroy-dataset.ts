import {execFileSync} from "child_process";
import {Command} from "commander";
import {getBucketUri} from "./@bench-common";
import {datasetParts} from "./@paths";

export function validateDatasetNames(datasets: string[]): string[] {
    if (datasets.length === 0) {
        throw new Error("Select at least one dataset with --dataset");
    }
    for (const dataset of datasets) {
        datasetParts(dataset);
    }
    return [...new Set(datasets)];
}

function main() {
    const program = new Command()
        .requiredOption("-d, --dataset <dataset...>", "Delete the selected dataset(s)")
        .option("--yes", "Confirm permanent deletion from S3")
        .parse(process.argv);
    const options = program.opts<{dataset: string[]; yes?: boolean}>();
    if (!options.yes) {
        throw new Error("Pass --yes to confirm permanent dataset deletion");
    }

    const datasets = validateDatasetNames(options.dataset);
    const bucket = getBucketUri().replace(/\/+$/, "");
    for (const dataset of datasets) {
        const target = `${bucket}/${dataset}`;
        console.log(`Deleting '${target}'...`);
        execFileSync("aws", ["s3", "rm", target, "--recursive"], {stdio: "inherit"});
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
