import path from "path";

export const ROOT = path.join(__dirname, "../../..");

export function datasetPath(dataset: string): string {
    const separator = dataset.indexOf("_");
    const suite = separator === -1 ? dataset : dataset.slice(0, separator);
    const variant = separator === -1 ? "" : dataset.slice(separator + 1);
    const directory = suite === "clickbench" && variant
        ? `benchmark_range${variant}`
        : variant
            ? `benchmark_${variant}`
            : "benchmark";

    return path.join(ROOT, "testdata", suite, directory);
}
