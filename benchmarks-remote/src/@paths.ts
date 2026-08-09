import {execFileSync} from "child_process";
import fs from "fs";
import path from "path";

export const ROOT = path.join(__dirname, "../..");

export function datasetParts(dataset: string): [string, string] {
    const match = /^([a-zA-Z0-9._-]+)\/([a-zA-Z0-9._-]+)$/.exec(dataset);
    if (!match) {
        throw new Error(`Invalid dataset '${dataset}'; expected a path such as tpch/sf10`);
    }
    return [match[1], match[2]];
}

export function testdataRoots(): string[] {
    if (process.env.BENCHMARK_TESTDATA_ROOT) {
        return [path.resolve(process.env.BENCHMARK_TESTDATA_ROOT)];
    }

    const roots = [path.join(ROOT, "testdata")];
    try {
        const commonGitDirectory = execFileSync(
            "git",
            ["-C", ROOT, "rev-parse", "--path-format=absolute", "--git-common-dir"],
            {encoding: "utf8", stdio: ["ignore", "pipe", "ignore"]},
        ).trim();
        roots.push(path.join(path.dirname(commonGitDirectory), "testdata"));
    } catch {
        // ROOT may be copied outside a Git checkout; the local testdata path is still valid.
    }
    return [...new Set(roots)];
}

export function datasetPath(dataset: string): string {
    const relative = path.join(...datasetParts(dataset));
    const roots = testdataRoots();
    return roots.map(root => path.join(root, relative)).find(candidate => fs.existsSync(candidate))
        ?? path.join(roots[0], relative);
}
