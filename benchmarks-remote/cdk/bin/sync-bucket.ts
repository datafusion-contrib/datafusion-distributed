import {execSync} from "child_process";
import fs from "fs";
import path from "path";
import {getBucketUri} from "./@bench-common";
import {ROOT} from "./@paths";

const testdataPath = path.join(ROOT, "testdata");
const target = getBucketUri().replace(/\/+$/, "");

for (const suite of ["tpch", "tpcds", "clickbench"]) {
    const suitePath = path.join(testdataPath, suite);
    for (const entry of fs.readdirSync(suitePath, {withFileTypes: true})) {
        if (!entry.isDirectory() || !entry.name.startsWith("benchmark_")) {
            continue;
        }
        const variant = entry.name.slice("benchmark_".length).replace(/^range/, "");
        const dataset = `${suite}_${variant}`;
        const source = path.join(suitePath, entry.name);
        console.log(`Syncing local dataset '${source}' to '${target}/${dataset}'...`);
        execSync(`aws s3 sync "${source}" "${target}/${dataset}"`, {stdio: "inherit"});
    }
}
