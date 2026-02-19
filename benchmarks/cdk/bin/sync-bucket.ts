import {execSync} from "child_process";
import path from "path";
import {getBucketUri, ROOT} from "./@bench-common";

const localDataPath = path.join(ROOT, "benchmarks", "data");
const target = `${getBucketUri().replace(/\/+$/, "")}/`;

console.log(`Syncing local data '${localDataPath}' to '${target}'...`);
execSync(`aws s3 sync "${localDataPath}" "${target}"`, {stdio: "inherit"});
