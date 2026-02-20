import {execSync} from "child_process";
import path from "path";
import {getBucketUri, ROOT} from "./@bench-common";

const localDataPath = path.join(ROOT, "benchmarks", "data");
// Keep a trailing slash so `aws s3 sync` treats destination as a prefix, not a renamed key.
const target = `${getBucketUri().replace(/\/+$/, "")}/`;

console.log(`Syncing local data '${localDataPath}' to '${target}'...`);
execSync(`aws s3 sync "${localDataPath}" "${target}"`, {stdio: "inherit"});
