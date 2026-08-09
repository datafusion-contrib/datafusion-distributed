import {execFileSync} from "child_process";
import path from "path";
import test from "node:test";

test("benchmark lock is owned, released, and recoverable after expiry", () => {
    execFileSync("bash", [path.resolve(__dirname, "benchmark-lock.sh")], {stdio: "pipe"});
});
