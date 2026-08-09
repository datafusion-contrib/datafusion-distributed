import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

test("benchmark results remain local", () => {
    const runner = fs.readFileSync(path.resolve(__dirname, "../k8s/run-benchmark.sh"), "utf8");
    assert.doesNotMatch(runner, /resultsBucketName|RESULTS_BUCKET|_SUCCESS|head-object/);
    assert.match(runner, /Benchmark run completed/);
});

test("all benchmark clients use the same local port", () => {
    for (const client of ["datafusion-bench.ts", "trino-bench.ts", "spark-bench.ts", "ballista-bench.ts"]) {
        const source = fs.readFileSync(path.resolve(__dirname, "../src", client), "utf8");
        assert.match(source, /http:\/\/localhost:9000/, client);
    }

    const runner = fs.readFileSync(path.resolve(__dirname, "../k8s/run-benchmark.sh"), "utf8");
    assert.match(runner, /"9000:9000"/);
    assert.doesNotMatch(runner, /case \$\{engine\}|_URL=/);
});
