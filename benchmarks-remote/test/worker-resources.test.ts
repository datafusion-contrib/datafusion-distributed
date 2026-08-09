import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import test from "node:test";

const root = path.resolve(__dirname, "..");
const commonResourceTemplate =
    'toYaml (required "workerResources is required" .Values.workerResources) | nindent 12';

test("all engine workers consume the same node-filling resources", () => {
    const values = fs.readFileSync(path.join(root, "k8s/worker-resources.yaml"), "utf8");
    assert.equal(
        values,
        ['workerResources:', '  requests:', '    cpu: "7"', '    memory: 17Gi', '  limits:', '    cpu: "7"', '    memory: 17Gi', ''].join("\n"),
    );

    for (const template of [
        "k8s/datafusion/templates/worker.yaml",
        "k8s/trino/templates/workload.yaml",
        "k8s/spark/templates/workload.yaml",
        "k8s/ballista/templates/workload.yaml",
    ]) {
        const contents = fs.readFileSync(path.join(root, template), "utf8");
        assert.equal(contents.split(commonResourceTemplate).length - 1, 1, template);
    }

    const deploy = fs.readFileSync(path.join(root, "k8s/deploy-engine.sh"), "utf8");
    assert.match(deploy, /--values .*worker-resources\.yaml/);
});

test("all engine charts default to twelve workers", () => {
    const defaults = [
        ["k8s/datafusion/values.yaml", /^  replicas: 12$/m],
        ["k8s/trino/values.yaml", /^workerReplicas: 12$/m],
        ["k8s/spark/values.yaml", /^workerReplicas: 12$/m],
        ["k8s/ballista/values.yaml", /^workerReplicas: 12$/m],
    ] as const;
    for (const [valuesPath, expected] of defaults) {
        assert.match(fs.readFileSync(path.join(root, valuesPath), "utf8"), expected, valuesPath);
    }
});
