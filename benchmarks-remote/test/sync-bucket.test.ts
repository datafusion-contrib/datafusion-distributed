import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import {validateDatasetNames} from "../src/destroy-dataset";
import {containsParquetFiles, Dataset, discoverDatasets, selectDatasets} from "../src/sync-bucket";

const datasets: Dataset[] = [
    {name: "tpch/sf1", source: "/testdata/tpch/sf1"},
    {name: "tpcds/sf10", source: "/testdata/tpcds/sf10"},
];

test("selects requested datasets without changing sync-all compatibility", () => {
    assert.deepEqual(selectDatasets(datasets, []), datasets);
    assert.deepEqual(selectDatasets(datasets, ["tpcds/sf10", "tpch/sf1", "tpcds/sf10"]), [
        datasets[1],
        datasets[0],
    ]);
});

test("rejects unknown datasets before syncing", () => {
    assert.throws(
        () => selectDatasets(datasets, ["tpch/sf100"]),
        /Unknown dataset\(s\): tpch\/sf100\. Available datasets: tpch\/sf1, tpcds\/sf10/,
    );
});

test("discovers ignored datasets from a shared worktree without overriding local data", t => {
    const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "benchmark-datasets-"));
    t.after(() => fs.rmSync(temporary, {recursive: true, force: true}));
    const local = path.join(temporary, "local");
    const shared = path.join(temporary, "shared");
    fs.mkdirSync(path.join(local, "tpch", "queries"), {recursive: true});
    fs.mkdirSync(path.join(shared, "tpch", "queries"), {recursive: true});
    fs.mkdirSync(path.join(local, "tpch", "sf1"), {recursive: true});
    fs.mkdirSync(path.join(shared, "tpch", "sf1"), {recursive: true});
    fs.mkdirSync(path.join(shared, "tpch", "sf10"), {recursive: true});

    assert.deepEqual(discoverDatasets([local, shared]), [
        {name: "tpch/sf1", source: path.join(local, "tpch", "sf1")},
        {name: "tpch/sf10", source: path.join(shared, "tpch", "sf10")},
    ]);
});

test("discovers datasets symlinked into a worktree", t => {
    const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "benchmark-dataset-link-"));
    t.after(() => fs.rmSync(temporary, {recursive: true, force: true}));
    const local = path.join(temporary, "local");
    const sharedDataset = path.join(temporary, "shared", "sf10");
    const linkedDataset = path.join(local, "tpch", "sf10");
    fs.mkdirSync(sharedDataset, {recursive: true});
    fs.mkdirSync(path.join(local, "tpch", "queries"), {recursive: true});
    fs.mkdirSync(path.dirname(linkedDataset), {recursive: true});
    fs.symlinkSync(sharedDataset, linkedDataset, "dir");

    assert.deepEqual(discoverDatasets(local), [
        {name: "tpch/sf10", source: linkedDataset},
    ]);
});

test("accepts arbitrary Parquet filenames and rejects empty datasets", t => {
    const temporary = fs.mkdtempSync(path.join(os.tmpdir(), "benchmark-parquet-files-"));
    t.after(() => fs.rmSync(temporary, {recursive: true, force: true}));
    const clickbench = path.join(temporary, "clickbench");
    const empty = path.join(temporary, "empty");
    fs.mkdirSync(path.join(clickbench, "hits"), {recursive: true});
    fs.mkdirSync(empty, {recursive: true});
    fs.writeFileSync(path.join(clickbench, "hits", "0.parquet"), "");
    fs.writeFileSync(path.join(clickbench, "previous-remote.json"), "{}");

    assert.equal(containsParquetFiles(clickbench), true);
    assert.equal(containsParquetFiles(empty), false);
});

test("requires safe dataset prefixes for deletion", () => {
    assert.deepEqual(validateDatasetNames(["tpch/sf1", "clickbench/0-100", "tpch/sf1"]), [
        "tpch/sf1",
        "clickbench/0-100",
    ]);
    assert.throws(() => validateDatasetNames([".benchmark-artifacts"]), /Invalid dataset/);
    assert.throws(() => validateDatasetNames(["tpch/sf1\/../"]), /Invalid dataset/);
});
