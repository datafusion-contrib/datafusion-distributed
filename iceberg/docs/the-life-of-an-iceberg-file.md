# The life of an iceberg file

## One table, two scans

The Iceberg integration fixture contains a data file for each `pickup_date`.
This query reads the same date's file through two different `FROM` references:

```sql
WITH a AS (
    SELECT 'a' AS kind, pickup_date FROM taxi
    WHERE pickup_date = DATE '2024-01-08' AND trip_distance > 1
), b AS (
    SELECT 'b' AS kind, pickup_date FROM taxi
    WHERE pickup_date = DATE '2024-01-08' AND trip_distance > 2
)
SELECT kind, pickup_date, COUNT(*)
FROM (SELECT * FROM a UNION ALL SELECT * FROM b)
GROUP BY kind, pickup_date
ORDER BY kind, pickup_date;
```

`iceberg/tests/file_task_routing.rs` executes this shape through the in-memory
worker transport and verifies both results and routing hints. It is not a blob-cache
benchmark. In this test the two references remain distinct physical scans. A CTE
is not itself a distributed stage, and SQL syntax alone does not guarantee the
number of scans: inspect the physical plan.

## Names refer to different objects

| Name | Meaning |
|---|---|
| F1–F4 | Iceberg `FileScanTask` work descriptors, carrying a file path and scan options |
| S1/S2 | Physical scan operators consuming those descriptors |
| T1/T2 | DFD tasks executing task-specific stage plans, potentially including several operators |
| W1/W2 | Workers chosen to run those tasks |

A physical file can be referenced by **different** FileScanTasks. In the query
above, S1's descriptor F1 and S2's descriptor F2 reference the same file but have
different predicates. They are not interchangeable query computations.

## 1. Discover work before choosing parallelism

With opt-in `iceberg.plan_files=true`, both Iceberg table providers call
`IcebergDataSource::with_planned_files()` from `TableProvider::scan()`. Iceberg
discovers files surviving the available predicates. By default, discovery is lazy
at execution and does not provide planning-time assignment or affinity hints.
`PlannedFiles` retains native descriptors for execution and exposes candidate bytes
and row-count estimates to physical planning. Filtered row counts are not exact.

This is still connector work. No file rows have been read into the query yet.
The planner subsequently reconciles desired task counts and constraints across
the stage. The connector's desired count is a hint, not ownership of the whole stage.

## 2. Assign files to a scan's eligible tasks

For a scan S1 with four descriptors and two eligible tasks:

```text
S1: F1 F2 F3 F4

Round-robin: T1 = [F1, F3]    T2 = [F2, F4]
Greedy:      T1 = [F1]        T2 = [F2, F3, F4]
             (illustration where F1 is larger than the other three combined)
```

`IcebergWorkUnitFeed` controls this mapping. With
`iceberg.greedy_file_assignment=true`, it uses DFD's generic
`greedy_work_unit_assignment()` helper. The assignment is retained per scan and task
count, shared across plan clones, and used for both affinity hints and execution.
Within a task, its assigned files are distributed across local partitions.

**Control:** choose the file-to-task assignment within this scan's consumers.
**Not control:** create consumers in tasks that do not execute this scan, split a
file, or move the surrounding join/UNION branch into another task.

In an isolated UNION child the scan's task index/count are relative to that child,
not necessarily the outer stage. A one-task child has no file-placement choice.
DFD's UNION assignment is unchanged by this feature.

## 3. Describe the assignment before routing

`WorkUnitFeedProvider::task_affinity()` is an optional, non-consuming hook.
Iceberg enables it with `iceberg.file_task_affinity=true` and returns paths/byte
weights for **only the files assigned to the requested scan-local task**.

The coordinator captures these hints before converting native feeds to remote
handles. `RouteTaskEvent::work_unit_affinity()` exposes the hints to the router.
They remain coordinator-only and do not change the worker protocol.

Hints are opaque to DFD. They neither grant storage access nor identify cached
contents. The Iceberg hints assume immutable file paths. Neither predicates nor
query IDs are part of the affinity key, so different scans may share a data home
without sharing execution state.

## 4. Choose a worker for each whole task

The opt-in `AffinityRouteTaskHandler` elects a home worker for each distinct key by
rendezvous hashing and prefers the worker owning the greatest total hinted weight.
Repeated keys count once, with their maximum weight. Worker URLs must be stable
identities of individual cache owners.

```text
S1 → F1 → T1 → W1
S2 → F2 → T2 → W1
     same physical file
```

T1 and T2 are still separate tasks. Choosing W1 twice is colocation, not another
level of task grouping. The fixture test verifies that this happens for the query
above, even though each UNION child has its own scan-local task context.

**Control:** choose a worker for the entire task, with normal connection failover.
**Not control:** place different files of an already formed task on different
workers, guarantee cache residency, or enforce a worker-wide memory budget.
If one task contains files with different homes, not every preference can be met.
A custom router can use the hints and the built-in `rank_workers()` utility while
adding its own load/admission policy.

## 5. Deliver work, read bytes, and forward rows

The coordinator invokes `WorkUnitFeedProvider::feed()` and streams descriptors to
the selected worker's local partitions. The worker reads the files and executes
its operators. Network shuffle, broadcast or gather nodes move Arrow batches as
required by the physical plan. A local repartition alone does not move rows between
workers. Unknown scan partitioning permits whole-file reassignment; that freedom
must not be assumed for sources advertising hash/range partitioning or ordering.

The storage wrapper may cache raw ranges and coalesce concurrent misses. The
connector does not call that cache. Repeated readers still independently decode
and process data; colocation can increase CPU and memory concentration. Measure
actual range overlap, backend GETs, cache hits/coalescing, query latency and peak
worker memory. Candidate bytes and routing counters are not cache-hit measurements.
