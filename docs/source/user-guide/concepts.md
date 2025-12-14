# Getting started with distributed DataFusion

This library contains a set of extensions to DataFusion that allow you to run distributed queries.

These are some terms you should be familiar with before getting started:

- `Stage`: a portion of the plan separated by a network boundary from other parts of the plan. A plan contains
  one or more stages.
- `Task`: a unit of work in a stage that executes the inner plan in parallel to other tasks within the stage. Each task
  in a stage is executed but a different worker.
- `Worker`: a physical machine listening to serialized execution plans over an Arrow Flight interface.
- `Network boundary`: a node in the plan that streams data from a network interface rather than directly from its
  children. Implemented as DataFusion `ExecutionPlan`s: `NeworkShuffle` and `NetworkCoalesce`.
- `Subplan`: a slice of the overall plan. Each stage will execute a subplan of the overall plan.

## [DistributedPhysicalOptimizerRule](https://github.com/datafusion-contrib/datafusion-distributed/blob/6d014eaebd809bcbe676823698838b2c83d93900/src/distributed_planner/distributed_physical_optimizer_rule.rs#L60)

This is a physical optimizer rule that converts a single-node DataFusion query into a distributed query. It reads
a fully formed physical plan and injects the appropriate nodes to execute the query in a distributed fashion.

It builds the distributed plan from bottom to top, and based on the present nodes in the original plan,
it will inject network boundaries in the appropriate places.

## [TaskEstimator](https://github.com/datafusion-contrib/datafusion-distributed/blob/6d014eaebd809bcbe676823698838b2c83d93900/src/distributed_planner/task_estimator.rs#L40-L40)

Estimates the number of tasks required in the leaf stage of a distributed query.

The number of tasks each stage has is determined from bottom to top. This means that leaf stages, depending on the
amount of data their leaf nodes are going to pull, they will decide how many tasks they need to execute. Upper stages
will get their number of tasks reduced or augmented depending on how much the cardinality of the data was reduced in
previous stages.

## [DistributedTaskContext](https://github.com/datafusion-contrib/datafusion-distributed/blob/6d014eaebd809bcbe676823698838b2c83d93900/src/stage.rs#L137-L137)

A `SessionConfig` extension, present during the `execute()` calls to every `ExecutionPlan` that contains information
about in which tasks the plan is being executed.

As a user, you will need to interact with this type in your custom leaf nodes, as dependending on in which task index
you are in, you might want to return a different set of data.

E.g. if you are on task with index 0 of a 3-task stage, you might want to return only the first 1/3 of the data. If
you are on task with index 2, you might want to return the last 2/3 of the data, and so on.

## [ChannelResolver](https://github.com/datafusion-contrib/datafusion-distributed/blob/6d014eaebd809bcbe676823698838b2c83d93900/src/channel_resolver_ext.rs#L57-L57)

Establishes the number of workers available in the distributed DataFusion cluster, their URLs, and how to connect
to them.

Each company does networking in a different way, so this extension allows you to plug in your a custom networking
implementation that appears to each company's needs.

## [NetworkBoundary](https://github.com/datafusion-contrib/datafusion-distributed/blob/6d014eaebd809bcbe676823698838b2c83d93900/src/distributed_planner/network_boundary.rs#L23-L23)

A network boundary is a node that instead of pulling data from their children by executing them, it serializes them
and sends them over the wire so that they are executed in a remote worker.

As a user of distributed DataFusion, you will not need to interact with this trait directly, but you'll need to know
that different implementations of this trait will be injected on your plans so that the queries get distributed.
