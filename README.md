# DataFusion Distributed

Library that brings distributed execution capabilities to [DataFusion](https://github.com/apache/datafusion).

> [!WARNING]
> This project is currently under construction and is not yet ready for production use.

## What can you do with this crate?

This crate is a toolkit that extends [DataFusion](https://github.com/apache/datafusion) with distributed capabilities,
providing a developer experience as close as possible to vanilla DataFusion and being opinionated about the networking
stack used for hosting the different workers involved in a query.

Users of this library can expect to take their existing single-node DataFusion-based systems and add distributed
capabilities with minimal changes.

## Core tenets of the project

- Be as close as possible to vanilla DataFusion, providing a seamless integration with existing DataFusion systems and
  a familiar API for building applications.
- Unopinionated networking. This crate does not take any opinion about the networking stack, and users are expected
  to leverage their own infrastructure for hosting DataFusion nodes.
- No coordinator-worker architecture. To keep infrastructure simple, any node can act as a coordinator or a worker.

## Architecture

A distributed DataFusion query is executed in a very similar fashion as a normal DataFusion query with one key
difference:

The physical plan is divided into stages that can be executed in different machines, and exchange data using Arrow
Flight. All of this is done at the physical plan level, and is implemented as a `PhysicalOptimizerRule` that:

1. Inspects the non-distributed plan, placing network boundaries (`ArrowFlightReadExec` nodes) in the appropriate places
2. Based on the placed network boundaries, divides the plan into stages and assigns tasks to them.

For example, imagine we have a plan that looks like this:

```
   ┌──────────────────────┐
   │    ProjectionExec    │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   │       (final)        │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    RepartionExec     │
   │ (3 input partitions) │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   │      (partial)       │
   └──────────────────────┘  
              ▲
   ┌──────────┴───────────┐
   │    DataSourceExec    │
   └──────────────────────┘  
```

We want to distribute the aggregation to something like this:

```
                              ┌──────────────────────┐
                              │    ProjectionExec    │
                              └──────────────────────┘  
                                         ▲
                              ┌──────────┴───────────┐
                              │    AggregateExec     │
                              │       (final)        │
                              └──────────────────────┘  
                                       ▲ ▲ ▲
              ┌────────────────────────┘ │ └─────────────────────────┐
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │    AggregateExec     │   │    AggregateExec     │   │    AggregateExec     │
   │      (partial)       │   │      (partial)       │   │      (partial)       │
   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘
              ▲                          ▲                           ▲
   ┌──────────┴───────────┐   ┌──────────┴───────────┐   ┌───────────┴──────────┐
   │    DataSourceExec    │   │    DataSourceExec    │   │    DataSourceExec    │
   └──────────────────────┘   └──────────────────────┘   └──────────────────────┘
```

The first step is to place the `ArrowFlightReadExec` network boundary in the appropriate place (the following drawing
shows the partitioning scheme in each node):

```
   ┌──────────────────────┐
   │    ProjectionExec    │
   └─────────[0]──────────┘  
              ▲
   ┌──────────┴───────────┐
   │    AggregateExec     │
   │       (final)        │
   └─────────[0]──────────┘  
              ▲
   ┌──────────┴───────────┐
   │   ArrowFlightRead    │   <- this node was injected to tell the distributed planner 
   │   (3 input tasks)    │      that there must be a network boundary here.
   └──────[0][1][2]───────┘  
           ▲  ▲  ▲
   ┌───────┴──┴──┴────────┐
   │    AggregateExec     │
   │      (partial)       │
   └──────[0][1][2]───────┘  
           ▲  ▲  ▲
   ┌───────┴──┴──┴────────┐
   │    DataSourceExec    │
   └──────[0][1][2]───────┘  
```

Based on that boundary, the plan is divided into stages and tasks are assigned to each stage:

```                            
                              ┌────── (stage 2) ───────┐
                              │┌──────────────────────┐│
                              ││    ProjectionExec    ││
                              │└──────────┬───────────┘│
                              │┌──────────┴───────────┐│
                              ││    AggregateExec     ││
                              ││       (final)        ││
                              │└──────────┬───────────┘│
                              │┌──────────┴───────────┐│
                              ││  ArrowFlightReadExec ││
                              │└──────[0][1][2]───────┘│
                              └─────────▲─▲─▲──────────┘
               ┌────────────────────────┘ │ └─────────────────────────┐            
               │                          │                           │            
   ┌─── task 0 (stage 1) ───┐ ┌── task 1 (stage 1) ────┐ ┌── task 2 (stage 1) ────┐
   │           │            │ │           │            │ │            │           │
   │┌─────────[0]──────────┐│ │┌─────────[0]──────────┐│ │┌──────────[0]─────────┐│
   ││    AggregateExec     ││ ││    AggregateExec     ││ ││    AggregateExec     ││
   ││      (partial)       ││ ││      (partial)       ││ ││      (partial)       ││
   │└──────────┬───────────┘│ │└──────────┬───────────┘│ │└───────────┬──────────┘│
   │┌─────────[0]──────────┐│ │┌─────────[0]──────────┐│ │┌──────────[0]─────────┐│
   ││   DataSourceExec     ││ ││   DataSourceExec     ││ ││   DataSourceExec     ││
   │└──────────────────────┘│ │└──────────────────────┘│ │└──────────────────────┘│
   └────────────────────────┘ └────────────────────────┘ └────────────────────────┘
```

The plan is immediately executable, and the same process that planned the distributed query can start executing the head
stage (stage 2). The `ArrowFlightReadExec` in that stage will know from which tasks to gather data from stage 1, and
will issue 3 concurrent Arrow Flight requests to the appropriate physical nodes.

This means that:

1. The head stage is executed normally as if the query was not distributed.
2. Upon calling `.execute()` on the `ArrowFlightReadExec`, instead of recursively calling `.execute()` on its children,
   they will be serialized and sent over the wire to another node.
3. The next node, which is hosting an Arrow Flight Endpoint listening for gRPC requests over an HTTP server, will pick up
   the request containing the serialized chunk of the overall plan, and execute it.
4. This is repeated for each stage, and data will start flowing from bottom to top until it reaches the head stage.

