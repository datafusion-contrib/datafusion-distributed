======================
DataFusion Distributed
======================

A library that extends `Apache DataFusion <https://datafusion.apache.org>`_ with distributed capabilities.

The problem
-----------

A single DataFusion node is fast, but it has a ceiling. Large scans, wide joins,
and heavy aggregations eventually run out of memory or CPU on one machine. The
usual escape routes are painful: build your own distribution layer on top of
DataFusion or abandon DataFusion for a heavyweight system that dictates how you
deploy and serve queries.

You might want this library if:

- You want to spread the CPU and memory pressure of an existing DataFusion query across several machines.
- You want your existing queries to run faster by putting more than one machine behind them.
- You want to have control over how your custom plans are distributed and assigned to workers.
- You want to distribute your queries without a framework dictating your deployment and networking setup.

It's still DataFusion
---------------------

A distributed plan **is** a normal DataFusion physical plan, with a few extra nodes that happen to stream
Arrow data between machines in a zero-copy manner using gRPC instead of passing it in memory — everything else
is the DataFusion you already know.

You can expect all your existing tooling built on top of DataFusion to work seamlessly with Distributed DataFusion.

Going distributed takes exactly three things:

1. **Enable the distributed planner**. Just a one-line addition while building the DataFusion session context.
2. **Tell it where your workers are**. Provide a list of worker URLs the distributed planner can use.
3. **Run the worker gRPC servers**. Mount them onto a gRPC service you already run, expose them on their own port, or deploy them however suits your infrastructure.

Everything else is opt-in.

.. note::

   This project is a library built on top of Apache DataFusion. It is **not**
   part of Apache DataFusion itself.


When to use Apache DataFusion Ballista instead of this library
--------------------------------------------------------------

`Apache DataFusion Ballista <https://github.com/apache/datafusion-ballista>`_ is a distributed query engine.
It has a scheduler that tracks executors, jobs, and stages; executors with per-task memory pools;
clients (Rust, Python, the CLI, and Flight SQL); and operational tooling (REST API, event logs,
history server, Kubernetes, and KEDA).

This project is a library intended to be embedded in existing DataFusion-based applications, allowing a system
to spread a query over workers it already manages. Like DataFusion, it does not persist intermediate results
for recovery: if a query fails mid-execution, it needs to be retried from scratch.

Consider using Ballista instead of this project if:

- You'd rather use an existing distributed engine as a foundation and customize it to your needs, rather than
  building a distributed query engine yourself.
- You are comfortable adopting Ballista's deployment and networking model.
- Your workload involves large, long-running batch queries or ETL that benefits from
  materializing intermediate results between stages.

.. toctree::
   :maxdepth: 1
   :caption: User Guide (basic)
   :hidden:

   user-guide/01-quick-start
   user-guide/02-worker-resolver
   user-guide/03-worker
   user-guide/04-distribute-custom-plan
   user-guide/05-metrics
   user-guide/06-channel-resolver

.. toctree::
   :maxdepth: 1
   :caption: User Guide (advanced)
   :hidden:

   advanced/01-passthrough-headers
   advanced/02-config-extensions
   advanced/03-plan-hooks
   advanced/04-work-unit-feeds
   advanced/05-custom-distributed-plans
   advanced/06-worker-routing
   advanced/07-worker-versioning
   advanced/08-adaptive-query-execution

.. toctree::
   :maxdepth: 1
   :caption: Learn
   :hidden:

   learn/01-concepts
   learn/02-how-a-distributed-plan-is-built
   learn/03-how-adaptive-query-execution-works

.. toctree::
   :maxdepth: 1
   :caption: Blog
   :hidden:

   blog/distributed-dynamic-filtering
   blog/reducing-shuffle-fanout

.. toctree::
   :maxdepth: 1
   :caption: Contributor Guide
   :hidden:

   contributor-guide/01-index
   contributor-guide/02-setup
   contributor-guide/03-tests
   contributor-guide/04-benchmarks
   contributor-guide/05-code-review
