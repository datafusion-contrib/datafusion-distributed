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


When not to use this library
----------------------------

This library doesn't change DataFusion's execution model — so it inherits
its limitations too. In both vanilla and Distributed DataFusion:

- If any node fails mid-query, the whole query fails; there are no retries.
- There's no persistence of intermediate results, so queries can't checkpoint
  or resume from where they stopped.

If your workload is large, long-running batch or ETL that benefits from
materializing intermediate results between stages,
`Apache DataFusion Ballista <https://github.com/apache/datafusion-ballista>`_
is worth a look: it's also built on DataFusion, but runs as a standing cluster —
a scheduler plus executors — that writes shuffle data to disk between stages
rather than streaming it.

Distributed DataFusion instead targets fast,
interactive analytics — the kind of query where someone is waiting on the other
side of the screen for the answer.

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
   :caption: Contributor Guide
   :hidden:

   contributor-guide/01-index
   contributor-guide/02-setup
   contributor-guide/03-tests
   contributor-guide/04-benchmarks
