========================
DataFusion Distributed
========================

DataFusion Distributed is a library that enhances `Apache DataFusion <https://datafusion.apache.org>`_ with distributed
capabilities.

These docs will guide you towards using the library for building your own Distributed DataFusion cluster, and
how to contribute changes to the library yourself.

.. _toc.understand:
.. toctree::
   :maxdepth: 2
   :caption: Understand

   user-guide/concepts
   user-guide/how-a-distributed-plan-is-built

.. _toc.cluster:
.. toctree::
   :maxdepth: 2
   :caption: Set up a cluster

   user-guide/getting-started
   user-guide/worker
   user-guide/worker-resolver
   user-guide/channel-resolver

.. _toc.customize:
.. toctree::
   :maxdepth: 2
   :caption: Customize execution

   user-guide/task-estimator
   user-guide/work-unit-feeds
   user-guide/custom-distributed-plans
   user-guide/metrics

.. _toc.contributor-guide:
.. toctree::
   :maxdepth: 2
   :caption: Contributor Guide

   contributor-guide/index
   contributor-guide/setup
   contributor-guide/tests
   contributor-guide/benchmarks
