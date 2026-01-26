# Distributed DataFusion Observability Proto Generation

---
This module provides gRPC-based observability services using Protocol Buffers.
The setup is inspired by [Apache DataFusion's proto crate](https://github.com/apache/datafusion/tree/main/datafusion/proto) structure but adapted
for tonic/gRPC services.

In the root of the datafusion-distribued repo, run:

```bash
./src/observability/gen/regen.sh
```
