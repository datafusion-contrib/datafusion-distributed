# Spawn a Worker

A `Worker` is a gRPC server that implements the Arrow Flight protocol for distributed query execution.
Worker nodes run these endpoints to receive execution plans, execute them, and stream results back.

## Overview

The `Worker` is the core worker component in Distributed DataFusion. It:

- Receives serialized execution plans via Arrow Flight's `do_get` method
- Deserializes plans using protobuf and user-provided codecs
- Executes plans using the local DataFusion runtime
- Streams results back as Arrow record batches through the gRPC Arrow Flight interface

## Launching the Arrow Flight server

The `Default` implementation of the `Worker` should satisfy most basic use cases

```rust
use datafusion_distributed::Worker;

async fn main() {
    let endpoint = Worker::default();

    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

If you are using DataFusion, though, it's very likely that you have your own custom UDFs, execution nodes, config
options, etc...

You'll need to tell the `Worker` how to build your DataFusion sessions:

```rust
async fn build_state(ctx: WorkerQueryContext) -> Result<SessionState, DataFusionError> {
    Ok(ctx
        .builder
        .with_scalar_functions(vec![your_custom_udf()])
        .build())
}

async fn main() {
    let endpoint = Worker::from_session_builder(build_sate);

    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

### WorkerSessionBuilder

The `WorkerSessionBuilder` is a closure or type that implements:

```rust
#[async_trait]
pub trait WorkerSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: WorkerQueryContext,
    ) -> Result<SessionState, DataFusionError>;
}
```

It receives a `WorkerQueryContext` containing:

- `SessionStateBuilder`: A pre-populated session state builder in which you can inject your custom stuff
- `headers`: HTTP headers from the incoming request (useful for passing metadata)

## Serving the Endpoint

Convert the endpoint to a gRPC service and serve it:

```rust
use tonic::transport::Server;
use datafusion_distributed::Worker;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

async fn main() {
    let endpoint = Worker::default();

    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080))
        .await?;
}
```

The `into_flight_server()` method wraps the endpoint in a `FlightServiceServer` with high message size limits (
`usize::MAX`) to avoid chunking overhead for internal communication.
