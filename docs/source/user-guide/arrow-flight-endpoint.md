# Building an Arrow Flight Endpoint

An `ArrowFlightEndpoint` is a gRPC server that implements the Arrow Flight protocol for distributed query execution.
Worker nodes run these endpoints to receive execution plans, execute them, and stream results back.

## Overview

The `ArrowFlightEndpoint` is the core worker component in Distributed DataFusion. It:

- Receives serialized execution plans via Arrow Flight's `do_get` method
- Deserializes plans using protobuf and user-provided codecs
- Executes plans using the local DataFusion runtime
- Streams results back as Arrow record batches through the gRPC Arrow Flight interface

## Creating an ArrowFlightEndpoint

The `Default` implementation of the `ArrowFlightEndpoint` should satisfy most basic use cases

```rust
use datafusion_distributed::{ArrowFlightEndpoint};

async fn main() {
    let endpoint = ArrowFlightEndpoint::default();

    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

If you are using DataFusion though, it's very likely that you have your own custom UDFs, execution nodes, config options
etc...

You'll need to tell the `ArrowFlightEndpoint` how to build your DataFusion sessions:

```rust
async fn build_state(ctx: DistributedSessionBuilderContext) -> Result<SessionState, DataFusionError> {
    Ok(ctx
        .builder
        .with_scalar_functions(vec![your_custom_udf()])
        .build())
}

async fn main() {
    let endpoint = ArrowFlightEndpoint::from_session_builder(build_sate);

    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8000))
        .await?;

    Ok(())
}
```

### DistributedSessionBuilder

The `DistributedSessionBuilder` is a closure or type that implements:

```rust
#[async_trait]
pub trait DistributedSessionBuilder {
    async fn build_session_state(
        &self,
        ctx: DistributedSessionBuilderContext,
    ) -> Result<SessionState, DataFusionError>;
}
```

It receives a `DistributedSessionBuilderContext` containing:

- `SessionStateBuilder`: A pre-populated session state builder in which you can inject your custom stuff
- `headers`: HTTP headers from the incoming request (useful for passing metadata)

## Serving the Endpoint

Convert the endpoint to a gRPC service and serve it:

```rust
use tonic::transport::Server;
use datafusion_distributed::ArrowFlightEndpoint;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

async fn main() {
    let endpoint = ArrowFlightEndpoint::default();

    Server::builder()
        .add_service(endpoint.into_flight_server())
        .serve(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080))
        .await?;
}
```

The `into_flight_server()` method wraps the endpoint in a `FlightServiceServer` with high message size limits (
`usize::MAX`) to avoid chunking overhead for internal communication.
