use std::io::Result;

// Build script for generating Rust bindings from protobuf definitions.
// This runs during `cargo build` and requires `protoc` to be available in PATH.
// The output is compiled into the crate, so no generated files are checked in.

fn main() -> Result<()> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&["src/proto/observability.proto"], &["src/proto"])?;

    Ok(())
}
