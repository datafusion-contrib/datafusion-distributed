use std::env;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = env::current_dir()?;

    let protocol_dir = repo_root.join("src/protocol");
    let messages_proto = protocol_dir.join("messages.proto");
    let worker_service_proto = protocol_dir.join("grpc/worker_service.proto");
    let out_dir = repo_root.join("src/protocol/generated");
    let grpc_out_dir = repo_root.join("src/protocol/grpc/generated");

    fs::create_dir_all(&out_dir)?;
    fs::create_dir_all(&grpc_out_dir)?;

    println!("Generating protobuf code...");
    println!("Protocol dir: {protocol_dir:?}");
    println!("Messages proto: {messages_proto:?}");
    println!("Worker service proto: {worker_service_proto:?}");
    println!("Output dir: {out_dir:?}");
    println!("gRPC Output dir: {grpc_out_dir:?}");

    // 1. Generate generic messages to out_dir
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .out_dir(&out_dir)
        .extern_path(".worker.FlightData", "::arrow_flight::FlightData")
        .extern_path(
            ".worker.FlightDescriptor",
            "::arrow_flight::FlightDescriptor",
        )
        .compile_protos(&[messages_proto.clone()], &[protocol_dir.clone()])?;

    // 2. Generate gRPC service to grpc_out_dir
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&grpc_out_dir)
        .extern_path(".worker.FlightData", "::arrow_flight::FlightData")
        .extern_path(
            ".worker.FlightDescriptor",
            "::arrow_flight::FlightDescriptor",
        )
        .extern_path(".worker", "crate::protocol::generated::worker")
        .compile_protos(&[worker_service_proto], &[protocol_dir])?;

    println!("Successfully generated worker proto code");

    Ok(())
}
