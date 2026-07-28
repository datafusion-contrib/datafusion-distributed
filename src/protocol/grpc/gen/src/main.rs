use std::env;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = env::current_dir()?;

    let protocol_dir = repo_root.join("src/protocol");
    let messages_proto = protocol_dir.join("messages.proto");
    let worker_service_proto = protocol_dir.join("grpc/worker_service.proto");
    let out_dir = repo_root.join("src/protocol/generated");

    fs::create_dir_all(&out_dir)?;

    println!("Generating protobuf code...");
    println!("Protocol dir: {protocol_dir:?}");
    println!("Messages proto: {messages_proto:?}");
    println!("Worker service proto: {worker_service_proto:?}");
    println!("Output dir: {out_dir:?}");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out_dir)
        .extern_path(".worker.FlightData", "::arrow_flight::FlightData")
        .extern_path(
            ".worker.FlightDescriptor",
            "::arrow_flight::FlightDescriptor",
        )
        .compile_protos(&[messages_proto, worker_service_proto], &[protocol_dir])?;

    println!("Successfully generated worker proto code");

    Ok(())
}
