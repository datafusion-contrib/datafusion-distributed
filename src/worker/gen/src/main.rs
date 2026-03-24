use std::env;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = env::current_dir()?;

    let proto_dir = repo_root.join("src/worker");
    let proto_file = proto_dir.join("worker.proto");
    let out_dir = repo_root.join("src/worker/generated");

    fs::create_dir_all(&out_dir)?;

    println!("Generating protobuf code...");
    println!("Proto dir: {proto_dir:?}");
    println!("Proto file: {proto_file:?}");
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
        .compile_protos(&[proto_file], &[proto_dir])?;

    println!("Successfully generated worker proto code");

    Ok(())
}
