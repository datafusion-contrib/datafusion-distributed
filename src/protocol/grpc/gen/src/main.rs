use std::env;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = env::current_dir()?;

    let proto_dir = repo_root.join("src/protocol/grpc");
    let proto_file = proto_dir.join("worker.proto");
    let out_dir = repo_root.join("src/protocol/generated");

    fs::create_dir_all(&out_dir)?;

    println!("Generating protobuf code...");
    println!("Proto dir: {proto_dir:?}");
    println!("Proto file: {proto_file:?}");
    println!("Output dir: {out_dir:?}");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // The generated messages build with `grpc` off; only the tonic client and server carry
        // the feature gate. Emitted here so a regeneration cannot drop the gates.
        .client_mod_attribute(".", "#[cfg(feature = \"grpc\")]")
        .server_mod_attribute(".", "#[cfg(feature = \"grpc\")]")
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
