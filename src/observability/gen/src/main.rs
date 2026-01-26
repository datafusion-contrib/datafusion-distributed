use std::env;
use std::fs;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = env::current_dir()?;

    let proto_dir = repo_root.join("src/observability/proto");
    let proto_file = proto_dir.join("observability.proto");
    let out_dir = repo_root.join("src/observability/generated");

    fs::create_dir_all(&out_dir)?;

    println!("Generating protobuf code...");
    println!("Proto dir: {proto_dir:?}");
    println!("Proto file: {proto_file:?}");
    println!("Output dir: {out_dir:?}");

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(&out_dir)
        .compile_protos(&[proto_file], &[proto_dir])?;

    println!("Successfully generated observability proto code");

    Ok(())
}
