use std::env;
use std::fs;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let repo_root = env::current_dir()?;

    generate_worker(&repo_root)?;
    generate_observability(&repo_root)?;
    generate_iceberg(&repo_root)?;

    println!("Successfully generated protobuf code");

    Ok(())
}

fn generate_worker(repo_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = repo_root.join("src/protocol/grpc");
    let proto_file = proto_dir.join("worker.proto");
    let out_dir = proto_dir.join("generated");

    fs::create_dir_all(&out_dir)?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(out_dir)
        .extern_path(".worker.FlightData", "::arrow_flight::FlightData")
        .extern_path(
            ".worker.FlightDescriptor",
            "::arrow_flight::FlightDescriptor",
        )
        .compile_protos(&[proto_file], &[proto_dir])?;

    Ok(())
}

fn generate_iceberg(repo_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = repo_root.join("iceberg/src/proto");
    let proto_file = proto_dir.join("iceberg.proto");
    let out_dir = proto_dir.join("generated");

    fs::create_dir_all(&out_dir)?;

    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .out_dir(out_dir)
        .extern_path(
            ".iceberg.DataFusionSchema",
            "::datafusion_proto::protobuf::Schema",
        )
        .extern_path(
            ".iceberg.DataFusionPartitioning",
            "::datafusion_proto::protobuf::Partitioning",
        )
        .compile_protos(&[proto_file], &[proto_dir])?;

    Ok(())
}

fn generate_observability(repo_root: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = repo_root.join("src/protocol/grpc/observability/proto");
    let proto_file = proto_dir.join("observability.proto");
    let out_dir = repo_root.join("src/protocol/grpc/observability/generated");

    fs::create_dir_all(&out_dir)?;

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(out_dir)
        .extern_path(
            ".observability.TaskKey",
            "crate::protocol::grpc::generated::worker::TaskKey",
        )
        .compile_protos(&[proto_file], &[proto_dir])?;

    Ok(())
}
