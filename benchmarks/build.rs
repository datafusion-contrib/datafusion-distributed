fn main() {
    println!("cargo::rerun-if-env-changed=FORCE_REBUILD");
    built::write_built_file().expect("Failed to acquire build-time information");
}
