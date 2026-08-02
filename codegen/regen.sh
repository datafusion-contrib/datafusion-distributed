#!/usr/bin/env bash

set -e

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root" && cargo run --manifest-path src/protocol/grpc/gen/Cargo.toml
