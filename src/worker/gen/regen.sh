#!/usr/bin/env bash

set -e

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root" && cargo run --manifest-path src/worker/gen/Cargo.toml
