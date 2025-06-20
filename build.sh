#!/bin/bash
set -e

echo "Building DataFusion Distributed..."

# Check if release flag is passed
if [ "$1" = "--release" ] || [ "$1" = "-r" ]; then
    echo "Building in release mode..."
    cargo build --release
    echo "✅ Build completed successfully!"
    echo "Binary available at: ./target/release/datafusion-distributed"
else
    echo "Building in debug mode..."
    cargo build
    echo "✅ Build completed successfully!"
    echo "Binary available at: ./target/debug/datafusion-distributed"
fi

echo ""
echo "Usage:"
echo "  ./target/debug/datafusion-distributed --help    # View help"
echo "  ./target/debug/datafusion-distributed --mode proxy --port 20200    # Start proxy"
echo "  ./target/debug/datafusion-distributed --mode worker --port 20201   # Start worker" 