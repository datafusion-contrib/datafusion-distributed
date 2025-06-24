#!/bin/bash
set -e

echo "🧹 Cleaning old build artifacts..."

# Remove old target directory containing the old binary name
if [ -d "target" ]; then
    echo "  Removing target/ directory..."
    rm -rf target/
    echo "  ✅ Old target/ directory removed"
else
    echo "  No target/ directory found"
fi

echo ""
echo "🔨 Building project with new binary name 'distributed-datafusion'..."

# Build the project (this will create a new target directory)
if [ "$1" = "--release" ] || [ "$1" = "-r" ]; then
    echo "Building in release mode..."
    cargo build --release
    echo "✅ Build completed successfully!"
    echo "New binary available at: ./target/release/distributed-datafusion"
else
    echo "Building in debug mode..."
    cargo build
    echo "✅ Build completed successfully!"
    echo "New binary available at: ./target/debug/distributed-datafusion"
fi

echo ""
echo "🎉 Cleanup and rebuild complete!"
echo ""
echo "Usage:"
echo "  ./target/debug/distributed-datafusion --help    # View help"
echo "  ./target/debug/distributed-datafusion --mode proxy --port 20200    # Start proxy"
echo "  ./target/debug/distributed-datafusion --mode worker --port 20201   # Start worker" 