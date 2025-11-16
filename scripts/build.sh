#!/bin/bash
# Build script for PrismDB Python bindings (macOS/Linux)

set -e

echo "==================================================="
echo "PrismDB Python Package Build Script"
echo "==================================================="
echo ""

# Check if maturin is installed
if ! command -v maturin &> /dev/null; then
    echo "❌ maturin not found. Installing..."
    pip3 install maturin
else
    echo "✓ maturin found"
fi

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo "❌ Rust not found. Please install from https://rustup.rs/"
    exit 1
else
    echo "✓ Rust found: $(rustc --version)"
fi

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python version: $PYTHON_VERSION"

# Set environment variable for Python 3.13+ compatibility
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

echo ""
echo "Building PrismDB Python package..."
echo ""

# Build the package
maturin build --release --features python

echo ""
echo "==================================================="
echo "Build Complete!"
echo "==================================================="
echo ""
echo "Wheel files created in: target/wheels/"
ls -lh target/wheels/*.whl 2>/dev/null || echo "No wheel files found"
echo ""
echo "To install, run:"
echo "  pip3 install target/wheels/prismdb-*.whl"
echo ""
