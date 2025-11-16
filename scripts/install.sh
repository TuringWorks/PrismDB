#!/bin/bash
# Installation script for PrismDB Python bindings (macOS/Linux)

set -e

echo "==================================================="
echo "PrismDB Python Package Installer"
echo "==================================================="
echo ""

# Detect OS
OS="$(uname -s)"
case "${OS}" in
    Linux*)     PLATFORM=Linux;;
    Darwin*)    PLATFORM=macOS;;
    *)          PLATFORM="UNKNOWN:${OS}"
esac

echo "Platform: $PLATFORM"
echo ""

# Check prerequisites
echo "Checking prerequisites..."
echo ""

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.8 or later."
    exit 1
fi

PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
echo "✓ Python: $PYTHON_VERSION"

# Check pip
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 not found. Installing..."
    python3 -m ensurepip --upgrade
fi
echo "✓ pip3 found"

# Check Rust
if ! command -v cargo &> /dev/null; then
    echo "❌ Rust not found. Installing..."
    if [ "$PLATFORM" = "macOS" ]; then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    elif [ "$PLATFORM" = "Linux" ]; then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    fi
    source $HOME/.cargo/env
fi
echo "✓ Rust: $(rustc --version)"

# Check build tools
if [ "$PLATFORM" = "Linux" ]; then
    if ! command -v gcc &> /dev/null; then
        echo "⚠️  gcc not found. Installing build essentials..."
        if command -v apt-get &> /dev/null; then
            sudo apt-get update
            sudo apt-get install -y build-essential python3-dev
        elif command -v yum &> /dev/null; then
            sudo yum groupinstall -y 'Development Tools'
            sudo yum install -y python3-devel
        fi
    fi
    echo "✓ Build tools found"
fi

# Install maturin
echo ""
echo "Installing maturin..."
pip3 install --upgrade maturin

# Build and install
echo ""
echo "Building PrismDB..."
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

# Use development install for quick iteration, or build wheel for production
if [ "$1" = "--dev" ]; then
    echo "Installing in development mode..."
    maturin develop --features python --release
else
    echo "Building release wheel..."
    maturin build --release --features python

    # Find the wheel file
    WHEEL=$(ls -t target/wheels/prismdb-*.whl 2>/dev/null | head -1)

    if [ -z "$WHEEL" ]; then
        echo "❌ Wheel file not found!"
        exit 1
    fi

    echo "Installing $WHEEL..."
    pip3 install --force-reinstall "$WHEEL"
fi

# Verify installation
echo ""
echo "Verifying installation..."
python3 -c "
import prismdb
db = prismdb.connect()
db.execute('CREATE TABLE test (id INTEGER)')
db.execute('INSERT INTO test VALUES (42)')
result = db.execute('SELECT * FROM test')
rows = list(result)
assert rows == [[42]], f'Test failed: {rows}'
print('✓ Installation verified successfully!')
"

echo ""
echo "==================================================="
echo "Installation Complete!"
echo "==================================================="
echo ""
echo "PrismDB Python bindings installed successfully."
echo ""
echo "Quick start:"
echo "  python3 -c 'import prismdb; print(prismdb.__version__)'"
echo ""
echo "See examples in: python_examples/"
echo "Documentation: README_PYTHON.md"
echo ""
