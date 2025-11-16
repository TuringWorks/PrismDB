#!/bin/bash
# Package PrismDB for distribution on all platforms

set -e

echo "==================================================="
echo "PrismDB Distribution Package Creator"
echo "==================================================="
echo ""

# Set environment for Python 3.13+ compatibility
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

# Create distribution directory
DIST_DIR="dist"
rm -rf "$DIST_DIR"
mkdir -p "$DIST_DIR"

echo "Building distribution packages..."
echo ""

# Build source distribution
echo "1. Building source distribution..."
maturin sdist
cp target/wheels/prismdb-*.tar.gz "$DIST_DIR/" 2>/dev/null || true
echo "   ✓ Source distribution created"

# Build wheel for current platform
echo "2. Building wheel for current platform..."
maturin build --release --features python
cp target/wheels/prismdb-*.whl "$DIST_DIR/" 2>/dev/null || true
echo "   ✓ Wheel created"

# Copy documentation
echo "3. Copying documentation..."
cp README.md "$DIST_DIR/"
cp README_PYTHON.md "$DIST_DIR/"
cp INSTALL.md "$DIST_DIR/"
cp QUICKSTART.md "$DIST_DIR/"
cp DISTRIBUTION.md "$DIST_DIR/"
cp LICENSE "$DIST_DIR/"
echo "   ✓ Documentation copied"

# Copy examples
echo "4. Copying examples..."
cp -r python_examples "$DIST_DIR/"
echo "   ✓ Examples copied"

# Copy scripts
echo "5. Copying build scripts..."
cp -r scripts "$DIST_DIR/"
echo "   ✓ Scripts copied"

# Create installation instructions
cat > "$DIST_DIR/README.txt" << 'EOF'
PrismDB Python Bindings - Distribution Package
==============================================

QUICK INSTALL:

  1. From wheel (if available for your platform):
     pip install prismdb-*.whl

  2. From source:
     pip install prismdb-*.tar.gz

  3. Using automated installer:
     # macOS/Linux
     bash scripts/install.sh

     # Windows
     scripts\install.bat

DOCUMENTATION:

  - QUICKSTART.md - Get started in 5 minutes
  - INSTALL.md - Detailed installation guide
  - README_PYTHON.md - Complete Python API reference
  - DISTRIBUTION.md - Distribution file information

EXAMPLES:

  - python_examples/basic_usage.py - Comprehensive examples
  - python_examples/test_basic.py - Test suite

SUPPORT:

  - GitHub: https://github.com/TuringWorks/PrismDB
  - Issues: https://github.com/TuringWorks/PrismDB/issues

LICENSE: MIT (see LICENSE file)
EOF

echo "   ✓ Installation instructions created"

# Create checksum file
echo "6. Creating checksums..."
cd "$DIST_DIR"
shasum -a 256 prismdb-* > CHECKSUMS.txt 2>/dev/null || true
cd ..
echo "   ✓ Checksums created"

# Summary
echo ""
echo "==================================================="
echo "Package Created Successfully!"
echo "==================================================="
echo ""
echo "Distribution directory: $DIST_DIR/"
echo ""
echo "Contents:"
ls -lh "$DIST_DIR/"
echo ""
echo "To distribute, compress the directory:"
echo "  tar -czf prismdb-distribution.tar.gz $DIST_DIR/"
echo "  zip -r prismdb-distribution.zip $DIST_DIR/"
echo ""
