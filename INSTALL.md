# PrismDB Installation Guide

Complete guide for installing PrismDB Python bindings on any platform.

## Quick Install (Recommended)

### From Source (All Platforms)

```bash
# Clone the repository
git clone https://github.com/TuringWorks/PrismDB.git
cd PrismDB

# Install dependencies
pip install maturin

# Build and install
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin develop --features python --release
```

## Platform-Specific Instructions

### macOS

**Prerequisites:**
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Install Python 3.8+
brew install python@3.11

# Install maturin
pip3 install maturin
```

**Build and Install:**
```bash
cd PrismDB
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
maturin build --release --features python
pip3 install target/wheels/prismdb-*.whl
```

### Linux (Ubuntu/Debian)

**Prerequisites:**
```bash
# Install build dependencies
sudo apt-get update
sudo apt-get install -y build-essential curl python3 python3-pip python3-dev

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Install maturin
pip3 install maturin
```

**Build and Install:**
```bash
cd PrismDB
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
maturin build --release --features python
pip3 install target/wheels/prismdb-*.whl
```

### Linux (CentOS/RHEL/Fedora)

**Prerequisites:**
```bash
# Install build dependencies
sudo yum groupinstall 'Development Tools'
sudo yum install python3 python3-pip python3-devel

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env

# Install maturin
pip3 install maturin
```

**Build and Install:**
```bash
cd PrismDB
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
maturin build --release --features python
pip3 install target/wheels/prismdb-*.whl
```

### Windows

**Prerequisites:**
1. Install Visual Studio 2019 or later with C++ build tools
2. Install Python 3.8+ from [python.org](https://www.python.org/downloads/)
3. Install Rust from [rustup.rs](https://rustup.rs/)

**Build and Install (PowerShell):**
```powershell
cd PrismDB
$env:PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
pip install maturin
maturin build --release --features python
pip install (Get-Item target\wheels\prismdb-*.whl)
```

**Build and Install (Command Prompt):**
```cmd
cd PrismDB
set PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
pip install maturin
maturin build --release --features python
pip install target\wheels\prismdb-*.whl
```

## Build Modes

### Development Build (Fast, Unoptimized)

```bash
maturin develop --features python
```

- Faster compilation
- Includes debug symbols
- Suitable for development and testing
- Lower runtime performance

### Release Build (Optimized)

```bash
maturin build --release --features python
```

- Full optimizations enabled
- LTO (Link-Time Optimization)
- Strip symbols
- Best runtime performance
- Longer compilation time

## Installation from Pre-built Wheel

If you have a pre-built wheel file:

```bash
pip install prismdb-0.1.0-*.whl
```

## Installation in Virtual Environment (Recommended)

```bash
# Create virtual environment
python3 -m venv prismdb-env

# Activate (macOS/Linux)
source prismdb-env/bin/activate

# Activate (Windows)
prismdb-env\Scripts\activate

# Install
pip install maturin
cd PrismDB
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin develop --features python --release
```

## Verify Installation

```python
import prismdb

# Create database
db = prismdb.connect()

# Run test query
db.execute("CREATE TABLE test (id INTEGER, name VARCHAR)")
db.execute("INSERT INTO test VALUES (1, 'PrismDB')")

result = db.execute("SELECT * FROM test")
print(list(result))  # [[1, 'PrismDB']]

print("✓ PrismDB installed successfully!")
```

## Troubleshooting

### Python Version Mismatch

**Error:** `the configured Python interpreter version (3.13) is newer than PyO3's maximum supported version (3.12)`

**Solution:** Set the forward compatibility flag:
```bash
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
```

### Rust Not Found

**Error:** `cargo: command not found`

**Solution:** Install Rust and add to PATH:
```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source $HOME/.cargo/env
```

### Missing C/C++ Compiler

**Linux:**
```bash
sudo apt-get install build-essential
```

**macOS:**
```bash
xcode-select --install
```

**Windows:**
- Install Visual Studio Build Tools 2019 or later

### maturin Not Found

```bash
pip install --upgrade maturin
```

### Permission Denied

Use `--user` flag or virtual environment:
```bash
pip install --user maturin
# or
python3 -m venv venv && source venv/bin/activate
```

### Linker Errors on macOS

Make sure Xcode Command Line Tools are installed:
```bash
xcode-select --install
```

### OpenSSL Issues on Linux

```bash
sudo apt-get install pkg-config libssl-dev
```

## Building from Source Distribution

```bash
# Download source distribution
wget https://github.com/TuringWorks/PrismDB/archive/refs/heads/master.zip
unzip master.zip
cd PrismDB-master

# Build and install
pip install maturin
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release --features python
pip install target/wheels/prismdb-*.whl
```

## Uninstallation

```bash
pip uninstall prismdb
```

## System Requirements

### Minimum Requirements
- Python 3.8 or later
- Rust 1.70 or later
- 2 GB RAM
- 1 GB disk space

### Recommended Requirements
- Python 3.11 or later
- Rust 1.75 or later
- 4 GB RAM
- 2 GB disk space

## Supported Platforms

- ✅ macOS 10.15+ (x86_64, ARM64)
- ✅ Linux (x86_64, ARM64)
  - Ubuntu 20.04+
  - Debian 10+
  - CentOS 8+
  - Fedora 35+
- ✅ Windows 10+ (x86_64)
  - Requires Visual Studio 2019+ Build Tools

## Building for Distribution

### Build Wheels for Current Platform

```bash
# Build optimized wheel
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release --features python

# Wheel is created in target/wheels/
```

### Build Source Distribution

```bash
maturin sdist
# Creates source tarball in target/wheels/
```

### Build Multiple Python Versions

```bash
# Build for specific Python version
maturin build --release --features python --interpreter python3.9
maturin build --release --features python --interpreter python3.10
maturin build --release --features python --interpreter python3.11
```

## Advanced Options

### Custom Installation Path

```bash
pip install --prefix=/custom/path prismdb-*.whl
```

### Development Mode with Editable Install

```bash
maturin develop --features python
# Code changes require rebuild
```

### Building with Different Rust Toolchain

```bash
rustup default stable
maturin build --release --features python

# Or use nightly
rustup default nightly
maturin build --release --features python
```

## Performance Tuning

The release build includes:
- LTO (Link-Time Optimization)
- Optimization level 3
- Symbol stripping
- Single codegen unit

For maximum performance, ensure you're using the `--release` flag:
```bash
maturin build --release --features python
```

## Getting Help

- GitHub Issues: https://github.com/TuringWorks/PrismDB/issues
- Documentation: https://github.com/TuringWorks/PrismDB/blob/master/README_PYTHON.md
- Examples: https://github.com/TuringWorks/PrismDB/tree/master/python_examples

## License

PrismDB is licensed under the MIT License.
