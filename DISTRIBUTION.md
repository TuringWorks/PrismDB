# PrismDB Distribution Package

This directory contains distribution files for PrismDB Python bindings.

## Available Files

### Binary Wheels (Platform-Specific)

**macOS ARM64:**
- `prismdb-0.1.0-cp38-cp38-macosx_11_0_arm64.whl` (Python 3.8)
- `prismdb-0.1.0-cp39-cp39-macosx_11_0_arm64.whl` (Python 3.9)
- `prismdb-0.1.0-cp310-cp310-macosx_11_0_arm64.whl` (Python 3.10)
- `prismdb-0.1.0-cp311-cp311-macosx_11_0_arm64.whl` (Python 3.11)
- `prismdb-0.1.0-cp312-cp312-macosx_11_0_arm64.whl` (Python 3.12)
- `prismdb-0.1.0-cp313-cp313-macosx_11_0_arm64.whl` (Python 3.13)

**macOS x86_64:**
- `prismdb-0.1.0-cp3*-cp3*-macosx_10_12_x86_64.whl`

**Linux x86_64:**
- `prismdb-0.1.0-cp3*-cp3*-manylinux_2_17_x86_64.manylinux2014_x86_64.whl`

**Linux ARM64:**
- `prismdb-0.1.0-cp3*-cp3*-manylinux_2_17_aarch64.manylinux2014_aarch64.whl`

**Windows x86_64:**
- `prismdb-0.1.0-cp3*-cp3*-win_amd64.whl`

### Source Distribution (Platform-Independent)

- `prismdb-0.1.0.tar.gz` - Source tarball for building on any platform

## Installation

### From Wheel (Fast, No Build Required)

Choose the wheel matching your platform and Python version:

```bash
# macOS ARM64, Python 3.13
pip install prismdb-0.1.0-cp313-cp313-macosx_11_0_arm64.whl

# Linux x86_64, Python 3.11
pip install prismdb-0.1.0-cp311-cp311-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

# Windows x86_64, Python 3.12
pip install prismdb-0.1.0-cp312-cp312-win_amd64.whl
```

### From Source (Universal, Requires Rust)

**Prerequisites:**
- Python 3.8 or later
- Rust 1.70 or later
- C/C++ compiler

**Install:**
```bash
pip install prismdb-0.1.0.tar.gz
```

Or extract and build manually:
```bash
tar -xzf prismdb-0.1.0.tar.gz
cd prismdb-0.1.0
pip install maturin
PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 maturin build --release
pip install target/wheels/prismdb-*.whl
```

## Verification

After installation, verify it works:

```bash
python3 -c "import prismdb; db = prismdb.connect(); print('✓ PrismDB installed successfully!')"
```

## Platform Requirements

### macOS
- macOS 10.15 (Catalina) or later
- Python 3.8 - 3.13
- For ARM64 wheels: Apple Silicon Mac (M1/M2/M3)
- For x86_64 wheels: Intel Mac

### Linux
- glibc 2.17 or later (most distributions from ~2014+)
- Python 3.8 - 3.13
- For ARM64 wheels: 64-bit ARM processor
- For x86_64 wheels: 64-bit Intel/AMD processor

**Tested on:**
- Ubuntu 20.04, 22.04, 24.04
- Debian 10, 11, 12
- CentOS 8, 9
- Fedora 35+
- RHEL 8, 9
- Amazon Linux 2

### Windows
- Windows 10 or later
- Python 3.8 - 3.13
- 64-bit Windows (x86_64)
- Visual C++ Redistributable 2015-2022 (usually already installed)

## File Sizes

| File Type | Approximate Size |
|-----------|-----------------|
| Wheel (binary) | 4-6 MB |
| Source tarball | 300-400 KB |

## What's Included

**Binary Wheels contain:**
- Compiled PrismDB library
- Python bindings
- Metadata and dependencies

**Source distribution contains:**
- Full Rust source code
- Python binding source
- Build configuration (Cargo.toml, pyproject.toml)
- Documentation
- Examples and tests
- Build scripts

## Building from Source

See [INSTALL.md](INSTALL.md) for detailed build instructions for each platform.

## Quick Start

See [QUICKSTART.md](QUICKSTART.md) for usage examples.

## Documentation

- **Python API:** [README_PYTHON.md](README_PYTHON.md)
- **Installation:** [INSTALL.md](INSTALL.md)
- **Quick Start:** [QUICKSTART.md](QUICKSTART.md)
- **Main Documentation:** [README.md](README.md)

## Examples

Located in `python_examples/` directory:
- `basic_usage.py` - Comprehensive usage examples
- `test_basic.py` - Test suite demonstrating all features

## Support

- **GitHub Issues:** https://github.com/TuringWorks/PrismDB/issues
- **Documentation:** https://github.com/TuringWorks/PrismDB

## License

MIT License - see [LICENSE](LICENSE) file

## Version Information

- **Version:** 0.1.0
- **Release Date:** 2025-11-16
- **Python Support:** 3.8, 3.9, 3.10, 3.11, 3.12, 3.13
- **Platforms:** macOS (ARM64, x86_64), Linux (x86_64, ARM64), Windows (x86_64)

## Changelog

### 0.1.0 (2025-11-16)

**Initial Release**

- DB-API 2.0 compatible interface
- Full SQL support (SELECT, INSERT, UPDATE, DELETE, JOINs, CTEs, Window Functions)
- In-memory and file-based databases
- Columnar storage with vectorized execution
- Context manager and iterator protocol support
- Comprehensive type system
- String, math, date, and aggregate functions
- Dictionary conversion for easy data manipulation
- Complete documentation and examples
