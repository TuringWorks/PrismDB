@echo off
REM Build script for PrismDB Python bindings (Windows)

echo ===================================================
echo PrismDB Python Package Build Script
echo ===================================================
echo.

REM Check if maturin is installed
where maturin >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo X maturin not found. Installing...
    pip install maturin
) else (
    echo [OK] maturin found
)

REM Check if Rust is installed
where cargo >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo X Rust not found. Please install from https://rustup.rs/
    exit /b 1
) else (
    for /f "tokens=2" %%i in ('rustc --version') do echo [OK] Rust version: %%i
)

REM Check Python version
for /f "tokens=2" %%i in ('python --version') do echo [OK] Python version: %%i

REM Set environment variable for Python 3.13+ compatibility
set PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

echo.
echo Building PrismDB Python package...
echo.

REM Build the package
maturin build --release --features python

echo.
echo ===================================================
echo Build Complete!
echo ===================================================
echo.
echo Wheel files created in: target\wheels\
dir target\wheels\*.whl 2>nul
echo.
echo To install, run:
echo   pip install target\wheels\prismdb-*.whl
echo.
