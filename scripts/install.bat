@echo off
REM Installation script for PrismDB Python bindings (Windows)

echo ===================================================
echo PrismDB Python Package Installer
echo ===================================================
echo.

echo Platform: Windows
echo.

echo Checking prerequisites...
echo.

REM Check Python
where python >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo X Python not found. Please install Python 3.8 or later from python.org
    exit /b 1
)
for /f "tokens=2" %%i in ('python --version') do echo [OK] Python: %%i

REM Check pip
where pip >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo X pip not found. Installing...
    python -m ensurepip --upgrade
)
echo [OK] pip found

REM Check Rust
where cargo >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo X Rust not found. Please install from https://rustup.rs/
    echo   Download and run: https://win.rustup.rs/x86_64
    exit /b 1
)
for /f "tokens=2" %%i in ('rustc --version') do echo [OK] Rust: %%i

REM Check Visual Studio Build Tools
where cl >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo WARNING: Visual Studio Build Tools not found in PATH
    echo Please ensure you have Visual Studio 2019 or later with C++ tools installed
    echo.
)

REM Install maturin
echo.
echo Installing maturin...
pip install --upgrade maturin

REM Build and install
echo.
echo Building PrismDB...
set PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

if "%1"=="--dev" (
    echo Installing in development mode...
    maturin develop --features python --release
) else (
    echo Building release wheel...
    maturin build --release --features python

    REM Find the wheel file
    for /f "delims=" %%i in ('dir /b /o-d target\wheels\prismdb-*.whl 2^>nul') do set WHEEL=%%i

    if not defined WHEEL (
        echo X Wheel file not found!
        exit /b 1
    )

    echo Installing target\wheels\%WHEEL%...
    pip install --force-reinstall target\wheels\%WHEEL%
)

REM Verify installation
echo.
echo Verifying installation...
python -c "import prismdb; db = prismdb.connect(); db.execute('CREATE TABLE test (id INTEGER)'); db.execute('INSERT INTO test VALUES (42)'); result = db.execute('SELECT * FROM test'); rows = list(result); assert rows == [[42]], f'Test failed: {rows}'; print('✓ Installation verified successfully!')"

echo.
echo ===================================================
echo Installation Complete!
echo ===================================================
echo.
echo PrismDB Python bindings installed successfully.
echo.
echo Quick start:
echo   python -c "import prismdb; print(prismdb.__version__)"
echo.
echo See examples in: python_examples\
echo Documentation: README_PYTHON.md
echo.
