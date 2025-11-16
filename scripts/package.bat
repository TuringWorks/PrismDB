@echo off
REM Package PrismDB for distribution on all platforms

echo ===================================================
echo PrismDB Distribution Package Creator
echo ===================================================
echo.

REM Set environment for Python 3.13+ compatibility
set PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1

REM Create distribution directory
set DIST_DIR=dist
if exist %DIST_DIR% rmdir /s /q %DIST_DIR%
mkdir %DIST_DIR%

echo Building distribution packages...
echo.

REM Build source distribution
echo 1. Building source distribution...
maturin sdist
copy target\wheels\prismdb-*.tar.gz %DIST_DIR%\ >nul 2>&1
echo    [OK] Source distribution created

REM Build wheel for current platform
echo 2. Building wheel for current platform...
maturin build --release --features python
copy target\wheels\prismdb-*.whl %DIST_DIR%\ >nul 2>&1
echo    [OK] Wheel created

REM Copy documentation
echo 3. Copying documentation...
copy README.md %DIST_DIR%\ >nul
copy README_PYTHON.md %DIST_DIR%\ >nul
copy INSTALL.md %DIST_DIR%\ >nul
copy QUICKSTART.md %DIST_DIR%\ >nul
copy DISTRIBUTION.md %DIST_DIR%\ >nul
copy LICENSE %DIST_DIR%\ >nul
echo    [OK] Documentation copied

REM Copy examples
echo 4. Copying examples...
xcopy /s /i /q python_examples %DIST_DIR%\python_examples >nul
echo    [OK] Examples copied

REM Copy scripts
echo 5. Copying build scripts...
xcopy /s /i /q scripts %DIST_DIR%\scripts >nul
echo    [OK] Scripts copied

REM Create installation instructions
echo 6. Creating installation instructions...
(
echo PrismDB Python Bindings - Distribution Package
echo ==============================================
echo.
echo QUICK INSTALL:
echo.
echo   1. From wheel ^(if available for your platform^):
echo      pip install prismdb-*.whl
echo.
echo   2. From source:
echo      pip install prismdb-*.tar.gz
echo.
echo   3. Using automated installer:
echo      scripts\install.bat
echo.
echo DOCUMENTATION:
echo.
echo   - QUICKSTART.md - Get started in 5 minutes
echo   - INSTALL.md - Detailed installation guide
echo   - README_PYTHON.md - Complete Python API reference
echo   - DISTRIBUTION.md - Distribution file information
echo.
echo EXAMPLES:
echo.
echo   - python_examples\basic_usage.py - Comprehensive examples
echo   - python_examples\test_basic.py - Test suite
echo.
echo SUPPORT:
echo.
echo   - GitHub: https://github.com/TuringWorks/PrismDB
echo   - Issues: https://github.com/TuringWorks/PrismDB/issues
echo.
echo LICENSE: MIT ^(see LICENSE file^)
) > %DIST_DIR%\README.txt
echo    [OK] Installation instructions created

echo.
echo ===================================================
echo Package Created Successfully!
echo ===================================================
echo.
echo Distribution directory: %DIST_DIR%\
echo.
echo Contents:
dir /b %DIST_DIR%
echo.
echo To distribute, compress the directory using:
echo   - 7-Zip: Right-click ^> 7-Zip ^> Add to archive
echo   - WinRAR: Right-click ^> Add to archive
echo.
