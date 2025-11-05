#!/bin/bash

# EloqStore Unit Test Coverage Analysis Script
# Used to analyze test coverage of actual project code

set -e

echo "=== EloqStore Unit Test Coverage Analysis ==="
echo

# Check necessary tools
echo "1. Checking necessary tools..."
if ! command -v lcov &> /dev/null; then
    echo "❌ lcov is not installed, please run: sudo apt-get install lcov"
    exit 1
fi

if ! command -v genhtml &> /dev/null; then
    echo "❌ genhtml is not installed, please run: sudo apt-get install lcov"
    exit 1
fi

echo "✓ lcov and genhtml are installed"
echo
echo "✓ lcov and genhtml are installed"
echo

echo "2. Checking GCC and GCOV version compatibility..."
if command -v gcc &> /dev/null && command -v gcov &> /dev/null; then
    GCC_VERSION=$(gcc --version | head -n1 | awk '{print $NF}')
    GCOV_VERSION=$(gcov --version | head -n1 | awk '{print $NF}')
    
    echo "   GCC version: $GCC_VERSION"
    echo "   GCOV version: $GCOV_VERSION"
    
    if [ "$GCC_VERSION" != "$GCOV_VERSION" ]; then
        echo "⚠️  Warning: GCC version ($GCC_VERSION) and GCOV version ($GCOV_VERSION) differ"
        echo "   This may cause coverage data compatibility issues"
        echo "   Consider using matching versions or ignore with --ignore-errors version"
    else
        echo "✓ GCC and GCOV versions match"
    fi
else
    echo "⚠️  Cannot check GCC/GCOV versions: one or both tools not found"
fi
echo



# Set directories - using relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/build_coverage"
COVERAGE_DIR="$BUILD_DIR/unit_test_coverage_report"

cd "$PROJECT_ROOT"

echo "2. Configuring project with coverage enabled..."
cmake -B "$BUILD_DIR" -DWITH_COVERAGE=ON -DWITH_UNIT_TESTS=ON
echo "✓ Project configuration completed"
echo

echo "3. Building project and unit tests..."
cmake --build "$BUILD_DIR" --parallel $(nproc)
echo "✓ Build completed"
echo

echo "4. Cleaning previous coverage data..."
find "$BUILD_DIR" -name "*.gcda" -delete 2>/dev/null || true
rm -f "$BUILD_DIR/unit_coverage.info" "$BUILD_DIR/unit_coverage_filtered.info"
rm -rf "$COVERAGE_DIR"
echo "✓ Cleanup completed"
echo

echo "5. Running unit tests..."
cd "$BUILD_DIR"

# First try to run all tests in the tests directory using ctest
echo "Running unit tests in tests directory using ctest..."
if [ ! -d "tests" ]; then
    echo "❌ Tests directory not found"
    exit 1
fi

echo "Found tests directory, running all CTest tests..."
# Set timeout and run all tests
ctest --test-dir tests/ || echo "⚠️  Some tests may have failed or timed out, continuing execution..."

echo "✓ Unit test execution completed"
echo

echo "6. Generating coverage data..."
lcov --capture --directory . --output-file unit_coverage.info --ignore-errors negative,gcov
echo "✓ Coverage data collection completed"
echo

echo "7. Filtering coverage data (excluding system files and third-party libraries)..."
lcov --remove unit_coverage.info \
    '/usr/*' \
    '*/abseil/*' \
    '*/concurrentqueue/*' \
    '*/inih/*' \
    '*/external/*' \
    '*/tests/*' \
    '*/benchmark/*' \
    '*/db_stress/*' \
    '*/examples/*' \
    --output-file unit_coverage_filtered.info \
    --ignore-errors unused

echo "✓ Data filtering completed"
echo

echo "8. Generating HTML coverage report..."
mkdir -p "$COVERAGE_DIR"
genhtml unit_coverage_filtered.info --output-directory "$COVERAGE_DIR"
echo "✓ HTML report generation completed"
echo

echo "9. Displaying coverage summary..."
lcov --summary unit_coverage_filtered.info
echo

echo "=== Unit Test Coverage Analysis Completed ==="
echo
echo "Report location: $COVERAGE_DIR/index.html"
echo
echo "How to view the report:"
echo "1. Open in browser: file://$COVERAGE_DIR/index.html"
echo "2. Or start HTTP server:"
echo "   cd $COVERAGE_DIR && python3 -m http.server 8081"
echo "   Then visit: http://localhost:8081"