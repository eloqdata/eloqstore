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

# Set directories - using relative paths
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_ROOT/build"
COVERAGE_DIR="$BUILD_DIR/unit_test_coverage_report"

cd "$PROJECT_ROOT"

echo "2. Configuring project for Coverage build mode..."
cmake -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Coverage -DWITH_UNIT_TESTS=ON
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
if [ -d "tests" ]; then
    echo "Found tests directory, running all CTest tests..."
    # Set timeout and run all tests
    timeout 300s ctest --test-dir tests/ || echo "⚠️  Some tests may have failed or timed out, continuing execution..."
else
    echo "Tests directory not found, trying to find other test executables..."
    
    # Fallback: find all test executables
    TEST_EXECUTABLES=$(find . -name "*test*" -type f -executable | head -20)
    
    if [ -z "$TEST_EXECUTABLES" ]; then
        echo "❌ No test executables found"
        echo "Available executables:"
        find . -type f -executable | head -10
        exit 1
    fi
    
    echo "Found test files:"
    echo "$TEST_EXECUTABLES"
    echo
    
    # Run each test
    for test_exe in $TEST_EXECUTABLES; do
        echo "Running test: $test_exe"
        if [ -f "$test_exe" ]; then
            timeout 30s "$test_exe" || echo "⚠️  Test $test_exe may have failed or timed out, continuing execution..."
        fi
    done
fi

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
echo
echo "Tips:"
echo "- To clean coverage data, run: rm -rf $BUILD_DIR/*coverage* && find $BUILD_DIR -name '*.gcda' -delete"
echo "- To regenerate report, simply re-run this script"
echo "- Green indicates code covered by tests, red indicates uncovered code"
echo