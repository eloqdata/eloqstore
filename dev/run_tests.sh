#!/bin/bash

# EloqStore Comprehensive Test Suite Runner
# This script manages building and running the comprehensive test suite

set -e  # Exit on error

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BUILD_DIR="${SCRIPT_DIR}/build"
PARENT_BUILD_DIR="${SCRIPT_DIR}/../build"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
BUILD_TYPE="Debug"
RUN_TESTS=true
WITH_COVERAGE=false
WITH_ASAN=false
WITH_TSAN=false
WITH_UBSAN=false
VERBOSE=false
FILTER=""
PARALLEL_JOBS=8

# Function to print colored output
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Function to display usage
usage() {
    cat << EOF
Usage: $0 [OPTIONS]

Options:
    -h, --help              Show this help message
    -b, --build-only        Only build tests, don't run them
    -r, --release           Build in Release mode (default: Debug)
    -c, --coverage          Enable code coverage reporting
    -a, --asan              Enable Address Sanitizer
    -t, --tsan              Enable Thread Sanitizer
    -u, --ubsan             Enable Undefined Behavior Sanitizer
    -f, --filter PATTERN    Run only tests matching pattern
    -v, --verbose           Enable verbose output
    -j, --jobs N            Number of parallel build jobs (default: 8)
    --clean                 Clean build directory before building
    --no-build              Skip building, only run tests

Test Categories:
    --unit                  Run only unit tests
    --integration           Run only integration tests
    --stress                Run only stress tests
    --edge-cases            Run only edge case tests
    --benchmark             Run benchmark tests

Examples:
    $0                      # Build and run all tests
    $0 -r                   # Build in Release mode and run tests
    $0 -c                   # Run with coverage reporting
    $0 -a                   # Run with Address Sanitizer
    $0 -f "[coding]"        # Run only coding tests
    $0 --unit               # Run only unit tests
    $0 -b -c                # Build with coverage, don't run
EOF
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            usage
            exit 0
            ;;
        -b|--build-only)
            RUN_TESTS=false
            shift
            ;;
        -r|--release)
            BUILD_TYPE="Release"
            shift
            ;;
        -c|--coverage)
            WITH_COVERAGE=true
            shift
            ;;
        -a|--asan)
            WITH_ASAN=true
            shift
            ;;
        -t|--tsan)
            WITH_TSAN=true
            shift
            ;;
        -u|--ubsan)
            WITH_UBSAN=true
            shift
            ;;
        -f|--filter)
            FILTER="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -j|--jobs)
            PARALLEL_JOBS="$2"
            shift 2
            ;;
        --clean)
            rm -rf "${BUILD_DIR}"
            print_info "Cleaned build directory"
            shift
            ;;
        --no-build)
            SKIP_BUILD=true
            shift
            ;;
        --unit)
            FILTER="${FILTER}[unit]"
            shift
            ;;
        --integration)
            FILTER="${FILTER}[integration]"
            shift
            ;;
        --stress)
            FILTER="${FILTER}[stress]"
            shift
            ;;
        --edge-cases)
            FILTER="${FILTER}[edge-case]"
            shift
            ;;
        --benchmark)
            FILTER="${FILTER}[benchmark]"
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            usage
            exit 1
            ;;
    esac
done

# Check for conflicting sanitizers
if [[ "${WITH_ASAN}" == "true" && "${WITH_TSAN}" == "true" ]]; then
    print_error "Cannot use Address Sanitizer and Thread Sanitizer together"
    exit 1
fi

# Build the parent project first if needed
build_parent_project() {
    print_info "Checking parent project build..."

    if [[ ! -d "${PARENT_BUILD_DIR}" ]]; then
        print_info "Building parent project..."
        mkdir -p "${PARENT_BUILD_DIR}"
        cd "${PARENT_BUILD_DIR}"
        cmake .. -DCMAKE_BUILD_TYPE="${BUILD_TYPE}"
        make -j${PARALLEL_JOBS} eloqstore test_util
        cd "${SCRIPT_DIR}"
        print_success "Parent project built successfully"
    else
        print_info "Parent project already built"
    fi
}

# Build the test suite
build_tests() {
    print_info "Building comprehensive test suite..."
    print_info "Build type: ${BUILD_TYPE}"
    print_info "Coverage: ${WITH_COVERAGE}"
    print_info "ASAN: ${WITH_ASAN}"
    print_info "TSAN: ${WITH_TSAN}"
    print_info "UBSAN: ${WITH_UBSAN}"

    mkdir -p "${BUILD_DIR}"
    cd "${BUILD_DIR}"

    # Prepare CMake arguments
    CMAKE_ARGS=(
        "-DCMAKE_BUILD_TYPE=${BUILD_TYPE}"
        "-DWITH_COVERAGE=${WITH_COVERAGE}"
        "-DWITH_ASAN=${WITH_ASAN}"
        "-DWITH_TSAN=${WITH_TSAN}"
        "-DWITH_UBSAN=${WITH_UBSAN}"
    )

    # Configure
    cmake .. "${CMAKE_ARGS[@]}"

    # Build
    if [[ "${VERBOSE}" == "true" ]]; then
        make -j${PARALLEL_JOBS} VERBOSE=1
    else
        make -j${PARALLEL_JOBS}
    fi

    cd "${SCRIPT_DIR}"
    print_success "Build completed successfully"
}

# Run the tests
run_tests() {
    print_info "Running tests..."

    cd "${BUILD_DIR}"

    if [[ -n "${FILTER}" ]]; then
        print_info "Filter: ${FILTER}"
    fi

    # Prepare ctest arguments
    CTEST_ARGS=(
        "--output-on-failure"
    )

    if [[ "${VERBOSE}" == "true" ]]; then
        CTEST_ARGS+=("--verbose")
    fi

    if [[ -n "${FILTER}" ]]; then
        # For Catch2 tests, we need to pass the filter to the test executable
        # CTest doesn't directly support Catch2 filters, so we run executables directly
        print_info "Running filtered tests: ${FILTER}"

        for test_exe in $(find . -maxdepth 1 -type f -executable -name "*_test"); do
            test_name=$(basename "${test_exe}")
            print_info "Running ${test_name} with filter '${FILTER}'"

            if [[ "${VERBOSE}" == "true" ]]; then
                "${test_exe}" "${FILTER}" --success --durations yes
            else
                "${test_exe}" "${FILTER}"
            fi

            if [[ $? -eq 0 ]]; then
                print_success "${test_name} passed"
            else
                print_error "${test_name} failed"
            fi
        done
    else
        # Run all tests through CTest
        ctest "${CTEST_ARGS[@]}"
    fi

    cd "${SCRIPT_DIR}"
}

# Generate coverage report
generate_coverage() {
    if [[ "${WITH_COVERAGE}" != "true" ]]; then
        return
    fi

    print_info "Generating coverage report..."
    cd "${BUILD_DIR}"

    # Capture coverage data
    lcov --capture --directory . --output-file coverage.info

    # Remove unwanted files from coverage
    lcov --remove coverage.info \
        '/usr/*' \
        '*/tests/*' \
        '*/external/*' \
        '*/concurrentqueue/*' \
        '*/abseil/*' \
        '*/inih/*' \
        --output-file coverage_filtered.info

    # Generate HTML report
    genhtml coverage_filtered.info --output-directory coverage-report

    print_success "Coverage report generated: ${BUILD_DIR}/coverage-report/index.html"

    # Print summary
    lcov --summary coverage_filtered.info

    cd "${SCRIPT_DIR}"
}

# Main execution
main() {
    print_info "EloqStore Comprehensive Test Suite Runner"
    print_info "=========================================="

    # Build parent project if needed
    if [[ "${SKIP_BUILD}" != "true" ]]; then
        build_parent_project
    fi

    # Build tests
    if [[ "${SKIP_BUILD}" != "true" ]]; then
        build_tests
    fi

    # Run tests
    if [[ "${RUN_TESTS}" == "true" ]]; then
        run_tests

        # Generate coverage if enabled
        if [[ "${WITH_COVERAGE}" == "true" ]]; then
            generate_coverage
        fi
    fi

    print_success "All operations completed successfully!"
}

# Run main function
main