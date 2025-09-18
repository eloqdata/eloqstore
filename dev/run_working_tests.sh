#!/bin/bash

# Script to run working tests in the dev directory
cd "$(dirname "$0")"

echo "Running all working tests..."
echo "=============================="

# List of tests that should be working
WORKING_TESTS=(
    "coding_test"
    "comparator_test"
    "data_page_builder_test"
    "data_page_test"
    "edge_case_test"
    "index_page_test"
    "overflow_page_test"
    "page_mapper_test"
    "page_test"
    "types_test"
    "boundary_values_test"
    "async_io_manager_test"
    "file_gc_test"
)

PASSED=0
FAILED=0
FAILED_TESTS=()

for test in "${WORKING_TESTS[@]}"; do
    echo ""
    echo "Running $test..."
    echo "----------------"

    if [[ -f "build/$test" ]]; then
        if ./build/$test; then
            echo "✓ $test PASSED"
            ((PASSED++))
        else
            echo "✗ $test FAILED"
            ((FAILED++))
            FAILED_TESTS+=("$test")
        fi
    else
        echo "✗ $test NOT FOUND (build failed?)"
        ((FAILED++))
        FAILED_TESTS+=("$test")
    fi
done

echo ""
echo "=============================="
echo "Test Results:"
echo "PASSED: $PASSED"
echo "FAILED: $FAILED"

if [[ $FAILED -gt 0 ]]; then
    echo ""
    echo "Failed tests:"
    for test in "${FAILED_TESTS[@]}"; do
        echo "  - $test"
    done
    exit 1
fi

echo ""
echo "All tests passed!"
exit 0