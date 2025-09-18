#!/bin/bash

# Run all EloqStore tests and generate summary report

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="$SCRIPT_DIR/build_test"
REPORT_FILE="$SCRIPT_DIR/test_execution_report.md"

cd "$BUILD_DIR" || exit 1

echo "# EloqStore Test Execution Report" > "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "Generated: $(date)" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0
SKIPPED_TESTS=0

echo "## Test Results" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

# List of all test executables
TEST_EXECUTABLES=(
    "async_io_manager_test"
    "async_ops_test"
    "async_pattern_test"
    "basic_operations_test"
    "benchmark_test"
    "boundary_values_test"
    "coding_test"
    "comparator_test"
    "concurrent_test"
    "data_integrity_test"
    "data_page_builder_test"
    "data_page_test"
    "edge_case_test"
    "fault_injection_test"
    "file_gc_test"
    "index_page_test"
    "overflow_page_test"
    "page_mapper_test"
    "page_test"
    "randomized_stress_test"
    "recovery_test"
    "types_test"
    "workflow_test"
)

echo "| Test Name | Status | Details |" >> "$REPORT_FILE"
echo "|-----------|--------|---------|" >> "$REPORT_FILE"

for test_exec in "${TEST_EXECUTABLES[@]}"; do
    if [ -f "$test_exec" ]; then
        echo "Running: $test_exec"
        TOTAL_TESTS=$((TOTAL_TESTS + 1))

        # Run test with timeout and capture output
        if timeout 10 ./"$test_exec" --success > /tmp/test_output.txt 2>&1; then
            # Extract test results from output
            if grep -q "All tests passed" /tmp/test_output.txt; then
                PASSED_TESTS=$((PASSED_TESTS + 1))
                DETAILS=$(grep "All tests passed" /tmp/test_output.txt | tail -1)
                echo "| $test_exec | ✅ PASSED | $DETAILS |" >> "$REPORT_FILE"
            else
                FAILED_TESTS=$((FAILED_TESTS + 1))
                DETAILS=$(grep -E "test cases:|assertions:" /tmp/test_output.txt | tail -1)
                echo "| $test_exec | ❌ FAILED | $DETAILS |" >> "$REPORT_FILE"
            fi
        else
            # Test crashed or timed out
            FAILED_TESTS=$((FAILED_TESTS + 1))
            echo "| $test_exec | 💥 CRASHED | Timeout or crash |" >> "$REPORT_FILE"
        fi
    else
        echo "| $test_exec | ⏭️ NOT BUILT | Test executable not found |" >> "$REPORT_FILE"
        SKIPPED_TESTS=$((SKIPPED_TESTS + 1))
    fi
done

# Summary section
echo "" >> "$REPORT_FILE"
echo "## Summary" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"
echo "- **Total Tests Run**: $TOTAL_TESTS" >> "$REPORT_FILE"
echo "- **Passed**: $PASSED_TESTS ($(echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc)%)" >> "$REPORT_FILE"
echo "- **Failed**: $FAILED_TESTS ($(echo "scale=1; $FAILED_TESTS * 100 / $TOTAL_TESTS" | bc)%)" >> "$REPORT_FILE"
echo "- **Not Built**: $SKIPPED_TESTS" >> "$REPORT_FILE"
echo "" >> "$REPORT_FILE"

if [ $PASSED_TESTS -gt 15 ]; then
    echo "### 🎉 Test Framework Status: GOOD" >> "$REPORT_FILE"
    echo "Most tests are passing. The framework is functional." >> "$REPORT_FILE"
elif [ $PASSED_TESTS -gt 10 ]; then
    echo "### ⚠️ Test Framework Status: FAIR" >> "$REPORT_FILE"
    echo "Some tests are passing but more work is needed." >> "$REPORT_FILE"
else
    echo "### ❌ Test Framework Status: NEEDS WORK" >> "$REPORT_FILE"
    echo "Many tests are failing. Significant fixes required." >> "$REPORT_FILE"
fi

echo ""
echo "Test execution complete. Report saved to: $REPORT_FILE"
cat "$REPORT_FILE"