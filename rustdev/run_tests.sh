#!/bin/bash
# Comprehensive test script for EloqStore

echo "================================"
echo "EloqStore Comprehensive Test Suite"
echo "================================"
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Test results
PASSED=0
FAILED=0

run_test() {
    local name=$1
    local cmd=$2

    echo -n "Running $name... "

    if eval "$cmd" > /tmp/test_output.log 2>&1; then
        echo -e "${GREEN}PASSED${NC}"
        PASSED=$((PASSED + 1))
    else
        echo -e "${RED}FAILED${NC}"
        echo "  Error output:"
        tail -20 /tmp/test_output.log | sed 's/^/    /'
        FAILED=$((FAILED + 1))
    fi
}

echo "1. Checking compilation..."
run_test "Library compilation" "cargo build --lib"
run_test "Test compilation" "cargo test --no-run"

echo
echo "2. Running unit tests..."
run_test "Codec tests" "cargo test codec:: -- --nocapture"
run_test "Page tests" "cargo test page:: -- --nocapture"
run_test "Index tests" "cargo test index:: -- --nocapture"
run_test "I/O Backend tests" "cargo test io::backend:: -- --nocapture"

echo
echo "3. Running integration tests..."
run_test "Storage integration" "cargo test storage_test -- --nocapture"
run_test "Manifest integration" "cargo test manifest_test -- --nocapture"

echo
echo "4. Running doc tests..."
run_test "Documentation tests" "cargo test --doc"

echo
echo "================================"
echo "Test Summary"
echo "================================"
echo -e "Passed: ${GREEN}$PASSED${NC}"
echo -e "Failed: ${RED}$FAILED${NC}"

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
    exit 0
else
    echo -e "${RED}Some tests failed!${NC}"
    exit 1
fi