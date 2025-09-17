#!/bin/bash
# Simple test runner that tests individual modules

echo "Running EloqStore Tests"
echo "======================="

# Build the library first
echo "Building library..."
if cargo build --lib 2>/dev/null; then
    echo "✓ Library builds successfully"
else
    echo "✗ Library build failed"
    exit 1
fi

# Run simple tests
echo
echo "Running basic tests..."

# Test page module
echo -n "Testing page module... "
if cargo test --lib page::tests 2>/dev/null; then
    echo "✓ PASSED"
else
    echo "✗ FAILED"
fi

# Test codec module
echo -n "Testing codec module... "
if cargo test --lib codec::tests 2>/dev/null; then
    echo "✓ PASSED"
else
    echo "✗ FAILED"
fi

# Test I/O backend
echo -n "Testing I/O backend... "
if cargo test --lib io::backend::tests 2>/dev/null; then
    echo "✓ PASSED"
else
    echo "✗ FAILED"
fi

echo
echo "Test run complete!"