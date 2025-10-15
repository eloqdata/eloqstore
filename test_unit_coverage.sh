#!/bin/bash

# EloqStore 单元测试覆盖率分析脚本
# 用于分析实际项目代码的测试覆盖率

set -e

echo "=== EloqStore 单元测试覆盖率分析 ==="
echo

# 检查必要工具
echo "1. 检查必要工具..."
if ! command -v lcov &> /dev/null; then
    echo "❌ lcov 未安装，请运行: sudo apt-get install lcov"
    exit 1
fi

if ! command -v genhtml &> /dev/null; then
    echo "❌ genhtml 未安装，请运行: sudo apt-get install lcov"
    exit 1
fi

echo "✓ lcov 和 genhtml 已安装"
echo

# 设置目录
PROJECT_ROOT="/root/eloqstore"
BUILD_DIR="$PROJECT_ROOT/build"
COVERAGE_DIR="$BUILD_DIR/unit_test_coverage_report"

cd "$PROJECT_ROOT"

echo "2. 配置项目为 Coverage 构建模式..."
cmake -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Coverage -DWITH_UNIT_TESTS=ON
echo "✓ 项目配置完成"
echo

echo "3. 编译项目和单元测试..."
cmake --build "$BUILD_DIR" --parallel $(nproc)
echo "✓ 编译完成"
echo

echo "4. 清理之前的覆盖率数据..."
find "$BUILD_DIR" -name "*.gcda" -delete 2>/dev/null || true
rm -f "$BUILD_DIR/unit_coverage.info" "$BUILD_DIR/unit_coverage_filtered.info"
rm -rf "$COVERAGE_DIR"
echo "✓ 清理完成"
echo

echo "5. 运行单元测试..."
cd "$BUILD_DIR"

# 首先尝试使用 ctest 运行 tests 目录下的所有测试
echo "使用 ctest 运行 tests 目录下的单元测试..."
if [ -d "tests" ]; then
    echo "找到 tests 目录，运行所有 CTest 测试..."
    # 设置超时并运行所有测试
    timeout 300s ctest --test-dir tests/ || echo "⚠️  部分测试可能失败或超时，继续执行..."
else
    echo "未找到 tests 目录，尝试查找其他测试可执行文件..."
    
    # 备用方案：查找所有测试可执行文件
    TEST_EXECUTABLES=$(find . -name "*test*" -type f -executable | head -20)
    
    if [ -z "$TEST_EXECUTABLES" ]; then
        echo "❌ 未找到任何测试可执行文件"
        echo "可用的可执行文件："
        find . -type f -executable | head -10
        exit 1
    fi
    
    echo "找到的测试文件："
    echo "$TEST_EXECUTABLES"
    echo
    
    # 运行每个测试
    for test_exe in $TEST_EXECUTABLES; do
        echo "运行测试: $test_exe"
        if [ -f "$test_exe" ]; then
            timeout 30s "$test_exe" || echo "⚠️  测试 $test_exe 可能失败或超时，继续执行..."
        fi
    done
fi

echo "✓ 单元测试执行完成"
echo

echo "6. 生成覆盖率数据..."
lcov --capture --directory . --output-file unit_coverage.info --ignore-errors negative,gcov
echo "✓ 覆盖率数据收集完成"
echo

echo "7. 过滤覆盖率数据（排除系统文件和第三方库）..."
lcov --remove unit_coverage.info \
    '/usr/*' \
    '*/abseil/*' \
    '*/concurrentqueue/*' \
    '*/inih/*' \
    '*/external/*' \
    '*/tests/*' \
    '*/coverage/*' \
    '*/benchmark/*' \
    '*/db_stress/*' \
    '*/examples/*' \
    --output-file unit_coverage_filtered.info \
    --ignore-errors unused

echo "✓ 数据过滤完成"
echo

echo "8. 生成 HTML 覆盖率报告..."
mkdir -p "$COVERAGE_DIR"
genhtml unit_coverage_filtered.info --output-directory "$COVERAGE_DIR"
echo "✓ HTML 报告生成完成"
echo

echo "9. 显示覆盖率摘要..."
lcov --summary unit_coverage_filtered.info
echo

echo "=== 单元测试覆盖率分析完成 ==="
echo
echo "报告位置: $COVERAGE_DIR/index.html"
echo
echo "查看报告的方法："
echo "1. 在浏览器中打开: file://$COVERAGE_DIR/index.html"
echo "2. 或者运行: firefox $COVERAGE_DIR/index.html"
echo "3. 或者启动 HTTP 服务器:"
echo "   cd $COVERAGE_DIR && python3 -m http.server 8081"
echo "   然后访问: http://localhost:8081"
echo
echo "提示："
echo "- 要清理覆盖率数据，运行: rm -rf $BUILD_DIR/*coverage* && find $BUILD_DIR -name '*.gcda' -delete"
echo "- 要重新生成报告，重新运行此脚本即可"
echo "- 绿色表示代码已被测试覆盖，红色表示未覆盖"
echo