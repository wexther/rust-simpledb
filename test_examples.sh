#!/bin/bash

# Simple DB Examples 测试脚本
# 用于运行和验证 examples 目录中的所有测试用例

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

echo -e "${BLUE}🧪 === Simple DB Examples 测试套件 ===${NC}"
echo

# 检查是否在项目根目录
if [ ! -f "Cargo.toml" ]; then
    echo -e "${RED}错误: 请在 simple_db 项目根目录下运行此脚本${NC}"
    exit 1
fi

# 编译项目
echo -e "🔨 编译项目..."
if cargo build; then
    echo -e "${GREEN}✅ 项目编译成功${NC}"
else
    echo -e "${RED}❌ 编译失败${NC}"
    exit 1
fi

# 创建测试结果目录
mkdir -p test_results

# 初始化计数器
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

echo -e "\n${BLUE}📝 === 运行SQL功能测试 ===${NC}"

# 规范化输出函数
normalize_output() {
    # 移除空行和前后空格，保留核心内容
    grep -v '^[[:space:]]*$' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//'
}

# 运行单个测试用例
run_test_case() {
    local test_num=$1
    local test_dir="examples/${test_num}"
    
    if [ ! -d "$test_dir" ]; then
        return
    fi
    
    if [ ! -f "$test_dir/input.txt" ] || [ ! -f "$test_dir/output.txt" ]; then
        return
    fi
    
    echo -e "🧪 运行测试用例: ${CYAN}${test_num}${NC}"
    TOTAL_TESTS=$((TOTAL_TESTS + 1))
    
    # 读取输入SQL
    local input_sql=$(cat "$test_dir/input.txt")
    echo "   📄 输入文件: $test_dir/input.txt"
    
    # 创建临时数据库目录
    local temp_db_dir="data/test_case_${test_num}"
    mkdir -p "$temp_db_dir"
    
    # 执行 simple_db 并捕获输出
    local actual_output_file="test_results/actual_${test_num}.txt"
    local expected_output_file="$test_dir/output.txt"
    
    echo "   🚀 执行SQL..."
    # 使用文件输入而不是 --execute 参数
    if cargo run -- --data-dir "$temp_db_dir" --db-name "test_${test_num}" "$test_dir/input.txt" 2>/dev/null > "$actual_output_file"; then
        echo "   📤 实际输出:"
        cat "$actual_output_file" | sed 's/^/      /'
        
        echo "   📥 期望输出:"
        cat "$expected_output_file" | sed 's/^/      /'
        
        # 规范化输出进行比较
        local normalized_actual=$(cat "$actual_output_file" | normalize_output)
        local normalized_expected=$(cat "$expected_output_file" | normalize_output)
        
        if [ "$normalized_actual" = "$normalized_expected" ]; then
            echo -e "   ${GREEN}✅ 测试通过${NC}"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo -e "   ${RED}❌ 测试失败${NC}"
            echo -e "   ${YELLOW}⚠️ 输出不匹配${NC}"
            echo "   实际输出 (规范化): '$normalized_actual'"
            echo "   期望输出 (规范化): '$normalized_expected'"
            FAILED_TESTS=$((FAILED_TESTS + 1))
        fi
    else
        echo -e "   ${RED}❌ 执行失败${NC}"
        echo "   错误信息:"
        cat "$actual_output_file" | sed 's/^/      /'
        FAILED_TESTS=$((FAILED_TESTS + 1))
    fi
    
    echo
}

# 发现并运行所有测试用例
echo "🔍 发现测试用例..."
for test_dir in examples/[0-9]*; do
    if [ -d "$test_dir" ]; then
        test_num=$(basename "$test_dir")
        run_test_case "$test_num"
    fi
done

echo "发现 $TOTAL_TESTS 个测试用例"

# 运行性能基准测试
echo -e "\n${BLUE}🚀 === 运行性能基准测试 ===${NC}"

echo "📊 运行基准测试..."
if cargo run --example benchmark > test_results/benchmark_output.txt 2>&1; then
    echo -e "${GREEN}✅ 基准测试完成${NC}"
    echo "结果保存在: test_results/benchmark_output.txt"
else
    echo -e "${RED}❌ 基准测试失败${NC}"
    echo "错误信息:"
    cat test_results/benchmark_output.txt | sed 's/^/   /'
fi

echo "📈 运行性能测试..."
if cargo run --example performance_test > test_results/performance_output.txt 2>&1; then
    echo -e "${GREEN}✅ 性能测试完成${NC}"
    echo "结果保存在: test_results/performance_output.txt"
else
    echo -e "${RED}❌ 性能测试失败${NC}"
    echo "错误信息:"
    cat test_results/performance_output.txt | sed 's/^/   /'
fi

# 输出测试总结
echo -e "\n${BLUE}📊 === 测试总结 ===${NC}"
echo "总测试用例: $TOTAL_TESTS"
echo -e "${GREEN}通过${NC}: $PASSED_TESTS"
echo -e "${RED}失败${NC}: $FAILED_TESTS"

if [ $TOTAL_TESTS -gt 0 ]; then
    SUCCESS_RATE=$(echo "scale=1; $PASSED_TESTS * 100 / $TOTAL_TESTS" | bc -l)
    echo "成功率: ${SUCCESS_RATE}%"
fi

if [ $FAILED_TESTS -eq 0 ]; then
    echo -e "\n${GREEN}🎉 所有测试都通过了！${NC}"
else
    echo -e "\n${YELLOW}⚠️ 有 $FAILED_TESTS 个测试失败${NC}"
fi

# 询问是否清理测试数据
echo
read -p "是否清理测试数据? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "🧹 清理测试数据..."
    rm -rf data/test_case_*
    rm -rf data/benchmark
    rm -rf data/performance_test
    rm -rf data/optimization_test
    rm -rf test_results
    echo -e "${GREEN}✅ 测试数据已清理${NC}"
fi

echo -e "\n测试完成！"
