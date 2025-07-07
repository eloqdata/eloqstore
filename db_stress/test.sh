#!/bin/bash

# 夜间压力测试脚本
# 晚上8点到凌晨4点：白盒测试（连续运行）
# 凌晨4点到早上10点：黑盒测试

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/log"
WHITEBOX_LOG_DIR="$LOG_DIR/whitebox"
BLACKBOX_LOG_DIR="$LOG_DIR/blackbox"
ERROR_LOG_DIR="$LOG_DIR/errors"
CRASH_TEST_PY="$SCRIPT_DIR/crash_test.py"
CLEANUP_CMD="../build/db_stress/clean_up"

# 创建日志目录
mkdir -p "$WHITEBOX_LOG_DIR"
mkdir -p "$BLACKBOX_LOG_DIR"
mkdir -p "$ERROR_LOG_DIR"

# 全局变量
CURRENT_TEST_PID=""
CURRENT_TEST_TYPE=""
LAST_STATUS=""  # 新增：记录上一次的状态，避免重复日志
# 日志函数
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# 错误日志函数
log_error() {
    local error_msg="$1"
    local test_type="$2"
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local error_log="$ERROR_LOG_DIR/error_${test_type}_${timestamp}.log"
    
    {
        echo "==================== 错误报告 ===================="
        echo "时间: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "测试类型: $test_type"
        echo "错误信息: $error_msg"
        echo "================================================="
        echo ""
    } >> "$error_log"
    
    log_message "错误已记录到: $error_log"
}

# 清理函数
perform_cleanup() {
    log_message "执行清理操作"
    if [ -f "$CLEANUP_CMD" ]; then
        $CLEANUP_CMD
        log_message "清理操作完成"
    else
        log_message "警告: 清理命令不存在: $CLEANUP_CMD"
    fi
}

# 终止当前测试进程
kill_current_test() {
    if [ -n "$CURRENT_TEST_PID" ] && kill -0 "$CURRENT_TEST_PID" 2>/dev/null; then
        log_message "正在终止当前测试进程 $CURRENT_TEST_PID"
        kill -TERM "$CURRENT_TEST_PID"
        sleep 5
        
        # 如果进程还在运行，强制kill
        if kill -0 "$CURRENT_TEST_PID" 2>/dev/null; then
            log_message "强制终止进程 $CURRENT_TEST_PID"
            kill -KILL "$CURRENT_TEST_PID"
        fi
        
        wait "$CURRENT_TEST_PID" 2>/dev/null
        CURRENT_TEST_PID=""
        log_message "测试进程已终止"
    fi
}
# 计算到指定小时的剩余秒数
calculate_duration_to_hour() {
    local target_hour=$1
    local current_time=$(date '+%s')
    local current_hour=$(date '+%H' | sed 's/^0//')
    local current_minute=$(date '+%M' | sed 's/^0//')
    local current_second=$(date '+%S' | sed 's/^0//')
    
    # 计算今天目标时间的时间戳
    local today_target=$(date -d "today ${target_hour}:00:00" '+%s')
    
    # 如果目标时间已经过了，计算明天的目标时间
    if [ $current_time -gt $today_target ]; then
        local tomorrow_target=$(date -d "tomorrow ${target_hour}:00:00" '+%s')
        echo $((tomorrow_target - current_time))
    else
        echo $((today_target - current_time))
    fi
}
# 启动白盒测试（连续运行直到被kill）
start_whitebox_test() {
    local duration=$(calculate_duration_to_hour 4)
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local log_file="$WHITEBOX_LOG_DIR/whitebox_${timestamp}.log"
    
    log_message "开始执行白盒测试，持续时间: ${duration}秒 (到凌晨4点)"
    log_message "日志文件: $log_file"
    
    # 执行清理操作
    perform_cleanup
    
    # 启动白盒测试，传入计算出的duration
    stdbuf -oL -eL python3 "$CRASH_TEST_PY" whitebox \
        --duration=$duration \
        --kill_odds=1000000 \
        --n_tables=10 \
        --max_key=10000 \
        --shortest_value=1024 \
        --longest_value=4096 \
        --use_random_params > "$log_file" 2>&1 &
    CURRENT_TEST_PID=$!
    CURRENT_TEST_TYPE="whitebox"
    
    log_message "白盒测试进程PID: $CURRENT_TEST_PID"
}

# 启动黑盒测试
start_blackbox_test() {
    local duration=$(calculate_duration_to_hour 10)
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local log_file="$BLACKBOX_LOG_DIR/blackbox_${timestamp}.log"
    
    log_message "开始执行黑盒测试，持续时间: ${duration}秒 (到早上10点)"
    log_message "日志文件: $log_file"
    
    # 执行清理操作
    perform_cleanup
    
    # 启动黑盒测试
    stdbuf -oL -eL python3 "$CRASH_TEST_PY" blackbox \
        --duration=$duration \
        --interval=360 \
        --n_tables=10 \
        --max_key=10000 \
        --shortest_value=1024 \
        --longest_value=4096 \
        --use_random_params > "$log_file" 2>&1 &
    CURRENT_TEST_PID=$!
    CURRENT_TEST_TYPE="blackbox"
    
    log_message "黑盒测试进程PID: $CURRENT_TEST_PID"
}

# 监控当前测试进程
monitor_current_test() {
    if [ -n "$CURRENT_TEST_PID" ]; then
        # 检查进程是否还在运行
        if ! kill -0 "$CURRENT_TEST_PID" 2>/dev/null; then
            # 进程已经停止，检查是否是异常退出
            wait "$CURRENT_TEST_PID"
            local exit_code=$?
            
            if [ $exit_code -ne 0 ]; then
                # 异常退出，记录错误并重启
                local error_msg="测试进程异常退出 (PID: $CURRENT_TEST_PID, 退出码: $exit_code)"
                log_message "$error_msg"
                log_error "$error_msg" "$CURRENT_TEST_TYPE"
                
                
                # 重启相同类型的测试
                if [ "$CURRENT_TEST_TYPE" = "whitebox" ]; then
                    log_message "重启白盒测试"
                    start_whitebox_test
                elif [ "$CURRENT_TEST_TYPE" = "blackbox" ]; then
                    log_message "重启黑盒测试"
                    start_blackbox_test
                fi
            else
                log_message "测试进程正常结束 (PID: $CURRENT_TEST_PID)"
                CURRENT_TEST_PID=""
                CURRENT_TEST_TYPE=""
            fi
        fi
    fi
}

# 获取当前小时
get_current_hour() {
    date '+%H' | sed 's/^0//'
}

# 主循环
main_loop() {
    while true; do
        current_hour=$(get_current_hour)
        # 监控当前测试进程状态
        monitor_current_test
        # 确定当前应该处于什么状态
        local expected_status="" 
        # 20 / 4    4/10 原先
        if [ $current_hour -ge 14 ] || [ $current_hour -lt 4 ]; then
            expected_status="whitebox"
        elif [ $current_hour -ge 4 ] && [ $current_hour -lt 13 ]; then
            expected_status="blackbox"
        else
            expected_status="rest"
        fi
        # 只有当状态发生变化时才输出日志和执行操作
        if [ "$expected_status" != "$LAST_STATUS" ]; then
            case "$expected_status" in
                "whitebox")
                    if [ "$CURRENT_TEST_TYPE" != "whitebox" ]; then
                        log_message "当前时间: ${current_hour}点，切换到白盒测试时段"
                        if [ -n "$CURRENT_TEST_PID" ]; then
                            kill_current_test
                        fi
                        start_whitebox_test
                    fi
                    ;;
                "blackbox")
                    if [ "$CURRENT_TEST_TYPE" != "blackbox" ]; then
                        log_message "当前时间: ${current_hour}点，切换到黑盒测试时段"
                        if [ -n "$CURRENT_TEST_PID" ]; then
                            kill_current_test
                        fi
                        start_blackbox_test
                    fi
                    ;;
                "rest")
                    if [ -n "$CURRENT_TEST_PID" ]; then
                        log_message "当前时间: ${current_hour}点，进入休息时间，终止当前测试"
                        kill_current_test
                    fi
                    log_message "当前时间: ${current_hour}点，休息时间，等待下一个测试时段"
                    ;;
            esac  # case反过来
            LAST_STATUS="$expected_status"
        fi
        # 每30秒检查一次
        sleep 30
    done
}

# 信号处理函数
cleanup_and_exit() {
    log_message "收到退出信号，正在清理..."
    # 终止当前测试
    kill_current_test
    # 杀死所有子进程
    jobs -p | xargs -r kill
    log_message "夜间压力测试脚本退出"
    exit 0
}

# 设置信号处理
trap cleanup_and_exit SIGINT SIGTERM

# 检查依赖
if [ ! -f "$CRASH_TEST_PY" ]; then
    log_message "错误: crash_test.py 不存在: $CRASH_TEST_PY"
    exit 1
fi

# 启动主循环
main_loop