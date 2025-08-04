#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/log"

DATA_DIR="$SCRIPT_DIR/data1"
# actually only need DataDir
DB_DIR="$DATA_DIR/db_stress"
SHARED_STATE_DIR="$DATA_DIR/shared_state"
WHITEBOX_LOG_DIR="$LOG_DIR/whitebox"
BLACKBOX_LOG_DIR="$LOG_DIR/blackbox"
ERROR_LOG_DIR="$LOG_DIR/errors"
CRASH_TEST_PY="$SCRIPT_DIR/crash_test.py"

DISK_LOG_DIR="$LOG_DIR/disk"
DISK_LOG_FILE="$DISK_LOG_DIR/disk_usage.log"

MINIO_DATA_PATH="/home/sjh/minio/data"
DISK_MONITOR_PID=""

SWITCH_INTERVAL_HOURS=1
KILL_WAIT_TIME=10
CURRENT_TEST_PARAMS=""

AUTO_UPDATE_ENABLED=false  # auto pull and build new code

PARAM_COMBINATIONS=(
    #  "--data_append_mode=false"  # Combination 0: no cloud_store_path, data_append_mode=false
    # "--data_append_mode=true"   # Combination 1: no cloud_store_path, data_append_mode=true
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=false"  # Combination 2: with cloud_store_path, data_append_mode=false
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=true"   # Combination 3: with cloud_store_path, data_append_mode=true
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=false"  # Combination 4: with cloud_store_path, data_append_mode=false
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=true"   # Combination 5: with cloud_store_path, data_append_mode=true
)
# Create log directories
mkdir -p "$WHITEBOX_LOG_DIR"
mkdir -p "$BLACKBOX_LOG_DIR"
mkdir -p "$ERROR_LOG_DIR"
mkdir -p "$DISK_LOG_DIR"

CURRENT_TEST_PID=""
CURRENT_TEST_TYPE=""
LAST_STATUS=""  # Added: record last status to avoid duplicate logs

NEXT_SWITCH_TIME=""
# Log function
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

STORAGE_PARAM_COMBINATIONS=(
    "--data_append_mode=false"  
    "--data_append_mode=true"   
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=false"  
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=true"   
)
SYSTEM_TYPE_PARAM_COMBINATIONS=(
    #验证表所占用的内存就是n_table*n_parition*max_key*sizeof(int)
    #磁盘所占用的理论容量是
    #n_table*n_partition*max_key*(12+(shortest_value+longestvalue)/2)*0.75(读和删除的比例)
    # ps:12是实际写入db的key的最小值,内存中不需要是因为使用index来优化掉key的开销
    
    #增大table和partition都可以让db的树变多,但是table可以提高并发请求的压力,因为table等价于线程数
    # 二更:其实也不一定,partition是写者数量,其实也就是一个线程内可以写多少次,同时每个partition也配置了固定数量的写者
    # 单个partition的磁盘占用为max_key*(12+(shortest_value+longestvalue)/2)*0.75
    #   我希望占用大概至少150G的理论磁盘,同时单个patition至少要有1百兆

    # 单个partiton超大的测试
    # 单个partition: 10000000 * (12 + (32+32)/2) * 0.75 = 100000000 * 44 * 0.75 = 3.3GB
    # 总磁盘占用: 20 * 10 * 330MB = 66GB
    "--n_tables=20 --n_partitions=1 --max_key=100000000 --shortest_value=32 --longest_value=32 --active_width=10000000 --keys_per_batch=500000"
    
    # 普通patition测试,超高并发
    # 单个partition: 10000000 * (12 + (32+32)/2) * 0.75 = 10000000 * 44 * 0.75 = 330MB
    # 总磁盘占用: 50 * 330MB = 15.36GB
    "--n_tables=50 --n_partitions=1 --max_key=10000000 --shortest_value=32 --longest_value=32 --active_width=500000 --keys_per_batch=30000"
    
    # 常规测试,正常并发,正常磁盘,正常patition
    # 单个partition: 10000000 * (12 + (32+32)/2) * 0.75 = 10000000 * 44 * 0.75 = 330MB
    # 总磁盘占用: 20 * 10 * 330MB = 66GB
    "--n_tables=20 --n_partitions=10 --max_key=10000000 --shortest_value=32 --longest_value=32 --active_width=200000 --keys_per_batch=2000"
    
    # 中小等value测试
    # 单个partition: 3000000 * (12 + (128+512)/2) * 0.75 = 3000000 * 332 * 0.75 = 747MB
    # 总磁盘占用: 30 * 3 * 747MB = 67GB
    "--n_tables=30 --n_partitions=3 --max_key=3000000 --shortest_value=128 --longest_value=512 --active_width=300000 --keys_per_batch=25000"
    
    # 中value测试
    # 单个partition: 1000000 * (12 + (1024+8192)/2) * 0.75 = 1000000 * 4620 * 0.75 = 3.47GB
    # 总磁盘占用: 20 * 3.47GB = 69.4GB
    "--n_tables=20 --n_partitions=1 --max_key=1000000 --shortest_value=1024 --longest_value=8192 --active_width=100000 --keys_per_batch=10000"
    
    # 大value测试
    # 单个partition: 100000 * (12 + (8192+65536)/2) * 0.75 = 100000 * 36876 * 0.75 = 2.77GB
    # 总磁盘占用: 20 * 2.77GB = 55.4GB
    "--n_tables=20 --n_partitions=1 --max_key=100000 --shortest_value=8192 --longest_value=65536 --active_width=10000 --keys_per_batch=5000"
    
    

    # 单个partition: 500000 * (12 + (4096+32768)/2) * 0.75 = 500000 * 18444 * 0.75 = 6.92GB (超过4GB上限!)
    # 总磁盘占用: 10 * 6.92GB = 69.2GB
    "--n_tables=10 --n_partitions=1 --max_key=500000 --shortest_value=4096 --longest_value=32768 --active_width=50000 --keys_per_batch=6000"
    

    # 单个partition: 1500000 * (12 + (32+64)/2) * 0.75 = 1500000 * 60 * 0.75 = 67.5MB (低于100MB下限!)
    # 总磁盘占用: 50 * 20 * 67.5MB = 60.2GB
    "--n_tables=50 --n_partitions=20 --max_key=1500000 --shortest_value=32 --longest_value=64 --active_width=150000 --keys_per_batch=15000"
    
    
)
calculate_theoretical_disk_usage() {
    local param_args="$1"
    
    # 从参数中提取关键值
    local n_tables=$(echo "$param_args" | grep -o "\--n_tables=[0-9]*" | cut -d'=' -f2)
    local n_partitions=$(echo "$param_args" | grep -o "\--n_partitions=[0-9]*" | cut -d'=' -f2)
    local max_key=$(echo "$param_args" | grep -o "\--max_key=[0-9]*" | cut -d'=' -f2)
    local shortest_value=$(echo "$param_args" | grep -o "\--shortest_value=[0-9]*" | cut -d'=' -f2)
    local longest_value=$(echo "$param_args" | grep -o "\--longest_value=[0-9]*" | cut -d'=' -f2)
    
    # 设置默认值（如果参数中没有找到）
    n_tables=${n_tables:-1}
    n_partitions=${n_partitions:-1}
    max_key=${max_key:-1000000}
    shortest_value=${shortest_value:-32}
    longest_value=${longest_value:-32}
    
    # 计算理论磁盘使用量
    # 公式: n_table * n_partition * max_key * (12 + (shortest_value + longest_value) / 2) * 0.75
    local avg_value=$(( (shortest_value + longest_value) / 2 ))
    local key_overhead=12
    local usage_ratio=0.75
    
    # 使用bc进行浮点计算
    local theoretical_bytes=$(echo "$n_tables * $n_partitions * $max_key * ($key_overhead + $avg_value) * $usage_ratio" | bc -l)
    
    # 转换为GB
    local theoretical_gb=$(echo "scale=2; $theoretical_bytes / 1024 / 1024 / 1024" | bc -l)
    
    echo "$theoretical_gb"
}
start_disk_monitor() {
    local test_type="$1"
    local theoretical_usage="$2"
    
    # 停止之前的监控进程（如果存在）
    stop_disk_monitor
    {
        echo ""
        echo ""
        echo ""
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting $test_type test"
        echo "Theoretical disk usage: ${theoretical_usage}GB"
        echo "Actual disk usage monitoring (every 1 minute):"
    } >> "$DISK_LOG_FILE"
    
    # 启动后台监控进程
    (
        while true; do
            sleep 60  # 每分钟监控一次
            
            # 获取当前磁盘使用量
            local current_usage=""
            if [ -d "$DB_DIR" ]; then
                # 获取数据目录的磁盘使用量（以GB为单位）
                current_usage=$(du -sb "$DB_DIR" 2>/dev/null | awk '{printf "%.2f", $1/1024/1024/1024}')
                if [ -z "$current_usage" ]; then
                    current_usage="0.00"
                fi
            else
                current_usage="0.00"
            fi
            
            # 检查是否使用minio存储
            local minio_usage=""
            local using_minio=false
            
            # 检查当前测试参数是否包含cloud_store_path
            if [ -n "$CURRENT_TEST_PARAMS" ] && echo "$CURRENT_TEST_PARAMS" | grep -q "cloud_store_path"; then
                using_minio=true
                
                # 获取minio数据目录的磁盘使用量
                if [ -d "$MINIO_DATA_PATH" ]; then
                    minio_usage=$(du -sb "$MINIO_DATA_PATH" 2>/dev/null | awk '{printf "%.2f", $1/1024/1024/1024}')
                    if [ -z "$minio_usage" ]; then
                        minio_usage="0.00"
                    fi
                else
                    minio_usage="0.00"
                fi
            fi
            
            # 根据是否使用minio来格式化输出
            if [ "$using_minio" = true ]; then
                # 格式：本地GB/minioGB
                echo -n "${current_usage}GB/${minio_usage}GB," >> "$DISK_LOG_FILE"
            else
                # 只有本地存储
                echo -n "${current_usage}GB," >> "$DISK_LOG_FILE"
            fi
        done
    ) &
    DISK_MONITOR_PID=$!
    log_message "Disk monitor started with PID: $DISK_MONITOR_PID"
}
stop_disk_monitor() {
    if [ -n "$DISK_MONITOR_PID" ] && kill -0 "$DISK_MONITOR_PID" 2>/dev/null; then
        log_message "Stopping disk monitor process $DISK_MONITOR_PID"
        kill -TERM "$DISK_MONITOR_PID" 2>/dev/null
        sleep 2
        
        # 如果进程仍在运行，强制终止
        if kill -0 "$DISK_MONITOR_PID" 2>/dev/null; then
            kill -KILL "$DISK_MONITOR_PID" 2>/dev/null
        fi
        
        wait "$DISK_MONITOR_PID" 2>/dev/null
        DISK_MONITOR_PID=""
        
        # 在日志文件末尾添加换行符
        echo "" >> "$DISK_LOG_FILE"
        log_message "Disk monitor stopped"
    fi
}
# 修改后的参数组合获取函数
get_random_param_combination() {
    # 从存储特性参数组合中随机选择一个
    local storage_count=${#STORAGE_PARAM_COMBINATIONS[@]}
    local storage_index=$((RANDOM % storage_count))
    local storage_params="${STORAGE_PARAM_COMBINATIONS[$storage_index]}"
    
    # 从系统类型参数组合中随机选择一个
    local system_count=${#SYSTEM_TYPE_PARAM_COMBINATIONS[@]}
    local system_index=$((RANDOM % system_count))
    local system_params="${SYSTEM_TYPE_PARAM_COMBINATIONS[$system_index]}"
    
    # 记录选择的参数组合类型
    local storage_type=$((storage_index + 1))
    local system_type=$((system_index + 1))
    log_message "Selected storage type: $storage_type, system type: $system_type"
    # 拼接并返回完整参数
    echo "$storage_params $system_params"
}
# Error log function
log_error() {
    local error_msg="$1"
    local test_type="$2"
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local error_log="$ERROR_LOG_DIR/error_${test_type}_${timestamp}.log"
    
    # Get disk usage information
    local data_dir_usage=""
    
    # Check ./data directory disk usage
    if [ -d "$DATA_DIR" ]; then
        data_dir_usage=$(du -sh "$DATA_DIR" 2>/dev/null || echo "Unable to get data directory size")
    else
        data_dir_usage="Data directory does not exist"
    fi
    
    
    {
        echo "==================== Error Report ===================="
        echo "Time: $(date '+%Y-%m-%d %H:%M:%S')"
        echo "Test Type: $test_type"
        echo "Error Message: $error_msg"
        echo "================================================="
        echo ""
        echo "==================== Disk Usage =================="
        echo "Data directory usage: $data_dir_usage"
        echo "================================================="
        echo ""
    } >> "$error_log"
    
    log_message "Error logged to: $error_log"
}
# Cleanup function
perform_cleanup() {
    log_message "Performing cleanup operation"
    if [ -d "$DATA_DIR" ]; then
        log_message "Deleting data directory: $DATA_DIR"
        rm -rf "$DATA_DIR" || {
            log_message "Cleanup operation failed: unable to delete $DATA_DIR"
            return 1
        }
        log_message "Cleanup operation completed"
    else
        log_message "Data directory does not exist, no cleanup needed: $DATA_DIR"
    fi
        # Clean up data in minio
    log_message "Starting cleanup of data in minio"
    
    # Check if rclone is available
    if ! command -v rclone >/dev/null 2>&1; then
        log_message "Warning: rclone command not available, skipping minio cleanup"
        return 0
    fi
    
    # Clean up all possible minio paths
    local minio_paths=(
        "minio:db-stress/db-stress/"
    )
    
    for path in "${minio_paths[@]}"; do
        log_message "Cleaning minio path: $path"
        if rclone delete "$path" --verbose 2>/dev/null; then
            log_message "Successfully cleaned minio path: $path"
        else
            log_message "Failed to clean minio path or path does not exist: $path"
        fi
    done
    
    log_message "Minio cleanup operation completed"
}
auto_update_code() {
    log_message "Starting automatic code update..."
    
    local project_root="$(cd "$SCRIPT_DIR/.." && pwd)"
    
    log_message "Executing git pull..."
    if ! (cd "$project_root" && git pull); then
        log_message "Error: git pull failed"
        log_error "git pull failed during automatic update" "auto_update"
        log_message "Terminating program due to update failure"
        cleanup_and_exit
    fi
    
    log_message "Executing build.sh..."
    if ! (cd "$SCRIPT_DIR" && ./build.sh); then
        log_message "Error: build.sh failed"
        log_error "build.sh failed during automatic update" "auto_update"
        log_message "Terminating program due to build failure"
        cleanup_and_exit
    fi
    
    log_message "Automatic code update completed successfully"
}
# Terminate current test process
kill_current_test() {

    stop_disk_monitor

    if [ -n "$CURRENT_TEST_PID" ] && kill -0 "$CURRENT_TEST_PID" 2>/dev/null; then
        log_message "Terminating current test process $CURRENT_TEST_PID"
        kill -TERM "$CURRENT_TEST_PID"
        sleep $KILL_WAIT_TIME
        
        # If process is still running, force kill
        if kill -0 "$CURRENT_TEST_PID" 2>/dev/null; then
            log_message "Force terminating process $CURRENT_TEST_PID"
            kill -KILL "$CURRENT_TEST_PID"
            sleep 2  
        fi
        
        wait "$CURRENT_TEST_PID" 2>/dev/null
        CURRENT_TEST_PID=""
        log_message "Test process terminated"
    fi
}
# 新增：计算下次切换时间
calculate_next_switch_time() {
    local current_time=$(date '+%s')
    local switch_interval_seconds=$((SWITCH_INTERVAL_HOURS * 3600))
    echo $((current_time + switch_interval_seconds))
}

# 新增：检查是否到了切换时间
should_switch_test() {
    if [ -z "$NEXT_SWITCH_TIME" ]; then
        return 0  # 首次运行，需要开始测试
    fi
    
    local current_time=$(date '+%s')
    [ $current_time -ge $NEXT_SWITCH_TIME ]
}

# 新增：获取下一个测试类型
get_next_test_type() {
    if [ "$CURRENT_TEST_TYPE" = "whitebox" ]; then
        echo "blackbox"
    else
        echo "whitebox"
    fi
}

# Start whitebox test (continuous run until assertion error)
start_whitebox_test() {
    local duration=$((SWITCH_INTERVAL_HOURS * 3600))  # 修改：使用配置的时间间隔
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local log_file="$WHITEBOX_LOG_DIR/whitebox_${timestamp}.log"
    local param_args=$(get_random_param_combination) # Randomly select a parameter combination

    CURRENT_TEST_PARAMS="$param_args"

    log_message "Starting whitebox test, duration: ${duration} seconds (until 4 AM)"
    log_message "Parameter combination: $param_args"
    log_message "Log file: $log_file"
    
    local theoretical_usage=$(calculate_theoretical_disk_usage "$param_args")
    log_message "Theoretical disk usage: ${theoretical_usage}GB"
    # Perform cleanup operation
    perform_cleanup
    if [ -d "$DB_DIR" ] && [ "$(ls -A "$DB_DIR" 2>/dev/null)" ]; then
        log_message "Warning: DB directory not empty after cleanup, forcing removal"
        rm -rf "$DB_DIR"
        sleep 10
    fi

    start_disk_monitor "whitebox" "$theoretical_usage"

    # Start whitebox test, pass calculated duration
    setsid stdbuf -oL -eL python3 "$CRASH_TEST_PY" whitebox \
        --db_path="$DB_DIR" \
        --shared_state_path="$SHARED_STATE_DIR" \
        --kill_odds=100000000 \
        --use_random_params \
        $param_args> "$log_file" 2>&1 &
    CURRENT_TEST_PID=$!
    CURRENT_TEST_TYPE="whitebox"

    log_message "Whitebox test process PID: $CURRENT_TEST_PID"
}

# Start blackbox test
start_blackbox_test() {
    local duration=$((SWITCH_INTERVAL_HOURS * 3600))
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local log_file="$BLACKBOX_LOG_DIR/blackbox_${timestamp}.log"
    local param_args=$(get_random_param_combination)
    
    CURRENT_TEST_PARAMS="$param_args"
    log_message "Starting blackbox test, duration: ${duration} seconds (${SWITCH_INTERVAL_HOURS} hours)"
    log_message "Parameter combination: $param_args"
    log_message "Log file: $log_file"
    
    local theoretical_usage=$(calculate_theoretical_disk_usage "$param_args")
    log_message "Theoretical disk usage: ${theoretical_usage}GB"
    # Perform cleanup operation
    perform_cleanup
    
    if [ -d "$DB_DIR" ] && [ "$(ls -A "$DB_DIR" 2>/dev/null)" ]; then
        log_message "Warning: DB directory not empty after cleanup, forcing removal"
        rm -rf "$DB_DIR"
        sleep 10
    fi

    start_disk_monitor "blackbox" "$theoretical_usage"

    # Start blackbox test
    setsid stdbuf -oL -eL python3 "$CRASH_TEST_PY" blackbox \
        --db_path="$DB_DIR" \
        --shared_state_path="$SHARED_STATE_DIR" \
        --interval=3600 \
        --use_random_params \
        $param_args > "$log_file" 2>&1 &
    CURRENT_TEST_PID=$!
    CURRENT_TEST_TYPE="blackbox"


    
    log_message "Blackbox test process PID: $CURRENT_TEST_PID"
}

# Monitor current test process
monitor_current_test() {
    if [ -n "$CURRENT_TEST_PID" ]; then
        # Check if process is still running
        if ! kill -0 "$CURRENT_TEST_PID" 2>/dev/null; then
            # Process has stopped, check if it exited abnormally
            wait "$CURRENT_TEST_PID"
            local exit_code=$?
            
            if [ $exit_code -ne 0 ]; then
                # Abnormal exit, log error and restart
                local error_msg="Test process exited abnormally (PID: $CURRENT_TEST_PID, Exit code: $exit_code)"
                log_message "$error_msg"
                log_error "$error_msg" "$CURRENT_TEST_TYPE"
                
                
                # Restart same type of test
                if [ "$CURRENT_TEST_TYPE" = "whitebox" ]; then
                    log_message "Restarting whitebox test"
                    start_whitebox_test
                elif [ "$CURRENT_TEST_TYPE" = "blackbox" ]; then
                    log_message "Restarting blackbox test"
                    start_blackbox_test
                fi
            else
                log_message "Test process ended normally (PID: $CURRENT_TEST_PID)"
                
                # Record disk usage when ending normally
                local data_dir_usage=""
                
                # Check data directory disk usage
                if [ -d "$DATA_DIR" ]; then
                    data_dir_usage=$(du -sh "$DATA_DIR" 2>/dev/null || echo "Unable to get data directory size")
                else
                    data_dir_usage="Data directory does not exist"
                fi
                log_message "Disk usage when test ended normally - data directory usage: $data_dir_usage"
                
                CURRENT_TEST_PID=""
                CURRENT_TEST_TYPE=""
            fi
        fi
    fi
}


main_loop() {
    log_message "Starting initial test cycle with whitebox test"
    if [ "$AUTO_UPDATE_ENABLED" = "true" ]; then
        log_message "Auto update is enabled, updating code before test switch"
        auto_update_code
    else
        log_message "Auto update is disabled, skipping code update"
    fi
    start_whitebox_test
    NEXT_SWITCH_TIME=$(calculate_next_switch_time)  
    log_message "Next switch time: $(date -d @$NEXT_SWITCH_TIME '+%Y-%m-%d %H:%M:%S')"
    while true; do
        # Monitor current test process status
        monitor_current_test
        
        if should_switch_test && [ -n "$CURRENT_TEST_PID" ]; then
            local next_test_type=$(get_next_test_type)
            log_message "Time to switch from $CURRENT_TEST_TYPE to $next_test_type test"
            
            kill_current_test
            
            # use git pull and build.sh to update code
            if [ "$AUTO_UPDATE_ENABLED" = "true" ]; then
                log_message "Auto update is enabled, updating code before test switch"
                auto_update_code
            else
                log_message "Auto update is disabled, skipping code update"
            fi

            if [ "$next_test_type" = "whitebox" ]; then
                start_whitebox_test
            else
                start_blackbox_test
            fi
            NEXT_SWITCH_TIME=$(calculate_next_switch_time)
            log_message "Test type switched. Next switch time: $(date -d @$NEXT_SWITCH_TIME '+%Y-%m-%d %H:%M:%S')"
        fi
        
        # Check every 30 seconds
        sleep 30
    done
}

# Signal handler function
cleanup_and_exit() {
    log_message "Received exit signal, cleaning up..."
    stop_disk_monitor
    # Terminate current test
    kill_current_test
    # Kill all child processes
    jobs -p | xargs -r kill
    log_message "Night stress test script exiting"
    exit 0
}

# Set signal handlers
trap cleanup_and_exit SIGINT SIGTERM

# Check dependencies
if [ ! -f "$CRASH_TEST_PY" ]; then
    log_message "Error: crash_test.py does not exist: $CRASH_TEST_PY"
    exit 1
fi

if [ ! -f "./build.sh" ]; then
    echo "Error: build.sh does not exist: ./build.sh"
    exit 1
fi


# Check python3
if ! command -v python3 &> /dev/null; then
    log_message "Error: python3 is not installed or not in PATH"
    exit 1
fi

# Check rclone
if ! command -v rclone &> /dev/null; then
    log_message "Error: rclone is not installed or not in PATH"
    exit 1
fi

# Check docker
if ! command -v docker &> /dev/null; then
    log_message "Error: docker is not installed or not in PATH"
    exit 1
fi

# Check if minio container is running
if ! docker ps --format "table {{.Names}}" | grep -q "minio"; then
    log_message "Error: minio container is not running in docker"
    log_message "Please start minio container before running the test"
    exit 1
fi

log_message "=== Test Configuration ==="
log_message "Switch interval: ${SWITCH_INTERVAL_HOURS} hours"
log_message "Kill wait time: ${KILL_WAIT_TIME} seconds"
log_message "========================="

main_loop