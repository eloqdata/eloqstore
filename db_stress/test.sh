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


SWITCH_INTERVAL_HOURS=24 
KILL_WAIT_TIME=10

PARAM_COMBINATIONS=(
     "--data_append_mode=false"  # Combination 0: no cloud_store_path, data_append_mode=false
    "--data_append_mode=true"   # Combination 1: no cloud_store_path, data_append_mode=true
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=false"  # Combination 2: with cloud_store_path, data_append_mode=false
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=true"   # Combination 3: with cloud_store_path, data_append_mode=true
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=false"  # Combination 4: with cloud_store_path, data_append_mode=false
    "--cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --data_append_mode=true"   # Combination 5: with cloud_store_path, data_append_mode=true
)
# Create log directories
mkdir -p "$WHITEBOX_LOG_DIR"
mkdir -p "$BLACKBOX_LOG_DIR"
mkdir -p "$ERROR_LOG_DIR"

CURRENT_TEST_PID=""
CURRENT_TEST_TYPE=""
LAST_STATUS=""  # Added: record last status to avoid duplicate logs

NEXT_SWITCH_TIME=""
# Log function
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}
# Randomly select parameter combination
get_random_param_combination() {
    # Get array length, then use modulo to get random index
    local combination_count=${#PARAM_COMBINATIONS[@]}
    local random_index=$((RANDOM % combination_count))
    echo "${PARAM_COMBINATIONS[$random_index]}"
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

    log_message "Starting whitebox test, duration: ${duration} seconds (until 4 AM)"
    log_message "Parameter combination: $param_args"
    log_message "Log file: $log_file"
    
    # Perform cleanup operation
    perform_cleanup
    
    # Start whitebox test, pass calculated duration
    setsid stdbuf -oL -eL python3 "$CRASH_TEST_PY" whitebox \
        --db_path="$DB_DIR" \
        --shared_state_path="$SHARED_STATE_DIR" \
        --duration=$duration \
        --kill_odds=100000000 \
        --n_tables=10 \
        --n_partitions=100\
        --max_key=10000 \
        --shortest_value=32 \
        --longest_value=2000 \
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
    
    log_message "Starting blackbox test, duration: ${duration} seconds (${SWITCH_INTERVAL_HOURS} hours)"
    log_message "Parameter combination: $param_args"
    log_message "Log file: $log_file"
    
    # Perform cleanup operation
    perform_cleanup
    
    # Start blackbox test
    setsid stdbuf -oL -eL python3 "$CRASH_TEST_PY" blackbox \
        --db_path="$DB_DIR" \
        --shared_state_path="$SHARED_STATE_DIR" \
        --duration=$duration \
        --interval=3600 \
        --n_tables=10 \
        --max_key=10000 \
        --n_partitions=100\
        --shortest_value=32 \
        --longest_value=2000 \
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
    auto_update_code
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
            auto_update_code

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