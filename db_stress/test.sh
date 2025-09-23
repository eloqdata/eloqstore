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

DISK_LOG_FILE="$LOG_DIR/disk_usage.log"


# no start cloud if it is empty
MINIO_DATA_PATH=""
#MINIO_DATA_PATH="/home/sjh/minio/data"
DISK_MONITOR_PID=""

SWITCH_INTERVAL_HOURS=1
KILL_WAIT_TIME=10

CURRENT_TEST_PARAMS=""

AUTO_UPDATE_ENABLED=false  # auto pull and build new code
CRASH_TEST_ENABLED=false

# Create log directories
mkdir -p "$WHITEBOX_LOG_DIR"
mkdir -p "$BLACKBOX_LOG_DIR"
mkdir -p "$ERROR_LOG_DIR"
mkdir -p "$DISK_LOG_DIR"

CURRENT_TEST_PID=""
CURRENT_TEST_TYPE=""
LAST_STATUS=""  # A--num_threads=1e --throughput_report_interval_secs=10 d: record last status to avoid duplicate logs

NEXT_SWITCH_TIME=""
# Log function
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}
PARAM_INDEX_FILE="$SCRIPT_DIR/log/current_param_index"
SYSTEM_TYPE_PARAM_COMBINATIONS=(

    " --data_append_mode=true --cloud_store_path=minio:db-stress/db-stress/  --fd_limit=1000 --num_gc_threads=1 --num_threads=2  --throughput_report_interval_secs=10 --n_tables=2 --n_partitions=2 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=4000 --max_verify_ops_per_write=0 --write_percent=100"
    # "--data_append_mode=true --cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --num_threads=4   --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"
    # "--data_append_mode=true --cloud_store_path=minio:db-stress/db-stress/ --num_gc_threads=0 --num_threads=4   --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"

    #"--data_append_mode=false --num_threads=1  --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"
    "--data_append_mode=false --num_threads=8  --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"
    "--data_append_mode=false --num_threads=32 --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"
    
    "--data_append_mode=true --num_threads=1 --file_amplify_factor=2  --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"
    "--data_append_mode=true --num_threads=8  --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"
    "--data_append_mode=true --num_threads=32 --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"

    # 压缩因子的影响
    "--data_append_mode=true  --file_amplify_factor=2 --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"
    "--data_append_mode=true  --file_amplify_factor=3 --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"
    "--data_append_mode=true  --file_amplify_factor=4 --throughput_report_interval_secs=10 --n_tables=10 --n_partitions=10 --max_key=10000 --shortest_value=1024 --longest_value=40960 --active_width=10000 --keys_per_batch=2000 --max_verify_ops_per_write=0 --write_percent=100"
)
calculate_theoretical_disk_usage() {
    local param_args="$1"
    
    local n_tables=$(echo "$param_args" | grep -o "\--n_tables=[0-9]*" | cut -d'=' -f2)
    local n_partitions=$(echo "$param_args" | grep -o "\--n_partitions=[0-9]*" | cut -d'=' -f2)
    local max_key=$(echo "$param_args" | grep -o "\--max_key=[0-9]*" | cut -d'=' -f2)
    local shortest_value=$(echo "$param_args" | grep -o "\--shortest_value=[0-9]*" | cut -d'=' -f2)
    local longest_value=$(echo "$param_args" | grep -o "\--longest_value=[0-9]*" | cut -d'=' -f2)
    
    n_tables=${n_tables:-1}
    n_partitions=${n_partitions:-1}
    max_key=${max_key:-1000000}
    shortest_value=${shortest_value:-32}
    longest_value=${longest_value:-32}
    
    #  n_table * n_partition * max_key * (12 + (shortest_value + longest_value) / 2) * 0.75
    local avg_value=$(( (shortest_value + longest_value) / 2 ))
    local key_overhead=12
    local usage_ratio=0.75
    
    local theoretical_bytes=$(echo "$n_tables * $n_partitions * $max_key * ($key_overhead + $avg_value) * $usage_ratio" | bc -l)
    
    local theoretical_gb=$(echo "scale=2; $theoretical_bytes / 1024 / 1024 / 1024" | bc -l)
    
    echo "$theoretical_gb"
}
start_disk_monitor() {
    local test_type="$1"
    local theoretical_usage="$2"
    
    stop_disk_monitor
    {
        echo ""
        echo ""
        echo ""
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting $test_type test"
        echo "Theoretical disk usage: ${theoretical_usage}GB"
        echo "Actual disk usage monitoring (every 1 minute):"
    } >> "$DISK_LOG_FILE"
    
    (
        while true; do
            sleep 30  
            
            local current_usage=""
            if [ -d "$DB_DIR" ]; then
                current_usage=$(du -sb "$DB_DIR" 2>/dev/null | awk '{printf "%.2f", $1/1024/1024/1024}')
                if [ -z "$current_usage" ]; then
                    current_usage="0.00"
                fi
            else
                current_usage="0.00"
            fi
            
            local minio_usage=""
            local using_minio=false
            

            if [ -n "$MINIO_DATA_PATH" ] && [ -n "$CURRENT_TEST_PARAMS" ] && echo "$CURRENT_TEST_PARAMS" | grep -q "cloud_store_path"; then
                using_minio=true
                
                if [ -d "$MINIO_DATA_PATH" ]; then
                    minio_usage=$(du -sb "$MINIO_DATA_PATH" 2>/dev/null | awk '{printf "%.2f", $1/1024/1024/1024}')
                    if [ -z "$minio_usage" ]; then
                        minio_usage="0.00"
                    fi
                else
                    minio_usage="0.00"
                fi
            fi
            
            if [ "$using_minio" = true ]; then
                echo -n "${current_usage}GB/${minio_usage}GB," >> "$DISK_LOG_FILE"
            else
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

        if kill -0 "$DISK_MONITOR_PID" 2>/dev/null; then
            kill -KILL "$DISK_MONITOR_PID" 2>/dev/null
        fi
        
        wait "$DISK_MONITOR_PID" 2>/dev/null
        DISK_MONITOR_PID=""
        
        echo "" >> "$DISK_LOG_FILE"
        log_message "Disk monitor stopped"
    fi
}
CURRENT_PARAM_INDEX=0

get_sequential_param_combination() {
    if [ -f "$PARAM_INDEX_FILE" ]; then
        CURRENT_PARAM_INDEX=$(cat "$PARAM_INDEX_FILE")
    else
        CURRENT_PARAM_INDEX=0
    fi
    
    local param_count=${#SYSTEM_TYPE_PARAM_COMBINATIONS[@]}
    
    local current_params="${SYSTEM_TYPE_PARAM_COMBINATIONS[$CURRENT_PARAM_INDEX]}"
    
    local param_type=$((CURRENT_PARAM_INDEX + 1))
    log_message "Selected parameter combination: $param_type/$param_count" >&2
    
    CURRENT_PARAM_INDEX=$(((CURRENT_PARAM_INDEX + 1) % param_count))
    
    echo "$CURRENT_PARAM_INDEX" > "$PARAM_INDEX_FILE"
    
    echo "$current_params"
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
    
    if [ -n "$MINIO_DATA_PATH" ]; then
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
    else
        log_message "MINIO_DATA_PATH is empty, skipping minio cleanup"
    fi
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
calculate_next_switch_time() {
    local current_time=$(date '+%s')
    local switch_interval_seconds=$((SWITCH_INTERVAL_HOURS * 3600))
    echo $((current_time + switch_interval_seconds))
}

should_switch_test() {
    if [ -z "$NEXT_SWITCH_TIME" ]; then
        return 0  
    fi
    
    local current_time=$(date '+%s')
    [ $current_time -ge $NEXT_SWITCH_TIME ]
}

get_next_test_type() {
    if [ "$CRASH_TEST_ENABLED" = "false" ]; then
        echo "whitebox"  
    elif [ "$CURRENT_TEST_TYPE" = "whitebox" ]; then
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
    local param_args=$(get_sequential_param_combination) # Randomly select a parameter combination

    CURRENT_TEST_PARAMS="$param_args"

    local kill_odds_param
    local open_wfile_param
    
    if [ "$CRASH_TEST_ENABLED" = "false" ]; then
        kill_odds_param="--kill_odds=0"
        open_wfile_param="--open_wfile=0"
        log_message "Starting whitebox test (crash test disabled), duration: ${duration} seconds"
    else
        kill_odds_param="--kill_odds=100000000"
        open_wfile_param=""  
        log_message "Starting whitebox test (crash test enabled), duration: ${duration} seconds"
    fi
    
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
    # 注意这边随机参数被删掉了
    setsid stdbuf -oL -eL python3 "$CRASH_TEST_PY" whitebox \
        --db_path="$DB_DIR" \
        --shared_state_path="$SHARED_STATE_DIR" \
        $kill_odds_param \
        $open_wfile_param \
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
    local param_args=$(get_sequential_param_combination)
    
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


# Check python3
if ! command -v python3 &> /dev/null; then
    log_message "Error: python3 is not installed or not in PATH"
    exit 1
fi
# Check bc for floating point calculations
if ! command -v bc &> /dev/null; then
    log_message "Error: bc is not installed or not in PATH (required for disk usage calculations)"
    exit 1
fi
# 只有在MINIO_DATA_PATH不为空时才检查rclone和docker
if [ -n "$MINIO_DATA_PATH" ]; then
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
else
    log_message "MINIO_DATA_PATH is empty, skipping rclone and docker dependency checks"
fi

log_message "=== Test Configuration ==="
log_message "Switch interval: ${SWITCH_INTERVAL_HOURS} hours"
log_message "Kill wait time: ${KILL_WAIT_TIME} seconds"
log_message "========================="

main_loop