#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/log"

DATA_DIR="$SCRIPT_DIR/data1"
DB_DIR="$DATA_DIR/db_stress"
SHARED_STATE_DIR="$DATA_DIR/shared_state"
WHITEBOX_LOG_DIR="$LOG_DIR/whitebox"
BLACKBOX_LOG_DIR="$LOG_DIR/blackbox"
ERROR_LOG_DIR="$LOG_DIR/errors"
CRASH_TEST_PY="$SCRIPT_DIR/crash_test.py"


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

# Terminate current test process
kill_current_test() {
    if [ -n "$CURRENT_TEST_PID" ] && kill -0 "$CURRENT_TEST_PID" 2>/dev/null; then
        log_message "Terminating current test process $CURRENT_TEST_PID"
        kill -TERM "$CURRENT_TEST_PID"
        sleep 5
        
        # If process is still running, force kill
        if kill -0 "$CURRENT_TEST_PID" 2>/dev/null; then
            log_message "Force terminating process $CURRENT_TEST_PID"
            kill -KILL "$CURRENT_TEST_PID"
        fi
        
        wait "$CURRENT_TEST_PID" 2>/dev/null
        CURRENT_TEST_PID=""
        log_message "Test process terminated"
    fi
}
# Calculate remaining seconds to specified hour
calculate_duration_to_hour() {
    local target_hour=$1
    local current_time=$(date '+%s')
    local current_hour=$(date '+%H' | sed 's/^0//')
    
    # Calculate today's target time timestamp
    local today_target=$(date -d "today ${target_hour}:00:00" '+%s')
    
    # If target time has passed, calculate tomorrow's target time
    if [ $current_time -gt $today_target ]; then
        local tomorrow_target=$(date -d "tomorrow ${target_hour}:00:00" '+%s')
        echo $((tomorrow_target - current_time))
    else
        echo $((today_target - current_time))
    fi
}
# Start whitebox test (continuous run until assertion error)
start_whitebox_test() {
    local duration=$(calculate_duration_to_hour 4)
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local log_file="$WHITEBOX_LOG_DIR/whitebox_${timestamp}.log"
    local param_args=$(get_random_param_combination) # Randomly select a parameter combination

    log_message "Starting whitebox test, duration: ${duration} seconds (until 4 AM)"
    log_message "Parameter combination: $param_args"
    log_message "Log file: $log_file"
    
    # Perform cleanup operation
    perform_cleanup
    
    # Start whitebox test, pass calculated duration
    stdbuf -oL -eL python3 "$CRASH_TEST_PY" whitebox \
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
    local duration=$(calculate_duration_to_hour 10)
    local timestamp=$(date '+%Y%m%d_%H%M%S')
    local log_file="$BLACKBOX_LOG_DIR/blackbox_${timestamp}.log"
    local param_args=$(get_random_param_combination)
    
    log_message "Starting blackbox test, duration: ${duration} seconds (until 10 AM)"
    log_message "Parameter combination: $param_args"
    log_message "Log file: $log_file"
    
    # Perform cleanup operation
    perform_cleanup
    
    # Start blackbox test
    stdbuf -oL -eL python3 "$CRASH_TEST_PY" blackbox \
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

# Get current hour
get_current_hour() {
    date '+%H' | sed 's/^0//'
}

# Main loop
main_loop() {
    while true; do
        current_hour=$(get_current_hour)
        # Monitor current test process status
        monitor_current_test
        # Determine what state should be current
        local expected_status="" 
        # 20 / 4    4/10 original
        if [ $current_hour -ge 11 ] || [ $current_hour -lt 4 ]; then
            expected_status="whitebox"
        elif [ $current_hour -ge 4 ] && [ $current_hour -lt 11 ]; then
            expected_status="blackbox"
        else
            expected_status="rest"
        fi
        # Only output logs and execute operations when status changes
        if [ "$expected_status" != "$LAST_STATUS" ]; then
            case "$expected_status" in
                "whitebox")
                    if [ "$CURRENT_TEST_TYPE" != "whitebox" ]; then
                        log_message "Current time: ${current_hour} o'clock, switching to whitebox test period"
                        if [ -n "$CURRENT_TEST_PID" ]; then
                            kill_current_test
                        fi
                        start_whitebox_test
                    fi
                    ;;
                "blackbox")
                    if [ "$CURRENT_TEST_TYPE" != "blackbox" ]; then
                        log_message "Current time: ${current_hour} o'clock, switching to blackbox test period"
                        if [ -n "$CURRENT_TEST_PID" ]; then
                            kill_current_test
                        fi
                        start_blackbox_test
                    fi
                    ;;
                "rest")
                    if [ -n "$CURRENT_TEST_PID" ]; then
                        log_message "Current time: ${current_hour} o'clock, entering rest time, terminating current test"
                        kill_current_test
                    fi
                    log_message "Current time: ${current_hour} o'clock, rest time, waiting for next test period"
                    ;;
            esac  # case reversed
            LAST_STATUS="$expected_status"
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

main_loop