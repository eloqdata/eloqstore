#!/usr/bin/env python3
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.

import argparse
import math
import os
import random
import shutil
import subprocess
import sys
import tempfile
import time
import csv
import glob
import resource
from datetime import datetime

# 添加core文件管理函数
def setup_core_dump():
    """设置core dump环境"""
    # 设置core文件大小限制为无限制
    os.system("ulimit -c unlimited")
    
    # 设置core文件命名格式（包含进程ID和时间戳）
    core_pattern = "./log/core/core.%e.%p.%t"
    os.makedirs("./log/core", exist_ok=True)

    print(f"当前core文件限制: {resource.getrlimit(resource.RLIMIT_CORE)}")
    # 尝试设置core文件模式（需要root权限，可能失败）
    try:
        with open('/proc/sys/kernel/core_pattern', 'w') as f:
            f.write(core_pattern)
    except PermissionError:
        print("警告: 无法设置全局core_pattern,使用默认设置")
        # 使用当前目录的core文件
        pass
def get_recent_core_files(pid, start_time):
    """获取指定进程在指定时间后生成的core文件"""
    core_patterns = [
        f"./log/core/core.db_stress.{pid}.*",
        f"./core.{pid}",
        f"./core",
        f"/tmp/core.{pid}",
        f"/var/crash/core.{pid}.*"
    ]
    
    recent_cores = []
    for pattern in core_patterns:
        cores = glob.glob(pattern)
        for core_file in cores:
            try:
                # 检查文件修改时间是否在测试开始之后
                if os.path.getmtime(core_file) >= start_time:
                    recent_cores.append(core_file)
            except OSError:
                continue
    
    return recent_cores
def manage_core_files(pid, start_time, exit_code, hit_timeout, test_type):
    """根据退出码管理core文件"""
    recent_cores = get_recent_core_files(pid, start_time)
    
    # 判断是否应该保留core文件
    should_keep_core = False
    reason = ""
    
    if exit_code == -6:  # SIGABRT - 断言失败
        should_keep_core = True
        reason = "断言失败"
    elif exit_code == -15:  # SIGTERM - 正常被kill或埋点死亡
        should_keep_core = False
        reason = "正常被kill或埋点死亡"
    elif exit_code == -9:  # SIGKILL - 超时被强制kill
        should_keep_core = False
        reason = "超时被强制kill"
    elif hit_timeout:
        should_keep_core = False
        reason = "测试超时"
    else:
        # 其他异常退出码，保留core文件用于调试
        should_keep_core = True
        reason = f"异常退出码: {exit_code}"
    
    if recent_cores:
        if should_keep_core:
            # 重命名core文件，添加时间戳和原因
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            for i, core_file in enumerate(recent_cores):
                new_name = f"./log/core/{test_type}_core_{timestamp}_{exit_code}_{i}.core"
                try:
                    os.rename(core_file, new_name)
                    print(f"保留core文件: {new_name} (原因: {reason})")
                except OSError as e:
                    print(f"重命名core文件失败: {e}")
        else:
            # 删除不需要的core文件
            for core_file in recent_cores:
                try:
                    os.remove(core_file)
                    print(f"删除core文件: {core_file} (原因: {reason})")
                except OSError as e:
                    print(f"删除core文件失败: {e}")
    else:
        if should_keep_core:
            print(f"预期生成core文件但未找到 (原因: {reason})")
        else:
            print(f"未生成core文件 (原因: {reason})")
def randomize_dynamic_params():
    return {
        #"num_threads": random.choice([1, 2, 4, 8, 16]),
        "num_threads": random.choice([1]),
        "data_page_restart_interval": random.choice([8, 16, 24, 32]),
        "index_page_restart_interval": random.choice([8, 16, 24, 32]),
        # "skip_verify_checksum": random.choice([True, False]),
        "index_buffer_pool_size": random.choice([1<<14, 1<<15, 1<<16]),
        "fd_limit": random.choice([5000, 10000, 15000]),
        "io_queue_size": random.choice([2048, 4096, 8192]),
        "max_inflight_write": random.choice([2048, 4096, 8192]),
        "max_write_batch_pages": random.choice([32, 64, 128]),
        "buf_ring_size": lambda: random.choice([1<<9, 1<<10, 1<<11, 1<<12]),
        # "coroutine_stack_size": lambda: random.choice([1<<13, 1<<14, 1<<15]),
        "file_amplify_factor": random.choice([2, 4, 6, 8]),
        # "num_gc_threads": random.choice([1, 2, 4]),
        "reserve_space_ratio": random.choice([50, 100, 150, 200]),
        "rclone_threads": random.choice([1, 2, 4]),
    }

def init_csv_file(test_type):
    """初始化CSV文件,如果不存在则创建并写入表头"""
    csv_dir = "./log/csv"
    os.makedirs(csv_dir, exist_ok=True)
    filename = os.path.join(csv_dir, f"{test_type}_test_records.csv")
    # 检查文件是否存在
    file_exists = os.path.isfile(filename)
    
    if not file_exists:
        # 创建文件并写入表头
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = [
                'start_time', 'end_time', 'duration_seconds', 'exit_code', 'hit_timeout',
                'num_threads', 'data_page_restart_interval', 'index_page_restart_interval',
                'index_buffer_pool_size', 'fd_limit', 'io_queue_size', 'max_inflight_write',
                'max_write_batch_pages', 'buf_ring_size', 'file_amplify_factor',
                'reserve_space_ratio', 'rclone_threads', 'open_wfile'
            ]
            
            # 根据测试类型添加特定字段
            if test_type == 'blackbox':
                fieldnames.append('interval')
            elif test_type == 'whitebox':
                fieldnames.append('kill_odds')
            
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
    
    return filename

def record_test_result(test_type, start_time, end_time, exit_code, hit_timeout, cmd_params, extra_params=None):
    """记录测试结果到CSV文件"""
    filename = init_csv_file(test_type)
    
    # 计算运行时间
    duration_seconds = round(end_time - start_time, 2)
    
    # 准备记录数据
    record = {
        'start_time': datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S'),
        'end_time': datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S'),
        'duration_seconds': duration_seconds,
        'exit_code': exit_code,
        'hit_timeout': hit_timeout,
        'num_threads': cmd_params.get('num_threads', ''),
        'data_page_restart_interval': cmd_params.get('data_page_restart_interval', ''),
        'index_page_restart_interval': cmd_params.get('index_page_restart_interval', ''),
        'index_buffer_pool_size': cmd_params.get('index_buffer_pool_size', ''),
        'fd_limit': cmd_params.get('fd_limit', ''),
        'io_queue_size': cmd_params.get('io_queue_size', ''),
        'max_inflight_write': cmd_params.get('max_inflight_write', ''),
        'max_write_batch_pages': cmd_params.get('max_write_batch_pages', ''),
        'buf_ring_size': cmd_params.get('buf_ring_size', ''),
        'file_amplify_factor': cmd_params.get('file_amplify_factor', ''),
        'reserve_space_ratio': cmd_params.get('reserve_space_ratio', ''),
        'rclone_threads': cmd_params.get('rclone_threads', ''),
        'open_wfile': cmd_params.get('open_wfile', '')
    }
    
    # 添加额外参数
    if extra_params:
        record.update(extra_params)
    
    # 追加到CSV文件
    with open(filename, 'a', newline='', encoding='utf-8') as csvfile:
        fieldnames = list(record.keys())
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writerow(record)
    
    print(f"测试记录已保存到 {filename}")

default_params = {
    "num_threads" : 4,#default 1
    "data_page_size" : lambda:1 << 12,#default 1<<12
    "data_page_restart_interval" : 16,# default 16
    "index_page_restart_interval" : 16,#default 16
    # "index_page_read_queue" : 1024,#default 1024
    "index_buffer_pool_size" : 0xFFFFFFFF,#default UINT32_MAX
    "init_page_count" : 1 << 15,#default 1<<15
    "pages_per_file_shift" : lambda:11,   #default 11  
    "manifest_limit" : lambda:16 <<20,#default 8<<20
    "fd_limit" : 10000,#default 10000
    "io_queue_size" : 4096,#default 4096
    "buf_ring_size" :lambda:1 << 10,#default 1<<10
    "coroutine_stack_size" :lambda:1<<14,#default 8*1024
    "max_inflight_write":4096,#default 4096
    "file_amplify_factor":4,#default 4
    "num_gc_threads":1,#default 1
    "local_space_limit":1<<40,#default 1<<40
    "reserve_space_ratio":100,#default 100
    "rclone_threads":1,#default 1
    "overflow_pointers":16,#default 16
    "open_wfile":True
}

stress_cmd = "../build/db_stress/db_stress"
cleanup_cmd = "../build/db_stress/clean_up"


# 总测试持续时间,和每次crash间隔时间,考虑去除持续时间
blackbox_default_params = {
    "duration": 3600,
    "interval": 600,
}

whitebox_default_params={
    "duration":3600,
    "kill_odds":800000,
}

# 800000大概一分钟以内会结束
# 数字越小,越容易被杀

def finalize_and_sanitize(src_params):
    dest_params = {k: v() if callable(v) else v for (k, v) in src_params.items()}
    return dest_params


def gen_cmd_params(args, use_random_params=False):
    params = {}

    params.update(default_params)

    # 如果启用随机参数，则覆盖部分默认参数
    if use_random_params:
        random_params = randomize_dynamic_params()
        params.update(random_params)
        print(f"使用随机参数组合: {random_params}")

    if args.test_type == "blackbox":
        params.update(blackbox_default_params)
    if args.test_type == "whitebox":
        params.update(whitebox_default_params)

    for k, v in vars(args).items():
        if v is not None:
            params[k] = v
    return params


def gen_cmd(params, unknown_params):
    finalzied_params = finalize_and_sanitize(params)
    cmd = (
        [stress_cmd]
        + [
            f"--{k}={v}"
            for k, v in [(k, finalzied_params[k]) for k in sorted(finalzied_params)]
            if k
            not in {
                "test_type",
                "simple",
                "duration",
                "interval",
                "cf_consistency",
                "txn",
                "optimistic_txn",
                "test_best_efforts_recovery",
                "enable_ts",
                "test_multiops_txn",
                "write_policy",
                "stress_cmd",
                "test_tiered_storage",
                "cleanup_cmd",
                "skip_tmpdir_check",
                "print_stderr_separately",
                "verify_timeout",
                "use_random_params",
            }
            and v is not None
        ]
        + unknown_params
    )
    return cmd


def execute_cmd(cmd, timeout=None, timeout_pstack=False):
    # 在启动进程前设置core dump
    setup_core_dump()
    child = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    # child = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr)

    print("Running db_stress with pid=%d: %s\n" % (child.pid, " ".join(cmd)))
    print("This process will be killed manually after interval=",timeout)
    pid = child.pid
    start_time = time.time()
    try:
        outs, errs = child.communicate(timeout=timeout)
        hit_timeout = False
        print("WARNING: db_stress ended before kill: exitcode=%d\n" % child.returncode)
    except subprocess.TimeoutExpired:
        hit_timeout = True
        if timeout_pstack:
            os.system("pstack %d" % pid)
        child.kill()
        print("\nkilled process manually, pid=%d\n" % child.pid)
        outs, errs = child.communicate()

    return hit_timeout, child.returncode, outs.decode("utf-8"), errs.decode("utf-8"), pid, start_time


def print_output_and_exit_on_error(stdout, stderr, print_stderr_separately=False):
    if len(stdout)>0:
        print("stdout:\n", stdout)
    else:
        print("stdout:None")
    if len(stderr) == 0:
        print("stderr:None\n")
        return

    if print_stderr_separately:
        print("stderr:\n", stderr, file=sys.stderr)
    else:
        print("stderr:\n", stderr)


def cleanup_after_success():
    #shutil.rmtree(dbname, True)
    if cleanup_cmd is not None:
        print("Running DB cleanup command - %s\n" % cleanup_cmd)
        ret = os.system(cleanup_cmd)
        if ret != 0:
            print("WARNING: DB cleanup returned error %d\n" % ret)


# This script runs and kills db_stress multiple times. It checks consistency
# in case of unsafe crashes in RocksDB.
def blackbox_crash_main(args, unknown_args):
#    dbname = get_dbname("blackbox")
    start_time=time.time()
    # exit_time = time.time() + args.duration if hasattr(args, 'duration') else time.time() + 3600
    exit_time = time.time() + args.duration if args.duration is not None else time.time() + 3600


    while time.time() < exit_time:
        cmd_params = gen_cmd_params(args, use_random_params=args.use_random_params)
        cmd = gen_cmd(
            dict(list(cmd_params.items())), unknown_args
        )
        process_rate=round((time.time()-start_time)/cmd_params["duration"]*100,2)
        print("crash_test process rate:",process_rate,"%")
        interval=random.randint(360, cmd_params["interval"])

        # 记录单次测试开始时间
        test_start_time = time.time()
        hit_timeout, retcode, outs, errs, pid, cmd_start_time = execute_cmd(cmd, interval)
        test_end_time = time.time()
        # 管理core文件
        manage_core_files(pid, cmd_start_time, retcode, hit_timeout, 'blackbox')
        # 记录测试结果到CSV
        record_test_result(
            'blackbox', 
            test_start_time, 
            test_end_time, 
            retcode, 
            hit_timeout, 
            cmd_params,
            {'interval': interval}
        )

        if not hit_timeout:
            print("Exit Before Killing")
            print_output_and_exit_on_error(outs, errs, args.print_stderr_separately)
            sys.exit(2)

        print_output_and_exit_on_error(outs, errs, args.print_stderr_separately)
        print("\n\n\n")

        time.sleep(1)  # time to stabilize before the next run

        time.sleep(1)  # time to stabilize before the next run
    print("blackbox crash test has succeeded!!!")
    # we need to clean up after ourselves -- only do this on test success
    cleanup_after_success()


def whitebox_crash_main(args, unknown_args):
    # cmd_params = gen_cmd_params(args)
    cur_time = time.time()
    # exit_time = time.time() + args.duration if hasattr(args, 'duration') else time.time() + 3600
    exit_time = time.time() + args.duration if args.duration is not None else time.time() + 3600

    succeeded = True
    hit_timeout = False
    start_time=time.time()
    while time.time() < exit_time:
        cmd_params = gen_cmd_params(args, use_random_params=args.use_random_params)
        process_rate=round((time.time()-start_time)/cmd_params["duration"]*100,2)
        print("crash_test process rate:",process_rate,"%")
        # 每次循环重新随机生成kill_odds
        kill_odds_upper = cmd_params["kill_odds"]
        kill_odds_lower = 400000
        odd = random.randint(kill_odds_lower, kill_odds_upper)
        
        cmd = gen_cmd(
            dict(
                list(cmd_params.items()) + [("kill_odds", odd)]
            ),
            unknown_args,
        )
        # 记录单次测试开始时间
        test_start_time = time.time()
        # hit_timeout, retcode, stdoutdata, stderrdata = execute_cmd(
        #     cmd, exit_time - time.time() + 900
        # )
        hit_timeout, retcode, stdoutdata, stderrdata, pid, cmd_start_time = execute_cmd(
            cmd, exit_time - time.time() + 900
        )
        test_end_time = time.time()

        # 管理core文件
        manage_core_files(pid, cmd_start_time, retcode, hit_timeout, 'whitebox')
        # 记录测试结果到CSV
        record_test_result(
            'whitebox', 
            test_start_time, 
            test_end_time, 
            retcode, 
            hit_timeout, 
            cmd_params,
            {'kill_odds': odd}
        )
        msg = "kill_odds=1/{}, exitcode={}\n".format(
                 odd,   retcode
        )
        print(msg)
        print_output_and_exit_on_error(
            stdoutdata, stderrdata, args.print_stderr_separately
        )
        if hit_timeout:
            print("Killing the run for running too long")
            break

        succeeded = False
        if odd>0 and (retcode == -15):
            succeeded = True
            print("killed process at KillPoint successfully\n\n\n\n")

        if not succeeded:
            # 测试失败,请查看上方的 kill 选项和退出码！
            print("TEST FAILED. See kill option and exit code above!!!\n")
            sys.exit(1)

        time.sleep(1)  # time to stabilize after a kill

    print("whitebox crash test has succeeded!!!")

    # Clean up after ourselves
    if succeeded or hit_timeout:
        cleanup_after_success()


def main():
    global stress_cmd
    global cleanup_cmd

    parser = argparse.ArgumentParser(
        description="This script runs and kills \
        db_stress multiple times"
    )
    parser.add_argument("test_type", choices=["blackbox", "whitebox"])
    parser.add_argument("--simple", action="store_true")
    parser.add_argument("--cf_consistency", action="store_true")
    parser.add_argument("--txn", action="store_true")
    parser.add_argument("--optimistic_txn", action="store_true")
    parser.add_argument("--test_best_efforts_recovery", action="store_true")
    parser.add_argument("--enable_ts", action="store_true")
    parser.add_argument("--test_multiops_txn", action="store_true")
    parser.add_argument("--write_policy", choices=["write_committed", "write_prepared"])
    parser.add_argument("--stress_cmd")
    parser.add_argument("--test_tiered_storage", action="store_true")
    parser.add_argument("--cleanup_cmd")
    parser.add_argument("--skip_tmpdir_check", action="store_true")
    parser.add_argument("--print_stderr_separately", action="store_true", default=False)
    parser.add_argument("--use_random_params", action="store_true")
    all_params = dict(
        list(default_params.items())
        + list(blackbox_default_params.items())
        +list(whitebox_default_params.items())
    )

    for k, v in all_params.items():
        parser.add_argument("--" + k, type=type(v() if callable(v) else v))
    # unknown_args are passed directly to db_stress
    args, unknown_args = parser.parse_known_args()

    if args.stress_cmd:
        stress_cmd = args.stress_cmd
    if args.cleanup_cmd:
        cleanup_cmd = args.cleanup_cmd
    if args.test_type == "blackbox":
        blackbox_crash_main(args, unknown_args)
    if args.test_type == "whitebox":
        whitebox_crash_main(args,unknown_args)


if __name__ == "__main__":
    main()