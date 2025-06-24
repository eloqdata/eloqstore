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

# params overwrite priority:
#   for default:
#       default_params < {blackbox,whitebox}_default_params < args
#   for simple:
#       default_params < {blackbox,whitebox}_default_params <
#       simple_default_params <
#       {blackbox,whitebox}_simple_default_params < args
#   for cf_consistency:
#       default_params < {blackbox,whitebox}_default_params <
#       cf_consistency_params < args
#   for txn:
#       default_params < {blackbox,whitebox}_default_params < txn_params < args
#   for ts:
#       default_params < {blackbox,whitebox}_default_params < ts_params < args
#   for multiops_txn:
#       default_params < {blackbox,whitebox}_default_params < multiops_txn_params < args


default_params = {
    "num_threads" : 4,#default 1
    "data_page_size" : lambda:1 << 12,#default 1<<12
    "data_page_restart_interval" : 16,# default 16
    "index_page_restart_interval" : 16,#default 16
    "index_page_read_queue" : 1024,#default 1024
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

blackbox_default_params = {
    "duration": 6000,
    "interval":random.randint(60,90),
}

whitebox_default_params={
    "duration":3000,
    "kill_odds":800000,
}



def finalize_and_sanitize(src_params):
    dest_params = {k: v() if callable(v) else v for (k, v) in src_params.items()}
    return dest_params


def gen_cmd_params(args):
    params = {}

    params.update(default_params)
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
            }
            and v is not None
        ]
        + unknown_params
    )
    return cmd


def execute_cmd(cmd, timeout=None, timeout_pstack=False):
    child = subprocess.Popen(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    print("Running db_stress with pid=%d: %s\n" % (child.pid, " ".join(cmd)))
    print("This process will be killed manually after interval=",timeout)
    pid = child.pid

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

    return hit_timeout, child.returncode, outs.decode("utf-8"), errs.decode("utf-8")


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
    cmd_params = gen_cmd_params(args)
#    dbname = get_dbname("blackbox")
    start_time=time.time()
    exit_time = time.time() + cmd_params["duration"]

    print(
        "Running blackbox-crash-test with \n"
        + "total-duration="
        + str(cmd_params["duration"])
        + "\n"
    )

    while time.time() < exit_time:
        cmd = gen_cmd(
            dict(list(cmd_params.items())), unknown_args
        )
        process_rate=round((time.time()-start_time)/cmd_params["duration"]*100,2)
        print("crash_test process rate:",process_rate,"%")
        interval=random.randint(30,40)
        hit_timeout, retcode, outs, errs = execute_cmd(cmd, interval)

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
    cmd_params = gen_cmd_params(args)
    #dbname = get_dbname("whitebox")

    cur_time = time.time()
    exit_time = cur_time + cmd_params["duration"]

    print(
        "Running whitebox-crash-test with \n"
        + "total-duration="
        + str(cmd_params["duration"])
        + "\n"
    )
    odd=cmd_params["kill_odds"]
    succeeded = True
    hit_timeout = False
    start_time=time.time()
    while time.time() < exit_time:
        process_rate=round((time.time()-start_time)/cmd_params["duration"]*100,2)
        print("crash_test process rate:",process_rate,"%")
        cmd = gen_cmd(
            dict(
                list(cmd_params.items())
            ),
            unknown_args,
        )

        hit_timeout, retncode, stdoutdata, stderrdata = execute_cmd(
            cmd, exit_time - time.time() + 900
        )
        msg = " kill_odds=1/{}, exitcode={}\n".format(
                 odd,   retncode
        )

        print(msg)
        print_output_and_exit_on_error(
            stdoutdata, stderrdata, args.print_stderr_separately
        )

        if hit_timeout:
            print("Killing the run for running too long")
            break

        succeeded = False
        if odd>0 and (retncode == -15):
            succeeded = True
            print("killed process at KillPoint successfully\n\n\n\n")

        if not succeeded:
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
    if args.test_type=="whitebox":
        whitebox_crash_main(args,unknown_args)


if __name__ == "__main__":
    main()