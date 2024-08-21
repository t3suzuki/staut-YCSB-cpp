import subprocess, os
import time

ABT_PATH = "{}/../argobots/install".format(os.getcwd())
MYLIB_PATH = "{}/../mylib".format(os.getcwd())
ROCKSDB_PATH = "{}/../rocksdb".format(os.getcwd())
BENCH_PATH = "{}/../../db_bench".format(os.getcwd())
PHOTON_BENCH_PATH = "{}/../../../../photon/build/db_bench".format(os.getcwd())

DB_DIR = "{}/../../db_dir".format(os.getcwd())

RECORD_COUNT = 100000

def exec_cmd_str(cmd_str):
    print("Exec: " + cmd_str)
    subprocess.run(cmd_str.split())

def get_cmd(op, n_th, cache_capacity, db_dir, bench_path):
    if op == "set":
        return "{} --benchmarks=fillrandom --use_existing_db=0 --disable_auto_compactions=1 --sync=0 -use_direct_reads=true -block_align=true --db={} --wal_dir={} --num={} --num_levels=6 --key_size=100 --value_size=1000 --block_size=4096 --cache_size={} --cache_numshardbits=6 --compression_max_dict_bytes=0 --compression_ratio=0.5 --compression_type=none --level_compaction_dynamic_level_bytes=true --bytes_per_sync=8388608 --cache_index_and_filter_blocks=0 --pin_l0_filter_and_index_blocks_in_cache=1 --benchmark_write_rate_limit=0 --hard_rate_limit=3 --rate_limit_delay_max_milliseconds=1000000 --write_buffer_size=134217728 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --verify_checksum=1 --delete_obsolete_files_period_micros=62914560 --max_bytes_for_level_multiplier=8 --statistics=0 --stats_per_interval=1 --stats_interval_seconds=60 --histogram=1 --memtablerep=skip_list --bloom_bits=10 --open_files=-1 --max_background_compactions=16 --max_write_buffer_number=8 --allow_concurrent_memtable_write=false --max_background_flushes=7 --level0_file_num_compaction_trigger=10485760 --level0_slowdown_writes_trigger=10485760 --level0_stop_writes_trigger=10485760 --threads={} --memtablerep=vector --allow_concurrent_memtable_write=false --disable_wal=1 --seed=1724243226".format(bench_path, db_dir, db_dir, RECORD_COUNT, cache_capacity, n_th)
    if op == "compact":
        return "{} --benchmarks=compact --use_existing_db=1 --disable_auto_compactions=1 --sync=0 -use_direct_reads=true -block_align=true --db={} --wal_dir={} --num={} --num_levels=6 --key_size=100 --value_size=1000 --block_size=4096 --cache_size={} --cache_numshardbits=6 --compression_max_dict_bytes=0 --compression_ratio=0.5 --compression_type=none --level_compaction_dynamic_level_bytes=true --bytes_per_sync=8388608 --cache_index_and_filter_blocks=0 --pin_l0_filter_and_index_blocks_in_cache=1 --benchmark_write_rate_limit=0 --hard_rate_limit=3 --rate_limit_delay_max_milliseconds=1000000 --write_buffer_size=134217728 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --verify_checksum=1 --delete_obsolete_files_period_micros=62914560 --max_bytes_for_level_multiplier=8 --statistics=0 --stats_per_interval=1 --stats_interval_seconds=60 --histogram=1 --memtablerep=skip_list --bloom_bits=10 --open_files=-1 --level0_file_num_compaction_trigger=4 --level0_stop_writes_trigger=20 --max_background_compactions=16 --max_write_buffer_number=8 --max_background_flushes=7 --threads={}".format(bench_path, db_dir, db_dir, RECORD_COUNT, cache_capacity, n_th)
    if op == "get":
        return "{} --benchmarks=readrandom --use_existing_db=1 -use_direct_reads=true -block_align=true --db={} --wal_dir={} --num={} --num_levels=6 --key_size=100 --value_size=1000 --block_size=4096 --cache_size={} --cache_numshardbits=6 --compression_max_dict_bytes=0 --compression_ratio=0.5 --compression_type=none --level_compaction_dynamic_level_bytes=true --bytes_per_sync=8388608 --cache_index_and_filter_blocks=0 --pin_l0_filter_and_index_blocks_in_cache=1 --benchmark_write_rate_limit=0 --hard_rate_limit=3 --rate_limit_delay_max_milliseconds=1000000 --write_buffer_size=134217728 --target_file_size_base=134217728 --max_bytes_for_level_base=1073741824 --verify_checksum=1 --delete_obsolete_files_period_micros=62914560 --max_bytes_for_level_multiplier=8 --statistics=0 --stats_per_interval=1 --stats_interval_seconds=60 --histogram=1 --memtablerep=skip_list --bloom_bits=10 --open_files=-1 --level0_file_num_compaction_trigger=4 --level0_stop_writes_trigger=20 --max_background_compactions=16 --max_write_buffer_number=8 --max_background_flushes=7 --threads={} --seed=1724243229".format(bench_path, db_dir, db_dir, RECORD_COUNT, cache_capacity, n_th)
    

def run(mode, op, n_core, n_th, cache_capacity):
    #exec_cmd_str("echo mode={}, op={}, dbengine={}, n_core={}, n_th={}, cache_size={}, workload={}".format(mode, op, dbengine, n_core, n_th, cache_capacity, workload))
    
    drive_ids = ["0000:0f:00.0","0000:0e:00.0"]

    db_path = DB_DIR
    
    exec_cmd_str("sudo chcpu -e 1-{}".format(n_core-1))
    exec_cmd_str("sudo chcpu -d {}-39".format(n_core))

    my_env = os.environ.copy()
    if mode == "abt":
        mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} ND={} USE_PREEMPT=0".format(MYLIB_PATH, ABT_PATH, n_core, len(drive_ids))
        process = exec_cmd_str(mylib_build_cmd)
        
        my_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
        my_env["ABT_PREEMPTION_INTERVAL_USEC"] = "10000000"
        my_env["HOOKED_ROCKSDB_DIR"] = db_path
        my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:{}/build".format(ROCKSDB_PATH)
        my_env["DRIVE_IDS"] = "_".join(drive_ids)
        my_env["MYFS_SUPERBLOCK_PATH"] = "/root/myfs_superblock"
    elif mode == "io_uring":
        mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} USE_PREEMPT=0 USE_IO_URING=1".format(MYLIB_PATH, ABT_PATH, n_core)
        process = subprocess.run(mylib_build_cmd.split())
        my_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
        my_env["HOOKED_ROCKSDB_DIR"] = db_path
        my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:{}/build".format(ROCKSDB_PATH)
        #my_env["LIBDEBUG"] = MYLIB_PATH + "/debug.so"
    else:
        my_env["LD_LIBRARY_PATH"] = "{}/build".format(ROCKSDB_PATH)

    if mode == "photon":
        bench_path = PHOTON_BENCH_PATH
    else:
        bench_path = BENCH_PATH

    cmd = get_cmd(op, n_th, cache_capacity, db_path, bench_path)
        
    print(cmd)
    res = subprocess.run(cmd.split(), env=my_env, capture_output=False)
    
def run_clean():
    exec_cmd_str("dd if=/dev/zero of=/root/myfs_superblock count=5 bs=500M")

cache_size = 1024 * 1024
    
run("native", "set", 1, 128, cache_size)
run("native", "compact", 1, 128, cache_size)
run("native", "get", 8, 128, cache_size)
run("photon", "get", 8, 128, cache_size)
#run("io_uring", "get", 8, 128, cache_size)
#run("abt", "get", 8, 128, cache_size)
