import subprocess, os


ABT_PATH = "/home/tomoya-s/work/github/ppopp21-preemption-artifact/argobots/install"
MYLIB_PATH = "/home/tomoya-s/mountpoint2/tomoya-s/pthabt/newlib"

def get_cmd(op, n_th, cache_capacity, workload, dbname):
    common_args = "-db rocksdb -P workloads/{} -P rocksdb/rocksdb.properties -p rocksdb.cache_size={} -p rocksdb.dbname={} -p threadcount={}".format(workload, cache_capacity, dbname, n_th)
    if op == "set":
        cmd = "./ycsb -load {} -p status=false".format(common_args)
    else:
        cmd = "./ycsb -run {} -p status=false".format(common_args)
    return cmd

def run(mode, op, n_core, n_th, cache_capacity, workload):
    print("mode={}, op={}, n_core={}, n_th={}, cache_size=1ULL*{}".format(mode, op, n_core, n_th, cache_capacity))

    drive_ids = ["0000:0f:00.0","0000:0e:00.0"]
    
    if mode == "abt" or mode == "pthpth":
        db_path = "/home/tomoya-s/mountpoint2/tomoya-s/ycsb-rocks-abt/{}".format(workload)
    else:
        db_path = "/home/tomoya-s/mountpoint/tomoya-s/ycsb-rocks-native/{}".format(workload)
        
    if op == "set":
        print("We are modifying database {}. Are you Sure? (Y/N)".format(db_path))
        x = input()
        assert x == "y"
        set_data = 1
    else:
        set_data = 0
        
    if mode == "native":
        add_sched_yield = 0
    else:
        add_sched_yield = 1

    subprocess.run("make -j -B ADD_SCHED_YIELD={}".format(add_sched_yield).split())
    if op == "get":
        subprocess.run("make -f Makefile.compact compact -B N_TH={} DB_PATH={}".format(n_th, db_path).split())
        if mode == "abt":
            mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} ND={} USE_PREEMPT=0".format(MYLIB_PATH, ABT_PATH, 1, len(drive_ids))
            process = subprocess.run(mylib_build_cmd.split())
        
            comp_env = os.environ.copy()
            comp_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
            comp_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:/home/tomoya-s/mountpoint2/tomoya-s/rocksdb/build"
            comp_env["HOOKED_ROCKSDB_DIR"] = db_path
            comp_env["DRIVE_IDS"] = "_".join(drive_ids)
            comp_env["MYFS_SUPERBLOCK_PATH"] = "/root/myfs_superblock"
            res = subprocess.run("./compact", env=comp_env)
        else:
            res = subprocess.run("./compact")
        
    subprocess.run("sudo chcpu -e 1-{}".format(n_core-1).split())
    subprocess.run("sudo chcpu -d {}-39".format(n_core).split())
    
    my_env = os.environ.copy()
    if mode == "abt":
        mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} ND={} USE_PREEMPT=0".format(MYLIB_PATH, ABT_PATH, n_core, len(drive_ids))
        process = subprocess.run(mylib_build_cmd.split())
        
        my_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
        my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:/home/tomoya-s/mountpoint2/tomoya-s/rocksdb/build"
        #my_env["ABT_PREEMPTION_INTERVAL_USEC"] = "1000000"
        my_env["ABT_PREEMPTION_INTERVAL_USEC"] = "10000000"
        #my_env["ABT_THREAD_STACKSIZE"] = "65536"
        #my_env["ABT_THREAD_STACKSIZE"] = "1048576"
        my_env["HOOKED_ROCKSDB_DIR"] = db_path
        my_env["DRIVE_IDS"] = "_".join(drive_ids)
        #my_env["ABT_INITIAL_NUM_SUB_XSTREAMS"] = str(n_th + 16)
        my_env["MYFS_SUPERBLOCK_PATH"] = "/root/myfs_superblock"
        #my_env["LIBDEBUG"] = MYLIB_PATH + "/debug.so"
    elif mode == "io_uring":
        mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} USE_PREEMPT=0 USE_IO_URING=1".format(MYLIB_PATH, ABT_PATH, n_core)
        process = subprocess.run(mylib_build_cmd.split())
        my_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
        my_env["HOOKED_ROCKSDB_DIR"] = db_path
        #my_env["LIBDEBUG"] = MYLIB_PATH + "/debug.so"
    cmd = get_cmd(op, n_th, cache_capacity, workload, db_path)
    print(cmd)
    
    res = subprocess.run(cmd.split(), env=my_env, capture_output=False)
    #print("captured stdout: {}".format(res.stdout.decode()))
    #print("captured stderr: {}".format(res.stderr.decode()))

#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloada")
#run("abt", "get", 10, 500, 1*1024*1024*1024, "workloada")

#run("abt", "set", 10, 100, 1*1024*1024*1024, "workloadb")
#run("abt", "get", 10, 100, 1*1024*1024*1024, "workloadb")

#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadf")
#run("abt", "get", 8, 128, 1*1024*1024, "workloadd")
#run("abt", "get", 8, 128, 1*1024*1024, "workloadc")

#run("native", "set", 1, 1,    1*1024*1024*1024, "workloada")
#run("native", "set", 1, 1,    1*1024*1024*1024, "workloadb")
#run("native", "set", 1, 1,    1*1024*1024*1024, "workloadc")
#run("native", "set", 1, 1,    1*1024*1024*1024, "workloadd")
#run("native", "set", 1, 1,    1*1024*1024*1024, "workloadf")
#run("native", "get", 8, 128,    1*1024*1024*1024, "workloada")
#run("native", "get", 8, 128,    1*1024*1024*1024, "workloadb")
#run("native", "get", 8, 128,    1*1024*1024*1024, "workloadc")
#run("native", "get", 8, 128,    1*1024*1024*1024, "workloadd")
#run("native", "get", 8, 128,    1*1024*1024*1024, "workloadf")

#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloada")
#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadb")
#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadc")
#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadd")
#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadf")

# run("abt", "get", 8, 128,    1*1024*1024*1024, "workloada")
# run("abt", "get", 8, 128,    1*1024*1024*1024, "workloadb")
# run("abt", "get", 8, 128,    1*1024*1024*1024, "workloadc")
# run("abt", "get", 8, 128,    1*1024*1024*1024, "workloadd")
# run("abt", "get", 8, 128,    1*1024*1024*1024, "workloadf")

#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadd")
#run("abt", "get", 8, 128,    2*1024*1024*1024, "workloadd")
#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadf")

#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadc")
run("abt", "get", 8, 128,    2*1024*1024*1024, "workloadc")
#run("native", "set", 1, 1,    1*1024*1024*1024, "workloadcu")
#run("native", "get", 8, 128,    2*1024*1024*1024, "workloadc")

#run("abt", "get", 8, 128,    2*1024*1024*1024, "workloadf")
#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadc")
#run("abt", "get", 8, 128,    2*1024*1024*1024, "workloadc")
#run("abt", "set", 1, 1,    1*1024*1024*1024, "workloadb")
#run("abt", "get", 8, 128,    2*1024*1024*1024, "workloadb")

#run("abt", "get", 8, 128,    1*1024*1024*1024, "workloadd")
#run("abt", "get", 8, 128,    1*1024*1024*1024, "workloadf")

#run("native", "get", 8, 128, 1*1024*1024, "workloadb")
#run("native", "get", 8, 128, 1*1024*1024, "workloadc")
#run("native", "get", 1, 128, 1*1024*1024, "workloadc")

#run("native", "get", 10, 100, 1*1024*1024*1024, "workloadb")
#run("native", "get", 10, 100, 1*1024*1024*1024, "workloadc")
#run("native", "get", 10, 100, 1*1024*1024*1024, "workloadd")
#run("native", "get", 10, 100, 1*1024*1024*1024, "workloade")
#run("native", "get", 10, 100, 1*1024*1024*1024, "workloadf")
#run("io_uring", "get", 10, 500, 1024*1024*1024)
#run("abt", "get", 10, 100, 2*1024*1024*1024)
