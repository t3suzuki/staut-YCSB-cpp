import subprocess, os


ABT_PATH = "/home/tomoya-s/work/github/ppopp21-preemption-artifact/argobots/install"
MYLIB_PATH = "/home/tomoya-s/mountpoint2/tomoya-s/pthabt/newlib"

def get_cmd(op, dbengine n_th, cache_capacity, workload, dbname):
    recordcount = 100*1000*1000
    if dbengine == "rocksdb":
        common_args = "-db rocksdb -P rocksdb/rocksdb.properties -p rocksdb.cache_size={} -p rocksdb.dbname={}".format(cache_capacity, dbname)
    elif dbengine == "wiredtiger":
        common_args = "-db wiredtiger ".format()
    common_args +=  "-P workloads/{} -p threadcount={} -p recordcount={} -p status=false -p zeropadding=20".format(workload, n_th, recordcount)
    
    if op == "set":
        cmd = "./ycsb -load {}".format(common_args)
    else:
        cmd = "./ycsb -run {}".format(common_args)
    return cmd

def run(mode, op, dbengine, n_core, n_th, cache_capacity, workload):
    print("mode={}, op={}, dbengine={}, n_core={}, n_th={}, cache_size={}ULL".format(mode, op, dbengine, n_core, n_th, cache_capacity))

    drive_ids = ["0000:0f:00.0","0000:0e:00.0"]
    
    if mode == "abt":
        db_path = "/home/tomoya-s/mountpoint2/tomoya-s/ycsb-{}-abt10m/{}".format(dbengine, workload)
    else:
        db_path = "/home/tomoya-s/mountpoint/tomoya-s/ycsb-{}-native/{}".format(dbengine, workload)
        
    if op == "set":
        #print("We are modifying database {}. Are you Sure? (Y/N)".format(db_path))
        #x = input()
        #assert x == "y"
        subprocess.run("rm -rf {}".format(db_path).split())
        
    if mode == "native":
        add_sched_yield = 0
    else:
        add_sched_yield = 1

    if dbengine == "rocksdb":
        cmake_flags = "BIND_ROCKSDB=1 EXTRA_CXXFLAGS=\"-I{}/include -DADD_SCHED_YIELD={}\" EXTRA_LDFLAGS=\"-L{}/build -ldl -lz -lsnappy -lzstd -lbz2 -llz4\""
    elif dbengine == "wiredtiger"
        cmake_flags = "BIND_WIREDTIGER=1 EXTRA_CXXFLAGS=\"-I{}/src/include -I{}/build/include -DADD_SCHED_YIELD={}\" EXTRA_LDFLAGS=\"-L{}/build -lwiredtiger\"".format(wiredtiger_path, wiredtiger_path, add_sched_yield, wiredtiger_path)
        
    subprocess.run("make -j -B {}".format(cmake_flags).split())
        
    subprocess.run("sudo chcpu -e 1-{}".format(n_core-1).split())
    subprocess.run("sudo chcpu -d {}-39".format(n_core).split())
    
    my_env = os.environ.copy()
    if mode == "abt":
        mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} ND={} USE_PREEMPT=0".format(MYLIB_PATH, ABT_PATH, n_core, len(drive_ids))
        process = subprocess.run(mylib_build_cmd.split())
        
        my_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
        my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:/home/tomoya-s/mountpoint2/tomoya-s/rocksdb/build"
        my_env["ABT_PREEMPTION_INTERVAL_USEC"] = "10000000"
        if dbengine == "rocksdb":
            my_env["HOOKED_ROCKSDB_DIR"] = db_path
        elif dbengine == "wiredtiger":
            my_env["HOOKED_FILENAME"] = db_path + "/test.wt"
        my_env["DRIVE_IDS"] = "_".join(drive_ids)
        #my_env["ABT_INITIAL_NUM_SUB_XSTREAMS"] = str(n_th + 16)
        my_env["MYFS_SUPERBLOCK_PATH"] = "/root/myfs_superblock"
        #my_env["LIBDEBUG"] = MYLIB_PATH + "/debug.so"
    cmd = get_cmd(op, n_th, cache_capacity, workload, db_path)
    print(cmd)
    
    res = subprocess.run(cmd.split(), env=my_env, capture_output=False)
    #print("captured stdout: {}".format(res.stdout.decode()))
    #print("captured stderr: {}".format(res.stderr.decode()))

    if op == "set" and dbengine == "rocksdb":
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

    
def run_clean():
    subprocess.run("dd if=/dev/zero of=/root/myfs_superblock count=1 bs=4G".split())
    

workloads = [
    "workloada",
    "workloadb",
    "workloadc",
    "workloadd",
    "workloadf",
    "workloadau",
    "workloadbu",
    "workloadcu",
    "workloaddu",
    "workloadfu",
    ]

#cache_size = 10*1024*1024*1024
cache_size = 1*1024*1024
mode = "abt"
#mode = "native"

#run(mode, "set", 1, 1, cache_size, "workloadfu")

for nctx in [128]:
    for workload in workloads:
        run_clean()
        run(mode, "set", "rocksdb", 1, 1, cache_size, workload)
        run(mode, "get", "rocksdb", 8, nctx, cache_size, workload)

#for nctx in [64,128,256]:
#    for workload in workloads:
#        run(mode, "get", 8, nctx, 1*1024*1024, workload)
        
#run(mode, "get", 8, 128, cache_size, "workloada")

#run("abt", "get", 8, 128,    2*1024*1024*1024, "workloadcu")
