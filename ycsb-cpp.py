import subprocess, os
import time

ABT_PATH = "/home/tomoya-s/work/github/ppopp21-preemption-artifact/argobots/install"
MYLIB_PATH = "/home/tomoya-s/mountpoint2/tomoya-s/pthabt/newlib"
ROCKSDB_PATH = "/home/tomoya-s/mountpoint2/tomoya-s/rocksdb"
WIREDTIGER_PATH = "/home/tomoya-s/mountpoint2/tomoya-s/wiredtiger"

RECORDCOUNT = 100*1000*1000

def get_cmd(mode, op, dbengine, n_th, cache_capacity, workload, dbname):
    if dbengine == "rocksdb":
        common_args = "-db rocksdb -P rocksdb/rocksdb.properties -p rocksdb.cache_size={} -p rocksdb.dbname={}".format(cache_capacity, dbname)
    elif dbengine == "wiredtiger":
        common_args = "-db wiredtiger -P wiredtiger/wiredtiger.properties -p wiredtiger.cache_size={} -p wiredtiger.home={}".format(cache_capacity, dbname)
    common_args +=  " -P workloads/{} -p threadcount={} -p recordcount={} -p zeropadding=20".format(workload, n_th, RECORDCOUNT)
    if mode == "native":
        common_args += " -p status=true"
    else:
        common_args += " -p status=false"
    
    if op == "set":
        cmd = "./ycsb -load {}".format(common_args)
    else:
        cmd = "./ycsb -run {}".format(common_args)
    return cmd

def run(mode, op, dbengine, n_core, n_th, cache_capacity, workload):
    print("mode={}, op={}, dbengine={}, n_core={}, n_th={}, cache_size={}, workload={}".format(mode, op, dbengine, n_core, n_th, cache_capacity, workload))

    drive_ids = ["0000:0f:00.0","0000:0e:00.0"]
    
    if mode == "abt":
        db_org_path = "/home/tomoya-s/mountpoint2/tomoya-s/ycsb-{}-abt-{}".format(dbengine, RECORDCOUNT)
    else:
        db_org_path = "/home/tomoya-s/mountpoint2/tomoya-s/ycsb-{}-native-{}.back".format(dbengine, RECORDCOUNT)
        db_path = "/home/tomoya-s/mountpoint/tomoya-s/ycsb-{}-native-{}".format(dbengine, RECORDCOUNT)
        
    if mode == "native":
        add_sched_yield = 0
    else:
        add_sched_yield = 1

    time_sec = 300
    time_warmup_sec = 60
    make_flags = []
    if dbengine == "rocksdb":
        make_flags.append("BIND_ROCKSDB=1")
        make_flags.append("EXTRA_CXXFLAGS=-I{}/include -DADD_SCHED_YIELD={} -DTIME_SEC={} -DTIME_WARMUP_SEC={}".format(ROCKSDB_PATH, add_sched_yield, time_sec, time_warmup_sec))
        make_flags.append("EXTRA_LDFLAGS=-L{}/build -ldl -lz -lsnappy -lzstd -lbz2 -llz4".format(ROCKSDB_PATH))
    elif dbengine == "wiredtiger":
        make_flags.append("BIND_WIREDTIGER=1")
        make_flags.append("EXTRA_CXXFLAGS=-I{}/src/include -I{}/build/include -DADD_SCHED_YIELD={} -DTIME_SEC={} -DWT_SESSION_MAX={}".format(WIREDTIGER_PATH, WIREDTIGER_PATH, add_sched_yield, time_sec, n_th))
        make_flags.append("EXTRA_LDFLAGS=-L{}/build -lwiredtiger".format(WIREDTIGER_PATH))

    print(make_flags)
    subprocess.run("make -j -B".split() + make_flags)
        
    subprocess.run("sudo chcpu -e 1-{}".format(n_core-1).split())
    subprocess.run("sudo chcpu -d {}-39".format(n_core).split())

    workloads_dir = "./workloads"
    if op == "get":
        cp_workload_cmd = "cp {workloads_dir}/{workload} {workloads_dir}/myworkload".format(workloads_dir=workloads_dir, workload=workload)
        print(cp_workload_cmd)
        subprocess.run(cp_workload_cmd.split())
        subprocess.run("rm -rf {db_path}/myworkload".format(db_path=db_path).split())
        cp_db_cmd = "cp -R {db_org_path}/myworkload {db_path}/".format(db_org_path=db_org_path,db_path=db_path)
        print(cp_db_cmd)
        subprocess.run(cp_db_cmd.split())
        subprocess.run("echo 3 > /proc/sys/vm/drop_caches".split())
        time.sleep(10)
    elif op == "set":
        subprocess.run("rm -rf {db_path}/myworkload".format(db_path=db_path).split())
        subprocess.run("mkdir -p {db_path}/myworkload".format(db_path=db_path).split())
        
    my_env = os.environ.copy()
    if mode == "abt":
        mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} ND={} USE_PREEMPT=0".format(MYLIB_PATH, ABT_PATH, n_core, len(drive_ids))
        process = subprocess.run(mylib_build_cmd.split())
        
        my_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
        my_env["ABT_PREEMPTION_INTERVAL_USEC"] = "10000000"
        if dbengine == "rocksdb":
            my_env["HOOKED_ROCKSDB_DIR"] = db_path + "/myworkload"
            my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:/home/tomoya-s/mountpoint2/tomoya-s/rocksdb/build"
        elif dbengine == "wiredtiger":
            my_env["HOOKED_FILENAME"] = db_path + "/ycsbc.wt"
            my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:/home/tomoya-s/mountpoint2/tomoya-s/wiredtiger/build"
        my_env["DRIVE_IDS"] = "_".join(drive_ids)
        #my_env["ABT_INITIAL_NUM_SUB_XSTREAMS"] = str(n_th + 16)
        my_env["MYFS_SUPERBLOCK_PATH"] = "/root/myfs_superblock"
        #my_env["LIBDEBUG"] = MYLIB_PATH + "/debug.so"
    cmd = get_cmd(mode, op, dbengine, n_th, cache_capacity, workload, db_path + "/myworkload")
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
        subprocess.run("rm -rf {db_org_path}".format(db_org_path=db_org_path).split())
        subprocess.run("mkdir {db_org_path}".format(db_org_path=db_org_path).split())
        subprocess.run("cp -R {db_path}/myworkload {db_org_path}".format(db_path=db_path, db_org_path=db_org_path).split())
    
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

cache_size = 10*1024*1024*1024
#cache_size = 1*1024*1024
#mode = "abt"
mode = "native"

#dbengine = "wiredtiger"
dbengine = "rocksdb"

#run(mode, "set", 1, 1, cache_size, "workloadfu")

#run(mode, "set", dbengine, 1, 1, cache_size, "myworkload")

#run(mode, "get", dbengine, 8, 128, cache_size, "workloadc")
if True:
    for cache_size in [1*1024*1024, 10*1024*1024*1024]:
        for nctx in [128]:
            for workload in workloads:
                for i in [0,1]:
                    run(mode, "get", dbengine, 8, 128, cache_size, workload)

#for nctx in [64,128,256]:
#    for workload in workloads:
#        run(mode, "get", 8, nctx, 1*1024*1024, workload)
        
#run(mode, "get", 8, 128, cache_size, "workloada")

#run("abt", "get", 8, 128,    2*1024*1024*1024, "workloadcu")
