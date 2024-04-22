import subprocess, os
import time

ABT_PATH = "{}/../ppopp21-preemption-artifact/argobots/install".format(os.getcwd())
MYLIB_PATH = "{}/../mylib".format(os.getcwd())
ROCKSDB_PATH = "{}/../rocksdb".format(os.getcwd())
WIREDTIGER_PATH = "{}/../wiredtiger".format(os.getcwd())

ABT_RESTORE_PATH = "{}/../abt_backup/abt_restore".format(os.getcwd())
ABT_BACKUP_PATH = "{}/../abt_backup/abt_backup".format(os.getcwd())

DB_DIR = "/home/tomoya/mountpoint2/tomoya-s"
RUN_DIR = "/home/tomoya/mountpoint/tomoya-s"

RECORDCOUNT = 50*1000*1000
#RECORDCOUNT = 400*1000
#RECORDCOUNT = 100*1000*1000
#RECORDCOUNT = 7*1000*1000

USE_BACKUP = True

def exec_cmd_str(cmd_str):
    print("Exec: " + cmd_str)
    subprocess.run(cmd_str.split())

def get_cmd(mode, op, dbengine, n_th, cache_capacity, workload, dbname):
    if dbengine == "rocksdb":
        common_args = "-db rocksdb -P rocksdb/rocksdb.properties -p rocksdb.cache_size={} -p rocksdb.dbname={}".format(cache_capacity, dbname)
    elif dbengine == "wiredtiger":
        common_args = "-db wiredtiger -P wiredtiger/wiredtiger.properties -p wiredtiger.cache_size={} -p wiredtiger.home={}".format(cache_capacity, dbname)
    common_args +=  " -P workloads/{} -p threadcount={} -p recordcount={} -p zeropadding=20".format(workload, n_th, RECORDCOUNT)
    if mode == "native":
        common_args += " -p status=true"
    else:
        common_args += " -p status=true"
    
    if op == "set":
        cmd = "./ycsb -load {}".format(common_args)
    else:
        cmd = "./ycsb -run {}".format(common_args)
    return cmd

def run(mode, op, dbengine, n_core, n_th, cache_capacity, workload):
    exec_cmd_str("echo mode={}, op={}, dbengine={}, n_core={}, n_th={}, cache_size={}, workload={}".format(mode, op, dbengine, n_core, n_th, cache_capacity, workload))

    drive_ids = ["0000:0f:00.0","0000:0e:00.0"]
    
    if mode == "abt":
        if USE_BACKUP:
            db_org_path = "{}/ycsb-{}-abt-{}.back".format(DB_DIR, dbengine, RECORDCOUNT)
            db_dat_path = "{}/ycsb-{}-abt-{}.dat".format(DB_DIR, dbengine, RECORDCOUNT)
        db_path = "{}/ycsb-{}-abt-{}".format(DB_DIR, dbengine, RECORDCOUNT)
    else:
        if USE_BACKUP:
            db_org_path = "{}/ycsb-{}-native-{}.back".format(DB_DIR, dbengine, RECORDCOUNT)
        db_path = "{}/ycsb-{}-native-{}".format(RUN_DIR, dbengine, RECORDCOUNT)
        
    if mode == "native":
        add_sched_yield = 0
    else:
        add_sched_yield = 1

    if mode == "set":
        disable_auto_compactions = 1
    else:
        disable_auto_compactions = 0
        
    time_sec = 180
    time_warmup_sec = 60
    make_flags = []
    if dbengine == "rocksdb":
        make_flags.append("BIND_ROCKSDB=1")
        make_flags.append("EXTRA_CXXFLAGS=-I{}/include -DADD_SCHED_YIELD={} -DTIME_SEC={} -DTIME_WARMUP_SEC={} -DDISABLE_AUTO_COMPACTIONS={}".format(ROCKSDB_PATH, add_sched_yield, time_sec, time_warmup_sec, disable_auto_compactions))
        make_flags.append("EXTRA_LDFLAGS=-L{}/build -ldl -lz -lsnappy -lzstd -lbz2 -llz4".format(ROCKSDB_PATH))
    elif dbengine == "wiredtiger":
        make_flags.append("BIND_WIREDTIGER=1")
        make_flags.append("EXTRA_CXXFLAGS=-I{}/src/include -I{}/build/include -DADD_SCHED_YIELD={} -DTIME_SEC={} -DWT_SESSION_MAX={} -DTIME_WARMUP_SEC={}".format(WIREDTIGER_PATH, WIREDTIGER_PATH, add_sched_yield, time_sec, n_th*2, time_warmup_sec))
        make_flags.append("EXTRA_LDFLAGS=-L{}/build -lwiredtiger".format(WIREDTIGER_PATH))

    print(make_flags)
    subprocess.run("make -j -B".split() + make_flags)
        
    exec_cmd_str("sudo chcpu -e 1-{}".format(n_core-1))
    exec_cmd_str("sudo chcpu -d {}-13".format(n_core))


    if USE_BACKUP:
        workloads_dir = "./workloads"
        if op == "get":
            
            cp_workload_cmd = "cp {workloads_dir}/{workload} {workloads_dir}/cp_workload".format(workloads_dir=workloads_dir, workload=workload)
            exec_cmd_str(cp_workload_cmd)

            if mode == "abt":
                restore_env = os.environ.copy()
                restore_env["DRIVE_IDS"] = "_".join(drive_ids)
                subprocess.run("{} {}".format(ABT_RESTORE_PATH, db_dat_path).split(), env=restore_env)
                exec_cmd_str("cp {}/myfs_superblock /root/".format(db_dat_path))
            
            exec_cmd_str("rm -rf {db_path}".format(db_path=db_path))
            cp_db_cmd = "cp -R {db_org_path} {db_path}".format(db_org_path=db_org_path,db_path=db_path)
            exec_cmd_str(cp_db_cmd)

            if False:
                sec1 = 5
                for x in range(0, sec1):
                    subprocess.run("echo {}/{}".format(x,sec1).split())
                    time.sleep(1)
                exec_cmd_str("vmtouch -e {db_path}".format(db_path=db_path))
                exec_cmd_str("numactl --preferred=0 /home/tomoya-s/work/run_rocksdb/staut/trial/a.out")
                sec1 = 10
                for x in range(0, sec1):
                    subprocess.run("echo {}/{}".format(x,sec1).split())
                    time.sleep(1)
    if op == "set":
        exec_cmd_str("rm -rf {db_path}".format(db_path=db_path))
        exec_cmd_str("mkdir -p {db_path}".format(db_path=db_path))
            
    my_env = os.environ.copy()
    if mode == "abt":
        mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} ND={} USE_PREEMPT=0".format(MYLIB_PATH, ABT_PATH, n_core, len(drive_ids))
        process = exec_cmd_str(mylib_build_cmd)
        
        my_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
        my_env["ABT_PREEMPTION_INTERVAL_USEC"] = "10000000"
        if dbengine == "rocksdb":
            my_env["HOOKED_ROCKSDB_DIR"] = db_path
            my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:{}/build".format(ROCKSDB_PATH)
        elif dbengine == "wiredtiger":
            my_env["HOOKED_FILENAMES"] = db_path + "/ycsbc.wt" + ":" + db_path + "/WiredTigerHS.wt"
            #my_env["HOOKED_FILENAMES"] = db_path + "/ycsbc.wt"
            my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:{}/build".format(WIREDTIGER_PATH)
        my_env["DRIVE_IDS"] = "_".join(drive_ids)
        #my_env["ABT_INITIAL_NUM_SUB_XSTREAMS"] = str(n_th + 16)
        my_env["MYFS_SUPERBLOCK_PATH"] = "/root/myfs_superblock"
        #my_env["LIBDEBUG"] = MYLIB_PATH + "/debug.so"
    elif mode == "io_uring":
        mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} USE_PREEMPT=0 USE_IO_URING=1".format(MYLIB_PATH, ABT_PATH, n_core)
        process = subprocess.run(mylib_build_cmd.split())
        my_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
        if dbengine == "rocksdb":
            my_env["HOOKED_ROCKSDB_DIR"] = db_path
            my_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:{}/build".format(ROCKSDB_PATH)
        elif dbengine == "wiredtiger":
            my_env["HOOKED_FILENAMES"] = db_path + "/ycsbc.wt" + ":" + db_path + "/WiredTigerHS.wt"
        #my_env["LIBDEBUG"] = MYLIB_PATH + "/debug.so"
    else:
        if dbengine == "rocksdb":
            my_env["LD_LIBRARY_PATH"] = "{}/build".format(ROCKSDB_PATH)
        else:
            my_env["LD_LIBRARY_PATH"] = "{}/build".format(WIREDTIGER_PATH)
            
        
    if USE_BACKUP and op == "get":
        cmd = get_cmd(mode, op, dbengine, n_th, cache_capacity, "cp_workload", db_path)
    else:
        cmd = get_cmd(mode, op, dbengine, n_th, cache_capacity, workload, db_path)
        
    print(cmd)
    #print(my_env)
    res = subprocess.run(cmd.split(), env=my_env, capture_output=False)
    #print("captured stdout: {}".format(res.stdout.decode()))
    #print("captured stderr: {}".format(res.stderr.decode()))

    if op == "set" and dbengine == "rocksdb":
        exec_cmd_str("make -f Makefile.compact compact -B N_TH={} DB_PATH={} ROCKSDB_PATH={}".format(n_th, db_path, ROCKSDB_PATH))
        if mode == "abt":
            mylib_build_cmd = "make -C {} ABT_PATH={} N_CORE={} ND={} USE_PREEMPT=0".format(MYLIB_PATH, ABT_PATH, 1, len(drive_ids))
            process = exec_cmd_str(mylib_build_cmd)
        
            comp_env = os.environ.copy()
            comp_env["LD_PRELOAD"] = MYLIB_PATH + "/mylib.so"
            comp_env["LD_LIBRARY_PATH"] = ABT_PATH + "/lib:{}/build".format(ROCKSDB_PATH)
            comp_env["HOOKED_ROCKSDB_DIR"] = db_path
            comp_env["DRIVE_IDS"] = "_".join(drive_ids)
            comp_env["MYFS_SUPERBLOCK_PATH"] = "/root/myfs_superblock"
            res = subprocess.run("./compact", env=comp_env)
        else:
            comp_env = os.environ.copy()
            comp_env["LD_LIBRARY_PATH"] = "{}/build".format(ROCKSDB_PATH)
            res = subprocess.run("./compact", env=comp_env)

    if op == "set":
        if USE_BACKUP:
            exec_cmd_str("rm -rf {db_org_path}".format(db_org_path=db_org_path))
            exec_cmd_str("cp -R {db_path} {db_org_path}".format(db_path=db_path, db_org_path=db_org_path))
            if mode == "abt":
                exec_cmd_str("rm -rf {db_dat_path}".format(db_dat_path=db_dat_path))
                exec_cmd_str("mkdir {db_dat_path}".format(db_dat_path=db_dat_path))
                exec_cmd_str("cp /root/myfs_superblock {db_dat_path}".format(db_dat_path=db_dat_path))
                backup_env = os.environ.copy()
                backup_env["DRIVE_IDS"] = "_".join(drive_ids)
                print(db_dat_path)
                subprocess.run("{} {}".format(ABT_BACKUP_PATH, db_dat_path).split(), env=backup_env)
    
def run_clean():
    exec_cmd_str("dd if=/dev/zero of=/root/myfs_superblock count=5 bs=500M")
    

workloads = [
     "workloada",
     "workloadb",
     "workloadc",
     "workloadd",
     "workloade",
     "workloadf",
     "workloadau",
     "workloadbu",
     "workloadcu",
     "workloaddu",
     "workloadeu",
     "workloadfu",
     ]

cache_size = 10*1024*1024*1024
#cache_size = 1*1024*1024
#mode = "abt"
mode = "native"
#mode = "io_uring"

#dbengine = "wiredtiger"
dbengine = "rocksdb"


#run_clean()
#run("native", "set", "wiredtiger", 1, 1, cache_size, "workloadau")
#run("native", "get", "wiredtiger", 1, 1, cache_size, "workloadau")

#run(mode, "get", dbengine, 8, 128, cache_size, "workloadc")
#run("native", "set", dbengine, 1, 1, cache_size, "workloadcu")
#run("native", "get", dbengine, 8, 128, cache_size, "workloadcu")
#run("io_uring", "get", dbengine, 8, 128, cache_size, "workloadcu")
#run("abt", "get", dbengine, 8, 128, cache_size, "workloada")


#run_clean()
run("native", "set", dbengine, 1, 1, cache_size, "workloadcu")
#run("native", "get", dbengine, 1, 1, cache_size, "workloadcu")
#run("native", "get", dbengine, 8, 128, cache_size, "workloadcu")
#run("abt", "get", dbengine, 8, 128, 10*1024*1024*1024, "workloadcu")
#run("abt", "get", dbengine, 8, 256, 1*1024*1024, "workloadau")

if True:
    #for n_ctx in [128, 256, 64, 32]:
    for n_ctx in [128]:
        #for cache_size in [10*1024*1024*1024]:
        for cache_size in [1*1024*1024, 10*1024*1024*1024]:
            for workload in workloads:
                for i in [0]:
                    run(mode, "get", dbengine, 8, n_ctx, cache_size, workload)

#run_clean()
#run("abt", "set", "wiredtiger", 1, 1, 10*1024*1024*1024, "workloadcu")

                    
#for nctx in [64,128,256]:
#    for workload in workloads:
#        run(mode, "get", 8, nctx, 1*1024*1024, workload)
        
#run(mode, "get", 8, 128, cache_size, "workloada")

#run("abt", "get", 8, 128,    2*1024*1024*1024, "workloadcu")
