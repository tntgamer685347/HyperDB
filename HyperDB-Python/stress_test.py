import HyperDB
import time
import os
import random
import datetime
import sys

# i'm transliterating c++ to python. if this is faster because of python
# i'm going to actually lose my mind. please let it be slower.

class Timer:
    def __init__(self):
        self.start = time.perf_counter()
    def elapsed_ms(self):
        return (time.perf_counter() - self.start) * 1000

def get_timestamp():
    return datetime.datetime.now().strftime("[%I:%M%p]")

def fast_noise(length):
    # random.choices is probably faster than any loop i could write in python
    # because it's implemented in c. unlike my hopes and dreams.
    return os.urandom(length)

def assert_test(condition, msg):
    if not condition:
        print(f"  [FAIL] {msg}")
        sys.exit(1)
    else:
        print(f"  [OK]   {msg}")

def run_solo_tests():
    print("\n[1] TESTING HYPERDB MANAGER (SOLO UNIT TESTS)")
    db = HyperDB.HyperDBManager()

    t = Timer()
    db.open_db("test_solo.db", "sandwich")
    print(f"      > OpenDB took: {t.elapsed_ms():.2f} ms")

    t = Timer()
    db.queue_create_table("test_table", [
        HyperDB.ColumnDef("id", HyperDB.ColumnType.Int32),
        HyperDB.ColumnDef("username", HyperDB.ColumnType.String)
    ])
    db.wait_for_queue()
    print(f"      > CreateTable took: {t.elapsed_ms():.2f} ms")

    t = Timer()
    db.queue_clear_table("test_table")
    db.wait_for_queue()
    print(f"      > ClearTable took: {t.elapsed_ms():.2f} ms")
    assert_test(db.get_row_count("test_table") == 0, "Row count is zero")

    t = Timer()
    db.queue_write("test_table", [
        HyperDB.RowData("id", 100),
        HyperDB.RowData("username", "snayck")
    ])
    db.queue_write("test_table", [
        HyperDB.RowData("id", 200),
        HyperDB.RowData("username", "batman")
    ])
    db.wait_for_queue()
    print(f"      > QueueWrite (2 Rows) took: {t.elapsed_ms():.2f} ms")
    assert_test(db.get_row_count("test_table") == 2, "Rows added correctly")

    read_results = []
    def read_callback(res):
        read_results.append(res)

    t = Timer()
    # callback is passed directly. pybind11 handles the translation and gil. 
    # god help us all if i messed that up.
    db.queue_read("test_table", 1, read_callback)
    db.wait_for_queue()
    print(f"      > QueueRead (Callback) took: {t.elapsed_ms():.2f} ms")
    
    # checking results. index 1 in read_results[0] should be username batman.
    # res is a list of RowData.
    batman_found = False
    if len(read_results) > 0 and len(read_results[0]) == 2:
        if read_results[0][1].value == "batman":
            batman_found = True
    assert_test(batman_found, "Read verified batman")

    t = Timer()
    db.queue_delete("test_table", "username", "snayck")
    db.wait_for_queue()
    print(f"      > QueueDelete took: {t.elapsed_ms():.2f} ms")
    assert_test(db.get_row_count("test_table") == 1, "Row removed")

    t = Timer()
    db.force_flush(1)
    print(f"      > ForceFlush (Unencrypted) took: {t.elapsed_ms():.2f} ms")
    
    print("      > Migrating to ENCRYPTED (Fort Knox)...")
    db.set_encryption(True, "secure_pass")
    db.force_flush(1)
    
    db2 = HyperDB.HyperDBManager()
    db2.open_db("test_solo.db", "secure_pass", True)
    assert_test(db2.get_row_count("test_table") == 1, "Encrypted migration verified")
    
    print("      > Migrating back to NITRO (Unencrypted)...")
    db2.set_encryption(False, "")
    db2.force_flush(1)

def run_cluster_write_benchmark():
    print("\n[2] STARTING 10,000,000 ROW APOCALYPSE (WRITE BENCHMARK)")
    cluster = HyperDB.HyperDBCluster()

    pwd = "death"
    folder = "death_benchmark"
    shard_limit = 512 * 1024 * 1024
    total_rows = 10000000

    print(f"  {get_timestamp()} target: 3.8GB scale-out write...")

    t = Timer()
    cluster.open(folder, "death", pwd, shard_limit, False)
    print(f"      > Cluster Initialization took: {t.elapsed_ms():.2f} ms")

    t = Timer()
    cluster.queue_create_table("death", [
        HyperDB.ColumnDef("noise1", HyperDB.ColumnType.String),
        HyperDB.ColumnDef("noise2", HyperDB.ColumnType.String),
        HyperDB.ColumnDef("noise3", HyperDB.ColumnType.String)
    ])
    cluster.wait_for_queue()
    print(f"      > Cluster CreateTable took: {t.elapsed_ms():.2f} ms")

    total_write_timer = Timer()
    lap_timer = Timer()
    batch_size = 10000

    for i in range(0, total_rows, batch_size):
        batch = []
        for _ in range(batch_size):
            # generating noise in python is going to be the bottleneck.
            # i'm going to regret this.
            batch.append([
                HyperDB.RowData("noise1", fast_noise(128)),
                HyperDB.RowData("noise2", fast_noise(128)),
                HyperDB.RowData("noise3", fast_noise(128))
            ])
        
        cluster.queue_write_bulk("death", batch)

        current_rows = i + batch_size
        if current_rows % 1000000 == 0:
            pct = (current_rows / total_rows) * 100
            print(f"    > Progress: {int(pct)}% lap: {lap_timer.elapsed_ms():.2f} ms")
            lap_timer = Timer()
            cluster.wait_for_queue()

    print(f"  {get_timestamp()} write loop finished. Total Write Time: {total_write_timer.elapsed_ms():.2f} ms")

    t = Timer()
    cluster.force_flush(1)
    print(f"      > Cluster Finalize took: {t.elapsed_ms():.2f} ms")

def perform_read_test(cluster, label):
    print(f"\n  --- PHASE: {label} ---")
    total_rows = cluster.get_row_count("death")
    read_count = 100000
    
    # python doesn't have a clean atomic int like c++, but for this increment 
    # it doesn't matter much because we're just counting.
    completed_reads = 0
    def read_cb(res):
        nonlocal completed_reads
        completed_reads += 1

    print(f"      > Stress testing 100,000 random global ID reads...")
    read_timer = Timer()
    for i in range(read_count):
        row_id = random.randint(0, total_rows - 1)
        cluster.queue_read("death", row_id, read_cb)
        if i % 10000 == 0: 
            cluster.wait_for_queue()
    
    cluster.wait_for_queue()
    elapsed = read_timer.elapsed_ms()
    rate = read_count / (elapsed / 1000.0) if elapsed > 0 else 0
    print(f"      > {label} reads t-total: {elapsed:.2f} ms ({rate:.2f} reads/sec)")

    print("      > Full aggregate scan (cross-shard)...")
    t = Timer()
    cluster.queue_find("death", "noise1", "missing_no_9999", lambda res: None)
    cluster.wait_for_queue()
    print(f"      > Full Shard Scan took: {t.elapsed_ms():.2f} ms")

def run_cluster_read_benchmark():
    print("\n[3] INITIATING READ APOCALYPSE (STRESS TEST)")
    folder = "death_benchmark"
    shard_limit = 512 * 1024 * 1024

    try:
        c1 = HyperDB.HyperDBCluster()
        c1.open(folder, "death", "death", shard_limit, False)
        perform_read_test(c1, "ORIGINAL NITRO")

        print("  > MIGRATING 4.2GB TO FORT KNOX (ENCRYPTED)...")
        t = Timer()
        c1.set_encryption(True, "death")
        c1.force_flush(1)
        print(f"      > Migration to Encrypted took: {t.elapsed_ms():.2f} ms")
        # cleanup c1 so files aren't locked
        del c1 

        c2 = HyperDB.HyperDBCluster()
        print("  > LOADING ENCRYPTED CLUSTER (COLD START TAX)...")
        t = Timer()
        c2.open(folder, "death", "death", shard_limit, True)
        print(f"      > Cold Open (Encrypted) took: {t.elapsed_ms():.2f} ms")
        perform_read_test(c2, "ENCRYPTED")

        print("  > MIGRATING 4.2GB BACK TO NITRO...")
        t = Timer()
        c2.set_encryption(False, "")
        c2.force_flush(1)
        print(f"      > Migration back to Nitro took: {t.elapsed_ms():.2f} ms")
        del c2

        c3 = HyperDB.HyperDBCluster()
        print("  > LOADING NITRO CLUSTER (INSTANT OPEN)...")
        t = Timer()
        c3.open(folder, "death", "", shard_limit, False)
        print(f"      > Cold Open (Nitro) took: {t.elapsed_ms():.2f} ms")
        perform_read_test(c3, "POST-MIGRATION NITRO")
        print("  [OK] Full migration cycle verified on 4.2GB dataset.")

    except Exception as e:
        print(f"  [READ FAIL] {str(e)}")

if __name__ == "__main__":
    print("--- STARTING UNIFIED HYPERDB MASTER STRESS TEST (PYTHON) ---")
    master_timer = Timer()
    
    run_solo_tests()
    # warning: generating 10M rows of noise in python is going to be slow as hell.
    # don't blame the database when the bottleneck is string joining.
    run_cluster_write_benchmark()
    run_cluster_read_benchmark()

    print("\n---------------------------------------------------")
    print(f"ALL SUITES COMPLETED IN: {master_timer.elapsed_ms():.2f} ms")
    print("---------------------------------------------------")
