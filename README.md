<div align="center">

# ⚡ HyperDB

**A deeply unhinged but maliciously fast C++ NoSQL database.**

[![C++20](https://img.shields.io/badge/C++-20-blue.svg)](https://en.wikipedia.org/wiki/C%2B%2B)
[![FlatBuffers](https://img.shields.io/badge/FlatBuffers-32Bit-orange.svg)](https://google.github.io/flatbuffers/)
[![Python](https://img.shields.io/badge/Python-Bindings-yellow.svg)](#-python-bindings)
[![Speed](https://img.shields.io/badge/Latency-<0.05ms-success.svg)](#)
[![Mental Health](https://img.shields.io/badge/Mental%20Health-Declining-critical.svg)](#)

*When I wrote this code, only God and I understood what I was doing. Now, only God knows.*

</div>

---

## 💀 What is this?

HyperDB is an in-memory, violently fast, flatbuffer-backed embedded NoSQL database written in C++20. It was born from a 3AM debugging session and a deep hatred for latency. It features **O(1)** column lookups, a **`condition_variable`-driven persistent worker queue**, **bulk write batching**, **optional AES-256-CTR encryption**, and **Python bindings** via pybind11.

Fort Knox when you need it. Raw FlatBuffers when you don't.

If you are looking for an ACID compliant, highly scalable, enterprise-grade cloud solution... go use Postgres. If you want to serialize your vectors into flatbuffer blobs at 0.03ms per query while redlining your CPU's L3 cache... you're in the right place.

---

## 🔥 Features that justify the technical debt

- **Zero-Variant Scan Loop**: `ExecFind` and `ExecDelete` never box values into `HyperValue` during the hot scan pass. Typed raw pointers only. The compiler can autovectorize the integer branches. It does.
- **Two-Pass Find**: Collect matching row indices first (one column, cache-friendly, vectorizable), build `ReadResult` rows only for hits. If matches << rows — which is the entire point — this wins dramatically.
- **Persistent Condition Variable Queue**: The worker thread sleeps on a `std::condition_variable`. No spinning. No wasted cycles. `AddToQueueBulk` drops N entries in a single lock acquisition and fires one `notify_one`.
- **`QueueWriteBulk`**: Batch thousands of rows into a single queue push. One mutex acquire for the whole batch. This is how you hit 311k+ rows/sec in Nitro Mode.
- **O(1) Column & Table Addressing**: `column_map` per table via `std::unordered_map`. No linear string-searching. Everything maps instantly.
- **Incremental `estimated_size_` Tracking**: Mirror size is maintained as a running total on every write/delete/clear. `EstimateMirrorSize()` is O(1). `MaybeSpillToNewShard` calls it constantly.
- **`QueueDelete` Callback**: Delete fires `std::function<void(int)>` with the actual removed row count. The cluster uses this to keep manifest row IDs consistent after deletions via `ShiftManifestEntries`.
- **Optional AES-256-CTR Encryption**: Fort Knox mode (`encrypt=true`, default) or Nitro Mode (`encrypt=false`). The cluster persists this in the `.manifest` — all future shards inherit it.
- **`SetEncryption` Live Migration**: Toggle encryption or change password on a live open database or cluster. Re-serializes on next flush. No restart required.
- **Python Bindings**: Full pybind11 bindings exposing the complete API. GIL is acquired correctly on callback threads. It works. It took 3 hours to get right because of a missing comma.
- **`HyperDBCluster` Auto-Sharding**: FlatBuffers has a ~2GB hard limit. `HyperDBCluster` spills data to new shards automatically, tracking row ID ranges in a `.manifest`, fanning out `Find`/`Delete` transparently.

---

## 🚀 Performance Benchmarks

*(C++ benchmarks on AMD Ryzen 7 5800X)*

### Solo Operations

| Operation | Encrypted | ⚡ Nitro Mode |
| :--- | :--- | :--- |
| **New DB Init** | `0.07 ms` | `0.06 ms` |
| **Flush small DB** | `43.65 ms` *(58k PBKDF2)* / `0.84 ms` *(1 iter)* | `0.28 ms` |
| **QueueWrite (2 Rows)** | `0.04 ms` | `0.03 ms` |
| **QueueRead (Async)** | `0.04 ms` | `0.02 ms` |
| **QueueFind** | `0.03 ms` | `0.02 ms` |
| **Cross-Shard Find** | `0.04 ms` | `0.04 ms` |

### 10,000,000 Row "Apocalypse" Benchmark
*(4.2GB | 512MB shards | 3× 128-byte string columns per row)*

| Metric | Encrypted (58k iter) | ⚡ Nitro Mode |
| :--- | :--- | :--- |
| **Total Write Loop Time** | `109.9 seconds` | `32.1 seconds` |
| **Throughput** | `~91,000 rows/sec` | `~311,000 rows/sec` |
| **Cluster Finalize (Flush All)** | `1,721 ms` | `148 ms` |
| **Cold Open (4.2GB, 8 shards)** | `~85 seconds` | `~7.8 seconds` |
| **100,000 Random Reads** | `21.2 ms` | `21.2 ms` |
| **Random Read Throughput** | `4,716,490 reads/sec` | `4,726,050 reads/sec` |
| **Full 4.2GB Cross-Shard Scan** | `8.998 ms` | `8.459 ms` |
| **Final Shard Count** | `8 Shards` | `8 Shards` |

Once data is in memory, encrypted and unencrypted are the same speed. The ~77 second cold open gap is entirely AES-256-CTR decrypting 4.2GB of ciphertext. In-memory operations don't care about encryption mode. They never did.

---

---

## 📊 Solo vs. Cluster — Know Which One You Need

`HyperDBCluster` is not just "more storage." It changes the performance profile in ways worth understanding before you commit to an architecture.

**Writes are slower in a cluster.** Every write goes through `MaybeSpillToNewShard`, manifest tracking, and global row ID accounting on top of the normal queue path. For small datasets the per-write overhead adds up. A single `HyperDBManager` will always write faster than a cluster at the same row count.

**Reads and searches are where the cluster pays off.** A cross-shard `QueueFind` dispatches to all shard worker threads simultaneously and merges results when they all return. Each shard scans its own 512MB slice in parallel. The 8.5ms full 4.2GB scan time is the proof — 8 threads scanning 512MB each beats one thread scanning 4.2GB sequentially by a wide margin.

| Scenario | Use |
| :--- | :--- |
| **< 500k rows, write speed is the priority** | `HyperDBManager` — zero overhead, maximum throughput |
| **> 1M rows, search and read heavy** | `HyperDBCluster` — parallel fan-out, shard-local scans |
| **> 1.5GB total data** | `HyperDBCluster` — required, FlatBuffers cannot go higher |

---

## 🏗️ Installation & Setup (C++)

1. Include `HyperDB.h` in your project.
2. Link against `HyperDB.lib`.
3. **CRITICAL:** Your include path must contain the `flatbuffers` headers and `hyperdb_generated.h`. The `.lib` statically links FlatBuffers but MSVC still needs the headers to compile your code. If your IDE lights up like a Christmas tree, this is why. You do **not** need `flatbuffers.lib` — it's already baked into `HyperDB.lib`.

---

## 🛠️ C++ Usage Example

```cpp
#include "HyperDB.h"
#include <iostream>

template<typename T>
void wait_for_queue(T& db) {
    while (!db.IsQueueEmpty())
        std::this_thread::yield(); // yield() is mandatory here. without it your main
}                                  // thread starves the worker on the L3 cache.

int main() {
    HyperDBManager db;

    // Fort Knox mode (default) — AES-256-CTR + PBKDF2. encrypted at rest.
    // slow cold open on large DBs. your data is a fortress. you wait in line.
    db.OpenDB("users.db", "sandwich");

    // Nitro Mode — raw flatbuffers. no crypto. instant open. no protection.
    // db.OpenDB("users.db", "sandwich", false);

    db.QueueCreateTable("users", {
        {"id",       HyperDB::ColumnType::ColumnType_Int32},
        {"username", HyperDB::ColumnType::ColumnType_String}
    });

    db.QueueWrite("users", {
        {"id",       int32_t(1337)},
        {"username", std::string("batman")}
    });

    db.QueueFind("users", "username", std::string("batman"),
        [](std::vector<ReadResult> results) {
            if (!results.empty())
                std::cout << "Found: "
                          << HyperDBUtil::HyperValueToString(results[0][0].value)
                          << "\n";
        });

    db.QueueDelete("users", "username", std::string("batman"),
        [](int removed) {
            std::cout << "Deleted " << removed << " row(s).\n";
        });

    wait_for_queue(db);

    // iterations only matter in Fort Knox mode.
    // in Nitro Mode this is just "serialize my flatbuffer to disk." fast. painless.
    db.ForceFlush(58253);
    return 0;
}
```

---

## 📦 Cluster Mode (C++)

For datasets that exceed the ~2GB FlatBuffers single-file limit, `HyperDBCluster` handles everything automatically.

```cpp
HyperDBCluster cluster;

// encrypted — safe, slower cold open
cluster.Open("db_folder", "chaos", "death", 512ULL * 1024 * 1024);

// nitro mode — 7.8s cold open on 4GB instead of 85s
// cluster.Open("db_folder", "chaos", "death", 512ULL * 1024 * 1024, false);

cluster.QueueCreateTable("logs", {
    {"log_id",  HyperDB::ColumnType::ColumnType_Int64},
    {"message", HyperDB::ColumnType::ColumnType_String}
});

// always use QueueWriteBulk for serious ingestion.
// QueueWrite = one mutex acquire per row.
// QueueWriteBulk = one mutex acquire for the whole batch.
// at 10k rows/batch, 10M total: 1,000 lock ops vs. 10,000,000. pick one.
std::vector<std::vector<RowData>> batch;
batch.reserve(10000);
for (int i = 0; i < 10000; ++i)
    batch.push_back({{"log_id", int64_t(i)}, {"message", std::string("entry")}});
cluster.QueueWriteBulk("logs", std::move(batch));
```

> [!TIP]
> **Always use `QueueWriteBulk` for bulk ingestion.** This is how we hit 311k rows/sec. Single-row `QueueWrite` at 10M rows is 10M mutex lock/unlock cycles. That's a very loud way to ask for a bad benchmark.

> [!NOTE]
> **Cluster writes go to the active shard only.** Cross-shard `Find` and `Delete` fan out to all matching shards in parallel — each shard's worker thread runs its scan independently. This is why large dataset searches are fast. It is also why write throughput doesn't scale with shard count.

---

## 🐍 Python Bindings

HyperDB ships with full pybind11 bindings. Build `HyperDB.pyd` (Windows) or `HyperDB.so` (Linux) from `bindings.cpp` and drop it next to your script.

### Python — Solo DB

```python
import HyperDB
import time

db = HyperDB.HyperDBManager()

# Fort Knox mode (default)
db.open_db("users.db", "sandwich")

# Nitro Mode — no encryption, no waiting
# db.open_db("users.db", "sandwich", encrypt=False)

db.queue_create_table("users", [
    HyperDB.ColumnDef("id",       HyperDB.ColumnType.Int32),
    HyperDB.ColumnDef("username", HyperDB.ColumnType.String),
])

db.queue_write("users", [
    HyperDB.RowData("id",       1337),
    HyperDB.RowData("username", "batman"),
])

# callback fires on the worker thread — GIL is acquired automatically
db.queue_find("users", "username", "batman",
    lambda results: print(f"Found: {results[0][1].value}") if results else None
)

db.queue_delete("users", "username", "batman",
    lambda count: print(f"Deleted {count} row(s)")
)

# time.sleep(0) is the Python equivalent of std::this_thread::yield()
# without it your main thread starves the worker. don't skip it.
while not db.is_queue_empty():
    time.sleep(0)

db.force_flush(58253)
```

### Python — Cluster

```python
import HyperDB
import time

cluster = HyperDB.HyperDBCluster()

# Nitro Mode — 7.8s cold open on 4GB instead of 85s
cluster.open("db_folder", "chaos", "death",
             shard_limit_bytes=512 * 1024 * 1024,
             encrypt=False)

cluster.queue_create_table("logs", [
    HyperDB.ColumnDef("log_id",  HyperDB.ColumnType.Int64),
    HyperDB.ColumnDef("message", HyperDB.ColumnType.String),
])

# bulk write — always use this for serious ingestion
batch = [
    [HyperDB.RowData("log_id", i), HyperDB.RowData("message", f"entry {i}")]
    for i in range(10000)
]
cluster.queue_write_bulk("logs", batch)

while not cluster.is_queue_empty():
    time.sleep(0)

cluster.force_flush(1)
```

### Python API Notes

- **Callbacks run on the worker thread.** The GIL is acquired automatically before your callback fires. No extra handling needed.
- **`time.sleep(0)`** in your wait loop is mandatory. It yields the main thread so the worker can actually run.
- **Types map directly**: Python `int` → integer type, `str` → `String`, `float` → `Float64`, `bool` → `Bool`. Pass the right type for the column or the coercion will silently do something unexpected.
- **`ColumnType.Bytes`** is available for raw binary data.

---

## ⚡ Encryption Mode vs. Nitro Mode

| Config | Cold Open (4.2GB) | Flush (small DB) | Security |
| :--- | :--- | :--- | :--- |
| **Fort Knox** (58k PBKDF2 iter) | `~85 seconds` | `~43 ms` | Fully encrypted. Brute-force resistant. |
| **Unlocked Fort Knox** (1 iter) | `~83 seconds` | `~0.84 ms` | Trivially brute-forceable. You saved 2 seconds. Congratulations. |
| **⚡ Nitro Mode** (no encryption) | `~7.8 seconds` | `~0.28 ms` | Plaintext flatbuffer. Readable by anyone with a hex editor. |

The cold start tax at scale is **AES-256-CTR grinding through gigabytes of ciphertext** — not PBKDF2. Dropping from 58k iterations to 1 saves ~2 seconds on a 4.2GB open. That's 2.4% of the problem. Nitro Mode saves ~77 seconds. That's the actual lever.

```cpp
// live migration — takes effect on next flush, no restart required
db.SetEncryption(true,  "new_password"); // nitro → fort knox
db.SetEncryption(false);                 // fort knox → nitro
```

**Rule of thumb:** Data that would embarrass you, get you sued, or start a federal investigation → Fort Knox. Dev data, temp buffers, local tooling → Nitro Mode. Stop paying the AES tax for data nobody wants.

---

## 🛠️ Shard Tuning

| Shard Size | Result | Why |
| :--- | :--- | :--- |
| **32 MB** | 💀 **Catastrophic** | 100+ files, 100+ worker threads, context switch hell. The Thread Hunger Games. Nobody wins. |
| **512 MB** | 🏆 **Sweet Spot** | Low fragmentation, thread count under 10, manageable SSD flushes. This is the number. |
| **1.2 GB** | 🐌 **Sluggish** | Large contiguous `std::vector` pools, TLB pressure, rising cache miss rates. Fragmentation and despair. |

Stick to `512MB`. Everything else is a cry for help.

---

## 🏗️ Scaling Reality Check

HyperDB is the **Workstation King**. Raw local ingestion throughput will smoke SQLite3, MariaDB, and MySQL on any consumer hardware.

**Where it wins:**
- Hobbyist and embedded projects — zero setup, one header, one lib
- Workstations and local dev machines — born on consumer hardware, thrives there
- High-speed local buffers and bot backends — data at RAM speed, encryption optional

**Where it fails:**
- **128-core servers** — single-threaded per shard. 127 cores watch one core work.
- **High-concurrency multi-user workloads** — not a Postgres replacement. A fast local buffer.
- **Data you cannot afford to lose mid-write** — flush-based, not ACID. Crashes between flushes lose data.

---

## 🔧 API Reference

### `HyperDBManager`

| Method | Description |
| :--- | :--- |
| `OpenDB(path, password, encrypt=true)` | Open or create a DB. `encrypt=false` = Nitro Mode. Throws on wrong password or corrupt file in encrypted mode. |
| `SetEncryption(encrypt, password)` | Live migration. Toggle AES-256 and/or change password. Takes effect on next flush. |
| `FlushDB(iterations)` | Serialize mirror to disk. No-op if not dirty or interval not elapsed. |
| `ForceFlush(iterations)` | Override interval, flush immediately. `iterations` ignored in Nitro Mode. |
| `SetFlushInterval(ms)` | Minimum ms between auto-flushes. `0` = always flush. |
| `QueueCreateTable(name, cols)` | Define a table schema. No-op if already exists. |
| `QueueDropTable(name)` | Drop table and all data. Permanent. |
| `QueueClearTable(name)` | Wipe all rows, keep schema. |
| `QueueClearColumn(table, col)` | Clear one column across all rows. |
| `QueueWrite(table, row)` | Append a single row. Non-blocking. |
| `QueueWriteBulk(table, rows)` | Append N rows in one queue push. Use this. |
| `QueueRead(table, row_id, cb)` | Async read by local row index. |
| `QueueFind(table, col, value, cb)` | Async two-pass typed column scan. Vectorizable. |
| `QueueDelete(table, col, value, cb)` | Delete matching rows. Optional callback gets deleted count. |
| `GetRowCount(table)` | Synchronous row count. Thread-safe. |
| `IsQueueEmpty()` | True when worker is idle and queue is empty. |
| `EstimateMirrorSize()` | Running in-memory size estimate. O(1). |

### `HyperDBCluster`

| Method | Description |
| :--- | :--- |
| `Open(folder, name, password, shard_limit, encrypt=true)` | Open or create a cluster. `encrypt` stored in `.manifest`, inherited by all future shards. |
| `SetEncryption(encrypt, password)` | Live migration across entire cluster. Re-serializes all shards on next flush. |
| `Flush(iterations)` | Flush all shards to disk. |
| `ForceFlush(iterations)` | Force-flush all shards immediately. |
| `SetFlushInterval(ms)` | Set flush interval on all shards. |
| `QueueCreateTable(name, cols)` | Create table on all shards. |
| `QueueDropTable(name, target)` | Drop table on target shards. |
| `QueueClearTable(name, target)` | Clear table on target shards. |
| `QueueWrite(table, row)` | Write single row to active shard. |
| `QueueWriteBulk(table, rows)` | Bulk write to active shard. Handles spillover automatically. |
| `QueueRead(table, global_row_id, cb)` | Resolves global row ID via manifest, reads from correct shard. |
| `QueueFind(table, col, value, cb, target)` | Fan-out find. Dispatches to all matching shards, merges on completion. |
| `QueueDelete(table, col, value, target)` | Fan-out delete. Cascades row ID corrections via `ShiftManifestEntries`. |
| `GetRowCount(table)` | Total rows across all shards. |
| `GetShardCount()` | Number of open shard files. |
| `GetActiveShard()` | Index of current write shard. |

### `ShardTarget`

| Value | Meaning |
| :--- | :--- |
| `ShardTarget::All` | Every shard (default) |
| `ShardTarget::ActiveOnly` | Only the current write shard |
| `ShardTarget::OldOnly` | Every shard except the active one |

---

## ⚠️ Notes for the Brave

1. **HyperDB is not ACID.** Writes go to an in-memory mirror. A crash between flushes loses everything since the last flush. This is the tradeoff for the speed numbers above. It is not a bug. It is the entire point.

2. **Encryption mode is sticky.** `encrypt=false` creates a Nitro Mode file. You cannot reopen it as encrypted — the verifier will throw. Use `SetEncryption()` to migrate a live database if you change your mind.

3. **`QueueDelete` has a callback.** `callback(int removed)` fires with the actual deleted count. The cluster depends on this for `ShiftManifestEntries` to keep manifest entries correct. Pass `nullptr` in C++ or omit in Python if you don't need it.

4. **Python callbacks run on the worker thread.** GIL is acquired automatically. Use `time.sleep(0)` in your wait loop — without it your main thread starves the worker on the L3 cache.

5. **`QueueWriteBulk` vs `QueueWrite`.** One mutex acquire per batch vs. one per row. At 10k rows/batch over 10M rows: 1,000 lock ops vs. 10,000,000. The benchmark numbers are arithmetic, not magic.

6. **Column types must match.** The `HyperValue` variant mirrors `HyperDB::ColumnType` exactly. Writing `int64_t` to an `Int32` column silently truncates. At 10M rows you won't notice until you read back garbage.

7. **Don't bypass `ShiftManifestEntries`.** Calling `ExecDelete` directly on a shard managed by a cluster drifts the manifest row ID ranges silently. Global reads will return wrong data or nothing.

8. **Thread safety model.** All `Exec*` methods are serialized through a single dedicated worker thread per shard. `FlushDB` has its own `flush_mutex_`. Do not call `Exec*` directly from outside the queue. The queue is not optional.

<div align="center">
<i>Maintained by someone deeply regretting their life choices.</i>
</div>