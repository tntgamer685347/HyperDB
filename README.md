<div align="center">

# ⚡ HyperDB

**A deeply unhinged but maliciously fast C++ NoSQL database.**

[![C++20](https://img.shields.io/badge/C++-20-blue.svg)](https://en.wikipedia.org/wiki/C%2B%2B)
[![FlatBuffers](https://img.shields.io/badge/FlatBuffers-32Bit-orange.svg)](https://google.github.io/flatbuffers/)
[![Speed](https://img.shields.io/badge/Latency-<0.05ms-success.svg)](#)
[![Mental Health](https://img.shields.io/badge/Mental%20Health-Declining-critical.svg)](#)

*When I wrote this code, only God and I understood what I was doing. Now, only God knows.*

</div>

---

## 💀 What is this?

HyperDB is an in-memory, violently fast, flatbuffer-backed embedded database written in C++20. It was born from a 3AM debugging session and a deep hatred for latency. It features **O(1)** column lookups via `unordered_map`, a **`condition_variable`-driven persistent worker queue**, **bulk write batching**, and **PBKDF2-HMAC-SHA256 / AES-256-CTR** encryption that makes sure your data is safely locked away from everyone (sometimes including yourself).

If you are looking for an ACID compliant, highly scalable, enterprise-grade cloud solution... go use Postgres. If you want to serialize your vectors into encrypted binary blobs at 0.03ms per query while redlining your CPU's L3 cache... you're in the right place.

---

## 🔥 Features that justify the technical debt

- **Zero-Variant Scan Loop**: `ExecFind` and `ExecDelete` never box a value into `HyperValue` during their hot scan pass. Typed raw pointers only. The compiler can autovectorize the integer branches. It does.
- **Two-Pass Find**: Collect matching row indices first (one column, cache-friendly, vectorizable), then build `ReadResult` rows only for hits. If matches << rows — which is the whole point — this wins dramatically.
- **Persistent Condition Variable Queue**: The worker thread sleeps on a `std::condition_variable`. No spinning. No wasted cycles. `AddToQueueBulk` drops N entries in a single lock acquisition and fires one `notify_one`. One.
- **`QueueWriteBulk`**: Batch thousands of rows into a single queue operation. This is how you hit 88k+ rows/sec without making your atomic counter cry.
- **O(1) Column & Table Addressing**: `column_map` per table, O(1) lookup via `std::unordered_map`. No linear string-searching for columns. Everything maps instantly. The old linear scan is a war story now.
- **Incremental `estimated_size_` Tracking**: `HyperDBManager` tracks mirror size as a running total updated on every write/delete/clear. `EstimateMirrorSize()` no longer loops over every column of every table — it just returns the number. `MaybeSpillToNewShard` calls this thousands of times per benchmark. You're welcome.
- **`QueueDelete` Callback**: Delete now fires an `std::function<void(int)>` with the number of rows actually removed. The cluster uses this to call `ShiftManifestEntries` and keep global row IDs consistent. No more silently corrupt manifests after a delete.
- **`ShiftManifestEntries`**: After a cross-shard delete, the cluster cascades the row count delta forward through all subsequent shard manifest entries. Global row ID resolution stays correct. This is the kind of thing that breaks at 10 million rows and not before.
- **FlatBuffers Serialization**: Zero-copy deserialization. We generate offsets in reverse topological order. *Magic. Do not touch.*
- **PBKDF2-HMAC-SHA256 & AES-256-CTR Encryption**: Because what good is a database if someone else can read it? Every file is encrypted using configurable key derivation iterations.
- **`HyperDBCluster` Auto-Sharding**: FlatBuffers has a 2GB hard limit. `HyperDBCluster` spills your data to new shards automatically, tracking row ID ranges per shard in a `.manifest` JSON file, executing writes on the active shard and fanning out `Find`/`Delete` queries transparently.

---

## 🚀 Performance Benchmarks
*(Tested on an AMD Ryzen 7 5800X doing its best not to catch fire)*

| Operation | Latency |
| :--- | :--- |
| **New DB Initialization** | `0.07 ms` |
| **Unlock existing AES DB** (58k PBKDF2 iterations) | `42.87 ms` |
| **QueueWrite (2 Rows)** | `0.04 ms` |
| **QueueRead (Async Callback)** | `0.04 ms` |
| **QueueFind (Search Logic)** | `0.03 ms` |
| **Cross-Shard Find (Multi-shard aggregation)** | `0.04 ms` |

---

## 🌪️ 10,000,000 Row "Apocalypse" Benchmark
*(3.8GB Encrypted Entropy | 512MB Shards | Ryzen 7 5800X)*

| Metric | Result |
| :--- | :--- |
| **Total Rows Written** | `10,000,000` |
| **Throughput** | `92,842 rows/sec` |
| **Total Data Size** | `4.2 GB` |
| **Total Write Loop Time** | `107.7 seconds` |
| **Final Shard Count** | `8 Shards` |

---

## 📖 "Read Apocalypse" Benchmark
*(10M Row Dataset | 8 Shards (512MB each) | Ryzen 7 5800X)*

| Operation | Latency / Throughput |
| :--- | :--- |
| **Random Global ID Read** | `5,212,430 reads/sec` |
| **100,000 Random Reads** | `19.18 ms` |
| **Full 4.2GB Multi-Shard Scan** | `8.50 ms` |
| **Cross-Shard Aggregation** | `VERIFIED (10M rows / 8 Shards)` |

---

## 🏗️ Installation & Setup

1. Include `HyperDB.h` in your project.
2. Link against the compiled `HyperDB.lib` static library.
3. **CRITICAL:** Ensure your project's include directories contain the `flatbuffers` headers and `hyperdb_generated.h`. The `.lib` statically links the FlatBuffers code, but the MSVC compiler still needs the headers to understand what a `flatbuffers::Offset` is when you try to compile. If your IDE lights up like a Christmas tree, this is why. You do **not** need `flatbuffers.lib` — it's already baked into `HyperDB.lib`.

---

## 🛠️ Usage Example

You want speed? Include `HyperDB.h` and embrace the void.

```cpp
#include "HyperDB.h"
#include <iostream>

// helper to block the main thread without causing cache-line fires.
// yield() is correct here. spin-waiting without it starves the worker thread.
// this has bitten people before. now there's a comment about it.
template<typename T>
void wait_for_queue(T& db) {
    while (!db.IsQueueEmpty()) {
        std::this_thread::yield();
    }
}

int main() {
    HyperDBManager db;

    // 1. open or create the database using a top-secret password
    db.OpenDB("users.db", "sandwich");

    // 2. define the schema
    db.QueueCreateTable("users", {
        {"id",       HyperDB::ColumnType::ColumnType_Int32},
        {"username", HyperDB::ColumnType::ColumnType_String}
    });

    // 3. write data to the queue (non-blocking)
    db.QueueWrite("users", {
        {"id",       int32_t(1337)},
        {"username", std::string("batman")}
    });

    // 4. retrieve data (async callback)
    db.QueueFind("users", "username", std::string("batman"),
        [](std::vector<ReadResult> results) {
            if (!results.empty()) {
                std::cout << "Found: "
                          << HyperDBUtil::HyperValueToString(results[0][0].value)
                          << "\n";
            }
        });

    // 5. delete with a callback telling you how many rows were removed.
    //    the cluster uses this internally to keep manifest row IDs in sync.
    //    you can use it to know if the thing you deleted actually existed.
    db.QueueDelete("users", "username", std::string("batman"),
        [](int removed) {
            std::cout << "Deleted " << removed << " row(s).\n";
        });

    // 6. spin-wait until the dedicated worker thread drains the queue
    wait_for_queue(db);

    // 7. force a synchronous disk flush
    //    58253 iterations is the default. drop to 1 if you don't care about
    //    brute-force resistance and do care about your sanity.
    db.ForceFlush(58253);

    return 0;
}
```

---

## 📦 Cluster Mode (Distributed Mojibake)

If you're going to breach the ~2GB FlatBuffers limit of a single shard, `HyperDBCluster` handles cross-file tracking automatically. Use `QueueWriteBulk` for any serious ingestion — it batches N rows into a single lock acquisition on the worker queue instead of hammering the mutex one row at a time.

```cpp
HyperDBCluster cluster;
// 512MB per shard — the sweet spot. see the tuning table below.
cluster.Open("db_folder", "chaos", "death", 512ULL * 1024 * 1024);

cluster.QueueCreateTable("system_logs", {
    {"log_id",  HyperDB::ColumnType::ColumnType_Int64},
    {"message", HyperDB::ColumnType::ColumnType_String}
});

// for massive throughput, use QueueWriteBulk.
// it batches the entire chunk into a single queue push.
// single-row QueueWrite at 10M rows means 10M mutex lock/unlock cycles.
// that's a very loud way to ask for a bad benchmark.
std::vector<std::vector<RowData>> batch;
batch.reserve(10000);
for (int i = 0; i < 10000; ++i) {
    batch.push_back({
        {"log_id",  int64_t(i)},
        {"message", std::string("log entry")}
    });
}
cluster.QueueWriteBulk("system_logs", std::move(batch));
```

The cluster tracks which row IDs live in which files via a `.manifest` JSON file, executing writes on the active shard and proxying `Find`/`Delete` queries transparently across all files. Deletes cascade a row count delta through subsequent manifest entries via `ShiftManifestEntries` so global row ID resolution stays valid after rows are removed.

> [!TIP]
> **Always use `QueueWriteBulk` for large data ingestion.** Mapping 10 million rows via single-row `QueueWrite` triggers millions of individual queue pushes, each acquiring the mutex, each firing `notify_one`. Batching into chunks of 10,000+ rows is how we hit **88k+ rows/sec** with full encryption on a single core.

> [!NOTE]
> Even with sharding, **Cluster Mode is currently limited to a single CPU core per shard.** Operations are fanned out to shards sequentially to avoid the eldritch horror of async data races. Cross-shard `Find` dispatches to all matching shards and merges results when the last one returns, but each shard's worker is still a single thread. Your 16-core CPU will be watching 15 cores eat chips while one core handles the data. Performance is still W. It's just not Overclocked W yet.

---

## 🛠️ Shard Tuning (The "Sweet Spot" from the Trenches)

Don't mess with `shard_limit` unless you've actually slept recently. After 10,000,000 rows of testing, here is the gospel:

| Shard Size | Result | Why? |
| :--- | :--- | :--- |
| **32 MB** | 💀 **Catastrophic** | 100+ `.db` files. Each one has its own persistent worker thread. Your CPU spends more time context-switching between 108 threads than actually touching your data. Thread Hunger Games. Everyone loses. |
| **512 MB** | 🏆 **Pure W** | The magic middle. Low memory fragmentation, thread count under 10, SSD flushes are manageable. This is the number. |
| **1.2 GB** | 🐌 **Sluggish** | Too much data in a single `std::vector` pool. TLB pressure increases. Cache miss rates climb. Large contiguous allocations are a recipe for fragmentation and despair. |

**Recommendation:** Stick to `512MB` or `1024MB`. Anything else is a cry for help.

---

## 🏗️ Scaling Reality Check (Workstation vs. Datacenter)

This database is the **Workstation King**. On anything from a 2GHz potato to a 5GHz gaming rig, HyperDB will smoke `SQLite3`, `MariaDB`, and `MySQL` in raw latency and throughput for local workloads.

**Where it wins:**
- **Hobbyist / Embedded Projects**: Zero setup, one header, one lib, a dream and a password.
- **Workstations / Old PCs**: Thrives on consumer hardware. Was born there. Feels at home.
- **High-Speed Local Buffers**: When you need local data to move at RAM speed and disk encryption comes standard.

**Where it fails (the Despair Zone):**
- **128-Core Servers**: HyperDB is single-threaded per shard. Your 128-core server will watch 127 cores do absolutely nothing while one core struggles with your multi-terabyte dataset. Don't. Just don't.
- **High Concurrency Multi-User**: This is not a Postgres replacement. It is a high-speed local buffer with a great serialization story.

If you're building a website for a million concurrent users, use a real server database. If you're building a high-speed local app that needs to feel like it's from the future, you're in the right place.

---

## 🗄️ Dataset Speed Expectations (The "Cold Start" Tax)

- **Sweet Spot (10 – 500,000 Rows)**: HyperDB is an absolute god. Loads instantly, saves instantly, scans are basically free. This is the core target.
- **The Millionaire Problem (1M – 10M Rows)**: Once you cross a million rows in a single table, the **Cold Start Tax** kicks in. Your app will still crunch that data in milliseconds — but **loading and saving** will take 15–100 seconds because it has to run PBKDF2 key derivation and physically encrypt gigabytes of state. The data processing is fast. The cryptography is not.

**Summary:** If your app can afford a "Loading..." screen at boot, go crazy. If not, keep shards lean and row counts under a million per cluster.

---

## ⚡ Speed Hack (Security vs. Sanity)

The **Cold Start Tax** is almost entirely the cost of **PBKDF2 iterations** (`58,253` by default).

- **Security Mode (`58253` iterations)**: Brute-force resistant. Your data is a fortress. You'll wait in line to get in.
- **Speed Mode (`1` iteration)**: If you don't care about someone guessing your password, dropping to `1` will make your 10M row database load in seconds. The remaining cost is the raw AES-256-CTR decryption pass over the ciphertext, which is fast.

```cpp
// security mode — the responsible adult choice
db.ForceFlush(58253);

// speed mode — you've made your peace with the consequences
db.ForceFlush(1);
```

Choose wisely. Or don't. You're already here.

---

## 🔧 API Reference

### `HyperDBManager`

| Method | Description |
| :--- | :--- |
| `OpenDB(path, password)` | Open or create an encrypted database file. Throws on wrong password or corrupt data. |
| `FlushDB(iterations)` | Serialize and encrypt the mirror to disk. No-op if not dirty or interval hasn't elapsed. |
| `ForceFlush(iterations)` | Override interval and flush immediately. |
| `SetFlushInterval(ms)` | Minimum milliseconds between flushes. `0` = always flush. |
| `QueueCreateTable(name, cols)` | Define a table schema. Ignored if the table already exists. |
| `QueueDropTable(name)` | Remove a table and all its data. |
| `QueueClearTable(name)` | Wipe all rows but keep the schema. |
| `QueueClearColumn(table, col)` | Wipe a single column's data across all rows. |
| `QueueWrite(table, row)` | Append a single row. Non-blocking. |
| `QueueWriteBulk(table, rows)` | Append N rows in a single queue push. **Use this for any serious ingestion.** |
| `QueueRead(table, row_id, cb)` | Async read by local row index. Fires callback with `ReadResult`. |
| `QueueFind(table, col, value, cb)` | Async typed column scan. Two-pass. Vectorizable. Fast. |
| `QueueDelete(table, col, value, cb)` | Delete all rows matching value. Optional callback receives deleted row count. |
| `GetRowCount(table)` | Synchronous row count. Thread-safe. |
| `IsQueueEmpty()` | Returns true when the worker thread is idle and the queue is empty. |
| `EstimateMirrorSize()` | Returns running estimated byte size of in-memory data. O(1). |

### `HyperDBCluster`

| Method | Description |
| :--- | :--- |
| `Open(folder, name, password, shard_limit)` | Open or create a sharded cluster. Creates `folder/` if needed. |
| `QueueWriteBulk(table, rows)` | Batch write. Handles shard spillover across the batch automatically. |
| `QueueRead(table, global_row_id, cb)` | Resolves global row ID to the correct shard and local index via manifest. |
| `QueueFind(table, col, value, cb, target)` | Fan-out find across shards matching `target`. Merges results when all return. |
| `QueueDelete(table, col, value, target)` | Fan-out delete. Fires `ShiftManifestEntries` to cascade row ID corrections. |
| `GetShardCount()` | Returns number of open shard files. |
| `GetActiveShard()` | Returns the index of the currently active write shard. |

### `ShardTarget`

| Value | Meaning |
| :--- | :--- |
| `ShardTarget::All` | Every shard (default for Find/Delete) |
| `ShardTarget::ActiveOnly` | Only the current write shard |
| `ShardTarget::OldOnly` | Every shard except the active one |

---

## ⚠️ Notes for the Brave

1. **`QueueDelete` now has a callback.** `void QueueDelete(table, col, value, callback)` fires `callback(int removed)` with the number of rows deleted. The cluster depends on this for manifest consistency. If you're using `HyperDBManager` directly and deleting from sharded data, you want this number. Pass `nullptr` if you genuinely don't care, which is allowed.

2. **`HyperValue` Types**: This `std::variant` mirrors `HyperDB::ColumnType` exactly. If you add a type there and forget to update the `switch` statements in `HyperDB.cpp`, you will suffer. This is not a threat. It is a prophecy.

3. **`QueueWriteBulk` vs. `QueueWrite`**: `QueueWrite` acquires the mutex once per row. `QueueWriteBulk` acquires it once for the entire batch. At 10,000 rows per batch and 10 million total rows, that's the difference between 10,000 lock acquisitions and 10,000,000. The benchmark numbers are not magic. They are arithmetic.

4. **`IsQueueEmpty()` Spin-Waiting**: Always `yield()` inside your wait loop. Without it, your main thread will starve the worker thread on the L3 cache by hammering the atomic counter. This seems like it shouldn't matter until it does, at which point you're staring at a performance cliff with no explanation.

5. **The `estimated_size_` counter**: `HyperDBManager` maintains a running estimate of mirror size, updated on every write, delete, and clear. `MaybeSpillToNewShard` uses this instead of summing every column on every call. At 88k writes/sec this matters. The estimate can drift slightly after edge cases, but it's close enough for shard spillover decisions where the limit has megabytes of headroom anyway.

6. **`ShiftManifestEntries`**: After a delete, the cluster adjusts the `row_end` of the affected shard's manifest entry and cascades the delta to all subsequent shard boundaries. If you bypass this — by calling `ExecDelete` directly on a shard managed by a cluster, for example — your global row IDs will silently drift out of alignment. Don't do that.

7. **Thread Safety**: All `Exec*` methods are serialized through the single worker thread per shard. `FlushDB` has its own `flush_mutex_` to prevent concurrent flushes. `data_mutex_` protects the mirror during all read/write operations. Do not call `Exec*` methods directly. That's what the queue is for. The queue is not optional.

<div align="center">
<i>Maintained by someone deeply regretting their life choices.</i>
</div>