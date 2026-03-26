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

HyperDB is an in-memory, violently fast, flatbuffer-backed embedded database written in C++. It was born from a 3AM debugging session and a deep hatred for latency. It features **O(1)** column lookups, a **lock-free condition variable worker queue**, and **PBKDF2/AES-256** encryption that makes sure your data is safely locked away from everyone (sometimes including yourself). 

If you are looking for an ACID compliant, highly scalable, enterprise-grade cloud solution... go use Postgres. If you want to serialize your vectors into encrypted binary blobs at 0.03ms per query while redlining your CPU's L3 cache... you're in the right place.

---

## 🔥 Features that justify the technical debt

- **Zero-Variant Dispatch**: We don't box data in hot `Find` or `Delete` loops. Typed vectors are scanned natively. It's fast. Very fast.
- **Lock-Free Asynchronous Queue**: The main thread pushes data via atomic counters and `std::condition_variable` magic. Queue `Read` operations take **~0.03ms** without starving the worker thread.
- **O(1) Column Addressing**: No linear string-searching for columns. Everything maps instantly through `std::unordered_map`.
- **FlatBuffers Serialization**: Zero-copy deserialization. We generate offsets in reverse topological order. *Magic. Do not touch.*
- **PBKDF2-HMAC-SHA256 & AES-256 Encryption**: Because what good is a database if someone else can read it? Every file is encrypted using heavy key derivation.
- **`HyperDBCluster` Auto-Sharding**: Flatbuffers have a hard limit. `HyperDBCluster` will automatically spill your data over to new shards entirely under the hood, updating manifest files on the fly.

---

## 🚀 Performance Benchmarks
*(Tested on an AMD Ryzen 7 5800X doing its best not to catch fire)*

| Operation | Latency |
| :--- | :--- |
| **New DB Initialization** | `0.07 ms` |
| **Unlock existing AES DB** (58k PBKDF2 iterations) | `42.87 ms` |
| **Queue Write (2 Rows)** | `0.04 ms` |
| **Queue Read (Async Callback)** | `0.04 ms` |
| **Queue Find (Search Logic)** | `0.03 ms` |
| **Cross-Shard Find (Multi-shard aggregation)** | `0.04 ms` |

---

## 🌪️ 10,000,000 Row "Apocalypse" Benchmark
*(3.8GB Encrpyted Entropy | 512MB Shards | Ryzen 7 5800X)*

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
3. **CRITICAL:** Ensure your project's include directories contain the `flatbuffers` headers and `hyperdb_generated.h`. The `.lib` statically links the Flatbuffers code, but the MSVC compiler still needs the headers to understand what a `flatbuffers::Offset` is when you try to compile. If your IDE lights up like a Christmas tree, this is why. (YOU DO NOT NEED THE flatbuffers.lib. its included in HyperDB.lib.)

---

## 🛠️ Usage Example

You want speed? Include `HyperDB.h` and embrace the void.

```cpp
#include "HyperDB.h"
#include <iostream>

// Helper to block the main thread without causing cache-line fires
template<typename T>
void wait_for_queue(T& db) {
    while (!db.IsQueueEmpty()) {
        std::this_thread::yield(); 
    }
}

int main() {
    HyperDBManager db;

    // 1. Open or create the database using a top-secret password
    db.OpenDB("users.db", "sandwich");

    // 2. Define the schema
    db.QueueCreateTable("users", {
        {"id", HyperDB::ColumnType::ColumnType_Int32},
        {"username", HyperDB::ColumnType::ColumnType_String}
    });

    // 3. Write data to the queue (Non-blocking)
    db.QueueWrite("users", { 
        {"id", int32_t(1337)}, 
        {"username", std::string("batman")} 
    });

    // 4. Retrieve data (Async Callback)
    db.QueueFind("users", "username", std::string("batman"), [](std::vector<ReadResult> results) {
        if (!results.empty()) {
            std::cout << "Found User: " << HyperDBUtil::HyperValueToString(results[0][0].value) << "\n";
        }
    });

    // 5. Spin-wait until the dedicated worker thread drains the queue
    wait_for_queue(db);

    // 6. Force a synchronous disk flush (generates AES key & writes flatbuffer)
    db.ForceFlush(300000); 

    return 0;
}
```

---

## 📦 Cluster Mode (Distributed Mojibake)

If you're going to breach the 1.5GB limit of a single flatbuffer shard, `HyperDBCluster` handles cross-file tracking automatically. 

```cpp
HyperDBCluster cluster;
// Limit each shard to 500MB for testing
cluster.Open("db_folder", "chaos", "death", 512ULL * 1024 * 1024);

cluster.QueueCreateTable("system_logs", {
    {"log_id", HyperDB::ColumnType::ColumnType_Int64},
    {"message", HyperDB::ColumnType::ColumnType_String}
});

// For massive throughput, use QueueWriteBulk to avoid task-scheduling overhead.
// It automatically handles shard spillover across the entire batch!
std::vector<std::vector<RowData>> batch;
for (int i=0; i<10000; ++i) {
    batch.push_back({ {"log_id", int64_t(i)}, {"message", std::string("log entry")} });
}
cluster.QueueWriteBulk("system_logs", std::move(batch));
```
It tracks which row IDs live in which files using a `.manifest`, executing writes on the active shard while proxying `Find`/`Delete` queries transparently across all files.

> [!TIP]
> **Always use `QueueWriteBulk` for large data ingestion.** Mapping 10 million rows via single-row `QueueWrite` triggers millions of atomic operations and task-scheduling events. Batching them into chunks of 10,000+ rows is how we hit **88k+ rows/sec** with full encryption.

> [!NOTE]
> Even with sharding, **Cluster Mode is currently limited to a single CPU core.** Operations are fanned out to shards sequentially to avoid the eldritch horror of async data races. This means if you have 50 shards, your 16-core CPU will still be watching from the sidelines while one core does all the heavy lifting. Performance is still "W speed," but it's not "Overclocked W" yet.

---

## 🛠️ Shard Tuning (The "Sweet Spot" from the Trenches)
Don't mess with the `shard_limit` unless you've actually slept recently. After 10,000,000 rows of testing, here is the gospel:

| Shard Size | Result | Why? |
| :--- | :--- | :--- |
| **32 MB** | 💀 **Catastrophic** | You end up with 100+ `.db` files. Each one has its own worker thread. Your CPU will spend more time context-switching between 108 threads than actually reading your data. It's a "Thread Hunger Games" where everyone loses. |
| **512 MB** | 🏆 **Pure W** | The magic middle. Low enough to avoid memory fragmentation, high enough to keep the thread count under 10. SSD flushes are manageable. |
| **1.2 GB** | 🐌 **Sluggish** | Too much data in a single `std::vector`. Your CPU's TLB and cache will start screaming. Large memory blocks are a recipe for fragmentation and despair. |

**Recommendation:** Stick to `512MB` or `1024MB`. Anything else is a cry for help.

---

## 🏗️ Scaling Reality Check (Workstation vs. Datacenter)
Listen carefully. This database is the **Workstation King**. If you're on a normal PC (anything from a 2GHz potato to a 5GHz gaming rig), HyperDB will absolutely **smoke** `SQLite3`, `MariaDB`, and `MySQL` in raw latency and throughput. It was born in the trenches of consumer hardware.

**Where it wins:**
*   **Hobbyist Projects**: Zero setup, just a header and a dream.
*   **Workstations / Old PCs**: It's incredibly light and thrives on 2GHz to 5GHz clock speeds.
*   **Single-User / Embedded**: When you need local data to move at the speed of your RAM.

**Where it fails (The "Despair" starts here):**
*   **128-Core Servers**: If you have 256GB of RAM and 12TB of NVMe disk, **do not use this**. HyperDB is single-threaded (per shard). Your 128-core server will be watching 127 cores do absolutely nothing while 1 core struggles to push your multi-terabyte dataset. 
*   **High Concurrency**: This is not a multi-user Postgres replacement. It's a high-speed local buffer.

If you're building a website for a million users, go use a "real" server DB. If you're building a high-speed local app that needs to feel like it's from the future? **You're in the right place.**

---

## 🗄️ Dataset Speed Expectations (The "Cold Start" Tax)
You’ve seen the benchmarks. Let’s talk about reality. 

*   **Sweet Spot (10 - 500,000 Rows)**: HyperDB is an absolute god. It loads instantly, saves instantly, and scans are basically "free." This is the core target. 
*   **The "Millionaire" Problem (1M - 10M Rows)**: Once you cross the million-row mark in a single table, the **Cold Start Tax** kicks in. Your app will still crunch that data in milliseconds, but **loading and saving** will take 15 - 100 seconds because it has to derive AES keys from your PBKDF2 salt and physically encrypt gigabytes of state file. 

**Summary:** If your app can afford a 20-second "Loading Profile..." screen at boot, go crazy. If not, try to keep your shards lean and your row counts under a million per cluster. 

---

## ⚡ Speed Hack (Security vs. Sanity)
If you’re reading this and thinking, "85s? I don't have time for this," listen up.

The **Cold Start Tax** is mostly the cost of **PBKDF2 Iterations (58,253 by default)**. 
*   **Security Mode**: 58k+ iterations. Brute-force resistant. Your data is a fortress, but you’ll be waiting in line to get in. 
*   **Speed Mode**: Set `iterations` to **1**. If you don't care about someone trying to guess your password, dropping this to 1 will make your 10M row database load in mere seconds. 

At 1 iteration, the only "tax" remaining is the SHA256 hashing and the physical CPU cost of AES-256 decryption. **Choose your destiny wisely.**

---

## ⚠️ Notes for the Brave

1. **`HyperValue` Types**: This `std::variant` mirrors `HyperDB::ColumnType` exactly. If you add a type there and forget to update the `switch` statements in `HyperDB.cpp`, you will suffer. I guarantee it.
2. **Parallelism**: The `HyperDBQueue` worker runs on a deeply detached thread sequentially. Adding parallelism to the worker queue itself is how you get a 3AM debugging session. Don't.
3. **Queue Empty Spinning**: `IsQueueEmpty()` is completely lock-free. Always call `std::this_thread::yield();` inside your check loops, or your main thread will starve the worker thread on the L3 cache.

<div align="center">
<i>Maintained by someone deeply regretting their life choices.</i>
</div>
