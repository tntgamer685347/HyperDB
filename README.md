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
*(Tested on an AMD Ryzen 5800X doing its best not to catch fire)*

| Operation | Latency |
| :--- | :--- |
| **New DB Initialization** | `0.07 ms` |
| **Unlock existing AES DB** (58k PBKDF2 iterations) | `43.14 ms` |
| **Queue Write (2 Rows)** | `0.04 ms` |
| **Queue Read (Async Callback)** | `0.04 ms` |
| **Queue Find (Search Logic)** | `0.03 ms` |
| **Cross-Shard Find (Multi-file aggregation)** | `0.03 ms` |

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
// Limit each shard to 500 bytes for testing
cluster.Open("db_folder", "chaos", "death", 500);

cluster.QueueCreateTable("system_logs", {
    {"log_id", HyperDB::ColumnType::ColumnType_Int64},
    {"message", HyperDB::ColumnType::ColumnType_String}
});

// Spamming data will automatically trigger Shard Spillover events.
cluster.QueueWrite("system_logs", {
    {"log_id", int64_t(1)},
    {"message", std::string("Triggering a shard spillover event right now")}
});
```
It tracks which row IDs live in which files using a `.manifest`, executing writes on the active shard while proxying `Find`/`Delete` queries transparently across all files.

---

## ⚠️ Notes for the Brave

1. **`HyperValue` Types**: This `std::variant` mirrors `HyperDB::ColumnType` exactly. If you add a type there and forget to update the `switch` statements in `HyperDB.cpp`, you will suffer. I guarantee it.
2. **Parallelism**: The `HyperDBQueue` worker runs on a deeply detached thread sequentially. Adding parallelism to the worker queue itself is how you get a 3AM debugging session. Don't.
3. **Queue Empty Spinning**: `IsQueueEmpty()` is completely lock-free. Always call `std::this_thread::yield();` inside your check loops, or your main thread will starve the worker thread on the L3 cache.

<div align="center">
<i>Maintained by someone deeply regretting their life choices.</i>
</div>
