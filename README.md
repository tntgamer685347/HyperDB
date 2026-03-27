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

HyperDB is an in-memory, violently fast, flatbuffer-backed embedded NoSQL database written in C++20. It was born from a 3AM debugging session and a deep hatred for latency. It features **O(1)** column lookups, a **`condition_variable`-driven persistent worker queue**, **bulk write batching**, **optional AES-256-CTR encryption with live migration**, **`BCryptGenRandom`-backed secure IV/salt generation** on Windows, and **Python bindings** via pybind11.

Fort Knox when you need it. Raw FlatBuffers when you don't. Live migration between the two without restarting.

If you are looking for an ACID compliant, highly scalable, enterprise-grade cloud solution... go use Postgres. If you want to serialize your vectors into flatbuffer blobs at 0.03ms per query while redlining your CPU's L3 cache... you're in the right place.

---

## 🔥 Features that justify the technical debt

- **Zero-Variant Scan Loop**: `ExecFind` and `ExecDelete` never box values into `HyperValue` during the hot scan pass. Typed raw pointers only. The compiler can autovectorize the integer branches. It does.
- **Two-Pass Find**: Collect matching row indices first (one column, cache-friendly, vectorizable), then build `ReadResult` rows only for hits. If matches << rows — which is the entire point — this wins dramatically.
- **Persistent Condition Variable Queue**: The worker thread sleeps on a `std::condition_variable`. No spinning. No wasted cycles. `AddToQueueBulk` drops N entries in a single lock acquisition and fires one `notify_one`.
- **`QueueWriteBulk` / `WriteBulk` Action**: Bulk writes are now a dedicated queue action type (`WriteBulk`). The entire batch is processed in a single `ExecWriteBulk` call — one lock acquire, one pass through the data, one `estimated_size_` update at the end. No per-row lock churn.
- **Sparse Write Padding**: If a row write omits a column, HyperDB pads that column with the appropriate zero/empty default value. Row counts stay consistent. The DB doesn't become a digital graveyard.
- **O(1) Column & Table Addressing**: `column_map` per table and `table_map` per database via `std::unordered_map`. No linear string-searching anywhere in the hot path.
- **`Bytes` Column Type**: `ColumnType::Bytes` stores raw `std::vector<uint8_t>` blobs. Serialize binary data directly. Find and delete on byte columns is supported. Serializes to FlatBuffers as a vector of raw string offsets.
- **Atomic `estimated_size_`**: Mirror size is tracked as `std::atomic<size_t>`, updated on every write/delete/clear. `EstimateMirrorSize()` is a single atomic load — O(1), lock-free.
- **`QueueDelete` Callback**: Delete fires `std::function<void(int)>` with the actual removed row count. The cluster uses this to keep manifest row IDs consistent after deletions.
- **`ShiftManifestEntries`**: After a cross-shard delete, the cluster cascades the row count delta forward through all subsequent manifest entries. Global row ID resolution stays correct.
- **Optional AES-256-CTR Encryption**: Fort Knox mode (`encrypt=true`, default) or Nitro Mode (`encrypt=false`). The cluster persists this in the `.manifest` — all future shards inherit it.
- **`SetEncryption` Live Migration**: Toggle encryption or change password on a live open database or entire cluster. Re-serializes everything on next flush. Benchmarked on 4.2GB: Nitro→Encrypted in ~84s, Encrypted→Nitro in ~8.2s.
- **PBKDF2 Iteration Security Floor**: HyperDB enforces a minimum of 10,000 PBKDF2 iterations on load and save in encrypted mode. Tampered files or ancient DBs with sub-10k iterations are bumped up automatically. You can't accidentally save with 1 iteration and call it encrypted.
- **`BCryptGenRandom` on Windows**: IV and salt generation uses `BCryptGenRandom` via the Windows CNG API instead of `std::random_device` in a loop. Proper CSPRNG for cryptographic material. Falls back to `std::random_device` on non-Windows.
- **Throttled Manifest Saves**: The cluster manifest is written at most once per second (`MANIFEST_SAVE_INTERVAL_MS = 1000`). High-throughput bulk writes no longer hammer the filesystem with a manifest rewrite per batch.
- **Worker Thread Exception Guard**: The worker thread catches and logs exceptions instead of dying. A bad callback or a corrupt entry won't take the entire shard offline.
- **Python Bindings**: Full pybind11 bindings exposing the complete API with GIL correctly acquired on callback threads.
- **`HyperDBCluster` Auto-Sharding**: FlatBuffers has a ~2GB hard limit. `HyperDBCluster` spills data to new shards automatically, tracking row ID ranges in a `.manifest`, fanning out `Find`/`Delete` transparently.

---

## 🚀 Performance Benchmarks

*(C++ benchmarks on AMD Ryzen 7 5800X | SQLite3 comparison on Intel i5-8400 2.8GHz bare metal Proxmox host)*

### Solo Operations

| Operation | Nitro Mode | Encrypted |
| :--- | :--- | :--- |
| **New DB Init** | `0.07 ms` | `0.07 ms` |
| **Flush small DB** | `8.02 ms` | `43.65 ms` *(58k PBKDF2)* |
| **QueueWrite (2 Rows)** | `0.03 ms` | `0.03 ms` |
| **QueueRead (Async)** | `0.03 ms` | `0.04 ms` |
| **QueueFind** | `0.02 ms` | `0.03 ms` |

> The Nitro flush going from `0.28ms` to `8ms` is expected — this includes the FlatBuffers serialization pass over the entire mirror. The `0.28ms` figure from earlier benchmarks was for a nearly-empty DB. A realistic small DB with actual data serializes in ~8ms, which is still fast.

### 10,000,000 Row "Apocalypse" Write Benchmark
*(4.2GB | 512MB shards | 3× 128-byte string columns per row)*

| Metric | Nitro Mode | Encrypted (58k iter) |
| :--- | :--- | :--- |
| **Total Write Loop Time** | `27.4 seconds` | `109.9 seconds` |
| **Throughput** | `~365,000 rows/sec` | `~91,000 rows/sec` |
| **Cluster Finalize (Flush All)** | `157 ms` | `1,721 ms` |
| **Final Shard Count** | `8 Shards` | `8 Shards` |
| **Total Data Size** | `4.2 GB` | `4.2 GB` |

### 10,000,000 Row "Read Apocalypse" + Live Migration Benchmark
*(Same 4.2GB dataset, cold opens, full migration cycle)*

| Operation | Nitro Mode | Encrypted |
| :--- | :--- | :--- |
| **Cold Open (4.2GB, 8 shards)** | `~8.2 seconds` | `~85.4 seconds` |
| **100,000 Random Reads** | `20.9 ms` | `22.7 ms` |
| **Random Read Throughput** | `4,747,890 reads/sec` | `4,373,400 reads/sec` |
| **Full 4.2GB Cross-Shard Scan** | `8.57 ms` | `8.84 ms` |
| **Live Migration: Nitro → Encrypted** | — | `84,431 ms` (~84s) |
| **Live Migration: Encrypted → Nitro** | `8,201 ms` (~8.2s) | — |

Once data is in memory, encrypted and unencrypted are nearly identical in speed — the read and scan numbers are within noise of each other. The entire gap is in cold open and migration, which is AES grinding through 4.2GB of ciphertext. In-memory operations don't care about encryption mode.

---

## 🆚 HyperDB vs. SQLite3
*(10,000,000 rows — 3× 128-byte binary columns)*

| | HyperDB Nitro (Ryzen 5800X) | HyperDB Encrypted (Ryzen 5800X) | SQLite3 "optimized" (i5-8400 bare metal) |
| :--- | :--- | :--- | :--- |
| **10M row write** | `27.4 sec` | `109.9 sec` | `~92 sec`* |
| **Durability** | Flush-based | Flush-based | Per-commit |
| **Cold open 4.2GB** | `8.2 sec` | `85.4 sec` | N/A |
| **Full dataset scan** | `8.6 ms` | `8.8 ms` | N/A |

*SQLite3 tested with `PRAGMA synchronous = OFF` and `PRAGMA journal_mode = MEMORY` — durability fully disabled to match HyperDB's non-ACID model. Total observed runtime was 257s for 20M rows on the i5-8400; 72 seconds was pre-generation of random binary data leaving ~185s of actual write time (~92s equivalent for 10M). HyperDB generated data concurrently during writes and still finished in 27s.*

The i5-8400 is real Coffee Lake silicon — AES-NI, proper IPC, not a potato. SQLite with durability disabled vs. HyperDB Nitro Mode — same guarantees (none) — and HyperDB is **~3.4x faster** on comparable hardware. No pragma fixes the B-tree.

> [!WARNING]
> HyperDB is **not ACID compliant**. A crash between writes and a flush loses everything since the last flush. SQLite with default settings does not have this problem. Choose your tradeoff before you need it.

---

## 📊 Solo vs. Cluster — Know Which One You Need

`HyperDBCluster` is not just "more storage." It changes the performance profile in ways worth understanding before you commit to an architecture.

**Writes are slower in a cluster.** Every `QueueWrite` goes through `MaybeSpillToNewShard`, manifest tracking, and global row ID accounting on top of the normal queue path. The manifest save is throttled (max once per second) so it doesn't hammer your disk, but the overhead is still real. A single `HyperDBManager` will always write faster than a cluster at the same row count.

**Reads and searches are where the cluster pays off.** A cross-shard `QueueFind` dispatches to all shard worker threads simultaneously — each shard scans its own 512MB slice in parallel and results are merged when they all return. The 8.6ms full 4.2GB scan on 8 shards is proof: parallel beats sequential at this scale.

| Scenario | Use |
| :--- | :--- |
| **< 500k rows, write throughput is the priority** | `HyperDBManager` — no manifest overhead, maximum write speed |
| **> 1M rows, search and read heavy** | `HyperDBCluster` — parallel fan-out, shard-local scans |
| **> 1.5GB total data** | `HyperDBCluster` — required, FlatBuffers cannot go higher |

---

## 🏗️ Installation & Setup (C++)

1. Include `HyperDB.h` in your project.
2. Link against `HyperDB.lib`.
3. On Windows, `bcrypt.lib` is linked automatically via `#pragma comment`. If you're on Linux, `SecureRandomBytes` falls back to `std::random_device`.
4. **CRITICAL:** Your include path must contain the `flatbuffers` headers and `hyperdb_generated.h`. The `.lib` statically links FlatBuffers but MSVC still needs the headers to compile your code. If your IDE lights up like a Christmas tree, this is why. You do **not** need `flatbuffers.lib` — it's already baked into `HyperDB.lib`.

---

## 🛠️ C++ Usage Example

```cpp
#include "HyperDB.h"
#include <iostream>

template<typename T>
void wait_for_queue(T& db) {
    while (!db.IsQueueEmpty())
        std::this_thread::yield(); // mandatory. without it your main thread
}                                  // starves the worker on the L3 cache.

int main() {
    HyperDBManager db;

    // Nitro Mode — raw flatbuffers, instant open, no protection.
    db.OpenDB("users.db", "", false);

    // Fort Knox (default) — AES-256-CTR + PBKDF2. encrypted at rest.
    // db.OpenDB("users.db", "sandwich");

    db.QueueCreateTable("users", {
        {"id",       HyperDB::ColumnType::ColumnType_Int32},
        {"username", HyperDB::ColumnType::ColumnType_String},
        {"avatar",   HyperDB::ColumnType::ColumnType_Bytes}  // binary blob column
    });

    // sparse write — omitting "avatar" is fine, it gets padded with a zero-length byte vec
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
    db.ForceFlush();

    // live migration — no restart, takes effect on next flush
    db.SetEncryption(true, "sandwich"); // nitro → fort knox
    db.ForceFlush(58253);               // now encrypted on disk
    return 0;
}
```

---

## 📦 Cluster Mode (C++)

```cpp
HyperDBCluster cluster;

// nitro mode — 8.2s cold open on 4.2GB
cluster.Open("db_folder", "chaos", "", 512ULL * 1024 * 1024, false);

// fort knox — 85s cold open on 4.2GB, but your data is actually safe
// cluster.Open("db_folder", "chaos", "death", 512ULL * 1024 * 1024);

cluster.QueueCreateTable("logs", {
    {"log_id",  HyperDB::ColumnType::ColumnType_Int64},
    {"message", HyperDB::ColumnType::ColumnType_String},
    {"payload", HyperDB::ColumnType::ColumnType_Bytes}
});

// always use QueueWriteBulk for serious ingestion.
// single-row QueueWrite = one queue push per row.
// QueueWriteBulk = one queue push for the whole batch, one lock acquire,
// one ExecWriteBulk call. at 10k rows/batch, 10M total: 1,000 vs 10,000,000.
std::vector<std::vector<RowData>> batch;
batch.reserve(10000);
for (int i = 0; i < 10000; ++i)
    batch.push_back({{"log_id", int64_t(i)}, {"message", std::string("entry")}});
cluster.QueueWriteBulk("logs", std::move(batch));

// live migration of the entire cluster — all shards, manifest updated
cluster.SetEncryption(true, "death"); // nitro → fort knox (~84s on 4.2GB)
cluster.ForceFlush(58253);
```

> [!TIP]
> **Always use `QueueWriteBulk` for bulk ingestion.** The `WriteBulk` action type processes the entire batch in a single `ExecWriteBulk` call: one lock acquire, one pass, one atomic size update. This is how we hit 365k rows/sec in Nitro Mode.

> [!NOTE]
> **Cluster writes go to the active shard only.** Cross-shard `Find` and `Delete` dispatch to all matching shards in parallel — each shard's worker runs independently. This is why 4.2GB scans finish in under 10ms. Writes don't parallelize because only the active shard accepts new data.

---

## 🐍 Python Bindings

Build `HyperDB.pyd` (Windows) or `HyperDB.so` (Linux) from `bindings.cpp`. Requires Python 3.x and the pybind11 headers.

### Python — Solo DB

```python
import HyperDB
import time

db = HyperDB.HyperDBManager()

# Nitro Mode
db.open_db("users.db", encrypt=False)

# Fort Knox (default)
# db.open_db("users.db", "sandwich")

db.queue_create_table("users", [
    HyperDB.ColumnDef("id",       HyperDB.ColumnType.Int32),
    HyperDB.ColumnDef("username", HyperDB.ColumnType.String),
    HyperDB.ColumnDef("avatar",   HyperDB.ColumnType.Bytes),
])

# sparse write — avatar is omitted, padded automatically with empty bytes
db.queue_write("users", [
    HyperDB.RowData("id",       1337),
    HyperDB.RowData("username", "batman"),
])

# callback fires on the worker thread — GIL acquired automatically
db.queue_find("users", "username", "batman",
    lambda results: print(f"Found: {results[0][1].value}") if results else None
)

db.queue_delete("users", "username", "batman",
    lambda count: print(f"Deleted {count} row(s)")
)

# time.sleep(0) = std::this_thread::yield(). mandatory in wait loops.
while not db.is_queue_empty():
    time.sleep(0)

db.force_flush()

# live migration
db.set_encryption(True, "sandwich")
db.force_flush(58253)
```

### Python — Cluster

```python
import HyperDB
import time

cluster = HyperDB.HyperDBCluster()
cluster.open("db_folder", "chaos", "",
             shard_limit_bytes=512 * 1024 * 1024,
             encrypt=False)

cluster.queue_create_table("logs", [
    HyperDB.ColumnDef("log_id",  HyperDB.ColumnType.Int64),
    HyperDB.ColumnDef("message", HyperDB.ColumnType.String),
])

batch = [
    [HyperDB.RowData("log_id", i), HyperDB.RowData("message", f"entry {i}")]
    for i in range(10000)
]
cluster.queue_write_bulk("logs", batch)

while not cluster.is_queue_empty():
    time.sleep(0)

cluster.force_flush()
```

### Python API Notes

- **Callbacks run on the worker thread.** The GIL is acquired automatically before your callback fires. You don't need to do anything special.
- **`time.sleep(0)`** in wait loops is mandatory. Without it your main thread starves the worker on the L3 cache.
- **Omit columns freely.** HyperDB pads any unwritten columns with zero/empty defaults. The schema stays consistent.
- **`ColumnType.Bytes`** accepts Python `bytes` objects for raw binary storage.

---

## ⚡ Encryption Mode vs. Nitro Mode

| Config | Cold Open (4.2GB) | Flush (small DB) | Security |
| :--- | :--- | :--- | :--- |
| **Fort Knox** (58k PBKDF2 iter, floor enforced) | `~85.4 seconds` | `~43 ms` | Fully encrypted. Brute-force resistant. |
| **⚡ Nitro Mode** (no encryption) | `~8.2 seconds` | `~8 ms` | Plaintext flatbuffer. Readable by anyone with a hex editor. |

The cold open gap is entirely **AES-256-CTR decrypting 4.2GB of ciphertext** on load — not PBKDF2. PBKDF2 at 10k+ iterations costs a few hundred milliseconds per shard. AES at 4.2GB costs everything else. Nitro Mode removes the AES pass entirely: 8.2 seconds is just raw SSD read speed plus FlatBuffers deserialization.

**Live migration** is a `SetEncryption()` call followed by a flush:

```cpp
// nitro → fort knox (~84s on 4.2GB — re-encrypts every shard)
cluster.SetEncryption(true, "password");
cluster.ForceFlush(58253);

// fort knox → nitro (~8.2s on 4.2GB — strips AES, writes raw flatbuffers)
cluster.SetEncryption(false);
cluster.ForceFlush();
```

The asymmetry makes sense: encrypting requires AES-CTR over all data plus PBKDF2 key derivation per shard. Decrypting to Nitro requires... writing the flatbuffer. That's why they're 10x apart.

**Security floor:** HyperDB enforces a minimum of 10,000 PBKDF2 iterations. Passing `1` to `ForceFlush` in encrypted mode silently becomes `10000`. You cannot accidentally save an "encrypted" DB with trivially weak key derivation.

**Rule of thumb:** Data that would embarrass you, get you sued, or start a federal investigation → Fort Knox. Dev data, temp buffers, local tooling → Nitro Mode.

---

## 🛠️ Shard Tuning

| Shard Size | Result | Why |
| :--- | :--- | :--- |
| **32 MB** | 💀 **Catastrophic** | 100+ files, 100+ worker threads, context switch hell. The Thread Hunger Games. Nobody wins. |
| **512 MB** | 🏆 **Sweet Spot** | Low fragmentation, thread count under 10, SSD flushes are manageable. This is the default. |
| **1.2 GB** | 🐌 **Sluggish** | Large contiguous `std::vector` pools, TLB pressure, rising cache miss rates. |

512MB is the default for a reason. Everything else is a cry for help.

---

## 🏗️ Scaling Reality Check

HyperDB is the **Workstation King**. Raw local ingestion throughput will smoke SQLite3, MariaDB, and MySQL on any consumer hardware.

**Where it wins:**
- Hobbyist and embedded projects — zero setup, one header, one lib
- Workstations and local dev machines — born on consumer hardware, thrives there
- High-speed local buffers and bot backends — data at RAM speed, encryption optional

**Where it fails:**
- **128-core servers** — single-threaded per shard. 127 cores watch one core work.
- **High-concurrency multi-user workloads** — not a Postgres replacement.
- **Data you cannot afford to lose mid-write** — flush-based, not ACID.

---

## 🔧 API Reference

### `HyperDBManager`

| Method | Description |
| :--- | :--- |
| `OpenDB(path, password="", encrypt=true)` | Open or create a DB. `encrypt=false` = Nitro Mode. Throws on wrong password or corrupt file in encrypted mode. |
| `SetEncryption(encrypt, password="")` | Live migration. Toggle AES-256 and/or change password. Takes effect on next flush. |
| `FlushDB(iterations=58253)` | Serialize mirror to disk. No-op if not dirty or interval not elapsed. Encrypted mode enforces 10k+ iterations. |
| `ForceFlush(iterations=58253)` | Override interval, flush immediately. |
| `SetFlushInterval(ms)` | Minimum ms between auto-flushes. `0` = always flush. |
| `QueueCreateTable(name, cols)` | Define a table schema. No-op if already exists. |
| `QueueDropTable(name)` | Drop table and all data. Permanent. |
| `QueueClearTable(name)` | Wipe all rows, keep schema. |
| `QueueClearColumn(table, col)` | Clear one column across all rows. |
| `QueueWrite(table, row)` | Append a single row. Omitted columns are padded with defaults. Non-blocking. |
| `QueueWriteBulk(table, rows)` | Append N rows as a single `WriteBulk` action. One lock acquire, one pass, one size update. Use this. |
| `QueueRead(table, row_id, cb)` | Async read by local row index. |
| `QueueFind(table, col, value, cb)` | Async two-pass typed column scan. Vectorizable. |
| `QueueDelete(table, col, value, cb)` | Delete matching rows. Optional callback gets deleted count. |
| `GetRowCount(table)` | Synchronous row count. Thread-safe. |
| `IsQueueEmpty()` | True when worker is idle and queue is empty. |
| `EstimateMirrorSize()` | Atomic load of running size estimate. O(1), lock-free. |

### `HyperDBCluster`

| Method | Description |
| :--- | :--- |
| `Open(folder, name, password="", shard_limit=512MB, encrypt=true)` | Open or create a cluster. `encrypt` stored in `.manifest`, inherited by all future shards. |
| `SetEncryption(encrypt, password="")` | Live migration across entire cluster. Re-serializes all shards on next flush, updates manifest. |
| `Flush(iterations)` | Flush all shards. |
| `ForceFlush(iterations)` | Force-flush all shards immediately. |
| `SetFlushInterval(ms)` | Set flush interval on all shards. |
| `QueueCreateTable(name, cols)` | Create table on all shards. Thread-safe via `manifest_mutex_`. |
| `QueueDropTable(name, target)` | Drop table on target shards. Updates manifest entries accordingly. |
| `QueueClearTable(name, target)` | Clear table on target shards. |
| `QueueWrite(table, row)` | Write to active shard with global row ID tracking. |
| `QueueWriteBulk(table, rows)` | Bulk write to active shard. One manifest update for the whole batch. Handles spillover. |
| `QueueRead(table, global_row_id, cb)` | Resolves global row ID via manifest, reads from correct shard. |
| `QueueFind(table, col, value, cb, target)` | Fan-out find. Dispatches to all matching shards in parallel, merges on completion. |
| `QueueDelete(table, col, value, target)` | Fan-out delete. Cascades row ID corrections via `ShiftManifestEntries`. |
| `GetRowCount(table)` | Total rows across all shards. |
| `GetShardCount()` | Number of open shard files. |
| `GetActiveShard()` | Index of current write shard. |

### Column Types

| Type | C++ | Python | Notes |
| :--- | :--- | :--- | :--- |
| `Int8` – `Int64` | `int8_t` – `int64_t` | `int` | Signed integers |
| `UInt8` – `UInt64` | `uint8_t` – `uint64_t` | `int` | Unsigned integers |
| `Float32` / `Float64` | `float` / `double` | `float` | Epsilon compare in Find (1e-6) |
| `Bool` | `bool` | `bool` | Stored as `uint8_t` internally |
| `String` | `std::string` | `str` | UTF-8 or whatever you put in |
| `Bytes` | `std::vector<uint8_t>` | `bytes` | Raw binary blob |

### `ShardTarget`

| Value | Meaning |
| :--- | :--- |
| `ShardTarget::All` | Every shard (default) |
| `ShardTarget::ActiveOnly` | Only the current write shard |
| `ShardTarget::OldOnly` | Every shard except the active one |

---

## ⚠️ Notes for the Brave

1. **HyperDB is not ACID.** Writes go to an in-memory mirror. A crash between flushes loses everything since the last flush. This is the tradeoff for the speed numbers above. It is not a bug. It is the entire point.

2. **PBKDF2 iteration floor is 10,000.** Passing fewer iterations to `FlushDB` or `ForceFlush` in encrypted mode silently clamps to 10,000. Loading a file with fewer than 10,000 stored iterations also clamps on read. This is intentional. You cannot accidentally create a "Fort Knox" DB with a trivially weak key.

3. **Sparse writes are fine.** Omitting a column from a `QueueWrite` or `QueueWriteBulk` row is allowed. HyperDB pads the missing column with the type's zero/empty default. Row counts stay consistent. If you omit a column for every row, that column is still there, it's just all zeros.

4. **`Bytes` columns store raw binary.** `ColumnType::Bytes` maps to `std::vector<uint8_t>` in C++ and `bytes` in Python. Find and delete on byte columns compare byte-by-byte. Don't put 50MB blobs in here and wonder why serialization is slow.

5. **`QueueDelete` has a callback.** `callback(int removed)` fires with the actual deleted count. The cluster depends on this for `ShiftManifestEntries`. Pass `nullptr` in C++ or omit in Python if you don't need it.

6. **`QueueWriteBulk` is a single queue action now.** Unlike older versions that pushed N individual `Write` entries, `QueueWriteBulk` pushes one `WriteBulk` entry containing all rows. It's processed in a single `ExecWriteBulk` call. The `estimated_size_` delta is accumulated locally and applied in one atomic add at the end. This is why 365k rows/sec is possible.

7. **Manifest saves are throttled.** The cluster manifest writes at most once per second. `SaveManifestInternal` checks the elapsed time and marks `manifest_dirty_ = true` if the interval hasn't elapsed. `ForceFlush` bypasses this and always saves. If you're mid-benchmark and the process dies, the last second of manifest updates may be lost. The actual shard data is whatever was last flushed.

8. **Python callbacks run on the worker thread.** GIL is acquired automatically. Use `time.sleep(0)` in wait loops or your main thread starves the worker.

9. **Worker thread exceptions are caught.** If an `Exec*` call throws, the error is printed and the worker continues. The queue isn't dead. The shard isn't dead. The bad entry is silently eaten. This is a better outcome than a crashed process at 3AM, but you should still not throw from callbacks.

10. **Thread safety model.** All `Exec*` methods run on the single dedicated worker thread per shard. `FlushDB` has its own `flush_mutex_`. `data_mutex_` covers the mirror during all exec operations. The cluster uses `manifest_mutex_` for all manifest and shard index operations. Do not call `Exec*` directly from outside the queue.

<div align="center">
<i>Maintained by someone deeply regretting their life choices.</i>
</div>