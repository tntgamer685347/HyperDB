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

HyperDB is an in-memory, violently fast, flatbuffer-backed embedded database written in C++20. It was born from a 3AM debugging session and a deep hatred for latency. It features **O(1)** column lookups via `unordered_map`, a **`condition_variable`-driven persistent worker queue**, **bulk write batching**, and **optional PBKDF2-HMAC-SHA256 / AES-256-CTR** encryption — Fort Knox when you need it, raw FlatBuffers when you don't.

If you are looking for an ACID compliant, highly scalable, enterprise-grade cloud solution... go use Postgres. If you want to serialize your vectors into flatbuffer blobs at 0.03ms per query while redlining your CPU's L3 cache... you're in the right place.

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
- **Optional PBKDF2-HMAC-SHA256 & AES-256-CTR Encryption**: Fort Knox mode (`encrypt=true`, default) or Nitro Mode (`encrypt=false`). The cluster persists the choice in the `.manifest` so future shards don't forget what they are. Choose your destiny once. Don't switch halfway through.
- **`HyperDBCluster` Auto-Sharding**: FlatBuffers has a 2GB hard limit. `HyperDBCluster` spills your data to new shards automatically, tracking row ID ranges per shard in a `.manifest` JSON file, executing writes on the active shard and fanning out `Find`/`Delete` queries transparently.

---

## 🚀 Performance Benchmarks
*(Tested on an AMD Ryzen 7 5800X doing its best not to catch fire)*

| Operation | Encrypted | Nitro (Unencrypted) |
| :--- | :--- | :--- |
| **New DB Initialization** | `0.07 ms` | `0.06 ms` |
| **Flush small DB to disk** | `43.65 ms` *(58k PBKDF2)* / `0.84 ms` *(1 iter)* | `0.28 ms` |
| **QueueWrite (2 Rows)** | `0.04 ms` | `0.03 ms` |
| **QueueRead (Async Callback)** | `0.04 ms` | `0.02 ms` |
| **QueueFind (Search Logic)** | `0.03 ms` | `0.02 ms` |
| **Cross-Shard Find (Multi-shard aggregation)** | `0.04 ms` | `0.04 ms` |

---

## 🌪️ 10,000,000 Row "Apocalypse" Benchmark
*(4.2GB | 512MB Shards | Ryzen 7 5800X)*

| Metric | Encrypted (58k iter) | ⚡ Nitro Mode |
| :--- | :--- | :--- |
| **Total Rows Written** | `10,000,000` | `10,000,000` |
| **Total Write Loop Time** | `109.9 seconds` | `32.1 seconds` |
| **Throughput** | `~91,000 rows/sec` | `~311,000 rows/sec` |
| **Cluster Finalize (Flush All)** | `1,721 ms` | `148 ms` |
| **Total Data Size** | `4.2 GB` | `4.2 GB` |
| **Final Shard Count** | `8 Shards` | `8 Shards` |

---

## 📖 "Read Apocalypse" Benchmark
*(10M Row Dataset | 8 Shards (512MB each) | Ryzen 7 5800X)*

| Operation | Encrypted | ⚡ Nitro Mode |
| :--- | :--- | :--- |
| **Cold Open (4.2GB, 8 shards)** | `~85 seconds` | `~7.8 seconds` |
| **100,000 Random Reads** | `21.2 ms` | `21.2 ms` |
| **Random Global ID Read** | `4,716,490 reads/sec` | `4,726,050 reads/sec` |
| **Full 4.2GB Multi-Shard Scan** | `8.998 ms` | `8.459 ms` |
| **Cross-Shard Aggregation** | `VERIFIED (10M rows / 8 Shards)` | `VERIFIED (10M rows / 8 Shards)` |

Once the data is in memory, encrypted and unencrypted are basically the same speed. The `~77 second` difference is entirely the cold open — AES-256-CTR grinding through 4.2GB of ciphertext on load. In-memory operations don't care. They never did.

---

## 🏎️ Live Migration Performance
*(4.2GB Dataset | Ryzen 7 5800X | Sharding Enabled)*

| Migration Path | Time Taken | Result |
| :--- | :--- | :--- |
| **Nitro → Fort Knox** (1 iter) | `~83.2 seconds` | Full AES-256 re-serialization of all shards. |
| **Fort Knox → Nitro** | `~10.0 seconds` | AES pipeline removed. Writing raw FlatBuffers to disk. |
| **Instant Cold Open** (Nitro) | `~7.8 seconds` | Raw SSD read speed. Zero crypto overhead. |

**Note:** Migrating a cluster is an IO-bound operation. All shards are flushed sequentially. If you have 8 shards, each one has to be re-encrypted and written before the cluster is considered "migrated." Plan your 3AM sessions accordingly.

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

    // Fort Knox mode (default) — AES-256-CTR + PBKDF2. your data is a fortress.
    // you will wait in line to get in. that's the deal.
    db.OpenDB("users.db", "sandwich");

    // Nitro Mode — raw flatbuffers, no encryption, no PBKDF2, no regrets.
    // if someone can read your db file, that's a you problem now.
    // db.OpenDB("users.db", "sandwich", false);

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
    db.QueueDelete("users", "username", std::string("batman"),
        [](int removed) {
            std::cout << "Deleted " << removed << " row(s).\n";
        });

    // 6. spin-wait until the dedicated worker thread drains the queue
    wait_for_queue(db);

    // 7. force a synchronous disk flush.
    //    iterations only matter in Fort Knox mode. in Nitro Mode this is just
    //    "please serialize my flatbuffer to disk." fast. painless. unencrypted.
    db.ForceFlush(58253);

    return 0;
}
```

---

## 📦 Cluster Mode (Distributed Mojibake)

If you're going to breach the ~2GB FlatBuffers limit of a single shard, `HyperDBCluster` handles cross-file tracking automatically. The `encrypt` flag is passed to `Open` and written to the `.manifest` — once a cluster is born unencrypted, it stays that way. You don't get to change your mind halfway through a 10 million row dataset.

```cpp
// Fort Knox cluster — encrypted, slightly slower to open, definitely safer.
HyperDBCluster cluster;
cluster.Open("db_folder", "chaos", "death", 512ULL * 1024 * 1024);

// Nitro Mode cluster — raw flatbuffers, 7.8s cold open on 4GB instead of 85s.
// the manifest remembers. future shards inherit the flag. there is no going back.
HyperDBCluster fast_cluster;
fast_cluster.Open("db_folder", "chaos", "death", 512ULL * 1024 * 1024, false);

fast_cluster.QueueCreateTable("system_logs", {
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
fast_cluster.QueueWriteBulk("system_logs", std::move(batch));
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
- **The Millionaire Problem (1M – 10M Rows, encrypted)**: The **Cold Start Tax** kicks in hard. Loading 4.2GB means AES-256-CTR decrypting every byte before a single query can run. That's ~85 seconds on a Ryzen 7 5800X. Your app will feel like it's thinking very hard about its choices.
- **The Millionaire Problem (1M – 10M Rows, Nitro Mode)**: Cold open drops to **~7.8 seconds**. That's raw SSD read speed. No AES pipeline. No PBKDF2. Just `fread` and a `flatbuffers::Verifier` doing its thing. The difference is not subtle.

**Summary:** If the data needs to be secret, pay the tax. If it doesn't, flip the flag and stop watching a progress bar at 3AM.

---

## ⚡ Encryption Mode vs. Nitro Mode — Pick Your Poison

Three ways to run HyperDB. One is a fortress. One is a racecar. One is a racecar that once was a fortress and now lives in denial.

| Config | Cold Open (4.2GB) | Flush (small DB) | Your data if someone steals the file |
| :--- | :--- | :--- | :--- |
| **Fort Knox** (58k PBKDF2 iter) | `~85 seconds` | `~43 ms` | Completely unreadable. You win. |
| **Unlocked Fort Knox** (1 iter) | `~83 seconds` | `~0.84 ms` | Trivially brute-forceable. You saved 2 seconds. Congratulations. |
| **⚡ Nitro Mode** (no encryption) | `~7.8 seconds` | `~0.28 ms` | Plaintext flatbuffer. Readable by anyone with a hex editor and bad intentions. |

The Cold Start Tax at scale is **AES-256-CTR grinding through gigabytes of ciphertext**. PBKDF2 derives a 32-byte key — microseconds at any sane iteration count. The decryption pass is a physics problem. Your CPU is moving 4.2GB through its AES-NI pipeline and that number does not negotiate regardless of what you put in the `iterations` field.

Dropping from 58k iterations to 1 saves you **~2 seconds** on a 4.2GB open. That's 2.4% of the problem. If that's your performance strategy, you've misread the situation.

**Nitro Mode saves you ~77 seconds.** That's the real number. That's AES leaving the room entirely.

```cpp
// fort knox — your data is safe. your cold open is not.
db.OpenDB("users.db", "sandwich");           // encrypt = true (default)
cluster.Open("folder", "db", "sandwich", 512ULL*1024*1024);  // ditto

// nitro mode — raw flatbuffers. no aes. no pbkdf2. no waiting.
// the manifest remembers this choice. all future shards inherit it.
// there is no migration path. choose wisely and don't come crying to me.
db.OpenDB("users.db", "sandwich", false);
cluster.Open("folder", "db", "sandwich", 512ULL*1024*1024, false);
```

**Rule of thumb:**
- Data that would embarrass you, get you sued, or start a federal investigation → Fort Knox.
- Local dev data, temp buffers, anything that lives and dies on your own machine → Nitro Mode. Stop paying the encryption tax for data nobody wants anyway.

---

## 🔧 API Reference

### `HyperDBManager`

| Method | Description |
| :--- | :--- |
| `OpenDB(path, password, encrypt=true)` | Open or create a database file. `encrypt=false` skips AES/PBKDF2 entirely — raw FlatBuffers, no cold start tax, no protection. Throws on wrong password or corrupt data in encrypted mode. |
| `SetEncryption(encrypt, password)` | Live migration. Change password or toggle AES-256 on/off for an open database. Marks DB dirty for next flush. |
| `FlushDB(iterations)` | Serialize mirror to disk. Encrypts if `encrypt=true`, writes raw if not. No-op if not dirty or interval hasn't elapsed. |
| `ForceFlush(iterations)` | Override interval and flush immediately. `iterations` is ignored in Nitro Mode. |
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
| `Open(folder, name, password, shard_limit, encrypt=true)` | Open or create a sharded cluster. The `encrypt` flag is stored in the `.manifest` — all future shards inherit it. You cannot change modes on an existing cluster. Creates `folder/` if needed. |
| `SetEncryption(encrypt, password)` | Live migration for the entire cluster. Upgrades/Downgrades every shard and updates the `.manifest`. |
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

1. **Encryption is now optional.** Pass `encrypt=false` to `OpenDB` or `Open` for Nitro Mode. The flag is stored in the cluster `.manifest` and inherited by all future shards. You cannot open an encrypted cluster as unencrypted or vice versa — the verifier will throw. Once you've chosen a mode, you've chosen it forever. Or until you delete the files. Same thing.

2. **`QueueDelete` now has a callback.** `void QueueDelete(table, col, value, callback)` fires `callback(int removed)` with the number of rows deleted. The cluster depends on this for manifest consistency. If you're using `HyperDBManager` directly and deleting from sharded data, you want this number. Pass `nullptr` if you genuinely don't care, which is allowed.

3. **`HyperValue` Types**: This `std::variant` mirrors `HyperDB::ColumnType` exactly. If you add a type there and forget to update the `switch` statements in `HyperDB.cpp`, you will suffer. This is not a threat. It is a prophecy.

4. **`QueueWriteBulk` vs. `QueueWrite`**: `QueueWrite` acquires the mutex once per row. `QueueWriteBulk` acquires it once for the entire batch. At 10,000 rows per batch and 10 million total rows, that's the difference between 10,000 lock acquisitions and 10,000,000. The benchmark numbers are not magic. They are arithmetic.

5. **`IsQueueEmpty()` Spin-Waiting**: Always `yield()` inside your wait loop. Without it, your main thread will starve the worker thread on the L3 cache by hammering the atomic counter. This seems like it shouldn't matter until it does, at which point you're staring at a performance cliff with no explanation.

6. **The `estimated_size_` counter**: `HyperDBManager` maintains a running estimate of mirror size, updated on every write, delete, and clear. `MaybeSpillToNewShard` uses this instead of summing every column on every call. At 311k writes/sec in Nitro Mode this matters. The estimate can drift slightly after edge cases, but it's close enough for shard spillover decisions where the limit has megabytes of headroom anyway.

7. **`ShiftManifestEntries`**: After a delete, the cluster adjusts the `row_end` of the affected shard's manifest entry and cascades the delta to all subsequent shard boundaries. If you bypass this — by calling `ExecDelete` directly on a shard managed by a cluster, for example — your global row IDs will silently drift out of alignment. Don't do that.

8. **Thread Safety**: All `Exec*` methods are serialized through the single worker thread per shard. `FlushDB` has its own `flush_mutex_` to prevent concurrent flushes. `data_mutex_` protects the mirror during all read/write operations. Do not call `Exec*` methods directly. That's what the queue is for. The queue is not optional.

<div align="center">
<i>Maintained by someone deeply regretting their life choices.</i>
</div>