#pragma once
#define NOMINMAX

#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <functional>
#include <string>
#include <vector>
#include <variant>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <cstdint>
#include <random>
#include <chrono>
#include <algorithm>
#include <unordered_map>
#include <memory>
#include <cmath>
#include <cstring>

// yes, you need these for HyperDB, im sorry. (flatbuffers.lib is inside the HyperDB.lib so you only need the headers.)
#include "flatbuffers/flatbuffers.h"
#include "flatbuffers/flatbuffer_builder.h"
#include "hyperdb_generated.h"


inline void SecureRandomBytes(uint8_t* buf, size_t len) {
    std::random_device rd;
    std::uniform_int_distribution<uint16_t> dist(0, 255);
    for (size_t i = 0; i < len; i++)
        buf[i] = static_cast<uint8_t>(dist(rd));
}

using HyperValue = std::variant<
    int8_t,      // ColumnType::Int8
    int16_t,     // ColumnType::Int16
    int32_t,     // ColumnType::Int32
    int64_t,     // ColumnType::Int64
    uint8_t,     // ColumnType::UInt8
    uint16_t,    // ColumnType::UInt16
    uint32_t,    // ColumnType::UInt32
    uint64_t,    // ColumnType::UInt64
    float,       // ColumnType::Float32
    double,      // ColumnType::Float64
    bool,        // ColumnType::Bool
    std::string  // ColumnType::String
>;

enum class ShardTarget : uint8_t {
    All,        // every shard (default)
    ActiveOnly, // only the current write shard
    OldOnly     // every shard except the active one
};

struct ColumnDef {
    std::string         name;
    HyperDB::ColumnType type;
};


struct RowData {
    std::string column_name;
    HyperValue  value;
};


using ReadResult = std::vector<RowData>;

namespace HyperDBUtil {
    inline std::string HyperValueToString(const HyperValue& val) {
        return std::visit([](auto&& v) -> std::string {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, std::string>) return v;
            else if constexpr (std::is_same_v<T, bool>)   return v ? "true" : "false";
            else                                           return std::to_string(v);
            }, val);
    }
}

enum class QueueActionType : uint8_t {
    CreateDatabase,
    CreateTable,
    DropTable,
    ClearColumn,
    ClearTable,
    Write,
    Read,
    Find,
    Delete
};

struct QueueEntry {
    QueueActionType action;

    std::string            db_name;
    std::string            table_name;
    std::string            column_name;
    std::vector<ColumnDef> column_defs;
    std::vector<RowData>   row_data;

    uint64_t                                     row_id = 0;
    HyperValue                                   value = int8_t(0);
    ShardTarget                                  target = ShardTarget::All;
    std::function<void(ReadResult)>              callback = nullptr;
    std::function<void(std::vector<ReadResult>)> find_callback = nullptr;
    std::function<void(int)>                     delete_callback = nullptr;
};

// in-memory mirror structs — parallel arrays per type.
// ugly but extremely cache-friendly. you don't have to like it.
struct ColumnMirror {
    std::string         name;
    HyperDB::ColumnType type;

    std::vector<int8_t>      i8;
    std::vector<int16_t>     i16;
    std::vector<int32_t>     i32;
    std::vector<int64_t>     i64;
    std::vector<uint8_t>     u8;
    std::vector<uint16_t>    u16;
    std::vector<uint32_t>    u32;
    std::vector<uint64_t>    u64;
    std::vector<float>       f32;
    std::vector<double>      f64;
    std::vector<uint8_t>     bools;
    std::vector<std::string> strings;
};

struct TableMirror {
    std::string               name;
    std::vector<ColumnMirror> columns;
    std::unordered_map<std::string, size_t> column_map;
    uint64_t                  row_count = 0;
};

struct DatabaseMirror {
    std::string              name;
    std::vector<TableMirror> tables;
    uint32_t                 version = 1;
};

class HyperDBManager;
class HyperDBQueue;
class HyperDBCluster;

class HyperDBQueue {
public:
    explicit HyperDBQueue(HyperDBManager* manager);
    ~HyperDBQueue();

    void AddToQueue(QueueEntry entry);
    void AddToQueueBulk(std::vector<QueueEntry> entries);
    bool IsEmpty();

private:
    void ProcessQueue();
    void Execute(QueueEntry entry);

    HyperDBManager* manager_;
    std::queue<QueueEntry> queue_;
    std::mutex             mutex_;
    std::condition_variable cv_;
    std::atomic<bool>      stop_worker_{ false };
    std::atomic<bool>      worker_busy_{ false };
    std::atomic<size_t>    queue_size_{ 0 };
    std::thread            worker_thread_;
};

class HyperDBManager {
public:
    HyperDBManager() : queue_(this) {}

    // open or create an encrypted db file. throws on wrong password or corrupt file.
    void OpenDB(const std::string& path, const std::string& password);

    // flush dirty mirror to disk. no-op if not dirty or interval hasn't elapsed.
    void FlushDB(uint32_t iterations = 58253);

    void SetFlushInterval(int64_t ms);
    void ForceFlush(uint32_t iterations = 58253);

    // async queue operations — fire and forget (except Read/Find which use callbacks)
    void QueueCreateDatabase(const std::string& name);
    void QueueCreateTable(const std::string& table_name, std::vector<ColumnDef> cols);
    void QueueDropTable(const std::string& table_name);
    void QueueClearTable(const std::string& table_name);
    void QueueClearColumn(const std::string& table_name, const std::string& column_name);
    void QueueWrite(const std::string& table_name, std::vector<RowData> row);
    void QueueWriteBulk(const std::string& table_name, std::vector<std::vector<RowData>> rows);
    void QueueRead(const std::string& table_name, uint64_t row_id, std::function<void(ReadResult)> callback);
    void QueueFind(const std::string& table_name, const std::string& column_name, HyperValue value, std::function<void(std::vector<ReadResult>)> callback);
    void QueueDelete(const std::string& table_name, const std::string& column_name, HyperValue value, std::function<void(int)> callback = nullptr);

    // synchronous read-only accessors (lock-safe)
    int    GetRowCount(const std::string& table_name);
    bool   IsQueueEmpty();
    bool   IsDirty();
    size_t EstimateMirrorSize();

    const DatabaseMirror& GetMirror() const { return mirror_; }

    // exec methods - called by HyperDBQueue. public so the queue can reach them.
    // do not call these directly unless you enjoy data races at 3am.
    void ExecCreateDatabase(const std::string& name);
    void ExecCreateTable(const std::string& table_name, const std::vector<ColumnDef>& cols);
    void ExecDropTable(const std::string& table_name);
    void ExecClearTable(const std::string& table_name);
    void ExecClearColumn(const std::string& table_name, const std::string& column_name);
    void ExecWrite(const std::string& table_name, const std::vector<RowData>& row);
    void ExecRead(const std::string& table_name, uint64_t row_id, const std::function<void(ReadResult)>& callback);
    void ExecFind(const std::string& table_name, const std::string& col_name, const HyperValue& val, const std::function<void(std::vector<ReadResult>)>& callback);
    void ExecDelete(const std::string& table_name, const std::string& col_name, const HyperValue& val, const std::function<void(int)>& callback = nullptr);

private:
    TableMirror* FindTable(const std::string& name);
    ColumnMirror* FindColumn(TableMirror& table, const std::string& name);
    HyperValue    GetValueAtIndex(const ColumnMirror& col, uint64_t idx);

    // flatbuffer ser/deser. magic. do not touch.
    flatbuffers::Offset<HyperDB::Database> SerializeFromMirror(flatbuffers::FlatBufferBuilder& builder);
    void DeserializeToMirror(const HyperDB::Database* db);

    // coercion helpers - deduplicated so we stop copy-pasting lambdas
    static int64_t  CoerceI64(const HyperValue& v) noexcept;
    static uint64_t CoerceU64(const HyperValue& v) noexcept;
    static double   CoerceF64(const HyperValue& v) noexcept;

    HyperDBQueue         queue_;
    DatabaseMirror       mirror_;
    std::string          db_path_;
    std::string          password_;
    std::vector<uint8_t> file_buffer_;
    std::mutex           data_mutex_;
    std::mutex           flush_mutex_;
    bool                 dirty_ = false;
    size_t               estimated_size_ = 0;
    int64_t              flush_interval_ms_ = 0;
    std::chrono::steady_clock::time_point last_flush_time_ = std::chrono::steady_clock::now();
};

struct ShardTableEntry {
    int      shard_index = 0;
    uint64_t row_start = 0;
    uint64_t row_end = 0;
};

class HyperDBCluster {
public:
    static constexpr size_t DEFAULT_SHARD_LIMIT = 512ULL * 1024 * 1024; // 1.5 GB = 1536. - we only need 512 GB per shard for best performance. (HyperDB's db single file limit is ~2Gb because flatbuffer is 32bit.)

    // open or create a cluster in folder/. creates folder if needed.
    void Open(const std::string& folder, const std::string& name,
        const std::string& password,
        size_t shard_limit_bytes = DEFAULT_SHARD_LIMIT);

    void Flush(uint32_t iterations = 58253);
    void SetFlushInterval(int64_t ms);
    void ForceFlush(uint32_t iterations = 58253);
    bool IsQueueEmpty();

    // schema ops — replicated across all shards automatically
    void QueueCreateTable(const std::string& table_name, std::vector<ColumnDef> cols);
    void QueueDropTable(const std::string& table_name, ShardTarget target = ShardTarget::All);
    void QueueClearTable(const std::string& table_name, ShardTarget target = ShardTarget::All);
    void QueueClearColumn(const std::string& table_name, const std::string& column_name, ShardTarget target = ShardTarget::All);

    // data ops
    void QueueWrite(const std::string& table_name, std::vector<RowData> row);
    void QueueWriteBulk(const std::string& table_name, std::vector<std::vector<RowData>> rows);
    void QueueRead(const std::string& table_name, uint64_t global_row_id, std::function<void(ReadResult)> callback);
    void QueueFind(const std::string& table_name, const std::string& column_name, HyperValue value, std::function<void(std::vector<ReadResult>)> callback, ShardTarget target = ShardTarget::All);
    void QueueDelete(const std::string& table_name, const std::string& column_name, HyperValue value, ShardTarget target = ShardTarget::All);

    int GetRowCount(const std::string& table_name);
    int GetShardCount() const { return static_cast<int>(shards_.size()); }
    int GetActiveShard() const { return active_shard_index_; }

private:
    std::string      ShardPath(int index) const;
    void             OpenShard(int index);
    HyperDBManager& ActiveShard();
    void             MaybeSpillToNewShard();
    uint64_t         NextGlobalRowId(const std::string& table_name);
    std::pair<int, uint64_t> ResolveRowId(const std::string& table_name, uint64_t global_row_id);
    std::vector<int> GetShardIndices(ShardTarget target);
    void             ForEachShard(ShardTarget target, std::function<void(HyperDBManager&)> fn);
    void             SaveManifest();
    void             LoadManifest();
    void             ShiftManifestEntries(const std::string& table_name, int from_shard_index, int delta);

    std::string folder_;
    std::string name_;
    std::string password_;
    std::string manifest_path_;
    size_t      shard_limit_ = DEFAULT_SHARD_LIMIT;
    int         active_shard_index_ = 0;

    std::vector<std::unique_ptr<HyperDBManager>>                  shards_;
    std::unordered_map<std::string, std::vector<ShardTableEntry>> manifest_tables_;
    std::unordered_map<std::string, std::vector<ColumnDef>>       cluster_schemas_;
    std::mutex                                                    manifest_mutex_;
};
