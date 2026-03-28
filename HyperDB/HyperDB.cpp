// hyperdb.cpp
// when i wrote this code, only god and i understood what i was doing. now, only
// god knows. — someone who has debugged this at 3am more times than they'd like
// to admit

#include "HyperDB.h"

//#define BUILD_PYTHON_MODULE // define if you want to use the lib file for the python bindings.
// python bindings.
#ifdef BUILD_PYTHON_MODULE
#include <pybind11/pybind11.h>
#endif

#include <json.hpp>
#include <pbkdf2/pbkdf2-sha256.hpp>
#include <tinyAES/aes.hpp>

using nlohmann::json;

// ═════════════════════════════════════════
// HyperDBManager — coercion helpers
// shared by execfind, execdelete, execwrite, execread. stop copy-pasting
// lambdas. deja vu intensifies every time i see another as_i64 lambda somewhere
// ═════════════════════════════════════════

int64_t HyperDBManager::CoerceI64(const HyperValue &v) noexcept {
  return std::visit(
      [](auto &&x) -> int64_t {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_arithmetic_v<T>)
          return static_cast<int64_t>(x);
        return 0;
      },
      v);
}

uint64_t HyperDBManager::CoerceU64(const HyperValue &v) noexcept {
  return std::visit(
      [](auto &&x) -> uint64_t {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_arithmetic_v<T>)
          return static_cast<uint64_t>(x);
        return 0;
      },
      v);
}

double HyperDBManager::CoerceF64(const HyperValue &v) noexcept {
  return std::visit(
      [](auto &&x) -> double {
        using T = std::decay_t<decltype(x)>;
        if constexpr (std::is_arithmetic_v<T>)
          return static_cast<double>(x);
        return 0.0;
      },
      v);
}

// ═════════════════════════════════════════
// HyperDBQueue
// ═════════════════════════════════════════

HyperDBQueue::HyperDBQueue(HyperDBManager *manager) : manager_(manager) {
  worker_thread_ = std::thread(&HyperDBQueue::ProcessQueue, this);
}

HyperDBQueue::~HyperDBQueue() {
  stop_worker_ = true;
  cv_.notify_all();
  if (worker_thread_.joinable())
    worker_thread_.join();
}

void HyperDBQueue::AddToQueue(QueueEntry entry) {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    queue_.push(std::move(entry));
    queue_size_++;
  }
  cv_.notify_one();
}

void HyperDBQueue::AddToQueueBulk(std::vector<QueueEntry> entries) {
  size_t count = entries.size();
  if (count == 0)
    return;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto &e : entries)
      queue_.push(std::move(e));
    queue_size_ += count;
  }
  cv_.notify_one();
}

bool HyperDBQueue::IsEmpty() { return queue_size_ == 0 && !worker_busy_; }

void HyperDBQueue::ProcessQueue() {
  while (!stop_worker_) {
    QueueEntry entry;
    bool has_entry = false;
    bool is_callback_action = false;

    {
      std::unique_lock<std::mutex> lock(mutex_);
      cv_.wait(lock, [this] { return !queue_.empty() || stop_worker_; });

      if (stop_worker_)
        return;

      if (!queue_.empty()) {
        entry = std::move(queue_.front());
        queue_.pop();
        worker_busy_ = true;
        queue_size_--;
        has_entry = true;
        auto t = entry.action;
        is_callback_action =
            (t == QueueActionType::Read || t == QueueActionType::Find);
      }
    }

    if (has_entry) {
      try {
#ifdef BUILD_PYTHON_MODULE
        if (is_callback_action) {
          pybind11::gil_scoped_acquire acquire;
          Execute(std::move(entry));
        } else {
          Execute(std::move(entry));
        }
#else
        (void)is_callback_action;
        Execute(std::move(entry));
#endif
      } catch (const std::exception &e) {
        // if we crash here, the worker thread would die and take the shard
        // with it. we choose life.
        printf("HYPERDB CRITICAL: Worker thread exception: %s\n", e.what());
      }
      worker_busy_ = false;
    }
  }
}

void HyperDBQueue::Execute(QueueEntry entry) {
  switch (entry.action) {
  case QueueActionType::CreateDatabase:
    manager_->ExecCreateDatabase(entry.db_name);
    break;
  case QueueActionType::CreateTable:
    manager_->ExecCreateTable(entry.table_name, entry.column_defs);
    break;
  case QueueActionType::DropTable:
    manager_->ExecDropTable(entry.table_name);
    break;
  case QueueActionType::ClearTable:
    manager_->ExecClearTable(entry.table_name);
    break;
  case QueueActionType::ClearColumn:
    manager_->ExecClearColumn(entry.table_name, entry.column_name);
    break;
  case QueueActionType::Write:
    manager_->ExecWrite(entry.table_name, entry.row_data);
    break;
  case QueueActionType::WriteBulk:
      manager_->ExecWriteBulk(entry.table_name, entry.row_data_bulk);
      break;
  case QueueActionType::Read:
    manager_->ExecRead(entry.table_name, entry.row_id, entry.callback);
    break;
  case QueueActionType::Find:
    manager_->ExecFind(entry.table_name, entry.column_name, entry.value,
                       entry.find_callback);
    break;
  case QueueActionType::Delete:
    manager_->ExecDelete(entry.table_name, entry.column_name, entry.value,
                         entry.delete_callback);
    break;
  }
}

// ═════════════════════════════════════════
// HyperDBManager — file i/o
// ═════════════════════════════════════════

void HyperDBManager::OpenDB(const std::string &path,
                            const std::string &password, bool encrypt) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  db_path_ = path;
  password_ = password;
  should_encrypt_ = encrypt;

  if (!std::filesystem::exists(path)) {
    // new db — create fresh mirror. we'll serialize it on first flush.
    mirror_.name = std::filesystem::path(path).stem().string();
    mirror_.version = 1;
    dirty_ = true;
    return;
  }

  std::ifstream file(path, std::ios::binary | std::ios::ate);
  if (!file.is_open())
    throw std::runtime_error("HyperDB: failed to open " + path);

  std::streamsize size = file.tellg();
  file.seekg(0, std::ios::beg);

  if (!should_encrypt_) {
    // assume raw flatbuffer
    file_buffer_.resize(static_cast<size_t>(size));
    file.read(reinterpret_cast<char *>(file_buffer_.data()), size);

    flatbuffers::Verifier verifier(file_buffer_.data(), file_buffer_.size());
    if (!HyperDB::VerifyDatabaseBuffer(verifier))
      throw std::runtime_error(
          "HyperDB: failed to verify unencrypted database: " + path);

    DeserializeToMirror(HyperDB::GetDatabase(file_buffer_.data()));
    file_buffer_.clear();
    file_buffer_.shrink_to_fit();
    return;
  }

  std::vector<uint8_t> data(static_cast<size_t>(size));
  file.read(reinterpret_cast<char *>(data.data()), size);

  if (data.size() < 36)
    throw std::runtime_error("HyperDB: file too small to be valid.");

  uint8_t salt[16], iv[16];
  uint32_t its = 0;
  std::memcpy(salt, data.data(), 16);
  std::memcpy(iv, data.data() + 16, 16);
  std::memcpy(&its, data.data() + 32, 4);

  // security floor: if stored iterations < 10000, someone tampered with the file
  // or we're loading an old file. use minimum acceptable value.
  if (its < 10000) its = 10000; 

  uint8_t key[32];
  PKCS5_PBKDF2_HMAC(const_cast<uint8_t *>(
                        reinterpret_cast<const uint8_t *>(password.c_str())),
                    password.length(), salt, 16, its, 32, key);

  size_t cipher_len = data.size() - 36;
  file_buffer_.resize(cipher_len);
  std::memcpy(file_buffer_.data(), data.data() + 36, cipher_len);

  AES_ctx ctx;
  AES_init_ctx_iv(&ctx, key, iv);
  AES_CTR_xcrypt_buffer(&ctx, file_buffer_.data(),
                        static_cast<uint32_t>(cipher_len));

  flatbuffers::Verifier v(file_buffer_.data(), file_buffer_.size());
  if (!HyperDB::VerifyDatabaseBuffer(v))
    throw std::runtime_error("HyperDB: wrong password or corrupt file.");

  DeserializeToMirror(HyperDB::GetDatabase(file_buffer_.data()));
  file_buffer_.clear();
  file_buffer_.shrink_to_fit();
}

void HyperDBManager::FlushDB(uint32_t iterations) {
  if (!dirty_)
    return;
  std::lock_guard<std::mutex> lock_f(flush_mutex_);

  if (flush_interval_ms_ > 0) {
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  std::chrono::steady_clock::now() - last_flush_time_)
                  .count();
    if (ms < flush_interval_ms_)
      return;
  }

  // security floor: if you try to save with iterations < 10000, we override.
  // because saving with 1 iteration is basically just giving the password away.
  if (iterations < 10000)
    iterations = 10000;

  flatbuffers::FlatBufferBuilder builder(1024 * 1024);
  {
    std::lock_guard<std::mutex> lock_d(data_mutex_);
    auto db_offset = SerializeFromMirror(builder);
    builder.Finish(db_offset, HyperDB::DatabaseIdentifier());
    dirty_ = false; // set false while locked to prevent race with ExecWrite
  }

  std::ofstream file(db_path_, std::ios::binary | std::ios::trunc);
  if (!file.is_open())
    throw std::runtime_error("HyperDB: failed to write " + db_path_);

  if (!should_encrypt_) {
    file.write(reinterpret_cast<const char *>(builder.GetBufferPointer()),
               builder.GetSize());
  } else {
    uint8_t salt[16], iv[16];
    SecureRandomBytes(salt, 16);
    SecureRandomBytes(iv, 16);

    uint8_t key[32];
    PKCS5_PBKDF2_HMAC(const_cast<uint8_t *>(
                          reinterpret_cast<const uint8_t *>(password_.c_str())),
                      password_.length(), salt, 16, iterations, 32, key);

    size_t plain_len = builder.GetSize();
    std::vector<uint8_t> cipher(plain_len);
    std::memcpy(cipher.data(), builder.GetBufferPointer(), plain_len);

    // tinyAES CTR takes uint32_t for length. if we somehow exceed 4GB in a single shard,
    // we need to chunk it or we'll overflow. given FB is 32-bit offset anyway, 
    // a 4GB shard is already a miracle. he who breaches 4GB deserves the crash.
    if (plain_len > 0xFFFFFFFF) throw std::runtime_error("HyperDB: shard size exceeds AES-CTR-32 limit. use more shards.");

    AES_ctx ctx;
    AES_init_ctx_iv(&ctx, key, iv);
    AES_CTR_xcrypt_buffer(&ctx, cipher.data(),
                          static_cast<uint32_t>(plain_len));

    file.write(reinterpret_cast<const char *>(salt), 16);
    file.write(reinterpret_cast<const char *>(iv), 16);
    file.write(reinterpret_cast<const char *>(&iterations), 4);
    file.write(reinterpret_cast<const char *>(cipher.data()),
               static_cast<std::streamsize>(plain_len));
  }

  last_flush_time_ = std::chrono::steady_clock::now();
}

void HyperDBManager::SetFlushInterval(int64_t ms) { flush_interval_ms_ = ms; }

void HyperDBManager::ForceFlush(uint32_t iterations) {
  flush_interval_ms_ = 0;
  FlushDB(iterations);
}

void HyperDBManager::SetEncryption(bool encrypt, const std::string &password) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  should_encrypt_ = encrypt;
  password_ = password;
  dirty_ = true; // force a flush next time
}

// ═════════════════════════════════════════
// HyperDBManager — queue dispatch
// ═════════════════════════════════════════

void HyperDBManager::QueueCreateDatabase(const std::string &name) {
  queue_.AddToQueue(
      QueueEntry{.action = QueueActionType::CreateDatabase, .db_name = name});
}
void HyperDBManager::QueueCreateTable(const std::string &table_name,
                                      std::vector<ColumnDef> cols) {
  queue_.AddToQueue(QueueEntry{.action = QueueActionType::CreateTable,
                               .table_name = table_name,
                               .column_defs = std::move(cols)});
}
void HyperDBManager::QueueDropTable(const std::string &table_name) {
  queue_.AddToQueue(QueueEntry{.action = QueueActionType::DropTable,
                               .table_name = table_name});
}
void HyperDBManager::QueueClearTable(const std::string &table_name) {
  queue_.AddToQueue(QueueEntry{.action = QueueActionType::ClearTable,
                               .table_name = table_name});
}
void HyperDBManager::QueueClearColumn(const std::string &table_name,
                                      const std::string &column_name) {
  queue_.AddToQueue(QueueEntry{.action = QueueActionType::ClearColumn,
                               .table_name = table_name,
                               .column_name = column_name});
}
void HyperDBManager::QueueWrite(const std::string &table_name,
                                std::vector<RowData> row) {
  QueueEntry e;
  e.action = QueueActionType::Write;
  e.table_name = table_name;
  e.row_data = std::move(row);
  queue_.AddToQueue(std::move(e));
}

void HyperDBManager::QueueWriteBulk(const std::string &table_name,
                                    std::vector<std::vector<RowData>> rows) {
  queue_.AddToQueue(QueueEntry{.action = QueueActionType::WriteBulk,
                               .table_name = table_name,
                               .row_data_bulk = std::move(rows)});
}
void HyperDBManager::QueueRead(const std::string &table_name, uint64_t row_id,
                               std::function<void(ReadResult)> callback) {
  queue_.AddToQueue(QueueEntry{.action = QueueActionType::Read,
                               .table_name = table_name,
                               .row_id = row_id,
                               .callback = std::move(callback)});
}
void HyperDBManager::QueueFind(
    const std::string &table_name, const std::string &column_name,
    HyperValue value, std::function<void(std::vector<ReadResult>)> callback) {
  queue_.AddToQueue(QueueEntry{.action = QueueActionType::Find,
                               .table_name = table_name,
                               .column_name = column_name,
                               .value = std::move(value),
                               .find_callback = std::move(callback)});
}
void HyperDBManager::QueueDelete(const std::string &table_name,
                                 const std::string &column_name,
                                 HyperValue value,
                                 std::function<void(int)> callback) {
  queue_.AddToQueue(QueueEntry{.action = QueueActionType::Delete,
                               .table_name = table_name,
                               .column_name = column_name,
                               .value = std::move(value),
                               .delete_callback = std::move(callback)});
}

// ═════════════════════════════════════════
// HyperDBManager — public accessors
// ═════════════════════════════════════════

int HyperDBManager::GetRowCount(const std::string &table_name) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  TableMirror *t = FindTable(table_name);
  return t ? static_cast<int>(t->row_count) : -1;
}

bool HyperDBManager::IsQueueEmpty() { return queue_.IsEmpty(); }
bool HyperDBManager::IsDirty() { return dirty_; }

size_t HyperDBManager::EstimateMirrorSize() {
    return estimated_size_.load();
}

// ═════════════════════════════════════════
// HyperDBManager — exec implementations
// ═════════════════════════════════════════

void HyperDBManager::ExecCreateDatabase(const std::string &name) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  mirror_.name = name;
  dirty_ = true;
}

void HyperDBManager::ExecCreateTable(const std::string &table_name,
                                     const std::vector<ColumnDef> &cols) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  if (mirror_.table_map.find(table_name) != mirror_.table_map.end())
    return; // already exists, touch nothing

  TableMirror table;
  table.name = table_name;
  table.columns.reserve(cols.size());
  for (size_t i = 0; i < cols.size(); ++i) {
    ColumnMirror cm;
    cm.name = cols[i].name;
    cm.type = cols[i].type;
    table.column_map[cm.name] = i;
    table.columns.push_back(std::move(cm));
  }
  size_t idx = mirror_.tables.size();
  mirror_.tables.push_back(std::move(table));
  mirror_.table_map[table_name] = idx;
  dirty_ = true;
  estimated_size_ += table_name.length();
  for (const auto &c : cols)
    estimated_size_ += c.name.length() + 8; // rough overhead per column
}

void HyperDBManager::ExecDropTable(const std::string &table_name) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  auto it = mirror_.table_map.find(table_name);
  if (it == mirror_.table_map.end())
    return;
  
  size_t idx = it->second;
  // Recalculate size reduction - subtract overhead only
  estimated_size_ -= table_name.length();
  for (const auto &col : mirror_.tables[idx].columns) {
    estimated_size_ -= col.name.length() + 8; // overhead
    // Note: actual data memory freed by vector destructor
  }
  
  // Erase table and update map
  mirror_.tables.erase(mirror_.tables.begin() + idx);
  mirror_.table_map.erase(it);
  
  // Update indices for tables after the erased one
  for (size_t i = idx; i < mirror_.tables.size(); ++i) {
    mirror_.table_map[mirror_.tables[i].name] = i;
  }
  
  dirty_ = true;
}

void HyperDBManager::ExecClearTable(const std::string &table_name) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  TableMirror *table = FindTable(table_name);
  if (!table)
    return;

  // Subtract old data size
  for (auto &col : table->columns) {
    estimated_size_ -= col.i8.size() * sizeof(int8_t);
    estimated_size_ -= col.i16.size() * sizeof(int16_t);
    estimated_size_ -= col.i32.size() * sizeof(int32_t);
    estimated_size_ -= col.i64.size() * sizeof(int64_t);
    estimated_size_ -= col.u8.size() * sizeof(uint8_t);
    estimated_size_ -= col.u16.size() * sizeof(uint16_t);
    estimated_size_ -= col.u32.size() * sizeof(uint32_t);
    estimated_size_ -= col.u64.size() * sizeof(uint64_t);
    estimated_size_ -= col.f32.size() * sizeof(float);
    estimated_size_ -= col.f64.size() * sizeof(double);
    estimated_size_ -= col.bools.size() * sizeof(uint8_t);
    for (const auto &s : col.strings)
      estimated_size_ -= s.size();
    for (const auto &b : col.bytes)
      estimated_size_ -= b.size();
    col = ColumnMirror{col.name, col.type}; // Clear data
  }
  table->row_count = 0;
  dirty_ = true;
}

void HyperDBManager::ExecClearColumn(const std::string &table_name,
                                     const std::string &column_name) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  TableMirror *table = FindTable(table_name);
  if (!table)
    return;
  ColumnMirror *col = FindColumn(*table, column_name);
  if (!col)
    return;

  // Subtract old data size
  estimated_size_ -= col->i8.size() * sizeof(int8_t);
  estimated_size_ -= col->i16.size() * sizeof(int16_t);
  estimated_size_ -= col->i32.size() * sizeof(int32_t);
  estimated_size_ -= col->i64.size() * sizeof(int64_t);
  estimated_size_ -= col->u8.size() * sizeof(uint8_t);
  estimated_size_ -= col->u16.size() * sizeof(uint16_t);
  estimated_size_ -= col->u32.size() * sizeof(uint32_t);
  estimated_size_ -= col->u64.size() * sizeof(uint64_t);
  estimated_size_ -= col->f32.size() * sizeof(float);
  estimated_size_ -= col->f64.size() * sizeof(double);
  estimated_size_ -= col->bools.size() * sizeof(uint8_t);
  for (const auto &s : col->strings)
    estimated_size_ -= s.size();
  for (const auto &b : col->bytes)
    estimated_size_ -= b.size();

  // clear all the things. yes all of them. no, you can't just clear one.
  col->i8.clear();
  col->i16.clear();
  col->i32.clear();
  col->i64.clear();
  col->u8.clear();
  col->u16.clear();
  col->u32.clear();
  col->u64.clear();
  col->f32.clear();
  col->f64.clear();
  col->bools.clear();
  col->strings.clear();
  col->bytes.clear();
  dirty_ = true;
}

void HyperDBManager::ExecWrite(const std::string &table_name,
                               const std::vector<RowData> &row) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  TableMirror *table = FindTable(table_name);
  if (!table)
    return;

  for (auto &rd : row) {
    auto it = table->column_map.find(rd.column_name);
    if (it == table->column_map.end())
      continue;

    ColumnMirror *col = &table->columns[it->second];

    switch (col->type) {
    case HyperDB::ColumnType::ColumnType_Int8:
      col->i8.push_back(static_cast<int8_t>(CoerceI64(rd.value)));
      estimated_size_ += sizeof(int8_t);
      break;
    case HyperDB::ColumnType::ColumnType_Int16:
      col->i16.push_back(static_cast<int16_t>(CoerceI64(rd.value)));
      estimated_size_ += sizeof(int16_t);
      break;
    case HyperDB::ColumnType::ColumnType_Int32:
      col->i32.push_back(static_cast<int32_t>(CoerceI64(rd.value)));
      estimated_size_ += sizeof(int32_t);
      break;
    case HyperDB::ColumnType::ColumnType_Int64:
      col->i64.push_back(CoerceI64(rd.value));
      estimated_size_ += sizeof(int64_t);
      break;
    case HyperDB::ColumnType::ColumnType_UInt8:
      col->u8.push_back(static_cast<uint8_t>(CoerceU64(rd.value)));
      estimated_size_ += sizeof(uint8_t);
      break;
    case HyperDB::ColumnType::ColumnType_UInt16:
      col->u16.push_back(static_cast<uint16_t>(CoerceU64(rd.value)));
      estimated_size_ += sizeof(uint16_t);
      break;
    case HyperDB::ColumnType::ColumnType_UInt32:
      col->u32.push_back(static_cast<uint32_t>(CoerceU64(rd.value)));
      estimated_size_ += sizeof(uint32_t);
      break;
    case HyperDB::ColumnType::ColumnType_UInt64:
      col->u64.push_back(CoerceU64(rd.value));
      estimated_size_ += sizeof(uint64_t);
      break;
    case HyperDB::ColumnType::ColumnType_Float32:
      col->f32.push_back(static_cast<float>(CoerceF64(rd.value)));
      estimated_size_ += sizeof(float);
      break;
    case HyperDB::ColumnType::ColumnType_Float64:
      col->f64.push_back(CoerceF64(rd.value));
      estimated_size_ += sizeof(double);
      break;
    case HyperDB::ColumnType::ColumnType_Bool:
      col->bools.push_back(CoerceI64(rd.value) ? 1u : 0u);
      estimated_size_ += sizeof(uint8_t);
      break;
    case HyperDB::ColumnType::ColumnType_String: {
      std::string s = HyperDBUtil::HyperValueToString(rd.value);
      estimated_size_ += s.length();
      col->strings.push_back(std::move(s));
      break;
    }
    case HyperDB::ColumnType::ColumnType_Bytes: {
      if (std::holds_alternative<std::vector<uint8_t>>(rd.value)) {
        const auto &b = std::get<std::vector<uint8_t>>(rd.value);
        estimated_size_ += b.size();
        col->bytes.push_back(b);
      } else {
        // someone's writing strings into bytes. degenerate behavior.
        col->bytes.push_back({});
      }
      break;
    }
    }
  }

  // padding loop: if a column wasn't in the write, give it a default value.
  // if you forget this, row_count drifts and the DB becomes a digital graveyard.
  for (auto &col : table->columns) {
    size_t target_len = table->row_count + 1;
    size_t current_len = 0;
    switch (col.type) {
      case HyperDB::ColumnType::ColumnType_Int8: current_len = col.i8.size(); break;
      case HyperDB::ColumnType::ColumnType_Int16: current_len = col.i16.size(); break;
      case HyperDB::ColumnType::ColumnType_Int32: current_len = col.i32.size(); break;
      case HyperDB::ColumnType::ColumnType_Int64: current_len = col.i64.size(); break;
      case HyperDB::ColumnType::ColumnType_UInt8: current_len = col.u8.size(); break;
      case HyperDB::ColumnType::ColumnType_UInt16: current_len = col.u16.size(); break;
      case HyperDB::ColumnType::ColumnType_UInt32: current_len = col.u32.size(); break;
      case HyperDB::ColumnType::ColumnType_UInt64: current_len = col.u64.size(); break;
      case HyperDB::ColumnType::ColumnType_Float32: current_len = col.f32.size(); break;
      case HyperDB::ColumnType::ColumnType_Float64: current_len = col.f64.size(); break;
      case HyperDB::ColumnType::ColumnType_Bool: current_len = col.bools.size(); break;
      case HyperDB::ColumnType::ColumnType_String: current_len = col.strings.size(); break;
      case HyperDB::ColumnType::ColumnType_Bytes: current_len = col.bytes.size(); break;
      default: break;
    }

    if (current_len < target_len) {
      auto def = GetDefaultValue(col.type);
      size_t diff = target_len - current_len;
      switch (col.type) {
      case HyperDB::ColumnType::ColumnType_Int8:
        col.i8.resize(target_len, std::get<int8_t>(def));
        estimated_size_ += diff * sizeof(int8_t);
        break;
      case HyperDB::ColumnType::ColumnType_Int16:
        col.i16.resize(target_len, std::get<int16_t>(def));
        estimated_size_ += diff * sizeof(int16_t);
        break;
      case HyperDB::ColumnType::ColumnType_Int32:
        col.i32.resize(target_len, std::get<int32_t>(def));
        estimated_size_ += diff * sizeof(int32_t);
        break;
      case HyperDB::ColumnType::ColumnType_Int64:
        col.i64.resize(target_len, std::get<int64_t>(def));
        estimated_size_ += diff * sizeof(int64_t);
        break;
      case HyperDB::ColumnType::ColumnType_UInt8:
        col.u8.resize(target_len, std::get<uint8_t>(def));
        estimated_size_ += diff * sizeof(uint8_t);
        break;
      case HyperDB::ColumnType::ColumnType_UInt16:
        col.u16.resize(target_len, std::get<uint16_t>(def));
        estimated_size_ += diff * sizeof(uint16_t);
        break;
      case HyperDB::ColumnType::ColumnType_UInt32:
        col.u32.resize(target_len, std::get<uint32_t>(def));
        estimated_size_ += diff * sizeof(uint32_t);
        break;
      case HyperDB::ColumnType::ColumnType_UInt64:
        col.u64.resize(target_len, std::get<uint64_t>(def));
        estimated_size_ += diff * sizeof(uint64_t);
        break;
      case HyperDB::ColumnType::ColumnType_Float32:
        col.f32.resize(target_len, std::get<float>(def));
        estimated_size_ += diff * sizeof(float);
        break;
      case HyperDB::ColumnType::ColumnType_Float64:
        col.f64.resize(target_len, std::get<double>(def));
        estimated_size_ += diff * sizeof(double);
        break;
      case HyperDB::ColumnType::ColumnType_Bool:
        col.bools.resize(target_len, std::get<bool>(def) ? 1 : 0);
        estimated_size_ += diff * sizeof(uint8_t);
        break;
      case HyperDB::ColumnType::ColumnType_String:
        col.strings.resize(target_len, std::get<std::string>(def));
        break;
      case HyperDB::ColumnType::ColumnType_Bytes:
        col.bytes.resize(target_len, std::get<std::vector<uint8_t>>(def));
        break;
      default:
        break;
      }
    }
  }

  table->row_count++;
  dirty_ = true;
}

void HyperDBManager::ExecWriteBulk(const std::string &table_name,
                                   const std::vector<std::vector<RowData>> &rows) {
  if (rows.empty())
    return;
  std::lock_guard<std::mutex> lock(data_mutex_);
  TableMirror *table = FindTable(table_name);
  if (!table)
    return;

  size_t size_accum = 0;
  for (const auto &row : rows) {
    for (const auto &rd : row) {
      auto it = table->column_map.find(rd.column_name);
      if (it == table->column_map.end())
        continue;

      ColumnMirror *col = &table->columns[it->second];
      switch (col->type) {
      case HyperDB::ColumnType::ColumnType_Int8:
        col->i8.push_back(static_cast<int8_t>(CoerceI64(rd.value)));
        size_accum += sizeof(int8_t);
        break;
      case HyperDB::ColumnType::ColumnType_Int16:
        col->i16.push_back(static_cast<int16_t>(CoerceI64(rd.value)));
        size_accum += sizeof(int16_t);
        break;
      case HyperDB::ColumnType::ColumnType_Int32:
        col->i32.push_back(static_cast<int32_t>(CoerceI64(rd.value)));
        size_accum += sizeof(int32_t);
        break;
      case HyperDB::ColumnType::ColumnType_Int64:
        col->i64.push_back(static_cast<int64_t>(CoerceI64(rd.value)));
        size_accum += sizeof(int64_t);
        break;
      case HyperDB::ColumnType::ColumnType_UInt8:
        col->u8.push_back(static_cast<uint8_t>(CoerceU64(rd.value)));
        size_accum += sizeof(uint8_t);
        break;
      case HyperDB::ColumnType::ColumnType_UInt16:
        col->u16.push_back(static_cast<uint16_t>(CoerceU64(rd.value)));
        size_accum += sizeof(uint16_t);
        break;
      case HyperDB::ColumnType::ColumnType_UInt32:
        col->u32.push_back(static_cast<uint32_t>(CoerceU64(rd.value)));
        size_accum += sizeof(uint32_t);
        break;
      case HyperDB::ColumnType::ColumnType_UInt64:
        col->u64.push_back(static_cast<uint64_t>(CoerceU64(rd.value)));
        size_accum += sizeof(uint64_t);
        break;
      case HyperDB::ColumnType::ColumnType_Float32:
        col->f32.push_back(static_cast<float>(CoerceF64(rd.value)));
        size_accum += sizeof(float);
        break;
      case HyperDB::ColumnType::ColumnType_Float64:
        col->f64.push_back(static_cast<double>(CoerceF64(rd.value)));
        size_accum += sizeof(double);
        break;
      case HyperDB::ColumnType::ColumnType_Bool:
        col->bools.push_back(CoerceI64(rd.value) ? 1 : 0);
        size_accum += sizeof(uint8_t);
        break;
      case HyperDB::ColumnType::ColumnType_String: {
        if (std::holds_alternative<std::string>(rd.value)) {
            const auto &s = std::get<std::string>(rd.value);
            size_accum += s.size();
            col->strings.push_back(s);
        } else {
            col->strings.push_back("");
        }
        break;
      }
      case HyperDB::ColumnType::ColumnType_Bytes: {
        if (std::holds_alternative<std::vector<uint8_t>>(rd.value)) {
            const auto &b = std::get<std::vector<uint8_t>>(rd.value);
            size_accum += b.size();
            col->bytes.push_back(b);
        } else {
            col->bytes.push_back({});
        }
        break;
      }
      }
    }

    // padding loop for bulk
    for (auto &col : table->columns) {
      size_t target_len = table->row_count + 1;
      size_t current_len = 0;
      switch (col.type) {
      case HyperDB::ColumnType::ColumnType_Int8: current_len = col.i8.size(); break;
      case HyperDB::ColumnType::ColumnType_Int16: current_len = col.i16.size(); break;
      case HyperDB::ColumnType::ColumnType_Int32: current_len = col.i32.size(); break;
      case HyperDB::ColumnType::ColumnType_Int64: current_len = col.i64.size(); break;
      case HyperDB::ColumnType::ColumnType_UInt8: current_len = col.u8.size(); break;
      case HyperDB::ColumnType::ColumnType_UInt16: current_len = col.u16.size(); break;
      case HyperDB::ColumnType::ColumnType_UInt32: current_len = col.u32.size(); break;
      case HyperDB::ColumnType::ColumnType_UInt64: current_len = col.u64.size(); break;
      case HyperDB::ColumnType::ColumnType_Float32: current_len = col.f32.size(); break;
      case HyperDB::ColumnType::ColumnType_Float64: current_len = col.f64.size(); break;
      case HyperDB::ColumnType::ColumnType_Bool: current_len = col.bools.size(); break;
      case HyperDB::ColumnType::ColumnType_String: current_len = col.strings.size(); break;
      case HyperDB::ColumnType::ColumnType_Bytes: current_len = col.bytes.size(); break;
      default: break;
      }

      if (current_len < target_len) {
        auto def = GetDefaultValue(col.type);
        size_t diff = target_len - current_len;
        switch (col.type) {
          case HyperDB::ColumnType::ColumnType_Int8: col.i8.resize(target_len, std::get<int8_t>(def)); size_accum += diff * sizeof(int8_t); break;
          case HyperDB::ColumnType::ColumnType_Int16: col.i16.resize(target_len, std::get<int16_t>(def)); size_accum += diff * sizeof(int16_t); break;
          case HyperDB::ColumnType::ColumnType_Int32: col.i32.resize(target_len, std::get<int32_t>(def)); size_accum += diff * sizeof(int32_t); break;
          case HyperDB::ColumnType::ColumnType_Int64: col.i64.resize(target_len, std::get<int64_t>(def)); size_accum += diff * sizeof(int64_t); break;
          case HyperDB::ColumnType::ColumnType_UInt8: col.u8.resize(target_len, std::get<uint8_t>(def)); size_accum += diff * sizeof(uint8_t); break;
          case HyperDB::ColumnType::ColumnType_UInt16: col.u16.resize(target_len, std::get<uint16_t>(def)); size_accum += diff * sizeof(uint16_t); break;
          case HyperDB::ColumnType::ColumnType_UInt32: col.u32.resize(target_len, std::get<uint32_t>(def)); size_accum += diff * sizeof(uint32_t); break;
          case HyperDB::ColumnType::ColumnType_UInt64: col.u64.resize(target_len, std::get<uint64_t>(def)); size_accum += diff * sizeof(uint64_t); break;
          case HyperDB::ColumnType::ColumnType_Float32: col.f32.resize(target_len, std::get<float>(def)); size_accum += diff * sizeof(float); break;
          case HyperDB::ColumnType::ColumnType_Float64: col.f64.resize(target_len, std::get<double>(def)); size_accum += diff * sizeof(double); break;
          case HyperDB::ColumnType::ColumnType_Bool: col.bools.resize(target_len, std::get<bool>(def) ? 1 : 0); size_accum += diff * sizeof(uint8_t); break;
          case HyperDB::ColumnType::ColumnType_String: col.strings.resize(target_len, std::get<std::string>(def)); break;
          case HyperDB::ColumnType::ColumnType_Bytes: col.bytes.resize(target_len, std::get<std::vector<uint8_t>>(def)); break;
          default: break;
        }
      }
    }
    table->row_count++;
  }
  estimated_size_ += size_accum;
  dirty_ = true;
}

void HyperDBManager::ExecRead(const std::string &table_name, uint64_t row_id,
                              const std::function<void(ReadResult)> &callback) {
  ReadResult result;
  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    TableMirror *table = FindTable(table_name);
    if (!table || row_id >= table->row_count) {
      if (callback)
        callback({});
      return;
    }
    try {
      result.reserve(table->columns.size());
      for (auto &col : table->columns)
        result.push_back({col.name, GetValueAtIndex(col, row_id)});
    } catch (const std::exception &e) {
      // if we get here, someone wrote fewer values than row_count says.
      // that's a you problem, future me.
      printf("CRITICAL ERROR IN EXECREAD: %s\n", e.what());
      if (callback)
        callback({});
      return;
    }
  }
  if (callback)
    callback(std::move(result));
}

void HyperDBManager::ExecFind(
    const std::string &table_name, const std::string &col_name,
    const HyperValue &val,
    const std::function<void(std::vector<ReadResult>)> &callback) {

  std::vector<ReadResult> results;
  {
    std::lock_guard<std::mutex> lock(data_mutex_);
    TableMirror *table = FindTable(table_name);
    if (!table) {
      if (callback)
        callback({});
      return;
    }
    ColumnMirror *col = FindColumn(*table, col_name);
    if (!col) {
      if (callback)
        callback({});
      return;
    }

    // coerce the search value exactly once, outside the loop.
    // not inside. never again inside. i learned this the hard way.
    const int64_t needle_i64 = CoerceI64(val);
    const uint64_t needle_u64 = CoerceU64(val);
    const double needle_f64 = CoerceF64(val);
    const std::string needle_str =
        (col->type == HyperDB::ColumnType::ColumnType_String)
            ? HyperDBUtil::HyperValueToString(val)
            : std::string{};

    // matching row indices. we collect indices first, build ReadResults after.
    // avoids re-locking or touching other columns during the hot scan.
    std::vector<uint64_t> hits;
    hits.reserve(16); // pray that's enough. it usually is.

    // dispatch once on type, then raw typed scan — zero variant overhead in the
    // loop
    switch (col->type) {
    case HyperDB::ColumnType::ColumnType_Int8: {
      auto n = static_cast<int8_t>(needle_i64);
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (col->i8[i] == n)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Int16: {
      auto n = static_cast<int16_t>(needle_i64);
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (col->i16[i] == n)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Int32: {
      auto n = static_cast<int32_t>(needle_i64);
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (col->i32[i] == n)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Int64: {
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (col->i64[i] == needle_i64)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_UInt8: {
      auto n = static_cast<uint8_t>(needle_u64);
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (col->u8[i] == n)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_UInt16: {
      auto n = static_cast<uint16_t>(needle_u64);
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (col->u16[i] == n)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_UInt32: {
      auto n = static_cast<uint32_t>(needle_u64);
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (col->u32[i] == n)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_UInt64: {
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (col->u64[i] == needle_u64)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Float32: {
      // float equality. i am not sorry. if you want epsilon control, make a
      // rangeFind.
      auto n = static_cast<float>(needle_f64);
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (std::fabs(col->f32[i] - n) < 1e-6f)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Float64: {
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (std::fabs(col->f64[i] - needle_f64) < 1e-6)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Bool: {
      bool n = (needle_i64 != 0);
      for (uint64_t i = 0; i < table->row_count; ++i)
        if ((col->bools[i] != 0) == n)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_String: {
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (col->strings[i] == needle_str)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Bytes: {
      if (std::holds_alternative<std::vector<uint8_t>>(val)) {
          auto& n = std::get<std::vector<uint8_t>>(val);
          for (uint64_t i = 0; i < table->row_count; ++i)
            if (col->bytes[i] == n)
              hits.push_back(i);
      }
      break;
    }
    default:
      break;
    }

    // now build ReadResults for all matching rows.
    // GetValueAtIndex boxes, but we only call it on actual hits, not every row.
    // much better.
    results.reserve(hits.size());
    for (uint64_t idx : hits) {
      ReadResult row;
      row.reserve(table->columns.size());
      for (auto &c : table->columns)
        row.push_back({c.name, GetValueAtIndex(c, idx)});
      results.push_back(std::move(row));
    }
  }

  if (callback)
    callback(std::move(results));
}

void HyperDBManager::ExecDelete(const std::string &table_name,
                                const std::string &col_name,
                                const HyperValue &val,
                                const std::function<void(int)> &callback) {

  std::lock_guard<std::mutex> lock(data_mutex_);
  TableMirror *table = FindTable(table_name);
  if (!table)
    return;
  ColumnMirror *col = FindColumn(*table, col_name);
  if (!col)
    return;

  const int64_t needle_i64 = CoerceI64(val);
  const uint64_t needle_u64 = CoerceU64(val);
  const double needle_f64 = CoerceF64(val);
  const std::string needle_str =
      (col->type == HyperDB::ColumnType::ColumnType_String)
          ? HyperDBUtil::HyperValueToString(val)
          : std::string{};

  std::vector<size_t> to_delete;
  to_delete.reserve(8);

  switch (col->type) {
  case HyperDB::ColumnType::ColumnType_Int8: {
    auto n = static_cast<int8_t>(needle_i64);
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (col->i8[i] == n)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Int16: {
    auto n = static_cast<int16_t>(needle_i64);
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (col->i16[i] == n)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Int32: {
    auto n = static_cast<int32_t>(needle_i64);
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (col->i32[i] == n)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Int64: {
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (col->i64[i] == needle_i64)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_UInt8: {
    auto n = static_cast<uint8_t>(needle_u64);
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (col->u8[i] == n)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_UInt16: {
    auto n = static_cast<uint16_t>(needle_u64);
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (col->u16[i] == n)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_UInt32: {
    auto n = static_cast<uint32_t>(needle_u64);
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (col->u32[i] == n)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_UInt64: {
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (col->u64[i] == needle_u64)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Float32: {
    auto n = static_cast<float>(needle_f64);
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (std::fabs(col->f32[i] - n) < 1e-6f)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Float64: {
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (std::fabs(col->f64[i] - needle_f64) < 1e-6)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Bool: {
    bool n = (needle_i64 != 0);
    for (uint64_t i = 0; i < table->row_count; ++i)
      if ((col->bools[i] != 0) == n)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_String: {
    for (uint64_t i = 0; i < table->row_count; ++i)
      if (col->strings[i] == needle_str)
        to_delete.push_back(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Bytes: {
    if (std::holds_alternative<std::vector<uint8_t>>(val)) {
        auto& n = std::get<std::vector<uint8_t>>(val);
        for (uint64_t i = 0; i < table->row_count; ++i)
          if (col->bytes[i] == n)
            to_delete.push_back(i);
    }
    break;
  }
  default:
    break;
  }

  if (to_delete.empty()) {
    if (callback)
      callback(0);
    return;
  }

  int count = static_cast<int>(to_delete.size());

  // erase in reverse so indices don't shift under us.
  // this code has been here since 2023. don't fucking touch it.
  for (auto &c : table->columns) {
    for (auto it = to_delete.rbegin(); it != to_delete.rend(); ++it) {
      size_t idx = *it;
      switch (c.type) {
      case HyperDB::ColumnType::ColumnType_Int8:
        estimated_size_ -= sizeof(int8_t);
        c.i8.erase(c.i8.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_Int16:
        estimated_size_ -= sizeof(int16_t);
        c.i16.erase(c.i16.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_Int32:
        estimated_size_ -= sizeof(int32_t);
        c.i32.erase(c.i32.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_Int64:
        estimated_size_ -= sizeof(int64_t);
        c.i64.erase(c.i64.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_UInt8:
        estimated_size_ -= sizeof(uint8_t);
        c.u8.erase(c.u8.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_UInt16:
        estimated_size_ -= sizeof(uint16_t);
        c.u16.erase(c.u16.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_UInt32:
        estimated_size_ -= sizeof(uint32_t);
        c.u32.erase(c.u32.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_UInt64:
        estimated_size_ -= sizeof(uint64_t);
        c.u64.erase(c.u64.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_Float32:
        estimated_size_ -= sizeof(float);
        c.f32.erase(c.f32.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_Float64:
        estimated_size_ -= sizeof(double);
        c.f64.erase(c.f64.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_Bool:
        estimated_size_ -= sizeof(uint8_t);
        c.bools.erase(c.bools.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_String:
        estimated_size_ -= c.strings[idx].length();
        c.strings.erase(c.strings.begin() + idx);
        break;
      case HyperDB::ColumnType::ColumnType_Bytes:
        estimated_size_ -= c.bytes[idx].size();
        c.bytes.erase(c.bytes.begin() + idx);
        break;
      default:
        break;
      }
    }
  }
  table->row_count -= count;
  dirty_ = true;
  if (callback)
    callback(count);
}

// ═════════════════════════════════════════
// HyperDBManager — private helpers
// ═════════════════════════════════════════

TableMirror *HyperDBManager::FindTable(const std::string &name) {
  auto it = mirror_.table_map.find(name);
  if (it != mirror_.table_map.end() && it->second < mirror_.tables.size())
    return &mirror_.tables[it->second];
  return nullptr;
}

ColumnMirror *HyperDBManager::FindColumn(TableMirror &table,
                                         const std::string &name) {
  auto it = table.column_map.find(name);
  if (it != table.column_map.end())
    return &table.columns[it->second];
  return nullptr;
}

HyperValue HyperDBManager::GetValueAtIndex(const ColumnMirror &col,
                                           uint64_t idx) {
  // this boxes into a variant. only acceptable for reads and result-building.
  // do not call this inside a scan loop or i will find you.
  switch (col.type) {
  case HyperDB::ColumnType::ColumnType_Int8:
    return col.i8.at(idx);
  case HyperDB::ColumnType::ColumnType_Int16:
    return col.i16.at(idx);
  case HyperDB::ColumnType::ColumnType_Int32:
    return col.i32.at(idx);
  case HyperDB::ColumnType::ColumnType_Int64:
    return col.i64.at(idx);
  case HyperDB::ColumnType::ColumnType_UInt8:
    return col.u8.at(idx);
  case HyperDB::ColumnType::ColumnType_UInt16:
    return col.u16.at(idx);
  case HyperDB::ColumnType::ColumnType_UInt32:
    return col.u32.at(idx);
  case HyperDB::ColumnType::ColumnType_UInt64:
    return col.u64.at(idx);
  case HyperDB::ColumnType::ColumnType_Float32:
    return col.f32.at(idx);
  case HyperDB::ColumnType::ColumnType_Float64:
    return col.f64.at(idx);
  case HyperDB::ColumnType::ColumnType_Bool:
    return static_cast<bool>(col.bools.at(idx));
  case HyperDB::ColumnType::ColumnType_String:
    return idx < col.strings.size() ? col.strings[idx] : "";
  case HyperDB::ColumnType::ColumnType_Bytes:
    return idx < col.bytes.size() ? col.bytes[idx] : std::vector<uint8_t>{};
  default:
    return int8_t(0); // if we reach here, the schema changed and i have no idea
                      // what's going on
  }
}

HyperValue HyperDBManager::GetDefaultValue(HyperDB::ColumnType type) {
    switch (type) {
    case HyperDB::ColumnType::ColumnType_Int8: return int8_t(0);
    case HyperDB::ColumnType::ColumnType_Int16: return int16_t(0);
    case HyperDB::ColumnType::ColumnType_Int32: return int32_t(0);
    case HyperDB::ColumnType::ColumnType_Int64: return int64_t(0);
    case HyperDB::ColumnType::ColumnType_UInt8: return uint8_t(0);
    case HyperDB::ColumnType::ColumnType_UInt16: return uint16_t(0);
    case HyperDB::ColumnType::ColumnType_UInt32: return uint32_t(0);
    case HyperDB::ColumnType::ColumnType_UInt64: return uint64_t(0);
    case HyperDB::ColumnType::ColumnType_Float32: return 0.0f;
    case HyperDB::ColumnType::ColumnType_Float64: return 0.0;
    case HyperDB::ColumnType::ColumnType_Bool: return false;
    case HyperDB::ColumnType::ColumnType_String: return std::string("");
    case HyperDB::ColumnType::ColumnType_Bytes: return std::vector<uint8_t>{};
    default: return int8_t(0);
    }
}

// ─────────────────────────────────────────
// SerializeFromMirror — here be dragons.
// flatbuffers requires offsets to be created in reverse topological order.
// if you change the order of these calls and things break, that's why.
// ─────────────────────────────────────────

flatbuffers::Offset<HyperDB::Database>
HyperDBManager::SerializeFromMirror(flatbuffers::FlatBufferBuilder &builder) {
  std::vector<flatbuffers::Offset<HyperDB::Table>> table_offsets;
  table_offsets.reserve(mirror_.tables.size());

  for (auto &tm : mirror_.tables) {
    std::vector<flatbuffers::Offset<HyperDB::Column>> col_offsets;
    col_offsets.reserve(tm.columns.size());

    for (auto &cm : tm.columns) {
      auto name_off = builder.CreateString(cm.name);
      flatbuffers::Offset<HyperDB::Column> col_offset;

      switch (cm.type) {
      case HyperDB::ColumnType::ColumnType_Int8: {
        auto t =
            HyperDB::CreateInt8Column(builder, builder.CreateVector(cm.i8));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_Int8Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_Int16: {
        auto t =
            HyperDB::CreateInt16Column(builder, builder.CreateVector(cm.i16));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_Int16Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_Int32: {
        auto t =
            HyperDB::CreateInt32Column(builder, builder.CreateVector(cm.i32));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_Int32Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_Int64: {
        auto t =
            HyperDB::CreateInt64Column(builder, builder.CreateVector(cm.i64));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_Int64Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_UInt8: {
        auto t =
            HyperDB::CreateUInt8Column(builder, builder.CreateVector(cm.u8));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_UInt8Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_UInt16: {
        auto t =
            HyperDB::CreateUInt16Column(builder, builder.CreateVector(cm.u16));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_UInt16Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_UInt32: {
        auto t =
            HyperDB::CreateUInt32Column(builder, builder.CreateVector(cm.u32));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_UInt32Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_UInt64: {
        auto t =
            HyperDB::CreateUInt64Column(builder, builder.CreateVector(cm.u64));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_UInt64Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_Float32: {
        auto t =
            HyperDB::CreateFloat32Column(builder, builder.CreateVector(cm.f32));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_Float32Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_Float64: {
        auto t =
            HyperDB::CreateFloat64Column(builder, builder.CreateVector(cm.f64));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_Float64Column, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_Bool: {
        auto t =
            HyperDB::CreateBoolColumn(builder, builder.CreateVector(cm.bools));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_BoolColumn, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_String: {
        auto t = HyperDB::CreateStringColumn(
            builder, builder.CreateVectorOfStrings(cm.strings));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_StringColumn, t.Union());
        break;
      }
      case HyperDB::ColumnType::ColumnType_Bytes: {
        std::vector<flatbuffers::Offset<flatbuffers::String>> vec;
        for (auto &b : cm.bytes)
          vec.push_back(builder.CreateString(reinterpret_cast<const char*>(b.data()), b.size()));
        auto t = HyperDB::CreateBytesColumn(builder, builder.CreateVector(vec));
        col_offset = HyperDB::CreateColumn(
            builder, name_off, cm.type,
            HyperDB::ColumnData::ColumnData_BytesColumn, t.Union());
        break;
      }
      default:
        break;
      }

      col_offsets.push_back(col_offset);
    }

    auto tbl =
        HyperDB::CreateTable(builder, builder.CreateString(tm.name),
                             builder.CreateVector(col_offsets), tm.row_count);
    table_offsets.push_back(tbl);
  }

  return HyperDB::CreateDatabase(builder, builder.CreateString(mirror_.name),
                                 builder.CreateVector(table_offsets),
                                 mirror_.version);
}

void HyperDBManager::DeserializeToMirror(const HyperDB::Database *db) {
  mirror_ = {};
  mirror_.name = db->name()->str();
  mirror_.version = db->version();
  estimated_size_ = mirror_.name.length(); // Initialize with DB name size
  if (!db->tables())
    return;

  for (auto *tbl : *db->tables()) {
    TableMirror tm;
    tm.name = tbl->name()->str();
    tm.row_count = tbl->row_count();
    estimated_size_ += tm.name.length(); // Add table name size

    if (tbl->columns()) {
      for (auto *col : *tbl->columns()) {
        ColumnMirror cm;
        cm.name = col->name()->str();
        cm.type = col->type();
        estimated_size_ +=
            cm.name.length() + 8; // Add column name size + overhead

        switch (col->type()) {
        case HyperDB::ColumnType::ColumnType_Int8:
          if (auto *c = col->data_as_Int8Column()) {
            cm.i8.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.i8.size() * sizeof(int8_t);
          }
          break;
        case HyperDB::ColumnType::ColumnType_Int16:
          if (auto *c = col->data_as_Int16Column()) {
            cm.i16.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.i16.size() * sizeof(int16_t);
          }
          break;
        case HyperDB::ColumnType::ColumnType_Int32:
          if (auto *c = col->data_as_Int32Column()) {
            cm.i32.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.i32.size() * sizeof(int32_t);
          }
          break;
        case HyperDB::ColumnType::ColumnType_Int64:
          if (auto *c = col->data_as_Int64Column()) {
            cm.i64.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.i64.size() * sizeof(int64_t);
          }
          break;
        case HyperDB::ColumnType::ColumnType_UInt8:
          if (auto *c = col->data_as_UInt8Column()) {
            cm.u8.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.u8.size() * sizeof(uint8_t);
          }
          break;
        case HyperDB::ColumnType::ColumnType_UInt16:
          if (auto *c = col->data_as_UInt16Column()) {
            cm.u16.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.u16.size() * sizeof(uint16_t);
          }
          break;
        case HyperDB::ColumnType::ColumnType_UInt32:
          if (auto *c = col->data_as_UInt32Column()) {
            cm.u32.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.u32.size() * sizeof(uint32_t);
          }
          break;
        case HyperDB::ColumnType::ColumnType_UInt64:
          if (auto *c = col->data_as_UInt64Column()) {
            cm.u64.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.u64.size() * sizeof(uint64_t);
          }
          break;
        case HyperDB::ColumnType::ColumnType_Float32:
          if (auto *c = col->data_as_Float32Column()) {
            cm.f32.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.f32.size() * sizeof(float);
          }
          break;
        case HyperDB::ColumnType::ColumnType_Float64:
          if (auto *c = col->data_as_Float64Column()) {
            cm.f64.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.f64.size() * sizeof(double);
          }
          break;
        case HyperDB::ColumnType::ColumnType_Bool:
          if (auto *c = col->data_as_BoolColumn()) {
            cm.bools.assign(c->values()->begin(), c->values()->end());
            estimated_size_ += cm.bools.size() * sizeof(uint8_t);
          }
          break;
        case HyperDB::ColumnType::ColumnType_String:
          if (auto *c = col->data_as_StringColumn()) {
            for (auto *s : *c->values()) {
              cm.strings.push_back(s->str());
              estimated_size_ += s->str().length();
            }
          }
          break;
        case HyperDB::ColumnType::ColumnType_Bytes:
          if (auto *c = col->data_as_BytesColumn()) {
            for (auto *s : *c->values()) {
              std::vector<uint8_t> b(s->begin(), s->end());
              cm.bytes.push_back(b);
              estimated_size_ += b.size();
            }
          }
          break;
        default:
          break;
        }
        tm.columns.push_back(std::move(cm));
      }
      // rebuild the map since we just nuked it during deser
      for (size_t i = 0; i < tm.columns.size(); ++i) {
        tm.column_map[tm.columns[i].name] = i;
      }
    }
    mirror_.tables.push_back(std::move(tm));
  }
  // rebuild table_map for O(1) lookups
  for (size_t i = 0; i < mirror_.tables.size(); ++i) {
    mirror_.table_map[mirror_.tables[i].name] = i;
  }
  // we already accumulated the size in 'estimated_size_' during the loops.
  // do not call the locked EstimateMirrorSize() here or we'll deadlock our own
  // open call. life is hard enough as it is.
}

// ═════════════════════════════════════════
// HyperDBCluster
// ═════════════════════════════════════════

void HyperDBCluster::Open(const std::string &folder, const std::string &name,
                          const std::string &password, size_t shard_limit_bytes,
                          bool encrypt) {
  folder_ = folder;
  name_ = name;
  password_ = password;
  shard_limit_ = shard_limit_bytes;
  should_encrypt_ = encrypt;
  manifest_path_ = folder + "/" + name + ".manifest";

  std::filesystem::create_directories(folder);

  if (std::filesystem::exists(manifest_path_)) {
    LoadManifest();
    for (int i = 0; i <= active_shard_index_; i++)
      OpenShard(i);

    // if we just loaded from disk, our in-memory schema tracker is empty.
    // grab the schema from shard 0 so any future shards don't get amnesia.
    // todo: someone explain why this only needs shard 0
    if (!shards_.empty() && cluster_schemas_.empty()) {
      for (const auto &table : shards_[0]->GetMirror().tables) {
        std::vector<ColumnDef> defs;
        defs.reserve(table.columns.size());
        for (const auto &col : table.columns)
          defs.push_back({col.name, col.type});
        cluster_schemas_[table.name] = std::move(defs);
      }
    }
  } else {
    active_shard_index_ = 0;
    OpenShard(0);
    SaveManifest();
  }
}

void HyperDBCluster::Flush(uint32_t iterations) {
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  for (auto &shard : shards_)
    shard->FlushDB(iterations);
  SaveManifestInternal();
}

void HyperDBCluster::SetFlushInterval(int64_t ms) {
  for (auto &shard : shards_)
    shard->SetFlushInterval(ms);
}

void HyperDBCluster::ForceFlush(uint32_t iterations) {
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  for (auto &shard : shards_)
    shard->ForceFlush(iterations);
  SaveManifestInternal();
}

void HyperDBCluster::SetEncryption(bool encrypt, const std::string &password) {
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  password_ = password;
  should_encrypt_ = encrypt;
  for (auto &shard : shards_) {
    shard->SetEncryption(encrypt, password);
  }
  SaveManifestInternal();
}

bool HyperDBCluster::IsQueueEmpty() {
  for (auto &shard : shards_)
    if (!shard->IsQueueEmpty())
      return false;
  return true;
}

void HyperDBCluster::QueueCreateTable(const std::string &table_name,
                                      std::vector<ColumnDef> cols) {
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  cluster_schemas_[table_name] = cols;
  for (auto &shard : shards_)
    shard->QueueCreateTable(table_name, cols);
  if (manifest_tables_.find(table_name) == manifest_tables_.end())
    manifest_tables_[table_name] = {};
}

void HyperDBCluster::QueueDropTable(const std::string &table_name,
                                    ShardTarget target) {
  ForEachShard(
      target, [&](HyperDBManager &shard) { shard.QueueDropTable(table_name); });

  std::lock_guard<std::mutex> lock(manifest_mutex_);
  if (target == ShardTarget::All) {
    manifest_tables_.erase(table_name);
  } else {
    // partial drop: find exactly which shards we hit and scrub them from manifest
    std::vector<int> indices = GetShardIndicesInternal(target);
    auto it = manifest_tables_.find(table_name);
    if (it != manifest_tables_.end()) {
      auto &entries = it->second;
      for (int idx : indices) {
        entries.erase(std::remove_if(entries.begin(), entries.end(),
                                     [idx](const ShardTableEntry &e) {
                                       return e.shard_index == idx;
                                     }),
                      entries.end());
      }
      if (entries.empty())
        manifest_tables_.erase(it);
    }
  }
  SaveManifestInternal();
}

void HyperDBCluster::QueueClearTable(const std::string &table_name,
                                     ShardTarget target) {
  ForEachShard(target, [&](HyperDBManager &shard) {
    shard.QueueClearTable(table_name);
  });
  if (target == ShardTarget::All)
    manifest_tables_[table_name].clear();
}

void HyperDBCluster::QueueClearColumn(const std::string &table_name,
                                      const std::string &column_name,
                                      ShardTarget target) {
  ForEachShard(target, [&](HyperDBManager &shard) {
    shard.QueueClearColumn(table_name, column_name);
  });
}

void HyperDBCluster::QueueWrite(const std::string &table_name,
                                std::vector<RowData> row) {
  MaybeSpillToNewShard();
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  uint64_t global_row_id = NextGlobalRowIdInternal(table_name);
  ActiveShard().QueueWrite(table_name, std::move(row));

  auto &entries = manifest_tables_[table_name];
  if (!entries.empty() && entries.back().shard_index == active_shard_index_)
    entries.back().row_end = global_row_id;
  else
    entries.push_back({active_shard_index_, global_row_id, global_row_id});
}

void HyperDBCluster::QueueWriteBulk(const std::string &table_name,
                                    std::vector<std::vector<RowData>> rows) {
  if (rows.empty())
    return;
  MaybeSpillToNewShard();

  std::lock_guard<std::mutex> lock(manifest_mutex_);
  uint64_t global_start = NextGlobalRowIdInternal(table_name);
  uint64_t global_end = global_start + rows.size() - 1;

  ActiveShard().QueueWriteBulk(table_name, std::move(rows));

  auto &entries = manifest_tables_[table_name];
  if (!entries.empty() && entries.back().shard_index == active_shard_index_)
    entries.back().row_end = global_end;
  else
    entries.push_back({active_shard_index_, global_start, global_end});
}

void HyperDBCluster::QueueRead(const std::string &table_name,
                               uint64_t global_row_id,
                               std::function<void(ReadResult)> callback) {
  std::pair<int, uint64_t> resolved;
  {
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    resolved = ResolveRowIdInternal(table_name, global_row_id);
  }
  
  if (resolved.first < 0) {
    if (callback)
      callback({});
    return;
  }
  shards_[resolved.first]->QueueRead(table_name, resolved.second, std::move(callback));
}

void HyperDBCluster::QueueFind(
    const std::string &table_name, const std::string &column_name,
    HyperValue value, std::function<void(std::vector<ReadResult>)> callback,
    ShardTarget target) {
  std::vector<int> indices;
  {
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    indices = GetShardIndicesInternal(target);
  }
  if (indices.empty()) {
    if (callback)
      callback({});
    return;
  }

  // fan out to N shards, merge results when all return.
  // shared_ptr soup because lambdas can't capture by ref safely across async
  // ops. i am not sure why this works but it fixes the problem.
  auto shared_results = std::make_shared<std::vector<ReadResult>>();
  auto shared_remaining =
      std::make_shared<std::atomic<int>>(static_cast<int>(indices.size()));
  auto shared_mutex = std::make_shared<std::mutex>();

  for (int idx : indices) {
    shards_[idx]->QueueFind(table_name, column_name, value,
                            [shared_results, shared_remaining, shared_mutex,
                             callback](std::vector<ReadResult> partial) {
                              {
                                std::lock_guard<std::mutex> lock(*shared_mutex);
                                for (auto &r : partial)
                                  shared_results->push_back(std::move(r));
                              }
                              if (--(*shared_remaining) == 0)
                                if (callback)
                                  callback(std::move(*shared_results));
                            });
  }
}

void HyperDBCluster::QueueDelete(const std::string &table_name,
                                 const std::string &column_name,
                                 HyperValue value, ShardTarget target) {
  std::vector<int> indices;
  {
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    indices = GetShardIndicesInternal(target);
  }
  for (int idx : indices) {
    shards_[idx]->QueueDelete(table_name, column_name, value,
                              [this, table_name, idx](int count) {
                                if (count > 0)
                                  ShiftManifestEntries(table_name, idx, -count);
                              });
  }
}

int HyperDBCluster::GetRowCount(const std::string &table_name) {
  int total = 0;
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  for (auto &shard : shards_) {
    int c = shard->GetRowCount(table_name);
    if (c > 0)
      total += c;
  }
  return total;
}

// ─────────────────────────────────────────
// HyperDBCluster — private helpers
// ─────────────────────────────────────────

std::string HyperDBCluster::ShardPath(int index) const {
  return folder_ + "/" + name_ + "_shard" + std::to_string(index) + ".db";
}

void HyperDBCluster::OpenShard(int index) {
  while (static_cast<int>(shards_.size()) <= index)
    shards_.push_back(std::make_unique<HyperDBManager>());
  shards_[index]->OpenDB(ShardPath(index), password_, should_encrypt_);
}

HyperDBManager &HyperDBCluster::ActiveShard() {
  return *shards_[active_shard_index_];
}

void HyperDBCluster::MaybeSpillToNewShard() {
  {
      std::lock_guard<std::mutex> lock(manifest_mutex_);
      if (shards_[active_shard_index_]->EstimateMirrorSize() < shard_limit_)
          return;

      // current shard is full. flush it so it doesn't sit dirty in memory.
      // we can't lock data_mutex_ here but FlushDB has its own lock.
      shards_[active_shard_index_]->FlushDB();

      active_shard_index_++;
      OpenShard(active_shard_index_);

      // teach the new shard the history of our universe.
      for (auto &[t_name, cols] : cluster_schemas_)
        shards_[active_shard_index_]->QueueCreateTable(t_name, cols);

      SaveManifestInternal();
  }
}

uint64_t HyperDBCluster::NextGlobalRowIdInternal(const std::string &table_name) {
  auto &entries = manifest_tables_[table_name];
  if (entries.empty())
    return 0;
  return entries.back().row_end + 1;
}

std::pair<int, uint64_t>
HyperDBCluster::ResolveRowIdInternal(const std::string &table_name,
                                     uint64_t global_row_id) {
  auto it = manifest_tables_.find(table_name);
  if (it == manifest_tables_.end())
    return {-1, 0};

  for (auto &entry : it->second) {
    if (global_row_id >= entry.row_start && global_row_id <= entry.row_end)
      return {entry.shard_index, global_row_id - entry.row_start};
  }
  return {-1, 0};
}

std::vector<int> HyperDBCluster::GetShardIndicesInternal(ShardTarget target) {
  std::vector<int> indices;
  indices.reserve(shards_.size());
  for (int i = 0; i < static_cast<int>(shards_.size()); i++) {
    if (target == ShardTarget::All)
      indices.push_back(i);
    else if (target == ShardTarget::ActiveOnly && i == active_shard_index_)
      indices.push_back(i);
    else if (target == ShardTarget::OldOnly && i != active_shard_index_)
      indices.push_back(i);
  }
  return indices;
}

void HyperDBCluster::ForEachShard(ShardTarget target,
                                  std::function<void(HyperDBManager &)> fn) {
  std::vector<int> indices;
  {
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    indices = GetShardIndicesInternal(target);
  }
  for (int idx : indices)
    fn(*shards_[idx]);
}

void HyperDBCluster::SaveManifest() {
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  SaveManifestInternal();
}

void HyperDBCluster::SaveManifestInternal() {
  auto now = std::chrono::steady_clock::now();
  auto ms_since_last = std::chrono::duration_cast<std::chrono::milliseconds>(
      now - last_manifest_save_).count();
  
  // Always save if enough time has passed, otherwise just mark dirty
  if (ms_since_last < MANIFEST_SAVE_INTERVAL_MS) {
    manifest_dirty_ = true;
    return;
  }
  
  json j;
  j["active_shard"] = active_shard_index_;
  j["shard_count"] = static_cast<int>(shards_.size());
  j["shard_limit"] = shard_limit_;
  j["should_encrypt"] = should_encrypt_;

  json tables = json::object();
  for (auto &[table_name, entries] : manifest_tables_) {
    json arr = json::array();
    for (auto &e : entries)
      arr.push_back({{"shard", e.shard_index},
                     {"row_start", e.row_start},
                     {"row_end", e.row_end}});
    tables[table_name] = arr;
  }
  j["tables"] = tables;

  std::ofstream f(manifest_path_);
  f << j.dump(2);
  
  last_manifest_save_ = now;
  manifest_dirty_ = false;
}

void HyperDBCluster::MaybeSaveManifest() {
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  if (manifest_dirty_) {
    auto now = std::chrono::steady_clock::now();
    auto ms_since_last = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - last_manifest_save_).count();
    if (ms_since_last >= MANIFEST_SAVE_INTERVAL_MS) {
      SaveManifestInternal();
    }
  }
}

void HyperDBCluster::LoadManifest() {
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  std::ifstream f(manifest_path_);
  if (!f.is_open())
    throw std::runtime_error("HyperDBCluster: failed to open manifest " +
                             manifest_path_);

  json j = json::parse(f);
  active_shard_index_ = j["active_shard"].get<int>();
  if (j.contains("shard_limit"))
    shard_limit_ = j["shard_limit"].get<size_t>();
  if (j.contains("should_encrypt"))
    should_encrypt_ = j["should_encrypt"].get<bool>();

  manifest_tables_.clear();
  for (auto &[table_name, arr] : j["tables"].items()) {
    std::vector<ShardTableEntry> entries;
    entries.reserve(arr.size());
    for (auto &e : arr)
      entries.push_back({e["shard"].get<int>(), e["row_start"].get<uint64_t>(),
                         e["row_end"].get<uint64_t>()});
    manifest_tables_[table_name] = std::move(entries);
  }
}

void HyperDBCluster::ShiftManifestEntries(const std::string &table_name,
                                          int from_shard_index, int delta) {
  std::lock_guard<std::mutex> lock(manifest_mutex_);
  auto it = manifest_tables_.find(table_name);
  if (it == manifest_tables_.end())
    return;

  auto &entries = it->second;
  bool found = false;
  for (size_t i = 0; i < entries.size(); ++i) {
    if (entries[i].shard_index == from_shard_index) {
      entries[i].row_end += delta;
      found = true;
      // cascade the shift to all subsequent shard boundaries
      for (size_t j = i + 1; j < entries.size(); ++j) {
        entries[j].row_start += delta;
        entries[j].row_end += delta;
      }
      break;
    }
  }
  if (found)
    SaveManifestInternal();
}
