// hyperdb.cpp
// when i wrote this code, only god and i understood what i was doing. now, only god knows. - someone who has debugged this at 3am more times than they'd like to admit
// WARNING: i really like switch statements lol.

#include "HyperDB.h"

//#define BUILD_PYTHON_MODULE // define if you want to use the lib file for the python bindings.
// if this is defined the queue callback calls automatically aquire and realase GIL for python, if you use this without python it will likely crash because no GIL exists.
// python bindings.
#ifdef BUILD_PYTHON_MODULE
#include <pybind11/pybind11.h>
#endif

#include <cstdio>
#include <future>
#include <iostream>
#include <json.hpp>
#include <pbkdf2/pbkdf2-sha256.hpp>
#include <tinyAES/aes.hpp>

#ifdef _WIN32
#include <io.h>
#else
#include <fcntl.h>
#include <unistd.h>
#endif

namespace {
// best-effort secure zero. compiler can't elide because of the volatile pointer.
inline void SecureZero(void *p, size_t n) noexcept {
#ifdef _WIN32
  ::SecureZeroMemory(p, n);
#else
  volatile uint8_t *vp = static_cast<volatile uint8_t *>(p);
  while (n--) *vp++ = 0;
#endif
}

// flush an ofstream all the way to disk. close-then-fsync the underlying fd.
inline void FlushToDisk(std::ofstream &f, const std::string &path) {
  f.flush();
  f.close();
#ifdef _WIN32
  FILE *fp = nullptr;
  if (fopen_s(&fp, path.c_str(), "rb") == 0 && fp) {
    int fd = _fileno(fp);
    if (fd >= 0) _commit(fd);
    fclose(fp);
  }
#else
  int fd = ::open(path.c_str(), O_RDONLY);
  if (fd >= 0) { ::fdatasync(fd); ::close(fd); }
#endif
}

// write+rename for crash-safe file replacement. tmp gets fsync'd, then rename.
// rename on the same filesystem is atomic on every OS we ship.
inline void AtomicWriteFile(const std::string &path,
                            const uint8_t *data, size_t size) {
  std::string tmp = path + ".tmp";
  {
    std::ofstream f(tmp, std::ios::binary | std::ios::trunc);
    if (!f.is_open())
      throw std::runtime_error("HyperDB: failed to open " + tmp);
    f.write(reinterpret_cast<const char *>(data),
            static_cast<std::streamsize>(size));
    FlushToDisk(f, tmp);
  }
  std::error_code ec;
  std::filesystem::rename(tmp, path, ec);
  if (ec) {
    // rename failed (Windows can be picky if target exists). remove + rename.
    std::filesystem::remove(path, ec);
    std::filesystem::rename(tmp, path, ec);
    if (ec) throw std::runtime_error("HyperDB: rename failed for " + path +
                                     ": " + ec.message());
  }
}
} // namespace

using nlohmann::json;

// HyperDBManager — coercion helpers
// shared by execfind, execdelete, execwrite, execread. stop copy-pasting (this took hours to move into singular "easy" functions.)
// lambdas. deja vu intensifies every time i see another as_i64 lambda somewhere

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
        // delete also fires a user callback now, so it needs the gil too.
        is_callback_action =
            (t == QueueActionType::Read || t == QueueActionType::Find ||
             t == QueueActionType::Delete);
      }
    }

    if (has_entry) {
      try {
#ifdef BUILD_PYTHON_MODULE
        if (is_callback_action) {
          // gil_scoped_acquire will crash if not used by actual python.
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
        // with it. we choose life. cerr is line-buffered — won't get sliced
        // by other writers like printf can.
        std::cerr << "HYPERDB CRITICAL: Worker thread exception: " << e.what()
                  << std::endl;
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

  // read straight into file_buffer_; we'll decrypt in place over the cipher
  // payload and pass the same buffer to the verifier. saves an entire 4GB
  // copy on cold-open of a full shard.
  file_buffer_.resize(static_cast<size_t>(size));
  file.read(reinterpret_cast<char *>(file_buffer_.data()), size);

  if (file_buffer_.size() < 36)
    throw std::runtime_error("HyperDB: file too small to be valid.");

  uint8_t salt[16], iv[16];
  uint32_t its = 0;
  std::memcpy(salt, file_buffer_.data(), 16);
  std::memcpy(iv, file_buffer_.data() + 16, 16);
  std::memcpy(&its, file_buffer_.data() + 32, 4);

  // security floor: if stored iterations < 10000, someone tampered with the file
  // or we're loading an old file. use minimum acceptable value. (just to be sure)
  if (its < 10000) its = 10000;

  uint8_t key[32];
  PKCS5_PBKDF2_HMAC(reinterpret_cast<uint8_t *>(const_cast<char *>(password.data())),
                    password.size(), salt, 16, its, 32, key);

  uint8_t *cipher = file_buffer_.data() + 36;
  size_t cipher_len = file_buffer_.size() - 36;

  AES_ctx ctx;
  AES_init_ctx_iv(&ctx, key, iv);
  AES_CTR_xcrypt_buffer(&ctx, cipher, static_cast<uint32_t>(cipher_len));
  // key is no longer needed; nuke it before any throw can happen.
  SecureZero(key, sizeof(key));
  SecureZero(&ctx, sizeof(ctx));

  flatbuffers::Verifier v(cipher, cipher_len);
  if (!HyperDB::VerifyDatabaseBuffer(v))
    throw std::runtime_error("HyperDB: wrong password or corrupt file.");

  DeserializeToMirror(HyperDB::GetDatabase(cipher));
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
  // because saving with 1 iteration is basically just giving the password away. (not really but bruteforcing will take literal seconds.)
  if (iterations < 10000)
    iterations = 10000;

  // size hint for the builder — avoids 8+ realloc/memcpy cycles on large mirrors.
  // overshoot a little for the FB overhead.
  size_t size_hint = estimated_size_.load() + 64 * 1024;
  if (size_hint < 1024 * 1024) size_hint = 1024 * 1024;
  flatbuffers::FlatBufferBuilder builder(size_hint);
  {
    std::lock_guard<std::mutex> lock_d(data_mutex_);
    auto db_offset = SerializeFromMirror(builder);
    builder.Finish(db_offset, HyperDB::DatabaseIdentifier());
    dirty_ = false; // set false while locked to prevent race with ExecWrite
  }

  if (!should_encrypt_) {
    AtomicWriteFile(db_path_, builder.GetBufferPointer(), builder.GetSize());
  } else {
    uint8_t salt[16], iv[16];
    SecureRandomBytes(salt, 16);
    SecureRandomBytes(iv, 16);

    uint8_t key[32];
    PKCS5_PBKDF2_HMAC(reinterpret_cast<uint8_t *>(const_cast<char *>(password_.data())),
                      password_.size(), salt, 16, iterations, 32, key);

    size_t plain_len = builder.GetSize();

    // tinyAES CTR takes uint32_t for length. if we somehow exceed 4GB in a single shard,
    // we need to chunk it or we'll overflow. given FB is 32-bit offset anyway,
    // a 4GB shard is already a miracle. he who breaches 4GB deserves the crash.
    if (plain_len > 0xFFFFFFFF) {
      SecureZero(key, sizeof(key));
      throw std::runtime_error("HyperDB: shard size exceeds AES-CTR-32 limit. use more shards.");
    }

    // assemble [salt][iv][iter][cipher] then encrypt the cipher slice in place.
    // single allocation, no extra memcpy of the plaintext payload.
    std::vector<uint8_t> out(36 + plain_len);
    std::memcpy(out.data(), salt, 16);
    std::memcpy(out.data() + 16, iv, 16);
    std::memcpy(out.data() + 32, &iterations, 4);
    std::memcpy(out.data() + 36, builder.GetBufferPointer(), plain_len);

    AES_ctx ctx;
    AES_init_ctx_iv(&ctx, key, iv);
    AES_CTR_xcrypt_buffer(&ctx, out.data() + 36,
                          static_cast<uint32_t>(plain_len));
    SecureZero(key, sizeof(key));
    SecureZero(&ctx, sizeof(ctx));

    AtomicWriteFile(db_path_, out.data(), out.size());
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
  // zero out the old password before overwriting. std::string::operator= can
  // reuse the existing buffer, leaving the old contents readable in memory.
  if (!password_.empty())
    SecureZero(password_.data(), password_.size());
  password_ = password;
  dirty_ = true; // force a flush next time
}

HyperDBManager::~HyperDBManager() {
  // password lives in std::string memory; std::string's destructor releases
  // the allocation but does NOT wipe it first. zero it here before member
  // destructors run. the queue worker doesn't touch password_, so this is
  // safe to do while it's still alive (it joins via ~HyperDBQueue after).
  if (!password_.empty())
    SecureZero(password_.data(), password_.size());
}

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

int HyperDBManager::GetRowCount(const std::string &table_name) {
  std::lock_guard<std::mutex> lock(data_mutex_);
  TableMirror *t = FindTable(table_name);
  return t ? static_cast<int>(t->row_count) : -1;
}

bool HyperDBManager::IsQueueEmpty() { return queue_.IsEmpty(); }
bool HyperDBManager::IsDirty() const noexcept { return dirty_.load(); }

size_t HyperDBManager::EstimateMirrorSize() const noexcept {
  return estimated_size_.load();
}

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
    // duplicate column names would orphan the first ColumnMirror — column_map
    // would point to the second, but both vectors would be in `columns`,
    // diverging row counts forever. fail loudly instead.
    if (table.column_map.find(cols[i].name) != table.column_map.end())
      throw std::runtime_error(
          "HyperDB: duplicate column name '" + cols[i].name +
          "' in table '" + table_name + "'");
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
    // explicit per-vector clear is cheaper than reassigning a fresh
    // ColumnMirror (which copies the name string).
    col.i8.clear();   col.i16.clear(); col.i32.clear(); col.i64.clear();
    col.u8.clear();   col.u16.clear(); col.u32.clear(); col.u64.clear();
    col.f32.clear();  col.f64.clear(); col.bools.clear();
    col.strings.clear(); col.bytes.clear();
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

  // clear all the things. yes all of them. no, you can't just clear one. (random comment: i wanted to be a comedian when i was young hehe)
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
        // someone's writing strings into bytes. degenerate behavior. (?? why would anyone do thatt ahhh)
        col->bytes.push_back({});
      }
      break;
    }
    }
  }

  // padding loop: if a column wasn't in the write, give it a default value.
  // if you dont do this, row_count drifts and the DB becomes a digital graveyard.
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
      switch (col.type) { // ugly ahh switch statement lol
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

  const size_t batch_size = rows.size();
  const size_t target_total = table->row_count + batch_size;

  // 1) reserve every typed vector once. saves ~14 reallocations per column at
  //    10k rows (vector growth doubles, log2(10000) ≈ 13.3).
  for (auto &col : table->columns) {
    switch (col.type) {
    case HyperDB::ColumnType::ColumnType_Int8:    col.i8.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_Int16:   col.i16.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_Int32:   col.i32.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_Int64:   col.i64.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_UInt8:   col.u8.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_UInt16:  col.u16.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_UInt32:  col.u32.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_UInt64:  col.u64.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_Float32: col.f32.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_Float64: col.f64.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_Bool:    col.bools.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_String:  col.strings.reserve(target_total); break;
    case HyperDB::ColumnType::ColumnType_Bytes:   col.bytes.reserve(target_total); break;
    default: break;
    }
  }

  // 2) per-row writes with a written-bitmap. avoids the expensive
  //    size()-via-switch padding scan; we just push a default for every column
  //    we didn't touch in this row. correctness-equivalent to the old
  //    per-row padding because we do it once per row, not once at the end.
  size_t size_accum = 0;
  std::vector<uint8_t> written(table->columns.size(), 0);
  for (const auto &row : rows) {
    std::fill(written.begin(), written.end(), 0);

    for (const auto &rd : row) {
      auto it = table->column_map.find(rd.column_name);
      if (it == table->column_map.end())
        continue;

      size_t col_idx = it->second;
      ColumnMirror *col = &table->columns[col_idx];
      switch (col->type) {
      case HyperDB::ColumnType::ColumnType_Int8:
        col->i8.push_back(static_cast<int8_t>(CoerceI64(rd.value)));
        size_accum += sizeof(int8_t); break;
      case HyperDB::ColumnType::ColumnType_Int16:
        col->i16.push_back(static_cast<int16_t>(CoerceI64(rd.value)));
        size_accum += sizeof(int16_t); break;
      case HyperDB::ColumnType::ColumnType_Int32:
        col->i32.push_back(static_cast<int32_t>(CoerceI64(rd.value)));
        size_accum += sizeof(int32_t); break;
      case HyperDB::ColumnType::ColumnType_Int64:
        col->i64.push_back(CoerceI64(rd.value));
        size_accum += sizeof(int64_t); break;
      case HyperDB::ColumnType::ColumnType_UInt8:
        col->u8.push_back(static_cast<uint8_t>(CoerceU64(rd.value)));
        size_accum += sizeof(uint8_t); break;
      case HyperDB::ColumnType::ColumnType_UInt16:
        col->u16.push_back(static_cast<uint16_t>(CoerceU64(rd.value)));
        size_accum += sizeof(uint16_t); break;
      case HyperDB::ColumnType::ColumnType_UInt32:
        col->u32.push_back(static_cast<uint32_t>(CoerceU64(rd.value)));
        size_accum += sizeof(uint32_t); break;
      case HyperDB::ColumnType::ColumnType_UInt64:
        col->u64.push_back(CoerceU64(rd.value));
        size_accum += sizeof(uint64_t); break;
      case HyperDB::ColumnType::ColumnType_Float32:
        col->f32.push_back(static_cast<float>(CoerceF64(rd.value)));
        size_accum += sizeof(float); break;
      case HyperDB::ColumnType::ColumnType_Float64:
        col->f64.push_back(CoerceF64(rd.value));
        size_accum += sizeof(double); break;
      case HyperDB::ColumnType::ColumnType_Bool:
        col->bools.push_back(CoerceI64(rd.value) ? 1u : 0u);
        size_accum += sizeof(uint8_t); break;
      case HyperDB::ColumnType::ColumnType_String: {
        // match ExecWrite: coerce non-strings via HyperValueToString so int-into-
        // string column produces "123" rather than "". diverging here would mean
        // bulk vs single writes give different stored values for the same input.
        std::string s = HyperDBUtil::HyperValueToString(rd.value);
        size_accum += s.size();
        col->strings.push_back(std::move(s));
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
      default: break;
      }
      written[col_idx] = 1;
    }

    // pad columns this row didn't write — single push per missing column,
    // no size()-via-switch needed because the bitmap already tells us.
    for (size_t i = 0; i < table->columns.size(); ++i) {
      if (written[i])
        continue;
      ColumnMirror &col = table->columns[i];
      switch (col.type) {
      case HyperDB::ColumnType::ColumnType_Int8:    col.i8.push_back(0);    size_accum += sizeof(int8_t); break;
      case HyperDB::ColumnType::ColumnType_Int16:   col.i16.push_back(0);   size_accum += sizeof(int16_t); break;
      case HyperDB::ColumnType::ColumnType_Int32:   col.i32.push_back(0);   size_accum += sizeof(int32_t); break;
      case HyperDB::ColumnType::ColumnType_Int64:   col.i64.push_back(0);   size_accum += sizeof(int64_t); break;
      case HyperDB::ColumnType::ColumnType_UInt8:   col.u8.push_back(0);    size_accum += sizeof(uint8_t); break;
      case HyperDB::ColumnType::ColumnType_UInt16:  col.u16.push_back(0);   size_accum += sizeof(uint16_t); break;
      case HyperDB::ColumnType::ColumnType_UInt32:  col.u32.push_back(0);   size_accum += sizeof(uint32_t); break;
      case HyperDB::ColumnType::ColumnType_UInt64:  col.u64.push_back(0);   size_accum += sizeof(uint64_t); break;
      case HyperDB::ColumnType::ColumnType_Float32: col.f32.push_back(0.f); size_accum += sizeof(float); break;
      case HyperDB::ColumnType::ColumnType_Float64: col.f64.push_back(0.0); size_accum += sizeof(double); break;
      case HyperDB::ColumnType::ColumnType_Bool:    col.bools.push_back(0); size_accum += sizeof(uint8_t); break;
      case HyperDB::ColumnType::ColumnType_String:  col.strings.emplace_back(); break;
      case HyperDB::ColumnType::ColumnType_Bytes:   col.bytes.emplace_back(); break;
      default: break;
      }
    }
  }

  table->row_count = target_total;
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
    // GetValueAtIndex now uses operator[] (no per-element bounds check). safe
    // because we hold data_mutex_ and verified row_id < row_count, and the
    // padding paths in ExecWrite/ExecWriteBulk maintain the invariant
    // col_size == row_count for every column.
    result.reserve(table->columns.size());
    for (auto &col : table->columns)
      result.push_back({col.name, GetValueAtIndex(col, row_id)});
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
    // not inside. never again inside. did it once, never trying that again.
    const int64_t needle_i64 = CoerceI64(val);
    const uint64_t needle_u64 = CoerceU64(val);
    const double needle_f64 = CoerceF64(val);
    const std::string needle_str =
        (col->type == HyperDB::ColumnType::ColumnType_String)
            ? HyperDBUtil::HyperValueToString(val)
            : std::string{};

    // matching row indices. we collect indices first, build ReadResults after. (FANCY!)
    // avoids re-locking or touching other columns during the hot scan.
    std::vector<uint64_t> hits;
    hits.reserve(64); // pray that's enough. it usually is. (had to upgrade from 16 to 64) - i should probably make the reservations dynamic with an argument. yes i mean you, future me.

    // dispatch once on type, then raw typed scan — zero variant overhead in the loop
    switch (col->type) { // big boi
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
      // ABSOLUTE epsilon, not relative. for floats around 1e10 this is
      // effectively bit-exact equality; for floats around 1e-10 everything
      // matches. if you need magnitude-aware comparisons, scan the column
      // yourself with QueueRead and apply your own predicate.
      auto n = static_cast<float>(needle_f64);
      for (uint64_t i = 0; i < table->row_count; ++i)
        if (std::fabs(col->f32[i] - n) < 1e-6f)
          hits.push_back(i);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Float64: {
      // see comment in Float32 — same absolute epsilon caveats apply here.
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

  // build a deletion bitmap in one type-dispatched scan. avoids the
  // M*N-shift erase loop the old code did with vector::erase.
  const uint64_t n_rows = table->row_count;
  std::vector<uint8_t> deleted(n_rows, 0);
  size_t hit_count = 0;

  auto mark = [&](uint64_t i) { deleted[i] = 1; ++hit_count; };

  switch (col->type) {
  case HyperDB::ColumnType::ColumnType_Int8: {
    auto n = static_cast<int8_t>(needle_i64);
    for (uint64_t i = 0; i < n_rows; ++i) if (col->i8[i] == n) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Int16: {
    auto n = static_cast<int16_t>(needle_i64);
    for (uint64_t i = 0; i < n_rows; ++i) if (col->i16[i] == n) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Int32: {
    auto n = static_cast<int32_t>(needle_i64);
    for (uint64_t i = 0; i < n_rows; ++i) if (col->i32[i] == n) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Int64: {
    for (uint64_t i = 0; i < n_rows; ++i) if (col->i64[i] == needle_i64) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_UInt8: {
    auto n = static_cast<uint8_t>(needle_u64);
    for (uint64_t i = 0; i < n_rows; ++i) if (col->u8[i] == n) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_UInt16: {
    auto n = static_cast<uint16_t>(needle_u64);
    for (uint64_t i = 0; i < n_rows; ++i) if (col->u16[i] == n) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_UInt32: {
    auto n = static_cast<uint32_t>(needle_u64);
    for (uint64_t i = 0; i < n_rows; ++i) if (col->u32[i] == n) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_UInt64: {
    for (uint64_t i = 0; i < n_rows; ++i) if (col->u64[i] == needle_u64) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Float32: {
    auto n = static_cast<float>(needle_f64);
    for (uint64_t i = 0; i < n_rows; ++i)
      if (std::fabs(col->f32[i] - n) < 1e-6f) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Float64: {
    for (uint64_t i = 0; i < n_rows; ++i)
      if (std::fabs(col->f64[i] - needle_f64) < 1e-6) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Bool: {
    bool n = (needle_i64 != 0);
    for (uint64_t i = 0; i < n_rows; ++i)
      if ((col->bools[i] != 0) == n) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_String: {
    for (uint64_t i = 0; i < n_rows; ++i)
      if (col->strings[i] == needle_str) mark(i);
    break;
  }
  case HyperDB::ColumnType::ColumnType_Bytes: {
    if (std::holds_alternative<std::vector<uint8_t>>(val)) {
      auto &n = std::get<std::vector<uint8_t>>(val);
      for (uint64_t i = 0; i < n_rows; ++i)
        if (col->bytes[i] == n) mark(i);
    }
    break;
  }
  default:
    break;
  }

  if (hit_count == 0) {
    if (callback)
      callback(0);
    return;
  }

  int count = static_cast<int>(hit_count);

  // two-pointer compaction per column. one pass, O(N) per column instead of
  // the old O(N*M) erase-in-reverse loop. for each column we walk read=r,
  // write=w forward; if !deleted[r], move element to position w and bump w.
  // resize at the end. this is the entire perf win.
  size_t size_drop = 0;
  for (auto &c : table->columns) {
    switch (c.type) {
    case HyperDB::ColumnType::ColumnType_Int8: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.i8[w++] = c.i8[r];
      c.i8.resize(w);
      size_drop += hit_count * sizeof(int8_t);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Int16: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.i16[w++] = c.i16[r];
      c.i16.resize(w);
      size_drop += hit_count * sizeof(int16_t);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Int32: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.i32[w++] = c.i32[r];
      c.i32.resize(w);
      size_drop += hit_count * sizeof(int32_t);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Int64: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.i64[w++] = c.i64[r];
      c.i64.resize(w);
      size_drop += hit_count * sizeof(int64_t);
      break;
    }
    case HyperDB::ColumnType::ColumnType_UInt8: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.u8[w++] = c.u8[r];
      c.u8.resize(w);
      size_drop += hit_count * sizeof(uint8_t);
      break;
    }
    case HyperDB::ColumnType::ColumnType_UInt16: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.u16[w++] = c.u16[r];
      c.u16.resize(w);
      size_drop += hit_count * sizeof(uint16_t);
      break;
    }
    case HyperDB::ColumnType::ColumnType_UInt32: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.u32[w++] = c.u32[r];
      c.u32.resize(w);
      size_drop += hit_count * sizeof(uint32_t);
      break;
    }
    case HyperDB::ColumnType::ColumnType_UInt64: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.u64[w++] = c.u64[r];
      c.u64.resize(w);
      size_drop += hit_count * sizeof(uint64_t);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Float32: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.f32[w++] = c.f32[r];
      c.f32.resize(w);
      size_drop += hit_count * sizeof(float);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Float64: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.f64[w++] = c.f64[r];
      c.f64.resize(w);
      size_drop += hit_count * sizeof(double);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Bool: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r)
        if (!deleted[r]) c.bools[w++] = c.bools[r];
      c.bools.resize(w);
      size_drop += hit_count * sizeof(uint8_t);
      break;
    }
    case HyperDB::ColumnType::ColumnType_String: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r) {
        if (deleted[r]) {
          size_drop += c.strings[r].size();
        } else if (w != r) {
          c.strings[w++] = std::move(c.strings[r]);
        } else {
          ++w;
        }
      }
      c.strings.resize(w);
      break;
    }
    case HyperDB::ColumnType::ColumnType_Bytes: {
      size_t w = 0;
      for (size_t r = 0; r < n_rows; ++r) {
        if (deleted[r]) {
          size_drop += c.bytes[r].size();
        } else if (w != r) {
          c.bytes[w++] = std::move(c.bytes[r]);
        } else {
          ++w;
        }
      }
      c.bytes.resize(w);
      break;
    }
    default:
      break;
    }
  }

  estimated_size_ -= size_drop;
  table->row_count -= count;
  dirty_ = true;
  if (callback)
    callback(count);
}

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
  // operator[] (no bounds check) — callers hold data_mutex_ and have already
  // verified idx < row_count, and write paths pad every column to row_count.
  switch (col.type) {
  case HyperDB::ColumnType::ColumnType_Int8:    return col.i8[idx];
  case HyperDB::ColumnType::ColumnType_Int16:   return col.i16[idx];
  case HyperDB::ColumnType::ColumnType_Int32:   return col.i32[idx];
  case HyperDB::ColumnType::ColumnType_Int64:   return col.i64[idx];
  case HyperDB::ColumnType::ColumnType_UInt8:   return col.u8[idx];
  case HyperDB::ColumnType::ColumnType_UInt16:  return col.u16[idx];
  case HyperDB::ColumnType::ColumnType_UInt32:  return col.u32[idx];
  case HyperDB::ColumnType::ColumnType_UInt64:  return col.u64[idx];
  case HyperDB::ColumnType::ColumnType_Float32: return col.f32[idx];
  case HyperDB::ColumnType::ColumnType_Float64: return col.f64[idx];
  case HyperDB::ColumnType::ColumnType_Bool:    return static_cast<bool>(col.bools[idx]);
  case HyperDB::ColumnType::ColumnType_String:  return col.strings[idx];
  case HyperDB::ColumnType::ColumnType_Bytes:   return col.bytes[idx];
  default:
    return int8_t(0); // if we reach here, the schema changed and i have no idea whats going on
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

flatbuffers::Offset<HyperDB::Database>
HyperDBManager::SerializeFromMirror(flatbuffers::FlatBufferBuilder &builder) {
  std::vector<flatbuffers::Offset<HyperDB::Table>> table_offsets;
  table_offsets.reserve(mirror_.tables.size());

  for (auto &tm : mirror_.tables) { // this ones a really big boi, dont even make me get started on the switch statement.
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

        switch (col->type()) { // yes another really big boi
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
  // do not call the locked EstimateMirrorSize() here or we'll deadlock our own open call. life is hard enough as it is.
}

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

    // parallel shard open. each shard's OpenDB does an independent file read
    // + AES-CTR pass + flatbuffer deserialize — perfectly parallelizable.
    // on encrypted mode this turns ~85s/4.2GB across 8 shards into roughly
    // wall-clock = max(per-shard time), bounded by hardware threads.
    // we pre-grow shards_ serially (vector can't be grown from threads),
    // then launch OpenDB in parallel.
    int total = active_shard_index_ + 1;
    while (static_cast<int>(shards_.size()) < total)
      shards_.push_back(std::make_unique<HyperDBManager>());

    std::vector<std::future<void>> futures;
    futures.reserve(total);
    for (int i = 0; i < total; ++i) {
      futures.push_back(std::async(std::launch::async, [this, i] {
        shards_[i]->OpenDB(ShardPath(i), password_, should_encrypt_);
      }));
    }
    // propagate the first exception to the caller; wait on all the rest so
    // none leak past Open() with the shard half-loaded.
    std::exception_ptr first_err;
    for (auto &f : futures) {
      try { f.get(); }
      catch (...) { if (!first_err) first_err = std::current_exception(); }
    }
    if (first_err) std::rethrow_exception(first_err);

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
                                 HyperValue value, ShardTarget target,
                                 std::function<void(int)> callback) {
  std::vector<int> indices;
  {
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    indices = GetShardIndicesInternal(target);
  }
  if (indices.empty()) {
    if (callback) callback(0);
    return;
  }

  // shared state lets us sum per-shard delete counts and fire the user
  // callback exactly once when the last shard reports back. same pattern
  // as QueueFind. shared_ptr because lambda capture by value duplicates
  // the atomic; we want one counter shared across all closures.
  auto remaining = std::make_shared<std::atomic<int>>(static_cast<int>(indices.size()));
  auto total = std::make_shared<std::atomic<int>>(0);

  for (int idx : indices) {
    shards_[idx]->QueueDelete(
        table_name, column_name, value,
        [this, table_name, idx, remaining, total, callback](int count) {
          if (count > 0)
            ShiftManifestEntries(table_name, idx, -count);
          total->fetch_add(count);
          if (--(*remaining) == 0)
            if (callback) callback(total->load());
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
  // flushing the old shard can take seconds (encrypted, large mirror) — we
  // don't want every other cluster operation blocked on it. so:
  //   1. under the lock, decide whether to spill, advance active_shard_index,
  //      open the new shard, replicate schemas, persist manifest.
  //   2. drop the lock.
  //   3. flush the (now no-longer-active) old shard.
  // future writes go straight to the new active shard while the old one
  // serializes/encrypts in the background.
  HyperDBManager *old_shard_to_flush = nullptr;
  {
    std::lock_guard<std::mutex> lock(manifest_mutex_);
    if (shards_[active_shard_index_]->EstimateMirrorSize() < shard_limit_)
      return;

    old_shard_to_flush = shards_[active_shard_index_].get();

    active_shard_index_++;
    OpenShard(active_shard_index_);

    // teach the new shard the history of our universe. (hehe universe)
    for (auto &[t_name, cols] : cluster_schemas_)
      shards_[active_shard_index_]->QueueCreateTable(t_name, cols);

    SaveManifestInternal();
  }

  // FlushDB has its own flush_mutex_ + data_mutex_; safe to call without
  // manifest_mutex_ held. while this runs, the new active shard is already
  // accepting writes.
  if (old_shard_to_flush)
    old_shard_to_flush->FlushDB();
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

  // compact dump (no pretty-print) + atomic temp+rename so a mid-write crash
  // can never leave a half-written manifest on disk. both pieces matter:
  // compact halves the bytes, rename guarantees crash safety.
  std::string serialized = j.dump();
  AtomicWriteFile(manifest_path_,
                  reinterpret_cast<const uint8_t *>(serialized.data()),
                  serialized.size());

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
