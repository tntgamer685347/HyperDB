#define BUILD_PYTHON_MODULE
#include "include/pybind11/pybind11.h"
#include "include/pybind11/stl.h"
#include "include/pybind11/functional.h"
#include "include/pybind11/chrono.h"
#include "include/HyperDB.h"

namespace py = pybind11;

// i'm so tired of writing wrappers. why can't computers just understand me?
// if you're reading this, i hope your python build actually works. 
// mine didn't for like 3 hours because of a missing comma.

PYBIND11_MODULE(HyperDB, m) {
    m.doc() = "hyperdb python bindings - because doing everything in c++ is a slow descent into madness";

    // -----------------------------------------------------------------------
    // enums - the only things that actually make sense in this codebase
    // -----------------------------------------------------------------------
    
    py::enum_<HyperDB::ColumnType>(m, "ColumnType", "types of data we can actually store without the database exploding")
        .value("Int8", HyperDB::ColumnType_Int8)
        .value("Int16", HyperDB::ColumnType_Int16)
        .value("Int32", HyperDB::ColumnType_Int32)
        .value("Int64", HyperDB::ColumnType_Int64)
        .value("UInt8", HyperDB::ColumnType_UInt8)
        .value("UInt16", HyperDB::ColumnType_UInt16)
        .value("UInt32", HyperDB::ColumnType_UInt32)
        .value("UInt64", HyperDB::ColumnType_UInt64)
        .value("Float32", HyperDB::ColumnType_Float32)
        .value("Float64", HyperDB::ColumnType_Float64)
        .value("Bool", HyperDB::ColumnType::ColumnType_Bool)
        .value("String", HyperDB::ColumnType::ColumnType_String)
        .value("Bytes", HyperDB::ColumnType::ColumnType_Bytes)
        .export_values();

    py::enum_<ShardTarget>(m, "ShardTarget", "which shards to hit. usually 'All' unless you're feeling adventurous")
        .value("All", ShardTarget::All)
        .value("ActiveOnly", ShardTarget::ActiveOnly)
        .value("OldOnly", ShardTarget::OldOnly)
        .export_values();

    // -----------------------------------------------------------------------
    // structs - glorified containers for my tears
    // -----------------------------------------------------------------------

    py::class_<ColumnDef>(m, "ColumnDef", "defines a column. name and type. simple, right? until it's not")
        .def(py::init<std::string, HyperDB::ColumnType>())
        .def_readwrite("name", &ColumnDef::name, "the name of the column. don't use 'rowid', it'll probably break something")
        .def_readwrite("type", &ColumnDef::type, "the data type from ColumnType enum");

    py::class_<RowData>(m, "RowData", "a single piece of data for a column. it's just a name and a value")
        .def(py::init<std::string, HyperValue>())
        .def_readwrite("column_name", &RowData::column_name)
        .def_readwrite("value", &RowData::value);

    // ensure pybind11 can handle the variant with vector<uint8_t>
    // pybind automatically maps bytes -> vector<uint8_t> if we're careful.

    // -----------------------------------------------------------------------
    // HyperDBManager - the core of the chaos
    // -----------------------------------------------------------------------

    py::class_<HyperDBManager>(m, "HyperDBManager", "the thing that actually does the work. handles a single database file")
        .def(py::init<>())
        .def("open_db", &HyperDBManager::OpenDB, 
            py::arg("path"), py::arg("password") = "", py::arg("encrypt") = true,
            "opens a database file. if it doesn't exist, it creates one. magic.")
        .def("set_encryption", &HyperDBManager::SetEncryption,
            py::arg("encrypt"), py::arg("password") = "",
            "changes encryption settings. don't lose the password or you're screwed")
        .def("flush_db", &HyperDBManager::FlushDB,
            py::arg("iterations") = 58253,
            "saves data to disk if it's dirty. iterates for bcrypt if needed")
        .def("force_flush", &HyperDBManager::ForceFlush,
            py::arg("iterations") = 58253,
            "saves data to disk NOW. no excuses")
        .def("set_flush_interval", &HyperDBManager::SetFlushInterval,
            py::arg("ms"),
            "how often should we auto-save? -1 to disable auto-save, because you like living on the edge")
        
        // async ops
        .def("queue_create_database", &HyperDBManager::QueueCreateDatabase, "asynchronously creates the database")
        .def("queue_create_table", &HyperDBManager::QueueCreateTable, 
            py::arg("table_name"), py::arg("cols"),
            "asynchronously creates a table with the given column definitions")
        .def("queue_drop_table", &HyperDBManager::QueueDropTable, "deletes a table. gone forever. rip")
        .def("queue_clear_table", &HyperDBManager::QueueClearTable, "empties a table but keeps the columns")
        .def("queue_clear_column", &HyperDBManager::QueueClearColumn, "sets all values in a column to null/zero")
        .def("queue_write", &HyperDBManager::QueueWrite, 
            py::arg("table_name"), py::arg("row"),
            "adds a row to the table. eventually.")
        .def("queue_write_bulk", &HyperDBManager::QueueWriteBulk,
            py::arg("table_name"), py::arg("rows"),
            "adds many rows at once. much faster, assuming i didn't break the loop logic")
        
        // callbacks that happen on background threads need the GIL. 
        // if you don't do this, python will explode and take your pride with it.
        .def("queue_read", [](HyperDBManager& self, const std::string& table_name, uint64_t row_id, py::function callback) {
            self.QueueRead(table_name, row_id, [callback](ReadResult result) {
                py::gil_scoped_acquire acquire;
                callback(result);
            });
        }, py::arg("table_name"), py::arg("row_id"), py::arg("callback"),
           "finds a row by id and calls your callback. callback runs on a background thread.")

        .def("queue_find", [](HyperDBManager& self, const std::string& table_name, const std::string& column_name, HyperValue value, py::function callback) {
            self.QueueFind(table_name, column_name, value, [callback](std::vector<ReadResult> results) {
                py::gil_scoped_acquire acquire;
                callback(results);
            });
        }, py::arg("table_name"), py::arg("column_name"), py::arg("value"), py::arg("callback"),
           "searches for rows where column matches value. callback runs on a background thread.")

        .def("queue_delete", [](HyperDBManager& self, const std::string& table_name, const std::string& column_name, HyperValue value, py::object callback) {
            if (callback.is_none()) {
                self.QueueDelete(table_name, column_name, value, nullptr);
            } else {
                py::function cb_func = py::reinterpret_borrow<py::function>(callback);
                self.QueueDelete(table_name, column_name, value, [cb_func](int count) {
                    py::gil_scoped_acquire acquire;
                    cb_func(count);
                });
            }
        }, py::arg("table_name"), py::arg("column_name"), py::arg("value"), py::arg("callback") = py::none(),
           "deletes rows. callback (optional) receives the number of deleted rows.")

        // sync accessors
        .def("get_row_count", &HyperDBManager::GetRowCount, "returns how many rows are in the table. right now")
        .def("is_queue_empty", &HyperDBManager::IsQueueEmpty, "is the manager actually doing anything?")
        .def("is_dirty", &HyperDBManager::IsDirty, "does the memory mirror have data that isn't on disk yet?")
        .def("estimate_mirror_size", &HyperDBManager::EstimateMirrorSize, "rough guess of how much ram we're eating")

        // blocks until the worker queue is fully drained.
        // releases the GIL while spinning so the worker thread can acquire it
        // when it needs to fire a read/find callback. without the release you'd
        // deadlock on any queue_find or queue_read call — the callback needs
        // the GIL and your main thread is holding it in a busy loop. not ideal.
        .def("wait_for_queue", [](HyperDBManager& self) {
            while (!self.IsQueueEmpty())
                std::this_thread::yield();
        }, py::call_guard<py::gil_scoped_release>(),
           "blocks until the worker queue is fully drained. use this instead of a manual sleep loop.");

    // -----------------------------------------------------------------------
    // HyperDBCluster - for when one file isn't enough pain
    // -----------------------------------------------------------------------

    py::class_<HyperDBCluster>(m, "HyperDBCluster", "handles multiple database shards. for the big boys")
        .def(py::init<>())
        .def("open", &HyperDBCluster::Open,
            py::arg("folder"), py::arg("name"), py::arg("password") = "", 
            py::arg("shard_limit_bytes") = HyperDBCluster::DEFAULT_SHARD_LIMIT, py::arg("encrypt") = true,
            "opens or creates a cluster in the given folder")
        .def("flush", &HyperDBCluster::Flush, py::arg("iterations") = 58253, "flushes all shards")
        .def("set_flush_interval", &HyperDBCluster::SetFlushInterval, py::arg("ms"))
        .def("force_flush", &HyperDBCluster::ForceFlush, py::arg("iterations") = 58253)
        .def("set_encryption", &HyperDBCluster::SetEncryption, py::arg("encrypt"), py::arg("password") = "")
        .def("is_queue_empty", &HyperDBCluster::IsQueueEmpty)
        
        .def("queue_create_table", &HyperDBCluster::QueueCreateTable)
        .def("queue_drop_table", &HyperDBCluster::QueueDropTable, py::arg("table_name"), py::arg("target") = ShardTarget::All)
        .def("queue_clear_table", &HyperDBCluster::QueueClearTable, py::arg("table_name"), py::arg("target") = ShardTarget::All)
        .def("queue_clear_column", &HyperDBCluster::QueueClearColumn, py::arg("table_name"), py::arg("column_name"), py::arg("target") = ShardTarget::All)
        
        .def("queue_write", &HyperDBCluster::QueueWrite)
        .def("queue_write_bulk", &HyperDBCluster::QueueWriteBulk)
        
        .def("queue_read", [](HyperDBCluster& self, const std::string& table_name, uint64_t global_row_id, py::function callback) {
            self.QueueRead(table_name, global_row_id, [callback](ReadResult result) {
                py::gil_scoped_acquire acquire;
                callback(result);
            });
        }, py::arg("table_name"), py::arg("global_row_id"), py::arg("callback"))

        .def("queue_find", [](HyperDBCluster& self, const std::string& table_name, const std::string& column_name, HyperValue value, py::function callback, ShardTarget target) {
            self.QueueFind(table_name, column_name, value, [callback](std::vector<ReadResult> results) {
                py::gil_scoped_acquire acquire;
                callback(results);
            }, target);
        }, py::arg("table_name"), py::arg("column_name"), py::arg("value"), py::arg("callback"), py::arg("target") = ShardTarget::All)

        .def("queue_delete", [](HyperDBCluster& self, const std::string& table_name, const std::string& column_name, HyperValue value, ShardTarget target, py::object callback) {
            if (callback.is_none()) {
                self.QueueDelete(table_name, column_name, value, target, nullptr);
            } else {
                py::function cb_func = py::reinterpret_borrow<py::function>(callback);
                self.QueueDelete(table_name, column_name, value, target, [cb_func](int count) {
                    py::gil_scoped_acquire acquire;
                    cb_func(count);
                });
            }
        }, py::arg("table_name"), py::arg("column_name"), py::arg("value"), py::arg("target") = ShardTarget::All, py::arg("callback") = py::none())

        .def("get_row_count", &HyperDBCluster::GetRowCount)
        .def("get_shard_count", &HyperDBCluster::GetShardCount)
        .def("get_active_shard", &HyperDBCluster::GetActiveShard)

        // same GIL release pattern as HyperDBManager.wait_for_queue.
        // cluster IsQueueEmpty checks all shards — if any shard still has work,
        // we keep spinning. the GIL release lets all of them fire their callbacks.
        .def("wait_for_queue", [](HyperDBCluster& self) {
            while (!self.IsQueueEmpty())
                std::this_thread::yield();
        }, py::call_guard<py::gil_scoped_release>(),
           "blocks until all shard worker queues are fully drained. use this instead of a manual sleep loop.");
}