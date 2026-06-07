#define BUILD_PYTHON_MODULE
#include "include/pybind11/pybind11.h"
#include "include/pybind11/stl.h"
#include "include/pybind11/stl_bind.h"
#include "include/pybind11/functional.h"
#include "include/pybind11/chrono.h"
#include "include/HyperDB.h"

#include <sstream>

namespace py = pybind11;
using namespace py::literals;

// i'm so tired of writing wrappers. why can't computers just understand me?
// if you're reading this, i hope your python build actually works.
// mine didn't for like 3 hours because of a missing comma. then a missing
// PYBIND11_MAKE_OPAQUE. then i forgot what i was doing. anyway, welcome.

// ---------------------------------------------------------------------------
// opaque vectors. without these, accessing mirror.tables or table.columns
// COPIES the whole vector — and TableMirror copies the parallel arrays inside
// every ColumnMirror, which can be hundreds of MB on big tables. ask me how
// i found out. (the answer rhymes with "swapping").
// must be at namespace scope, before any pybind11 template instantiation.
// ---------------------------------------------------------------------------
PYBIND11_MAKE_OPAQUE(std::vector<ColumnMirror>)
PYBIND11_MAKE_OPAQUE(std::vector<TableMirror>)

// ---------------------------------------------------------------------------
// custom type caster: std::vector<uint8_t> <-> py::bytes.
// without this, a Bytes column comes back as list[int] which nobody wants.
// safe because no other bound symbol surfaces a vector<uint8_t> field —
// ColumnMirror's per-type arrays (u8, bools, bytes) are intentionally not
// exposed, so the only place this caster ever fires is the HyperValue
// variant's Bytes alternative. magic. do not touch.
// ---------------------------------------------------------------------------
namespace pybind11 { namespace detail {
template <>
struct type_caster<std::vector<uint8_t>> {
public:
    PYBIND11_TYPE_CASTER(std::vector<uint8_t>, const_name("bytes"));

    // py -> c++. accept py::bytes only. bytearrays can convert themselves first.
    bool load(handle src, bool) {
        if (!PyBytes_Check(src.ptr())) return false;
        char *buf = nullptr;
        Py_ssize_t len = 0;
        if (PyBytes_AsStringAndSize(src.ptr(), &buf, &len) == -1) {
            PyErr_Clear();
            return false;
        }
        value.assign(reinterpret_cast<const uint8_t *>(buf),
                     reinterpret_cast<const uint8_t *>(buf) + len);
        return true;
    }

    // c++ -> py. always send bytes back. closes the loop.
    static handle cast(const std::vector<uint8_t> &src, return_value_policy, handle) {
        return py::bytes(reinterpret_cast<const char *>(src.data()),
                         static_cast<Py_ssize_t>(src.size())).release();
    }
};
}} // namespace pybind11::detail

// pretty-print column type. matches the python enum value names.
static const char *ColumnTypeName(HyperDB::ColumnType t) {
    switch (t) {
        case HyperDB::ColumnType_Int8:    return "Int8";
        case HyperDB::ColumnType_Int16:   return "Int16";
        case HyperDB::ColumnType_Int32:   return "Int32";
        case HyperDB::ColumnType_Int64:   return "Int64";
        case HyperDB::ColumnType_UInt8:   return "UInt8";
        case HyperDB::ColumnType_UInt16:  return "UInt16";
        case HyperDB::ColumnType_UInt32:  return "UInt32";
        case HyperDB::ColumnType_UInt64:  return "UInt64";
        case HyperDB::ColumnType_Float32: return "Float32";
        case HyperDB::ColumnType_Float64: return "Float64";
        case HyperDB::ColumnType_Bool:    return "Bool";
        case HyperDB::ColumnType_String:  return "String";
        case HyperDB::ColumnType_Bytes:   return "Bytes";
    }
    return "?"; // if we reach here, the enum gained a value and i didn't notice. pray.
}

PYBIND11_MODULE(HyperDB, m) {
    m.doc() = "hyperdb python bindings v1.0.8 — same wiring as 1.0.7, but the "
              "release zip now ships the python utility scripts (stress_test, "
              "browser, cli) alongside the .pyd/.so so you can actually use "
              "this thing without git-cloning the repo first.";
    m.attr("__version__") = "1.0.8";

    // -----------------------------------------------------------------------
    // exception type. lets callers write `except HyperDB.Error:` instead of
    // squinting at every RuntimeError that flies past. inherits from
    // RuntimeError so existing try/except blocks keep working unchanged.
    // -----------------------------------------------------------------------
    static py::exception<std::runtime_error> hyperdb_error(m, "Error", PyExc_RuntimeError);
    py::register_exception_translator([](std::exception_ptr p) {
        try {
            if (p) std::rethrow_exception(p);
        } catch (const std::runtime_error &e) {
            // route everything HyperDB throws (it's all runtime_error) to HyperDB.Error.
            // if some library underneath us also throws runtime_error, it gets caught
            // by the same net. acceptable collateral.
            PyErr_SetString(hyperdb_error.ptr(), e.what());
        }
    });

    // -----------------------------------------------------------------------
    // enums - the only things that actually make sense in this codebase
    // -----------------------------------------------------------------------
    // NOTE: no .export_values() here. it dumps every enum member at module
    // scope (HyperDB.Int8, HyperDB.Bytes, HyperDB.String, ...) and that
    // collides head-on with the explicit value-constructor factories below
    // (HyperDB.Bytes(b"..."), HyperDB.I8(x), HyperDB.U32(x), ...). pybind11
    // throws "Cannot overload existing non-function object" at import time
    // and the whole module fails to load. been there. cried about it. use
    // HyperDB.ColumnType.Bytes etc. — which is what every existing caller
    // does anyway.
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
        .value("Bytes", HyperDB::ColumnType::ColumnType_Bytes);

    py::enum_<ShardTarget>(m, "ShardTarget", "which shards to hit. usually 'All' unless you're feeling adventurous")
        .value("All", ShardTarget::All)
        .value("ActiveOnly", ShardTarget::ActiveOnly)
        .value("OldOnly", ShardTarget::OldOnly)
        .export_values();

    // -----------------------------------------------------------------------
    // structs - glorified containers for my tears
    // -----------------------------------------------------------------------
    py::class_<ColumnDef>(m, "ColumnDef", "defines a column. name and type. simple, right? until it's not")
        .def(py::init<std::string, HyperDB::ColumnType>(),
             py::arg("name"), py::arg("type"))
        .def_readwrite("name", &ColumnDef::name, "the name of the column. don't use 'rowid', it'll probably break something")
        .def_readwrite("type", &ColumnDef::type, "the data type from ColumnType enum")
        .def("__repr__", [](const ColumnDef &c) {
            std::ostringstream oss;
            oss << "ColumnDef(name='" << c.name << "', type=ColumnType." << ColumnTypeName(c.type) << ")";
            return oss.str();
        });

    py::class_<RowData>(m, "RowData", "a single piece of data for a column. it's just a name and a value")
        .def(py::init([](std::string name, HyperValue value) {
            return RowData(StringPool::Intern(name), value);
        }), py::arg("name"), py::arg("value"))
        .def_property("column_name",
            [](const RowData &self) -> std::string { return std::string(self.column_name); },
            [](RowData &self, std::string value) { self.column_name = StringPool::Intern(value); })
        .def_readwrite("value", &RowData::value)
        .def("__repr__", [](const RowData &r) {
            std::ostringstream oss;
            oss << "RowData(column_name='" << r.column_name
                << "', value=" << HyperDBUtil::HyperValueToString(r.value) << ")";
            return oss.str();
        });

    // -----------------------------------------------------------------------
    // explicit numeric constructors for HyperValue. python's int -> the first
    // matching variant alternative (int8_t), which is usually fine because
    // typed columns coerce on write. but for callbacks (queue_find, queue_read)
    // the value comes back as whatever the variant holds, and you can't pick
    // U32 vs I32 or F32 vs F64 from python natively. these help.
    // -----------------------------------------------------------------------
    m.def("I8",  [](int64_t v)  -> HyperValue { return static_cast<int8_t>(v);  }, py::arg("value"), "wrap as Int8.");
    m.def("I16", [](int64_t v)  -> HyperValue { return static_cast<int16_t>(v); }, py::arg("value"), "wrap as Int16.");
    m.def("I32", [](int64_t v)  -> HyperValue { return static_cast<int32_t>(v); }, py::arg("value"), "wrap as Int32.");
    m.def("I64", [](int64_t v)  -> HyperValue { return static_cast<int64_t>(v); }, py::arg("value"), "wrap as Int64.");
    m.def("U8",  [](uint64_t v) -> HyperValue { return static_cast<uint8_t>(v);  }, py::arg("value"), "wrap as UInt8.");
    m.def("U16", [](uint64_t v) -> HyperValue { return static_cast<uint16_t>(v); }, py::arg("value"), "wrap as UInt16.");
    m.def("U32", [](uint64_t v) -> HyperValue { return static_cast<uint32_t>(v); }, py::arg("value"), "wrap as UInt32.");
    m.def("U64", [](uint64_t v) -> HyperValue { return static_cast<uint64_t>(v); }, py::arg("value"), "wrap as UInt64.");
    m.def("F32", [](double v)   -> HyperValue { return static_cast<float>(v);    }, py::arg("value"), "wrap as Float32 (precision loss may happen, deal with it).");
    m.def("F64", [](double v)   -> HyperValue { return v;                         }, py::arg("value"), "wrap as Float64.");
    m.def("Bytes", [](py::bytes b) -> HyperValue {
        // not using the caster here because we want to be explicit — this is THE
        // way to disambiguate bytes from string when the variant gets fussy.
        char *buf = nullptr;
        Py_ssize_t len = 0;
        if (PyBytes_AsStringAndSize(b.ptr(), &buf, &len) == -1) {
            throw std::runtime_error("HyperDB.Bytes: argument is not a bytes object");
        }
        return std::vector<uint8_t>(buf, buf + len);
    }, py::arg("value"), "wrap as Bytes. use this when the variant would otherwise pick String.");

    // -----------------------------------------------------------------------
    // mirror introspection types — read-only views of the in-memory db.
    // exposed fields are metadata only (names, types, counts). the per-type
    // parallel arrays inside ColumnMirror are NOT exposed because (a) they're
    // huge and (b) you should be using queue_read/queue_find to actually
    // touch row data. the container vectors are opaque (see top of file) so
    // accessing them doesn't trigger a full copy.
    // -----------------------------------------------------------------------
    py::class_<ColumnMirror>(m, "ColumnMirror", "metadata for a single column. name + type. the data lives in the arrays we are politely not showing you.")
        .def_readonly("name", &ColumnMirror::name)
        .def_readonly("type", &ColumnMirror::type)
        .def("__repr__", [](const ColumnMirror &c) {
            std::ostringstream oss;
            oss << "ColumnMirror(name='" << c.name << "', type=ColumnType." << ColumnTypeName(c.type) << ")";
            return oss.str();
        });

    py::bind_vector<std::vector<ColumnMirror>>(m, "ColumnMirrorList",
        "opaque list of ColumnMirror. supports len()/indexing/iteration. zero-copy by design.");

    py::class_<TableMirror>(m, "TableMirror", "metadata for a table. name, columns, row count. data lives elsewhere on purpose.")
        .def_readonly("name", &TableMirror::name)
        .def_property_readonly("columns",
            [](TableMirror &t) -> std::vector<ColumnMirror>& { return t.columns; },
            py::return_value_policy::reference_internal,
            "ColumnMirrorList of the columns in this table")
        .def_readonly("row_count", &TableMirror::row_count)
        .def("__repr__", [](const TableMirror &t) {
            std::ostringstream oss;
            oss << "TableMirror(name='" << t.name
                << "', columns=" << t.columns.size()
                << ", row_count=" << t.row_count << ")";
            return oss.str();
        });

    py::bind_vector<std::vector<TableMirror>>(m, "TableMirrorList",
        "opaque list of TableMirror. supports len()/indexing/iteration. zero-copy by design.");

    py::class_<DatabaseMirror>(m, "DatabaseMirror", "the root mirror object. what GetMirror returns. tables + version + name.")
        .def_readonly("name", &DatabaseMirror::name)
        .def_property_readonly("tables",
            [](DatabaseMirror &dm) -> std::vector<TableMirror>& { return dm.tables; },
            py::return_value_policy::reference_internal,
            "TableMirrorList of all tables in this database")
        .def_readonly("version", &DatabaseMirror::version)
        .def("__repr__", [](const DatabaseMirror &dm) {
            std::ostringstream oss;
            oss << "DatabaseMirror(name='" << dm.name
                << "', tables=" << dm.tables.size()
                << ", version=" << dm.version << ")";
            return oss.str();
        });

    py::class_<ShardTableEntry>(m, "ShardTableEntry",
        "manifest entry: which shard owns which row-id range for a given table. "
        "useful for poking at cluster layout when something looks wrong.")
        .def_readonly("shard_index", &ShardTableEntry::shard_index)
        .def_readonly("row_start", &ShardTableEntry::row_start)
        .def_readonly("row_end", &ShardTableEntry::row_end)
        .def("__repr__", [](const ShardTableEntry &e) {
            std::ostringstream oss;
            oss << "ShardTableEntry(shard=" << e.shard_index
                << ", rows=[" << e.row_start << ".." << e.row_end << "))";
            return oss.str();
        });

    // ensure pybind11 can handle the variant with vector<uint8_t>.
    // pybind automatically maps bytes -> vector<uint8_t> via our custom caster.

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
            py::arg("iterations") = HyperDBConstants::DEFAULT_PBKDF2_ITERATIONS,
            "saves data to disk IF it's dirty AND the flush interval has elapsed. respects set_flush_interval.")
        .def("force_flush", &HyperDBManager::ForceFlush,
            py::arg("iterations") = HyperDBConstants::DEFAULT_PBKDF2_ITERATIONS,
            "saves data to disk NOW. no excuses, no waiting on the interval.")
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

        // new in 1.0.6: state inspection
        .def("is_encrypted", &HyperDBManager::IsEncrypted,
            "am i currently configured to encrypt on flush? this answers that.")
        .def("get_path", &HyperDBManager::GetPath,
            py::return_value_policy::copy,
            "the file path you passed to open_db. empty string if open_db was never called.")

        // new in 1.0.6: mirror inspection
        // NOTE: the mirror is mutated by the worker thread. if you call this while
        // writes are happening, you're racing. wait_for_queue() first if you care.
        // i do not care. you might. just letting you know.
        .def("get_mirror", &HyperDBManager::GetMirror,
            py::return_value_policy::reference_internal,
            "returns the DatabaseMirror — tables, columns, types, row counts. "
            "the per-column data arrays are NOT exposed (use queue_read/queue_find for that). "
            "treat as read-only and ideally call wait_for_queue() first.")

        // blocks until the worker queue is fully drained.
        // releases the GIL while spinning so the worker thread can acquire it
        // when it needs to fire a read/find callback. without the release you'd
        // deadlock on any queue_find or queue_read call — the callback needs
        // the GIL and your main thread is holding it in a busy loop. not ideal.
        .def("wait_for_queue", [](HyperDBManager& self) {
            while (!self.IsQueueEmpty())
                std::this_thread::yield();
        }, py::call_guard<py::gil_scoped_release>(),
           "blocks until the worker queue is fully drained. use this instead of a manual sleep loop.")

        // context manager support. `with HyperDB.HyperDBManager() as db: ...`
        // forces a flush on exit so you don't lose work because you forgot.
        // you would have forgotten. don't lie.
        .def("__enter__", [](HyperDBManager &self) -> HyperDBManager& { return self; },
             py::return_value_policy::reference)
        .def("__exit__", [](HyperDBManager &self, py::object, py::object, py::object) {
            // swallow flush errors on the way out — we're already exiting and there's
            // nothing useful to do with the exception. the original __exit__ args (if
            // any) propagate via the `return false` (no suppression).
            try { self.ForceFlush(); } catch (...) {}
            return false;
        })

        .def("__repr__", [](HyperDBManager &self) {
            std::ostringstream oss;
            const std::string &p = self.GetPath();
            oss << "HyperDBManager(path='" << (p.empty() ? "<unopened>" : p)
                << "', encrypted=" << (self.IsEncrypted() ? "true" : "false")
                << ", dirty=" << (self.IsDirty() ? "true" : "false")
                << ", mirror_size=" << self.EstimateMirrorSize() << ")";
            return oss.str();
        });

    // -----------------------------------------------------------------------
    // HyperDBCluster - for when one file isn't enough pain
    // -----------------------------------------------------------------------
    py::class_<HyperDBCluster>(m, "HyperDBCluster", "handles multiple database shards. for the big boys")
        .def(py::init<>())
        .def("open", &HyperDBCluster::Open,
            py::arg("folder"), py::arg("name"), py::arg("password") = "",
            py::arg("shard_limit_bytes") = HyperDBCluster::DEFAULT_SHARD_LIMIT, py::arg("encrypt") = true,
            "opens or creates a cluster in the given folder")
        .def("flush", &HyperDBCluster::Flush, py::arg("iterations") = HyperDBConstants::DEFAULT_PBKDF2_ITERATIONS,
             "flushes all shards if dirty AND interval elapsed")
        .def("set_flush_interval", &HyperDBCluster::SetFlushInterval, py::arg("ms"))
        .def("force_flush", &HyperDBCluster::ForceFlush, py::arg("iterations") = HyperDBConstants::DEFAULT_PBKDF2_ITERATIONS,
             "flushes all shards NOW")
        .def("set_encryption", &HyperDBCluster::SetEncryption, py::arg("encrypt"), py::arg("password") = "")
        .def("is_queue_empty", &HyperDBCluster::IsQueueEmpty)

        .def("queue_create_table", &HyperDBCluster::QueueCreateTable,
            py::arg("table_name"), py::arg("cols"))
        .def("queue_drop_table", &HyperDBCluster::QueueDropTable, py::arg("table_name"), py::arg("target") = ShardTarget::All)
        .def("queue_clear_table", &HyperDBCluster::QueueClearTable, py::arg("table_name"), py::arg("target") = ShardTarget::All)
        .def("queue_clear_column", &HyperDBCluster::QueueClearColumn, py::arg("table_name"), py::arg("column_name"), py::arg("target") = ShardTarget::All)

        .def("queue_write", &HyperDBCluster::QueueWrite,
            py::arg("table_name"), py::arg("row"))
        .def("queue_write_bulk", &HyperDBCluster::QueueWriteBulk,
            py::arg("table_name"), py::arg("rows"))

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

        .def("get_row_count", &HyperDBCluster::GetRowCount, py::arg("table_name"))
        .def("get_shard_count", &HyperDBCluster::GetShardCount)
        .def("get_active_shard", &HyperDBCluster::GetActiveShard)

        // new in 1.0.6: cluster-wide diagnostics
        .def("is_dirty", &HyperDBCluster::IsDirty,
            "true if ANY shard has unsaved data. fans out across every shard under the manifest lock.")
        .def("estimate_mirror_size", &HyperDBCluster::EstimateMirrorSize,
            "sum of EstimateMirrorSize across every shard. rough.")
        .def("is_encrypted", &HyperDBCluster::IsEncrypted,
            "am i currently configured to encrypt on flush?")
        .def("get_folder", &HyperDBCluster::GetFolder,
            py::return_value_policy::copy,
            "the folder you passed to open(). empty if open was never called.")
        .def("get_name", &HyperDBCluster::GetName,
            py::return_value_policy::copy,
            "the cluster name you passed to open(). same caveat.")
        .def("get_schemas", &HyperDBCluster::GetClusterSchemas,
            py::return_value_policy::reference_internal,
            "dict of table_name -> list of ColumnDef. the cluster-wide schema view. "
            "treat as read-only or weird things will happen.")
        .def("get_manifest", &HyperDBCluster::GetManifestTables,
            py::return_value_policy::reference_internal,
            "dict of table_name -> list of ShardTableEntry. which shards own which row-id ranges per table.")
        .def("get_shard", &HyperDBCluster::GetShard,
            py::arg("index"),
            py::return_value_policy::reference_internal,
            "direct access to shard #index as a HyperDBManager. raises if index is out of range. "
            "useful for diagnostics like 'which shard is dirty'.")

        // same GIL release pattern as HyperDBManager.wait_for_queue.
        // cluster IsQueueEmpty checks all shards — if any shard still has work,
        // we keep spinning. the GIL release lets all of them fire their callbacks.
        .def("wait_for_queue", [](HyperDBCluster& self) {
            while (!self.IsQueueEmpty())
                std::this_thread::yield();
        }, py::call_guard<py::gil_scoped_release>(),
           "blocks until all shard worker queues are fully drained. use this instead of a manual sleep loop.")

        // context manager support — same idea as the manager.
        // `with HyperDB.HyperDBManager()...` etc. force-flushes on exit.
        .def("__enter__", [](HyperDBCluster &self) -> HyperDBCluster& { return self; },
             py::return_value_policy::reference)
        .def("__exit__", [](HyperDBCluster &self, py::object, py::object, py::object) {
            try { self.ForceFlush(); } catch (...) {}
            return false;
        })

        .def("__repr__", [](HyperDBCluster &self) {
            std::ostringstream oss;
            const std::string &n = self.GetName();
            const std::string &f = self.GetFolder();
            oss << "HyperDBCluster(name='" << (n.empty() ? "<unopened>" : n)
                << "', folder='" << f
                << "', shards=" << self.GetShardCount()
                << ", active=" << self.GetActiveShard()
                << ", encrypted=" << (self.IsEncrypted() ? "true" : "false") << ")";
            return oss.str();
        });
}
