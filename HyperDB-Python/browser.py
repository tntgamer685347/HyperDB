import sys
import os
import csv
import json
import binascii
import HyperDB
from PySide6.QtCore import Qt, QSize, QPoint
from PySide6.QtGui import QAction, QPalette, QColor, QIcon, QFont
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QSplitter,
    QListWidget, QTableWidget, QTableWidgetItem, QPushButton, QLineEdit,
    QLabel, QToolBar, QStatusBar, QDialog, QFormLayout, QDialogButtonBox,
    QCheckBox, QFileDialog, QMessageBox, QInputDialog, QHeaderView,
    QComboBox, QPlainTextEdit, QScrollArea, QFrame, QMenu, QAbstractItemView,
    QStackedWidget, QSpinBox
)

# ----------------------------------------------------------------------------
# column type catalogue
# ----------------------------------------------------------------------------

# (display name, ColumnType enum, default python value used as type template
#  for the AddRow dialog's parse_like inference)
COLUMN_TYPES = [
    ("Int8",    HyperDB.ColumnType.Int8,    0),
    ("Int16",   HyperDB.ColumnType.Int16,   0),
    ("Int32",   HyperDB.ColumnType.Int32,   0),
    ("Int64",   HyperDB.ColumnType.Int64,   0),
    ("UInt8",   HyperDB.ColumnType.UInt8,   0),
    ("UInt16",  HyperDB.ColumnType.UInt16,  0),
    ("UInt32",  HyperDB.ColumnType.UInt32,  0),
    ("UInt64",  HyperDB.ColumnType.UInt64,  0),
    ("Float32", HyperDB.ColumnType.Float32, 0.0),
    ("Float64", HyperDB.ColumnType.Float64, 0.0),
    ("Bool",    HyperDB.ColumnType.Bool,    False),
    ("String",  HyperDB.ColumnType.String,  ""),
    ("Bytes",   HyperDB.ColumnType.Bytes,   b""),
]
TYPE_BY_NAME = {n: (e, t) for n, e, t in COLUMN_TYPES}
TEMPLATE_BY_ENUM = {e: t for _, e, t in COLUMN_TYPES}


def safe_iter(seq):
    """Iterate sequences whose bindings raise on past-the-end instead of stopping."""
    n = len(seq)
    for i in range(n):
        yield seq[i]


SORT_KEY_ROLE = Qt.UserRole + 1


class SmartTableItem(QTableWidgetItem):
    """Sorts by the value stashed in SORT_KEY_ROLE when both items have one;
    otherwise falls back to text compare."""
    def __lt__(self, other):
        a = self.data(SORT_KEY_ROLE)
        b = other.data(SORT_KEY_ROLE) if isinstance(other, QTableWidgetItem) else None
        if a is not None and b is not None:
            try:
                return a < b
            except TypeError:
                pass
        return super().__lt__(other)

# ----------------------------------------------------------------------------
# helpers
# ----------------------------------------------------------------------------

def value_to_display(v):
    if isinstance(v, (bytes, bytearray)):
        return v.hex()
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)

def parse_like(template, text):
    # convert a string from the editor into the same python type as `template`.
    if isinstance(template, bool):
        return text.strip().lower() in ("1", "true", "yes", "y", "t")
    if isinstance(template, int):
        return int(text)
    if isinstance(template, float):
        return float(text)
    if isinstance(template, (bytes, bytearray)):
        return bytes.fromhex(text.strip())
    return text

# ----------------------------------------------------------------------------
# adapters: unify Manager and Cluster behind one interface
# ----------------------------------------------------------------------------

DEFAULT_ITERATIONS = 58253


class DBAdapter:
    """Common surface for HyperDBManager and HyperDBCluster."""
    kind = "db"
    label = ""
    iterations = DEFAULT_ITERATIONS

    def list_tables(self):
        """Returns list[(name, schema)] where schema is list[(col_name, python_template_value)]."""
        raise NotImplementedError

    def get_row_count(self, table): raise NotImplementedError
    def queue_read(self, table, row_id, cb): raise NotImplementedError
    def queue_find(self, table, col, value, cb): raise NotImplementedError
    def queue_write(self, table, row): raise NotImplementedError
    def queue_delete(self, table, col, value): raise NotImplementedError
    def queue_create_table(self, name, cols): raise NotImplementedError
    def queue_drop_table(self, name): raise NotImplementedError
    def queue_clear_table(self, name): raise NotImplementedError
    def queue_clear_column(self, name, col): raise NotImplementedError
    def force_flush(self): raise NotImplementedError
    def wait_for_queue(self): raise NotImplementedError
    def is_dirty(self): return False
    def is_encrypted(self): return False
    def extra_status(self): return ""   # cluster appends shard info, etc.


class ManagerAdapter(DBAdapter):
    kind = "manager"

    def __init__(self, mgr: "HyperDB.HyperDBManager", path: str, iterations: int = DEFAULT_ITERATIONS):
        self.mgr = mgr
        self.path = path
        self.label = path
        self.iterations = iterations

    def list_tables(self):
        out = []
        mir = self.mgr.get_mirror()
        for t in safe_iter(mir.tables):
            schema = []
            for c in safe_iter(t.columns):
                schema.append((c.name, TEMPLATE_BY_ENUM.get(c.type, "")))
            out.append((t.name, schema))
        return out

    def get_row_count(self, table):       return self.mgr.get_row_count(table)
    def queue_read(self, table, rid, cb): self.mgr.queue_read(table, rid, cb)
    def queue_find(self, table, c, v, cb):self.mgr.queue_find(table, c, v, cb)
    def queue_write(self, table, row):    self.mgr.queue_write(table, row)
    def queue_delete(self, table, c, v):  self.mgr.queue_delete(table, c, v)
    def queue_create_table(self, name, cols): self.mgr.queue_create_table(name, cols)
    def queue_drop_table(self, name):     self.mgr.queue_drop_table(name)
    def queue_clear_table(self, name):    self.mgr.queue_clear_table(name)
    def queue_clear_column(self, n, c):   self.mgr.queue_clear_column(n, c)
    def force_flush(self):                self.mgr.force_flush(self.iterations)
    def wait_for_queue(self):             self.mgr.wait_for_queue()
    def is_dirty(self):                   return self.mgr.is_dirty()
    def is_encrypted(self):               return self.mgr.is_encrypted()


class ClusterAdapter(DBAdapter):
    kind = "cluster"

    def __init__(self, cluster: "HyperDB.HyperDBCluster", folder: str, name: str,
                 iterations: int = DEFAULT_ITERATIONS):
        self.cl = cluster
        self.folder = folder
        self.name = name
        self.label = f"{folder} / {name}"
        self.iterations = iterations

    def list_tables(self):
        out = []
        schemas = self.cl.get_schemas()  # dict[str, list[ColumnDef]]
        for tname, cols in schemas.items():
            schema = [(cd.name, TEMPLATE_BY_ENUM.get(cd.type, "")) for cd in cols]
            out.append((tname, schema))
        return out

    def get_row_count(self, table):       return self.cl.get_row_count(table)
    def queue_read(self, table, rid, cb): self.cl.queue_read(table, rid, cb)
    def queue_find(self, table, c, v, cb):self.cl.queue_find(table, c, v, cb)
    def queue_write(self, table, row):    self.cl.queue_write(table, row)
    def queue_delete(self, table, c, v):  self.cl.queue_delete(table, c, v)
    def queue_create_table(self, name, cols): self.cl.queue_create_table(name, cols)
    def queue_drop_table(self, name):     self.cl.queue_drop_table(name)
    def queue_clear_table(self, name):    self.cl.queue_clear_table(name)
    def queue_clear_column(self, n, c):   self.cl.queue_clear_column(n, c)
    def force_flush(self):                self.cl.force_flush(self.iterations)
    def wait_for_queue(self):             self.cl.wait_for_queue()
    def is_dirty(self):                   return self.cl.is_dirty()
    def is_encrypted(self):               return self.cl.is_encrypted()

    def extra_status(self):
        return f"shards: {self.cl.get_shard_count()} (active #{self.cl.get_active_shard()})"


# ----------------------------------------------------------------------------
# dialogs
# ----------------------------------------------------------------------------

class OpenDialog(QDialog):
    def __init__(self, parent=None, intent="open"):
        # intent: "open" or "new"
        super().__init__(parent)
        self.intent = intent
        self.setWindowTitle("New Database" if intent == "new" else "Open Database")
        self.setMinimumWidth(460)
        root = QVBoxLayout(self)

        # mode selector
        mode_row = QHBoxLayout()
        mode_row.addWidget(QLabel("Mode:"))
        self.mode_combo = QComboBox()
        self.mode_combo.addItem("Single File (Manager)", "manager")
        self.mode_combo.addItem("Cluster (Folder)", "cluster")
        self.mode_combo.currentIndexChanged.connect(self._on_mode_changed)
        mode_row.addWidget(self.mode_combo, 1)
        root.addLayout(mode_row)

        form = QFormLayout()
        root.addLayout(form)

        # ---- file mode ----
        self.path_edit = QLineEdit("example.db")
        path_row = QHBoxLayout()
        path_row.addWidget(self.path_edit)
        path_btn = QPushButton("…")
        path_btn.setFixedWidth(28)
        path_btn.clicked.connect(self._browse_file)
        path_row.addWidget(path_btn)
        self.path_wrap = QWidget()
        self.path_wrap.setLayout(path_row)
        self.path_label = QLabel("File:")
        form.addRow(self.path_label, self.path_wrap)

        # ---- cluster mode (OPEN intent: pick a manifest) ----
        self.manifest_edit = QLineEdit()
        self.manifest_edit.setPlaceholderText("path/to/<name>.manifest")
        manifest_row = QHBoxLayout()
        manifest_row.addWidget(self.manifest_edit)
        manifest_btn = QPushButton("…")
        manifest_btn.setFixedWidth(28)
        manifest_btn.clicked.connect(self._browse_manifest)
        manifest_row.addWidget(manifest_btn)
        self.manifest_wrap = QWidget()
        self.manifest_wrap.setLayout(manifest_row)
        self.manifest_label = QLabel("Manifest:")
        form.addRow(self.manifest_label, self.manifest_wrap)

        # ---- cluster mode (NEW intent: folder + name + shard limit) ----
        self.folder_edit = QLineEdit("cluster_demo")
        folder_row = QHBoxLayout()
        folder_row.addWidget(self.folder_edit)
        folder_btn = QPushButton("…")
        folder_btn.setFixedWidth(28)
        folder_btn.clicked.connect(self._browse_folder)
        folder_row.addWidget(folder_btn)
        self.folder_wrap = QWidget()
        self.folder_wrap.setLayout(folder_row)
        self.folder_label = QLabel("Folder:")
        form.addRow(self.folder_label, self.folder_wrap)

        self.name_edit = QLineEdit("demo")
        self.name_label = QLabel("Cluster name:")
        form.addRow(self.name_label, self.name_edit)

        self.shard_edit = QLineEdit("512")
        self.shard_label = QLabel("Shard limit (MB):")
        form.addRow(self.shard_label, self.shard_edit)

        # ---- shared ----
        self.pw_edit = QLineEdit("example_password")
        self.pw_edit.setEchoMode(QLineEdit.Password)
        form.addRow("Password:", self.pw_edit)

        self.enc_check = QCheckBox("Encrypted")
        self.enc_check.setChecked(True)
        form.addRow("", self.enc_check)

        self.iter_spin = QSpinBox()
        self.iter_spin.setRange(1, 10_000_000)
        self.iter_spin.setValue(DEFAULT_ITERATIONS)
        self.iter_spin.setSuffix("  rounds")
        form.addRow("PBKDF2 iterations:", self.iter_spin)

        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        if intent == "new":
            bb.button(QDialogButtonBox.Ok).setText("Create")
        else:
            bb.button(QDialogButtonBox.Ok).setText("Open")
        bb.accepted.connect(self.accept)
        bb.rejected.connect(self.reject)
        root.addWidget(bb)

        if intent == "new":
            # blank defaults for a fresh DB
            self.path_edit.setText("new.db")
            self.folder_edit.setText("new_cluster")
            self.name_edit.setText("main")

        self._on_mode_changed()

    def _on_mode_changed(self):
        cluster = self.mode_combo.currentData() == "cluster"
        opening = self.intent == "open"
        # manager file picker
        self.path_label.setVisible(not cluster)
        self.path_wrap.setVisible(not cluster)
        # cluster open: just the manifest picker
        show_manifest = cluster and opening
        self.manifest_label.setVisible(show_manifest)
        self.manifest_wrap.setVisible(show_manifest)
        # cluster new: folder + name + shard limit
        show_new_cluster = cluster and not opening
        self.folder_label.setVisible(show_new_cluster)
        self.folder_wrap.setVisible(show_new_cluster)
        self.name_label.setVisible(show_new_cluster)
        self.name_edit.setVisible(show_new_cluster)
        self.shard_label.setVisible(show_new_cluster)
        self.shard_edit.setVisible(show_new_cluster)

    def _browse_file(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open Database", "", "All Files (*)")
        if path:
            self.path_edit.setText(path)

    def _browse_folder(self):
        folder = QFileDialog.getExistingDirectory(self, "Cluster Folder")
        if folder:
            self.folder_edit.setText(folder)

    def _browse_manifest(self):
        path, _ = QFileDialog.getOpenFileName(
            self, "Pick Cluster Manifest", "", "Manifest (*.manifest);;All Files (*)")
        if not path:
            return
        self.manifest_edit.setText(path)
        # auto-fill encrypted flag from the manifest
        try:
            with open(path, "r", encoding="utf-8") as f:
                m = json.load(f)
            if "should_encrypt" in m:
                self.enc_check.setChecked(bool(m["should_encrypt"]))
        except Exception:
            pass

    @staticmethod
    def _parse_manifest(path):
        """Returns dict with folder, name, shard_limit, encrypted parsed from a *.manifest."""
        with open(path, "r", encoding="utf-8") as f:
            m = json.load(f)
        folder = os.path.dirname(os.path.abspath(path)) or "."
        base = os.path.basename(path)
        if base.lower().endswith(".manifest"):
            name = base[:-len(".manifest")]
        else:
            name = os.path.splitext(base)[0]
        return {
            "folder": folder,
            "name": name,
            "shard_limit": int(m.get("shard_limit", 512 * 1024 * 1024)),
            "encrypted": bool(m.get("should_encrypt", True)),
        }

    def values(self):
        mode = self.mode_combo.currentData()
        common = dict(password=self.pw_edit.text(),
                      encrypted=self.enc_check.isChecked(),
                      iterations=self.iter_spin.value())
        if mode == "cluster":
            if self.intent == "open":
                manifest_path = self.manifest_edit.text().strip()
                info = self._parse_manifest(manifest_path)
                return {"mode": "cluster",
                        "folder": info["folder"],
                        "name": info["name"],
                        "shard_limit": info["shard_limit"],
                        **common}
            try:
                shard_mb = max(1, int(self.shard_edit.text()))
            except ValueError:
                shard_mb = 512
            return {"mode": "cluster",
                    "folder": self.folder_edit.text(),
                    "name": self.name_edit.text(),
                    "shard_limit": shard_mb * 1024 * 1024,
                    **common}
        return {"mode": "manager", "path": self.path_edit.text(), **common}


class CreateTableDialog(QDialog):
    """Define a new table: name + ordered list of (column_name, ColumnType)."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Create Table")
        self.setMinimumWidth(520)
        self.setMinimumHeight(420)

        root = QVBoxLayout(self)

        top = QFormLayout()
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("table name")
        top.addRow("Table name:", self.name_edit)
        root.addLayout(top)

        root.addWidget(QLabel("Columns:"))

        # scrollable list of column rows
        self.rows_host = QWidget()
        self.rows_lay = QVBoxLayout(self.rows_host)
        self.rows_lay.setContentsMargins(0, 0, 0, 0)
        self.rows_lay.setSpacing(4)
        self.rows_lay.addStretch(1)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(self.rows_host)
        scroll.setFrameShape(QFrame.NoFrame)
        root.addWidget(scroll, 1)

        btn_row = QHBoxLayout()
        add_btn = QPushButton("+ Add column")
        add_btn.clicked.connect(lambda: self._add_row())
        btn_row.addWidget(add_btn)
        btn_row.addStretch(1)
        root.addLayout(btn_row)

        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        bb.accepted.connect(self._on_accept)
        bb.rejected.connect(self.reject)
        root.addWidget(bb)

        # seed with one column row
        self._add_row()

    def _add_row(self, name="", type_name="Int32"):
        row_w = QWidget()
        row = QHBoxLayout(row_w)
        row.setContentsMargins(0, 0, 0, 0)

        name_edit = QLineEdit(name)
        name_edit.setPlaceholderText("column name")
        type_combo = QComboBox()
        for tn, _, _ in COLUMN_TYPES:
            type_combo.addItem(tn)
        idx = type_combo.findText(type_name)
        if idx >= 0:
            type_combo.setCurrentIndex(idx)

        rm_btn = QPushButton("✕")
        rm_btn.setFixedWidth(28)

        row.addWidget(name_edit, 2)
        row.addWidget(type_combo, 1)
        row.addWidget(rm_btn, 0)

        def remove():
            self.rows_lay.removeWidget(row_w)
            row_w.deleteLater()
        rm_btn.clicked.connect(remove)

        # insert above the trailing stretch
        self.rows_lay.insertWidget(self.rows_lay.count() - 1, row_w)
        row_w.name_edit = name_edit
        row_w.type_combo = type_combo

    def _iter_row_widgets(self):
        for i in range(self.rows_lay.count()):
            w = self.rows_lay.itemAt(i).widget()
            if w is not None and hasattr(w, "name_edit"):
                yield w

    def _on_accept(self):
        if not self.name_edit.text().strip():
            QMessageBox.warning(self, "Missing", "Table name is required.")
            return
        cols = list(self._iter_row_widgets())
        if not cols:
            QMessageBox.warning(self, "Missing", "Add at least one column.")
            return
        seen = set()
        for w in cols:
            cn = w.name_edit.text().strip()
            if not cn:
                QMessageBox.warning(self, "Missing", "Every column needs a name.")
                return
            if cn in seen:
                QMessageBox.warning(self, "Duplicate", f"Column '{cn}' is listed twice.")
                return
            seen.add(cn)
        self.accept()

    def values(self):
        name = self.name_edit.text().strip()
        cols = []        # list[ColumnDef] for HyperDB
        templates = []   # list[(col_name, python_template_value)] for AddRow inference
        for w in self._iter_row_widgets():
            cn = w.name_edit.text().strip()
            tn = w.type_combo.currentText()
            enum_val, tmpl = TYPE_BY_NAME[tn]
            cols.append(HyperDB.ColumnDef(cn, enum_val))
            templates.append((cn, tmpl))
        return name, cols, templates


class AddRowDialog(QDialog):
    def __init__(self, columns, template_row, parent=None):
        # columns: list[str], template_row: list[RowData] from first existing row (or None)
        super().__init__(parent)
        self.setWindowTitle("Add Row")
        self.setMinimumWidth(440)
        self.columns = columns
        self.template_row = template_row
        self.edits = {}

        form = QFormLayout(self)
        for col in columns:
            le = QLineEdit()
            tmpl_val = None
            if template_row:
                for rd in template_row:
                    if rd.column_name == col:
                        tmpl_val = rd.value
                        break
            hint = f"  ({type(tmpl_val).__name__})" if tmpl_val is not None else ""
            form.addRow(col + hint + ":", le)
            self.edits[col] = (le, tmpl_val)

        bb = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        bb.accepted.connect(self.accept)
        bb.rejected.connect(self.reject)
        form.addRow(bb)

    def row_data(self):
        out = []
        for col, (le, tmpl) in self.edits.items():
            text = le.text()
            if tmpl is None:
                # no template — best-effort: try int, float, else string
                try:
                    val = int(text)
                except ValueError:
                    try:
                        val = float(text)
                    except ValueError:
                        val = text
            else:
                val = parse_like(tmpl, text)
            out.append(HyperDB.RowData(col, val))
        return out


# ----------------------------------------------------------------------------
# main window
# ----------------------------------------------------------------------------

class Browser(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("HyperDB Browser")
        self.resize(1200, 720)

        self.db = None          # DBAdapter
        self.page_size = 1000
        self.page_offset = 0
        self.total_rows = 0
        self.tables = []
        self.current_table = None
        self.current_rows = []      # list[list[RowData]]
        self.current_columns = []   # list[str]
        # known schemas for tables created in-session (or via Open dialog later):
        # name -> list[(col_name, python_template_value)]
        self.schemas = {}

        self._build_ui()
        self._update_status()
        self._show_welcome()

    # ---- ui ----------------------------------------------------------------

    def _build_ui(self):
        self.toolbar = QToolBar()
        self.toolbar.setObjectName("MainToolbar")
        self.toolbar.setIconSize(QSize(16, 16))
        self.toolbar.setMovable(False)
        self.toolbar.setFloatable(False)
        self.toolbar.setToolButtonStyle(Qt.ToolButtonTextOnly)
        self.addToolBar(self.toolbar)

        self.db_actions = []     # actions that should be hidden until a DB is open
        self.db_separators = []  # separators tied to db_actions

        def add_action(text, slot, needs_db=True):
            a = QAction(text, self)
            a.triggered.connect(slot)
            self.toolbar.addAction(a)
            if needs_db:
                self.db_actions.append(a)
            return a

        def add_separator(needs_db=True):
            sep = self.toolbar.addSeparator()
            if needs_db:
                self.db_separators.append(sep)
            return sep

        def add_gap(needs_db=True):
            spacer = QWidget()
            spacer.setFixedWidth(14)
            spacer.setAttribute(Qt.WA_TransparentForMouseEvents)
            act = self.toolbar.addWidget(spacer)
            if needs_db:
                self.db_separators.append(act)
            return act

        # group 1: file
        add_action("Open",            self.action_open, needs_db=False)
        add_action("New",             self.action_new,  needs_db=False)
        self._close_action = add_action("Close", self.action_close)
        add_gap()
        # group 2: persistence
        add_action("Save",            self.action_force_save)
        add_action("Reload",          self.action_reload)
        add_gap()
        # group 3: structure
        add_action("Add Table",       self.action_create_table)
        add_gap()
        # group 4: row ops
        add_action("Add Row",         self.action_add_row)
        add_action("Delete Selected", self.action_delete_selected)
        add_action("Find",            self.action_find)
        add_gap()
        # group 5: i/o
        add_action("Export CSV",      self.action_export_csv)
        add_action("Import CSV",      self.action_import_csv)

        self.stack = QStackedWidget()
        self.setCentralWidget(self.stack)

        # ---- welcome page ----
        self.welcome = self._build_welcome_page()
        self.stack.addWidget(self.welcome)

        # ---- main page (existing UI) ----
        self.main_page = QWidget()
        root = QHBoxLayout(self.main_page)
        root.setContentsMargins(12, 8, 12, 12)
        root.setSpacing(10)
        self.stack.addWidget(self.main_page)

        splitter = QSplitter(Qt.Horizontal)
        root.addWidget(splitter)

        # sidebar
        side = QWidget()
        side_lay = QVBoxLayout(side)
        side_lay.setContentsMargins(0, 0, 0, 0)
        side_lay.setSpacing(8)
        tables_lbl = QLabel("Tables")
        tables_lbl.setStyleSheet("color: #9aa0ad; font-weight: 600; padding: 2px 4px;")
        side_lay.addWidget(tables_lbl)
        self.table_list = QListWidget()
        self.table_list.itemSelectionChanged.connect(self._on_table_changed)
        self.table_list.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table_list.customContextMenuRequested.connect(self._sidebar_menu)
        side_lay.addWidget(self.table_list)
        splitter.addWidget(side)

        # right pane
        right = QWidget()
        right_lay = QVBoxLayout(right)
        right_lay.setContentsMargins(0, 0, 0, 0)
        right_lay.setSpacing(8)

        filter_row = QHBoxLayout()
        filter_row.setSpacing(8)
        filter_row.addWidget(QLabel("Filter:"))
        self.filter_col = QComboBox()
        self.filter_col.setMinimumWidth(140)
        filter_row.addWidget(self.filter_col)
        self.filter_edit = QLineEdit()
        self.filter_edit.setPlaceholderText("substring mask (case-insensitive)")
        self.filter_edit.textChanged.connect(self._apply_filter)
        self.filter_col.currentIndexChanged.connect(self._apply_filter)
        filter_row.addWidget(self.filter_edit, 1)
        clear = QPushButton("Clear")
        clear.clicked.connect(lambda: self.filter_edit.clear())
        filter_row.addWidget(clear)
        right_lay.addLayout(filter_row)

        self.table = QTableWidget()
        self.table.setAlternatingRowColors(True)
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self.table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self.table.setEditTriggers(QAbstractItemView.DoubleClicked | QAbstractItemView.EditKeyPressed)
        self.table.horizontalHeader().setSectionResizeMode(QHeaderView.Interactive)
        self.table.horizontalHeader().setStretchLastSection(True)
        self.table.horizontalHeader().setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.horizontalHeader().customContextMenuRequested.connect(self._header_menu)
        self.table.setContextMenuPolicy(Qt.CustomContextMenu)
        self.table.customContextMenuRequested.connect(self._row_menu)
        self.table.itemChanged.connect(self._on_cell_edited)
        self.table.setSortingEnabled(True)
        self._suppress_item_changed = False
        right_lay.addWidget(self.table)

        # page navigation
        page_row = QHBoxLayout()
        page_row.setSpacing(6)
        self.page_label = QLabel("—")
        self.page_label.setStyleSheet("color: #9aa0ad;")
        page_row.addWidget(self.page_label, 1)

        def page_btn(text, slot):
            b = QPushButton(text)
            b.setFixedHeight(26)
            b.clicked.connect(slot)
            return b
        self.btn_first = page_btn("« First", lambda: self._goto_page(0))
        self.btn_prev  = page_btn("‹ Prev",  self._page_prev)
        self.btn_next  = page_btn("Next ›",  self._page_next)
        self.btn_last  = page_btn("Last »",  self._goto_last_page)
        self.page_size_spin = QSpinBox()
        self.page_size_spin.setRange(10, 100000)
        self.page_size_spin.setSingleStep(100)
        self.page_size_spin.setValue(self.page_size)
        self.page_size_spin.setSuffix("  /page")
        self.page_size_spin.editingFinished.connect(self._on_page_size_changed)
        for w in (self.btn_first, self.btn_prev, self.btn_next, self.btn_last, self.page_size_spin):
            page_row.addWidget(w)
        right_lay.addLayout(page_row)

        splitter.addWidget(right)
        splitter.setStretchFactor(0, 0)
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([220, 980])

        self.status = QStatusBar()
        self.setStatusBar(self.status)

    # ---- welcome page ------------------------------------------------------

    def _build_welcome_page(self):
        page = QWidget()
        page.setObjectName("WelcomePage")
        outer = QVBoxLayout(page)
        outer.setContentsMargins(40, 40, 40, 40)
        outer.addStretch(1)

        card = QWidget()
        card.setObjectName("WelcomeCard")
        card.setMaximumWidth(560)
        card_lay = QVBoxLayout(card)
        card_lay.setContentsMargins(40, 40, 40, 40)
        card_lay.setSpacing(18)

        title = QLabel("HyperDB Browser")
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("font-size: 22pt; font-weight: 700; color: #e6e8ef;")
        sub = QLabel("Inspect, edit, and manage HyperDB files and clusters.")
        sub.setAlignment(Qt.AlignCenter)
        sub.setStyleSheet("color: #9aa0ad; font-size: 11pt;")

        card_lay.addWidget(title)
        card_lay.addWidget(sub)
        card_lay.addSpacing(20)

        btn_row = QHBoxLayout()
        btn_row.setSpacing(14)

        def big_btn(text, hint, slot, primary=False):
            b = QPushButton(f"{text}\n{hint}")
            b.setMinimumHeight(96)
            b.setCursor(Qt.PointingHandCursor)
            base = "#7c8cff" if primary else "#1c1f26"
            border = "#7c8cff" if primary else "#2a2f3a"
            text_color = "#ffffff" if primary else "#e6e8ef"
            b.setStyleSheet(f"""
                QPushButton {{
                    background: {base};
                    color: {text_color};
                    border: 1px solid {border};
                    border-radius: 14px;
                    font-size: 12pt;
                    font-weight: 600;
                    padding: 14px 22px;
                    text-align: center;
                }}
                QPushButton:hover {{
                    background: {'#9aa6ff' if primary else '#232730'};
                    border-color: {'#9aa6ff' if primary else '#3a4154'};
                }}
                QPushButton:pressed {{
                    background: #6b7aff;
                    color: #ffffff;
                    border-color: #6b7aff;
                }}
            """)
            b.clicked.connect(slot)
            return b

        btn_row.addWidget(big_btn("New Database", "create a fresh DB or cluster",
                                  self.action_new, primary=True))
        btn_row.addWidget(big_btn("Open Database", "browse to an existing file or cluster",
                                  self.action_open))
        card_lay.addLayout(btn_row)

        card_lay.addSpacing(8)
        hint = QLabel("Pick a mode (file / cluster), tweak encryption + iterations, you're in.")
        hint.setAlignment(Qt.AlignCenter)
        hint.setStyleSheet("color: #6b7280; font-size: 9pt;")
        card_lay.addWidget(hint)

        center_row = QHBoxLayout()
        center_row.addStretch(1)
        center_row.addWidget(card)
        center_row.addStretch(1)
        outer.addLayout(center_row)
        outer.addStretch(1)

        page.setStyleSheet("""
            #WelcomePage { background: #15171c; }
            #WelcomeCard {
                background: #1c1f26;
                border: 1px solid #2a2f3a;
                border-radius: 18px;
            }
        """)
        return page

    def _show_welcome(self):
        self.stack.setCurrentWidget(self.welcome)
        for a in self.db_actions:
            a.setVisible(False)
        for s in self.db_separators:
            s.setVisible(False)
        self.status.clearMessage()

    def _show_main(self):
        self.stack.setCurrentWidget(self.main_page)
        for a in self.db_actions:
            a.setVisible(True)
        for s in self.db_separators:
            s.setVisible(True)

    # ---- status ------------------------------------------------------------

    def _update_status(self):
        if self.db is None:
            self.status.showMessage("no database open")
            return
        parts = [f"{self.db.kind}: {self.db.label}",
                 "ENCRYPTED" if self.db.is_encrypted() else "PLAIN"]
        extra = self.db.extra_status()
        if extra:
            parts.append(extra)
        if self.current_table:
            parts.append(f"table: {self.current_table}")
            parts.append(f"rows: {self.total_rows:,}")
            if self.db.is_dirty():
                parts.append("DIRTY")
        self.status.showMessage("  |  ".join(parts))

    # ---- actions -----------------------------------------------------------

    def action_open(self):
        self._open_with_intent("open")

    def action_new(self):
        self._open_with_intent("new")

    def action_close(self):
        if self.db is None:
            self._show_welcome()
            return
        if self.db.is_dirty():
            r = QMessageBox.question(self, "Unsaved changes",
                "There are unsaved changes. Force-save before closing?",
                QMessageBox.Yes | QMessageBox.No | QMessageBox.Cancel)
            if r == QMessageBox.Cancel:
                return
            if r == QMessageBox.Yes:
                try:
                    self.db.force_flush()
                except Exception as e:
                    QMessageBox.critical(self, "Save failed", str(e))
                    return
        self.db = None
        self.tables = []
        self.schemas = {}
        self.current_table = None
        self.current_rows = []
        self.current_columns = []
        self.total_rows = 0
        self.page_offset = 0
        self.table_list.clear()
        self._render_rows([])
        self._update_page_bar()
        self._update_status()
        self._show_welcome()

    def _open_with_intent(self, intent):
        dlg = OpenDialog(self, intent=intent)
        if dlg.exec() != QDialog.Accepted:
            return
        try:
            cfg = dlg.values()
        except Exception as e:
            QMessageBox.critical(self, "Manifest error",
                f"Could not read manifest: {e}")
            return
        try:
            if cfg["mode"] == "cluster":
                folder = cfg["folder"]
                if folder and not os.path.exists(folder):
                    os.makedirs(folder, exist_ok=True)
                cl = HyperDB.HyperDBCluster()
                cl.open(folder, cfg["name"], cfg["password"], cfg["shard_limit"], cfg["encrypted"])
                adapter = ClusterAdapter(cl, folder, cfg["name"], cfg["iterations"])
            else:
                mgr = HyperDB.HyperDBManager()
                mgr.open_db(cfg["path"], cfg["password"], cfg["encrypted"])
                adapter = ManagerAdapter(mgr, cfg["path"], cfg["iterations"])
        except Exception as e:
            QMessageBox.critical(self, "Open failed", str(e))
            return
        self.db = adapter
        self._refresh_tables_from_db()
        self._update_status()
        self._show_main()

    def _refresh_tables_from_db(self):
        """Auto-detect tables + schemas via the adapter."""
        self.tables = []
        self.schemas = {}
        try:
            for name, schema in self.db.list_tables():
                self.tables.append(name)
                self.schemas[name] = schema
        except Exception as e:
            QMessageBox.critical(self, "Schema fetch failed", str(e))
            return

        self.table_list.clear()
        for t in self.tables:
            self.table_list.addItem(t)
        if self.tables:
            self.table_list.setCurrentRow(0)

    def action_create_table(self):
        if self.db is None:
            QMessageBox.warning(self, "No DB", "Open a database first.")
            return
        dlg = CreateTableDialog(self)
        if dlg.exec() != QDialog.Accepted:
            return
        name, cols, templates = dlg.values()
        if name in self.tables:
            QMessageBox.warning(self, "Exists", f"Table '{name}' is already listed.")
            return
        try:
            self.db.queue_create_table(name, cols)
            self.db.wait_for_queue()
        except Exception as e:
            QMessageBox.critical(self, "Create failed", str(e))
            return
        # re-pull authoritative schemas/tables from the mirror
        self._refresh_tables_from_db()
        # select the new one
        for i in range(self.table_list.count()):
            if self.table_list.item(i).text() == name:
                self.table_list.setCurrentRow(i)
                break
        self._update_status()

    def action_reload(self):
        self._load_current_table()

    def action_force_save(self):
        if self.db is None:
            return
        try:
            self.db.force_flush()
            QMessageBox.information(self, "Saved", "Force flush complete.")
        except Exception as e:
            QMessageBox.critical(self, "Save failed", str(e))
        self._update_status()

    def action_add_row(self):
        if self.db is None or not self.current_table:
            return
        schema = self.schemas.get(self.current_table)
        if not self.current_columns and not schema:
            QMessageBox.warning(self, "No schema",
                "Cannot determine columns — table is empty and was not created in this session. "
                "Use Create Table, or add a row via code first so types can be inferred.")
            return
        if self.current_rows:
            template = self.current_rows[0]
            columns = self.current_columns
        else:
            # build a synthetic template row from the stored schema
            columns = [c for c, _ in schema]
            template = [HyperDB.RowData(c, t) for c, t in schema]
        dlg = AddRowDialog(columns, template, self)
        if dlg.exec() != QDialog.Accepted:
            return
        try:
            self.db.queue_write(self.current_table, dlg.row_data())
            self.db.wait_for_queue()
        except Exception as e:
            QMessageBox.critical(self, "Write failed", str(e))
            return
        self._load_current_table()

    def action_delete_selected(self):
        if self.db is None or not self.current_table:
            return
        rows = sorted({i.row() for i in self.table.selectedIndexes()})
        if not rows:
            return
        if not self.current_columns:
            return
        # use the first column as the match key (assume it's unique-ish, e.g. id)
        key_col = self.current_columns[0]
        # map view rows back to underlying RowData via stored mapping
        keys = []
        for r in rows:
            item = self.table.item(r, 0)
            if item is None:
                continue
            rd_list = item.data(Qt.UserRole)
            if rd_list is None:
                continue
            for rd in rd_list:
                if rd.column_name == key_col:
                    keys.append(rd.value)
                    break
        if not keys:
            return
        msg = f"Delete {len(keys)} row(s) where {key_col} matches selection?"
        if QMessageBox.question(self, "Confirm delete", msg) != QMessageBox.Yes:
            return
        try:
            for k in keys:
                self.db.queue_delete(self.current_table, key_col, k)
            self.db.wait_for_queue()
        except Exception as e:
            QMessageBox.critical(self, "Delete failed", str(e))
            return
        self._load_current_table()

    def action_find(self):
        if self.db is None or not self.current_table or not self.current_columns:
            return
        col, ok = QInputDialog.getItem(self, "Find", "Column:", self.current_columns, 0, False)
        if not ok:
            return
        # use first row to know the value type for parsing
        template_val = None
        if self.current_rows:
            for rd in self.current_rows[0]:
                if rd.column_name == col:
                    template_val = rd.value
                    break
        text, ok = QInputDialog.getText(self, "Find",
            f"Value for '{col}' ({type(template_val).__name__ if template_val is not None else 'string'}):")
        if not ok:
            return
        try:
            value = parse_like(template_val, text) if template_val is not None else text
        except Exception as e:
            QMessageBox.warning(self, "Bad value", str(e))
            return

        results = []
        def cb(res):
            results.extend(res)
        try:
            self.db.queue_find(self.current_table, col, value, cb)
            self.db.wait_for_queue()
        except Exception as e:
            QMessageBox.critical(self, "Find failed", str(e))
            return
        if not results:
            QMessageBox.information(self, "Find", "No matches.")
            return
        self._render_rows(results)
        self.status.showMessage(f"find {col}={text!r}: {len(results)} match(es) shown")

    # ---- csv import / export ----------------------------------------------

    def action_export_csv(self):
        if self.db is None or not self.current_table or not self.current_rows:
            QMessageBox.information(self, "Export CSV", "Nothing to export.")
            return
        path, _ = QFileDialog.getSaveFileName(
            self, "Export CSV", f"{self.current_table}.csv", "CSV (*.csv)")
        if not path:
            return
        try:
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(self.current_columns)
                for row in self.current_rows:
                    by = {rd.column_name: rd.value for rd in row}
                    w.writerow([value_to_display(by.get(c, "")) for c in self.current_columns])
            self.status.showMessage(f"exported {len(self.current_rows)} rows -> {path}")
        except Exception as e:
            QMessageBox.critical(self, "Export failed", str(e))

    def action_import_csv(self):
        if self.db is None or not self.current_table:
            return
        schema = self.schemas.get(self.current_table)
        if not schema:
            QMessageBox.warning(self, "No schema",
                "Need schema to import (open a non-empty table or use Create Table first).")
            return
        path, _ = QFileDialog.getOpenFileName(self, "Import CSV", "", "CSV (*.csv)")
        if not path:
            return
        try:
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    QMessageBox.warning(self, "Empty", "CSV has no header.")
                    return
                tmpl_by_col = dict(schema)
                missing = [c for c in header if c not in tmpl_by_col]
                if missing:
                    QMessageBox.warning(self, "Unknown columns",
                        f"CSV has columns not in table: {missing}")
                    return
                rows_to_write = []
                for line_no, raw in enumerate(reader, start=2):
                    if not raw:
                        continue
                    row_data = []
                    for col, cell in zip(header, raw):
                        tmpl = tmpl_by_col[col]
                        try:
                            val = parse_like(tmpl, cell)
                        except Exception as e:
                            QMessageBox.critical(self, "Import failed",
                                f"Line {line_no}, column {col!r}: {e}")
                            return
                        row_data.append(HyperDB.RowData(col, val))
                    rows_to_write.append(row_data)
        except Exception as e:
            QMessageBox.critical(self, "Import failed", str(e))
            return

        if not rows_to_write:
            QMessageBox.information(self, "Import CSV", "No rows in CSV.")
            return
        if QMessageBox.question(self, "Import CSV",
                f"Append {len(rows_to_write)} row(s) to '{self.current_table}'?") != QMessageBox.Yes:
            return
        try:
            for r in rows_to_write:
                self.db.queue_write(self.current_table, r)
            self.db.wait_for_queue()
        except Exception as e:
            QMessageBox.critical(self, "Import failed", str(e))
            return
        self._load_current_table()
        self.status.showMessage(f"imported {len(rows_to_write)} rows from {os.path.basename(path)}")

    # ---- editing / context menus ------------------------------------------

    def _replace_row(self, old_row, new_row):
        """Replace an existing row: delete-by-key-col then write. Returns True on success."""
        if not old_row:
            return False
        key_col = old_row[0].column_name
        key_val = old_row[0].value
        try:
            self.db.queue_delete(self.current_table, key_col, key_val)
            self.db.queue_write(self.current_table, new_row)
            self.db.wait_for_queue()
        except Exception as e:
            QMessageBox.critical(self, "Edit failed", str(e))
            return False
        return True

    def _on_cell_edited(self, item):
        if self._suppress_item_changed or self.db is None or not self.current_table:
            return
        row_idx = item.row()
        col_idx = item.column()
        col_name = self.current_columns[col_idx]
        # the underlying row is stashed on column-0's item
        anchor = self.table.item(row_idx, 0)
        if anchor is None:
            return
        old_row = anchor.data(Qt.UserRole)
        if old_row is None:
            return
        # find template type for parsing
        tmpl = None
        for rd in old_row:
            if rd.column_name == col_name:
                tmpl = rd.value
                break
        try:
            new_val = parse_like(tmpl, item.text()) if tmpl is not None else item.text()
        except Exception as e:
            QMessageBox.warning(self, "Bad value",
                f"Could not parse {item.text()!r} as {type(tmpl).__name__}: {e}")
            self._load_current_table()
            return
        # build the new row, swapping the edited column
        new_row = []
        for rd in old_row:
            if rd.column_name == col_name:
                new_row.append(HyperDB.RowData(col_name, new_val))
            else:
                new_row.append(HyperDB.RowData(rd.column_name, rd.value))
        if self._replace_row(old_row, new_row):
            self._load_current_table()

    def _sidebar_menu(self, pos: QPoint):
        if self.db is None:
            return
        item = self.table_list.itemAt(pos)
        if item is None:
            return
        name = item.text()
        m = QMenu(self)
        a_open  = m.addAction("Open")
        m.addSeparator()
        a_add   = m.addAction("Add Row…")
        a_clear = m.addAction("Clear Table")
        a_drop  = m.addAction("Drop Table")
        chosen = m.exec(self.table_list.viewport().mapToGlobal(pos))
        if chosen == a_open:
            self.current_table = name
            self._load_current_table()
        elif chosen == a_add:
            self.current_table = name
            self.table_list.setCurrentItem(item)
            self.action_add_row()
        elif chosen == a_clear:
            if QMessageBox.question(self, "Clear table",
                    f"Clear all rows from '{name}'? Columns stay intact.") != QMessageBox.Yes:
                return
            try:
                self.db.queue_clear_table(name)
                self.db.wait_for_queue()
            except Exception as e:
                QMessageBox.critical(self, "Clear failed", str(e))
                return
            if name == self.current_table:
                self._load_current_table()
        elif chosen == a_drop:
            if QMessageBox.question(self, "Drop table",
                    f"DROP table '{name}'? This deletes the table and all its data.") != QMessageBox.Yes:
                return
            try:
                self.db.queue_drop_table(name)
                self.db.wait_for_queue()
            except Exception as e:
                QMessageBox.critical(self, "Drop failed", str(e))
                return
            self._refresh_tables_from_db()
            self._load_current_table()
            self._update_status()

    def _header_menu(self, pos: QPoint):
        if self.db is None or not self.current_table:
            return
        idx = self.table.horizontalHeader().logicalIndexAt(pos)
        if idx < 0 or idx >= len(self.current_columns):
            return
        col_name = self.current_columns[idx]
        m = QMenu(self)
        a_clear = m.addAction(f"Clear Column '{col_name}'")
        a_resize = m.addAction("Auto-fit Columns")
        chosen = m.exec(self.table.horizontalHeader().mapToGlobal(pos))
        if chosen == a_clear:
            if QMessageBox.question(self, "Clear column",
                    f"Reset every value in '{col_name}' to its default?") != QMessageBox.Yes:
                return
            try:
                self.db.queue_clear_column(self.current_table, col_name)
                self.db.wait_for_queue()
            except Exception as e:
                QMessageBox.critical(self, "Clear failed", str(e))
                return
            self._load_current_table()
        elif chosen == a_resize:
            self.table.resizeColumnsToContents()

    def _row_menu(self, pos: QPoint):
        if self.db is None or not self.current_table:
            return
        m = QMenu(self)
        a_edit = m.addAction("Edit Row…")
        a_dup  = m.addAction("Duplicate Row")
        a_del  = m.addAction("Delete Row(s)")
        m.addSeparator()
        a_add  = m.addAction("Add Row…")
        chosen = m.exec(self.table.viewport().mapToGlobal(pos))
        if chosen == a_add:
            self.action_add_row()
            return
        rows = sorted({i.row() for i in self.table.selectedIndexes()})
        if not rows:
            return
        if chosen == a_del:
            self.action_delete_selected()
        elif chosen == a_edit:
            r = rows[0]
            anchor = self.table.item(r, 0)
            if anchor is None:
                return
            old_row = anchor.data(Qt.UserRole)
            if old_row is None:
                return
            dlg = AddRowDialog(self.current_columns, old_row, self)
            dlg.setWindowTitle("Edit Row")
            # prefill text values from the existing row
            for col, (le, _tmpl) in dlg.edits.items():
                for rd in old_row:
                    if rd.column_name == col:
                        le.setText(value_to_display(rd.value))
                        break
            if dlg.exec() != QDialog.Accepted:
                return
            if self._replace_row(old_row, dlg.row_data()):
                self._load_current_table()
        elif chosen == a_dup:
            wrote = False
            for r in rows:
                anchor = self.table.item(r, 0)
                if anchor is None:
                    continue
                old_row = anchor.data(Qt.UserRole)
                if old_row is None:
                    continue
                try:
                    self.db.queue_write(self.current_table,
                        [HyperDB.RowData(rd.column_name, rd.value) for rd in old_row])
                    wrote = True
                except Exception as e:
                    QMessageBox.critical(self, "Duplicate failed", str(e))
                    break
            if wrote:
                self.db.wait_for_queue()
                self._load_current_table()

    # ---- table loading -----------------------------------------------------

    def _on_table_changed(self):
        items = self.table_list.selectedItems()
        if not items:
            return
        self.current_table = items[0].text()
        self.page_offset = 0
        self._load_current_table()

    def _load_current_table(self):
        if self.db is None or not self.current_table:
            return
        try:
            count = self.db.get_row_count(self.current_table)
        except Exception as e:
            QMessageBox.critical(self, "Read failed", str(e))
            return

        self.total_rows = count
        if self.page_offset >= count:
            self.page_offset = max(0, ((count - 1) // self.page_size) * self.page_size) if count else 0
        start = self.page_offset
        end = min(start + self.page_size, count)

        collected = []
        def cb(res):
            if res:
                collected.append(list(res))

        for i in range(start, end):
            self.db.queue_read(self.current_table, i, cb)
        try:
            self.db.wait_for_queue()
        except Exception as e:
            QMessageBox.critical(self, "Read failed", str(e))
            return

        self.current_rows = collected
        if collected:
            self.current_columns = [rd.column_name for rd in collected[0]]
        elif self.current_table in self.schemas:
            self.current_columns = [c for c, _ in self.schemas[self.current_table]]
        else:
            self.current_columns = []
        self._render_rows(collected)
        self._refresh_filter_columns()
        self._update_page_bar()
        self._update_status()

    def _update_page_bar(self):
        if self.total_rows <= 0:
            self.page_label.setText(f"{self.current_table or 'no table'}  ·  0 rows")
        else:
            start = self.page_offset + 1
            end = min(self.page_offset + self.page_size, self.total_rows)
            page_no = self.page_offset // self.page_size + 1
            total_pages = max(1, (self.total_rows + self.page_size - 1) // self.page_size)
            self.page_label.setText(
                f"{self.current_table}  ·  rows {start:,}–{end:,} of {self.total_rows:,}  ·  page {page_no}/{total_pages}")
        at_start = self.page_offset <= 0
        at_end = self.page_offset + self.page_size >= self.total_rows
        self.btn_first.setEnabled(not at_start)
        self.btn_prev.setEnabled(not at_start)
        self.btn_next.setEnabled(not at_end)
        self.btn_last.setEnabled(not at_end)

    def _goto_page(self, offset):
        if self.db is None or not self.current_table:
            return
        self.page_offset = max(0, min(offset, max(0, self.total_rows - 1)))
        self._load_current_table()

    def _goto_last_page(self):
        if self.total_rows <= 0:
            return
        last = ((self.total_rows - 1) // self.page_size) * self.page_size
        self._goto_page(last)

    def _page_prev(self):
        self._goto_page(self.page_offset - self.page_size)

    def _page_next(self):
        self._goto_page(self.page_offset + self.page_size)

    def _on_page_size_changed(self):
        new_size = self.page_size_spin.value()
        if new_size == self.page_size:
            return
        self.page_size = new_size
        # keep current first-row visible roughly
        self._load_current_table()

    def _refresh_filter_columns(self):
        self.filter_col.blockSignals(True)
        self.filter_col.clear()
        self.filter_col.addItem("<any column>")
        for c in self.current_columns:
            self.filter_col.addItem(c)
        self.filter_col.blockSignals(False)

    def _render_rows(self, rows):
        self._suppress_item_changed = True
        self.table.clear()
        try:
            self._render_rows_impl(rows)
        finally:
            self._suppress_item_changed = False

    def _render_rows_impl(self, rows):
        was_sorting = self.table.isSortingEnabled()
        self.table.setSortingEnabled(False)
        try:
            if not rows:
                self.table.setRowCount(0)
                if self.current_columns:
                    self.table.setColumnCount(len(self.current_columns))
                    self.table.setHorizontalHeaderLabels(self.current_columns)
                else:
                    self.table.setColumnCount(0)
                return
            cols = [rd.column_name for rd in rows[0]]
            self.table.setColumnCount(len(cols))
            self.table.setHorizontalHeaderLabels(cols)
            self.table.setRowCount(len(rows))
            for r, row in enumerate(rows):
                by_name = {rd.column_name: rd for rd in row}
                for c, col in enumerate(cols):
                    rd = by_name.get(col)
                    val = rd.value if rd is not None else None
                    disp = value_to_display(val) if rd is not None else ""
                    item = SmartTableItem(disp)
                    item.setToolTip(disp)
                    # numeric / bool sort key — bytes/string fall back to text
                    if isinstance(val, (int, float, bool)) and not isinstance(val, bool):
                        item.setData(SORT_KEY_ROLE, val)
                    elif isinstance(val, bool):
                        item.setData(SORT_KEY_ROLE, int(val))
                    if c == 0:
                        item.setData(Qt.UserRole, row)
                    self.table.setItem(r, c, item)
            self.table.resizeColumnsToContents()
            self._apply_filter()
        finally:
            self.table.setSortingEnabled(was_sorting)

    def _apply_filter(self):
        mask = self.filter_edit.text().strip().lower()
        col_idx = self.filter_col.currentIndex() - 1  # -1 = any
        for r in range(self.table.rowCount()):
            if not mask:
                self.table.setRowHidden(r, False)
                continue
            if col_idx < 0:
                hit = False
                for c in range(self.table.columnCount()):
                    it = self.table.item(r, c)
                    if it and mask in it.text().lower():
                        hit = True
                        break
            else:
                it = self.table.item(r, col_idx)
                hit = bool(it and mask in it.text().lower())
            self.table.setRowHidden(r, not hit)


# ----------------------------------------------------------------------------
# dark theme
# ----------------------------------------------------------------------------

def apply_dark_theme(app):
    app.setStyle("Fusion")
    pal = QPalette()
    # palette inspired by a soft, modern "graphite + indigo" dark look
    bg        = QColor("#15171c")   # window
    surface   = QColor("#1c1f26")   # cards / inputs
    surface_2 = QColor("#232730")   # raised / hover
    border    = QColor("#2a2f3a")
    text      = QColor("#e6e8ef")
    text_dim  = QColor("#9aa0ad")
    disabled  = QColor("#5a606e")
    accent    = QColor("#7c8cff")   # indigo
    accent_h  = QColor("#9aa6ff")

    pal.setColor(QPalette.Window, bg)
    pal.setColor(QPalette.WindowText, text)
    pal.setColor(QPalette.Base, surface)
    pal.setColor(QPalette.AlternateBase, surface_2)
    pal.setColor(QPalette.ToolTipBase, surface_2)
    pal.setColor(QPalette.ToolTipText, text)
    pal.setColor(QPalette.Text, text)
    pal.setColor(QPalette.Disabled, QPalette.Text, disabled)
    pal.setColor(QPalette.Button, surface)
    pal.setColor(QPalette.ButtonText, text)
    pal.setColor(QPalette.Disabled, QPalette.ButtonText, disabled)
    pal.setColor(QPalette.BrightText, QColor("#ff6b6b"))
    pal.setColor(QPalette.Link, accent)
    pal.setColor(QPalette.Highlight, accent)
    pal.setColor(QPalette.HighlightedText, QColor("#ffffff"))
    pal.setColor(QPalette.PlaceholderText, text_dim)
    app.setPalette(pal)

    f = QFont("Segoe UI", 10)
    app.setFont(f)

    app.setStyleSheet("""
        * { outline: 0; }
        QMainWindow, QWidget {
            background: #15171c;
            color: #e6e8ef;
            font-family: 'Segoe UI', 'Inter', sans-serif;
            font-size: 10pt;
        }

        /* toolbar */
        QToolBar#MainToolbar {
            background: #15171c;
            border: 0;
            border-bottom: 1px solid #2a2f3a;
            padding: 10px 14px;
            spacing: 4px;
        }
        QToolBar#MainToolbar::separator {
            background: #2a2f3a;
            width: 1px;
            margin: 6px 8px;
        }
        QToolBar#MainToolbar QToolButton {
            background: #1c1f26;
            color: #e6e8ef;
            min-height: 20px;
            max-height: 20px;
            padding: 4px 14px;
            margin: 0;
            border: 1px solid #2a2f3a;
            border-radius: 8px;
            font-size: 9.5pt;
        }
        QToolBar#MainToolbar QToolButton:hover {
            background: #232730;
            border-color: #3a4154;
        }
        QToolBar#MainToolbar QToolButton:pressed,
        QToolBar#MainToolbar QToolButton:checked {
            background: #7c8cff;
            color: #ffffff;
            border-color: #7c8cff;
        }

        /* status bar */
        QStatusBar {
            background: #15171c;
            color: #9aa0ad;
            border-top: 1px solid #2a2f3a;
            padding: 4px 10px;
        }
        QStatusBar::item { border: 0; }

        /* labels */
        QLabel { color: #e6e8ef; background: transparent; }

        /* table */
        QTableWidget, QTableView {
            background: #1c1f26;
            alternate-background-color: #1f2330;
            gridline-color: #232730;
            selection-background-color: #7c8cff;
            selection-color: #ffffff;
            border: 1px solid #2a2f3a;
            border-radius: 12px;
        }
        QTableWidget::item, QTableView::item {
            padding: 6px 8px;
            border: 0;
        }
        QHeaderView {
            background: transparent;
            border: 0;
        }
        QHeaderView::section:horizontal {
            background: #232730;
            color: #c9cdd8;
            padding: 8px 10px;
            border: 0;
            border-right: 1px solid #2a2f3a;
            font-weight: 600;
        }
        QHeaderView::section:horizontal:last {
            border-top-right-radius: 12px;
            border-right: 0;
        }
        QHeaderView::section:vertical {
            background: #232730;
            color: #6b7280;
            padding: 4px 8px;
            border: 0;
            border-bottom: 1px solid #2a2f3a;
        }
        QHeaderView::section:vertical:last {
            border-bottom-left-radius: 12px;
            border-bottom: 0;
        }
        QTableCornerButton::section {
            background: #232730;
            border: 0;
            border-top-left-radius: 12px;
        }

        /* list (sidebar) */
        QListWidget {
            background: #1c1f26;
            border: 1px solid #2a2f3a;
            border-radius: 12px;
            padding: 6px;
        }
        QListWidget::item {
            padding: 8px 12px;
            border-radius: 8px;
            margin: 2px 0;
            color: #c9cdd8;
        }
        QListWidget::item:hover {
            background: #232730;
            color: #e6e8ef;
        }
        QListWidget::item:selected {
            background: #7c8cff;
            color: #ffffff;
        }

        /* inputs */
        QLineEdit, QPlainTextEdit, QTextEdit {
            background: #1c1f26;
            color: #e6e8ef;
            border: 1px solid #2a2f3a;
            border-radius: 10px;
            padding: 7px 10px;
            selection-background-color: #7c8cff;
            selection-color: #ffffff;
        }
        QLineEdit:hover, QPlainTextEdit:hover, QTextEdit:hover { border-color: #3a4154; }
        QLineEdit:focus, QPlainTextEdit:focus, QTextEdit:focus { border-color: #7c8cff; }
        QLineEdit:disabled { color: #5a606e; }

        /* combo box */
        QComboBox {
            background: #1c1f26;
            color: #e6e8ef;
            border: 1px solid #2a2f3a;
            border-radius: 10px;
            padding: 6px 10px;
            min-height: 18px;
        }
        QComboBox:hover { border-color: #3a4154; }
        QComboBox:focus { border-color: #7c8cff; }
        QComboBox::drop-down {
            border: 0;
            width: 22px;
        }
        QComboBox::down-arrow {
            image: none;
            border-left: 4px solid transparent;
            border-right: 4px solid transparent;
            border-top: 5px solid #9aa0ad;
            margin-right: 8px;
        }
        QComboBox QAbstractItemView {
            background: #1c1f26;
            color: #e6e8ef;
            border: 1px solid #2a2f3a;
            border-radius: 10px;
            padding: 4px;
            selection-background-color: #7c8cff;
            selection-color: #ffffff;
            outline: 0;
        }

        /* buttons */
        QPushButton {
            background: #1c1f26;
            color: #e6e8ef;
            border: 1px solid #2a2f3a;
            border-radius: 10px;
            padding: 7px 16px;
            min-height: 18px;
        }
        QPushButton:hover {
            background: #232730;
            border-color: #3a4154;
        }
        QPushButton:pressed {
            background: #6b7aff;
            color: #ffffff;
            border-color: #6b7aff;
        }
        QPushButton:default {
            background: #7c8cff;
            color: #ffffff;
            border-color: #7c8cff;
        }
        QPushButton:default:hover {
            background: #9aa6ff;
            border-color: #9aa6ff;
        }

        /* checkbox */
        QCheckBox { spacing: 8px; color: #e6e8ef; background: transparent; }
        QCheckBox::indicator {
            width: 16px; height: 16px;
            border-radius: 5px;
            border: 1px solid #3a4154;
            background: #1c1f26;
        }
        QCheckBox::indicator:hover { border-color: #7c8cff; }
        QCheckBox::indicator:checked {
            background: #7c8cff;
            border-color: #7c8cff;
        }

        /* splitter */
        QSplitter::handle {
            background: transparent;
        }
        QSplitter::handle:horizontal { width: 8px; }
        QSplitter::handle:vertical   { height: 8px; }

        /* scrollbars */
        QScrollBar:vertical {
            background: transparent;
            width: 10px;
            margin: 4px 2px 4px 2px;
        }
        QScrollBar::handle:vertical {
            background: #2a2f3a;
            border-radius: 4px;
            min-height: 24px;
        }
        QScrollBar::handle:vertical:hover { background: #3a4154; }
        QScrollBar:horizontal {
            background: transparent;
            height: 10px;
            margin: 2px 4px 2px 4px;
        }
        QScrollBar::handle:horizontal {
            background: #2a2f3a;
            border-radius: 4px;
            min-width: 24px;
        }
        QScrollBar::handle:horizontal:hover { background: #3a4154; }
        QScrollBar::add-line, QScrollBar::sub-line { background: transparent; border: 0; height: 0; width: 0; }
        QScrollBar::add-page, QScrollBar::sub-page { background: transparent; }

        /* dialogs */
        QDialog, QMessageBox { background: #15171c; }

        /* tooltips */
        QToolTip {
            background: #232730;
            color: #e6e8ef;
            border: 1px solid #2a2f3a;
            padding: 6px 8px;
            border-radius: 6px;
        }

        /* scroll area frames */
        QScrollArea { background: transparent; border: 0; }
        QFrame { background: transparent; }
    """)


def main():
    if "-cli" in sys.argv or "--cli" in sys.argv:
        import cli
        sys.exit(cli.run())
    app = QApplication(sys.argv)
    apply_dark_theme(app)
    win = Browser()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
