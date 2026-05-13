"""HyperDB terminal UI. Run with `python browser.py -cli`.

Renders into an off-screen pad and swaps the visible window with one
doupdate() per frame so the screen never tears.
"""
import curses
import os
import csv
import json
import HyperDB

from browser import (
    ManagerAdapter, ClusterAdapter, DEFAULT_ITERATIONS,
    COLUMN_TYPES, TEMPLATE_BY_ENUM, TYPE_BY_NAME,
    parse_like, value_to_display, safe_iter,
)

# ---------------------------------------------------------------------------
# styling
# ---------------------------------------------------------------------------

# color pair indices
CP_BASE       = 1   # body
CP_HEADER     = 2   # title bar
CP_FOOTER     = 3   # help bar
CP_ACCENT     = 4   # selected item / focus highlight
CP_DIM        = 5   # secondary text
CP_DANGER     = 6   # errors
CP_OK         = 7   # status: success
CP_PANEL_HDR  = 8   # pane header (TABLES / rows / ...)

# unicode box drawing
BOX_H, BOX_V = "─", "│"
BOX_TL, BOX_TR, BOX_BL, BOX_BR = "╭", "╮", "╰", "╯"


def init_colors():
    curses.start_color()
    curses.use_default_colors()
    # base text
    curses.init_pair(CP_BASE,      curses.COLOR_WHITE,  -1)
    curses.init_pair(CP_HEADER,    curses.COLOR_BLACK,  curses.COLOR_CYAN)
    curses.init_pair(CP_FOOTER,    curses.COLOR_BLACK,  curses.COLOR_WHITE)
    curses.init_pair(CP_ACCENT,    curses.COLOR_BLACK,  curses.COLOR_MAGENTA)
    curses.init_pair(CP_DIM,       curses.COLOR_WHITE,  -1)  # use A_DIM with this
    curses.init_pair(CP_DANGER,    curses.COLOR_WHITE,  curses.COLOR_RED)
    curses.init_pair(CP_OK,        curses.COLOR_BLACK,  curses.COLOR_GREEN)
    curses.init_pair(CP_PANEL_HDR, curses.COLOR_CYAN,   -1)


def addstr_clip(win, y, x, s, attr=0, max_w=None):
    """Write s into win without wrapping or running off the right edge."""
    h, w = win.getmaxyx()
    if y < 0 or y >= h or x >= w:
        return
    if x < 0:
        s = s[-x:]
        x = 0
    avail = w - x
    if max_w is not None:
        avail = min(avail, max_w)
    if avail <= 0:
        return
    s = s.replace("\t", "    ")
    s = "".join(c if (c >= " " or c == " ") else "?" for c in s)
    if len(s) > avail:
        s = s[:avail - 1] + "…" if avail > 1 else s[:avail]
    try:
        win.addstr(y, x, s, attr)
    except curses.error:
        # writing the very last cell raises ERR even on success
        pass


def hline(win, y, x, w, ch=BOX_H, attr=0):
    addstr_clip(win, y, x, ch * w, attr)


def box(win, y, x, h, w, attr=0, title=None):
    if h < 2 or w < 2:
        return
    addstr_clip(win, y, x, BOX_TL + BOX_H * (w - 2) + BOX_TR, attr)
    addstr_clip(win, y + h - 1, x, BOX_BL + BOX_H * (w - 2) + BOX_BR, attr)
    for yy in range(y + 1, y + h - 1):
        addstr_clip(win, yy, x, BOX_V, attr)
        addstr_clip(win, yy, x + w - 1, BOX_V, attr)
    if title:
        t = f" {title} "
        addstr_clip(win, y, x + 2, t, attr | curses.A_BOLD, max_w=w - 4)


def pad_or_trim(s, width):
    if len(s) >= width:
        return s[:width - 1] + "…" if width > 1 else s[:width]
    return s + " " * (width - len(s))


# ---------------------------------------------------------------------------
# input helpers (modal popups)
# ---------------------------------------------------------------------------

class Cancelled(Exception):
    pass


def centered_box(stdscr, h, w, title=""):
    sh, sw = stdscr.getmaxyx()
    h = min(h, sh - 2)
    w = min(w, sw - 2)
    y = (sh - h) // 2
    x = (sw - w) // 2
    win = curses.newwin(h, w, y, x)
    win.bkgd(" ", curses.color_pair(CP_BASE))
    box(win, 0, 0, h, w, attr=curses.color_pair(CP_PANEL_HDR), title=title)
    return win


def prompt(stdscr, title, fields, on_change=None):
    """Multi-field prompt. fields = [(label, default, kind)] where kind in
    {'text','password','bool','int','choice:opt1|opt2'}.
    Returns dict[label] -> value, or raises Cancelled on Esc."""
    values = [f[1] for f in fields]
    cur = 0
    h = len(fields) + 6
    w = max(56, max(len(f[0]) for f in fields) + 40)
    while True:
        win = centered_box(stdscr, h, w, title=title)
        addstr_clip(win, h - 2, 2,
            "Tab/↑↓: move   Enter: submit   Esc: cancel",
            curses.color_pair(CP_PANEL_HDR) | curses.A_DIM)
        for i, (label, _default, kind) in enumerate(fields):
            row = 2 + i
            focused = (i == cur)
            attr_lbl = curses.color_pair(CP_PANEL_HDR) | (curses.A_BOLD if focused else 0)
            addstr_clip(win, row, 2, f"{label}:", attr_lbl, max_w=22)
            val = values[i]
            shown = val
            if kind == "password":
                shown = "*" * len(val)
            elif kind == "bool":
                shown = "[x] yes" if val else "[ ] no"
            elif kind.startswith("choice:"):
                shown = f"< {val} >"
            box_w = w - 28
            attr_val = curses.color_pair(CP_ACCENT) if focused else curses.color_pair(CP_BASE) | curses.A_DIM
            addstr_clip(win, row, 24, pad_or_trim(str(shown), box_w), attr_val)
        win.noutrefresh()
        curses.doupdate()

        ch = stdscr.getch()
        if ch in (27,):  # esc
            raise Cancelled()
        if ch in (10, 13, curses.KEY_ENTER):
            # submit
            result = {}
            for (label, _d, kind), v in zip(fields, values):
                result[label] = v
            if on_change is not None:
                on_change(result)
            return result
        if ch in (9, curses.KEY_DOWN):
            cur = (cur + 1) % len(fields)
            continue
        if ch in (curses.KEY_BTAB, curses.KEY_UP):
            cur = (cur - 1) % len(fields)
            continue

        kind = fields[cur][2]
        if kind == "bool":
            if ch in (ord(" "), ord("y"), ord("n"), ord("Y"), ord("N")):
                if ch in (ord("y"), ord("Y")):
                    values[cur] = True
                elif ch in (ord("n"), ord("N")):
                    values[cur] = False
                else:
                    values[cur] = not values[cur]
            continue
        if kind.startswith("choice:"):
            opts = kind.split(":", 1)[1].split("|")
            cur_v = values[cur]
            idx = opts.index(cur_v) if cur_v in opts else 0
            if ch in (curses.KEY_LEFT, curses.KEY_RIGHT, ord(" ")):
                idx = (idx + (1 if ch != curses.KEY_LEFT else -1)) % len(opts)
                values[cur] = opts[idx]
            continue
        # text-like
        if ch in (curses.KEY_BACKSPACE, 127, 8):
            values[cur] = values[cur][:-1]
        elif ch == curses.KEY_DC:
            values[cur] = ""
        elif 32 <= ch < 127:
            values[cur] = values[cur] + chr(ch)
        elif kind == "int" and ch in (ord("+"), ord("-")):
            pass


def message(stdscr, text, level="info"):
    sh, sw = stdscr.getmaxyx()
    lines = text.split("\n")
    w = min(sw - 4, max(40, max(len(l) for l in lines) + 6))
    h = len(lines) + 6
    win = centered_box(stdscr, h, w, title={"info": "Info", "ok": "OK", "error": "Error"}.get(level, "Info"))
    color = {"info": CP_PANEL_HDR, "ok": CP_OK, "error": CP_DANGER}.get(level, CP_PANEL_HDR)
    for i, l in enumerate(lines):
        addstr_clip(win, 2 + i, 3, l, curses.color_pair(color))
    addstr_clip(win, h - 2, 3, "Press any key…", curses.color_pair(CP_PANEL_HDR) | curses.A_DIM)
    win.noutrefresh()
    curses.doupdate()
    stdscr.getch()


def confirm(stdscr, text):
    sh, sw = stdscr.getmaxyx()
    w = min(sw - 4, max(50, len(text) + 6))
    h = 7
    win = centered_box(stdscr, h, w, title="Confirm")
    addstr_clip(win, 2, 3, text, curses.color_pair(CP_BASE))
    addstr_clip(win, h - 2, 3, "[y]es / [n]o", curses.color_pair(CP_PANEL_HDR) | curses.A_DIM)
    win.noutrefresh()
    curses.doupdate()
    while True:
        ch = stdscr.getch()
        if ch in (ord("y"), ord("Y"), 10, 13):
            return True
        if ch in (ord("n"), ord("N"), 27):
            return False


def pick_from_list(stdscr, title, items, current=None):
    if not items:
        message(stdscr, "Nothing to pick.", "info")
        return None
    sh, sw = stdscr.getmaxyx()
    w = min(sw - 4, max(40, max(len(str(i)) for i in items) + 8))
    h = min(sh - 4, len(items) + 6)
    idx = items.index(current) if current in items else 0
    while True:
        win = centered_box(stdscr, h, w, title=title)
        view_h = h - 4
        if idx < 0:
            idx = 0
        if idx >= len(items):
            idx = len(items) - 1
        start = max(0, min(idx - view_h // 2, len(items) - view_h))
        for r in range(view_h):
            i = start + r
            if i >= len(items):
                break
            attr = curses.color_pair(CP_ACCENT) if i == idx else curses.color_pair(CP_BASE)
            addstr_clip(win, 2 + r, 2, pad_or_trim(f"  {items[i]}", w - 4), attr)
        addstr_clip(win, h - 2, 2, "↑↓ select, Enter to choose, Esc to cancel",
            curses.color_pair(CP_PANEL_HDR) | curses.A_DIM)
        win.noutrefresh()
        curses.doupdate()
        ch = stdscr.getch()
        if ch == curses.KEY_UP:
            idx -= 1
        elif ch == curses.KEY_DOWN:
            idx += 1
        elif ch == curses.KEY_PPAGE:
            idx -= view_h
        elif ch == curses.KEY_NPAGE:
            idx += view_h
        elif ch in (10, 13):
            return items[idx]
        elif ch == 27:
            return None


# ---------------------------------------------------------------------------
# main app
# ---------------------------------------------------------------------------

KEY_HINTS = [
    ("O", "Open"), ("N", "New"), ("C", "Close"),
    ("S", "Save"), ("R", "Reload"),
    ("T", "Add Table"), ("A", "Add"), ("E", "Edit"), ("D", "Del"),
    ("F", "Find"),
    (",", "PrevPg"), (".", "NextPg"), ("P", "PgSize"),
    ("x", "Export"), ("I", "Import"),
    ("?", "Help"), ("Q", "Quit"),
]


class CLIApp:
    def __init__(self, stdscr):
        self.stdscr = stdscr
        self.db = None             # adapter
        self.tables = []
        self.schemas = {}
        self.current_table = None
        self.current_rows = []     # list[list[RowData]] — only current page
        self.current_columns = []  # list[str]
        self.row_idx = 0
        self.row_scroll = 0
        self.table_idx = 0
        self.focus = "tables"      # "tables" or "rows"
        self.toast = ""            # transient bottom status
        self.page_size = 1000
        self.page_offset = 0
        self.total_rows = 0

    # ---- adapter operations -----------------------------------------------

    def load_tables(self):
        self.tables = []
        self.schemas = {}
        try:
            for name, schema in self.db.list_tables():
                self.tables.append(name)
                self.schemas[name] = schema
        except Exception as e:
            self.toast = f"schema fetch failed: {e}"

    def load_current_table(self):
        if self.db is None or not self.current_table:
            self.current_rows = []
            self.current_columns = []
            self.total_rows = 0
            return
        try:
            count = self.db.get_row_count(self.current_table)
        except Exception as e:
            self.toast = f"row count failed: {e}"
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
            self.toast = f"read failed: {e}"
            return
        self.current_rows = collected
        if collected:
            self.current_columns = [rd.column_name for rd in collected[0]]
        elif self.current_table in self.schemas:
            self.current_columns = [c for c, _ in self.schemas[self.current_table]]
        else:
            self.current_columns = []
        if self.row_idx >= len(self.current_rows):
            self.row_idx = max(0, len(self.current_rows) - 1)

    def page_prev(self):
        if self.page_offset <= 0:
            return
        self.page_offset = max(0, self.page_offset - self.page_size)
        self.row_idx = 0
        self.row_scroll = 0
        self.load_current_table()

    def page_next(self):
        if self.page_offset + self.page_size >= self.total_rows:
            return
        self.page_offset += self.page_size
        self.row_idx = 0
        self.row_scroll = 0
        self.load_current_table()

    def page_set_size(self):
        try:
            r = prompt(self.stdscr, "Page size", [("Rows per page", str(self.page_size), "int")])
            n = max(10, int(r["Rows per page"]))
        except (Cancelled, ValueError):
            return
        self.page_size = n
        self.page_offset = 0
        self.load_current_table()

    # ---- rendering --------------------------------------------------------

    def render(self):
        s = self.stdscr
        s.erase()
        h, w = s.getmaxyx()

        # title bar
        title = "HyperDB CLI"
        if self.db:
            title += f"  ·  {self.db.kind}: {self.db.label}"
            if self.db.is_encrypted():
                title += "  ·  ENCRYPTED"
            else:
                title += "  ·  PLAIN"
            extra = self.db.extra_status()
            if extra:
                title += "  ·  " + extra
        addstr_clip(s, 0, 0, pad_or_trim(" " + title, w),
            curses.color_pair(CP_HEADER) | curses.A_BOLD)

        if self.db is None:
            self._render_welcome(h, w)
        else:
            self._render_main(h, w)

        # footer help bar
        self._render_footer(h, w)

        # transient toast above footer
        if self.toast:
            addstr_clip(s, h - 2, 1, pad_or_trim("  " + self.toast, w - 2),
                curses.color_pair(CP_DIM) | curses.A_DIM)

        s.noutrefresh()
        curses.doupdate()

    def _render_welcome(self, h, w):
        s = self.stdscr
        lines = [
            "",
            "  HyperDB Terminal Browser",
            "",
            "  Inspect, edit, and manage HyperDB files and clusters.",
            "",
            "",
            "    [O] Open Database",
            "    [N] New Database",
            "    [Q] Quit",
            "",
        ]
        bw = min(w - 6, 60)
        bh = len(lines) + 2
        by = (h - bh) // 2
        bx = (w - bw) // 2
        box(s, by, bx, bh, bw,
            attr=curses.color_pair(CP_PANEL_HDR), title="Welcome")
        for i, line in enumerate(lines):
            attr = curses.color_pair(CP_BASE)
            if line.strip().startswith("HyperDB"):
                attr |= curses.A_BOLD
            elif line.strip().startswith("[") and "]" in line:
                attr = curses.color_pair(CP_PANEL_HDR) | curses.A_BOLD
            elif line.startswith("  "):
                attr = curses.color_pair(CP_BASE) | curses.A_DIM
            addstr_clip(s, by + 1 + i, bx + 2, pad_or_trim(line, bw - 4), attr)

    def _render_main(self, h, w):
        s = self.stdscr
        # layout
        sidebar_w = max(20, min(28, w // 4))
        body_top = 1
        body_h = h - 3   # leaves room for toast + footer

        # ---- sidebar (tables) ----
        tab_focus = self.focus == "tables"
        side_attr = curses.color_pair(CP_PANEL_HDR if tab_focus else CP_DIM) | (curses.A_BOLD if tab_focus else 0)
        box(s, body_top, 0, body_h, sidebar_w, attr=side_attr, title="Tables")
        for i, name in enumerate(self.tables):
            if i + 2 + body_top >= body_top + body_h - 1:
                break
            selected = (i == self.table_idx)
            attr = curses.color_pair(CP_ACCENT) if (selected and tab_focus) else (
                curses.color_pair(CP_PANEL_HDR) | curses.A_BOLD if selected else curses.color_pair(CP_BASE))
            label = "  " + name
            addstr_clip(s, body_top + 1 + i, 1, pad_or_trim(label, sidebar_w - 2), attr)

        # ---- rows pane ----
        rows_x = sidebar_w + 1
        rows_w = w - rows_x - 1
        rows_focus = self.focus == "rows"
        rows_attr = curses.color_pair(CP_PANEL_HDR if rows_focus else CP_DIM) | (curses.A_BOLD if rows_focus else 0)
        rows_title = self.current_table or "rows"
        if self.current_table:
            if self.total_rows == 0:
                rows_title += "  (empty)"
            else:
                start = self.page_offset + 1
                end = min(self.page_offset + self.page_size, self.total_rows)
                page_no = self.page_offset // self.page_size + 1
                total_pages = max(1, (self.total_rows + self.page_size - 1) // self.page_size)
                rows_title += f"  ({start:,}–{end:,} of {self.total_rows:,}  ·  page {page_no}/{total_pages})"
        box(s, body_top, rows_x, body_h, rows_w, attr=rows_attr, title=rows_title)

        if not self.current_columns:
            addstr_clip(s, body_top + 2, rows_x + 2, "(empty / no schema)",
                curses.color_pair(CP_BASE) | curses.A_DIM)
            return

        col_widths = self._compute_col_widths(rows_w - 6)

        # header row
        header_y = body_top + 1
        x = rows_x + 2
        addstr_clip(s, header_y, x - 1, "#", curses.color_pair(CP_PANEL_HDR) | curses.A_BOLD, max_w=4)
        x += 4
        for col, cw in zip(self.current_columns, col_widths):
            addstr_clip(s, header_y, x, pad_or_trim(col, cw),
                curses.color_pair(CP_PANEL_HDR) | curses.A_BOLD)
            x += cw + 1
        hline(s, header_y + 1, rows_x + 1, rows_w - 2,
            attr=curses.color_pair(CP_PANEL_HDR) | curses.A_DIM)

        # data rows
        view_h = body_h - 4
        if self.row_idx < self.row_scroll:
            self.row_scroll = self.row_idx
        if self.row_idx >= self.row_scroll + view_h:
            self.row_scroll = self.row_idx - view_h + 1

        for vi in range(view_h):
            ri = self.row_scroll + vi
            if ri >= len(self.current_rows):
                break
            row = self.current_rows[ri]
            by_name = {rd.column_name: rd for rd in row}
            selected = (ri == self.row_idx)
            base_attr = curses.color_pair(CP_ACCENT) if (selected and rows_focus) else (
                curses.color_pair(CP_PANEL_HDR) | curses.A_BOLD if selected else curses.color_pair(CP_BASE))
            y = body_top + 3 + vi
            x = rows_x + 1
            addstr_clip(s, y, x, pad_or_trim(f" {ri + 1:>3}", 4), base_attr)
            x = rows_x + 6
            for col, cw in zip(self.current_columns, col_widths):
                v = by_name.get(col)
                disp = value_to_display(v.value) if v is not None else ""
                addstr_clip(s, y, x, pad_or_trim(disp, cw), base_attr)
                x += cw + 1

    def _compute_col_widths(self, total):
        cols = self.current_columns
        if not cols:
            return []
        # start with column-name length
        widths = [max(len(c), 6) for c in cols]
        # widen with sample values
        for row in self.current_rows[:60]:
            by = {rd.column_name: rd for rd in row}
            for i, c in enumerate(cols):
                v = by.get(c)
                if v is None:
                    continue
                disp = value_to_display(v.value)
                widths[i] = max(widths[i], min(len(disp), 30))
        used = sum(widths) + len(widths) - 1
        if used <= total:
            return widths
        # shrink proportionally
        scale = total / used
        scaled = [max(4, int(w * scale)) for w in widths]
        # final tweak
        while sum(scaled) + len(scaled) - 1 > total:
            i = scaled.index(max(scaled))
            scaled[i] -= 1
        return scaled

    def _render_footer(self, h, w):
        s = self.stdscr
        y = h - 1
        # paint background
        addstr_clip(s, y, 0, " " * w, curses.color_pair(CP_FOOTER))
        x = 0
        key_attr  = curses.color_pair(CP_FOOTER) | curses.A_BOLD | curses.A_REVERSE
        lbl_attr  = curses.color_pair(CP_FOOTER)
        sep_attr  = curses.color_pair(CP_FOOTER) | curses.A_DIM
        for i, (k, label) in enumerate(KEY_HINTS):
            if i > 0:
                if x + 3 >= w:
                    break
                addstr_clip(s, y, x, " · ", sep_attr)
                x += 3
            key_text = f" {k} "
            if x + len(key_text) >= w:
                break
            addstr_clip(s, y, x, key_text, key_attr)
            x += len(key_text)
            lbl_text = " " + label
            if x + len(lbl_text) >= w:
                break
            addstr_clip(s, y, x, lbl_text, lbl_attr)
            x += len(lbl_text)

    # ---- actions ----------------------------------------------------------

    def action_open(self, intent="open"):
        try:
            mode_default = "manager"
            fields = [
                ("Mode", mode_default, "choice:manager|cluster"),
            ]
            # show a tiny mode picker first; then a tailored form
            r = prompt(self.stdscr, "New Database" if intent == "new" else "Open Database", fields)
            mode = r["Mode"]
            if mode == "manager":
                form = [
                    ("Path", "new.db" if intent == "new" else "example.db", "text"),
                    ("Password", "", "password"),
                    ("Encrypted", True, "bool"),
                    ("Iterations", str(DEFAULT_ITERATIONS), "int"),
                ]
                r = prompt(self.stdscr,
                    "New Database" if intent == "new" else "Open Database", form)
                path = r["Path"].strip()
                pw   = r["Password"]
                enc  = bool(r["Encrypted"])
                try:
                    iters = max(1, int(r["Iterations"]))
                except ValueError:
                    iters = DEFAULT_ITERATIONS
                mgr = HyperDB.HyperDBManager()
                mgr.open_db(path, pw, enc)
                self.db = ManagerAdapter(mgr, path, iters)
            else:
                if intent == "open":
                    form = [
                        ("Manifest", "", "text"),
                        ("Password", "", "password"),
                        ("Iterations", str(DEFAULT_ITERATIONS), "int"),
                    ]
                    r = prompt(self.stdscr, "Open Cluster", form)
                    manifest_path = r["Manifest"].strip()
                    with open(manifest_path, "r", encoding="utf-8") as f:
                        m = json.load(f)
                    folder = os.path.dirname(os.path.abspath(manifest_path)) or "."
                    base = os.path.basename(manifest_path)
                    name = base[:-len(".manifest")] if base.lower().endswith(".manifest") else os.path.splitext(base)[0]
                    shard_limit = int(m.get("shard_limit", 512 * 1024 * 1024))
                    enc = bool(m.get("should_encrypt", True))
                    pw  = r["Password"]
                    try:
                        iters = max(1, int(r["Iterations"]))
                    except ValueError:
                        iters = DEFAULT_ITERATIONS
                else:
                    form = [
                        ("Folder", "new_cluster", "text"),
                        ("Cluster name", "main", "text"),
                        ("Shard limit (MB)", "512", "int"),
                        ("Password", "", "password"),
                        ("Encrypted", True, "bool"),
                        ("Iterations", str(DEFAULT_ITERATIONS), "int"),
                    ]
                    r = prompt(self.stdscr, "New Cluster", form)
                    folder = r["Folder"].strip()
                    name = r["Cluster name"].strip()
                    try:
                        shard_limit = max(1, int(r["Shard limit (MB)"])) * 1024 * 1024
                    except ValueError:
                        shard_limit = 512 * 1024 * 1024
                    pw  = r["Password"]
                    enc = bool(r["Encrypted"])
                    try:
                        iters = max(1, int(r["Iterations"]))
                    except ValueError:
                        iters = DEFAULT_ITERATIONS
                    if folder and not os.path.exists(folder):
                        os.makedirs(folder, exist_ok=True)
                cl = HyperDB.HyperDBCluster()
                cl.open(folder, name, pw, shard_limit, enc)
                self.db = ClusterAdapter(cl, folder, name, iters)
        except Cancelled:
            return
        except Exception as e:
            message(self.stdscr, f"open failed: {e}", "error")
            return
        self.load_tables()
        if self.tables:
            self.table_idx = 0
            self.current_table = self.tables[0]
            self.load_current_table()
        self.toast = "opened"

    def action_close(self):
        if self.db is None:
            return
        if self.db.is_dirty():
            if confirm(self.stdscr, "Unsaved changes. Force-save before close?"):
                try:
                    self.db.force_flush()
                except Exception as e:
                    message(self.stdscr, str(e), "error")
                    return
        self.db = None
        self.tables = []
        self.schemas = {}
        self.current_table = None
        self.current_rows = []
        self.current_columns = []
        self.row_idx = self.row_scroll = self.table_idx = 0
        self.toast = "closed"

    def action_save(self):
        if not self.db:
            return
        try:
            self.db.force_flush()
            self.toast = "saved"
        except Exception as e:
            message(self.stdscr, str(e), "error")

    def action_reload(self):
        if not self.db:
            return
        self.load_tables()
        # keep selection if still present
        if self.current_table not in self.tables:
            self.current_table = self.tables[0] if self.tables else None
            self.table_idx = 0
        self.load_current_table()
        self.toast = "reloaded"

    def action_add_table(self):
        if not self.db:
            return
        try:
            r = prompt(self.stdscr, "Add Table — basics", [
                ("Table name", "", "text"),
                ("Column count", "3", "int"),
            ])
            name = r["Table name"].strip()
            try:
                n = max(1, int(r["Column count"]))
            except ValueError:
                n = 1
            if not name:
                return
            type_choices = "|".join(tn for tn, _, _ in COLUMN_TYPES)
            fields = []
            for i in range(n):
                fields.append((f"Col {i+1} name", f"col{i+1}", "text"))
                fields.append((f"Col {i+1} type", "Int32", f"choice:{type_choices}"))
            r2 = prompt(self.stdscr, f"Add Table — '{name}' columns", fields)
            cols = []
            for i in range(n):
                cn = r2[f"Col {i+1} name"].strip()
                tn = r2[f"Col {i+1} type"]
                enum_val, _tmpl = TYPE_BY_NAME[tn]
                cols.append(HyperDB.ColumnDef(cn, enum_val))
            self.db.queue_create_table(name, cols)
            self.db.wait_for_queue()
        except Cancelled:
            return
        except Exception as e:
            message(self.stdscr, f"create failed: {e}", "error")
            return
        self.load_tables()
        if name in self.tables:
            self.table_idx = self.tables.index(name)
            self.current_table = name
            self.load_current_table()
        self.toast = f"created {name}"

    def _build_row_form_fields(self, schema, prefill=None):
        """schema: list[(col_name, template)]; prefill: list[RowData] or None."""
        prefill_by = {rd.column_name: rd.value for rd in prefill} if prefill else {}
        fields = []
        for col, tmpl in schema:
            val = prefill_by.get(col, tmpl)
            if isinstance(tmpl, bool):
                fields.append((col, bool(val), "bool"))
            elif isinstance(tmpl, int):
                fields.append((col, str(val), "int"))
            elif isinstance(tmpl, float):
                fields.append((col, str(val), "text"))
            elif isinstance(tmpl, (bytes, bytearray)):
                hexed = val.hex() if isinstance(val, (bytes, bytearray)) else (str(val) if val else "")
                fields.append((f"{col} (hex)", hexed, "text"))
            else:
                fields.append((col, str(val) if val is not None else "", "text"))
        return fields

    def _row_from_form(self, schema, form_result):
        out = []
        for col, tmpl in schema:
            key = f"{col} (hex)" if isinstance(tmpl, (bytes, bytearray)) else col
            raw = form_result[key]
            try:
                if isinstance(tmpl, bool):
                    val = bool(raw)
                elif isinstance(tmpl, (bytes, bytearray)):
                    val = bytes.fromhex(raw.strip()) if raw.strip() else b""
                else:
                    val = parse_like(tmpl, raw)
            except Exception as e:
                raise ValueError(f"column {col!r}: {e}")
            out.append(HyperDB.RowData(col, val))
        return out

    def action_add_row(self):
        if not self.db or not self.current_table:
            return
        schema = self.schemas.get(self.current_table)
        if not schema:
            message(self.stdscr, "No schema for this table.", "error")
            return
        try:
            fields = self._build_row_form_fields(schema)
            r = prompt(self.stdscr, f"Add Row — {self.current_table}", fields)
            row = self._row_from_form(schema, r)
            self.db.queue_write(self.current_table, row)
            self.db.wait_for_queue()
        except Cancelled:
            return
        except Exception as e:
            message(self.stdscr, str(e), "error")
            return
        self.load_current_table()
        self.toast = "row added"

    def action_edit_row(self):
        if not self.db or not self.current_table or not self.current_rows:
            return
        schema = self.schemas.get(self.current_table)
        if not schema:
            message(self.stdscr, "No schema for this table.", "error")
            return
        old_row = self.current_rows[self.row_idx]
        try:
            fields = self._build_row_form_fields(schema, prefill=old_row)
            r = prompt(self.stdscr, f"Edit Row — {self.current_table}", fields)
            new_row = self._row_from_form(schema, r)
            key_col = old_row[0].column_name
            key_val = old_row[0].value
            self.db.queue_delete(self.current_table, key_col, key_val)
            self.db.queue_write(self.current_table, new_row)
            self.db.wait_for_queue()
        except Cancelled:
            return
        except Exception as e:
            message(self.stdscr, str(e), "error")
            return
        self.load_current_table()
        self.toast = "row updated"

    def action_delete_row(self):
        if not self.db or not self.current_table or not self.current_rows:
            return
        row = self.current_rows[self.row_idx]
        if not row:
            return
        key_col = row[0].column_name
        key_val = row[0].value
        if not confirm(self.stdscr, f"Delete row where {key_col}={key_val!r}?"):
            return
        try:
            self.db.queue_delete(self.current_table, key_col, key_val)
            self.db.wait_for_queue()
        except Exception as e:
            message(self.stdscr, str(e), "error")
            return
        self.load_current_table()
        self.toast = "row deleted"

    def action_drop_table(self):
        if not self.db or not self.current_table:
            return
        if not confirm(self.stdscr, f"Drop table '{self.current_table}'?"):
            return
        try:
            self.db.queue_drop_table(self.current_table)
            self.db.wait_for_queue()
        except Exception as e:
            message(self.stdscr, str(e), "error")
            return
        self.current_table = None
        self.load_tables()
        if self.tables:
            self.table_idx = 0
            self.current_table = self.tables[0]
            self.load_current_table()
        self.toast = "table dropped"

    def action_find(self):
        if not self.db or not self.current_table or not self.current_columns:
            return
        col = pick_from_list(self.stdscr, "Find — column", self.current_columns)
        if col is None:
            return
        schema = dict(self.schemas.get(self.current_table, []))
        tmpl = schema.get(col, "")
        try:
            r = prompt(self.stdscr, f"Find — {col}", [
                ("Value", "", "text"),
            ])
            text = r["Value"]
            try:
                val = parse_like(tmpl, text) if tmpl != "" or not isinstance(tmpl, str) else text
            except Exception as e:
                message(self.stdscr, f"bad value: {e}", "error")
                return
            results = []
            def cb(res):
                results.extend(res)
            self.db.queue_find(self.current_table, col, val, cb)
            self.db.wait_for_queue()
        except Cancelled:
            return
        except Exception as e:
            message(self.stdscr, str(e), "error")
            return
        if not results:
            message(self.stdscr, "no matches", "info")
            return
        self.current_rows = [list(r) for r in results]
        self.row_idx = 0
        self.row_scroll = 0
        self.toast = f"find: {len(results)} match(es)"

    def action_export_csv(self):
        if not self.db or not self.current_table or not self.current_rows:
            return
        try:
            r = prompt(self.stdscr, "Export CSV", [
                ("Path", f"{self.current_table}.csv", "text"),
            ])
            path = r["Path"].strip()
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(self.current_columns)
                for row in self.current_rows:
                    by = {rd.column_name: rd.value for rd in row}
                    w.writerow([value_to_display(by.get(c, "")) for c in self.current_columns])
        except Cancelled:
            return
        except Exception as e:
            message(self.stdscr, str(e), "error")
            return
        self.toast = f"exported {len(self.current_rows)} rows -> {path}"

    def action_import_csv(self):
        if not self.db or not self.current_table:
            return
        schema = self.schemas.get(self.current_table)
        if not schema:
            message(self.stdscr, "Need schema to import.", "error")
            return
        try:
            r = prompt(self.stdscr, "Import CSV", [
                ("Path", "", "text"),
            ])
            path = r["Path"].strip()
            with open(path, "r", newline="", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)
                if not header:
                    return
                tmpl_by_col = dict(schema)
                rows_to_write = []
                for ln, raw in enumerate(reader, start=2):
                    if not raw:
                        continue
                    row_data = []
                    for col, cell in zip(header, raw):
                        if col not in tmpl_by_col:
                            raise ValueError(f"unknown column {col!r}")
                        tmpl = tmpl_by_col[col]
                        val = parse_like(tmpl, cell)
                        row_data.append(HyperDB.RowData(col, val))
                    rows_to_write.append(row_data)
            for row in rows_to_write:
                self.db.queue_write(self.current_table, row)
            self.db.wait_for_queue()
        except Cancelled:
            return
        except Exception as e:
            message(self.stdscr, str(e), "error")
            return
        self.load_current_table()
        self.toast = f"imported {len(rows_to_write)} rows"

    def action_help(self):
        msg = (
            "Navigation\n"
            "  ↑/↓ / PgUp/PgDn  move selection\n"
            "  Tab              switch focus tables/rows\n"
            "  Enter            open selected table\n\n"
            "Data\n"
            "  A add row     E edit row     D delete row\n"
            "  T add table   shift+X drop table\n"
            "  F find\n\n"
            "File\n"
            "  O open   N new   C close   S save   R reload\n"
            "  X export csv   I import csv\n\n"
            "  Q quit   ? help"
        )
        message(self.stdscr, msg, "info")

    # ---- main loop --------------------------------------------------------

    def run(self):
        curses.curs_set(0)
        self.stdscr.timeout(-1)
        # show welcome immediately
        self.render()
        while True:
            try:
                ch = self.stdscr.getch()
            except KeyboardInterrupt:
                break
            self.toast = ""
            if ch == curses.KEY_RESIZE:
                pass
            elif ch in (ord("q"), ord("Q")):
                if self.db and self.db.is_dirty():
                    if not confirm(self.stdscr, "Unsaved changes. Quit anyway?"):
                        continue
                break
            elif ch in (ord("?"),):
                self.action_help()
            elif self.db is None:
                if ch in (ord("o"), ord("O")):
                    self.action_open("open")
                elif ch in (ord("n"), ord("N")):
                    self.action_open("new")
            else:
                self._handle_main_key(ch)
            self.render()

    def _handle_main_key(self, ch):
        if ch in (ord("o"), ord("O")):
            self.action_close()
            self.action_open("open"); return
        if ch in (ord("n"), ord("N")):
            self.action_close()
            self.action_open("new"); return
        if ch in (ord("c"), ord("C")):
            self.action_close(); return
        if ch in (ord("s"), ord("S")):
            self.action_save(); return
        if ch in (ord("r"), ord("R")):
            self.action_reload(); return
        if ch in (ord("t"), ord("T")):
            self.action_add_table(); return
        if ch == ord("X"):
            self.action_drop_table(); return
        if ch in (ord("a"), ord("A")):
            self.action_add_row(); return
        if ch in (ord("e"), ord("E")):
            self.action_edit_row(); return
        if ch in (ord("d"), ord("D")):
            self.action_delete_row(); return
        if ch in (ord("f"), ord("F")):
            self.action_find(); return
        if ch in (ord("x"),):
            self.action_export_csv(); return
        if ch in (ord("i"), ord("I")):
            self.action_import_csv(); return
        if ch in (ord(","), ord("[")):
            self.page_prev(); return
        if ch in (ord("."), ord("]")):
            self.page_next(); return
        if ch in (ord("<"), ord("{")):
            self.page_offset = 0; self.load_current_table(); return
        if ch in (ord(">"), ord("}")):
            if self.total_rows > 0:
                self.page_offset = ((self.total_rows - 1) // self.page_size) * self.page_size
                self.load_current_table()
            return
        if ch in (ord("p"), ord("P")):
            self.page_set_size(); return
        if ch == 9:  # tab
            self.focus = "rows" if self.focus == "tables" else "tables"
            return
        if self.focus == "tables":
            if ch == curses.KEY_UP:
                self.table_idx = max(0, self.table_idx - 1)
            elif ch == curses.KEY_DOWN:
                self.table_idx = min(len(self.tables) - 1, self.table_idx + 1)
            elif ch in (10, 13, curses.KEY_RIGHT):
                if self.tables:
                    self.current_table = self.tables[self.table_idx]
                    self.row_idx = self.row_scroll = 0
                    self.page_offset = 0
                    self.load_current_table()
                    self.focus = "rows"
        else:
            if ch == curses.KEY_UP:
                self.row_idx = max(0, self.row_idx - 1)
            elif ch == curses.KEY_DOWN:
                self.row_idx = min(max(0, len(self.current_rows) - 1), self.row_idx + 1)
            elif ch == curses.KEY_PPAGE:
                self.row_idx = max(0, self.row_idx - 10)
            elif ch == curses.KEY_NPAGE:
                self.row_idx = min(max(0, len(self.current_rows) - 1), self.row_idx + 10)
            elif ch == curses.KEY_HOME:
                self.row_idx = 0
            elif ch == curses.KEY_END:
                self.row_idx = max(0, len(self.current_rows) - 1)
            elif ch == curses.KEY_LEFT:
                self.focus = "tables"


def _curses_main(stdscr):
    init_colors()
    stdscr.bkgd(" ", curses.color_pair(CP_BASE))
    app = CLIApp(stdscr)
    app.run()


def run():
    try:
        curses.wrapper(_curses_main)
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
