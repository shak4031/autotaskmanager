import pandas as pd
from collections import defaultdict, deque
import datetime
import tkinter as tk
from tkinter import messagebox, ttk, simpledialog
import sqlite3, os

# =============================
# Config / Constants
# =============================
PRIORITY_ORDER = {"High": 1, "Medium": 2, "Low": 3}
WORK_START_HOUR = 9
WORK_END_HOUR = 17
LUNCH_START_HOUR = 12
LUNCH_END_HOUR = 13

KANBAN_STATUSES = ["Pending", "In Progress", "Paused", "Completed", "Canceled"]
KANBAN_COLORS = {
    "Pending":  "#F3F4F6",  # gray-100
    "In Progress": "#DBEAFE",  # blue-100
    "Paused": "#FEF3C7",  # amber-100
    "Completed": "#DCFCE7",  # green-100
    "Canceled": "#FEE2E2",  # red-100
}
HEADER_COLORS = {
    "Pending":  "#111827",  # gray-900
    "In Progress": "#1D4ED8",  # blue-700
    "Paused": "#92400E",  # amber-800
    "Completed": "#065F46",  # green-800
    "Canceled": "#991B1B",  # red-800
}

# =============================
# Data Loading / Persistence (SQLite)
# =============================

DB_DEFAULT_PATH = "tasks.db"

SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS tasks (
  TaskID TEXT PRIMARY KEY,
  Project TEXT NOT NULL,
  Milestone TEXT NOT NULL,
  Task TEXT NOT NULL,
  DependsOn TEXT,
  EstimatedHours REAL NOT NULL,
  Priority TEXT NOT NULL,
  StartDate TEXT NOT NULL,
  DueDate TEXT NOT NULL,
  Owner TEXT NOT NULL,
  Status TEXT DEFAULT 'Pending',
  ActualHours REAL DEFAULT 0.0,
  ActualSeconds INTEGER DEFAULT 0,
  InProgressStart TEXT,
  LastComment TEXT,
  CommentLog TEXT,
  LastUpdated TEXT
);
CREATE INDEX IF NOT EXISTS idx_tasks_owner ON tasks(Owner);
CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(Project);
CREATE INDEX IF NOT EXISTS idx_tasks_milestone ON tasks(Milestone);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(Status);
"""

def db_connect(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def ensure_schema(db_path: str):
    with db_connect(db_path) as conn:
        conn.executescript(SCHEMA_SQL)

def load_all_tasks_sqlite(db_path: str):
    ensure_schema(db_path)
    with db_connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM tasks").fetchall()
    tasks = {}
    owners = set()
    for r in rows:
        d = dict(r)
        d["EstimatedHours"] = float(d.get("EstimatedHours") or 0)
        d["ActualHours"] = float(d.get("ActualHours") or 0)
        d["ActualSeconds"] = int(d.get("ActualSeconds") or 0)
        d["DependsOn"] = (d.get("DependsOn") or "").split("|") if d.get("DependsOn") else []
        tasks[d["TaskID"]] = d
        owners.add(d["Owner"])
    return tasks, sorted(owners)

def update_task_sqlite(task: dict, db_path: str):
    fields = [
        "Project","Milestone","Task","DependsOn","EstimatedHours","Priority","StartDate","DueDate","Owner",
        "Status","ActualHours","LastComment","CommentLog","LastUpdated","ActualSeconds","InProgressStart"
    ]
    sets = ", ".join([f"{f}=?" for f in fields])
    vals = [
        task["Project"], task["Milestone"], task["Task"],
        "|".join(task.get("DependsOn", [])) if task.get("DependsOn") else "",
        float(task.get("EstimatedHours",0)), task["Priority"],
        str(task["StartDate"]).split(" ")[0], str(task["DueDate"]).split(" ")[0], task["Owner"],
        task.get("Status","Pending"), float(task.get("ActualHours",0.0)), task.get("LastComment"),
        task.get("CommentLog"), task.get("LastUpdated"), int(task.get("ActualSeconds",0)),
        task.get("InProgressStart")
    ]
    vals.append(task["TaskID"])
    with db_connect(db_path) as conn:
        conn.execute(f"UPDATE tasks SET {sets} WHERE TaskID=?", vals)

# =============================
# Ordering & Scheduling
# =============================

def topological_sort(tasks_all):
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    all_task_ids = set(tasks_all.keys())

    for task_id, task in tasks_all.items():
        for dep in task["DependsOn"]:
            if dep not in all_task_ids:
                raise ValueError(f"Task '{task_id}' depends on undefined task '{dep}'.")
            graph[dep].append(task_id)
            in_degree[task_id] += 1
        in_degree.setdefault(task_id, 0)

    queue = deque([tid for tid in tasks_all if in_degree[tid] == 0])
    ordered_ids = []

    while queue:
        ready_sorted = sorted(list(queue), key=lambda tid: PRIORITY_ORDER.get(tasks_all[tid]["Priority"], 99))
        current = ready_sorted[0]
        queue.remove(current)
        ordered_ids.append(current)

        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(ordered_ids) != len(tasks_all):
        unresolved = all_task_ids - set(ordered_ids)
        raise ValueError(f"Circular dependency detected or unresolved tasks: {', '.join(sorted(unresolved))}")

    return ordered_ids

def order_tasks_for_owner(ordered_ids_all, owner_tasks):
    return [owner_tasks[tid] for tid in ordered_ids_all if tid in owner_tasks]

def allocate_schedule(task_list):
    today = datetime.datetime.now().replace(hour=WORK_START_HOUR, minute=0, second=0, microsecond=0)
    current_time = today
    for task in task_list:
        duration = task["EstimatedHours"]
        hours_remaining = duration
        task_start = None
        while hours_remaining > 0:
            if current_time.hour < WORK_START_HOUR:
                current_time = current_time.replace(hour=WORK_START_HOUR, minute=0)
            elif LUNCH_START_HOUR <= current_time.hour < LUNCH_END_HOUR:
                current_time = current_time.replace(hour=LUNCH_END_HOUR, minute=0)
            elif current_time.hour >= WORK_END_HOUR:
                current_time = (current_time + datetime.timedelta(days=1)).replace(hour=WORK_START_HOUR, minute=0)
            else:
                if task_start is None:
                    task_start = current_time
                hours_to_work = min(hours_remaining, WORK_END_HOUR - current_time.hour)
                if current_time.hour < LUNCH_START_HOUR and current_time.hour + hours_to_work > LUNCH_START_HOUR:
                    hours_to_work = LUNCH_START_HOUR - current_time.hour
                current_time += datetime.timedelta(hours=hours_to_work)
                hours_remaining -= hours_to_work
        task["ScheduledStart"] = task_start
        task["ScheduledEnd"] = current_time

# =============================
# Blocked logic
# =============================

def get_block_reasons(task, tasks_all):
    reasons = []
    for dep_id in task.get("DependsOn", []):
        dep = tasks_all.get(dep_id)
        if dep is None:
            reasons.append(f"Depends on missing task {dep_id}")
        elif dep["Status"] != "Completed":
            reasons.append(f"Waiting on {dep_id} ({dep['Task']}) owned by {dep['Owner']}")
    return reasons

def is_blocked(task, tasks_all):
    return len(get_block_reasons(task, tasks_all)) > 0

# =============================
# Status / Comments / Timing
# =============================

def prompt_comment(action_label):
    c = simpledialog.askstring("Comment", f"Add a comment for '{action_label}' (optional):")
    return c.strip() if c else None

def append_comment_log(task, action_label, comment):
    ts = datetime.datetime.now().isoformat(timespec='seconds')
    entry = f"{ts} | {action_label}: {comment if comment else ''}"
    if task.get("CommentLog"):
        task["CommentLog"] = str(task["CommentLog"]) + "\n" + entry
    else:
        task["CommentLog"] = entry
    task["LastComment"] = comment
    task["LastUpdated"] = ts

def update_task_status(task, status, actual_hours=None, action_label=None, tasks_all=None, db_path=None):
    def parse_iso(ts):
        try:
            return datetime.datetime.fromisoformat(ts) if ts else None
        except Exception:
            return None

    now = datetime.datetime.now()
    prev_status = task.get("Status", "Pending")
    in_start = parse_iso(task.get("InProgressStart"))

    # If leaving In Progress, accumulate time
    if prev_status == "In Progress" and status != "In Progress":
        if in_start:
            elapsed = (now - in_start).total_seconds()
            task["ActualSeconds"] = int(task.get("ActualSeconds", 0)) + max(0, int(elapsed))
        task["InProgressStart"] = None
        task["ActualHours"] = round(task.get("ActualSeconds", 0) / 3600.0, 2)

    # If entering In Progress, start session
    if status == "In Progress" and prev_status != "In Progress":
        if not task.get("InProgressStart"):
            task["InProgressStart"] = now.isoformat()

    valid_statuses = ["Pending", "In Progress", "Paused", "Completed", "Canceled"]
    if status not in valid_statuses:
        raise ValueError(f"Invalid task status: {status}")
    task["Status"] = status

    label = action_label or f"Status -> {status}"
    comment = prompt_comment(label)
    append_comment_log(task, label, comment)
    if tasks_all is not None and db_path:
        update_task_sqlite(task, db_path)

# =============================
# Helpers
# =============================

def owner_active_counts(tasks_all, owners):
    counts = {o: 0 for o in owners}
    for t in tasks_all.values():
        if t.get("Status") not in ("Completed", "Canceled"):
            counts[t["Owner"]] = counts.get(t["Owner"], 0) + 1
    return counts

def calc_progress_all(tasks_all, project=None, milestone=None):
    """Percent complete across *all owners* for a project/milestone."""
    filtered = [t for t in tasks_all.values()
                if (project is None or t["Project"] == project)
                and (milestone is None or t["Milestone"] == milestone)]
    if not filtered:
        return 0
    completed = sum(1 for t in filtered if t.get("Status") == "Completed")
    return int((completed / len(filtered)) * 100)

# =============================
# Kanban UI with Drag & Drop
# =============================

def show_kanban_ui(tasks_for_owner, owner, tasks_all, owners, db_path):
    import textwrap

    # ---------- tiny tooltip helper ----------
    class ToolTip:
    def __init__(self, widget, textfunc):
        self.widget = widget
        self.textfunc = textfunc
        self.tip = None
        widget.bind("<Motion>", self._show)
        widget.bind("<Leave>", self._hide)

    def _show(self, event):
        idx = self.widget.nearest(event.y)
        if idx < 0:
            self._hide(None); return
        label = self.widget.get(idx)
        text = self.textfunc(label)
        if not text:
            self._hide(None); return

        # kill previous tip
        if self.tip:
            try: self.tip.destroy()
            except Exception: pass
            self.tip = None

        x = self.widget.winfo_rootx() + event.x + 16
        y = self.widget.winfo_rooty() + event.y + 16
        self.tip = tk.Toplevel(self.widget)
        self.tip.wm_overrideredirect(True)
        self.tip.attributes("-topmost", True)

        lbl = tk.Label(
            self.tip, text=text, justify="left",
            background="#111827", foreground="#F9FAFB",
            relief=tk.SOLID, borderwidth=1, font=("Segoe UI", 9)
        )
        # ✅ correct padding args
        lbl.pack(ipadx=6, ipady=4)

        self.tip.wm_geometry(f"+{x}+{y}")

    def _hide(self, _):
        if self.tip:
            try: self.tip.destroy()
            except Exception: pass
            self.tip = None


    root = tk.Tk()
    root.title(f"Kanban - {owner}")
    root.geometry("1250x780+60+60")
    root.configure(bg="#FFFFFF")
    root.attributes("-topmost", True)

    # --- State ---
    drag_data = {"task_id": None, "source": None}

    # --- Styles ---
    style = ttk.Style(root)
    style.theme_use("clam")
    header_font = ("Segoe UI", 11, "bold")
    body_font = ("Segoe UI", 10)

    # --- Top Bar ---
    top = tk.Frame(root, bg="#111827")
    top.pack(fill="x")
    title = tk.Label(top, text=f"Task Board — {owner}", fg="white", bg="#111827", font=("Segoe UI", 13, "bold"))
    title.pack(side=tk.LEFT, padx=12, pady=10)
    time_label = tk.Label(top, text="", fg="#E5E7EB", bg="#111827", font=("Segoe UI", 10))
    time_label.pack(side=tk.RIGHT, padx=12)

    # --- Columns container ---
    board = tk.Frame(root, bg="#FFFFFF")
    board.pack(fill="both", expand=True, padx=10, pady=10)

    columns, lists, xscrolls = {}, {}, {}

    def make_column(name):
        col = tk.Frame(board, bg=KANBAN_COLORS[name], bd=1, relief=tk.SOLID)
        header = tk.Label(col, text=name, bg=KANBAN_COLORS[name], fg=HEADER_COLORS[name], font=header_font, pady=8)
        header.pack(fill="x")
        lb = tk.Listbox(col, activestyle='dotbox', selectmode=tk.SINGLE, font=body_font, bd=0, highlightthickness=0)
        xscroll = tk.Scrollbar(col, orient="horizontal", command=lb.xview)
        lb.configure(xscrollcommand=xscroll.set)
        lb.pack(fill="both", expand=True, padx=8, pady=(8,0))
        xscroll.pack(fill="x", padx=8, pady=(0,8))
        col.pack(side=tk.LEFT, fill="both", expand=True, padx=6)
        columns[name] = col
        lists[name] = lb
        xscrolls[name] = xscroll
        return lb

    for status in KANBAN_STATUSES:
        make_column(status)

    # --- Details Pane & Progress (already in your version) ---
    details = tk.Frame(root, bg="#F9FAFB", bd=1, relief=tk.SOLID)
    details.pack(fill="x", padx=10, pady=(0,10))
    d_task = tk.Label(details, text="", bg="#F9FAFB", font=("Segoe UI", 11))
    d_pm = tk.Label(details, text="", bg="#F9FAFB", font=("Segoe UI", 10))
    d_blocked = tk.Label(details, text="", bg="#F9FAFB", fg="#B91C1C", font=("Segoe UI", 10))
    d_task.pack(anchor="w", padx=10, pady=(8,2))
    d_pm.pack(anchor="w", padx=10)
    d_blocked.pack(anchor="w", padx=10, pady=(0,8))

    prog = tk.Frame(details, bg="#F9FAFB")
    prog.pack(fill="x", padx=10, pady=(0,10))
    proj_lbl = tk.Label(prog, text="Project Progress", bg="#F9FAFB", font=("Segoe UI", 9))
    proj_lbl.grid(row=0, column=0, sticky="w")
    proj_pct = tk.Label(prog, text="0%", bg="#F9FAFB", font=("Segoe UI", 9))
    proj_pct.grid(row=0, column=2, sticky="e")
    proj_bar = ttk.Progressbar(prog, orient="horizontal", length=640, mode="determinate")
    proj_bar.grid(row=0, column=1, padx=8, sticky="we")
    ms_lbl = tk.Label(prog, text="Milestone Progress", bg="#F9FAFB", font=("Segoe UI", 9))
    ms_lbl.grid(row=1, column=0, sticky="w", pady=(6,0))
    ms_pct = tk.Label(prog, text="0%", bg="#F9FAFB", font=("Segoe UI", 9))
    ms_pct.grid(row=1, column=2, sticky="e", pady=(6,0))
    ms_bar = ttk.Progressbar(prog, orient="horizontal", length=640, mode="determinate")
    ms_bar.grid(row=1, column=1, padx=8, sticky="we", pady=(6,0))
    prog.grid_columnconfigure(1, weight=1)

    # --- Buttons, now with Reopen ---
    btns = tk.Frame(details, bg="#F9FAFB")
    btns.pack(anchor="w", padx=10, pady=(0,10))
    pause_btn = ttk.Button(btns, text="Pause")
    complete_btn = ttk.Button(btns, text="Complete")
    cancel_btn = ttk.Button(btns, text="Cancel")
    reopen_btn = ttk.Button(btns, text="Reopen")
    reassign_btn = ttk.Button(btns, text="Reassign…")
    for b in (pause_btn, complete_btn, cancel_btn, reopen_btn, reassign_btn):
        b.pack(side=tk.LEFT, padx=6)

    # ---------- label helpers ----------
    def truncate(text, max_len=60):
        return text if len(text) <= max_len else text[:max_len - 1] + "…"

    def priority_badge_and_color(priority):
        p = (priority or "").lower()
        if p.startswith("high"):   return "🔴", "#B91C1C"
        if p.startswith("medium"): return "🟠", "#92400E"
        if p.startswith("low"):    return "🟢", "#065F46"
        return "", "#111827"

    def label_for_task(t):
        badge, color = priority_badge_and_color(t.get("Priority"))
        proj_pct_badge = calc_progress_all(tasks_all, project=t['Project'])
        blk = " ⛔" if is_blocked(t, tasks_all) else ""
        base = f"{badge} [{t['TaskID']}] {truncate(t['Task'], 50)} · {proj_pct_badge}% proj{blk}"
        return base, color

    def tooltip_text_for_label(label):
        # robust extraction of [TID]
        tid = None
        if " [" in label and "]" in label:
            try:
                tid = label.split(" [",1)[1].split("]",1)[0]
            except Exception:
                pass
        if not tid and "[" in label and "]" in label:
            tid = label.split("[",1)[1].split("]",1)[0]
        if not tid: return None
        t = tasks_all.get(tid)
        if not t: return None
        lines = [
            f"[{t['TaskID']}] {t['Task']}",
            f"Project: {t['Project']} | Milestone: {t['Milestone']}",
            f"Owner: {t['Owner']} | Priority: {t['Priority']} | Status: {t['Status']}",
        ]
        if t.get("DependsOn"):
            lines.append(f"Depends on: {', '.join(t['DependsOn'])}")
        return "\n".join(lines)

    # ---------- population ----------
    def populate_lists():
        for status, lb in lists.items():
            lb.delete(0, tk.END)
        for t in tasks_for_owner:
            if t.get("Owner") != owner:
                continue
            text, color = label_for_task(t)
            lb = lists[t["Status"]]
            lb.insert(tk.END, text)
            try:
                # color the whole line based on priority; blocked badge still visible
                lb.itemconfig(tk.END, foreground=color)
            except tk.TclError:
                pass

    # add tooltips to all columns
    for lb in lists.values():
        ToolTip(lb, tooltip_text_for_label)

    # ---------- selection & timing ----------
    def find_task_by_label(label):
        """
        Extract TaskID from labels that may start with an emoji badge,
        e.g., '🔴 [T12] Build API · 40% proj' or '[T12] Build API'.
        """
        try:
            if not label.startswith("["):
                if " [" in label:
                    label = "[" + label.split(" [", 1)[1]
                else:
                    lb = label.find("[")
                    if lb != -1:
                        label = label[lb:]
            tid = label.split("]", 1)[0].split("[")[-1].strip()
            return tasks_all.get(tid)
        except Exception:
            return None

    def get_first_in_progress():
        ips = [t for t in tasks_for_owner if t.get("Owner") == owner and t["Status"] == "In Progress"]
        return ips[0] if ips else None

    def fmt_hms(seconds: int) -> str:
        seconds = max(0, int(seconds))
        h = seconds // 3600; m = (seconds % 3600) // 60; s = seconds % 60
        return f"{h:02d}:{m:02d}:{s:02d}"

    def parse_iso(ts):
        try:
            return datetime.datetime.fromisoformat(ts) if ts else None
        except Exception:
            return None

    def refresh_details(selection_task=None):
        t = selection_task or get_first_in_progress()
        if not t:
            d_task.config(text="No task selected")
            d_pm.config(text=""); d_blocked.config(text="")
            proj_bar["value"] = 0; proj_pct.config(text="0%")
            ms_bar["value"] = 0; ms_pct.config(text="0%")
            return
        d_task.config(text=f"{t['Task']} — {t['Status']}")
        d_pm.config(text=f"Project: {t['Project']}  |  Milestone: {t['Milestone']}")
        reasons = get_block_reasons(t, tasks_all) if t["Status"] != "In Progress" else []
        d_blocked.config(text=("Blocked: " + "; ".join(reasons)) if reasons else "")
        p = calc_progress_all(tasks_all, project=t['Project'])
        m = calc_progress_all(tasks_all, project=t['Project'], milestone=t['Milestone'])
        proj_bar["value"] = p; proj_pct.config(text=f"{p}%")
        ms_bar["value"] = m; ms_pct.config(text=f"{m}%")

    def refresh_timer():
        t = get_first_in_progress()
        total = 0; session = 0
        if t:
            total = int(t.get("ActualSeconds", 0))
            start = parse_iso(t.get("InProgressStart"))
            if start:
                session = int((datetime.datetime.now() - start).total_seconds())
        time_label.config(text=f"Current: {fmt_hms(session)}  |  Total: {fmt_hms(total + session)}")
        root.after(1000, refresh_timer)

    # ---------- transitions ----------
    def enforce_single_in_progress(target_task):
        ip = get_first_in_progress()
        if ip and ip != target_task:
            messagebox.showinfo("In Progress", "Pause, complete, or cancel your current task before starting another.")
            return False
        return True

    def ensure_owner(t):
        if t.get("Owner") != owner:
            messagebox.showerror("Not Allowed", f"This task is owned by {t.get('Owner')}. You can only move your own tasks.")
            return False
        return True

    def move_task_to_status(t, new_status, action_label=None):
        if not ensure_owner(t):
            return False
        if new_status == "In Progress" and is_blocked(t, tasks_all):
            messagebox.showwarning("Blocked", "This task is blocked by incomplete dependencies.")
            return False
        if new_status == "In Progress" and not enforce_single_in_progress(t):
            return False
        update_task_status(t, new_status, action_label=action_label or f"Move -> {new_status}",
                           tasks_all=tasks_all, db_path=db_path)
        populate_lists()
        refresh_details(t)
        return True

    # Drag & Drop
    def on_start_drag(event, status):
        lb = lists[status]
        idx = lb.nearest(event.y)
        if idx < 0: return
        label = lb.get(idx)
        task = find_task_by_label(label)
        if not task: return
        drag_data["task_id"] = task["TaskID"]
        drag_data["source"] = status

    def listbox_under_pointer():
        x_root = root.winfo_pointerx() - board.winfo_rootx()
        y_root = root.winfo_pointery() - board.winfo_rooty()
        for status, lb in lists.items():
            bx = lb.winfo_rootx() - board.winfo_rootx()
            by = lb.winfo_rooty() - board.winfo_rooty()
            bw = lb.winfo_width(); bh = lb.winfo_height()
            if 0 <= x_root - bx <= bw and 0 <= y_root - by <= bh:
                return status
        return None

    def on_drop(event):
        if not drag_data["task_id"]: return
        target_status = listbox_under_pointer()
        source_status = drag_data["source"]
        tid = drag_data["task_id"]
        drag_data["task_id"] = None; drag_data["source"] = None
        if not target_status or target_status == source_status: return
        t = tasks_all.get(tid)
        if not t: return
        allowed = {
            "Pending": {"In Progress", "Canceled"},
            "Paused": {"In Progress", "Canceled"},
            "In Progress": {"Paused", "Completed", "Canceled"},
            "Completed": {"Pending"},      # <-- allow reopen by drag
            "Canceled": set(),
        }
        if target_status not in allowed.get(t["Status"], set()):
            messagebox.showinfo("Move not allowed", f"Cannot move from {t['Status']} to {target_status}.")
            return
        move_task_to_status(t, target_status)

    for status, lb in lists.items():
        lb.bind('<ButtonPress-1>', lambda e, s=status: on_start_drag(e, s))
        lb.bind('<ButtonRelease-1>', on_drop)

    # ---------- selection helpers ----------
    def get_selected_task():
        ip = get_first_in_progress()
        if ip:
            return ip
        for st, lb in lists.items():
            try:
                idx = lb.curselection()
            except tk.TclError:
                continue
            if idx:
                label = lb.get(idx[0])
                t = find_task_by_label(label)
                if t and t.get("Owner") == owner:
                    return t
        return None

    # ---------- quick actions / keyboard & double-click ----------
    def start_or_pause_selected():
        t = get_selected_task()
        if not t: return
        if t["Status"] in ("Pending", "Paused"):
            move_task_to_status(t, "In Progress", action_label="Start")
        elif t["Status"] == "In Progress":
            move_task_to_status(t, "Paused", action_label="Pause")

    def do_pause():
        t = get_selected_task()
        if not t: return
        if t["Status"] != "In Progress":
            messagebox.showinfo("Pause", "You can only pause an In Progress task.")
            return
        move_task_to_status(t, "Paused", action_label="Pause")

    def do_complete():
        t = get_selected_task()
        if not t: return
        if t["Status"] != "In Progress":
            messagebox.showinfo("Complete", "Start the task before completing it.")
            return
        move_task_to_status(t, "Completed", action_label="Complete")

    def do_cancel():
        t = get_selected_task()
        if not t: return
        if t["Status"] not in ("Pending", "In Progress", "Paused"):
            messagebox.showinfo("Cancel", "Only Pending/In Progress/Paused tasks can be canceled.")
            return
        move_task_to_status(t, "Canceled", action_label="Cancel")

    def do_reopen():
        t = get_selected_task()
        if not t: return
        if t["Status"] != "Completed":
            messagebox.showinfo("Reopen", "Only completed tasks can be reopened.")
            return
        move_task_to_status(t, "Pending", action_label="Reopen")

    def do_reassign():
        t = get_selected_task()
        if not t: return
        counts = owner_active_counts(tasks_all, owners)
        choices = [(o, f"{o} ({counts.get(o,0)})") for o in owners if o != t["Owner"]]
        display_to_owner = {disp: o for o, disp in choices}
        display_values = [disp for _, disp in choices]
        win = tk.Toplevel(root)
        win.title(f"Reassign {t['TaskID']}")
        tk.Label(win, text=f"Reassign task {t['TaskID']} to:").pack(padx=10, pady=6)
        sel = tk.StringVar(value=display_values[0] if display_values else "")
        combo = ttk.Combobox(win, textvariable=sel, values=display_values, state="readonly", width=30)
        combo.pack(padx=10, pady=6)
        def ok():
            disp = sel.get()
            if not disp: win.destroy(); return
            new_owner = display_to_owner.get(disp)
            if not new_owner or new_owner == t["Owner"]:
                win.destroy(); return
            t["Owner"] = new_owner
            append_comment_log(t, f"Reassign to {new_owner}", prompt_comment("Reassign"))
            update_task_sqlite(t, db_path)
            win.destroy()
            populate_lists()
            refresh_details()
        ttk.Button(win, text="OK", command=ok).pack(pady=6)
        win.grab_set(); win.transient(); win.focus_set(); win.wait_window(win)

    # Buttons
    pause_btn.configure(command=do_pause)
    complete_btn.configure(command=do_complete)
    cancel_btn.configure(command=do_cancel)
    reopen_btn.configure(command=do_reopen)
    reassign_btn.configure(command=do_reassign)

    # Double-click to start (Pending/Paused) or pause (In Progress)
    for lb in lists.values():
        lb.bind("<Double-Button-1>", lambda e: start_or_pause_selected())

    # Key bindings (board-wide)
    root.bind("<Return>", lambda e: start_or_pause_selected())
    root.bind("<Control-Return>", lambda e: do_complete())
    root.bind("c", lambda e: do_complete())
    root.bind("p", lambda e: do_pause())
    root.bind("x", lambda e: do_cancel())
    root.bind("r", lambda e: do_reassign())

    # Init
    populate_lists()
    refresh_details()

    # Timer
    def parse_iso_local(ts):
        try:
            return datetime.datetime.fromisoformat(ts) if ts else None
        except Exception:
            return None

    def refresh_timer():
        t = get_first_in_progress()
        total = 0; session = 0
        if t:
            total = int(t.get("ActualSeconds", 0))
            start = parse_iso_local(t.get("InProgressStart"))
            if start:
                session = int((datetime.datetime.now() - start).total_seconds())
        time_label.config(text=f"Current: {fmt_hms(session)}  |  Total: {fmt_hms(total + session)}")
        root.after(1000, refresh_timer)

    refresh_timer()
    root.mainloop()


# =============================
# Main
# =============================
if __name__ == "__main__":
    db_path = "tasks.db"  # adjust as needed
    try:
        tasks_all, owners = load_all_tasks_sqlite(db_path)
        ordered_ids_all = topological_sort(tasks_all)
        # Select user dialog
        sel_root = tk.Tk(); sel_root.title("Select User")
        tk.Label(sel_root, text="Select the user:").pack(pady=6, padx=10)
        selected_user = tk.StringVar(value=owners[0] if owners else "")
        combo = ttk.Combobox(sel_root, textvariable=selected_user, values=owners, state="readonly", width=30)
        combo.pack(pady=6, padx=10)
        def ok():
            sel_root.destroy()
        ttk.Button(sel_root, text="OK", command=ok).pack(pady=8)
        sel_root.mainloop()
        user_name = selected_user.get()

        owner_tasks = {tid: t for tid, t in tasks_all.items() if t["Owner"] == user_name}
        ordered_tasks_for_owner = [owner_tasks[tid] for tid in ordered_ids_all if tid in owner_tasks]
        allocate_schedule(ordered_tasks_for_owner)
        show_kanban_ui(ordered_tasks_for_owner, owner=user_name, tasks_all=tasks_all, owners=owners, db_path=db_path)
    except ValueError as e:
        print(f"Error: {e}")
