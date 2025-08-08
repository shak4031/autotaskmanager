import pandas as pd
from collections import defaultdict, deque
import datetime
import tkinter as tk
from tkinter import messagebox, ttk, simpledialog

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
# Data Loading / Persistence
# =============================

def load_all_tasks(csv_path):
    df = pd.read_csv(csv_path)
    required_columns = [
        "Project", "Milestone", "Task", "TaskID", "DependsOn",
        "EstimatedHours", "Priority", "StartDate", "DueDate", "Owner"
    ]
    for col in required_columns:
        if col not in df.columns:
            raise ValueError(f"Missing required column: {col}")

    # Ensure optional audit/time columns exist
    for opt in [
        "Status", "ActualHours", "LastComment", "CommentLog", "LastUpdated",
        "ActualSeconds", "InProgressStart"
    ]:
        if opt not in df.columns:
            df[opt] = None

    tasks = {}
    for _, row in df.iterrows():
        task_id = row["TaskID"]
        tasks[task_id] = {
            "Project": row["Project"],
            "Milestone": row["Milestone"],
            "Task": row["Task"],
            "TaskID": task_id,
            "DependsOn": str(row["DependsOn"]).split("|") if pd.notna(row["DependsOn"]) and row["DependsOn"] else [],
            "EstimatedHours": float(row["EstimatedHours"]),
            "Priority": row["Priority"],
            "StartDate": pd.to_datetime(row["StartDate"]),
            "DueDate": pd.to_datetime(row["DueDate"]),
            "Owner": row["Owner"],
            "ScheduledStart": None,
            "ScheduledEnd": None,
            "Status": (row["Status"] if pd.notna(row["Status"]) else "Pending"),
            "ActualHours": float(row["ActualHours"]) if pd.notna(row["ActualHours"]) else 0.0,
            "LastComment": row["LastComment"] if pd.notna(row["LastComment"]) else None,
            "CommentLog": row["CommentLog"] if pd.notna(row["CommentLog"]) else None,
            "LastUpdated": row["LastUpdated"] if pd.notna(row["LastUpdated"]) else None,
            # timing fields
            "ActualSeconds": int(row["ActualSeconds"]) if pd.notna(row["ActualSeconds"]) else 0,
            "InProgressStart": row["InProgressStart"] if pd.notna(row["InProgressStart"]) else None,
        }
    owners = sorted(df["Owner"].unique())
    return tasks, owners

def tasks_to_dataframe(tasks_all):
    rows = []
    for t in tasks_all.values():
        rows.append({
            "Project": t["Project"],
            "Milestone": t["Milestone"],
            "Task": t["Task"],
            "TaskID": t["TaskID"],
            "DependsOn": "|".join(t.get("DependsOn", [])) if t.get("DependsOn") else "",
            "EstimatedHours": t["EstimatedHours"],
            "Priority": t["Priority"],
            "StartDate": pd.to_datetime(t["StartDate"]).date(),
            "DueDate": pd.to_datetime(t["DueDate"]).date(),
            "Owner": t["Owner"],
            "Status": t.get("Status", "Pending"),
            "ActualHours": t.get("ActualHours", 0.0),
            "LastComment": t.get("LastComment"),
            "CommentLog": t.get("CommentLog"),
            "LastUpdated": t.get("LastUpdated"),
            "ActualSeconds": int(t.get("ActualSeconds", 0)),
            "InProgressStart": t.get("InProgressStart"),
        })
    return pd.DataFrame(rows, columns=[
        "Project","Milestone","Task","TaskID","DependsOn","EstimatedHours","Priority",
        "StartDate","DueDate","Owner","Status","ActualHours","LastComment","CommentLog",
        "LastUpdated","ActualSeconds","InProgressStart"
    ])

def save_all_tasks_to_csv(tasks_all, csv_path):
    df = tasks_to_dataframe(tasks_all)
    df.to_csv(csv_path, index=False)

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

def update_task_status(task, status, actual_hours=None, action_label=None, tasks_all=None, csv_path=None):
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
    if tasks_all is not None and csv_path:
        save_all_tasks_to_csv(tasks_all, csv_path)

# =============================
# Helpers
# =============================

def owner_active_counts(tasks_all, owners):
    counts = {o: 0 for o in owners}
    for t in tasks_all.values():
        if t.get("Status") not in ("Completed", "Canceled"):
            counts[t["Owner"]] = counts.get(t["Owner"], 0) + 1
    return counts

# =============================
# Kanban UI with Drag & Drop
# =============================

def show_kanban_ui(tasks_for_owner, owner, tasks_all, owners, csv_path):
    root = tk.Tk()
    root.title(f"Kanban - {owner}")
    root.geometry("1050x620+60+60")
    root.configure(bg="#FFFFFF")
    root.attributes("-topmost", True)

    # --- State ---
    drag_data = {"task_id": None, "source": None}
    selected_task_id = None  # focus when none in progress

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

    columns = {}
    lists = {}

    def make_column(name):
        col = tk.Frame(board, bg=KANBAN_COLORS[name], bd=1, relief=tk.SOLID)
        header = tk.Label(col, text=name, bg=KANBAN_COLORS[name], fg=HEADER_COLORS[name], font=header_font, pady=8)
        header.pack(fill="x")
        lb = tk.Listbox(col, activestyle='dotbox', selectmode=tk.SINGLE, font=body_font, bd=0, highlightthickness=0)
        lb.pack(fill="both", expand=True, padx=8, pady=8)
        col.pack(side=tk.LEFT, fill="both", expand=True, padx=6)
        columns[name] = col
        lists[name] = lb
        return lb

    for status in KANBAN_STATUSES:
        make_column(status)

    # --- Details Pane ---
    details = tk.Frame(root, bg="#F9FAFB", bd=1, relief=tk.SOLID)
    details.pack(fill="x", padx=10, pady=(0,10))
    d_task = tk.Label(details, text="", bg="#F9FAFB", font=("Segoe UI", 11))
    d_pm = tk.Label(details, text="", bg="#F9FAFB", font=("Segoe UI", 10))
    d_blocked = tk.Label(details, text="", bg="#F9FAFB", fg="#B91C1C", font=("Segoe UI", 10))
    d_task.pack(anchor="w", padx=10, pady=(8,2))
    d_pm.pack(anchor="w", padx=10)
    d_blocked.pack(anchor="w", padx=10, pady=(0,8))

    # --- Buttons ---
    btns = tk.Frame(details, bg="#F9FAFB")
    btns.pack(anchor="w", padx=10, pady=(0,10))
    pause_btn = ttk.Button(btns, text="Pause")  # no Unpause here
    complete_btn = ttk.Button(btns, text="Complete")
    cancel_btn = ttk.Button(btns, text="Cancel")
    reassign_btn = ttk.Button(btns, text="Reassign…")
    for b in (pause_btn, complete_btn, cancel_btn, reassign_btn):
        b.pack(side=tk.LEFT, padx=6)

    # --- Mapping from TaskID -> listbox index ---
    def label_for_task(t):
        return f"[{t['TaskID']}] {t['Task']}"

    def populate_lists():
        for status, lb in lists.items():
            lb.delete(0, tk.END)
        for t in tasks_for_owner:
            lists[t["Status"]].insert(tk.END, label_for_task(t))

    def find_task_by_label(label):
        # label ex: "[T12] Something"
        if not label.startswith("["):
            return None
        tid = label.split("]", 1)[0][1:]
        for t in tasks_for_owner:
            if t["TaskID"] == tid:
                return t
        return None

    def get_first_in_progress():
        ips = [t for t in tasks_for_owner if t["Status"] == "In Progress"]
        return ips[0] if ips else None

    def fmt_hms(seconds: int) -> str:
        seconds = max(0, int(seconds))
        h = seconds // 3600
        m = (seconds % 3600) // 60
        s = seconds % 60
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
            d_pm.config(text="")
            d_blocked.config(text="")
            return
        d_task.config(text=f"{t['Task']} — {t['Status']}")
        d_pm.config(text=f"Project: {t['Project']}  |  Milestone: {t['Milestone']}")
        reasons = get_block_reasons(t, tasks_all) if t["Status"] != "In Progress" else []
        d_blocked.config(text=("Blocked: " + "; ".join(reasons)) if reasons else "")

    def refresh_timer():
        t = get_first_in_progress()
        total = 0
        session = 0
        if t:
            total = int(t.get("ActualSeconds", 0))
            start = parse_iso(t.get("InProgressStart"))
            if start:
                session = int((datetime.datetime.now() - start).total_seconds())
        time_label.config(text=f"Current: {fmt_hms(session)}  |  Total: {fmt_hms(total + session)}")
        root.after(1000, refresh_timer)

    def enforce_single_in_progress(target_task):
        ip = get_first_in_progress()
        if ip and ip != target_task:
            messagebox.showinfo("In Progress", "Pause, complete, or cancel your current task before starting another.")
            return False
        return True

    def move_task_to_status(t, new_status, action_label=None):
        # Validate deps
        if new_status == "In Progress" and is_blocked(t, tasks_all):
            messagebox.showwarning("Blocked", "This task is blocked by incomplete dependencies.")
            return False
        # Enforce single in-progress
        if new_status == "In Progress" and not enforce_single_in_progress(t):
            return False
        # Transition with timing & persistence
        update_task_status(t, new_status, action_label=action_label or f"Move -> {new_status}", tasks_all=tasks_all, csv_path=csv_path)
        populate_lists()
        refresh_details(t)
        return True

    # --- Drag & Drop handlers ---
    def on_start_drag(event, status):
        lb = lists[status]
        idx = lb.nearest(event.y)
        if idx < 0:
            return
        label = lb.get(idx)
        task = find_task_by_label(label)
        if not task:
            return
        drag_data["task_id"] = task["TaskID"]
        drag_data["source"] = status

    def listbox_under_pointer():
        x_root = root.winfo_pointerx() - board.winfo_rootx()
        y_root = root.winfo_pointery() - board.winfo_rooty()
        for status, lb in lists.items():
            bx = lb.winfo_rootx() - board.winfo_rootx()
            by = lb.winfo_rooty() - board.winfo_rooty()
            bw = lb.winfo_width()
            bh = lb.winfo_height()
            if 0 <= x_root - bx <= bw and 0 <= y_root - by <= bh:
                return status
        return None

    def on_drop(event):
        if not drag_data["task_id"]:
            return
        target_status = listbox_under_pointer()
        source_status = drag_data["source"]
        if not target_status:
            drag_data["task_id"] = None; drag_data["source"] = None
            return
        # If dropped in same column, ignore
        if target_status == source_status:
            drag_data["task_id"] = None; drag_data["source"] = None
            return
        # Apply transition rules
        t = next((x for x in tasks_for_owner if x["TaskID"] == drag_data["task_id"]), None)
        drag_data["task_id"] = None; drag_data["source"] = None
        if not t:
            return
        # Only allow sensible moves
        allowed = {
            "Pending": {"In Progress", "Canceled"},
            "Paused": {"In Progress", "Canceled"},
            "In Progress": {"Paused", "Completed", "Canceled"},
            "Completed": set(),
            "Canceled": set(),
        }
        if target_status not in allowed.get(t["Status"], set()):
            messagebox.showinfo("Move not allowed", f"Cannot move from {t['Status']} to {target_status}.")
            return
        move_task_to_status(t, target_status)

    # Bind handlers
    for status, lb in lists.items():
        lb.bind('<ButtonPress-1>', lambda e, s=status: on_start_drag(e, s))
        lb.bind('<ButtonRelease-1>', on_drop)

    # --- Button actions ---
    def get_selected_task():
        # Try In Progress first
        ip = get_first_in_progress()
        if ip:
            return ip
        # else try selected from any list
        for st, lb in lists.items():
            try:
                idx = lb.curselection()
            except tk.TclError:
                continue
            if idx:
                label = lb.get(idx[0])
                return find_task_by_label(label)
        return None

    def do_pause():
        t = get_selected_task()
        if not t:
            return
        if t["Status"] != "In Progress":
            messagebox.showinfo("Pause", "You can only pause an In Progress task.")
            return
        move_task_to_status(t, "Paused", action_label="Pause")

    def do_complete():
        t = get_selected_task()
        if not t:
            return
        if t["Status"] != "In Progress":
            messagebox.showinfo("Complete", "Start the task before completing it.")
            return
        move_task_to_status(t, "Completed", action_label="Complete")

    def do_cancel():
        t = get_selected_task()
        if not t:
            return
        if t["Status"] not in ("Pending", "In Progress", "Paused"):
            messagebox.showinfo("Cancel", "Only Pending/In Progress/Paused tasks can be canceled.")
            return
        move_task_to_status(t, "Canceled", action_label="Cancel")

    def do_reassign():
        t = get_selected_task()
        if not t:
            return
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
            if not disp:
                win.destroy(); return
            new_owner = display_to_owner.get(disp)
            if not new_owner or new_owner == t["Owner"]:
                win.destroy(); return
            old_owner = t["Owner"]
            t["Owner"] = new_owner
            append_comment_log(t, f"Reassign to {new_owner}", prompt_comment("Reassign"))
            save_all_tasks_to_csv(tasks_all, csv_path)
            if old_owner != new_owner and t in tasks_for_owner:
                tasks_for_owner.remove(t)
            populate_lists()
            refresh_details()
            win.destroy()
        ttk.Button(win, text="OK", command=ok).pack(pady=6)
        win.grab_set(); win.transient(); win.focus_set(); win.wait_window(win)

    pause_btn.configure(command=do_pause)
    complete_btn.configure(command=do_complete)
    cancel_btn.configure(command=do_cancel)
    reassign_btn.configure(command=do_reassign)

    # Init
    populate_lists()
    refresh_details()
    refresh_timer()

    root.mainloop()

# =============================
# Main
# =============================
if __name__ == "__main__":
    csv_file = "project_tasks_with_comments.csv"  # adjust as needed
    try:
        tasks_all, owners = load_all_tasks(csv_file)
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
        show_kanban_ui(ordered_tasks_for_owner, owner=user_name, tasks_all=tasks_all, owners=owners, csv_path=csv_file)
    except ValueError as e:
        print(f"Error: {e}")
