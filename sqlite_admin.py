# sqlite_admin.py
import sqlite3, argparse, sys, csv, os, datetime

DB = "tasks.db"

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
CREATE TABLE IF NOT EXISTS tasks (
  TaskID TEXT PRIMARY KEY,
  Project TEXT NOT NULL,
  Milestone TEXT NOT NULL,
  Task TEXT NOT NULL,
  DependsOn TEXT,
  EstimatedHours REAL NOT NULL,
  Priority TEXT NOT NULL,          -- High/Medium/Low
  StartDate TEXT NOT NULL,         -- yyyy-mm-dd
  DueDate TEXT NOT NULL,           -- yyyy-mm-dd
  Owner TEXT NOT NULL,
  Status TEXT DEFAULT 'Pending',   -- Pending/In Progress/Paused/Completed/Canceled
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

def db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn

def ensure_schema():
    with db() as conn:
        conn.executescript(SCHEMA)

def now_iso():
    return datetime.datetime.now().isoformat(timespec="seconds")

# ---------- CRUD ----------
def add_task(args):
    with db() as conn:
        conn.execute("""INSERT INTO tasks (
            TaskID, Project, Milestone, Task, DependsOn, EstimatedHours, Priority,
            StartDate, DueDate, Owner, Status, ActualHours, ActualSeconds,
            InProgressStart, LastComment, CommentLog, LastUpdated
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
            args.task_id, args.project, args.milestone, args.task, args.depends_on or "",
            float(args.estimated_hours), args.priority,
            args.start_date, args.due_date, args.owner,
            args.status or "Pending", 0.0, 0, None, None, None, now_iso()
        ))
    print(f"Added task {args.task_id}")

def update_task(args):
    # update arbitrary fields by name
    allowed = {"Project","Milestone","Task","DependsOn","EstimatedHours","Priority",
               "StartDate","DueDate","Owner","Status"}
    sets, vals = [], []
    for pair in args.set or []:
        k,v = pair.split("=",1)
        if k not in allowed:
            print(f"Field not allowed: {k}")
            sys.exit(2)
        # normalize a couple
        if k in {"EstimatedHours"}: v = float(v)
        sets.append(f"{k}=?"); vals.append(v)
    if not sets:
        print("Nothing to update.")
        return
    vals.append(args.task_id)
    with db() as conn:
        conn.execute(f"UPDATE tasks SET {', '.join(sets)}, LastUpdated=? WHERE TaskID=?",
                     vals[:-1] + [now_iso(), vals[-1]])
    print(f"Updated {args.task_id}: {', '.join(args.set)}")

def set_deps(args):
    with db() as conn:
        conn.execute("UPDATE tasks SET DependsOn=?, LastUpdated=? WHERE TaskID=?",
                     (args.depends_on or "", now_iso(), args.task_id))
    print(f"Set dependencies for {args.task_id} -> {args.depends_on or '(none)'}")

def reassign(args):
    with db() as conn:
        conn.execute("UPDATE tasks SET Owner=?, LastUpdated=? WHERE TaskID=?",
                     (args.owner, now_iso(), args.task_id))
    print(f"Reassigned {args.task_id} to {args.owner}")

def set_priority(args):
    with db() as conn:
        conn.execute("UPDATE tasks SET Priority=?, LastUpdated=? WHERE TaskID=?",
                     (args.priority, now_iso(), args.task_id))
    print(f"Priority for {args.task_id} -> {args.priority}")

def set_status(args):
    with db() as conn:
        conn.execute("UPDATE tasks SET Status=?, LastUpdated=? WHERE TaskID=?",
                     (args.status, now_iso(), args.task_id))
    print(f"Status for {args.task_id} -> {args.status}")

def delete_task(args):
    with db() as conn:
        conn.execute("DELETE FROM tasks WHERE TaskID=?", (args.task_id,))
    print(f"Deleted {args.task_id}")

# ---------- Listing ----------
def list_projects(args):
    with db() as conn:
        cur = conn.execute("SELECT DISTINCT Project FROM tasks ORDER BY Project")
        for r in cur.fetchall():
            print(r["Project"])

def list_milestones(args):
    with db() as conn:
        if args.project:
            cur = conn.execute("SELECT DISTINCT Milestone FROM tasks WHERE Project=? ORDER BY Milestone", (args.project,))
        else:
            cur = conn.execute("SELECT DISTINCT Milestone FROM tasks ORDER BY Milestone")
        for r in cur.fetchall():
            print(r["Milestone"])

def list_tasks(args):
    q = "SELECT TaskID,Project,Milestone,Task,Owner,Priority,Status,DependsOn FROM tasks WHERE 1=1"
    vals = []
    if args.project:
        q += " AND Project=?"; vals.append(args.project)
    if args.milestone:
        q += " AND Milestone=?"; vals.append(args.milestone)
    if args.owner:
        q += " AND Owner=?"; vals.append(args.owner)
    if args.status:
        q += " AND Status=?"; vals.append(args.status)
    q += " ORDER BY Project, Milestone, Priority"
    with db() as conn:
        for r in conn.execute(q, vals):
            print(f"[{r['TaskID']}] {r['Project']} / {r['Milestone']} — {r['Task']} | {r['Owner']} | {r['Priority']} | {r['Status']} | deps:{r['DependsOn'] or '-'}")

# ---------- Import CSV again (on demand) ----------
def import_csv(args):
    if not os.path.exists(args.csv):
        print(f"CSV not found: {args.csv}")
        sys.exit(1)
    with db() as conn:
        conn.executescript(SCHEMA)
        if args.replace:
            conn.execute("DELETE FROM tasks")
        cols = ["TaskID","Project","Milestone","Task","DependsOn","EstimatedHours","Priority",
                "StartDate","DueDate","Owner","Status","ActualHours","LastComment",
                "CommentLog","LastUpdated","ActualSeconds","InProgressStart"]
        placeholders = ",".join(["?"]*len(cols))
        sql = f"INSERT OR REPLACE INTO tasks ({','.join(cols)}) VALUES ({placeholders})"
        with open(args.csv, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            data = []
            for r in reader:
                r.setdefault("Status","Pending")
                r.setdefault("ActualHours","0")
                r.setdefault("ActualSeconds","0")
                row = [r.get(c,"") for c in cols]
                data.append(row)
        conn.executemany(sql, data)
    print(f"Imported {len(data)} rows from {args.csv}")

def main():
    ensure_schema()
    ap = argparse.ArgumentParser(description="SQLite admin for tasks.db")
    sub = ap.add_subparsers(dest="cmd")

    sp = sub.add_parser("add-task", help="Add a new task")
    sp.add_argument("--task-id", required=True)
    sp.add_argument("--project", required=True)
    sp.add_argument("--milestone", required=True)
    sp.add_argument("--task", required=True)
    sp.add_argument("--depends-on", default="")
    sp.add_argument("--estimated-hours", required=True)
    sp.add_argument("--priority", choices=["High","Medium","Low"], required=True)
    sp.add_argument("--start-date", required=True)  # yyyy-mm-dd
    sp.add_argument("--due-date", required=True)    # yyyy-mm-dd
    sp.add_argument("--owner", required=True)
    sp.add_argument("--status", choices=["Pending","In Progress","Paused","Completed","Canceled"])
    sp.set_defaults(func=add_task)

    sp = sub.add_parser("update-task", help="Update fields on an existing task")
    sp.add_argument("--task-id", required=True)
    sp.add_argument("--set", action="append", help="FIELD=VALUE (repeatable), allowed: Project,Milestone,Task,DependsOn,EstimatedHours,Priority,StartDate,DueDate,Owner,Status")
    sp.set_defaults(func=update_task)

    sp = sub.add_parser("set-deps", help="Set DependsOn (pipe-separated TaskIDs)")
    sp.add_argument("--task-id", required=True)
    sp.add_argument("--depends-on", default="")
    sp.set_defaults(func=set_deps)

    sp = sub.add_parser("reassign", help="Change owner")
    sp.add_argument("--task-id", required=True)
    sp.add_argument("--owner", required=True)
    sp.set_defaults(func=reassign)

    sp = sub.add_parser("set-priority", help="Change priority")
    sp.add_argument("--task-id", required=True)
    sp.add_argument("--priority", choices=["High","Medium","Low"], required=True)
    sp.set_defaults(func=set_priority)

    sp = sub.add_parser("set-status", help="Change status manually")
    sp.add_argument("--task-id", required=True)
    sp.add_argument("--status", choices=["Pending","In Progress","Paused","Completed","Canceled"], required=True)
    sp.set_defaults(func=set_status)

    sp = sub.add_parser("delete-task", help="Delete task")
    sp.add_argument("--task-id", required=True)
    sp.set_defaults(func=delete_task)

    sp = sub.add_parser("list-projects", help="List projects")
    sp.set_defaults(func=list_projects)

    sp = sub.add_parser("list-milestones", help="List milestones (optionally for a project)")
    sp.add_argument("--project")
    sp.set_defaults(func=list_milestones)

    sp = sub.add_parser("list-tasks", help="List tasks")
    sp.add_argument("--project")
    sp.add_argument("--milestone")
    sp.add_argument("--owner")
    sp.add_argument("--status", choices=["Pending","In Progress","Paused","Completed","Canceled"])
    sp.set_defaults(func=list_tasks)

    sp = sub.add_parser("import-csv", help="Import/replace from CSV")
    sp.add_argument("--csv", required=True)
    sp.add_argument("--replace", action="store_true", help="Delete all then import")
    sp.set_defaults(func=import_csv)

    args = ap.parse_args()
    if not args.cmd:
        ap.print_help(); sys.exit(1)
    args.func(args)

if __name__ == "__main__":
    main()
