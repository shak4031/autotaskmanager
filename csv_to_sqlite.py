# csv_to_sqlite.py
import sqlite3, csv, sys, os

CSV_PATH = sys.argv[1] if len(sys.argv) > 1 else "project_tasks_with_comments.csv"
DB_PATH  = sys.argv[2] if len(sys.argv) > 2 else "tasks.db"

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;

CREATE TABLE IF NOT EXISTS tasks (
  TaskID TEXT PRIMARY KEY,
  Project TEXT NOT NULL,
  Milestone TEXT NOT NULL,
  Task TEXT NOT NULL,
  DependsOn TEXT,                  -- pipe-separated IDs for now (you can normalize later)
  EstimatedHours REAL NOT NULL,
  Priority TEXT NOT NULL,          -- High/Medium/Low
  StartDate TEXT NOT NULL,         -- ISO yyyy-mm-dd
  DueDate TEXT NOT NULL,           -- ISO yyyy-mm-dd
  Owner TEXT NOT NULL,

  Status TEXT DEFAULT 'Pending',   -- Pending/In Progress/Paused/Completed/Canceled
  ActualHours REAL DEFAULT 0.0,
  ActualSeconds INTEGER DEFAULT 0,
  InProgressStart TEXT,            -- ISO timestamp while in progress

  LastComment TEXT,
  CommentLog TEXT,
  LastUpdated TEXT
);

-- Helpful indexes (reads & filters)
CREATE INDEX IF NOT EXISTS idx_tasks_owner ON tasks(Owner);
CREATE INDEX IF NOT EXISTS idx_tasks_project ON tasks(Project);
CREATE INDEX IF NOT EXISTS idx_tasks_milestone ON tasks(Milestone);
CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(Status);
"""

def main():
  if not os.path.exists(CSV_PATH):
    print(f"CSV not found: {CSV_PATH}")
    sys.exit(1)

  conn = sqlite3.connect(DB_PATH)
  conn.executescript(SCHEMA)

  # Optional: clear existing rows before re-import
  conn.execute("DELETE FROM tasks")

  with open(CSV_PATH, newline='', encoding="utf-8") as f:
    reader = csv.DictReader(f)
    rows = list(reader)

  cols = ["TaskID","Project","Milestone","Task","DependsOn","EstimatedHours","Priority",
          "StartDate","DueDate","Owner","Status","ActualHours","LastComment",
          "CommentLog","LastUpdated","ActualSeconds","InProgressStart"]

  # fill missing optional columns with defaults
  for r in rows:
    r.setdefault("Status","Pending")
    r.setdefault("ActualHours","0")
    r.setdefault("ActualSeconds","0")
    r.setdefault("InProgressStart","")

  placeholders = ",".join(["?"]*len(cols))
  sql = f"INSERT INTO tasks ({','.join(cols)}) VALUES ({placeholders})"

  data = []
  for r in rows:
    data.append([r.get(c, "") for c in cols])

  with conn:
    conn.executemany(sql, data)

  print(f"Imported {len(rows)} rows into {DB_PATH}")
  conn.close()

if __name__ == "__main__":
  main()
