import pandas as pd
import random
from datetime import datetime, timedelta

projects = [
    "Website Redesign",
    "Mobile App Launch",
    "Cloud Migration",
    "CRM Upgrade",
    "E-commerce Expansion",
    "Data Warehouse Build"
]

users = ["Alice", "Bob", "Charlie", "David", "Eve"]
priorities = ["High", "Medium", "Low"]

start_base = datetime(2025, 8, 10)
task_counter = 1
data = []

for project in projects:
    for milestone_num in range(1, 5):  # 4 milestones per project (6 projects * 4 = 24 milestones)
        milestone_name = f"Milestone {milestone_num}"
        milestone_task_ids = []
        prev_task_id = None
        for task_num in range(1, 3 + random.randint(0, 2)):  # 3-5 tasks per milestone
            task_id = f"T{task_counter}"
            depends_on = prev_task_id if prev_task_id and random.choice([True, False]) else ""
            est_hours = random.randint(4, 20)
            priority = random.choice(priorities)
            start_date = start_base + timedelta(days=random.randint(0, 30))
            due_date = start_date + timedelta(days=random.randint(1, 7))
            owner = random.choice(users)

            data.append([
                project,
                milestone_name,
                f"Task {task_counter} for {milestone_name}",
                task_id,
                depends_on,
                est_hours,
                priority,
                start_date.strftime("%Y-%m-%d"),
                due_date.strftime("%Y-%m-%d"),
                owner
            ])

            milestone_task_ids.append(task_id)
            prev_task_id = task_id
            task_counter += 1

columns = ["Project", "Milestone", "Task", "TaskID", "DependsOn", "EstimatedHours", "Priority", "StartDate", "DueDate", "Owner"]

df = pd.DataFrame(data, columns=columns)

output_path = "project_tasks_template.csv"
df.to_csv(output_path, index=False)
print(f"Sample CSV template with {len(projects)} projects, {df['Milestone'].nunique()} milestones, and {len(df)} tasks saved to {output_path}")
