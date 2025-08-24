import requests
import pandas as pd
from datetime import datetime

ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"  # must include SDPOnDemand.tasks.READ and SDPOnDemand.worklogs.READ
BASE = "https://servicedesk.regalcream.com.au/app/itdesk/api/v3"
HEADERS = {"Authorization": f"Zoho-oauthtoken {ACCESS_TOKEN}",
           "Accept": "application/vnd.manageengine.v3+json"}

src_csv = "sdp_requests_latest.csv"
df = pd.read_csv(src_csv)

# Detect column names
task_col = "task_id" if "task_id" in df.columns else ("TaskID" if "TaskID" in df.columns else None)
req_col  = "request_id" if "request_id" in df.columns else ("RequestID" if "RequestID" in df.columns else None)
if not task_col:
    raise ValueError("CSV must contain a 'task_id' or 'TaskID' column.")

# Map task -> request (for fallback)
task_to_req = {}
if req_col:
    for _, r in df[[task_col, req_col]].iterrows():
        t = r.get(task_col)
        q = r.get(req_col)
        if pd.notna(t):
            task_to_req[str(int(t))] = str(int(q)) if pd.notna(q) else None

unique_tasks = sorted({str(int(t)) for t in df[task_col].dropna()})

rows = []
for t in unique_tasks:
    # 1) Try task-only endpoint
    url = f"{BASE}/tasks/{t}/worklogs"
    resp = requests.get(url, headers=HEADERS)

    # 2) Fallback to request+task if non-200 and we have request_id
    if resp.status_code != 200 and req_col:
        req_id = task_to_req.get(t)
        if req_id:
            url = f"{BASE}/requests/{req_id}/tasks/{t}/worklogs"
            resp = requests.get(url, headers=HEADERS)

    if resp.status_code != 200:
        print(f"skip task {t}: {resp.status_code}")
        continue

    data = resp.json()
    worklogs = data.get("worklogs", [])
    for wl in worklogs:
        # time_spent can be dict or "HH:MM"
        ts = wl.get("time_spent")
        hrs = mins = None
        ts_str = None
        if isinstance(ts, dict):
            hrs = ts.get("hours")
            mins = ts.get("minutes")
            ts_str = f"{hrs}:{int(mins):02d}" if hrs is not None and mins is not None else None
        elif isinstance(ts, str) and ":" in ts:
            hh, mm = ts.split(":", 1)
            ts_str = ts
            hrs = int(hh) if hh.isdigit() else None
            mins = int(mm) if mm.isdigit() else None
        else:
            ts_str = ts  # leave as-is

        row = {
            "TaskID": t,
            "WorklogID": wl.get("id"),
            "WorklogType": wl.get("worklog_type", {}).get("name") if isinstance(wl.get("worklog_type"), dict) else wl.get("worklog_type"),
            "StartTime": wl.get("start_time"),
            "EndTime": wl.get("end_time"),
            "TimeSpent": ts_str,
            "Description": wl.get("description"),
            "IncludeNonOperationalHours": wl.get("include_non_operational_hours", wl.get("include_nonoperational_hours")),
            "OtherCharges": wl.get("other_charges", wl.get("additional_cost")),
            "CreatedBy": wl.get("created_by", {}).get("name") if isinstance(wl.get("created_by"), dict) else wl.get("created_by"),
            "Owner": wl.get("owner", {}).get("name") if isinstance(wl.get("owner"), dict) else wl.get("owner"),
            "LoadDateTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "time_spent_hours": hrs,
            "time_spent_minutes": mins
        }
        rows.append(row)

# Save
out_cols = [
    "TaskID","WorklogID","WorklogType","StartTime","EndTime","TimeSpent",
    "Description","IncludeNonOperationalHours","OtherCharges","CreatedBy",
    "Owner","LoadDateTime","time_spent_hours","time_spent_minutes"
]
pd.DataFrame(rows, columns=out_cols).to_csv("sdp_task_worklogs.csv", index=False, encoding="utf-8-sig")
print(f"Saved {len(rows)} worklogs -> sdp_task_worklogs.csv")
