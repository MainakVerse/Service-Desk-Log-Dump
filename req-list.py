import requests
import pandas as pd
import json

access_token = "1000.344320cb2619b9d8fb37639688625083.9a63c73838d2cae6efad7765bb152934"
base_url = "https://servicedesk.regalcream.com.au/app/itdesk/api/v3/requests"

headers = {
    "Authorization": f"Zoho-oauthtoken {access_token}",
    "Content-Type": "application/json",
    "Accept": "application/vnd.manageengine.v3+json"
}

all_requests = []
page = 1
max_pages = 100

while page <= max_pages:
    list_info = {
        "list_info": {
            "row_count": "100",
            "page": str(page),
            "sort_field": "created_time",
            "sort_order": "desc",
            "search_criteria": {
                "field": "created_time",
                "condition": "after",
                "value": "2024-06-01 00:00:00"
            }
        }
    }
    
    params = {"input_data": json.dumps(list_info)}
    response = requests.get(base_url, headers=headers, params=params)

    if response.status_code != 200:
        print(f"❌ Failed at page {page}: {response.status_code}")
        print("Response:", response.text)
        break

    data = response.json()
    requests_list = data.get("requests", [])

    # Only keep BI group requests
    filtered_requests = [
        req for req in requests_list
        if isinstance(req.get("group"), dict) and req["group"].get("name") == "BI"
    ]

    if not filtered_requests:
        print(f"⚠️ No BI requests found on page {page}")
        page += 1
        continue

    # Extract required fields
    for req in filtered_requests:
        row = {
            "task_id": req.get("id"),
            "request_id": req.get("requester", {}).get("id"),
            "title": req.get("title"),
            "created_date": req.get("created_time"),
            "module": req.get("module"),
            "scheduled_start_time": req.get("scheduled_start_time"),
            "scheduled_end_time": req.get("scheduled_end_time"),
            "actual_start_time": req.get("actual_start_time"),
            "actual_end_time": req.get("actual_end_time"),
            "status_id": req.get("status", {}).get("id"),
            "status_name": req.get("status", {}).get("name"),
            "group_name": req.get("group", {}).get("name"),
            "group_id": req.get("group", {}).get("id"),
            "owner_Email": req.get("owner", {}).get("email_id"),
            "owner_Name": req.get("owner", {}).get("name"),
            "owner_id": req.get("owner", {}).get("id"),
            "CreatedBy_Email": req.get("created_by", {}).get("email_id"),
            "CreatedBy_Name": req.get("created_by", {}).get("name"),
            "CreatedBy_id": req.get("created_by", {}).get("id"),
            "Priority_name": req.get("priority", {}).get("name"),
            "percentage_completion": req.get("percentage_completion"),
            "Estimated_effort_days": req.get("estimated_effort", {}).get("days"),
            "task_type_name": req.get("task_type", {}).get("name"),
            "Task_order": req.get("task_order"),
            "overdue": req.get("overdue"),
            "ticket_id": req.get("ticket_id"),
            "subject": req.get("subject"),
            "description": req.get("description"),
            "site_id": req.get("site", {}).get("id"),
            "site_name": req.get("site", {}).get("name"),
        }
        all_requests.append(row)

    print(f"✅ Extracted {len(filtered_requests)} BI requests from page {page}")
    page += 1

# Convert to DataFrame and save
df = pd.DataFrame(all_requests)
df.to_csv("sdp_requests_latest.csv", index=False, encoding="utf-8-sig")
print(f"✅ Saved {len(all_requests)} BI requests with selected fields to 'sdp_requests_latest.csv'")
