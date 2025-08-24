import requests
import concurrent.futures
import csv
import json

# Replace with your actual Zoho OAuth access token
access_token = "1000.4e8e1e2dc207bedb803f387ef1142e05.4037e4c1ec77fc5380ba60badfd71dfd"
REQUEST_IDS_FILE = 'sdp_requests_latest.csv'   # Input file containing request IDs
OUTPUT_FILE = 'worklogs.csv'                   # Final output

BASE_URL = "https://servicedeskplus.net.au/app/itdesk/api/v3/requests"

def read_request_ids_from_csv(filename):
    """ Reads request IDs from CSV (assumed first column). """
    request_ids = []
    try:
        with open(filename, mode='r', newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader, None)  # skip header
            for row in reader:
                if row and row[0].isdigit():
                    request_ids.append(int(row[0]))
    except FileNotFoundError:
        print(f"Error: The file '{filename}' was not found.")
    except Exception as e:
        print(f"CSV read error: {e}")
    return request_ids


def fetch_and_process_worklogs(request_id):
    try:
        url = f"{BASE_URL}/{request_id}/worklogs"
        headers = {
            "Accept": "application/vnd.manageengine.sdp.v3+json",
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        params = {
            "input_data": json.dumps({
                "list_info": {
                    "row_count": "100",
                    "start_index": "1",
                    "sort_field": "created_time",
                    "sort_order": "desc"
                }
            })
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        # Debug: print the structure of response for strange cases
        if not isinstance(data, dict):
            print(f"API for request {request_id} did not return a dict: {type(data)}, value:\n{data}")
            return []

        worklogs = data.get("worklogs", [])
        if not isinstance(worklogs, list):
            print(f"Worklogs for {request_id} is not a list: {type(worklogs)}\n{worklogs}")
            return []

        processed = []
        for wl in worklogs:
            if not isinstance(wl, dict):
                print(f"Worklog for request {request_id} is not a dict:\n{wl}")
                continue
            flat = {
                "request_id": request_id,
                "worklog_id": wl.get("id"),
                "description": wl.get("description"),
                "owner_name": wl.get("owner", {}).get("name"),
                "start_time": wl.get("start_time", {}).get("display_value"),
                "end_time": wl.get("end_time", {}).get("display_value"),
                "time_spent": wl.get("time_spent", {}).get("value"),
                "total_charge": wl.get("total_charge", {}).get("value"),
                "created_by": wl.get("created_by", {}).get("name"),
                "recorded_time": wl.get("recorded_time", {}).get("display_value"),
            }
            processed.append(flat)

        print(f"✓ Worklogs fetched for request {request_id}: {len(processed)} records")
        return processed

    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP error for {request_id}: {e.response.status_code} - {e.response.text}")
    except Exception as ex:
        print(f"❌ Unexpected error for {request_id}: {ex}")
    return []

    """ Fetch worklogs for a single request ID. """
    try:
        url = f"{BASE_URL}/{request_id}/worklogs"
        headers = {
            "Accept": "application/vnd.manageengine.sdp.v3+json",
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        params = {
            "input_data": json.dumps({
                "list_info": {
                    "row_count": "100",  # up to 100 per call
                    "start_index": "1",  # API pagination starts at 1
                    "sort_field": "created_time",
                    "sort_order": "desc"
                }
            })
        }

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("response_status", {}).get("status") != "success":
            print(f"API returned error for request {request_id}: {data}")
            return []

        worklogs = data.get("worklogs", [])
        
        processed = []
        for wl in worklogs:
            flat = {
                "request_id": request_id,
                "worklog_id": wl.get("id"),
                "description": wl.get("description"),
                "owner_name": wl.get("owner", {}).get("name"),
                "start_time": wl.get("start_time", {}).get("display_value"),
                "end_time": wl.get("end_time", {}).get("display_value"),
                "time_spent": wl.get("time_spent", {}).get("value"),
                "total_charge": wl.get("total_charge", {}).get("value"),
                "created_by": wl.get("created_by", {}).get("name"),
                "recorded_time": wl.get("recorded_time", {}).get("display_value"),
            }
            processed.append(flat)

        print(f"✓ Worklogs fetched for request {request_id}: {len(processed)} records")
        return processed

    except requests.exceptions.HTTPError as e:
        print(f"❌ HTTP error for {request_id}: {e.response.status_code} - {e.response.text}")
    except Exception as ex:
        print(f"❌ Unexpected error for {request_id}: {ex}")
    return []


def write_to_csv(data, filename):
    """ Write JSON list[dict] to CSV flat file. """
    if not data:
        print("⚠ No worklog data to write.")
        return

    headers = sorted({key for d in data for key in d.keys()})

    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(data)

    print(f"✅ Wrote {len(data)} worklogs into {filename}")


if __name__ == "__main__":
    request_ids = read_request_ids_from_csv(REQUEST_IDS_FILE)

    if not request_ids:
        print("No request IDs found to fetch. Exiting.")
    else:
        all_worklogs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(fetch_and_process_worklogs, rid) for rid in request_ids]
            for future in concurrent.futures.as_completed(futures):
                result = future.result()
                if result:
                    all_worklogs.extend(result)

        write_to_csv(all_worklogs, OUTPUT_FILE)
