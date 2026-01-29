# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "jupyter",
# META     "jupyter_kernel_name": "python3.11"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import os
import requests
import json
import notebookutils
from sempy import fabric

FABRIC_API = "https://api.fabric.microsoft.com"
WORKSPACE_ID = fabric.get_workspace_id()
TIMEZONE = "AUS Eastern Standard Time"
START_DATE = os.environ.get("START_DATE", "2025-07-18T00:00:00Z")
END_DATE = os.environ.get("END_DATE", "2030-12-31T23:59:00Z")
TARGET_TIME = os.environ.get("TARGET_TIME", "06:30")
TARGET_DAYS = json.loads(os.environ.get("TARGET_DAYS", '["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]'))
PIPELINE_SCHEDULES = {
    "531d39e8-42cb-4007-84e5-ae1dcbaa4d01": TARGET_TIME
}

# You need to set up authentication and get a valid access token for Azure REST API
access_token = notebookutils.credentials.getToken('pbi')  # Set this securely

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}

for pipeline_id, desired_time in PIPELINE_SCHEDULES.items():
    print(f"🔍 Inspecting schedules for {pipeline_id}...")

    get_url = f"{FABRIC_API}/v1/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/jobs/Pipeline/schedules"
    response = requests.get(get_url, headers=headers)
    response.raise_for_status()
    schedules = response.json().get("value", [])

    matched_schedule = False

    for sched in schedules:
        sched_id = sched.get("id")
        sched_time = sched.get("configuration", {}).get("times", [""])[0]
        sched_weekdays = sched.get("configuration", {}).get("weekdays", [])
        sched_owner_type = sched.get("owner", {}).get("type")

        if (
            sched_time == desired_time and
            sched_weekdays == TARGET_DAYS and
            sched_owner_type == "ServicePrincipal"
        ):
            print(f"✅ Found matching schedule ({sched_id}) owned by SPN. Skipping creation.")
            matched_schedule = True
        else:
            print(f"🗑️ Removing schedule {sched_id} (Time: {sched_time}, Days: {sched_weekdays}, Owner: {sched_owner_type})")
            delete_url = f"{FABRIC_API}/v1/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/jobs/Pipeline/schedules/{sched_id}"
            requests.delete(delete_url, headers=headers)

    if not matched_schedule:
        print(f"📆 Creating schedule for {pipeline_id} at {desired_time} Brisbane time...")
        post_url = f"{FABRIC_API}/v1/workspaces/{WORKSPACE_ID}/items/{pipeline_id}/jobs/Pipeline/schedules"
        body = {
            "enabled": True,
            "configuration": {
                "startDateTime": START_DATE,
                "endDateTime": END_DATE,
                "localTimeZoneId": TIMEZONE,
                "type": "Weekly",
                "weekdays": TARGET_DAYS,
                "times": [desired_time]
            }
        }
        requests.post(post_url, headers=headers, data=json.dumps(body))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "jupyter_python"
# META }
