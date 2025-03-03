import requests, time, random
import os
import json
from datetime import datetime

HUBSPOT_API_KEY = os.getenv("RevOps")
BASE_URL = "https://api.hubapi.com"
ASSOCIATIONS_URL = lambda contact_id: f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}/associations/meetings"
MEETINGS_URL = f"{BASE_URL}/crm/v3/objects/meetings/batch/read"

def get_associated_meetings(contact_id):
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }
    delay1 = random.uniform(1,5)
    time.sleep(delay1)
    response = requests.get(ASSOCIATIONS_URL(contact_id), headers=headers)
    if response.status_code != 200:
        raise Exception(f"Error fetching associations: {response.status_code}, {response.text}")
    data = response.json()
    return [assoc["id"] for assoc in data.get("results", [])]

def get_meeting_details(meeting_ids):
    if not meeting_ids:
        return []
    headers = {
        "Authorization": f"Bearer {HUBSPOT_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "properties": ["hs_timestamp", "hs_activity_type"],
        "inputs": [{"id": meeting_id} for meeting_id in meeting_ids]
    }
    delay2 = random.uniform(1,5)
    time.sleep(delay2)
    response = requests.post(MEETINGS_URL, headers=headers, json=payload)
    if response.status_code != 200:
        raise Exception(f"Error fetching meeting details: {response.status_code}, {response.text}")
    data = response.json()
    if not data or "results" not in data:
        return []
    meetings = []
    for meeting in data["results"]:
        properties = meeting.get("properties", {})
        timestamp_str = properties.get("hs_timestamp")
        activity_type = properties.get("hs_activity_type", "")
        if activity_type and "Zeni Overview - " in activity_type:
            try:
                timestamp_unix = int(datetime.fromisoformat(timestamp_str.replace("Z", "+00:00")).timestamp()) if timestamp_str else 0
            except (ValueError, TypeError):
                timestamp_unix = 0
            meetings.append({"id": meeting.get("id"), "timestamp": timestamp_unix})
    meetings_sorted = sorted(meetings, key=lambda x: x["timestamp"], reverse=True)
    return [meeting["id"] for meeting in meetings_sorted]

def main(event):
    contact_id = event.get("inputFields").get("cId")
    try:
        meeting_ids = get_associated_meetings(contact_id)
        sorted_meeting_ids = get_meeting_details(meeting_ids)
        if len(sorted_meeting_ids) > 0:
          successID = sorted_meeting_ids[0]
          return {
            "outputFields": {
              "mid": successID
            }
          }
        else:
          return {
            "outputFields": {
              "mid": 0
            }
          }
    except Exception as e:
        print("Error:", str(e))
        return {
            "outputFields": {
              "mid": 0
            }
        }