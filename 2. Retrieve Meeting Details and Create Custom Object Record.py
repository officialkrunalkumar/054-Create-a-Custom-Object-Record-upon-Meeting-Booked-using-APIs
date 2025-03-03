import os, requests, json, re, time, random
from hubspot import HubSpot
from pprint import pprint
from hubspot.crm.owners import ApiException

def main(event):
  mid = event.get("inputFields").get("mid") #73568429076 74096214030 
  token = os.getenv("RevOps")
  email = event.get("inputFields").get("email")
  domain = event.get("inputFields").get("domain")
  setter = event.get("inputFields").get("setter")
  disqualified_reason = event.get("inputFields").get("disqualifiedReason")
  url = f"https://api.hubapi.com/engagements/v1/engagements/{mid}"
  headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
  }
  delay1 = random.uniform(1,5)
  time.sleep(delay1)
  response = requests.get(url, headers=headers)
  if response.status_code == 200:
    meeting_data = response.json()
    print("Meeting Data Retrieved Successfully!")
    print(meeting_data)
    engagement = meeting_data["engagement"]
    metadata = meeting_data["metadata"]
    #print("Engagement = ", engagement)
    #print("Metadata = ", metadata)
    startTime = metadata["startTime"]
    endTime = metadata["endTime"]
    recordId = mid
    title = metadata["title"]
    outcome = metadata["meetingOutcome"]
    mtype = engagement["activityType"]
    activityDate = engagement["timestamp"]
    summary = metadata["body"]
    summary = re.sub(r'<[^>]+>', '', summary)
    assignee = engagement["ownerId"]
    if "BDR - " in summary:
      temp = summary.split("BDR - ")
      temp2 = temp[1].split("\n")
      settername = temp2[0]
      if settername == "Alexys Brown":
        settername = "Aly Brown"
      if settername == "Clara Fowler":
        settername = "Clara Bush"
      if settername == "Trebor Hogue":
        settername = "TJ Hogue"
      urlname = "https://api.hubapi.com/crm/v3/objects/users/search"
      payloadname = {
          "properties": ["hs_email"],
          "filterGroups": [
              {
                  "filters": [
                      {
                          "propertyName": "hs_searchable_calculated_name",
                          "operator": "EQ",
                          "value": settername
                      }
                  ]
              }
          ],
          "limit": 1
      }
      headersname = {
          'accept': "application/json",
          'content-type': "application/json",
          'authorization': f"Bearer {token}"
      }
      responsename = requests.post(urlname, data=json.dumps(payloadname), headers=headersname)
      response_data = responsename.json()
      if 'results' in response_data and len(response_data['results']) > 0:
          email_id = response_data['results'][0]['properties'].get('hs_email', 'none')
          try:
            client = HubSpot(access_token=os.getenv('RevOps'))
            api_response = client.crm.owners.owners_api.get_page(email=email_id, limit=1, archived=False)
            setter = api_response.results[0].id
          except ApiException as e:
            setter = ""
            print("Exception when calling owners_api->get_page: %s\n" % e)
      else:
          setter = ""
    else:
        setter = assignee
  
    custom_object_id = "2-39538272"
    record_created = 0
    url = "https://api.hubapi.com/crm/v3/objects"
    custom_object_data = {
        "properties": {
            "meeting_record_id": title,
            "meeting_date": activityDate,
            "setter": setter,
            "outcome": outcome,
            "meeting_summary": summary,
            "meeting_start_time": startTime,
            "meeting_end_time": endTime,
            "disqualified_reason": disqualified_reason,
            "meeting_title": recordId,
            "meeting_type": mtype,
            "contact_email": email,
            "company_domain": domain
        }
    }
  
    delay2 = random.uniform(1,5)
    time.sleep(delay2)
    
    response = requests.post(f"{url}/{custom_object_id}", headers=headers, json=custom_object_data)
    print(response.json())
    
    if response.status_code == 201:
        record_created = 1
    else:
        record_created = 0
    return {
        "outputFields": {
            "RecordCreated": record_created
        }
    }
  else:
    record_created = 0
    return {
        "outputFields": {
            "RecordCreated": record_created
        }
    }