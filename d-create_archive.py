#!/usr/bin/env python3

import settings
import json
import time
import datetime
import requests
from requests.auth import HTTPDigestAuth

urlCreate = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + \
    settings.PROJECT_ID + "/clusters/" + settings.CLUSTER_NAME + "/onlineArchives"

headers = {
    'Content-Type': 'application/json',
}

data = {
    "collName": "student_grades",
    "collectionType": "STANDARD",
    "criteria": {
        "type": "DATE",
        "dateField": "date_completed",
        "expireAfterDays": 7
    },
    "dataProcessRegion": {
        "cloudProvider": "AWS",
        "region": "US_WEST_2"
    },
    "dbName": "education",
    "partitionFields": [
        {
            "fieldName": "date_completed",
            "order": 0
        },
        {
            "fieldName": "student_name.last",
            "order": 1
        },
        {
            "fieldName": "assignment_name",
            "order": 2
        }
    ],
    "paused": True,
    "schedule": {
        "type": "DEFAULT"
    }
}

print("#4 CREATING ONLINE ARCHIVE")
t1 = datetime.datetime.now()
print(t1, " - Creating online archive: ", settings.CLUSTER_NAME)
response = requests.post(urlCreate, headers=headers, data=json.dumps(data),
                         auth=HTTPDigestAuth(settings.PUBLIC_KEY, settings.PRIVATE_KEY))
if response.status_code != 200:
    print('Error: ', response.status_code)
    print(response.json())
else:
    archive_id = response.json()["_id"]
    print("Archive id: ", archive_id)

urlStatus2 = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + settings.PROJECT_ID + \
    "/clusters/" + settings.CLUSTER_NAME + "/onlineArchives/" + archive_id
timeout = time.time() + 60*15  # 15 minute timeout
while True:
    response = requests.get(urlStatus2, auth=HTTPDigestAuth(
        settings.PUBLIC_KEY, settings.PRIVATE_KEY))
    stateName = response.json()["state"]
    t2 = datetime.datetime.now()
    print(t2, " - Archive status: ", stateName)

    if stateName == "ACTIVE":
        print("Archive created in: ", t2-t1)
        break
    if time.time() > timeout:
        print("TIMEOUT: Archive creation is still in progress")
        print("Please go to the MongoDB Atlas UI.")
        break
    time.sleep(5)
