#!/usr/bin/env python3

import settings
import json
import time
import datetime
import requests
from requests.auth import HTTPDigestAuth

urlCreate = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + \
    settings.PROJECT_ID + "/clusters"
urlStatus = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + \
    settings.PROJECT_ID + "/clusters/" + settings.CLUSTER_NAME

headers = {
    'Content-Type': 'application/json',
}

data = {
    "name": settings.CLUSTER_NAME,
    "clusterType": "REPLICASET",
    "mongoDBMajorVersion": "7.0",
    "numShards": 1,
    "providerSettings": {
        "providerName": "AWS",
        "regionName": "US_WEST_2",
        "instanceSizeName": "M10",
        "encryptEBSVolume": True
    },
    "replicationFactor": 3,
    "providerBackupEnabled": False,
    "autoScaling": {"diskGBEnabled": True}
}

print("#1 CREATING AN ATLAS CLUSTER")

t1 = datetime.datetime.now()
print(t1, " - Creating cluster: ", settings.CLUSTER_NAME)
response = requests.post(urlCreate, headers=headers, data=json.dumps(data),
                         auth=HTTPDigestAuth(settings.PUBLIC_KEY, settings.PRIVATE_KEY))
if response.status_code != 201:
    print('Error:')
    print(response.json())
else:
    timeout = time.time() + 60*15  # 15 minute timeout
    while True:
        response = requests.get(urlStatus, auth=HTTPDigestAuth(
            settings.PUBLIC_KEY, settings.PRIVATE_KEY))
        stateName = response.json()["stateName"]
        t2 = datetime.datetime.now()
        print(t2, " - Cluster status: ", stateName)

        if stateName == "IDLE":
            print("Cluster created in: ", t2-t1)
            break
        if time.time() > timeout:
            print("TIMEOUT: Cluster creation is still in progress")
            print("Please go to the MongoDB Atlas UI.")
            break
        time.sleep(5)
