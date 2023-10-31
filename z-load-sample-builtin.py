#!/usr/bin/env python3

import settings
import time
import datetime
import requests
from requests.auth import HTTPDigestAuth

headers = {
    'Content-Type': 'application/json',
}

# Loads the sample dataset into that cluster
print("#2 LOADING THE SAMPLE DATASET")
urlCreate2 = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + \
    settings.PROJECT_ID + "/sampleDatasetLoad/" + settings.CLUSTER_NAME

t1 = datetime.datetime.now()
print(t1, " - Loading sample dataset: ", settings.CLUSTER_NAME)
response = requests.post(urlCreate2, headers=headers,
                         auth=HTTPDigestAuth(settings.PUBLIC_KEY, settings.PRIVATE_KEY))
if response.status_code != 201:
    print('Error:')
    print(response.json())
else:
    datasetid = response.json()["_id"]
    print("Sample dataset id: ", datasetid)
    urlStatus2 = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + \
        settings.PROJECT_ID + "/sampleDatasetLoad/" + datasetid
    timeout = time.time() + 60*15  # 15 minute timeout
    while True:
        response = requests.get(urlStatus2, auth=HTTPDigestAuth(
            settings.PUBLIC_KEY, settings.PRIVATE_KEY))
        print(response.json())
        stateName = response.json()["state"]
        t2 = datetime.datetime.now()
        print(t2, " - Sample dataset status: ", stateName)

        if stateName == "COMPLETED":
            print("Sample dataset created in: ", t2-t1)
            break
        if time.time() > timeout:
            print("TIMEOUT: Sample datase creation is still in progress")
            print("Please go to the MongoDB Atlas UI.")
            break
        time.sleep(5)
