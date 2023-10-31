#!/usr/bin/env python3

import settings
import pymongo

if __name__ == "__main__":
    try:
        conn = pymongo.MongoClient(settings.URI_STRING)
        print("Connected to HOT MongoDB")
        db = conn[settings.DB_NAME]
        collection = db[settings.COLLECTION_NAME]
        print("Counting HOT: ", collection.count_documents({}))

        conn2 = pymongo.MongoClient(settings.URI_ARCHIVE_STRING)
        print("Connected to COLD MongoDB")
        db2 = conn2[settings.DB_NAME]
        collection2 = db2[settings.COLLECTION_NAME]
        print("Counting COLD: ", collection2.count_documents({}))

    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)
    conn.close()
    conn2.close()
