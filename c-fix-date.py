#!/usr/bin/env python3

import settings
import pymongo

def create_index(col):
    print("Creating index... ")
    resp = col.create_index([("date_completed", 1)])
    print("index response:", resp)


def alter_model(col):
    print("Calculating max date... ")
    docs = col.aggregate([
        {
            "$sort": {"date_completed": -1}
        },
        {
            "$limit": 1
        },
        {
            "$project": {
                "max_date_completed": "$date_completed",
                "_id": 0
            }
        }
    ])

    maxdate = None

    for doc in docs:
        maxdate = doc["max_date_completed"]

    pipeline = [
        {
            "$addFields": {
                "diff": {
                    "$dateDiff": {
                        "startDate": maxdate,
                        "endDate": "$$NOW",
                        "unit": "day"
                    }
                }
            }
        },
        {
            "$set": {
                "date_assigned": {
                    "$dateAdd": {
                        "startDate": "$date_assigned",
                        "unit": "day",
                        "amount": {"$subtract": ["$diff", 1]}
                    }
                },
                "date_completed":{
                    "$cond": {
                        "if": {"$eq": ["$status", "complete"]},
                        "then":{
                            "$dateAdd": {
                                "startDate": "$date_completed",
                                "unit": "day",
                                "amount": {"$subtract": ["$diff", 1]}
                            }
                        },
                        "else": None
                    }
                }
            }
        },
        {
            "$unset": "diff"
        }
    ]

    print("Executing pipeline... ")
    docs = col.update_many({}, pipeline)
    print(docs.modified_count)


if __name__ == "__main__":
    try:
        conn = pymongo.MongoClient(settings.URI_STRING)
        print("Connected to MongoDB")

        db = conn[settings.DB_NAME]
        collection = db[settings.COLLECTION_NAME]

        print("Creating index ...")
        create_index(collection)
        print("Operation completed successfully!!!")

        print("Fixing dates ...")
        alter_model(collection)
        print("Operation completed successfully!!!")

    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)
    conn.close()
