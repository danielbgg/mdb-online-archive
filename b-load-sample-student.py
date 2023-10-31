#!/usr/bin/env python3

import settings
import subprocess
import pymongo

if __name__ == "__main__":
    try:
        cmd = ['mongoimport', '--db=education', '--collection=student_grades',
               '--file=student_grades.json', '--jsonArray', '--uri', settings.URI_STRING]
        return_code = subprocess.run(cmd)
        print("Output of run() : ", return_code.returncode)

    except pymongo.errors.ConnectionFailure as e:
        print("Could not connect to MongoDB: %s" % e)
