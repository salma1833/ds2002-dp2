from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json
# Step 1-3: connect to MongoDB Atlas
MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.o3xrg4c.mongodb.net/"
client = MongoClient(uri, username='sa2qt', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)

# Specify a database
db = client.project2db
# Specify a collection
collection = db.project2collection

# Step 4: Listing directory contents
# Listing directory
directory = "data"

for filename in os.listdir(directory):
    with open(os.path.join(directory, filename)) as f:
        # Step 5-6: Importing & Error handling
        try:
            file_data = json.load(f)
        except Exception as e:
            print(e, "error when loading", f)
            continue

        if isinstance(file_data, list):
            try:
                collection.insert_many(file_data)
            except Exception as e:
                print(e, "when importing into Mongo")
        else:
            try:
                collection.insert_one(file_data)
            except Exception as e:
                print(e)
