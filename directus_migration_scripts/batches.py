# Ready, uncomment 1 post request
import os

import pandas as pd
import requests
from dotenv import load_dotenv

# Loads environment variables
load_dotenv()

# Access the environment variables
DIRECTUS_EMAIL = os.getenv("DIRECTUS_EMAIL")
DIRECTUS_PASSWORD = os.getenv("DIRECTUS_PASSWORD")

# Create a session object for making requests
session = requests.Session()

# Collect data from old directus instance
directus_instance = "https://emi-collection.unifr.ch/directus"
directus_collection = f"{directus_instance}/items/Batch/?limit=-1"
response = session.get(url=directus_collection)

# Extract data
data = response.json()["data"]

# Convert data to a dataframe
df = pd.DataFrame(data)

# Rename columns to match new directus instance
new_column_names = {"UUID_batch": "uuid_batch", "batch_id": "old_id", "comments": "comments"}
df.rename(columns=new_column_names, inplace=True)

# Add new columns with associated values
df["status"] = "ok"
df["batch_type"] = 5
df["batch_id"] = df["old_id"].str.replace("dbgi_", "", regex=False)

# Delete useless columns
df.drop(
    [
        "user_created",
        "date_created",
        "user_updated",
        "date_updated",
        "Reserved",
    ],
    axis=1,
    inplace=True,
)

# Add data to new directus instance
# Define the Directus base URL
base_url = "https://emi-collection.unifr.ch/directus-test"

# Define the login endpoint URL
login_url = base_url + "/auth/login"
# Create a session object for making requests
session = requests.Session()
# Send a POST request to the login endpoint
# response = session.post(login_url, json={"email": DIRECTUS_EMAIL, "password": DIRECTUS_PASSWORD})
# Test if connection is successful
if response.status_code == 200:
    # Stores the access token
    data = response.json()["data"]
    access_token = data["access_token"]
    print("connection established")

    directus_instance = "https://emi-collection.unifr.ch/directus-test"
    directus_collection = f"{directus_instance}/items/Batches/"
    headers = {"Content-Type": "application/json"}

    records = df.to_json(orient="records")

    # Add the codes to the database
    session.headers.update({"Authorization": f"Bearer {access_token}"})
    response = session.post(url=directus_collection, headers=headers, data=records)
    if response.status_code == 200:
        print("data correctly entered")
    else:
        print(response.status_code)
        print(response.text)

# If connection to directus failed
else:
    print("connection error")
