# Ready, uncomment 1 post request

import os
from typing import Optional

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
directus_collection = f"{directus_instance}/items/Field_Samples/?limit=-1"
response = session.get(url=directus_collection)

# Extract data
data = response.json()["data"]

# Convert data to a dataframe
df = pd.DataFrame(data)

# Delete old blank placeholder
df = df.drop(df.index[0])

# Rename columns to match new directus instance
new_column_names = {
    "UUID_field_sample": "uuid_dried_sample",
    # "field_sample_id": "sample_container",
    # "mobile_container_id": "parent_container",
    "status": "status",
    "user_created": "user_created",
    "date_created": "date_created",
    "date_updated": "date_updated",
}
df.rename(columns=new_column_names, inplace=True)


# Function to get sample containers primary keys
def get_primary_key_sample(sample_code: str) -> Optional[int]:
    params = {
        "filter[container_id][_eq]": sample_code,
        "fields": "id",
    }
    # Create a session object for making requests
    session = requests.Session()
    response = session.get("https://emi-collection.unifr.ch/directus-test/items/Containers/", params=params)
    if response.status_code == 200:
        data = response.json()
        if data["data"]:
            return int(data["data"][0]["id"])
        else:
            return None
    else:
        return None


# Apply the function to retrieve primary keys and store them in a new column
df["sample_container"] = df["field_sample_id"].apply(get_primary_key_sample)


# Function to get mobile containers primary keys
def get_primary_key_mobile(sample_code: str) -> Optional[int]:
    params = {
        "filter[old_id][_eq]": sample_code,
        "fields": "id",
    }
    # Create a session object for making requests
    session = requests.Session()
    response = session.get("https://emi-collection.unifr.ch/directus-test/items/Containers/", params=params)
    if response.status_code == 200:
        data = response.json()
        if data["data"]:
            return int(data["data"][0]["id"])
        else:
            return None
    else:
        return None


# Apply the function to retrieve primary keys and store them in a new column
df["parent_container"] = df["mobile_container_id"].apply(get_primary_key_mobile)

# Delete useless columns
df.drop(
    [
        "qfield_link",
        "inaturalist_link",
        "inat_observation_id",
        "user_updated",
        "field_sample_id",
        "mobile_container_id",
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
response = session.post(login_url, json={"email": DIRECTUS_EMAIL, "password": DIRECTUS_PASSWORD})
# Test if connection is successful
if response.status_code == 200:
    # Stores the access token
    data = response.json()["data"]
    access_token = data["access_token"]
    print("connection established")

    directus_instance = "https://emi-collection.unifr.ch/directus-test"
    directus_collection = f"{directus_instance}/items/Dried_Samples_Data/"
    headers = {"Content-Type": "application/json"}

    records = df.to_json(orient="records")

    # Add the codes to the database
    session.headers.update({"Authorization": f"Bearer {access_token}"})
    # response = session.post(url=directus_collection, headers=headers, data=records)
    if response.status_code == 200:
        print("data correctly entered")
    else:
        print(response.status_code)
        print(response.text)

# If connection to directus failed
else:
    print("connection error")
