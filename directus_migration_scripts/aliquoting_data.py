# Ready, uncomment 1 post request

import os
from typing import Any, Optional

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
directus_collection = f"{directus_instance}/items/Aliquots/?limit=-1"
response = session.get(url=directus_collection)

# Extract data
data = response.json()["data"]

# Convert data to a dataframe
df = pd.DataFrame(data)

# Rename columns to match new directus instance
new_column_names = {"UUID_aliquot": "uuid_aliquot", "aliquot_volume_microliter": "aliquot_volume", "status": "status"}
df.rename(columns=new_column_names, inplace=True)

# Attribute values to some columns (18 = microliter)
df["aliquot_volume_unit"] = 18


# Replace lab extracts old blanks names by new ones
def assign_values_blk_ext(row: pd.Series) -> Any:
    if row["lab_extract_id"].startswith("dbgi_batch_blk_"):
        return row["lab_extract_id"].replace("dbgi_batch_", "")
    else:
        return row["lab_extract_id"]


# Apply the function to each row
df["lab_extract_id"] = df.apply(assign_values_blk_ext, axis=1)


# Replace aliquots old blanks names by new ones
def assign_values_blk_al(row: pd.Series) -> Any:
    if row["aliquot_id"].startswith("dbgi_batch_blk_"):
        return row["aliquot_id"].replace("dbgi_batch_", "")
    else:
        return row["aliquot_id"]


# Apply the function to each row
df["aliquot_id"] = df.apply(assign_values_blk_al, axis=1)


# Function to get parent sample containers primary keys
def get_primary_key_parent(sample_code: str) -> Optional[int]:
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
df["parent_sample_container"] = df["lab_extract_id"].apply(get_primary_key_parent)


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
df["sample_container"] = df["aliquot_id"].apply(get_primary_key_sample)


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
        "aliquot_id",
        "lab_extract_id",
        "mobile_container_id",
        "user_created",
        "date_created",
        "user_updated",
        "date_updated",
    ],
    axis=1,
    inplace=True,
)

print(df.columns)

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
    directus_collection = f"{directus_instance}/items/Aliquoting_Data/"
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
