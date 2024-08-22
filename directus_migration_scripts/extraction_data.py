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
directus_collection = f"{directus_instance}/items/Lab_Extracts/?limit=-1"
response = session.get(url=directus_collection)

# Extract data
data = response.json()["data"]

# Convert data to a dataframe
df = pd.DataFrame(data)

# Rename columns to match new directus instance
new_column_names = {
    "UUID_lab_extract": "uuid_extraction",
    "status": "status",
    "dried_weight": "dried_weight",
    "dried_weight_unit": "dried_weight_unit",
    "solvent_volume": "solvent_volume",
    "solvent_volume_unit": "solvent_volume_unit",
    "extraction_method": "extraction_method",
    # "batch_id": "batch",
}
df.rename(columns=new_column_names, inplace=True)

# Attribute values to some columns (22 = milligram and 18 = microliter)
df["dried_weight_unit"] = 22
df["solvent_volume_unit"] = 18


# Replace old blanks names by new ones
def assign_values_blk(row: pd.Series) -> Any:
    if row["lab_extract_id"].startswith("dbgi_batch_blk_"):
        return row["lab_extract_id"].replace("dbgi_batch_", "")
    else:
        return row["lab_extract_id"]


# Apply the function to each row
df["container_id"] = df.apply(assign_values_blk, axis=1)


# Move old weights column to new one
def assign_values_weight(row: pd.Series) -> Any:
    if row["dried_plant_weight"] is not None:
        return row["dried_plant_weight"]
    else:
        return row["dried_weight"]


# Apply the function to each row
df["dried_weight"] = df.apply(assign_values_weight, axis=1)


# Move old volume column to new one
def assign_values_volume(row: pd.Series) -> Any:
    if row["solvent_volume_micro"] is not None:
        return row["solvent_volume_micro"]
    else:
        return row["solvent_volume"]


# Apply the function to each row
df["solvent_volume"] = df.apply(assign_values_volume, axis=1)


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
df["parent_sample_container"] = df["field_sample_id"].apply(get_primary_key_parent)


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
df["sample_container"] = df["container_id"].apply(get_primary_key_sample)


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


# Function to get mobile containers primary keys
def get_primary_key_batch(sample_code: str) -> Optional[int]:
    params = {
        "filter[old_id][_eq]": sample_code,
        "fields": "id",
    }
    # Create a session object for making requests
    session = requests.Session()
    response = session.get("https://emi-collection.unifr.ch/directus-test/items/Batches/", params=params)
    if response.status_code == 200:
        data = response.json()
        if data["data"]:
            return int(data["data"][0]["id"])
        else:
            return None
    else:
        return None


# Apply the function to retrieve primary keys and store them in a new column
df["batch"] = df["batch_id"].apply(get_primary_key_batch)


# Function to get mobile containers primary keys
def get_primary_key_method(sample_code: str) -> Optional[int]:
    params = {
        "filter[method_name][_eq]": sample_code,
        "fields": "id",
    }
    # Create a session object for making requests
    session = requests.Session()
    response = session.get("https://emi-collection.unifr.ch/directus-test/items/Extraction_Methods/", params=params)
    if response.status_code == 200:
        data = response.json()
        if data["data"]:
            return int(data["data"][0]["id"])
        else:
            return None
    else:
        return None


# Apply the function to retrieve primary keys and store them in a new column
df["extraction_method"] = df["extraction_method"].apply(get_primary_key_method)

# Delete useless columns
df.drop(
    [
        "field_sample_id",
        "lab_extract_id",
        "mobile_container_id",
        "dried_plant_weight",
        "solvent_volume_micro",
        "user_created",
        "date_created",
        "user_updated",
        "date_updated",
        "batch_id",
        "container_id",
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
    directus_collection = f"{directus_instance}/items/Extraction_Data/"
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
