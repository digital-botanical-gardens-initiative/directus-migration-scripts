# Ready, uncomment 4 post requests to populate directus

import os
from typing import Any

import pandas as pd
import requests
from dotenv import load_dotenv

# Loads environment variables
load_dotenv()

# Access the environment variables
DIRECTUS_EMAIL = os.getenv("DIRECTUS_EMAIL")
DIRECTUS_PASSWORD = os.getenv("DIRECTUS_PASSWORD")

# _________________________________________________________Labels part______________________________________________________________

# Create a session object for making requests
session = requests.Session()

# Collect data from old directus instance
directus_instance = "https://emi-collection.unifr.ch/directus"
directus_collection = f"{directus_instance}/items/Labels/?limit=-1"
response = session.get(url=directus_collection)

# Extract data
data = response.json()["data"]

# Convert data to a dataframe
df_labels = pd.DataFrame(data)

# Rename columns to match new directus instance
new_column_names = {
    "field_sample_id": "container_id",
    "status": "status",
    "user_created": "user_created",
    "date_created": "date_created",
    "user_updated": "user_updated",
    "date_updated": "date_updated",
    "reserved": "reserved",
}
df_labels.rename(columns=new_column_names, inplace=True)

# Add some columns
df_labels["container_model"] = "1"
df_labels["status"] = "present"
df_labels["used"] = "False"
df_labels["is_finite"] = "True"
df_labels["columns"] = "1"
df_labels["columns_numeric"] = "True"
df_labels["rows"] = "1"
df_labels["rows_numeric"] = "True"

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
    directus_collection = f"{directus_instance}/items/Containers/"
    headers = {"Content-Type": "application/json"}

    records = df_labels.to_json(orient="records")

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


# _________________________________________________________Mobile Containers part______________________________________________________________

# Create a session object for making requests
session = requests.Session()

# Collect data from old directus instance
directus_instance = "https://emi-collection.unifr.ch/directus"
directus_collection = f"{directus_instance}/items/Mobile_Container/?limit=-1"
response = session.get(url=directus_collection)

# Extract data
data = response.json()["data"]

# Convert data to a dataframe
df_mobile = pd.DataFrame(data)

# Delete useless columns
df_mobile.drop(["container_location", "parent_container", "container_type"], axis=1, inplace=True)

# Delete old absent placeholder
df_mobile = df_mobile.drop(df_mobile.index[0])

# Rename columns to match new directus instance
new_column_names = {
    "container_id": "old_id",
    "user_created": "user_created",
    "date_created": "date_created",
    "user_updated": "user_updated",
    "date_updated": "date_updated",
    "UUID_container": "uuid_container",
    "reserved": "reserved",
}
df_mobile.rename(columns=new_column_names, inplace=True)

# Generate new container ids
df_mobile["container_id"] = [f"container_{i:06d}" for i in range(1, len(df_mobile) + 1)]


# Add some columns with content depending on the container type
def assign_values_mobile(row: pd.Series) -> pd.Series:
    if row["old_id"].startswith("container_8x3_"):
        return pd.Series(
            {
                "container_model": "6",
                "status": "present",
                "used": "False",
                "is_finite": "True",
                "columns": "8",
                "columns_numeric": "True",
                "rows": "3",
                "rows_numeric": "False",
            }
        )
    else:
        return pd.Series(
            {
                "container_model": "7",
                "status": "present",
                "used": "False",
                "is_finite": "True",
                "columns": "9",
                "columns_numeric": "True",
                "rows": "9",
                "rows_numeric": "False",
            }
        )


# Apply the function to each row
df_mobile[
    ["container_model", "status", "used", "is_finite", "columns", "columns_numeric", "rows", "rows_numeric"]
] = df_mobile.apply(assign_values_mobile, axis=1)

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
    directus_collection = f"{directus_instance}/items/Containers/"
    headers = {"Content-Type": "application/json"}

    records = df_mobile.to_json(orient="records")

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


# _________________________________________________________Lab Extracts part______________________________________________________________

# Create a session object for making requests
session = requests.Session()

# Collect data from old directus instance
directus_instance = "https://emi-collection.unifr.ch/directus"
directus_collection = f"{directus_instance}/items/Lab_Extracts/?limit=-1"
response = session.get(url=directus_collection)

# Extract data
data = response.json()["data"]

# Convert data to a dataframe
df_extract = pd.DataFrame(data)

# Delete useless columns
df_extract.drop(
    [
        "user_created",
        "date_created",
        "user_updated",
        "date_updated",
        "UUID_lab_extract",
        "field_sample_id",
        "dried_plant_weight",
        "extraction_method",
        "status",
        "batch_id",
        "solvent_volume_micro",
        "mobile_container_id",
        "dried_weight",
        "solvent_volume",
        "dried_weight_unit",
        "solvent_volume_unit",
    ],
    axis=1,
    inplace=True,
)


# Add some columns with content depending on the container type
def assign_values_ext(row: pd.Series) -> Any:
    if row["lab_extract_id"].startswith("dbgi_batch_blk_"):
        return row["lab_extract_id"].replace("dbgi_batch_", "")
    else:
        return row["lab_extract_id"]


# Apply the function to each row
df_extract["container_id"] = df_extract.apply(assign_values_ext, axis=1)

# Delete old id column
df_extract.drop("lab_extract_id", axis=1, inplace=True)

# Add the columns with good values for lab extracts
df_extract["container_model"] = 5
df_extract["used"] = True
df_extract["is_finite"] = True
df_extract["columns"] = 1
df_extract["columns_numeric"] = True
df_extract["rows"] = 1
df_extract["rows_numeric"] = True

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
    directus_collection = f"{directus_instance}/items/Containers/"
    headers = {"Content-Type": "application/json"}

    records = df_extract.to_json(orient="records")

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


# _________________________________________________________Aliquots part______________________________________________________________

# Create a session object for making requests
session = requests.Session()

# Collect data from old directus instance
directus_instance = "https://emi-collection.unifr.ch/directus"
directus_collection = f"{directus_instance}/items/Aliquots/?limit=-1"
response = session.get(url=directus_collection)

# Extract data
data = response.json()["data"]

# Convert data to a dataframe
df_aliquot = pd.DataFrame(data)

# Delete useless columns
df_aliquot.drop(
    [
        "user_created",
        "date_created",
        "user_updated",
        "date_updated",
        "UUID_aliquot",
        "lab_extract_id",
        "status",
        "aliquot_volume_microliter",
        "mobile_container_id",
    ],
    axis=1,
    inplace=True,
)

print(df_aliquot)
print(df_aliquot.columns)


# Add some columns with content depending on the container type
def assign_values(row: pd.Series) -> Any:
    if row["aliquot_id"].startswith("dbgi_batch_blk_"):
        return row["aliquot_id"].replace("dbgi_batch_", "")
    else:
        return row["aliquot_id"]


# Apply the function to each row
df_aliquot["container_id"] = df_aliquot.apply(assign_values, axis=1)

# Delete old id column
df_aliquot.drop("aliquot_id", axis=1, inplace=True)

# Add the columns with good values for lab extracts
df_aliquot["container_model"] = 5
df_aliquot["used"] = True
df_aliquot["is_finite"] = True
df_aliquot["columns"] = 1
df_aliquot["columns_numeric"] = True
df_aliquot["rows"] = 1
df_aliquot["rows_numeric"] = True

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
    directus_collection = f"{directus_instance}/items/Containers/"
    headers = {"Content-Type": "application/json"}

    records = df_aliquot.to_json(orient="records")

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
