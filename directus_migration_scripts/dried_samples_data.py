import pandas as pd
import requests

# Create a session object for making requests
session = requests.Session()

# Collect data from old directus instance
directus_instance = "https://emi-collection.unifr.ch/directus"
directus_collection = f"{directus_instance}/items/Field_Samples/?limit=10"
response = session.get(url=directus_collection)

# Extract data
data = response.json()["data"]

# Convert data to a dataframe
df = pd.DataFrame(data)

# Rename columns to match new directus instance
new_column_names = {
    "UUID_field_sample": "uuid_dried_sample",
    "field_sample_id": "sample_container",
    "mobile_container_id": "parent_container",
    "status": "status",
    "user_created": "user_created",
    "date_created": "date_created",
    "user_updated": "user_updated",
    "date_updated": "date_updated",
}
df.rename(columns=new_column_names, inplace=True)

# Delete useless columns
df.drop(["qfield_link", "inaturalist_link", "inat_observation_id"], axis=1, inplace=True)
print(df)
