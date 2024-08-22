import requests

# Create a session object for making requests
session = requests.Session()

# Collection url
directus_instance = "https://emi-collection.unifr.ch/directus"
directus_collection = f"{directus_instance}/items/University/?limit=1"

response = session.get(url=directus_collection)

data = response.json()["data"]
print(data)
