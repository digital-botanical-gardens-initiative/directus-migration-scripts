import requests

# Create a session object for making requests
session = requests.Session()

# Collection url
directus_instance = "http://134.21.20.118:8057"
directus_collection = f"{directus_instance}/items/Containers_Rules/?limit=1"

response = session.get(url=directus_collection)

data = response.json()["data"]
rule = data[0]["child_container"]

directus_instance = "http://134.21.20.118:8057"
directus_collection = f"{directus_instance}/items/Containers_Types/{rule}"

response = session.get(url=directus_collection)

data = response.json()["data"]
container_type = data["container_type"]
print(container_type)
