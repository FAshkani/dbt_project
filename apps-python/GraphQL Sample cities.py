from azure.identity import DefaultAzureCredential
import requests
import json


# Acquire token
credential = DefaultAzureCredential()
token = credential.get_token("https://api.fabric.microsoft.com/.default")


# Print token to verify
#print("Access Token:", token.token)

# Use it in your GraphQL request
url = "https://96beaf9936794b94abf23bc8a4312e8d.z96.graphql.fabric.microsoft.com/v1/workspaces/96beaf99-3679-4b94-abf2-3bc8a4312e8d/graphqlapis/fdfa0364-256e-458a-b675-b9fbaa34c835/graphql"
headers = {
    "Authorization": f"Bearer {token.token}",
    "Content-Type": "application/json"
}

query = """
query {
  departments (first: 2 ) {
     items {
        DepartmentID,
        Name,
        GroupName,
        ModifiedDate
     }
    }
}
"""

body = {
    "query": query,
    "variables": {},
    "operationName": None
}

response = requests.post(url, json=body, headers=headers)

print(json.dumps(response.json(), indent=2))


 

 
 
