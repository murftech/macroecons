import requests


          
# dataset_id = "d_8b84c4ee58e3cfc0ece0d773c8ca6abc"
# url = "https://data.gov.sg/api/action/datastore_search?resource_id="  + dataset_id 
        
# response = requests.get(url)
# print(response.json())

import requests

response = requests.get(
    "https://api-open.data.gov.sg/v1/public/api/datasets/d_11e68bba3b3c76733475a72d09759eeb/initiate-download",
    headers={"Content-Type":"application/json"},
    data={"columnNames":["name","title","address","email_address"],"filters":{"fitlers":[{"columnName":"epi_week","type":"EQ","value":2},{"columnName":"status","type":"ILIKE","value":"covid"},{"columnName":"epi_year","type":"LIKE","value":23}]}}
)

data = response.json()

type(data)



import requests

response = requests.get(
    "https://api-open.data.gov.sg/v1/public/api/datasets/d_11e68bba3b3c76733475a72d09759eeb/poll-download",
    headers={"Content-Type":"application/json"},
    data={"columnNames":["name","title","address","email_address"],"filters":{"fitlers":[{"columnName":"epi_week","type":"EQ","value":2},{"columnName":"status","type":"ILIKE","value":"covid"},{"columnName":"epi_year","type":"LIKE","value":23}]}}
)

data = response.json()



import pandas as pd
# Make API request
# response = requests.get(url)
# data = response.json()

# Extract records and convert to DataFrame
records = data["result"]["records"]
df = pd.DataFrame(records)

# Save DataFrame to CSV
df.to_csv("output.csv", index=False, encoding="utf-8")

print("CSV file 'output.csv' has been created successfully.")


###############
# Fetch total number of records
response = requests.get(url, params={"resource_id": dataset_id, "limit": 1})

total_records = response.json()["result"]["total"]

# Set batch size
batch_size = 500  # Adjust based on API limits
all_records = []

# Fetch data in batches
for offset in range(0, total_records, batch_size):
    response = requests.get(url, params={"resource_id": dataset_id, "limit": batch_size, "offset": offset})
    data = response.json()
    all_records.extend(data["result"]["records"])

# Convert to DataFrame and save to CSV
df = pd.DataFrame(all_records)
df.to_csv("output.csv", index=False, encoding="utf-8")

print(f"CSV file 'output.csv' has been created successfully with {len(df)} rows.")