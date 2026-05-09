# import installed bigquery package

from google.cloud import bigquery
import os

# instantiating a bigquery client that will read the env variable setup earlier
client = bigquery.Client()

# printing project to confirm the authorisation
print(f"Connected to project: {client.project}")

# listing datasets to confirm access
datasets = list(client.list_datasets())
print("Datasets in project:")
for ds in datasets:
    print(f"- {ds.dataset_id}")




