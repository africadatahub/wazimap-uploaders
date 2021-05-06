import sys
import requests

ENDPOINT = sys.argv[1]
TOKEN = sys.argv[2]
FILE_PATH = sys.argv[3]
DATASET_ID = sys.argv[4]

url = f"{ENDPOINT}/api/v1/datasets/{DATASET_ID}/upload/"

headers = {'authorization': f"Token {TOKEN}"}
files = {'file': open(FILE_PATH, 'rb')}
payload = {'update': True, 'overwrite': True}

r = requests.post(url, headers=headers, data=payload, files=files)
r.raise_for_status()

print(r.json())
