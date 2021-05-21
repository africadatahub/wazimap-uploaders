import sys
import requests

ENDPOINT = sys.argv[1]
TOKEN = sys.argv[2]
TASK_ID = sys.argv[3]

url = f"{ENDPOINT}/api/v1/datasets/{TASK_ID}"

headers = {'authorization': f"Token {TOKEN}"}

r = requests.post(url, headers=headers)
r.raise_for_status()

print(r.json())
