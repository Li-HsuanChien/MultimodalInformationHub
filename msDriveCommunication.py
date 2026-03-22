import requests
import os
from dotenv import load_dotenv

load_dotenv()  # loads variables from .env

site_url = os.getenv("SITE_URL")
list_name = os.getenv("LIST_NAME")
access_token = os.getenv("ACCESS_TOKEN")

url = f"{site_url}/_api/lists/getbytitle('{list_name}')/items"


headers = {
    "Authorization": f"Bearer {access_token}",
    "Accept": "application/json;odata=verbose"
}

response = requests.get(url, headers=headers)

# Check response
if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Error: {response.status_code}")
    print(response.text)