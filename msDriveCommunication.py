import requests
import os
from dotenv import load_dotenv

load_dotenv()

tenant_id = os.getenv("TENANT_ID")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")
site_url = os.getenv("SITE_URL")

# IMPORTANT: resource scope
scope = f"{site_url}/.default"

token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"

data = {
    "client_id": client_id,
    "client_secret": client_secret,
    "grant_type": "client_credentials",
    "scope": scope
}

response = requests.post(token_url, data=data)
token = response.json().get("access_token")

print(token)