import requests

client_id = "1000.6TJ78DQZ9VT8S9XAK9CXABD0FOQUHK"
client_secret = "4995e711db43712e6d22ac5081d6a175dbcc120015"
refresh_token = "1000.047017b7d496a5e36e6a66a195649e45.3ca39d9b197c5cd5bc69dde7c8dd0fd9"

# Make sure to use the correct regional domain
token_url = "https://accounts.zoho.com.au/oauth/v2/token"  # adjust region

# Zoho expects form-encoded parameters
data = {
    "refresh_token": refresh_token,
    "client_id": client_id,
    "client_secret": client_secret,
    "grant_type": "refresh_token"
}

response = requests.post(token_url, data=data)

try:
    resp_json = response.json()
except ValueError:
    print("Response is not valid JSON:")
    print(response.text)
    exit()

# Print full response for debugging
print("Full Response:", resp_json)

# Extract access token if available
access_token = resp_json.get("access_token")
if access_token:
    print("New Access Token:", access_token)
else:
    print("Access token not returned. Possible reasons:")
    print("- Refresh token invalid or revoked")
    print("- Client ID/Secret mismatch")
    print("- Account region mismatch with URL")


# 1000.344320cb2619b9d8fb37639688625083.9a63c73838d2cae6efad7765bb152934