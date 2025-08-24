import requests

client_id = "1000.6TJ78DQZ9VT8S9XAK9CXABD0FOQUHK"
client_secret = "4995e711db43712e6d22ac5081d6a175dbcc120015"
redirect_uri = "https://helpdesk.regalcream.com.au/app/itdesk/ui/tasks"
code = "PASTE_CODE_HERE"

token_url = "https://accounts.zoho.com.au/oauth/v2/token"

payload = {
    "grant_type": "authorization_code",
    "client_id": client_id,
    "client_secret": client_secret,
    "redirect_uri": redirect_uri,
    "code": code,
}

res = requests.post(token_url, data=payload)
print("Token response:", res.json())
