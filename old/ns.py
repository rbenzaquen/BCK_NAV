import requests

API_KEY = "05f65558-ccc1-4ef0-b488-6cb0d1a82662"
ADDRESS = "0x0A152c957FD7bCC1212Eab27233da0433b7C8EA4"
NETWORK = "ethereum"

url = f"https://api.zapper.xyz/v2/balances/tokens"
params = {
    "addresses[]": ADDRESS,
    "network": NETWORK,
    "api_key": API_KEY
}

response = requests.get(url, params=params)
response.raise_for_status()

data = response.json()
# Procesa los datos según tus necesidades
print(data)

