import os
import requests

def get_debank_total(address: str) -> float:
    """
    Fetches the total USD net assets for a given Ethereum address
    from DeBank’s OpenAPI.
    """
    api_key = os.getenv("DEBANK_API_KEY")
    if not api_key:
        raise RuntimeError("Set DEBANK_API_KEY environment variable")

    url = "https://pro-openapi.debank.com/v1/user/total_balance"
    params = {"id": address}
    headers = {
        "accept": "application/json",
        "AccessKey": api_key
    }

    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()

    data = resp.json()
    # total_usd_value is the combined net asset value across all chains
    return data["total_usd_value"]

if __name__ == "__main__":
    addr = "0xad920e4870b3ff09d56261f571955990200b863e"
    total = get_debank_total(addr)
    print(f"Total Portfolio Value for {addr}: ${total:,.2f}")

