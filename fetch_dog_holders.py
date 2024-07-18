import requests
import pandas as pd
import time
import sys

def fetch_holders(offset, api_key='142cf1b0-1ca7-11ee-bb5e-9d74c2e854ac'):
    url = f'https://api.geniidata.com/api/1/runes/840000%3A3/holders?limit=20&offset={offset}'
    headers = {
        'accept': 'application/json',
        'api-key': api_key
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 404:
        print(f"Offset {offset} not found (404 error). URL: {url}")
        return None
    if response.status_code == 429 or response.json().get('code') == 1003:
        print(f"Quota exceeded for offset {offset}. URL: {url}")
        return 'quota_exceeded'
    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error for offset {offset}: {e}")
        print(f"Response content: {response.text}")
        return None
    try:
        return response.json()
    except ValueError:
        print(f"Failed to parse JSON for offset {offset}. Response: {response.text}")
        return None

def parse_data(data, current_rank):
    parsed_data = []
    total_supply = 100_000_000_000  # Total supply of DOG
    if not isinstance(data, dict):
        print(f"Unexpected data type: {type(data)}. Data: {data}")
        return parsed_data
    try:
        for holder in data['data']['list']:
            address = holder.get('address')
            balance = holder.get('balance', 0)
            percentage = (balance / total_supply) * 100
            parsed_data.append([current_rank, address, f"{percentage:.2f}%", balance])
            current_rank += 1
    except KeyError as e:
        print(f"Key error during parsing: {e}")
    return parsed_data, current_rank

def save_to_csv(data):
    df = pd.DataFrame(data, columns=['Rank', 'Address', 'Percentage', 'Balance'])
    df.to_csv('DOG_Holders.csv', index=False)

def main(start_rank, num_holders, api_key):
    all_data = []
    failed_offsets = []

    current_rank = start_rank

    for offset in range(start_rank - 1, start_rank - 1 + num_holders, 20):
        retry_count = 0
        while retry_count < 3:
            data = fetch_holders(offset, api_key)
            if data == 'quota_exceeded':
                print("Quota exceeded. Waiting for 1 hour before retrying...")
                time.sleep(3600)  # Wait for 1 hour before retrying
                retry_count += 1
            elif data is not None:
                parsed_data, current_rank = parse_data(data, current_rank)
                all_data.extend(parsed_data)
                break
            else:
                print(f"Error fetching data for offset {offset}. Retrying...")
                retry_count += 1
                time.sleep(0.2)  # Sleep for 200 milliseconds to avoid exceeding 5 requests per second
        else:
            failed_offsets.append(offset)

    # Save data to CSV
    save_to_csv(all_data)
    print("Data saved to DOG_Holders.csv")

if __name__ == "__main__":
    # This script fetches and saves the top DOG Rune holders from the GeniiData API.
    #
    # Usage:
    # python fetch_dog_holders.py <start_rank> <num_holders> <api_key>
    #
    # Example:
    # python fetch_dog_holders.py 1 100 your_api_key_here
    #
    # To get a GeniiData general API key, visit https://geniidata.com and sign up for an account.
    api_key = '142cf1b0-1ca7-11ee-bb5e-9d74c2e854ac'
    if len(sys.argv) != 4 and not api_key:
        print("Usage: python fetch_dog_holders.py <start_rank> <num_holders> <api_key>")
        sys.exit(1)
    start_rank = int(sys.argv[1])
    num_holders = int(sys.argv[2])
    api_key = sys.argv[3] if not api_key else api_key
    main(start_rank, num_holders, api_key)
