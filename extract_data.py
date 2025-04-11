import requests
import pandas as pd
from datetime import date, timedelta

def extract():
    # URLs
    states_url = "https://gist.githubusercontent.com/mshafrir/2646763/raw/8b0dbb93521f5d6889502305335104218454c2bf/states_hash.json"
    API_URL = "https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/?size=500&date_received_max={}&date_received_min={}&state={}"
    
    # Get list of US states
    states = list(requests.get(states_url).json().keys())

    # Define date range for last year
    current_date = date.today()
    min_date = (current_date - timedelta(days=365)).strftime("%Y-%m-%d")
    max_date = current_date.strftime("%Y-%m-%d")

    all_data = []  # Store extracted data

    for state in states:
        print(f"Fetching data for state: {state}")
        url = API_URL.format(max_date, min_date, state)
        response = requests.get(url)

        if response.status_code == 200:
            data = response.json().get("hits", {}).get("hits", [])
            for item in data:
                all_data.append(item["_source"])  # Extract required fields
        else:
            print(f"Failed to fetch data for {state}, Status Code: {response.status_code}")

    # Convert to DataFrame and save as JSON
    df = pd.DataFrame(all_data)
    df.to_json('extracted_data.json', orient='records', lines=True)

    return df.to_dict(orient='records')  # Return extracted data as a list of dictionaries

# Run extraction (for local testing)
if __name__ == "__main__":
    extracted_data = extract()
    print(f"Extracted {len(extracted_data)} records.")
