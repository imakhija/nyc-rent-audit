import os
import requests
from dotenv import load_dotenv
import time
from datetime import datetime
from dateutil.relativedelta import relativedelta
import json
import glob

API_BASE_URL = "https://api.rentcast.io/v1/listings/rental/long-term"
LISTINGS_OUTPUT_DIR = "data/raw"
FETCH_METADATA = "data/.fetch_history.json"

MAX_REQUESTS = 50
PAGE_SIZE = 500
REQUEST_DELAY = 1

def load_api_key():
    load_dotenv()
    api_key = os.getenv("RENTCAST_API_KEY")

    if not api_key:
        raise ValueError("The RentCast API key is not set in .env")
    
    return api_key

def fetch_page(api_key, offset, status="Active", include_total=False):
    params = {
        "city": "New York",
        "state": "NY",
        "propertyType": "Apartment",
        "status": status,
        "limit": PAGE_SIZE,
        "offset": offset,
    }

    headers = {
        "accept": "application/json",
        "X-Api-Key": api_key
    }

    if include_total:
        params["includeTotalCount"] = "true"

    response = requests.get(API_BASE_URL, headers=headers, params=params)
    response.raise_for_status()

    total_count = None
    if include_total and "X-Total-Count" in response.headers:
        total_count = int(response.headers["X-Total-Count"])
    
    return response.json(), total_count

def fetch_all(api_key, status, requests_used):
    listings = []
    requests_remaining = MAX_REQUESTS - requests_used
    
    if requests_remaining <= 0:
        print(f"\nNo requests remaining for {status} listings")
        return listings, 0
    
    print(f"Fetching {status.lower()} listings...")

    # initial request to get total count
    data, total_count = fetch_page(api_key, offset=0, status=status, include_total=True)
    listings.extend(data)
    requests_made = 1

    if total_count:
        print(f"Total {status.lower()} listings available: {total_count}")
        max_offset = min(requests_remaining * PAGE_SIZE, total_count)
    else:
        max_offset = requests_remaining * PAGE_SIZE

    print(f"Fetched {len(data)} listings (offset 0)")
    
    # loop through remaining pages
    for offset in range(PAGE_SIZE, max_offset, PAGE_SIZE):
        if requests_made >= requests_remaining:
            print(f"Reached request quota for {status.lower()} listings")
            break
            
        print(f"Fetching offset {offset}...")
        time.sleep(REQUEST_DELAY)
        
        data, _ = fetch_page(api_key, offset=offset, status=status)
        
        if not data:
            print("No more listings found.")
            break
        
        listings.extend(data)
        requests_made += 1
        print(f"Fetched {len(data)} listings (total so far: {len(listings)})")
    
    print(f"\nTotal {status.lower()} listings fetched: {len(listings)}")
    print(f"Requests used: {requests_made}")
    
    return listings, requests_made

def save_listings(listings, status="Active"):
    date = datetime.now().strftime("%Y-%m")
    filename = f"{status.lower()}_listings_{date}.json"
    filepath = os.path.join(LISTINGS_OUTPUT_DIR, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(listings, f, indent=2)

    return filename

def save_fetch_metadata(num_requests, num_listings):
    if os.path.exists(FETCH_METADATA):
        with open(FETCH_METADATA, "r") as f:
            history = json.load(f)
    else:
        history = {"fetches": []}

    history["fetches"].append({
        "date": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
        "num_requests": num_requests,
        "num_listings": num_listings
    })

    os.makedirs(os.path.dirname(FETCH_METADATA), exist_ok=True)
    with open(FETCH_METADATA, "r") as f:
        json.dump(history, f, indent=4)

def get_last_fetch():
    if not os.path.exists(FETCH_METADATA):
        return None
    
    with open(FETCH_METADATA, "r") as f:
        history = json.load(f)

    fetches = history.get("fetches", [])
    if not fetches:
        return None

    return datetime.fromisoformat(fetches[-1]["date"])

def can_fetch():
    last_fetch_date = get_last_fetch()
    
    if not last_fetch_date:
        return True
    
    current_date = datetime.now()
    api_reset_date = last_fetch_date + relativedelta(months=1)
    
    print(f"Last fetch: {last_fetch_date.strftime('%Y-%m-%d')}")
    print(f"Current date: {current_date.strftime('%Y-%m-%d')}")
    print(f"Earliest next fetch: {api_reset_date.strftime('%Y-%m-%d')}")
    
    if current_date <= api_reset_date:
        return False
    
    return True

def main():
    api_key = load_api_key()
    os.makedirs(LISTINGS_OUTPUT_DIR, exist_ok=True)

    total_requests_used = 0
    
    # fetch active listings
    active_listings, active_requests = fetch_all(
        api_key, 
        status="Active", 
        requests_used=total_requests_used
    )
    total_requests_used += active_requests
    save_listings(active_listings, status="Active")
    
    # fetch inactive listings with remaining quota
    inactive_listings, inactive_requests = fetch_all(
        api_key,
        status="Inactive",
        requests_used=total_requests_used
    )
    total_requests_used += inactive_requests
    save_listings(inactive_listings, status="Inactive")

    save_fetch_metadata(total_requests_used, len(active_listings) + len(inactive_listings))
    print(f"Total requests used: {total_requests_used}/{MAX_REQUESTS}")
    print(f"Active listings: {len(active_listings)}")
    print(f"Inactive listings: {len(inactive_listings)}")

if __name__ == "__main__":
    if can_fetch():
        main()
    else:
        print("The maximum number of requests in the current billing period have been used. Please wait until the API rate limit resets.")