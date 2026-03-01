from scripts.fetch_listings import can_fetch, fetch_listings
from scripts.process_listings import process_listings
from scripts.train_models import train_models

def run_pipeline():
    if can_fetch():
        fetch_listings()
        process_listings()
        train_models()
        print("All data and models successfully refreshed")
    else:
        print("The maximum number of requests in the current billing period have been used. Please wait until the API rate limit resets.")

if __name__ == "__main__":
    run_pipeline()