import json
import pandas as pd
import numpy as np
from datetime import datetime
import os
from fetch_listings import get_last_fetch
from pathlib import Path

CURR_FETCH_DATE = get_last_fetch().strftime("%Y-%m")
RAW_LISTINGS_DIR = "data/raw"
PROCESSED_OUTPUT_PATH = Path(f"data/processed/listings_{CURR_FETCH_DATE}.parquet")

def load_raw_listings():
    fpa = os.path.join(RAW_LISTINGS_DIR, f'active_listings_{CURR_FETCH_DATE}.json')
    with open(fpa, 'r') as f:
        active_listings = json.load(f)
    dfa = pd.DataFrame(active_listings)

    fpia = os.path.join(RAW_LISTINGS_DIR, f'inactive_listings_{CURR_FETCH_DATE}.json')
    with open(fpia, 'r') as f:
        inactive_listings = json.load(f)
    dfia = pd.DataFrame(inactive_listings)

    print(f"Total active listings: {len(dfa)}")
    print(f"Total inactive listings: {len(dfia)}")
    print(f"Total listings: {len(dfa) + len(dfia)}")

    return dfa, dfia

def merge_active_inactive(dfa, dfia):
    dfia = dfia.drop(columns=["hoa"])
    df = pd.concat([dfa, dfia], ignore_index=True)
    
    return df

def modify_date_data_types(df):
    date_cols = [
        "listedDate",
        "removedDate",
        "createdDate",
        "lastSeenDate",
    ]

    for col in date_cols:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    reference_date = df["lastSeenDate"].max()
    df["daysSinceSeen"] = (
        reference_date - df["lastSeenDate"]
    ).dt.days

    return df

def impute_values(df_cleaned):
    # impute null bathrooms with median
    df_cleaned["bathrooms"] = df_cleaned["bathrooms"].fillna(df_cleaned["bathrooms"].median())

    # impute null square footage using median of listings with same bedrooms, bathrooms, zip code
    global_sqft_median = df_cleaned["squareFootage"].median()
    
    df_cleaned["squareFootage"] = (
        df_cleaned
        .groupby(["zipCode", "bedrooms", "bathrooms"])["squareFootage"]
        .transform(lambda x: x.fillna(x.median()))
    )

    df_cleaned["squareFootage"] = df_cleaned["squareFootage"].fillna(global_sqft_median)

    # impute null year built using median of listings with same bedrooms, bathrooms, zip code
    global_year_median = df_cleaned["yearBuilt"].median()

    df_cleaned["yearBuilt"] = (
        df_cleaned
        .groupby(["zipCode", "bedrooms", "bathrooms"])["yearBuilt"]
        .transform(lambda x: x.fillna(x.median()))
    )

    df_cleaned["yearBuilt"] = df_cleaned["yearBuilt"].fillna(global_sqft_median)
    df_cleaned["yearBuilt"] = df_cleaned["yearBuilt"].round(0).astype(int)
    
    return df_cleaned

def process_listings():
    dfa, dfia = load_raw_listings()
    df = merge_active_inactive(dfa, dfia)
    df = modify_date_data_types(df)

    analysis_cols = [
        "id",
        "zipCode",
        "bedrooms",
        "bathrooms",
        "squareFootage",
        "price",
        "daysSinceSeen",
        "yearBuilt"
    ]

    df_cleaned = df[analysis_cols].copy()
    df_cleaned = impute_values(df_cleaned)

    PROCESSED_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df_cleaned.to_parquet(PROCESSED_OUTPUT_PATH, index=False)

if __name__ == "__main__":
    process_listings()