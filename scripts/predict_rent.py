import joblib
import numpy as np
import pandas as pd
from fetch_listings import get_last_fetch
import json

CURR_FETCH_DATE = get_last_fetch().strftime("%Y-%m")
MARKET_WINDOWS = [30, 60, 90, 180]

def prepare_listing(listing, artifact):
    df = pd.DataFrame([listing])

    # impute square footage and year built, if missing
    df["squareFootage"] = (
        listing.get("sqft")
        if listing.get("sqft") is not None
        else artifact["global_sqft_median"]
    )

    df["yearBuilt"] = (
        listing.get("year_built")
        if listing.get("year_built") is not None
        else artifact["global_year_built_median"]
    )

    # encode zip code
    df["zipCodeEncoded"] = (
        df["zip_code"]
        .map(artifact["mean_rent_by_zip"])
        .fillna(artifact["global_rent_mean"])
    )

    df.drop(columns=["zip_code"], inplace=True)

    df = df[artifact["features"]]
    return df

def predict_rent(listing, model_path):
    artifact = joblib.load(model_path)
    model = artifact["model"]

    X = prepare_listing(listing, artifact)

    log_pred = model.predict(X)[0]
    rent_pred = np.expm1(log_pred)

    pct_diff = (listing["rent"] - rent_pred) / rent_pred * 100

    if pct_diff > 10:
        label = "Overpriced"
    elif pct_diff < -10:
        label = "Underpriced"
    else:
        label = "Fairly Priced"

    return {
        "predicted_rent": float(round(rent_pred, 2)), 
        "classification": label, 
        "percent_difference": float(round(pct_diff, 1))
    }

def predict_all(listing):
    res = {}

    for w in MARKET_WINDOWS:
        model_path = f"models/rf-model_{w}d_{CURR_FETCH_DATE}.pkl"
        res[w] = predict_rent(listing, model_path)

    return res

if __name__ == "__main__":
    listing = {
        "bedrooms": 2,
        "bathrooms": 1,
        "sqft": 420,
        "zip_code": 10009,
        "year_built": 1900,
        "rent": 4230
    }

    print(json.dumps(predict_all(listing), indent=4))