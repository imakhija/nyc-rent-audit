import pandas as pd
import numpy as np
from pathlib import Path
from fetch_listings import get_last_fetch
import joblib
import json

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

CURR_FETCH_DATE = get_last_fetch().strftime("%Y-%m")
MARKET_WINDOWS = [30, 60, 90, 180]
MODELS_DIR = Path("models")

def encode_with_zip(X_train, y_train, X_test):
    mean_by_zip = (
        X_train.assign(y=y_train)
        .groupby("zipCode")["y"]
        .mean()
    )

    global_mean = y_train.mean()

    def encode(df):
        df = df.copy()
        df["zipCodeEncoded"] = df["zipCode"].map(mean_by_zip)
        df["zipCodeEncoded"] = df["zipCodeEncoded"].fillna(global_mean)
        df.drop(columns=["zipCode"], inplace=True)
        return df

    return encode(X_train), encode(X_test), mean_by_zip, global_mean

def train_model_by_window(df, window):
    print(f"Training model for last {window} days")

    dfw = df[df["daysSinceSeen"] <= window]

    y = np.log1p(dfw["price"])
    X = dfw.drop(columns=["price", "id", "daysSinceSeen"])

    X_train, X_test, y_train, y_test = train_test_split(
        X, 
        y, 
        test_size=0.2,
        random_state=42
    )

    X_train_enc, X_test_enc, mean_by_zip, global_mean = encode_with_zip(X_train, y_train, X_test)

    model = RandomForestRegressor(
        n_estimators=300,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1
    )

    model.fit(X_train_enc, y_train)
    y_pred = model.predict(X_test_enc)

    mse = mean_squared_error(y_test, y_pred)
    rmse = np.sqrt(mse)
    r2 = r2_score(y_test, y_pred)

    print(f"RMSE (log): {rmse:.3f}")
    print(f"R²: {r2:.3f}")

    return model, mean_by_zip, global_mean, {"rmse": rmse, "r2": r2}

def train_models():
    df = pd.read_parquet(Path(f"data/processed/listings_{CURR_FETCH_DATE}.parquet"))

    for w in MARKET_WINDOWS:
        model, mean_by_zip, global_mean, metrics = train_model_by_window(df, w)
        
        model_path = MODELS_DIR / f"rf-model_{w}d_{CURR_FETCH_DATE}.pkl"
        meta_path = MODELS_DIR / f"rf-model_{w}d_meta_{CURR_FETCH_DATE}.json"

        joblib.dump(model, model_path)

        metadata = {
            "window_days": w,
            "global_rent_mean": global_mean,
            "mean_rent_by_zip": mean_by_zip.to_dict(),
            "metrics": metrics
        }

        with open(meta_path, "w") as f:
            json.dump(metadata, f)

        print(f"Saved {model_path.name}")

if __name__ == "__main__":
    train_models()