import joblib
import numpy as np
import pandas as pd
from fetch_listings import get_last_fetch

CURR_FETCH_DATE = get_last_fetch().strftime("%Y-%m")

def predict_rent():
    return 0

if __name__ == "__main__":
    predict_rent()