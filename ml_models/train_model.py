# train_model.py  ← trains Isolation Forest model

# Load data from PostgreSQL using features.py
# Train one Isolation Forest model per coin
# Save each trained model as a .pkl file to disk

# PostgreSQL → load data per coin → engineer features → train Isolation Forest → save .pkl file

import os
import pickle
import pandas as pd
from sklearn.ensemble import IsolationForest
from ml_models.features import load_crypto_data, engineer_features, clean_features

COINS = ["bitcoin", "ethereum", "solana", "dogecoin", "ripple"]
MODEL_DIR = "ml_models/saved_models"


def train_and_save_models():
    os.makedirs(MODEL_DIR, exist_ok=True)
    for coin in COINS:
        print(f"\nTraining model for: {coin}")
        df = load_crypto_data(coin)
        df = engineer_features(df)
        df = clean_features(df)
        features = ["price_usd", "price_change_pct", "price_rolling_mean", "price_rolling_std"]
        X = df[features]
        model = IsolationForest(n_estimators=100, contamination=0.1, random_state=42)
        model.fit(X)
        model_path = os.path.join(MODEL_DIR, f"{coin}_model.pkl")
        with open(model_path, "wb") as f:
            pickle.dump(model, f)
        print(f"Saved: {model_path}")
    print("\nAll models trained and saved!")


if __name__ == "__main__":
    train_and_save_models()