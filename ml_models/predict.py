# predict.py   ← runs predictions, saves to ml_predictions table

# load .pkl model → load new data → engineer features → predict → save results to ml_predictions table



import os
import pickle
import pandas as pd
from datetime import datetime, timezone
from ml_models.features import load_crypto_data, engineer_features, clean_features
from api_service.config import settings
import psycopg2


COINS = ["bitcoin", "ethereum", "solana", "dogecoin", "ripple"]
MODEL_DIR = "ml_models/saved_models"




def run_predictions():
    conn = psycopg2.connect(settings.POSTGRES_URL)
    cursor = conn.cursor()
    for coin in COINS:
        print(f"\nRunning predictions for: {coin}")
        model_path = os.path.join(MODEL_DIR, f"{coin}_model.pkl")
        with open(model_path, "rb") as f:
            model = pickle.load(f)
        df = load_crypto_data(coin)
        df = engineer_features(df)
        df = clean_features(df)
        features = ["price_usd", "price_change_pct", "price_rolling_mean", "price_rolling_std"]
        X = df[features]
        predictions = model.predict(X)
        scores = model.decision_function(X)
        df["prediction"] = predictions
        df["anomaly_score"] = scores
        df["is_anomaly"] = df["prediction"] == -1
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO ml_predictions 
                (coin_id, price_usd, prediction, confidence, is_anomaly, predicted_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                coin,
                row["price_usd"],
                "anomaly" if row["is_anomaly"] else "normal",
                row["anomaly_score"],
                row["is_anomaly"],
                datetime.now(timezone.utc)
            ))
    conn.commit()
    cursor.close()
    conn.close()
    print("\nAll predictions saved to ml_predictions table!")


if __name__ == "__main__":
    run_predictions()