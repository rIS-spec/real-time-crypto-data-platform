# features.py     ← reads from PostgreSQL, creates new columns

import pandas as pd
from sqlalchemy import create_engine
from api_service.config import settings


def get_db_engine():   # connects to PostgreSQL database
    return create_engine(settings.POSTGRES_URL)    # return SQLAlchemy engine 



def load_crypto_data(coin_id: str) -> pd.DataFrame:       # fetches coin history from PostgreSQL
    engine = get_db_engine()
    query = f"SELECT * FROM crypto_events WHERE coin_id = '{coin_id}' ORDER BY fetched_at ASC"
    df = pd.read_sql(query, engine)
    return df



def engineer_features(df: pd.DataFrame) -> pd.DataFrame:    # creates 3 new columns from existing ones
    df = df.sort_values("fetched_at").reset_index(drop=True)
    df["price_change_pct"] = df["price_usd"].pct_change() * 100   # % price movement per row 
    df["price_rolling_mean"] = df["price_usd"].rolling(5).mean()   # average of last 5 prices
    df["price_rolling_std"] = df["price_usd"].rolling(5).std()    # volatility of last 5 prices 
    return df



def clean_features(df: pd.DataFrame) -> pd.DataFrame:     # removes null rows and resets index 
    feature_cols = ["price_usd", "price_change_pct", "price_rolling_mean", "price_rolling_std"]
    df = df.dropna(subset=feature_cols)
    df = df.reset_index(drop=True)
    return df



if __name__ == "__main__":
    df_raw = load_crypto_data("bitcoin")
    df_features = engineer_features(df_raw)
    df_clean = clean_features(df_features)
    print(df_clean[["price_usd", "price_change_pct", "price_rolling_mean", "price_rolling_std"]].head(10))
