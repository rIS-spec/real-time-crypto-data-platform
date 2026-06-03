
# ml_predictions table → load data → show anomaly chart → highlight red dots → show anomaly table


import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
from api_service.config import settings


st.set_page_config(page_title="Anomaly Detection", page_icon="🚨")
st.title("🚨 Anomaly Detection")
st.markdown("Isolation Forest model detecting unusual crypto price movements.")

engine = create_engine(settings.POSTGRES_URL)

query = """
    SELECT coin_id, price_usd, prediction, confidence, is_anomaly, predicted_at
    FROM ml_predictions
    ORDER BY predicted_at ASC
"""
df = pd.read_sql(query, engine)

st.subheader("Anomaly Summary by Coin")
summary = df.groupby("coin_id").agg(
    total=("coin_id", "count"),
    anomalies=("is_anomaly", "sum")
).reset_index()
summary["anomaly_pct"] = (summary["anomalies"] / summary["total"] * 100).round(2)
st.dataframe(summary, use_container_width=True)

st.subheader("Price Chart with Anomalies Highlighted")
coin = st.selectbox("Select Coin", df["coin_id"].unique())
coin_df = df[df["coin_id"] == coin].copy()
coin_df["color"] = coin_df["is_anomaly"].map({True: "Anomaly", False: "Normal"})

fig = px.scatter(
    coin_df,
    x="predicted_at",
    y="price_usd",
    color="color",
    color_discrete_map={"Anomaly": "red", "Normal": "green"},
    title=f"{coin.capitalize()} Price — Anomalies in Red",
    labels={"predicted_at": "Time", "price_usd": "Price (USD)"}
)
st.plotly_chart(fig, use_container_width=True)

st.subheader("Anomaly Details")
anomalies_only = coin_df[coin_df["is_anomaly"] == True][
    ["coin_id", "price_usd", "confidence", "predicted_at"]
].reset_index(drop=True)
st.dataframe(anomalies_only, use_container_width=True)