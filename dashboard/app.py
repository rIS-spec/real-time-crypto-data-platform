# PURPOSE: Main entry point for the Crypto Dashboard

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus


def get_engine():
    db = st.secrets["database"]
    user = quote_plus(db["user"])
    password = quote_plus(db["password"])
    url = (
        f"postgresql+psycopg2://{user}:{password}"
        f"@{db['host']}:{db['port']}/{db['database']}"
        f"?sslmode=require"
    )
    return create_engine(url)


st.set_page_config(
    page_title="Crypto Data Platform",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Crypto Data Platform")
st.markdown("Live cryptocurrency prices powered by CoinGecko API")

st.sidebar.title("Navigation")
st.sidebar.markdown("Use the pages above to navigate")

engine = get_engine()

with engine.connect() as conn:
    total_rows = conn.execute(text("SELECT COUNT(*) FROM crypto_events")).scalar()
    last_fetch = conn.execute(text("SELECT MAX(fetched_at) FROM crypto_events")).scalar()
    coin_count = conn.execute(text("SELECT COUNT(DISTINCT coin_id) FROM crypto_events")).scalar()

st.subheader("Platform Overview")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("Coins Tracked", coin_count)
with col2:
    st.metric("Total Price Records", f"{total_rows:,}")
with col3:
    st.metric("Data Source", "CoinGecko")
with col4:
    if last_fetch:
        st.metric("Last Updated", last_fetch.strftime("%H:%M:%S"))
    else:
        st.metric("Last Updated", "N/A")

st.divider()

st.subheader("Coins Being Tracked")
coins = {
    "Bitcoin (BTC)": "bitcoin",
    "Ethereum (ETH)": "ethereum",
    "Solana (SOL)": "solana",
    "Dogecoin (DOGE)": "dogecoin",
    "Ripple (XRP)": "ripple",
}

tabs = st.tabs(list(coins.keys()))
for tab, (label, coin_id) in zip(tabs, coins.items()):
    with tab:
        query = text("""
            SELECT price_usd, fetched_at
            FROM crypto_events
            WHERE coin_id = :coin_id
            ORDER BY fetched_at DESC
            LIMIT 20
        """)
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"coin_id": coin_id})

        if df.empty:
            st.info(f"No data yet for {label}.")
        else:
            latest_price = df.iloc[0]["price_usd"]
            st.metric(f"{label} — Latest Price", f"${latest_price:,.2f}")
            chart_df = df.sort_values("fetched_at")
            st.line_chart(chart_df.set_index("fetched_at")["price_usd"])

st.divider()
st.caption("Navigate using the sidebar pages: Overview · Real-Time · Pipeline Monitor · Anomaly Detection")