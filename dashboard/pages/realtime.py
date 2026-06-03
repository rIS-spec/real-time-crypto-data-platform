# PURPOSE: Real-time page — auto-refreshes to show live prices

import streamlit as st
import psycopg2
import pandas as pd
import time


def get_connection():
    db = st.secrets["database"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        database=db["database"],
        user=db["user"],
        password=db["password"]
    )

st.title("⚡ Real-Time Prices")
st.markdown("Auto-refreshes every 30 seconds")
refresh_rate = st.slider("Refresh rate (seconds)", 10, 60, 30)
placeholder = st.empty()

while True:
    conn = get_connection()
    query = """
        SELECT coin_name, symbol, price_usd,
               price_change_pct_24h, fetched_at
        FROM crypto_events
        ORDER BY fetched_at DESC
        LIMIT 10
    """
    df = pd.read_sql(query, conn)
    conn.close()
    with placeholder.container():
        st.dataframe(df, use_container_width=True)
        st.caption(f"Last updated: {pd.Timestamp.now().strftime('%H:%M:%S')}")
        for i in range(refresh_rate, 0, -1):
            st.caption(f"Next refresh in: {i} seconds")
            time.sleep(1)