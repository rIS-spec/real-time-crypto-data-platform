# PURPOSE: Overview page — shows latest crypto prices from database

import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px


def get_connection():
    db = st.secrets["database"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        database=db["database"],
        user=db["user"],
        password=db["password"]
    )


st.title("📊 Market Overview")

def fetch_latest_prices():
    conn = get_connection()
    query = """
        SELECT coin_name, symbol, price_usd, 
               price_change_pct_24h, market_cap, fetched_at
        FROM crypto_events
        ORDER BY fetched_at DESC
        LIMIT 500
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


st.subheader("Latest Crypto Prices")
df = fetch_latest_prices()
st.dataframe(df, use_container_width=True)


st.subheader("Current Prices")
cols = st.columns(5)
coins_order = ["BTC", "ETH", "SOL", "DOGE", "ADA"]

for i, symbol in enumerate(coins_order):
    coin_df = df[df["symbol"] == symbol]
    if not coin_df.empty:
        row = coin_df.iloc[0]
        with cols[i]:
            st.metric(
                label=row["coin_name"],
                value=f"${row['price_usd']:,.2f}",
                delta=f"{row['price_change_pct_24h']:.2f}%" if row['price_change_pct_24h'] else "N/A"
            )

st.subheader("Search Coin")
search = st.text_input("Enter coin name or symbol", "")
if search:
    search_df = df[
        df["coin_name"].str.contains(search, case=False) |
        df["symbol"].str.contains(search, case=False)
    ]
    if not search_df.empty:
        st.dataframe(search_df, use_container_width=True)
    else:
        st.warning(f"No coin found for: {search}")



st.subheader("Price Comparison Chart")
chart_df = df.groupby("symbol")["price_usd"].max().reset_index()
fig = px.bar(
    chart_df,
    x="symbol",
    y="price_usd",
    title="Current Price by Coin (USD)",
    color="symbol",
    labels={"price_usd": "Price (USD)", "symbol": "Coin"}
)
st.plotly_chart(fig, use_container_width=True)


def fetch_price_history(coin_symbol):
    conn = get_connection()
    query = """
        SELECT fetched_at, price_usd
        FROM crypto_events
        WHERE symbol = %s
        ORDER BY fetched_at ASC
    """
    df = pd.read_sql(query, conn, params=(coin_symbol,))
    conn.close()
    return df

st.subheader("Price History")
selected_coin = st.selectbox(
    "Select a coin",
    ["BTC", "ETH", "SOL", "DOGE", "XRP"]
)

history_df = fetch_price_history(selected_coin)
if not history_df.empty:
    fig2 = px.line(
        history_df,
        x="fetched_at",
        y="price_usd",
        title=f"{selected_coin} Price History",
        labels={"fetched_at": "Time", "price_usd": "Price (USD)"}
    )
    st.plotly_chart(fig2, use_container_width=True)
else:
    st.warning("No price history found for this coin.")



st.subheader("Price Statistics")
time_filter = st.selectbox(
    "Select time range",
    ["All Time", "Today", "Last 24 Hours"],
    key="stats_filter"
)

if time_filter == "Today":
    df_filtered = df[df["fetched_at"].dt.date == pd.Timestamp.now().date()]
elif time_filter == "Last 24 Hours":
    df_filtered = df[df["fetched_at"] >= pd.Timestamp.now() - pd.Timedelta(hours=24)]
else:
    df_filtered = df

stats_df = df.groupby("symbol")["price_usd"].agg(["min", "max", "mean"]).reset_index()
stats_df.columns = ["Symbol", "Min Price", "Max Price", "Avg Price"]
stats_df = stats_df.round(2)
st.dataframe(
    stats_df.style.format({
        "Min Price": "${:,.2f}",
        "Max Price": "${:,.2f}",
        "Avg Price": "${:,.2f}"
    }),
    use_container_width=True
)