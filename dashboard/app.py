# PURPOSE: Main entry point for the Crypto Dashboard

import streamlit as st


st.set_page_config(
    page_title="Crypto Data Platform",
    page_icon="📈",
    layout="wide"
)

st.title("📈 Real-Time Crypto Data Platform")
st.markdown("Live cryptocurrency prices powered by CoinGecko API")

st.sidebar.title("Navigation")
st.sidebar.markdown("Use the pages above to navigate")

st.subheader("Platform Overview")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Coins Tracked", "5")
with col2:
    st.metric("Data Source", "CoinGecko")
with col3:
    st.metric("Update Frequency", "Every 5 min")


st.subheader("Coins Being Tracked")
coins = ["Bitcoin (BTC)", "Ethereum (ETH)", "Solana (SOL)", "Dogecoin (DOGE)", "Cardano (ADA)"]
for coin in coins:
    st.write(f"• {coin}")