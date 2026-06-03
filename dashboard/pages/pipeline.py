# PURPOSE: Pipeline monitor — shows Airflow pipeline run history

import streamlit as st
import psycopg2
import pandas as pd

def get_connection():
    db = st.secrets["database"]
    return psycopg2.connect(
        host=db["host"],
        port=db["port"],
        database=db["database"],
        user=db["user"],
        password=db["password"]
    )


st.title("🔧 Pipeline Monitor")
st.markdown("Track all pipeline runs — success, failure, duration")

def fetch_pipeline_logs():
    conn = get_connection()
    query = """
        SELECT pipeline_name, task_name, status,
               started_at, duration_seconds, rows_processed,
               error_message
        FROM pipeline_logs
        ORDER BY started_at DESC
        LIMIT 50
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


df = fetch_pipeline_logs()

total = len(df)
success = len(df[df["status"] == "success"])
failed = len(df[df["status"] == "failed"])

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Runs", total)
with col2:
    st.metric("Successful", success)
with col3:
    st.metric("Failed", failed)

st.subheader("Pipeline Run History")
st.dataframe(df, use_container_width=True)



st.subheader("Filter by Pipeline")
pipeline_names = ["All"] + list(df["pipeline_name"].unique())
selected_pipeline = st.selectbox("Select pipeline", pipeline_names, key="pipeline_filter")

if selected_pipeline != "All":
    filtered_df = df[df["pipeline_name"] == selected_pipeline]
else:
    filtered_df = df

st.dataframe(filtered_df, use_container_width=True)


def color_status(val):
    if val == "success":
        return "background-color: #d4edda"
    elif val == "failed":
        return "background-color: #f8d7da"
    else:
        return "background-color: #fff3cd"

styled_df = filtered_df.style.applymap(color_status, subset=["status"])
st.dataframe(styled_df, use_container_width=True)



