import psycopg2
from dotenv import load_dotenv
import os

load_dotenv(".env.cloud")
url = os.getenv("NEON_DATABASE_URL")

print("Testing connection to Neon directly...")
try:
    conn = psycopg2.connect(url)
    cursor = conn.cursor()
    cursor.execute("SELECT 1;")
    result = cursor.fetchone()
    print("SUCCESS — connected to Neon. Result:", result)
    cursor.close()
    conn.close()
except Exception as e:
    print("FAILED —", e)