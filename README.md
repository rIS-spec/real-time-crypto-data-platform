# Real-Time Crypto Data Platform

> A production-grade, end-to-end real-time data engineering platform that ingests live cryptocurrency prices for 5 coins, streams events through Apache Kafka, transforms data with PySpark, orchestrates pipelines via Airflow, detects anomalies using Isolation Forest ML and serves insights through a live Streamlit dashboard — fully deployed on AWS EC2.

**Live Demo:**
- 🚀 API: [http://3.108.53.215:8001/docs](http://3.108.53.215:8001/docs)
- 📊 Dashboard: [http://3.108.53.215:8501](http://3.108.53.215:8501)

**GitHub:** [github.com/rIS-spec/real-time-crypto-data-platform](https://github.com/rIS-spec/real-time-crypto-data-platform)
Now I did terminate EC2 Instance.
---

## Table of Contents

- [System Architecture](#system-architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Key Features](#key-features)
- [Major Errors Fixed](#major-errors-fixed)
- [How I Deployed This Project](#how-i-deployed-this-project)
- [Local Setup](#local-setup)
- [API Endpoints](#api-endpoints)
- [Database Schema](#database-schema)
- [ML Model](#ml-model)

---

## System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    DATA INGESTION LAYER                         │
│                                                                 │
│   CoinGecko API ──► FastAPI Service ──► Kafka Producer         │
│   (5 coins: BTC, ETH, SOL, DOGE, XRP)    (crypto-events topic) │
└─────────────────────────┬───────────────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────────────┐
│                    STREAMING LAYER                              │
│                                                                 │
│   Apache Kafka (3 partitions) ──► Kafka Consumer               │
│   Zookeeper (coordination)         │                            │
│   Kafka UI (monitoring)            │                            │
└────────────────────────────────────┼────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────┐
│                    PROCESSING LAYER                             │
│                                                                 │
│   PySpark ──► Transformations ──► Aggregations                 │
│               Window Functions     ML Anomaly Detection         │
│               Data Cleaning        Isolation Forest             │
└────────────────────────────────────┬────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────┐
│                    STORAGE LAYER                                │
│                                                                 │
│   PostgreSQL ──► crypto_events table                           │
│                  pipeline_logs table                            │
│                  ml_predictions table                           │
│                                                                 │
│   AWS S3 ──► raw/crypto/ (JSON files)                          │
│              processed/ (transformed data)                      │
│              logs/ (pipeline logs)                              │
│                                                                 │
│   AWS RDS ──► Cloud PostgreSQL (production database)           │
└────────────────────────────────────┬────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────┐
│                    ORCHESTRATION LAYER                          │
│                                                                 │
│   Apache Airflow ──► ingest_dag (fetch + produce + consume)    │
│   (12 Phases)        main_pipeline_dag (full pipeline)         │
│                      analytics_dag (PySpark + ML)              │
└────────────────────────────────────┬────────────────────────────┘
                                     │
┌────────────────────────────────────▼────────────────────────────┐
│                    SERVING LAYER                                │
│                                                                 │
│   Streamlit Dashboard ──► Market Overview                      │
│   (Live on EC2)            Real-Time Prices                    │
│                            Pipeline Monitor                     │
│                            Anomaly Detection                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Category | Technology | Purpose |
|---|---|---|
| Ingestion | FastAPI + CoinGecko API | Fetch live crypto prices |
| Streaming | Apache Kafka + Zookeeper | Event streaming pipeline |
| Processing | Apache PySpark 3.5.1 | Transformations + aggregations |
| Orchestration | Apache Airflow 2.8.0 | Pipeline scheduling + monitoring |
| Storage | PostgreSQL 15 | Primary data warehouse |
| Cloud Storage | AWS S3 | Raw data lake |
| Cloud Database | AWS RDS PostgreSQL | Production managed database |
| ML | Scikit-learn Isolation Forest | Anomaly detection |
| Dashboard | Streamlit | Interactive data visualization |
| Containerization | Docker Compose | Service orchestration |
| Cloud | AWS EC2, S3, RDS, IAM | Production deployment |
| Language | Python 3.12.4 | Primary language |

---

## Project Structure

```
real-time-crypto-data-platform/
│
├── api_service/                  # FastAPI ingestion service
│   ├── main.py                   # FastAPI app entry point
│   ├── routes.py                 # API endpoints
│   ├── schemas.py                # Pydantic data models
│   ├── config.py                 # Settings + env variables
│   └── fetchers/
│       └── crypto.py             # CoinGecko API fetcher
│
├── kafka_service/                # Kafka producer + consumer
│   ├── producer.py               # Fetches + sends to Kafka
│   ├── consumer.py               # Reads from Kafka → PostgreSQL
│   └── topics.py                 # Topic creation
│
├── spark_processing/             # PySpark transformations
│   ├── transformations.py        # Price cleaning + feature engineering
│   ├── aggregations.py           # Groupby, window functions
│   └── spark_stream.py           # Structured streaming
│
├── airflow_dags/                 # Airflow DAG definitions
│   ├── ingest_dag.py             # Data ingestion pipeline
│   ├── main_pipeline_dag.py      # Full end-to-end pipeline
│   └── analytics_dag.py          # PySpark + ML analytics
│
├── ml_models/                    # Machine learning
│   ├── train_model.py            # Train Isolation Forest
│   ├── predict.py                # Run predictions
│   ├── features.py               # Feature engineering
│   └── saved_models/             # Trained .pkl files (5 coins)
│
├── dashboard/                    # Streamlit dashboard
│   ├── app.py                    # Main dashboard entry
│   └── pages/
│       ├── overview.py           # Market overview page
│       ├── realtime.py           # Live prices page
│       ├── pipeline.py           # Pipeline monitor page
│       └── anomalies.py          # Anomaly detection page
│
├── warehouse/
│   ├── create_tables.sql         # Database schema
│   └── load_data.py              # Data loading utilities
│
├── docker/
│   └── docker-compose.yml        # All services configuration
│
├── tests/                        # Unit tests
├── requirements.txt              # Python dependencies
└── .env.example                  # Environment variables template
```

---

## Key Features

- **Real-Time Streaming:** Live crypto prices fetched every 5 minutes from CoinGecko API and streamed through Kafka with sub-second latency
- **5 Coins Tracked:** Bitcoin (BTC), Ethereum (ETH), Solana (SOL), Dogecoin (DOGE), XRP
- **Kafka 3 Partitions:** One partition per major coin group for parallel processing
- **PySpark Transformations:** Window functions, rolling averages, price change calculations
- **Isolation Forest ML:** Anomaly detection model trained per coin — detects price spikes and unusual volume patterns
- **Airflow Orchestration:** 12-phase DAG implementation with retry logic, SLA monitoring, XCom data passing
- **AWS Data Lake:** Raw JSON files in S3 with Bronze/Silver folder structure
- **Live Dashboard:** 4-page Streamlit dashboard with real-time charts, pipeline monitoring, and ML anomaly visualization
- **Full Cloud Deployment:** Entire stack running on AWS EC2 with Docker Compose — publicly accessible

---

## Major Errors Fixed

These are the 7 most important errors I encountered and fixed during this project. Each taught me something critical about production data engineering.

---

### Error 1: ModuleNotFoundError — `kafka` vs `kafka_service`

**What happened:**
```
ModuleNotFoundError: No module named 'kafka_service'
```
My Kafka folder was originally named `kafka/` which conflicted with the `kafka-python` library. Python was importing the library instead of my folder.

**Root cause:** Python's import system searches installed packages before local folders when names collide.

**Fix:**
```bash
# Renamed folder from kafka/ to kafka_service/
# Updated all imports from:
from kafka.producer import ...
# To:
from kafka_service.producer import ...
```

**Lesson:** Never name your project folders the same as installed libraries.

---

### Error 2: datetime Serialization Error in Kafka Messages

**What happened:**
```
TypeError: Object of type datetime is not JSON serializable
```
When the Kafka producer tried to send `CryptoPrice` objects to Kafka, it crashed because Python's `datetime` objects cannot be serialized to JSON directly.

**Root cause:** Pydantic models contain `datetime` fields which are not JSON-serializable by default.

**Fix:**
```python
# WRONG — crashes with datetime error
message = coin.dict()

# CORRECT — converts datetime to ISO string automatically
message = coin.model_dump(mode='json')
```

**Lesson:** Always use `model_dump(mode='json')` when serializing Pydantic models to JSON.

---

### Error 3: PostgreSQL Transaction Abort — `conn.rollback()` Required

**What happened:**
```
psycopg2.errors.InFailedSqlTransaction: 
current transaction is aborted, commands ignored until rollback
```
After one failed INSERT, all subsequent database operations failed — the connection was stuck in an aborted transaction state.

**Root cause:** PostgreSQL keeps a connection in "aborted" state after any error. All further queries fail until the transaction is explicitly rolled back.

**Fix:**
```python
try:
    cursor.execute(insert_query, values)
    conn.commit()
except Exception as e:
    conn.rollback()  # CRITICAL — reset the transaction
    logger.error(f"Insert failed: {e}")
```

**Lesson:** Always call `conn.rollback()` in the except block when using psycopg2 — never assume the connection resets itself.

---

### Error 4: Kafka Consumer Not Reading Messages — Stale Offset

**What happened:**
The consumer started, connected successfully, but never received any messages — it just sat there silently with no output.

**Root cause:** The consumer group had an old committed offset from a previous run. Kafka remembered the last position and the consumer started reading from there — but all new messages were after that position with `auto_offset_reset='latest'`, so nothing was read.

**Fix:**
```python
# WRONG — reuses old offset, misses messages
consumer = KafkaConsumer(
    'crypto-events',
    group_id='crypto-group'
)

# CORRECT — new group sees all messages from beginning
consumer = KafkaConsumer(
    'crypto-events',
    group_id=f'crypto-group-{int(time.time())}',  # unique group
    auto_offset_reset='earliest',
    consumer_timeout_ms=-1   # wait forever
)
```

**Lesson:** When testing, always use a new consumer group ID or reset offsets. In production, understand exactly where your consumer left off.

---

### Error 5: PySpark Fails on Windows — Missing `hadoop.dll`

**What happened:**
```
ERROR Shell: Failed to locate the winutils binary in the hadoop binary path
java.io.IOException: Could not locate executable null\bin\winutils.exe
```
PySpark requires Hadoop binaries even on Windows when no Hadoop cluster is present. Without them, even basic DataFrame operations crash.

**Root cause:** PySpark on Windows needs `winutils.exe` and `hadoop.dll` from Hadoop 3.3.5 to simulate HDFS operations locally.

**Fix:**
```powershell
# Downloaded winutils.exe + hadoop.dll to C:\hadoop\bin\
# Set environment variables every session:
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:HADOOP_HOME\bin;$env:PATH"
$env:JAVA_TOOL_OPTIONS = "-Xmx512m"
$env:PYTHONPATH = "D:\Desktop\real-time-data-platform"
$env:PYSPARK_PYTHON = "D:\Desktop\real-time-data-platform\venv\Scripts\python.exe"
```

**Lesson:** PySpark on Windows has specific binary requirements. Always set all 5 environment variables before starting any Spark session on Windows.

---

### Error 6: Airflow DAG Not Found — `sys.path` Issue Inside Docker

**What happened:**
```
ModuleNotFoundError: No module named 'api_service'
```
Airflow DAGs that imported from `api_service` worked locally but failed inside the Docker container because the container's Python path didn't include the project root.

**Root cause:** Inside Docker, Airflow's Python interpreter doesn't know about the project structure mounted into `/opt/airflow/dags/`.

**Fix:**
```python
# Added at the top of every DAG file:
import sys
sys.path.insert(0, '/opt/airflow/dags')

# Also had to manually copy files into container:
# docker cp api_service/ airflow:/opt/airflow/dags/api_service/
```

**Lesson:** Docker containers are isolated environments. Always verify that `sys.path` includes your project root when running code inside containers.

---

### Error 7: Kafka Out of Memory on EC2 t3.micro

**What happened:**
```
OpenJDK 64-Bit Server VM: Out of Memory Error
os::commit_memory failed to map 1073741824 bytes
```
Kafka's JVM tried to allocate 1GB of RAM on a t3.micro instance that only has 1GB total. With PostgreSQL and Zookeeper already running, there was no memory left.

**Root cause:** Kafka's default JVM heap size is 1GB — which equals the entire RAM of a t3.micro instance.

**Fix — Two steps:**
```bash
# Step 1: Add 1GB swap space
sudo dd if=/dev/zero of=/swapfile bs=128M count=8
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

# Step 2: Limit Kafka heap in docker-compose.yml
environment:
  KAFKA_HEAP_OPTS: "-Xmx256m -Xms256m"
```

**Lesson:** Always check memory requirements before deploying. On constrained instances, explicitly limit JVM heap size and add swap space as a safety net.

---

## How I Deployed This Project

### Overview

I deployed this project on AWS EC2 with Docker Compose. The goal was to have the full pipeline — FastAPI, Kafka, PostgreSQL, Airflow, and Streamlit — running on a real cloud server accessible from anywhere in the world.

Here are the exact steps I followed and the reason behind each decision.

---

### Step 1: Created AWS EC2 Instance

**What I did:**
- Launched a `t3.micro` EC2 instance in Mumbai (`ap-south-1`) region
- Chose Amazon Linux 2023 as the OS
- Created a security group with ports 22, 80, 443, 8001, 8501, 8090, 8080 open

**Why t3.micro:**
t3.micro is free tier eligible — 750 hours/month free for 12 months. For a portfolio project, cost optimization matters. In production I would scale based on actual load.

**Why Mumbai region:**
Lower latency for India-based interviews and demos. Also, AWS RDS and S3 in the same region avoids cross-region data transfer costs.

**Why these ports:**
- Port 22 — SSH access for deployment and debugging
- Port 8001 — FastAPI service (I chose 8001 because 8000 was already in use)
- Port 8501 — Streamlit dashboard
- Port 8090 — Kafka UI for monitoring

---

### Step 2: Configured IAM Role for Secure S3 Access

**What I did:**
- Created IAM role `crypto-ec2-s3-role` with `AmazonS3FullAccess` policy
- Attached the role to the EC2 instance

**Why IAM Role instead of access keys:**
Hardcoding AWS access keys in code is a critical security risk — if the code is pushed to GitHub, anyone can access your entire AWS account. IAM Roles provide temporary, automatically rotating credentials. The EC2 instance authenticates automatically — no keys in code.

```python
# With IAM Role — no credentials needed
s3 = boto3.client('s3', region_name='ap-south-1')
# AWS automatically provides credentials via the role
```

---

### Step 3: Set Up AWS S3 Data Lake

**What I did:**
- Created bucket `crypto-data-platform-arish` in `ap-south-1`
- Created folder structure: `raw/`, `processed/`, `logs/`
- Enabled Block All Public Access

**Why this folder structure:**
This follows the Data Lake pattern used at Amazon, Flipkart and JP Morgan:
- `raw/` — Data exactly as received from CoinGecko. Never modified. Source of truth.
- `processed/` — Data after PySpark transformations. Ready for analytics.
- `logs/` — Pipeline execution records for debugging and auditing.

**Why Block Public Access:**
Crypto price data and pipeline logs should never be publicly accessible. Only authenticated AWS services (EC2 with IAM role) should read/write this data.

---

### Step 4: Created AWS RDS PostgreSQL

**What I did:**
- Created `db.t4g.micro` RDS instance (free tier) in Mumbai
- Named it `crypto-rds-db`
- Set database name to `data_platform` to match local development
- Created security group `crypto-rds-sg` with inbound rules for EC2 IP and laptop IP

**Why RDS instead of PostgreSQL on EC2:**
RDS is a managed service — AWS handles backups, patching and availability automatically. Running PostgreSQL directly on EC2 requires manual maintenance, which is impractical in production. RDS also provides point-in-time recovery which is critical for financial data.

**Why separate security group for RDS:**
The security group acts as a firewall — only my EC2 instance (IP: 172.31.2.68) and my laptop can connect to RDS on port 5432. This follows the principle of least privilege.

---

### Step 5: Pushed Code to GitHub

**What I did:**
- Initialized Git in the project folder
- Created repository `rIS-spec/real-time-crypto-data-platform`
- Force-added `docker/docker-compose.yml` (was gitignored by default)
- Never pushed `.env` file — kept credentials off GitHub

**Why GitHub:**
GitHub is the standard for version control and enables clean deployment — just `git clone` on any server and the code is there. It also serves as a backup and portfolio showcase for recruiters.

**Why `.env` never goes to GitHub:**
The `.env` file contains database passwords and API keys. If pushed to a public repository, anyone can access your AWS account, database, and services. The `.env.example` file is pushed instead — it shows the structure without real credentials.

---

### Step 6: Cloned Project on EC2 and Added Swap Space

**What I did:**
- SSH'd into EC2 and ran `git clone`
- Added 1GB swap space before starting services

**Why swap space before starting services:**
t3.micro has only 1GB RAM. Running PostgreSQL + Zookeeper + Kafka + Airflow simultaneously requires more than 1GB. Swap space uses disk as overflow RAM — slower than real RAM but prevents Out of Memory crashes.

```bash
sudo dd if=/dev/zero of=/swapfile bs=128M count=8
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

---

### Step 7: Created `.env` File on EC2 and Started Docker Compose

**What I did:**
- Created `.env` file directly on EC2 (never via GitHub)
- Ran `docker-compose up -d` from the `docker/` folder

**Why `.env` created directly on EC2:**
Credentials never travel through GitHub. The `.env` file is created manually on the server — this is the correct production practice. In enterprise environments, AWS Secrets Manager or HashiCorp Vault would be used instead.

**Why Docker Compose:**
Docker Compose starts all 5 services (PostgreSQL, Kafka, Zookeeper, Airflow, Kafka UI) with a single command and manages networking between them automatically. Each service runs in an isolated container — a crash in one container doesn't affect others.

---

### Step 8: Started FastAPI and Streamlit with `nohup`

**What I did:**
```bash
# FastAPI — runs permanently in background
PYTHONPATH=/home/ec2-user/real-time-crypto-data-platform \
nohup python3 -m uvicorn api_service.main:app \
  --host 0.0.0.0 --port 8001 > fastapi.log 2>&1 &

# Streamlit — runs permanently in background
PYTHONPATH=/home/ec2-user/real-time-crypto-data-platform \
nohup streamlit run dashboard/app.py \
  --server.port 8501 --server.address 0.0.0.0 > streamlit.log 2>&1 &
```

**Why `nohup`:**
Without `nohup`, the process dies when you close the SSH terminal. `nohup` (no hangup) keeps the process running permanently even after the terminal session ends.

**Why `PYTHONPATH`:**
The Streamlit and FastAPI code imports from `api_service/` and `kafka_service/` modules. Without setting `PYTHONPATH` to the project root, Python cannot find these modules and raises `ModuleNotFoundError`.

**Why `> fastapi.log 2>&1`:**
Redirects both stdout and stderr to a log file. This lets me debug issues later with `cat fastapi.log` without needing to see live output.

**Why `&`:**
Runs the process in the background so the terminal remains free for other commands.

**Why `--host 0.0.0.0`:**
By default, FastAPI and Streamlit only listen on `localhost` (127.0.0.1) — accessible only from the server itself. `0.0.0.0` means "listen on all network interfaces" — making the service accessible from any external IP via the EC2 public IP.

---

### Deployment Architecture Summary

```
Your Browser / Any Device
         │
         │ HTTP
         ▼
AWS EC2 (3.108.53.215) — t3.micro, Mumbai
│
├── Port 8001 ──► FastAPI (uvicorn + nohup)
│                  └── Fetches from CoinGecko API
│                  └── Writes to Kafka + PostgreSQL
│
├── Port 8501 ──► Streamlit (nohup)
│                  └── Reads from PostgreSQL
│                  └── Shows live charts + ML results
│
└── Docker Compose Services:
    ├── postgres_db  (port 5432) — local pipeline database
    ├── zookeeper    (port 2181) — Kafka coordination
    ├── kafka_broker (port 9092) — event streaming
    ├── kafka_ui     (port 8090) — Kafka monitoring
    └── airflow      (port 8080) — pipeline orchestration
         │
AWS S3 (crypto-data-platform-arish) — raw data lake
AWS RDS (crypto-rds-db) — production cloud database
```

---

## Local Setup

### Prerequisites
- Python 3.12+
- Docker Desktop
- Java 11+ (for PySpark)
- Git

### 1. Clone the repository
```bash
git clone https://github.com/rIS-spec/real-time-crypto-data-platform.git
cd real-time-crypto-data-platform
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate  # Windows
source venv/bin/activate  # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Set up environment variables
```bash
cp .env.example .env
# Edit .env with your credentials
```

### 5. Start Docker services
```bash
cd docker
docker-compose up -d
```

### 6. Set PYTHONPATH (Windows PowerShell)
```powershell
$env:PYTHONPATH = "D:\Desktop\real-time-crypto-data-platform"
```

### 7. Start FastAPI
```bash
python -m uvicorn api_service.main:app --host 0.0.0.0 --port 8001 --reload
```

### 8. Run the pipeline
```bash
# Create Kafka topic
python kafka_service/topics.py

# Start producer (fetches prices → Kafka)
python kafka_service/producer.py

# Start consumer (Kafka → PostgreSQL)
python kafka_service/consumer.py
```

### 9. Start Streamlit dashboard
```bash
streamlit run dashboard/app.py --server.port 8501
```

---

## API Endpoints

| Method | Endpoint | Description |
|---|---|---|
| GET | `/` | Health check |
| GET | `/crypto/health` | API + DB connection status |
| GET | `/crypto/prices` | Fetch + store live prices for all 5 coins |
| GET | `/crypto/prices/history` | Price history from database |
| GET | `/crypto/prices/{coin_id}` | Price for a specific coin |

**Example response:**
```json
{
  "success": true,
  "message": "Live prices fetched successfully",
  "data": [
    {
      "coin_id": "bitcoin",
      "coin_name": "Bitcoin",
      "symbol": "BTC",
      "price_usd": 63433.0,
      "price_change_pct_24h": -1.08,
      "market_cap": 1268956839077.0,
      "fetched_at": "2026-06-05T05:30:19Z"
    }
  ],
  "total_coins": 5
}
```

---

## Database Schema

### crypto_events
```sql
CREATE TABLE crypto_events (
    id                   SERIAL PRIMARY KEY,
    coin_id              VARCHAR(50) NOT NULL,
    coin_name            VARCHAR(100),
    symbol               VARCHAR(20),
    price_usd            DECIMAL(20,8),
    price_change_24h     DECIMAL(10,4),
    price_change_pct_24h DECIMAL(10,4),
    market_cap           DECIMAL(25,2),
    volume_24h           DECIMAL(25,2),
    high_24h             DECIMAL(20,8),
    low_24h              DECIMAL(20,8),
    fetched_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### pipeline_logs
```sql
CREATE TABLE pipeline_logs (
    id               SERIAL PRIMARY KEY,
    pipeline_name    VARCHAR(100) NOT NULL,
    task_name        VARCHAR(100) NOT NULL,
    status           VARCHAR(20) CHECK (status IN ('running', 'success', 'failed')),
    started_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    finished_at      TIMESTAMP,
    duration_seconds DECIMAL(10,2),
    rows_processed   INTEGER DEFAULT 0,
    error_message    TEXT,
    dag_run_id       VARCHAR(200)
);
```

### ml_predictions
```sql
CREATE TABLE ml_predictions (
    id            SERIAL PRIMARY KEY,
    coin_id       VARCHAR(50),
    price_usd     DECIMAL(20,8),
    prediction    VARCHAR(50),
    confidence    DECIMAL(5,4),
    is_anomaly    BOOLEAN DEFAULT FALSE,
    anomaly_type  VARCHAR(100),
    model_version VARCHAR(50),
    predicted_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## ML Model

### Isolation Forest Anomaly Detection

I trained one Isolation Forest model per coin (5 models total) to detect unusual price movements.

**Why Isolation Forest:**
- Works well with small, unlabeled datasets — no need for labeled anomaly data
- Fast inference — suitable for real-time detection
- Interpretable — outputs anomaly scores that map directly to confidence values

**Features used:**
- `price_usd` — current price
- `price_change_pct_24h` — 24-hour price change percentage
- `volume_24h` — trading volume

**Configuration:**
```python
IsolationForest(
    n_estimators=100,
    contamination=0.1,   # expect 10% anomalies
    random_state=42
)
```

**Model storage:**
- Models saved as `.pkl` files to `ml_models/saved_models/`
- Also uploaded to `s3://crypto-data-platform-arish/models/` for cloud access

---

## AWS Infrastructure

| Resource | Details |
|---|---|
| EC2 Instance | i-0143385df9d43a86e, t3.micro, ap-south-1 |
| S3 Bucket | crypto-data-platform-arish (ap-south-1) |
| RDS Instance | crypto-rds-db, db.t4g.micro, PostgreSQL 18.3 |
| IAM Role | crypto-ec2-s3-role (AmazonS3FullAccess) |
| Security Group (EC2) | crypto-platform-sg |
| Security Group (RDS) | crypto-rds-sg |

---

## Author - Arish

- Email: arishmahammad8@gmail.com
- LinkedIn: [linkedin.com/in/arishmahammad](https://linkedin.com/in/arishmahammad)
- GitHub: [github.com/rIS-spec](https://github.com/rIS-spec)

---

*Built as a portfolio project targeting Data Engineer and ML Engineer roles at Amazon, Microsoft, Flipkart, Walmart and JP Morgan for 2027 placements.*
