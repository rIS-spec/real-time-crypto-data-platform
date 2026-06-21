# Real-Time Crypto Data Platform

An end-to-end, production-style data engineering pipeline that ingests live cryptocurrency prices, streams them through Kafka, processes them with PySpark, orchestrates the entire workflow with Airflow, and deploys it on AWS. Built to demonstrate the core skill set expected of a Data Engineer: Demonstrate ETL Pipelines, API integration, event streaming, distributed processing, workflow orchestration, cloud deployment and basic ML.

**Author:** Arish Mahammad

---

## Why This Project Exists

Most "crypto dashboard" tutorials stop at calling an API and plotting a chart. This project is intentionally built the way a real data platform would be — with durability, retries, idempotency, security and orchestration baked in from day one, not bolted on afterward. Every architectural decision below was made deliberately, and the reasoning is documented so it can be defended in a technical interview.

---

## Architecture Flowcharts

```
                     ┌─────────────────┐
                     │   CoinGecko API │
                     └────────┬────────┘
                              │
                     ┌────────▼─────────┐
                     │     FastAPI      │  (api_service/)
                     │  fetch + validate│
                     └────────┬─────────┘
                              │
                     ┌────────▼─────────┐
                     │   Kafka Producer │  (kafka_service/producer.py)
                     └────────┬─────────┘
                              │
                     ┌────────▼─────────┐
                     │  Kafka Topic     │
                     │  "crypto-events" │
                     └────┬─────────┬───┘
                          │         │
              ┌───────────▼──┐  ┌───▼─────────────────┐
              │ Kafka Consumer│  │ Spark Structured   │
              │ (Python)      │  │ Streaming          │
              └───────┬───────┘  └──────────┬─────────┘
                      │                     │
              ┌───────▼───────┐   ┌─────────▼─────────┐
              │ crypto_events │   │ streaming_results │
              │ (PostgreSQL)  │   │ (PostgreSQL)      │
              └───────┬───────┘   └───────────────────┘
                      │
              ┌───────▼───────────┐
              │ PySpark Batch Jobs│  (spark_processing/transformations.py)
              │ filter, groupBy,  │
              │ window functions  │
              └───────┬───────────┘
                      │
              ┌───────▼───────────┐
              │ crypto_aggregations│
              │ (PostgreSQL)       │
              └───────┬───────────┘
                      │
       ┌──────────────┼──────────────┐
       │              │              │
┌──────▼─────┐ ┌──────▼──────┐ ┌────▼────────┐
│ ML Anomaly  │ │ Streamlit    │ │ Airflow      │
│ Detection   │ │ Dashboard    │ │ Orchestration│
│(Isolation   │ │              │ │(schedules &  │
│ Forest)     │ │              │ │ monitors all │
│             │ │              │ │ of the above)│
└─────────────┘ └─────────────┘ └─────────────┘
```

The whole stack is containerized with Docker Compose and was deployed to AWS (EC2 + RDS + S3) to validate it runs outside a local dev environment.

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| API | FastAPI + Pydantic | Auto validation, auto docs, async support |
| Streaming | Apache Kafka | Decouples producers/consumers, durable buffer, replay support |
| Processing | PySpark (Structured Streaming + batch) | Distributed transformations, window functions, scales beyond single-machine pandas |
| Orchestration | Apache Airflow | Scheduling, retries, SLAs, dependency management, dataset-driven triggers |
| Storage | PostgreSQL (local Docker + AWS RDS) | Relational integrity, strong SQL support for analytics |
| Object Storage | AWS S3 | Raw data archive, partitioned storage |
| Compute | AWS EC2 | Hosts the live FastAPI + Streamlit services |
| ML | scikit-learn (Isolation Forest) | Unsupervised anomaly detection per coin |
| Dashboard | Streamlit | Lightweight, fast to iterate on |
| Containerization | Docker + Docker Compose | Identical environment locally and in production |

---

## Data Flow, Step by Step

1. **Ingestion** — FastAPI calls the CoinGecko API for 5 coins (BTC, ETH, SOL, DOGE, XRP), validates the response shape with Pydantic, and converts it into typed `CryptoPrice` objects.
2. **Streaming** — A Kafka producer serializes each price to JSON and publishes it to the `crypto-events` topic with `acks='all'` for delivery guarantees.
3. **Dual consumption** — Two independent consumers read the same topic (this is the core value of Kafka — multiple readers, one source of truth):
   - A lightweight Python consumer writes directly to the `crypto_events` table.
   - A Spark Structured Streaming job processes 10-second micro-batches and writes to `streaming_results`, demonstrating real-time stream processing with checkpointing.
4. **Batch processing** — A scheduled PySpark job reads `crypto_events`, applies filtering, aggregation (avg/max/min per coin), price categorization, and window functions (`rank`, `lag`, `row_number`) to compute price-change metrics, then writes the results to `crypto_aggregations`.
5. **Orchestration** — Airflow DAGs schedule the ingestion pipeline hourly, with HTTP/File/SQL sensors guarding each stage, retries with exponential backoff, SLA monitoring, and dataset-driven triggering (the analytics DAG only runs *after* new data actually lands, not on a fixed clock).
6. **Anomaly detection** — A per-coin Isolation Forest model flags unusual price movements.
7. **Dashboard** — Streamlit surfaces live prices, historical trends, pipeline health, and anomaly alerts.
8. **Cloud deployment** — The full stack was deployed to AWS EC2 (Docker Compose), with PostgreSQL on RDS and raw data archived to S3, to validate production readiness.

---

## Key Engineering Decisions.

**Why Kafka instead of inserting directly into PostgreSQL from FastAPI?**
Kafka adds durability (data survives a database outage), buffering (absorbs bursts faster than Postgres can write), and decoupling (multiple independent consumers — Python and Spark — read the same stream without FastAPI knowing or caring who's listening).

**Why both a plain Kafka consumer *and* a Spark Structured Streaming consumer?**
They serve different purposes. The Python consumer is the lightweight, reliable path for the primary table. The Spark Streaming job exists to demonstrate real-time distributed stream processing with micro-batches and checkpoint-based fault tolerance — a skill set directly relevant to large-scale streaming systems.

**Why is every database insert idempotent?**
Airflow retries failed tasks automatically. Every insert uses `ON CONFLICT DO NOTHING` keyed on `(coin_id, fetched_at)`, so re-running a task after a crash never produces duplicate rows — the pipeline is safe to retry without manual cleanup.

**Why dataset-driven scheduling instead of pure cron?**
The analytics DAG is triggered by an Airflow `Dataset` update, not a fixed time. If the ingestion DAG fails or runs late, analytics doesn't run on stale or missing data — it only fires once new data is actually confirmed written.

**Why PySpark for a project this size?**
To demonstrate distributed processing patterns that scale beyond a single machine: lazy evaluation, the Catalyst optimizer, partitioning, broadcast joins, and salting for skewed keys — all implemented and benchmarked against real crypto data, not synthetic examples.

**How are credentials handled?**
No password is hardcoded anywhere in the codebase. Local development reads from a gitignored `.env` file via Pydantic `BaseSettings`. Airflow tasks use `PostgresHook` with credentials stored in Airflow Connections, never in DAG code.

---

## Project Structure

```
real-time-data-platform/
├── api_service/          # FastAPI app: config, schemas, routes, CoinGecko fetcher
├── kafka_service/        # Kafka producer, consumer, topic setup
├── spark_processing/     # Batch transformations + Spark Structured Streaming
├── airflow_dags/         # DAGs: ingestion, analytics, dataset-driven triggers
├── ml_models/            # Isolation Forest anomaly detection per coin
├── dashboard/             # Streamlit app
├── warehouse/             # SQL schema (crypto_events, pipeline_logs, ml_predictions)
├── docker/                # docker-compose.yml and service configs
├── project_screenshots/   # Proof of AWS deployment (see below)
└── tests/
```

---

## AWS Deployment

The platform was deployed end-to-end on AWS to validate it runs outside local Docker:

- **EC2** (t3.micro, Mumbai region) — ran the full Docker Compose stack, with FastAPI and Streamlit publicly accessible.
- **RDS PostgreSQL** — managed database replacing the local Docker Postgres instance.
- **S3** — raw data storage with `raw/`, `processed/`, `logs/` structure.
- **IAM** — least-privilege roles for EC2 access to S3, separate from the root account.

**Note:** EC2, RDS, and S3 resources have since been **deleted** to avoid ongoing AWS charges after credits were used for learning/demo purposes — standard practice for a portfolio project. Screenshots below are proof of the working deployment:

| Screenshot | Description |
|---|---|
| `aws-ec2-instance.png` | EC2 instance running the deployed Docker stack |
| `aws-rds-postgres.png` | RDS PostgreSQL instance backing the platform |
| `aws-s3-bucket.png` | S3 bucket structure |
| `aws-s3-raw-data.png` | Raw data partitioning in S3 |
| `fastapi_swagger_ui.png` | Live FastAPI docs running on EC2 |
| `streamlit_dashboard_home.png` | Live dashboard running on EC2 |
| `crypto_ingest_dag_graph.png` | Airflow ingestion DAG graph view |
| `main_pipeline_dag_graph.png` | TaskFlow API version of the pipeline DAG |
| `analytics_dag_graph.png` | Dataset-triggered analytics DAG |
| `airflow_dags_overview.png` | All DAGs in the Airflow UI |

---

## Running Locally

```bash
git clone https://github.com/rIS-spec/real-time-crypto-data-platform.git
cd real-time-crypto-data-platform
cp .env.example .env        # fill in your own local credentials
docker-compose up -d
```

This starts PostgreSQL, Zookeeper, Kafka, Kafka UI and Airflow. FastAPI and Streamlit are run separately inside their virtual environment (see individual service READMEs for exact commands).

- FastAPI docs: `http://localhost:8000/docs`   Currently Not Available Becoz i remove from AWS
- Kafka UI: `http://localhost:8090`   Currently Not Available Becoz i remove from AWS
- Airflow UI: `http://localhost:8080` (admin/admin)

- But I Have Screenshots OF All Deployments INFO.
---

## What's Next

Next Plan to make Data Lake Kind of Projects.

---

## Contact

Email: arishmahammad8@gmail.com    |    Linkedin: www.linkedin.com/in/arishmahammad
