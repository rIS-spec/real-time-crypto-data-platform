# Real-Time Crypto Data Platform

A real-time data engineering platform for ingesting, streaming, processing, storing, and analyzing cryptocurrency market data.

The project is designed as a production-style portfolio system rather than a single analytics script. It demonstrates event streaming, fault handling, distributed processing, workflow orchestration, database persistence, ML-based anomaly detection, containerization, and measurable load testing.

> **Current status:** The AWS EC2 deployment used during development has been terminated. The project remains fully available for local execution with Docker and Python.

---

## Architecture

```text
                    ┌──────────────────────┐
                    │   CoinGecko API      │
                    │  Live Crypto Prices   │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │      FastAPI         │
                    │   Ingestion Layer    │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Kafka Producer    │
                    │  crypto-events topic │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │    Apache Kafka      │
                    │   Event Streaming    │
                    └──────────┬───────────┘
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
        ┌─────────────────┐        ┌─────────────────┐
        │ Kafka Consumer  │        │ PySpark Stream  │
        │                 │        │ Processing      │
        └────────┬────────┘        └────────┬────────┘
                 │                          │
                 ▼                          ▼
        ┌─────────────────┐        ┌─────────────────┐
        │   PostgreSQL    │        │ Transformations │
        │ Operational DB  │        │ & Aggregations  │
        └────────┬────────┘        └────────┬────────┘
                 │                          │
                 └─────────────┬────────────┘
                               ▼
                    ┌──────────────────────┐
                    │    Airflow DAGs      │
                    │ Orchestration &      │
                    │ Pipeline Monitoring  │
                    └──────────┬───────────┘
                               │
                 ┌─────────────┴─────────────┐
                 ▼                           ▼
        ┌─────────────────┐        ┌─────────────────┐
        │ Isolation       │        │   Streamlit     │
        │ Forest ML       │        │   Dashboard     │
        │ Anomaly Detection│       │   Visualization │
        └─────────────────┘        └─────────────────┘
```

---

## What the project demonstrates

- **Event-driven ingestion** using FastAPI and Apache Kafka
- **Reliable database persistence** using PostgreSQL transactions and retry handling
- **Streaming processing** with PySpark Structured Streaming
- **Workflow orchestration** with Apache Airflow
- **ML anomaly detection** using Isolation Forest
- **Containerized infrastructure** using Docker Compose
- **Observability and validation** through Kafka UI, pipeline logs, database reconciliation, and load tests
- **Cloud deployment experience** using AWS EC2, S3, RDS, and IAM
- **Performance benchmarking** with reproducible Kafka → PostgreSQL load-test scripts

---

## Technology Stack

| Layer | Technology | Role |
|---|---|---|
| API / Ingestion | FastAPI, Python | Fetch and expose crypto market data |
| Source | CoinGecko API | Live cryptocurrency prices |
| Streaming | Apache Kafka | Durable event streaming and buffering |
| Coordination | Zookeeper | Kafka coordination in the local setup |
| Processing | PySpark | Streaming transformations and aggregations |
| Orchestration | Apache Airflow | Workflow scheduling and monitoring |
| Database | PostgreSQL 15 | Persistent event and analytics storage |
| ML | Scikit-learn | Isolation Forest anomaly detection |
| Dashboard | Streamlit | Data and pipeline visualization |
| Containerization | Docker Compose | Local service orchestration |
| Cloud | AWS EC2, S3, RDS, IAM | Cloud deployment and storage |
| Validation | Python benchmark scripts | Throughput and reliability measurement |

---

## Real-Time Pipeline

The core data path is:

```text
CoinGecko
   ↓
FastAPI
   ↓
Kafka Producer
   ↓
Kafka: crypto-events
   ↓
Kafka Consumer
   ↓
PostgreSQL
   ↓
PySpark / Airflow
   ↓
Analytics + ML
   ↓
Streamlit
```

The Kafka layer separates ingestion from downstream consumers. This allows producers to continue publishing while consumers process events independently and provides the foundation for adding additional consumers later.

---

## Performance Validation

The project includes reproducible benchmark scripts under `benchmarks/`.

### Kafka → PostgreSQL load test

A controlled load test was performed with **1,000 synthetic crypto events** using the same event schema as the application.

| Metric | Result |
|---|---:|
| Events produced | 1,000 |
| Producer time | 0.286 s |
| Producer throughput | 3,493.37 events/s |
| Consumer events processed | 1,000 |
| Kafka offsets | 30–1029 |
| Consumer processing time | 3.231 s |
| Consumer throughput | **309.53 events/s** |
| Consumer failures | **0** |
| PostgreSQL rows before | 240 |
| PostgreSQL rows after | 1,240 |
| Rows added | **1,000** |

This validates that the controlled test produced 1,000 Kafka events and that the consumer persisted all 1,000 events into PostgreSQL without a failed write during the run.

> **Important:** The benchmark demonstrates the observed behavior of this local configuration. It should not be interpreted as a universal Kafka throughput limit or as a formal exactly-once guarantee.

### Benchmark scripts

```text
benchmarks/
├── benchmark_producer.py
├── benchmark_consumer.py
├── benchmark_1000_producer.py
└── benchmark_1000_consumer.py
```

---

## Reliability and Failure Handling

The consumer uses explicit PostgreSQL transaction handling:

```python
try:
    cursor.execute(insert_query, values)
    conn.commit()
except Exception:
    conn.rollback()
```

A failed PostgreSQL operation leaves the current transaction in an aborted state. Calling `rollback()` resets the transaction so the connection can safely retry the operation.

The consumer also implements database retry logic:

```text
Kafka message
     ↓
PostgreSQL INSERT
     ↓
   success ──► COMMIT
     │
   failure
     ↓
ROLLBACK
     ↓
retry up to 3 times
```

Kafka producer configuration includes:

- `acks=all`
- `retries=3`

The consumer uses manual database commits rather than relying on PostgreSQL autocommit.

> Kafka offset management and database commits are separate concerns. The current implementation should therefore not be described as a formal end-to-end exactly-once system.

---

## Why Kafka?

Kafka was selected instead of directly writing every API response into PostgreSQL for three main reasons:

1. **Durability** — events are persisted by Kafka and can be consumed again.
2. **Buffering** — Kafka absorbs differences between producer and consumer processing rates.
3. **Decoupling** — multiple independent consumers can read the same event stream.

For this project, Kafka also provides a clean foundation for adding additional consumers such as Spark processing, monitoring, or future downstream analytics.

---

## Why Kafka instead of RabbitMQ?

Kafka was a better fit for this project because the workload is an event-streaming/data-platform workload rather than a traditional task queue.

- **Replay:** Kafka retains events so consumers can re-read historical data.
- **Throughput:** Kafka is designed for high-throughput event streaming.
- **Multiple consumers:** Independent consumer groups can consume the same topic for different purposes.
- **Data pipeline fit:** Kafka integrates naturally with streaming analytics and Spark-based processing.

RabbitMQ would be a strong choice for task queues and job distribution, but Kafka better matches the event-streaming requirements of this platform.

---

## Kafka Ordering

Kafka guarantees ordering **within a partition**, not across multiple partitions.

For example, messages with the same key can be routed to the same partition:

```python
producer.send(
    topic="crypto-events",
    key="bitcoin",
    value=data
)
```

This preserves the order of Bitcoin events within that partition.

The local benchmark topic used **one partition**, so the benchmark did not measure multi-partition parallelism.

---

## Project Structure

```text
real-time-crypto-data-platform/
│
├── api_service/
│   ├── main.py
│   ├── routes.py
│   ├── schemas.py
│   ├── config.py
│   └── fetchers/
│       └── crypto.py
│
├── kafka_service/
│   ├── producer.py
│   ├── consumer.py
│   └── topics.py
│
├── spark_processing/
│   ├── transformations.py
│   ├── aggregations.py
│   └── spark_stream.py
│
├── airflow_dags/
│   ├── ingest_dag.py
│   ├── main_pipeline_dag.py
│   └── analytics_dag.py
│
├── ml_models/
│   ├── train_model.py
│   ├── predict.py
│   └── features.py
│
├── dashboard/
│   └── app.py
│
├── benchmarks/
│   ├── benchmark_producer.py
│   ├── benchmark_consumer.py
│   ├── benchmark_1000_producer.py
│   └── benchmark_1000_consumer.py
│
├── docker/
│   ├── Dockerfile
│   └── airflow_dags/
│
├── tests/
├── requirements.txt
├── .env.example
└── README.md
```

---

## Database

The main PostgreSQL event table is:

```sql
CREATE TABLE crypto_events (
    id SERIAL PRIMARY KEY,
    coin_id VARCHAR(50) NOT NULL,
    coin_name VARCHAR(100),
    symbol VARCHAR(20),
    price_usd DECIMAL(20,8),
    price_change_24h DECIMAL(10,4),
    market_cap DECIMAL(25,2),
    volume_24h DECIMAL(25,2),
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

Additional pipeline and ML tables are used for execution tracking and anomaly predictions.

---

## Machine Learning

### Isolation Forest

The platform uses Isolation Forest for unsupervised anomaly detection.

The model is suitable for this use case because cryptocurrency price data does not come with a reliable labelled anomaly dataset.

Example features include:

- `price_usd`
- `price_change_pct_24h`
- `volume_24h`

The model identifies observations that are statistically unusual compared with normal market behavior.

---

## Airflow Orchestration

Airflow is used to coordinate data-engineering workflows such as:

```text
Fetch data
    ↓
Produce events
    ↓
Process data
    ↓
Store results
    ↓
Run analytics / ML
```

The project uses separate DAGs for ingestion, the main pipeline, and analytics.

Airflow provides:

- task dependencies
- retries
- scheduling
- execution history
- failure visibility
- pipeline monitoring

---

## Docker

The infrastructure is containerized to make the local environment reproducible.

The Docker-based stack includes services such as:

```text
PostgreSQL
Kafka
Zookeeper
Kafka UI
Airflow
```

The application services can be run alongside this infrastructure from the project environment.

---

## AWS Deployment

The project was previously deployed on AWS EC2 using Docker-based infrastructure.

AWS services used during development included:

- **EC2** — compute environment
- **S3** — raw/processed data storage
- **RDS PostgreSQL** — managed database
- **IAM** — access control

The EC2 instance used for the public demo has since been terminated, so the previous public API/dashboard URLs are no longer presented as live endpoints.

For a production deployment, credentials should be managed through AWS Secrets Manager or another dedicated secrets-management solution rather than committed to source control.

---

## Local Setup

### Prerequisites

- Python 3.12+
- Docker Desktop
- Git
- Java 11+ for PySpark

### 1. Clone

```bash
git clone https://github.com/rIS-spec/real-time-crypto-data-platform.git
cd real-time-crypto-data-platform
```

### 2. Create virtual environment

Windows:

```cmd
python -m venv venv
venv\Scripts\activate
```

macOS / Linux:

```bash
python -m venv venv
source venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure environment

```bash
copy .env.example .env
```

Add the required local database, Kafka, and API configuration to `.env`.

**Never commit `.env` to GitHub.**

### 5. Start infrastructure

From the project root:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Verify:

```bash
docker compose -f docker/docker-compose.yml ps
```

### 6. Create Kafka topic

```bash
python -m kafka_service.topics
```

### 7. Start producer

```bash
python -m kafka_service.producer
```

### 8. Start consumer

```bash
python -m kafka_service.consumer
```

### 9. Run benchmark

```bash
python benchmarks/benchmark_1000_producer.py
python benchmarks/benchmark_1000_consumer.py
```

---

## API Endpoints

| Method | Endpoint | Purpose |
|---|---|---|
| GET | `/` | Service health check |
| GET | `/crypto/health` | API / database health |
| GET | `/crypto/prices` | Fetch current crypto prices |
| GET | `/crypto/prices/history` | Retrieve historical prices |
| GET | `/crypto/prices/{coin_id}` | Retrieve data for one coin |

---

## Engineering Decisions

### Why PostgreSQL?

PostgreSQL provides transactional persistence, strong SQL support, and a reliable relational model for the structured market events produced by the pipeline.

### Why PySpark?

PySpark provides distributed DataFrame processing and Structured Streaming capabilities, making the processing layer extensible beyond the small local workload.

### Why Airflow?

Airflow separates workflow orchestration from application code and provides scheduling, dependencies, retries, and execution monitoring.

### Why Docker?

Docker makes the Kafka, PostgreSQL, Airflow, and supporting infrastructure reproducible across environments.

### Why FastAPI?

FastAPI provides a lightweight typed API layer with Pydantic validation and good support for asynchronous service development.

---

## Lessons From Building the System

Some of the main engineering issues encountered during development included:

### 1. Python package naming conflict

A local `kafka` package conflicted with the `kafka-python` dependency. Renaming the application package to `kafka_service` removed the import ambiguity.

### 2. Pydantic datetime serialization

Pydantic objects containing `datetime` values need JSON-compatible serialization before being sent through Kafka.

```python
message = coin.model_dump(mode="json")
```

### 3. PostgreSQL transaction recovery

A failed SQL operation aborts the active PostgreSQL transaction. The connection must be rolled back before another SQL operation can safely execute.

### 4. Kafka consumer offsets

Consumer groups retain their offsets. Understanding `group_id`, `auto_offset_reset`, and manual offset management was necessary when testing replay and controlled benchmarks.

### 5. PySpark on Windows

Local PySpark development required the appropriate Java and Hadoop/Windows environment configuration.

### 6. Docker import paths

Code that works locally can fail inside a container when Python module paths differ. The container environment must explicitly expose the application package structure.

### 7. Resource constraints

Running Kafka, PostgreSQL, Airflow, and other services on a small EC2 instance required JVM/resource tuning and swap-space configuration.

---

## Portfolio Highlights

This project demonstrates experience across the complete data-engineering lifecycle:

```text
API Ingestion
      ↓
Event Streaming
      ↓
Reliable Persistence
      ↓
Distributed Processing
      ↓
Workflow Orchestration
      ↓
Analytics / ML
      ↓
Visualization
      ↓
Containerization
      ↓
Cloud Deployment
      ↓
Performance Validation
```

The strongest measured result from the current local benchmark is:

> **309.53 events/sec through the Kafka → PostgreSQL consumer path while processing 1,000 events with 0 failed writes.**

---

## Author

**Arish Mahammad**

B.Tech CSE — Data Science

- LinkedIn: https://www.linkedin.com/in/arishmahammad/
- GitHub: https://github.com/rIS-spec
- Email: arishmahammad8@gmail.com

---

## Repository

https://github.com/rIS-spec/real-time-crypto-data-platform
