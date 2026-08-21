#!/bin/bash
set -e

echo "Waiting for PostgreSQL..."
sleep 10

echo "Initializing Airflow database..."
airflow db init

echo "Creating admin user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

echo "Starting Airflow Webserver..."
airflow webserver --port 8080 &
WEBSERVER_PID=$!

echo "Starting Airflow Scheduler..."
airflow scheduler &
SCHEDULER_PID=$!

echo "Airflow started. Monitoring processes..."
while kill -0 $WEBSERVER_PID 2>/dev/null && kill -0 $SCHEDULER_PID 2>/dev/null; do
    sleep 5
done

echo "ERROR: Airflow Webserver or Scheduler died! Exiting container."
exit 1