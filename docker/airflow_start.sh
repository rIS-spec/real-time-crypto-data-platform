#!/bin/bash
set -e  # Exit immediately if any command fails

# Wait for PostgreSQL to be fully ready (optional safety)
echo "Waiting for PostgreSQL..."
sleep 10

# Initialize the Airflow database (only runs once, safe to run multiple times)
echo "Initializing Airflow database..."
airflow db init

# Create admin user (only if it doesn't exist, safe to run multiple times)
echo "Creating admin user..."
airflow users create \
    --username admin \
    --password admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com || true

# Start the Webserver in the background
echo "Starting Airflow Webserver..."
airflow webserver --port 8080 &
WEBSERVER_PID=$!

# Start the Scheduler in the background
echo "Starting Airflow Scheduler..."
airflow scheduler &
SCHEDULER_PID=$!


# This loop checks if BOTH processes are still alive.
# If either one dies, the loop breaks and the script exits.
echo "Airflow started. Monitoring processes..."

while kill -0 $WEBSERVER_PID 2>/dev/null && kill -0 $SCHEDULER_PID 2>/dev/null; do
    sleep 5
done

# If we reach here, one of the processes died.
echo "ERROR: Airflow Webserver or Scheduler died! Exiting container."
exit 1