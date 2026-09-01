#!/bin/bash

# Migrate database
airflow db migrate

# Create admin user if it doesn't exist
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin 2>/dev/null || true

# Start Airflow
exec airflow standalone
