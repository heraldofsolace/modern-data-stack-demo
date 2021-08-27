#!/usr/bin/env bash
up() {
  trap 'kill $ABID; kill $AFID; kill $SSID; exit' INT
  (
      echo "Starting Airbyte..."
      docker-compose -f docker-compose-airbyte.yaml down -v
      docker-compose -f docker-compose-airbyte.yaml up -d
  )&
  ABID=$!
  (
      echo "Starting Airflow..."
      docker-compose -f docker-compose-airflow.yaml down -v
      mkdir -p ./dags ./logs ./plugins
      echo -e "AIRFLOW_UID=$(id -u)\nAIRFLOW_GID=0" >> .env
      docker-compose -f docker-compose-airflow.yaml up airflow-init
      docker-compose -f docker-compose-airflow.yaml up -d
  )&
  AFID=$!
  (
      echo "Starting Superset..."
      cd superset
      docker-compose -f docker-compose-superset.yaml down -v
      docker-compose -f docker-compose-superset.yaml up -d
  )&
  SSID=$!

  echo "Waiting for applications to start..."
  wait
  echo "Access Airbyte at http://localhost:8000 and set up a connection."
  echo "Enter your Airbyte connection ID: "
  read connection_id
  # Set connection ID for DAG.
  docker-compose -f docker-compose-airflow.yaml run airflow-webserver airflow variables set 'AIRBYTE_CONNECTION_ID' "$connection_id"
  docker-compose -f docker-compose-airflow.yaml run airflow-webserver airflow connections add 'airbyte_example' --conn-uri 'airbyte://host.docker.internal:8000'
  echo "Enter your DBT account ID"
  read dbt_acc_id
  echo "Enter your DBT job ID"
  read dbt_job_id
  echo "Enter your DBT API token"
  read dbt_api_token
  docker-compose -f docker-compose-airflow.yaml run airflow-webserver airflow variables set 'DBT_ACCOUNT_ID' "$dbt_acc_id"
  docker-compose -f docker-compose-airflow.yaml run airflow-webserver airflow variables set 'DBT_JOB_ID' "$dbt_job_id"
  docker-compose -f docker-compose-airflow.yaml run airflow-webserver airflow variables set 'DBT_API_TOKEN' "$dbt_api_token"
  docker-compose -f docker-compose-airflow.yaml run airflow-webserver airflow connections add 'dbt_api' --conn-uri 'https://cloud.getdbt.com'

  echo "Access Airflow at http://localhost:8080 to kick off your Airbyte sync DAG."
  echo "Access Superset at http://localhost:8088 to set up your dashboards."
}

down() {
  echo "Stopping Airbyte..."
  docker-compose -f docker-compose-airbyte.yaml down -v
  echo "Stopping Airflow..."
  docker-compose -f docker-compose-airflow.yaml down -v
  echo "Stopping Superset..."
  docker-compose -f superset/docker-compose-superset.yaml down -v
}

case $1 in
  up)
    up
    ;;
  down)
    down
    ;;
  *)
    echo "Usage: $0 {up|down}"
    ;;
esac