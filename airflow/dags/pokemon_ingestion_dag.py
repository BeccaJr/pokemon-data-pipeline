from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery

import requests
import time
import logging
import json
import os
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential

from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


BASE_URL = "https://pokeapi.co/api/v2/pokemon"
PROJECT_ID = "pokemon-data-pipeline-492719"
DATASET = "pokemon_raw"
TABLE = "pokemons"


# =========================
# RETRY HTTP
# =========================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2))
def fetch_url(url):
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.json()


# =========================
# GET URLS
# =========================
def get_pokemon_urls(**context):
    urls = []
    next_url = f"{BASE_URL}?limit=100&offset=0"

    while next_url:
        data = fetch_url(next_url)
        urls.extend([p["url"] for p in data["results"]])
        next_url = data["next"]

    logging.info(f"Total de URLs coletadas: {len(urls)}")
    context["ti"].xcom_push(key="pokemon_urls", value=urls)


# =========================
# SAVE FILE
# =========================
def save_batch_to_file(batch, file_path):
    with open(file_path, "w") as f:
        for row in batch:
            f.write(json.dumps(row) + "\n")


# =========================
# LOAD VIA BIGQUERY JOB
# =========================
def load_to_bq(file_path):
    hook = BigQueryHook(gcp_conn_id="google_cloud_default")
    client = hook.get_client()

    table_id = f"{PROJECT_ID}.{DATASET}.{TABLE}"

    with open(file_path, "rb") as f:
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

        job = client.load_table_from_file(
            f,
            table_id,
            job_config=job_config,
        )

    job.result()

    logging.info(f"Arquivo carregado com sucesso: {file_path}")

    # limpa arquivo depois
    os.remove(file_path)


# =========================
# EXTRACT + LOAD
# =========================
def extract_and_load(**context):
    urls = context["ti"].xcom_pull(key="pokemon_urls")

    if not urls:
        raise Exception("Nenhuma URL recebida do XCom")

    batch = []
    batch_size = 50  # agora pode aumentar (não é streaming)

    for i, url in enumerate(urls):
        data = fetch_url(url)

        # remove payload pesado
        data.pop("moves", None)

        record = {
            "id": data["id"],
            "name": data["name"],
            "raw_payload": data,  # agora pode ser dict (não precisa dumps)
            "ingestion_timestamp": datetime.utcnow().isoformat(),
        }

        batch.append(record)

        time.sleep(0.1)

        if len(batch) >= batch_size:
            file_path = f"/tmp/pokemon_batch_{i}.json"

            save_batch_to_file(batch, file_path)
            load_to_bq(file_path)

            batch = []
            logging.info(f"Batch processado até índice {i}")

    if batch:
        file_path = f"/tmp/pokemon_batch_final.json"
        save_batch_to_file(batch, file_path)
        load_to_bq(file_path)

    logging.info("Ingestão finalizada com sucesso")


# =========================
# DAG
# =========================
with DAG(
    dag_id="pokemon_ingestion",
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["pokemon", "ingestion", "bigquery"],
) as dag:

    get_urls = PythonOperator(
        task_id="get_pokemon_urls",
        python_callable=get_pokemon_urls,
    )

    extract_load = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
    )

    get_urls >> extract_load
