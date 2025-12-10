import logging
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from google.cloud import bigquery

logging.getLogger().setLevel(logging.INFO)

def run_simple_query(**context):
    """
    Query BigQuery แบบง่ายที่สุด
    """
    logging.info("▶ Starting BigQuery Query...")

    client = bigquery.Client()
    # project: the1-insight-{WORKSPACE_ENV}insight.events_consents
    query = """ SELECT * FROM `the1-insight-stg.insight.events_consents` """

    result = client.query(query).result()
    logging.info(f"Query Results: {result}")
    for row in result:
        logging.info(f"result: {row}")
    logging.info("✔ BigQuery query completed.")
    # [WriteToBigLakeDoFn] output: {'personasId': '0044a849-4da7-46cc-919c-4264dbda38d3#1-1580295796#ac626272-2dfb-4060-81a4-b733e5c319c4', 'memberId': '1-1580295796', 'consents': '{"PWB": {"suppression": {"email": true}}, "T1C TH": {"consentFlag": "Y", "consentVersion": 1.1, "consentDate": 1764752044963}}', 'processDate': '2025-12-09 14:39:15.607'}
    insert_query = """ 
    INSERT INTO `the1-insight-stg.insight.events_consents` 
    VALUES ('0044a849-4da7-46cc-919c-4264dbda38d3#1-1580295796#ac626272-2dfb-4060-81a4-b733e5c319c4', '1-1580295796', '{"PWB": {"suppression": {"email": true}}, "T1C TH": {"consentFlag": "Y", "consentVersion": 1.1, "consentDate": 1764752044963}}', '2025-12-09 14:39:15.607')
    """

default_args = {
    "owner": "data-team",
    "start_date": days_ago(1),
}

with DAG(
    dag_id="simple_bigquery_query_dag",
    default_args=default_args,
    schedule_interval=None,     # รันเอง / manual trigger
    catchup=False,
    tags=["bigquery", "simple"],
) as dag:

    query_task = PythonOperator(
        task_id="run_bigquery_query",
        python_callable=run_simple_query,
    )

