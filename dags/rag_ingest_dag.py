from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow_chroma_rag.pipeline import chunk_task, ingest_task, load_task, validate_task


def _ingest(**context):
    docs = ingest_task()
    context["ti"].xcom_push(key="raw_docs", value=docs)


def _validate(**context):
    raw_docs = context["ti"].xcom_pull(task_ids="ingest_public_dataset", key="raw_docs")
    valid = validate_task(raw_docs)
    context["ti"].xcom_push(key="valid_docs", value=valid)


def _chunk(**context):
    valid_docs = context["ti"].xcom_pull(task_ids="validate_records", key="valid_docs")
    chunks = chunk_task(valid_docs)
    context["ti"].xcom_push(key="chunks", value=chunks)


def _load(**context):
    chunks = context["ti"].xcom_pull(task_ids="chunk_documents", key="chunks")
    result = load_task(chunks)
    context["ti"].xcom_push(key="load_result", value=result)


with DAG(
    dag_id="rag_ingest_pipeline",
    description="Ingest public Wikipedia summaries, validate, chunk, and load Chroma for RAG.",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["rag", "chroma", "portfolio"],
) as dag:
    ingest = PythonOperator(task_id="ingest_public_dataset", python_callable=_ingest)
    validate = PythonOperator(task_id="validate_records", python_callable=_validate)
    chunk = PythonOperator(task_id="chunk_documents", python_callable=_chunk)
    load = PythonOperator(task_id="load_chroma", python_callable=_load)

    ingest >> validate >> chunk >> load
