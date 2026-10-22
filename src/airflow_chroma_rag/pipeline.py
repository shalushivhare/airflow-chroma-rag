from __future__ import annotations

from airflow_chroma_rag.chroma_store import replace_collection
from airflow_chroma_rag.chunk import chunk_documents
from airflow_chroma_rag.ingest import ingest_public_dataset
from airflow_chroma_rag.validate import validate_documents


def ingest_task() -> list[dict]:
    return ingest_public_dataset()


def validate_task(records: list[dict]) -> list[dict]:
    valid, report = validate_documents(records)
    # Airflow logs the report via the return value / XCom
    print(f"validation_report={report}")
    return valid


def chunk_task(records: list[dict]) -> list[dict]:
    return chunk_documents(records)


def load_task(chunks: list[dict]) -> dict:
    loaded = replace_collection(chunks)
    return {"upserted_chunks": loaded}
