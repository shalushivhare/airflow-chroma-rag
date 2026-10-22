from pathlib import Path


def test_dag_wires_expected_tasks():
    dag_source = Path("dags/rag_ingest_dag.py").read_text(encoding="utf-8")
    assert 'dag_id="rag_ingest_pipeline"' in dag_source
    for task_id in (
        "ingest_public_dataset",
        "validate_records",
        "chunk_documents",
        "load_chroma",
    ):
        assert task_id in dag_source
    assert "ingest >> validate >> chunk >> load" in dag_source
