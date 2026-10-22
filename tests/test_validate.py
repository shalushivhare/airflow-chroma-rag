from airflow_chroma_rag.config import Settings
from airflow_chroma_rag.validate import validate_documents


def test_validate_keeps_long_text_and_drops_short():
    records = [
        {
            "id": "ok",
            "title": "Airflow",
            "source": "test",
            "url": "https://example.com",
            "text": "Apache Airflow is a workflow orchestrator used to schedule data pipelines. " * 2,
        },
        {
            "id": "short",
            "title": "Nope",
            "source": "test",
            "url": "",
            "text": "tiny",
        },
    ]
    valid, report = validate_documents(records, settings=Settings(min_text_chars=80))
    assert report["accepted"] == 1
    assert report["rejected"] == 1
    assert valid[0]["id"] == "ok"


def test_validate_rejects_empty_corpus():
    try:
        validate_documents(
            [{"id": "x", "title": "x", "source": "t", "text": "short"}],
            settings=Settings(min_text_chars=80),
        )
    except ValueError as exc:
        assert "empty corpus" in str(exc)
    else:
        raise AssertionError("expected ValueError")
