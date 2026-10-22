import json

import httpx

from airflow_chroma_rag.config import Settings
from airflow_chroma_rag.ingest import ingest_public_dataset, load_fixture_documents


class _Transport(httpx.BaseTransport):
    def __init__(self, fail: bool = False):
        self.fail = fail

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        if self.fail:
            return httpx.Response(500, text="nope")
        title = request.url.path.rsplit("/", 1)[-1]
        payload = {
            "title": title.replace("_", " "),
            "extract": "Apache Airflow is an open-source platform to programmatically author, schedule, and monitor workflows. " * 3,
            "content_urls": {"desktop": {"page": f"https://en.wikipedia.org/wiki/{title}"}},
        }
        return httpx.Response(200, json=payload)


def test_fixture_corpus_loads():
    docs = load_fixture_documents()
    assert {doc.id for doc in docs} >= {"Apache_Airflow", "too_short"}


def test_ingest_uses_http_client():
    client = httpx.Client(transport=_Transport())
    docs = ingest_public_dataset(
        settings=Settings(),
        client=client,
        fallback_to_fixture=False,
    )
    assert len(docs) >= 1
    assert docs[0]["source"] == "wikipedia"


def test_ingest_falls_back_to_fixture():
    client = httpx.Client(transport=_Transport(fail=True))
    docs = ingest_public_dataset(settings=Settings(), client=client, fallback_to_fixture=True)
    assert any(doc["source"] == "fixture" for doc in docs)
    json.dumps(docs)
