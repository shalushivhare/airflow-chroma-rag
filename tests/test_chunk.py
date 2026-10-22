from airflow_chroma_rag.chunk import chunk_documents, split_text
from airflow_chroma_rag.config import Settings


def test_split_text_respects_overlap():
    text = "abcdefghijklmnopqrstuvwxyz"
    parts = split_text(text, chunk_size=10, overlap=3)
    assert parts[0] == "abcdefghij"
    assert parts[1].startswith("hij")


def test_chunk_documents_builds_stable_ids():
    records = [
        {
            "id": "doc-1",
            "title": "Doc",
            "source": "test",
            "url": "https://example.com",
            "text": "word " * 200,
        }
    ]
    chunks = chunk_documents(records, settings=Settings(chunk_size=80, chunk_overlap=10))
    assert chunks[0]["id"] == "doc-1:0"
    assert chunks[1]["id"] == "doc-1:1"
    assert all(chunk["document_id"] == "doc-1" for chunk in chunks)
