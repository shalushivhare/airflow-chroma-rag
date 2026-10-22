from airflow_chroma_rag.chroma_store import replace_collection
from airflow_chroma_rag.rag import answer_question


class FakeCollection:
    def __init__(self):
        self.ids: list[str] = []
        self.documents: list[str] = []
        self.metadatas: list[dict] = []

    def upsert(self, ids, documents, metadatas):
        self.ids = ids
        self.documents = documents
        self.metadatas = metadatas

    def query(self, query_texts, n_results, include=None):
        hits = list(zip(self.documents, self.metadatas))[:n_results]
        return {
            "documents": [[doc for doc, _ in hits]],
            "metadatas": [[meta for _, meta in hits]],
            "distances": [[0.1 for _ in hits]],
        }


class FakeClient:
    def __init__(self):
        self.collection = FakeCollection()
        self.deleted = False

    def delete_collection(self, name):
        self.deleted = True
        self.collection = FakeCollection()

    def get_or_create_collection(self, name, metadata=None):
        return self.collection


def test_replace_collection_is_idempotent_full_refresh():
    client = FakeClient()
    chunks = [
        {
            "id": "a:0",
            "document_id": "a",
            "title": "A",
            "source": "t",
            "url": "https://example.com",
            "text": "Airflow schedules DAG tasks.",
            "chunk_index": 0,
        }
    ]
    replace_collection(chunks, client=client)
    replace_collection(chunks, client=client)
    assert client.deleted is True
    assert client.collection.ids == ["a:0"]


def test_extractive_query_returns_sources():
    collection = FakeCollection()
    collection.upsert(
        ids=["a:0"],
        documents=["Chroma stores embeddings for similarity search."],
        metadatas=[{"title": "Vector database", "url": "https://example.com", "source": "t", "chunk_index": 0}],
    )
    result = answer_question("What is Chroma used for?", collection=collection)
    assert result["mode"] == "extractive"
    assert "embeddings" in result["answer"]
    assert result["sources"][0]["title"] == "Vector database"
