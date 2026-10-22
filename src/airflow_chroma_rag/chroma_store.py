from __future__ import annotations

from typing import Any, Protocol

import chromadb

from airflow_chroma_rag.config import Settings, get_settings


class CollectionLike(Protocol):
    def upsert(self, ids: list[str], documents: list[str], metadatas: list[dict]) -> None: ...

    def query(
        self, query_texts: list[str], n_results: int, include=None
    ) -> dict[str, Any]: ...


def get_client(settings: Settings | None = None):
    settings = settings or get_settings()
    return chromadb.HttpClient(host=settings.chroma_host, port=settings.chroma_port)


def get_collection(client=None, settings: Settings | None = None):
    settings = settings or get_settings()
    chroma = client or get_client(settings)
    return chroma.get_or_create_collection(
        name=settings.chroma_collection,
        metadata={"hnsw:space": "cosine"},
    )


def replace_collection(chunks: list[dict], client=None, settings: Settings | None = None) -> int:
    """Full-refresh load so DAG reruns stay idempotent for this small corpus."""
    settings = settings or get_settings()
    chroma = client or get_client(settings)
    try:
        chroma.delete_collection(settings.chroma_collection)
    except Exception:  # noqa: BLE001 - collection may not exist yet
        pass
    collection = get_collection(chroma, settings)
    collection.upsert(
        ids=[chunk["id"] for chunk in chunks],
        documents=[chunk["text"] for chunk in chunks],
        metadatas=[
            {
                "document_id": chunk["document_id"],
                "title": chunk["title"],
                "source": chunk["source"],
                "url": chunk["url"],
                "chunk_index": chunk["chunk_index"],
            }
            for chunk in chunks
        ],
    )
    return len(chunks)


def query_collection(
    question: str,
    top_k: int = 4,
    collection: CollectionLike | None = None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    target = collection or get_collection(settings=settings)
    return target.query(
        query_texts=[question],
        n_results=top_k,
        include=["documents", "metadatas", "distances"],
    )
