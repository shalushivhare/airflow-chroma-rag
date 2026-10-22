from __future__ import annotations

from airflow_chroma_rag.config import Settings, get_settings
from airflow_chroma_rag.models import Chunk, ValidDocument


def split_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    cleaned = " ".join(text.split())
    if len(cleaned) <= chunk_size:
        return [cleaned]

    chunks: list[str] = []
    start = 0
    while start < len(cleaned):
        end = min(start + chunk_size, len(cleaned))
        piece = cleaned[start:end].strip()
        if piece:
            chunks.append(piece)
        if end == len(cleaned):
            break
        start = max(end - overlap, start + 1)
    return chunks


def chunk_documents(records: list[dict], settings: Settings | None = None) -> list[dict]:
    settings = settings or get_settings()
    chunks: list[Chunk] = []
    for record in records:
        document = ValidDocument.model_validate(record)
        parts = split_text(document.text, settings.chunk_size, settings.chunk_overlap)
        for index, part in enumerate(parts):
            chunks.append(
                Chunk(
                    id=f"{document.id}:{index}",
                    document_id=document.id,
                    title=document.title,
                    source=document.source,
                    url=document.url,
                    text=part,
                    chunk_index=index,
                )
            )
    if not chunks:
        raise ValueError("Chunking produced no text windows")
    return [chunk.model_dump() for chunk in chunks]
