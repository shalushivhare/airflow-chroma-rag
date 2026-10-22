from __future__ import annotations

import json
import os
from typing import Any
from urllib import request as urllib_request

from airflow_chroma_rag.chroma_store import query_collection
from airflow_chroma_rag.config import Settings, get_settings


def _extractive_answer(documents: list[str]) -> str:
    if not documents:
        return "No matching context was found in the knowledge base. Run the Airflow DAG first."
    numbered = [f"[{idx + 1}] {doc}" for idx, doc in enumerate(documents)]
    return "\n\n".join(numbered)


def _maybe_llm_answer(question: str, context: str, settings: Settings):
    api_key = settings.openai_api_key or os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None
    payload = {
        "model": settings.openai_model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Answer using only the provided context. If the context is insufficient, say so. "
                    "Cite snippet numbers like [1]."
                ),
            },
            {"role": "user", "content": f"Question: {question}\n\nContext:\n{context}"},
        ],
        "temperature": 0.2,
    }
    req = urllib_request.Request(
        "https://api.openai.com/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        method="POST",
    )
    with urllib_request.urlopen(req, timeout=30) as response:
        body = json.loads(response.read().decode("utf-8"))
    return body["choices"][0]["message"]["content"]


def answer_question(
    question: str,
    top_k: int = 4,
    collection=None,
    settings: Settings | None = None,
) -> dict[str, Any]:
    settings = settings or get_settings()
    raw = query_collection(question, top_k=top_k, collection=collection, settings=settings)
    documents = (raw.get("documents") or [[]])[0]
    metadatas = (raw.get("metadatas") or [[]])[0]
    distances = (raw.get("distances") or [[]])[0]
    extractive = _extractive_answer(documents)
    llm_answer = None
    try:
        llm_answer = _maybe_llm_answer(question, extractive, settings)
    except Exception:  # noqa: BLE001 - RAG should still return retrieved context
        llm_answer = None

    sources = []
    for meta, distance in zip(metadatas, distances):
        sources.append(
            {
                "title": meta.get("title"),
                "url": meta.get("url"),
                "source": meta.get("source"),
                "chunk_index": meta.get("chunk_index"),
                "distance": distance,
            }
        )
    return {
        "question": question,
        "answer": llm_answer or extractive,
        "mode": "generative" if llm_answer else "extractive",
        "sources": sources,
    }
