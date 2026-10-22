from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from airflow_chroma_rag.rag import answer_question

app = FastAPI(
    title="airflow-chroma-rag",
    description="Extractive RAG over the corpus loaded by the Airflow DAG.",
    version="0.1.0",
)


class QueryRequest(BaseModel):
    question: str = Field(min_length=3)
    top_k: int = Field(default=4, ge=1, le=10)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/query")
def query(payload: QueryRequest) -> dict:
    try:
        return answer_question(payload.question, top_k=payload.top_k)
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=str(exc)) from exc
