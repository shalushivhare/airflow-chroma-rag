from __future__ import annotations

from typing import Optional

from pydantic_settings import BaseSettings, SettingsConfigDict


WIKI_TITLES: tuple[str, ...] = (
    "Apache_Airflow",
    "Docker_(software)",
    "Vector_database",
    "Retrieval-augmented_generation",
    "Embedding_(machine_learning)",
    "Extract,_transform,_load",
    "Data_pipeline",
    "Python_(programming_language)",
)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    chroma_host: str = "localhost"
    chroma_port: int = 8000
    chroma_collection: str = "knowledge"
    wiki_user_agent: str = "airflow-chroma-rag/1.0 (portfolio project)"
    min_text_chars: int = 80
    chunk_size: int = 700
    chunk_overlap: int = 120
    openai_api_key: Optional[str] = None
    openai_model: str = "gpt-4o-mini"


def get_settings() -> Settings:
    return Settings()
