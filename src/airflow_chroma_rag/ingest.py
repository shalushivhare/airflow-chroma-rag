from __future__ import annotations

import json
import logging
from pathlib import Path

import httpx

from airflow_chroma_rag.config import WIKI_TITLES, Settings, get_settings
from airflow_chroma_rag.models import RawDocument

logger = logging.getLogger(__name__)

WIKI_SUMMARY_URL = "https://en.wikipedia.org/api/rest_v1/page/summary/{title}"
FIXTURE_PATH = Path(__file__).resolve().parent / "data" / "sample_docs.json"


def fetch_wikipedia_summary(title: str, settings: Settings, client: httpx.Client) -> RawDocument:
    response = client.get(
        WIKI_SUMMARY_URL.format(title=title),
        headers={"User-Agent": settings.wiki_user_agent, "Accept": "application/json"},
        timeout=20.0,
    )
    response.raise_for_status()
    payload = response.json()
    extract = payload.get("extract") or ""
    return RawDocument(
        id=title,
        title=payload.get("title") or title.replace("_", " "),
        source="wikipedia",
        url=payload.get("content_urls", {}).get("desktop", {}).get("page", ""),
        text=extract,
    )


def load_fixture_documents(path: Path | None = None) -> list[RawDocument]:
    fixture = path or FIXTURE_PATH
    records = json.loads(fixture.read_text(encoding="utf-8"))
    return [RawDocument.model_validate(item) for item in records]


def ingest_public_dataset(
    settings: Settings | None = None,
    client: httpx.Client | None = None,
    fallback_to_fixture: bool = True,
) -> list[dict]:
    """Fetch Wikimedia summaries; fall back to bundled public-domain-style fixtures."""
    settings = settings or get_settings()
    docs: list[RawDocument] = []
    owns_client = client is None
    http_client = client or httpx.Client()
    try:
        for title in WIKI_TITLES:
            try:
                docs.append(fetch_wikipedia_summary(title, settings, http_client))
            except Exception as exc:  # noqa: BLE001 - keep DAG running with partial data
                logger.warning("Failed to fetch %s: %s", title, exc)
    finally:
        if owns_client:
            http_client.close()

    if not docs and fallback_to_fixture:
        logger.warning("Live ingest returned no documents; using local fixture corpus")
        docs = load_fixture_documents()

    return [doc.model_dump() for doc in docs]
