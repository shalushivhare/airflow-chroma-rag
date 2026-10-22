from __future__ import annotations

from airflow_chroma_rag.config import Settings, get_settings
from airflow_chroma_rag.models import RawDocument, ValidDocument, ValidationReport


def validate_documents(
    records: list[dict],
    settings: Settings | None = None,
) -> tuple[list[dict], dict]:
    settings = settings or get_settings()
    accepted: list[ValidDocument] = []
    errors: list[str] = []

    for record in records:
        try:
            raw = RawDocument.model_validate(record)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"schema:{exc}")
            continue

        if len(raw.text) < settings.min_text_chars:
            errors.append(f"{raw.id}: text shorter than {settings.min_text_chars} characters")
            continue
        if not raw.title:
            errors.append(f"{raw.id}: missing title")
            continue

        accepted.append(
            ValidDocument(
                id=raw.id,
                title=raw.title,
                source=raw.source,
                url=raw.url,
                text=raw.text,
            )
        )

    if not accepted:
        raise ValueError("Validation rejected every record; refusing to load an empty corpus")

    report = ValidationReport(
        received=len(records),
        accepted=len(accepted),
        rejected=len(records) - len(accepted),
        errors=errors,
    )
    return [doc.model_dump() for doc in accepted], report.model_dump()
