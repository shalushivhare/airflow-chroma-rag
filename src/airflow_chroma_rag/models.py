from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


class RawDocument(BaseModel):
    id: str
    title: str
    source: str
    url: str = ""
    text: str = ""

    @field_validator("id", "title", "source", "text")
    @classmethod
    def strip_strings(cls, value: str) -> str:
        return value.strip()


class ValidDocument(BaseModel):
    id: str
    title: str
    source: str
    url: str
    text: str = Field(min_length=1)


class Chunk(BaseModel):
    id: str
    document_id: str
    title: str
    source: str
    url: str
    text: str
    chunk_index: int


class ValidationReport(BaseModel):
    received: int
    accepted: int
    rejected: int
    errors: list[str] = Field(default_factory=list)
