from datetime import datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_validator, model_validator

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# Your task is to define the Unified Schema for all sources.
# This is v1. Note: A breaking change is coming at 11:00 AM!


class UnifiedDocument(BaseModel):
    """
    Unified contract shared by all pipeline stages.

    Canonical source_type values:
    - PDF
    - CSV
    - HTML
    - Video
    - Code

    The model accepts a few alias names to make the mid-lab schema migration
    easier and to avoid breaking downstream stages during field renames.
    """

    model_config = ConfigDict(
        populate_by_name=True,
        validate_assignment=True,
        extra="allow",
        str_strip_whitespace=True,
    )

    document_id: str = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("document_id", "doc_id", "id"),
        description="Stable document identifier used across ingestion and de-duplication.",
    )
    content: str = Field(
        ...,
        min_length=1,
        validation_alias=AliasChoices("content", "text", "body", "summary"),
        description="Main normalized content stored in the knowledge base.",
    )
    source_type: str = Field(
        ...,
        validation_alias=AliasChoices("source_type", "source", "document_type", "type"),
        description="Canonical source type: PDF, CSV, HTML, Video, or Code.",
    )
    author: Optional[str] = Field(
        default="Unknown",
        validation_alias=AliasChoices("author", "created_by", "owner"),
    )
    timestamp: Optional[datetime] = Field(
        default=None,
        validation_alias=AliasChoices("timestamp", "created_at", "ingested_at"),
        description="Primary event or ingestion timestamp when available.",
    )
    schema_version: str = Field(
        default="v1",
        validation_alias=AliasChoices("schema_version", "version"),
        description="Schema version marker to support future migrations.",
    )
    source_metadata: Dict[str, Any] = Field(
        default_factory=dict,
        validation_alias=AliasChoices("source_metadata", "metadata", "source_meta"),
        description=(
            "Source-specific metadata such as original values, extraction method, "
            "business rules, and audit information."
        ),
    )
    data_quality_flags: List[str] = Field(
        default_factory=list,
        validation_alias=AliasChoices("data_quality_flags", "quality_flags"),
        description=(
            "Quality issues detected during ingestion, e.g. duplicate_detected, "
            "missing_value, invalid_price_format."
        ),
    )
    quality_score: Optional[float] = Field(
        default=None,
        ge=0.0,
        le=1.0,
        validation_alias=AliasChoices("quality_score", "score"),
        description="0-1 score assigned by the quality gate.",
    )

    @model_validator(mode="before")
    @classmethod
    def normalize_input_payload(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        normalized = dict(data)

        metadata = normalized.get("source_metadata")
        if metadata is None:
            metadata = normalized.get("metadata") or normalized.get("source_meta") or {}
        if not isinstance(metadata, dict):
            metadata = {"raw_metadata": metadata}
        normalized["source_metadata"] = dict(metadata)
        normalized.pop("metadata", None)
        normalized.pop("source_meta", None)

        if not normalized.get("author") and normalized["source_metadata"].get("author"):
            normalized["author"] = normalized["source_metadata"]["author"]

        top_level_flags = normalized.get("data_quality_flags") or normalized.get("quality_flags") or []
        nested_flags = normalized["source_metadata"].get("data_quality_flags", [])
        if not top_level_flags and nested_flags:
            normalized["data_quality_flags"] = nested_flags

        return normalized

    @field_validator("source_type")
    @classmethod
    def normalize_source_type(cls, value: str) -> str:
        canonical_map = {
            "PDF": "PDF",
            "CSV": "CSV",
            "HTML": "HTML",
            "VIDEO": "Video",
            "TRANSCRIPT": "Video",
            "TXT": "Video",
            "CODE": "Code",
            "LEGACY_CODE": "Code",
            "PY": "Code",
        }

        normalized = str(value).strip()
        return canonical_map.get(normalized.upper(), normalized)

    @field_validator("data_quality_flags", mode="before")
    @classmethod
    def normalize_quality_flags(cls, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list):
            raise TypeError("data_quality_flags must be a list of strings")

        deduped_flags: List[str] = []
        for item in value:
            if item is None:
                continue
            flag = str(item).strip()
            if flag and flag not in deduped_flags:
                deduped_flags.append(flag)
        return deduped_flags

    @model_validator(mode="after")
    def sync_quality_metadata(self) -> "UnifiedDocument":
        metadata = dict(self.source_metadata)
        nested_flags = metadata.get("data_quality_flags", [])
        if isinstance(nested_flags, str):
            nested_flags = [nested_flags]
        elif not isinstance(nested_flags, list):
            nested_flags = []

        merged_flags: List[str] = []
        for flag in [*self.data_quality_flags, *nested_flags]:
            normalized_flag = str(flag).strip()
            if normalized_flag and normalized_flag not in merged_flags:
                merged_flags.append(normalized_flag)

        metadata["data_quality_flags"] = merged_flags
        metadata.setdefault("schema_version", self.schema_version)

        if self.quality_score is not None:
            metadata.setdefault("quality_score", self.quality_score)

        object.__setattr__(self, "data_quality_flags", merged_flags)
        object.__setattr__(self, "source_metadata", metadata)

        return self

    def to_record(self) -> Dict[str, Any]:
        """Serialize safely for json.dump in Role 4."""
        return self.model_dump(mode="json", exclude_none=True)
