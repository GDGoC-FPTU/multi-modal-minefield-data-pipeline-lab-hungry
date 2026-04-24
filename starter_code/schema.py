from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime

# ==========================================
# ROLE 1: LEAD DATA ARCHITECT
# ==========================================
# Your task is to define the Unified Schema for all sources.
# This is v1. Note: A breaking change is coming at 11:00 AM!

class IngestionMetadata(BaseModel):
    """Global metadata for ETL pipeline tracking"""
    ingestion_timestamp: str
    processing_duration_ms: int
    source_file: str
    source_file_hash: str
    schema_version: str = "v1"
    validation_status: str = "passed"  # 'passed', 'partial', 'failed'
    duplicate_of: Optional[str] = None

class UnifiedDocument(BaseModel):
    """Unified document schema v1 for Knowledge Base"""

    # Core fields (bắt buộc)
    document_id: str
    content: str
    source_type: str  # 'PDF', 'HTML', 'CSV', 'TRANSCRIPT', 'CODE'
    author: Optional[str] = "Unknown"
    timestamp: Optional[str] = None  # ISO 8601 format

    # Global metadata (cho ETL tracking)
    ingestion_metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Global metadata about ingestion/processing"
    )

    # Source-specific metadata (chi tiết từng loại source)
    source_metadata: Dict[str, Any] = Field(
        default_factory=dict,
        description="Source-specific metadata including original values"
    )

    # Data quality tracking
    data_quality_flags: List[str] = Field(
        default_factory=list,
        description="Quality flags: duplicate_record, invalid_price, noise_detected, etc."
    )

    quality_score: Optional[float] = None  # 0-1, tính bởi Role 3

    class Config:
        json_schema_extra = {
            "example": {
                "document_id": "csv-001",
                "content": "Product: Laptop Pro 14, Price: 1200 USD",
                "source_type": "CSV",
                "author": "S001",
                "timestamp": "2026-01-15T00:00:00Z",
                "ingestion_metadata": {
                    "ingestion_timestamp": "2026-04-24T10:35:42Z",
                    "processing_duration_ms": 245,
                    "source_file": "sales_records.csv",
                    "source_file_hash": "sha256:abc123",
                    "schema_version": "v1",
                    "validation_status": "passed",
                    "duplicate_of": None
                },
                "source_metadata": {
                    "original_row": {
                        "id": "1",
                        "product_name": "Laptop VinAI Pro 14"
                    }
                },
                "data_quality_flags": [],
                "quality_score": 0.95
            }
        }
