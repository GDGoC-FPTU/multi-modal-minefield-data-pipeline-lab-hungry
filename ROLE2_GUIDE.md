# Role 2: ETL/ELT Builder - Implementation Guide

## 📋 Overview

Bạn là **ETL/ELT Builder** (Role 2), chịu trách nhiệm **trích xuất và xử lý dữ liệu** từ 5 nguồn khác nhau thành format chuẩn (UnifiedDocument).

---

## 🎯 Nhiệm Vụ Chính

### **File cần hoàn thiện:**
1. ✅ `process_csv.py` - Xử lý sales_records.csv (DONE)
2. ✅ `process_html.py` - Trích xuất từ product_catalog.html (DONE)
3. ✅ `process_transcript.py` - Làm sạch demo_transcript.txt (DONE)
4. ✅ `process_legacy_code.py` - Trích xuất business logic từ legacy_pipeline.py (DONE)
5. ⚠️ `process_pdf.py` - Dùng Gemini API để xử lý lecture_notes.pdf (NEED API KEY)

---

## 📊 Dữ Liệu Đầu Vào (Raw Data)

### **1. CSV: sales_records.csv**
**Thách thức:**
- 20 rows, nhưng có **duplicates** (ID=1 xuất hiện 3 lần)
- **Mixed price formats**: "$1200", "250000", "five dollars", "N/A"
- **Mixed date formats**: "2026-01-15", "15/01/2026", "January 16th 2026", etc.
- **Invalid values**: Negative stock (-350000), NULL fields

**Output:**
```python
{
    "document_id": "csv-1",
    "content": "Product: Laptop VinAI Pro 14, Category: Electronics, Price: 1200.0 USD, Date: 2026-01-15, Seller: S001",
    "source_type": "CSV",
    "source_metadata": {
        "original_row": {...},  # Bản gốc
        "normalized_values": {...},  # Đã chuẩn hóa
        "csv_info": {
            "row_number": 2,
            "columns": [...]
        }
    },
    "ingestion_metadata": {
        "ingestion_timestamp": "2026-04-24T11:02:26Z",
        "processing_duration_ms": 54,
        "source_file": "sales_records.csv",
        "schema_version": "v1",
        "validation_status": "passed",
        "duplicate_of": null  # hoặc "csv-1" nếu duplicate
    },
    "data_quality_flags": []  # duplicate_record, invalid_price_format, etc.
}
```

### **2. HTML: product_catalog.html**
**Thách thức:**
- Có **boilerplate** (navbar, footer, ads) cần bỏ
- Giá có format như "28,500,000 VND", "N/A", "Liên hệ"
- Stock có thể **negative** (-5 ở row 4)
- Table ID là "main-catalog"

**Output:**
```python
{
    "document_id": "html-SP-001",
    "content": "Product: VinAI Laptop Pro 14, Category: Laptop, Price: 28500000.0 VND, Stock: 45",
    "source_type": "HTML",
    "source_metadata": {
        "original_values": {"price_text": "28,500,000 VND", ...},
        "parsed_values": {"price": 28500000.0, ...},
        "html_info": {
            "table_id": "main-catalog",
            "row_number": 1
        }
    },
    ...
}
```

### **3. TRANSCRIPT: demo_transcript.txt**
**Thách thức:**
- **Timestamps** [00:00:00] cần xóa
- **Noise tokens**: [Music], [inaudible], [Laughter]
- **Vietnamese numerals**: "năm trăm nghìn" = 500,000
- Multiple speakers

**Output:**
```python
{
    "document_id": "transcript-001",
    "content": "Chào mừng các bạn đến với buổi học hôm nay...",
    "source_type": "TRANSCRIPT",
    "source_metadata": {
        "speakers": ["Speaker 1", "Speaker 2"],
        "language": "Vietnamese",
        "noise_removed": {
            "timestamps": 11,
            "sound_effects": ["[Music starts]", "[Music ends]"]
        },
        "extracted_data": {
            "prices_mentioned": [
                {
                    "original_text": "năm trăm nghìn VND",
                    "parsed_value": 500000,
                    "confidence": 0.90
                }
            ]
        }
    },
    "data_quality_flags": ["noise_detected", "vietnamese_numeral_detected"]
}
```

### **4. CODE: legacy_pipeline.py**
**Thách thức:**
- Trích xuất **business rules** từ docstrings
- Phát hiện **code anomalies**: comment nói "8%" nhưng code là "0.10"
- Extract **region mapping** và **discount rules**

**Output:**
```python
{
    "document_id": "code-001",
    "content": "Extracted business logic from legacy pipeline:\n- BLR-001: GOLD tier = 15% discount, SILVER tier = 10% discount",
    "source_type": "CODE",
    "source_metadata": {
        "functions_extracted": [
            {
                "function_name": "calculate_discount",
                "docstring": "Business Logic Rule 001: ...",
                "business_rules": [...]
            }
        ],
        "code_quality": {
            "has_docstrings": True,
            "has_anomalies": True,
            "anomalies_count": 1
        }
    },
    "data_quality_flags": ["semantic_drift"]
}
```

### **5. PDF: lecture_notes.pdf**
**Thách thức:**
- Dùng **Gemini API** vì PDF có layout phức tạp
- Cần **GEMINI_API_KEY** trong `.env`
- Handle retry logic cho 429 errors

**Output:**
```python
{
    "document_id": "pdf-001",
    "content": "Summary of lecture notes...",
    "source_type": "PDF",
    "author": "[Extracted from Gemini]",
    "source_metadata": {
        "extracted_by_gemini": {
            "model": "gemini-2.5-flash",
            "extraction_timestamp": "2026-04-24T10:30:00Z"
        },
        "extracted_fields": {
            "title": "...",
            "author": "...",
            "summary": "...",
            "main_topics": "..."
        },
        "api_call_info": {
            "status": "success",
            "latency_ms": 3450
        }
    }
}
```

---

## 🔧 Implementation Details

### **process_csv.py**
✅ **Hoàn thiện:**
- Normalize giá: "$1200" → 1200.0, "five dollars" → 5.0
- Normalize date: Multiple formats → "YYYY-MM-DD"
- Detect duplicates dựa trên `id` field
- Flag quality issues: invalid_price, missing_value, out_of_range

### **process_html.py**
✅ **Hoàn thiện:**
- Dùng BeautifulSoup để find table id="main-catalog"
- Parse giá từ "28,500,000 VND" → 28500000.0
- Detect negative stock, missing ratings

### **process_transcript.py**
✅ **Hoàn thiện:**
- Remove timestamps [00:00:00]
- Remove noise: [Music], [inaudible], [Laughter]
- Parse Vietnamese numerals: "năm trăm nghìn" → 500000
- Track compression ratio

### **process_legacy_code.py**
✅ **Hoàn thiện:**
- Dùng `ast` module để extract docstrings
- Find business rules (BLR-001, BLR-002)
- Detect semantic drift: comment ≠ code

### **process_pdf.py**
⚠️ **Cần API Key:**
1. Lấy Gemini API Key từ [Google AI Studio](https://aistudio.google.com/app/apikey)
2. Thêm vào `.env`: `GEMINI_API_KEY=your_key_here`
3. Run pipeline lại

---

## 📈 Global Metadata (Tối Giản)

Mỗi document có `ingestion_metadata` với 7 fields bắt buộc:

```python
"ingestion_metadata": {
    "ingestion_timestamp": "2026-04-24T11:02:26Z",  # Khi xử lý
    "processing_duration_ms": 54,                    # Bao lâu
    "source_file": "sales_records.csv",              # Từ file nào
    "source_file_hash": "sha256:abc123",             # Verify file
    "schema_version": "v1",                          # Chuẩn bị v2
    "validation_status": "passed",                   # Qua validation?
    "duplicate_of": null                             # Link duplicate
}
```

**Dùng cho:**
- Role 3: Validate quality
- Role 4: Monitor SLA, manage duplicates
- Role 1: Chuẩn bị schema migration (v1→v2)

---

## ✅ Checklist - Role 2

- [x] **process_csv.py** - Xử lý 21 documents, phát hiện duplicates
- [x] **process_html.py** - Trích xuất 5 sản phẩm từ table
- [x] **process_transcript.py** - Làm sạch transcript, extract price
- [x] **process_legacy_code.py** - Extract 2 business rules, phát hiện anomaly
- [ ] **process_pdf.py** - Cần Gemini API Key (optional, nhưng có thể skip)
- [x] **quality_check.py** - Basic validation gates
- [x] **orchestrator.py** - Dùng để run toàn bộ pipeline

---

## 🚀 Cách Chạy

### **1. Cài đặt dependencies:**
```bash
pip install -r requirements.txt
```

### **2. Thiết lập Gemini API (Optional):**
```bash
# Tạo .env file
echo "GEMINI_API_KEY=your_key_here" > .env
```

### **3. Chạy pipeline:**
```bash
cd starter_code
python orchestrator.py
```

### **4. Kiểm tra kết quả:**
```bash
cat processed_knowledge_base.json | jq '.summary'
```

---

## 📊 Expected Output

```
============================================================
[START] STARTING DATA PIPELINE ORCHESTRATION
============================================================

[CSV] Processing CSV (Sales Records)...
[OK] CSV: 21 documents extracted in 54ms

[HTML] Processing HTML (Product Catalog)...
[OK] HTML: 5 documents extracted in 0ms

[TRANSCRIPT] Processing Transcript...
[OK] Transcript: 1 document extracted in 0ms

[CODE] Processing Legacy Code...
[OK] Code: 1 document extracted in 3ms

[PDF] Processing PDF (Gemini)...
[WARN] PDF: Skipped (file not found or no API key)

============================================================
[SAVE] SAVING RESULTS
============================================================

[OK] Knowledge Base saved to: processed_knowledge_base.json

============================================================
[SUMMARY] FINAL PIPELINE SUMMARY
============================================================
Total valid documents stored: 27
Total rejected documents: 0
Pipeline finished in 0.66 seconds.
============================================================
```

---

## 🎯 Key Insights

### **Data Quality Flags (20+ flags):**
- `duplicate_record` - Trùng lặp
- `invalid_price_format` - Giá không hợp lệ
- `date_format_inconsistent` - Date format sai
- `missing_value` - Thiếu giá trị
- `out_of_range_value` - Giá trị ngoài phạm vi
- `semantic_drift` - Code ≠ comment
- `noise_detected` - Có noise tokens
- `vietnamese_numeral_detected` - Numeral tiếng Việt

### **Metadata Levels:**
1. **ingestion_metadata** - Global tracking (7 fields)
2. **source_metadata** - Chi tiết từng loại (50+ fields tùy loại)
3. **data_quality_flags** - List các issues phát hiện

---

## 💡 Best Practices

1. **Lưu bản gốc + bản đã normalize** - Giúp audit trail
2. **Ghi lại transformations** - Biết chính xác dữ liệu thay đổi nào
3. **Flag quality issues** - Để Role 3 kiểm tra
4. **Timestamp tất cả** - Để Role 4 track SLA
5. **Schema version** - Chuẩn bị cho breaking change (11:00 AM)

---

## 🔗 Liên Quan Đến Roles Khác

- **Role 1 (Architect)**: Dùng `schema_version` để migration v1→v2
- **Role 3 (QA)**: Dùng `data_quality_flags` để validate
- **Role 4 (DevOps)**: Dùng `ingestion_metadata` để monitor SLA

---

## 📝 Notes

- CSV có **21 records** (20 + 1 duplicate)
- HTML có **5 products** từ table
- Transcript có **1 document** nhưng với extracted price
- Code có **2 business rules** + **1 anomaly** (comment ≠ code)
- PDF cần API key (optional, có thể skip hoặc test sau)

**Total: 27 valid documents** (nếu không có PDF)

---

**Hoàn tất! 🎉 Role 2 đã xử lý xong dữ liệu và sẵn sàng cho Role 3 & 4!**
