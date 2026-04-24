import json
import time
import os
from datetime import datetime

# Robust path handling
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DATA_DIR = os.path.join(os.path.dirname(SCRIPT_DIR), "raw_data")

# Import role-specific modules
from schema import UnifiedDocument
from process_pdf import extract_pdf_data
from process_transcript import clean_transcript
from process_html import parse_html_catalog
from process_csv import process_sales_csv
from process_legacy_code import extract_logic_from_code
from quality_check import run_quality_gate

# ==========================================
# ROLE 4: DEVOPS & INTEGRATION SPECIALIST
# ==========================================
# Task: Orchestrate the ingestion pipeline and handle errors/SLA.

def main():
    start_time = time.time()
    final_kb = []
    rejected_docs = []
    processing_stats = {}

    # --- FILE PATH SETUP ---
    pdf_path = os.path.join(RAW_DATA_DIR, "lecture_notes.pdf")
    trans_path = os.path.join(RAW_DATA_DIR, "demo_transcript.txt")
    html_path = os.path.join(RAW_DATA_DIR, "product_catalog.html")
    csv_path = os.path.join(RAW_DATA_DIR, "sales_records.csv")
    code_path = os.path.join(RAW_DATA_DIR, "legacy_pipeline.py")

    output_path = os.path.join(os.path.dirname(SCRIPT_DIR), "processed_knowledge_base.json")
    # ---------------------

    print("=" * 60)
    print("[START] STARTING DATA PIPELINE ORCHESTRATION")
    print("=" * 60)

    # 1. Process CSV
    print("\n[CSV] Processing CSV (Sales Records)...")
    csv_start = time.time()
    try:
        csv_docs = process_sales_csv(csv_path)
        csv_duration = int((time.time() - csv_start) * 1000)
        processing_stats['csv'] = {
            'status': 'success',
            'duration_ms': csv_duration,
            'documents_count': len(csv_docs)
        }
        print(f"[OK] CSV: {len(csv_docs)} documents extracted in {csv_duration}ms")

        for doc in csv_docs:
            if run_quality_gate(doc):
                final_kb.append(doc)
            else:
                rejected_docs.append(doc)
    except Exception as e:
        print(f"[FAIL] CSV processing failed: {e}")
        processing_stats['csv'] = {'status': 'failed', 'error': str(e)}

    # 2. Process HTML
    print("\n[HTML] Processing HTML (Product Catalog)...")
    html_start = time.time()
    try:
        html_docs = parse_html_catalog(html_path)
        html_duration = int((time.time() - html_start) * 1000)
        processing_stats['html'] = {
            'status': 'success',
            'duration_ms': html_duration,
            'documents_count': len(html_docs)
        }
        print(f"[OK] HTML: {len(html_docs)} documents extracted in {html_duration}ms")

        for doc in html_docs:
            if run_quality_gate(doc):
                final_kb.append(doc)
            else:
                rejected_docs.append(doc)
    except Exception as e:
        print(f"[FAIL] HTML processing failed: {e}")
        processing_stats['html'] = {'status': 'failed', 'error': str(e)}

    # 3. Process Transcript
    print("\n[TRANSCRIPT] Processing Transcript...")
    trans_start = time.time()
    try:
        trans_doc = clean_transcript(trans_path)
        trans_duration = int((time.time() - trans_start) * 1000)
        processing_stats['transcript'] = {
            'status': 'success',
            'duration_ms': trans_duration,
            'documents_count': 1
        }
        print(f"[OK] Transcript: 1 document extracted in {trans_duration}ms")

        if run_quality_gate(trans_doc):
            final_kb.append(trans_doc)
        else:
            rejected_docs.append(trans_doc)
    except Exception as e:
        print(f"[FAIL] Transcript processing failed: {e}")
        processing_stats['transcript'] = {'status': 'failed', 'error': str(e)}

    # 4. Process Legacy Code
    print("\n[CODE] Processing Legacy Code...")
    code_start = time.time()
    try:
        code_doc = extract_logic_from_code(code_path)
        code_duration = int((time.time() - code_start) * 1000)
        processing_stats['code'] = {
            'status': 'success',
            'duration_ms': code_duration,
            'documents_count': 1
        }
        print(f"[OK] Code: 1 document extracted in {code_duration}ms")

        if run_quality_gate(code_doc):
            final_kb.append(code_doc)
        else:
            rejected_docs.append(code_doc)
    except Exception as e:
        print(f"[FAIL] Code processing failed: {e}")
        processing_stats['code'] = {'status': 'failed', 'error': str(e)}

    # 5. Process PDF (Gemini)
    print("\n[PDF] Processing PDF (Gemini)...")
    pdf_start = time.time()
    try:
        pdf_doc = extract_pdf_data(pdf_path)
        pdf_duration = int((time.time() - pdf_start) * 1000)
        if pdf_doc:
            processing_stats['pdf'] = {
                'status': 'success',
                'duration_ms': pdf_duration,
                'documents_count': 1
            }
            print(f"[OK] PDF: 1 document extracted in {pdf_duration}ms")

            if run_quality_gate(pdf_doc):
                final_kb.append(pdf_doc)
            else:
                rejected_docs.append(pdf_doc)
        else:
            processing_stats['pdf'] = {
                'status': 'failed',
                'error': 'PDF file not found or upload failed'
            }
            print("[WARN] PDF: Skipped (file not found)")
    except Exception as e:
        print(f"[FAIL] PDF processing failed: {e}")
        processing_stats['pdf'] = {'status': 'failed', 'error': str(e)}

    # 6. Save Results
    print("\n" + "=" * 60)
    print("[CODE] SAVING RESULTS")
    print("=" * 60)

    # Prepare output structure
    output = {
        'metadata': {
            'pipeline_timestamp': datetime.now().isoformat() + 'Z',
            'total_processing_time_ms': int((time.time() - start_time) * 1000),
            'processing_stats': processing_stats
        },
        'summary': {
            'total_documents_processed': sum(stats.get('documents_count', 0) for stats in processing_stats.values()),
            'total_documents_valid': len(final_kb),
            'total_documents_rejected': len(rejected_docs)
        },
        'documents': final_kb
    }

    # Save to file
    try:
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        print(f"\n[OK] Knowledge Base saved to: {output_path}")
    except Exception as e:
        print(f"[FAIL] Failed to save output: {e}")

    # Print Summary
    print("\n" + "=" * 60)
    print("[HTML] FINAL PIPELINE SUMMARY")
    print("=" * 60)
    total_time = time.time() - start_time
    print(f"Total valid documents stored: {len(final_kb)}")
    print(f"Total rejected documents: {len(rejected_docs)}")
    print(f"Pipeline finished in {total_time:.2f} seconds.")
    print("=" * 60)


if __name__ == "__main__":
    main()
