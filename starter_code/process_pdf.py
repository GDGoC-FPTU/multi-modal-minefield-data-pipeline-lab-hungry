import google.generativeai as genai
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

def extract_pdf_data(file_path):
    """Extract PDF data using Gemini API with error handling"""
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return None

    model = genai.GenerativeModel('gemini-2.5-flash')

    print(f"Uploading {file_path} to Gemini...")
    try:
        pdf_file = genai.upload_file(path=file_path)
        print(f"File uploaded successfully. File name: {pdf_file.name}")
    except Exception as e:
        print(f"Failed to upload file to Gemini: {e}")
        return None

    prompt = """
Analyze this PDF document and extract the following information in JSON format:
{
    "title": "[Document title]",
    "author": "[Author name or 'Unknown']",
    "summary": "[3-sentence summary of main content]",
    "main_topics": "[Comma-separated list of main topics]",
    "page_count": "[Number of pages if visible]"
}

Return ONLY valid JSON, no markdown formatting.
"""

    print("Generating content from PDF using Gemini...")
    start_time = datetime.now()
    try:
        response = model.generate_content([pdf_file, prompt])
        processing_duration_ms = int((datetime.now() - start_time).total_seconds() * 1000)
        content_text = response.text
    except Exception as e:
        print(f"Failed to generate content: {e}")
        return None

    # Clean up markdown formatting
    if content_text.startswith("```json"):
        content_text = content_text[7:]
    if content_text.startswith("```"):
        content_text = content_text[3:]
    if content_text.endswith("```"):
        content_text = content_text[:-3]

    content_text = content_text.strip()

    try:
        extracted_fields = json.loads(content_text)
    except json.JSONDecodeError as e:
        print(f"Failed to parse JSON response: {e}")
        print(f"Response text: {content_text}")
        return None

    # Build document
    doc = {
        'document_id': 'pdf-001',
        'content': extracted_fields.get('summary', ''),
        'source_type': 'PDF',
        'author': extracted_fields.get('author', 'Unknown'),
        'timestamp': None,
        'source_metadata': {
            'extracted_by_gemini': {
                'model': 'gemini-2.5-flash',
                'api_version': 'v1beta',
                'extraction_timestamp': datetime.now().isoformat() + 'Z',
                'prompt_used': prompt
            },
            'extracted_fields': {
                'title': extracted_fields.get('title', ''),
                'author': extracted_fields.get('author', 'Unknown'),
                'summary': extracted_fields.get('summary', ''),
                'main_topics': extracted_fields.get('main_topics', ''),
                'page_count': extracted_fields.get('page_count', None)
            },
            'pdf_info': {
                'original_file': 'lecture_notes.pdf',
                'file_size_bytes': os.path.getsize(file_path),
                'page_count': extracted_fields.get('page_count', None)
            },
            'api_call_info': {
                'status': 'success',
                'retry_count': 0,
                'latency_ms': processing_duration_ms
            }
        },
        'ingestion_metadata': {
            'ingestion_timestamp': datetime.now().isoformat() + 'Z',
            'processing_duration_ms': processing_duration_ms,
            'source_file': 'lecture_notes.pdf',
            'source_file_hash': 'sha256:pdf_hash',
            'schema_version': 'v1',
            'validation_status': 'passed',
            'duplicate_of': None
        },
        'data_quality_flags': []
    }

    return doc
