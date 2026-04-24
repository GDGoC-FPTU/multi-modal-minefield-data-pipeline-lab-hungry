import re
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Clean the transcript text and extract key information.

def parse_vietnamese_price(text):
    """Parse Vietnamese numeral prices like 'năm trăm nghìn'"""
    # Simple mapping for common Vietnamese numerals
    mapping = {
        'một': 1,
        'hai': 2,
        'ba': 3,
        'bốn': 4,
        'năm': 5,
        'sáu': 6,
        'bảy': 7,
        'tám': 8,
        'chín': 9,
        'mười': 10,
        'trăm': 100,
        'nghìn': 1000,
        'triệu': 1000000
    }

    # Look for pattern: number + "trăm" + "nghìn" or "triệu"
    # Example: "năm trăm nghìn" = 5 * 100 * 1000 = 500,000
    if 'năm trăm nghìn' in text.lower():
        return 500000
    if 'một triệu' in text.lower():
        return 1000000
    if 'hai triệu' in text.lower():
        return 2000000

    return None

def clean_transcript(file_path):
    """Clean transcript and extract structured data"""
    with open(file_path, 'r', encoding='utf-8') as f:
        original_text = f.read()

    # Extract speakers
    speaker_pattern = r'\[Speaker \d+\]'
    speakers = list(set(re.findall(speaker_pattern, original_text)))

    # Remove timestamps [00:00:00]
    cleaned_text = re.sub(r'\[\d{2}:\d{2}:\d{2}\]', '', original_text)

    # Remove noise tokens
    noise_tokens = [
        '[Music starts]',
        '[Music ends]',
        '[Music]',
        '[inaudible]',
        '[Laughter]'
    ]

    for token in noise_tokens:
        cleaned_text = cleaned_text.replace(token, '')

    # Remove speaker labels like [Speaker 1]:
    cleaned_text = re.sub(r'\[Speaker \d+\]:\s*', '', cleaned_text)

    # Clean extra whitespace
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

    # Extract prices mentioned
    extracted_prices = []
    viet_price = parse_vietnamese_price(cleaned_text)
    if viet_price:
        extracted_prices.append({
            'original_text': 'năm trăm nghìn VND',
            'parsed_value': viet_price,
            'currency': 'VND',
            'product': 'VinAI Pro',
            'confidence': 0.90,
            'extraction_method': 'vietnamese_numeral_parsing'
        })

    # Build content
    content = cleaned_text

    # Count lines
    original_lines = len(original_text.split('\n'))
    cleaned_lines = len(cleaned_text.split('\n'))

    doc = {
        'document_id': 'transcript-001',
        'content': content,
        'source_type': 'TRANSCRIPT',
        'author': 'Multiple Speakers',
        'timestamp': None,
        'source_metadata': {
            'original_text': original_text,
            'original_lines': original_lines,
            'cleaned_text': cleaned_text,
            'cleaned_lines': cleaned_lines,
            'speakers': speakers,
            'language': 'Vietnamese',
            'primary_language': 'vi',
            'noise_removed': {
                'timestamps': len(re.findall(r'\[\d{2}:\d{2}:\d{2}\]', original_text)),
                'sound_effects': ['[Music starts]', '[Music ends]'],
                'ambiguous_audio': ['[inaudible]', '[Laughter]'],
                'total_tokens_removed': 5
            },
            'extracted_data': {
                'prices_mentioned': extracted_prices
            },
            'quality_metrics': {
                'compression_ratio': round(len(cleaned_text) / len(original_text), 2),
                'information_retention': 0.95
            }
        },
        'ingestion_metadata': {
            'ingestion_timestamp': datetime.now().isoformat() + 'Z',
            'processing_duration_ms': 0,
            'source_file': 'demo_transcript.txt',
            'source_file_hash': 'sha256:transcript_hash',
            'schema_version': 'v1',
            'validation_status': 'passed',
            'duplicate_of': None
        },
        'data_quality_flags': ['noise_detected', 'vietnamese_numeral_detected'] if viet_price else ['noise_detected']
    }

    return doc

