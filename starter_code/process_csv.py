import pandas as pd
import re
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Process sales records, handling type traps and duplicates.

def normalize_price(price_str):
    """Convert various price formats to float"""
    if pd.isna(price_str) or price_str == '' or price_str == 'NULL':
        return None

    price_str = str(price_str).strip()

    # Handle text numbers
    if price_str.lower() == 'five dollars':
        return 5.0
    if price_str.lower() in ['n/a', 'liên hệ']:
        return None

    # Remove currency symbols and commas
    price_str = re.sub(r'[$₫VND,\s]', '', price_str)

    try:
        price = float(price_str)
        return price if price >= 0 else None  # Reject negative prices
    except ValueError:
        return None

def normalize_date(date_str):
    """Convert various date formats to YYYY-MM-DD"""
    if pd.isna(date_str) or date_str == '':
        return None

    date_str = str(date_str).strip()

    # Try common date formats
    formats = [
        '%Y-%m-%d',           # 2026-01-15
        '%d/%m/%Y',           # 15/01/2026
        '%m/%d/%Y',           # 01/15/2026
        '%d-%m-%Y',           # 15-01-2026
        '%Y/%m/%d',           # 2026/01/15
        '%B %d %Y',           # January 15 2026
        '%B %dth %Y',         # January 15th 2026
        '%d %b %Y',           # 15 Jan 2026
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime('%Y-%m-%d')
        except ValueError:
            continue

    return None

def process_sales_csv(file_path):
    """Process sales_records.csv with data cleaning and normalization"""
    df = pd.read_csv(file_path)

    results = []
    seen_ids = {}

    for idx, row in df.iterrows():
        # Create document ID
        doc_id = f"csv-{row['id']}"
        row_num = idx + 2  # +2 because idx starts at 0 and CSV has header

        # Normalize values
        normalized_price = normalize_price(row['price'])
        normalized_date = normalize_date(row['date_of_sale'])

        # Detect duplicates
        duplicate_of = None
        if row['id'] in seen_ids:
            duplicate_of = seen_ids[row['id']]
        else:
            seen_ids[row['id']] = doc_id

        # Detect quality issues
        quality_flags = []
        if duplicate_of:
            quality_flags.append('duplicate_record')
        if normalized_price is None and str(row['price']).lower() not in ['n/a', 'liên hệ', 'null']:
            quality_flags.append('invalid_price_format')
        if normalized_date is None:
            quality_flags.append('date_format_inconsistent')
        if pd.isna(row['stock_quantity']) or row['stock_quantity'] == '':
            quality_flags.append('missing_value')
        elif int(row['stock_quantity']) < 0:
            quality_flags.append('out_of_range_value')

        # Build content string
        content = f"Product: {row['product_name']}, Category: {row['category']}, "
        if normalized_price:
            content += f"Price: {normalized_price} {row['currency']}, "
        content += f"Date: {normalized_date}, Seller: {row['seller_id']}"

        # Create document
        doc = {
            'document_id': doc_id,
            'content': content,
            'source_type': 'CSV',
            'author': str(row['seller_id']),
            'timestamp': normalized_date + 'T00:00:00Z' if normalized_date else None,
            'source_metadata': {
                'original_row': {
                    'id': str(row['id']),
                    'product_name': str(row['product_name']),
                    'category': str(row['category']),
                    'price': str(row['price']),
                    'currency': str(row['currency']),
                    'date_of_sale': str(row['date_of_sale']),
                    'seller_id': str(row['seller_id']),
                    'stock_quantity': str(row['stock_quantity'])
                },
                'normalized_values': {
                    'price': normalized_price,
                    'date_of_sale': normalized_date,
                    'stock_quantity': int(row['stock_quantity']) if pd.notna(row['stock_quantity']) else None
                },
                'csv_info': {
                    'row_number': row_num,
                    'column_count': len(df.columns),
                    'columns': list(df.columns)
                }
            },
            'ingestion_metadata': {
                'ingestion_timestamp': datetime.now().isoformat() + 'Z',
                'processing_duration_ms': 0,
                'source_file': 'sales_records.csv',
                'source_file_hash': 'sha256:csv_hash',
                'schema_version': 'v1',
                'validation_status': 'passed' if not quality_flags else 'partial',
                'duplicate_of': duplicate_of
            },
            'data_quality_flags': quality_flags
        }

        results.append(doc)

    return results

