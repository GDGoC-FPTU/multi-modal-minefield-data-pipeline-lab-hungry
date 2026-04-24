from bs4 import BeautifulSoup
import re
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract product data from the HTML table, ignoring boilerplate.

def parse_price_html(price_text):
    """Extract numeric price from HTML price text"""
    if not price_text or price_text.lower() in ['n/a', 'liên hệ']:
        return None

    # Remove currency symbols and format
    price_text = re.sub(r'[^\d,.]', '', price_text)
    price_text = price_text.replace(',', '')

    try:
        return float(price_text)
    except ValueError:
        return None

def parse_html_catalog(file_path):
    """Extract product data from HTML table"""
    with open(file_path, 'r', encoding='utf-8') as f:
        soup = BeautifulSoup(f, 'html.parser')

    results = []

    # Find the main catalog table
    table = soup.find('table', {'id': 'main-catalog'})
    if not table:
        return results

    # Extract all rows (skip header)
    rows = table.find_all('tr')[1:]  # Skip header row

    for idx, row in enumerate(rows, start=1):
        cols = row.find_all('td')
        if len(cols) < 6:
            continue

        # Extract values
        product_code = cols[0].text.strip()
        product_name = cols[1].text.strip()
        category = cols[2].text.strip()
        price_text = cols[3].text.strip()
        stock_text = cols[4].text.strip()
        rating_text = cols[5].text.strip()

        # Normalize values
        normalized_price = parse_price_html(price_text)
        try:
            stock = int(stock_text)
        except (ValueError, TypeError):
            stock = None

        # Detect quality issues
        quality_flags = []
        if price_text.lower() in ['n/a', 'liên hệ']:
            quality_flags.append('invalid_price_format')
        if stock and stock < 0:
            quality_flags.append('out_of_range_value')
        if rating_text.lower() == 'không có đánh giá':
            quality_flags.append('missing_value')

        # Build content
        content = f"Product: {product_name} ({product_code}), Category: {category}, "
        if normalized_price:
            content += f"Price: {normalized_price} VND, "
        content += f"Stock: {stock}"

        doc = {
            'document_id': f"html-{product_code}",
            'content': content,
            'source_type': 'HTML',
            'author': 'VinShop',
            'timestamp': None,
            'source_metadata': {
                'original_values': {
                    'product_code': product_code,
                    'product_name': product_name,
                    'category': category,
                    'price_text': price_text,
                    'stock_text': stock_text,
                    'rating_text': rating_text
                },
                'parsed_values': {
                    'price': normalized_price,
                    'currency': 'VND',
                    'stock': stock,
                    'rating': rating_text
                },
                'html_info': {
                    'table_id': 'main-catalog',
                    'table_name': 'product-grid',
                    'row_number': idx,
                    'extraction_method': 'beautifulsoup4'
                }
            },
            'ingestion_metadata': {
                'ingestion_timestamp': datetime.now().isoformat() + 'Z',
                'processing_duration_ms': 0,
                'source_file': 'product_catalog.html',
                'source_file_hash': 'sha256:html_hash',
                'schema_version': 'v1',
                'validation_status': 'passed' if not quality_flags else 'partial',
                'duplicate_of': None
            },
            'data_quality_flags': quality_flags
        }

        results.append(doc)

    return results

