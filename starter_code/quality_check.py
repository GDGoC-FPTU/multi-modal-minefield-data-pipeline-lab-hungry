# ==========================================
# ROLE 3: OBSERVABILITY & QA ENGINEER
# ==========================================
# Task: Implement quality gates to reject corrupt data or logic discrepancies.

TOXIC_STRINGS = [
    'null pointer exception',
    'error:',
    'exception',
    'failed to',
    'traceback',
    'undefined',
    'nan',
    'null'
]

def run_quality_gate(document_dict):
    """Quality gate to validate documents before adding to KB"""

    # 1️⃣ Basic structure validation
    if not isinstance(document_dict, dict):
        return False

    # 2️⃣ Check required fields
    required_fields = ['document_id', 'content', 'source_type']
    for field in required_fields:
        if field not in document_dict:
            return False

    # 3️⃣ Content length check
    content = str(document_dict.get('content', ''))
    if len(content) < 20:
        return False

    # 4️⃣ Check for toxic strings
    content_lower = content.lower()
    for toxic in TOXIC_STRINGS:
        if toxic in content_lower:
            print(f"⚠️ WARNING: Toxic string '{toxic}' found in document {document_dict.get('document_id')}")
            return False

    # 5️⃣ Check validation status
    ingestion_metadata = document_dict.get('ingestion_metadata', {})
    validation_status = ingestion_metadata.get('validation_status')
    if validation_status == 'failed':
        return False

    # 6️⃣ Flag semantic drift or anomalies
    data_quality_flags = document_dict.get('data_quality_flags', [])
    if 'semantic_drift' in data_quality_flags:
        print(f"⚠️ WARNING: Semantic drift detected in {document_dict.get('document_id')}")
        # Still pass, but with warning

    return True
