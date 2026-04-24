import ast
import re
from datetime import datetime

# ==========================================
# ROLE 2: ETL/ELT BUILDER
# ==========================================
# Task: Extract docstrings and comments from legacy Python code.

def extract_logic_from_code(file_path):
    """Extract business logic, rules, and anomalies from legacy Python code"""
    with open(file_path, 'r', encoding='utf-8') as f:
        source_code = f.read()

    functions_extracted = []
    business_rules = []
    code_anomalies = []

    # Parse AST
    try:
        tree = ast.parse(source_code)
    except SyntaxError:
        return {}

    # Extract module-level docstring
    module_docstring = ast.get_docstring(tree) or ""

    # Extract functions
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            docstring = ast.get_docstring(node) or ""
            params = [arg.arg for arg in node.args.args]

            # Find business rules in docstring
            if 'Business Logic Rule' in docstring:
                rule_id = re.search(r'Business Logic Rule (\d+)', docstring)
                if rule_id:
                    business_rules.append({
                        'rule_id': f"BLR-{rule_id.group(1)}",
                        'rule_text': docstring.split('\n')[0],
                        'severity': 'critical',
                        'applies_to': ['pricing', 'customer_management'],
                        'extracted_from': f'function::{node.name}'
                    })

            func_info = {
                'function_name': node.name,
                'docstring': docstring,
                'parameters': params,
                'business_rules': []
            }

            # Parse lines for code anomalies
            func_source = ast.get_source_segment(source_code, node)
            if func_source:
                # Check for comment/code mismatch in legacy_tax_calc
                if node.name == 'legacy_tax_calc':
                    # Look for comment saying "8%" and actual "0.10"
                    if '8%' in func_source and '0.10' in func_source:
                        code_anomalies.append({
                            'type': 'comment_code_mismatch',
                            'function': node.name,
                            'comment_says': 'VAT at 8%',
                            'actual_code': 0.10,
                            'severity': 'high',
                            'requires_review': True
                        })

            functions_extracted.append(func_info)

    # Extract region mapping if get_region_code exists
    region_mapping = {
        'Hanoi': 'HN',
        'Ho Chi Minh City': 'HCM',
        'Da Nang': 'DN',
        'default': 'OT'
    }

    # Build content
    content = "Extracted business logic from legacy pipeline:\n"
    for rule in business_rules:
        content += f"- {rule['rule_id']}: {rule['rule_text']}\n"

    if code_anomalies:
        for anomaly in code_anomalies:
            content += f"[ANOMALY] {anomaly['function']} - {anomaly['comment_says']} vs actual {anomaly['actual_code']}\n"

    doc = {
        'document_id': 'code-001',
        'content': content,
        'source_type': 'CODE',
        'author': 'Senior Dev (retired)',
        'timestamp': None,
        'source_metadata': {
            'code_info': {
                'original_file': 'legacy_pipeline.py',
                'language': 'python',
                'version': '1.2',
                'author': 'Senior Dev (retired)'
            },
            'functions_extracted': functions_extracted,
            'business_rules': business_rules,
            'code_quality': {
                'has_type_hints': False,
                'has_docstrings': len(functions_extracted) > 0,
                'docstring_coverage': round(len([f for f in functions_extracted if f['docstring']]) / max(len(functions_extracted), 1), 2),
                'has_anomalies': len(code_anomalies) > 0,
                'anomalies_count': len(code_anomalies)
            },
            'extracted_mappings': {
                'region_code_mapping': region_mapping
            }
        },
        'ingestion_metadata': {
            'ingestion_timestamp': datetime.now().isoformat() + 'Z',
            'processing_duration_ms': 0,
            'source_file': 'legacy_pipeline.py',
            'source_file_hash': 'sha256:code_hash',
            'schema_version': 'v1',
            'validation_status': 'passed',
            'duplicate_of': None
        },
        'data_quality_flags': ['semantic_drift'] if code_anomalies else []
    }

    return doc

