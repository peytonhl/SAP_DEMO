#!/usr/bin/env python3
"""
Debug script to test natural language response generation
"""

import pandas as pd
from query_executor import SAPQueryExecutor

# Create a simple test DataFrame
test_data = {
    'BUDAT': ['2024-01-01', '2024-01-02', '2024-01-03'],
    'LIFNR': ['V001', 'V002', 'V001'],
    'WRBTR': [100.0, 200.0, 150.0],
    'BLART': ['KR', 'KG', 'KR']
}
df = pd.DataFrame(test_data)

# Create schema analysis
schema_analysis = {
    'sap_table_type': 'BSEG',
    'file_info': {
        'total_rows': 3,
        'total_columns': 4
    },
    'schema_summary': 'This is a BSEG table with accounting line items.',
    'column_analysis': {
        'BUDAT': {'data_category': 'date', 'sap_patterns': ['posting_date']},
        'LIFNR': {'data_category': 'categorical', 'sap_patterns': ['vendor_number']},
        'WRBTR': {'data_category': 'numeric', 'sap_patterns': ['local_amount']},
        'BLART': {'data_category': 'categorical', 'sap_patterns': ['document_type']}
    }
}

# Create executor
executor = SAPQueryExecutor(df, schema_analysis)

# Test different questions
test_questions = [
    "explain to me what this report does",
    "what does this report do",
    "what is this report",
    "describe this report",
    "tell me about this data"
]

print("Testing natural language response generation:")
print("=" * 50)

for question in test_questions:
    print(f"\nQuestion: '{question}'")
    
    # Test the natural language response generation directly
    nl_response = executor._generate_natural_language_response(question, 'schema', {
        'table_type': 'BSEG',
        'total_rows': 3,
        'total_columns': 4,
        'schema_summary': 'This is a BSEG table with accounting line items.'
    })
    
    print(f"Response: '{nl_response}'")
    print(f"Response length: {len(nl_response)}")
    print(f"Response is empty: {nl_response == ''}")

print("\n" + "=" * 50)
print("Test completed.") 