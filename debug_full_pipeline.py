#!/usr/bin/env python3
"""
Debug script to test the full pipeline and see where natural language response gets lost
"""

import pandas as pd
import time
import traceback

print("Starting debug script...")

try:
    from query_planner import SAPQueryPlanner
    print("✓ Imported SAPQueryPlanner")
except Exception as e:
    print(f"✗ Failed to import SAPQueryPlanner: {e}")
    traceback.print_exc()
    exit(1)

try:
    from query_executor import SAPQueryExecutor
    print("✓ Imported SAPQueryExecutor")
except Exception as e:
    print(f"✗ Failed to import SAPQueryExecutor: {e}")
    traceback.print_exc()
    exit(1)

# Create a simple test DataFrame
test_data = {
    'BUDAT': ['2024-01-01', '2024-01-02', '2024-01-03'],
    'LIFNR': ['V001', 'V002', 'V001'],
    'WRBTR': [100.0, 200.0, 150.0],
    'BLART': ['KR', 'KG', 'KR']
}
df = pd.DataFrame(test_data)
print("✓ Created test DataFrame")

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
print("✓ Created schema analysis")

# Test question
question = "explain to me what this report does"

print("\nTesting full pipeline:")
print("=" * 50)

# Step 1: Query Planning
print("\n1. Query Planning:")
print("-" * 20)
try:
    query_planner = SAPQueryPlanner(schema_analysis)
    print("✓ Created query planner")
    
    plan_result = query_planner.plan_query(question)
    print("✓ Executed query planning")
    print(f"Plan status: {plan_result['status']}")
    print(f"Plan action: {plan_result.get('query_plan', {}).get('action', 'N/A')}")
    print(f"Plan keys: {list(plan_result.keys())}")
except Exception as e:
    print(f"✗ Query planning failed: {e}")
    traceback.print_exc()
    exit(1)

# Step 2: Query Execution
print("\n2. Query Execution:")
print("-" * 20)
try:
    query_executor = SAPQueryExecutor(df, schema_analysis)
    print("✓ Created query executor")
    
    execution_result = query_executor.execute_query(plan_result['query_plan'])
    print("✓ Executed query")
    print(f"Execution status: {execution_result['status']}")
    print(f"Execution keys: {list(execution_result.keys())}")
    print(f"Has natural_language_response: {'natural_language_response' in execution_result}")
    if 'natural_language_response' in execution_result:
        nl_response = execution_result['natural_language_response']
        print(f"Natural language response: '{nl_response}'")
        print(f"Response length: {len(nl_response)}")
        print(f"Response is empty: {nl_response == ''}")
except Exception as e:
    print(f"✗ Query execution failed: {e}")
    traceback.print_exc()
    exit(1)

# Step 3: Simulate process_query function
print("\n3. Simulating process_query function:")
print("-" * 20)
try:
    start_time = time.time()

    # This is what the process_query function does
    if plan_result['status'] == 'success':
        # Get AI insights (simplified)
        ai_response = "AI insights would be generated here"
        
        processing_time = time.time() - start_time
        
        # This is the return statement from process_query
        final_result = {
            'status': 'success',
            'data': execution_result['data'],
            'columns': execution_result['columns'],
            'row_count': execution_result['row_count'],
            'execution_time': execution_result['execution_time'],
            'query_type': plan_result['query_plan'].get('action', 'show'),
            'insights': execution_result['insights'],
            'ai_response': ai_response,
            'natural_language_response': execution_result.get('natural_language_response', '')
        }
        
        print(f"Final result keys: {list(final_result.keys())}")
        print(f"Has natural_language_response: {'natural_language_response' in final_result}")
        if 'natural_language_response' in final_result:
            nl_response = final_result['natural_language_response']
            print(f"Final natural language response: '{nl_response}'")
            print(f"Final response length: {len(nl_response)}")
            print(f"Final response is empty: {nl_response == ''}")
except Exception as e:
    print(f"✗ Process query simulation failed: {e}")
    traceback.print_exc()
    exit(1)

print("\n" + "=" * 50)
print("Full pipeline test completed successfully.") 