"""
SAP AI Demo - Enterprise Prompt Templates
Contains structured prompts for government-grade SAP data analysis
"""

# Enterprise-grade system prompt for government SAP analysis
ENTERPRISE_SYSTEM_PROMPT = """You are an expert SAP ECC financial data analyst assistant designed for government agencies and enterprise organizations, with specialized knowledge for Department of Defense (DoD) and Navy operations. You help users understand and query SAP financial data through natural language with enterprise-grade precision and compliance awareness.

ORGANIZATIONAL CONTEXT:
- You are assisting Navy personnel and DoD organizations
- Focus on Navy-specific financial processes, procurement, and compliance requirements
- Emphasize DoD financial regulations, Navy acquisition processes, and military-specific accounting standards
- Consider Navy organizational structure, commands, and funding sources
- Reference Navy-specific terminology and processes when relevant

SAP ECC Financial Tables You Can Work With:
- BKPF (Accounting Document Header): Document metadata, posting dates, document types, company codes, fiscal years
- BSEG (Accounting Document Segment): Line items, account assignments, amounts, posting keys, vendor/customer references
- LFA1 (Vendor Master): Vendor information, addresses, payment terms, blocking status
- KNA1 (Customer Master): Customer information, addresses, payment terms, blocking status
- SKAT (G/L Account Master): Chart of accounts, account descriptions, account types

Government & Enterprise SAP Financial Concepts:
- Document Types: K1 (Customer Invoice), K2 (Customer Payment), S1 (Vendor Invoice), S2 (Vendor Payment), G1 (G/L Document)
- Posting Keys: 40 (Debit), 50 (Credit) for G/L accounts, 01-09 for vendor/customer accounts
- Company Codes: 4-digit codes representing legal entities or government departments
- Fiscal Years: YYYY format, critical for government reporting
- Posting Periods: 01-12 for months, 13-16 for special periods
- Cost Centers: Government cost allocation and budget tracking
- Funds Centers: Government fund management and appropriation tracking
- Business Areas: Government program and activity segregation

NAVY/DOD SPECIFIC CONTEXT:
- Navy commands, fleets, and shore establishments
- DoD appropriation categories and funding sources
- Navy acquisition processes and procurement regulations
- Military-specific vendor relationships and contracts
- Navy financial reporting requirements and audit standards
- DoD compliance with federal financial regulations
- Navy budget execution and fiscal law compliance

Your Enterprise Capabilities:
1. Interpret complex natural language queries about SAP financial data
2. Provide compliance-aware analysis for government regulations and DoD requirements
3. Suggest relevant data filters and time periods for audit trails
4. Explain SAP concepts and terminology in Navy/DoD business context
5. Provide insights on financial trends, anomalies, and compliance issues
6. Support government reporting requirements and transparency initiatives
7. Identify potential data quality issues and reconciliation needs
8. Provide Navy-specific recommendations and best practices

Government & Compliance Focus:
- Emphasize data accuracy and audit trails for DoD requirements
- Highlight compliance with government accounting standards and Navy regulations
- Identify potential fraud indicators or unusual patterns
- Support transparency and public disclosure requirements
- Ensure proper segregation of duties and access controls
- Reference Navy-specific financial processes and procedures

Always provide professional, accurate, and compliance-aware responses suitable for Navy and DoD environments, incorporating relevant Navy terminology and processes when appropriate."""

# Function to create context-aware prompts for enterprise analysis
def create_enterprise_query_prompt(user_question, schema_analysis=None, execution_context=None):
    """
    Creates a context-aware prompt for enterprise SAP queries
    
    Args:
        user_question (str): The user's natural language question
        schema_analysis (dict): Schema analysis results (optional)
        execution_context (dict): Query execution results (optional)
    
    Returns:
        list: Messages for OpenAI API
    """
    messages = [
        {"role": "system", "content": ENTERPRISE_SYSTEM_PROMPT}
    ]
    
    # Add schema context if available
    if schema_analysis:
        schema_context = f"""
Data Schema Context:
- Table Type: {schema_analysis.get('sap_table_type', 'Unknown')}
- Total Records: {schema_analysis.get('file_info', {}).get('total_rows', 0):,}
- Date Range: {schema_analysis.get('data_insights', {}).get('date_range', 'Not specified')}
- Company Codes: {', '.join(schema_analysis.get('file_info', {}).get('company_codes', []))}
- Data Quality: {schema_analysis.get('data_insights', {}).get('data_quality', {}).get('null_percentage', 0)}% null values

Key Columns Available:
"""
        for col_name, col_info in schema_analysis.get('column_analysis', {}).items():
            if col_info.get('sap_patterns'):
                schema_context += f"- {col_name}: {', '.join(col_info['sap_patterns'])}\n"
        
        messages.append({"role": "system", "content": schema_context})
    
    # Add execution context if available
    if execution_context:
        exec_context = f"""
Query Execution Results:
- Records Returned: {execution_context.get('row_count', 0):,}
- Processing Time: {execution_context.get('execution_time', 0):.2f} seconds
- Query Type: {execution_context.get('query_type', 'Unknown')}

Analysis Summary:
"""
        for insight in execution_context.get('insights', []):
            exec_context += f"- {insight}\n"
        
        messages.append({"role": "system", "content": exec_context})
    
    # Add the user's question
    messages.append({"role": "user", "content": user_question})
    
    return messages

# Government and enterprise-focused example queries
ENTERPRISE_EXAMPLE_QUERIES = [
    "Show me all vendor payments over $50,000 for Q2 2024",
    "Identify overdue invoices past 90 days by department",
    "Summarize cost center spending by fiscal year",
    "Find duplicate vendor payments or suspicious transactions",
    "Show accounts receivable aging by customer",
    "Analyze budget vs actual spending by cost center",
    "Identify transactions posted outside normal business hours",
    "Show vendor payment trends and top suppliers by volume",
    "Find documents with posting errors or blocked status",
    "Analyze cash flow patterns and payment timing",
    "Show interdepartmental transfers and allocations",
    "Identify potential fraud indicators or unusual patterns"
]

# Compliance and audit-focused queries
COMPLIANCE_QUERIES = [
    "Show all transactions requiring management review",
    "Identify segregation of duties violations",
    "Find transactions exceeding approval thresholds",
    "Show audit trail for specific document numbers",
    "Analyze access patterns and user activity",
    "Identify transactions posted by terminated users",
    "Show budget overruns and variance analysis",
    "Find transactions with missing or invalid references"
]

# Government reporting queries
GOVERNMENT_REPORTING_QUERIES = [
    "Generate quarterly financial report by program",
    "Show federal fund usage and compliance",
    "Analyze grant spending and remaining balances",
    "Show cost allocation by government function",
    "Identify unallowable costs and adjustments",
    "Generate transparency report for public disclosure",
    "Show performance metrics and efficiency indicators",
    "Analyze procurement compliance and vendor diversity"
]

# Data quality and reconciliation queries
DATA_QUALITY_QUERIES = [
    "Identify data quality issues and missing information",
    "Show reconciliation differences and variances",
    "Find orphaned transactions or broken references",
    "Analyze posting period accuracy and fiscal year compliance",
    "Show duplicate entries and potential duplicates",
    "Identify transactions with invalid account combinations",
    "Find documents with incomplete or inconsistent data",
    "Show data integrity checks and validation results"
]

# Function to get context-appropriate query suggestions
def get_query_suggestions(schema_analysis=None, user_role="analyst"):
    """
    Get context-appropriate query suggestions based on schema and user role
    
    Args:
        schema_analysis (dict): Schema analysis results
        user_role (str): User role (analyst, auditor, manager, etc.)
    
    Returns:
        list: Relevant query suggestions
    """
    base_suggestions = ENTERPRISE_EXAMPLE_QUERIES[:6]
    
    if user_role == "auditor":
        base_suggestions.extend(COMPLIANCE_QUERIES[:4])
    elif user_role == "manager":
        base_suggestions.extend(GOVERNMENT_REPORTING_QUERIES[:4])
    elif user_role == "data_steward":
        base_suggestions.extend(DATA_QUALITY_QUERIES[:4])
    
    # Add schema-specific suggestions
    if schema_analysis:
        sap_table_type = schema_analysis.get('sap_table_type', '')
        if sap_table_type == 'BKPF':
            base_suggestions.extend([
                "Show document posting patterns by user",
                "Analyze document type distribution by period"
            ])
        elif sap_table_type == 'BSEG':
            base_suggestions.extend([
                "Show account balance trends over time",
                "Analyze posting key patterns and anomalies"
            ])
    
    return base_suggestions[:10]  # Limit to 10 suggestions

# Function to create compliance-aware analysis prompts
def create_compliance_analysis_prompt(query_results, schema_analysis):
    """
    Create prompts for compliance and audit analysis
    
    Args:
        query_results (dict): Query execution results
        schema_analysis (dict): Schema analysis results
    
    Returns:
        list: Messages for compliance analysis
    """
    compliance_prompt = f"""
You are a government compliance auditor analyzing SAP financial data. Review the following query results and identify:

1. Compliance Issues: Any transactions that may violate government regulations or policies
2. Audit Concerns: Unusual patterns, anomalies, or red flags
3. Data Quality: Missing information, inconsistencies, or integrity issues
4. Recommendations: Suggested actions for management review or further investigation

Query Results Summary:
- Records Analyzed: {query_results.get('row_count', 0):,}
- Query Type: {query_results.get('query_type', 'Unknown')}
- Processing Time: {query_results.get('execution_time', 0):.2f} seconds

Provide a professional audit analysis suitable for government reporting and management review.
"""
    
    return [
        {"role": "system", "content": compliance_prompt},
        {"role": "user", "content": "Please analyze these results for compliance and audit concerns."}
    ]

# Function to create business insights prompts
def create_business_insights_prompt(query_results, schema_analysis, business_context=""):
    """
    Create prompts for business insights and recommendations
    
    Args:
        query_results (dict): Query execution results
        schema_analysis (dict): Schema analysis results
        business_context (str): Additional business context
    
    Returns:
        list: Messages for business analysis
    """
    insights_prompt = f"""
You are a senior financial analyst providing business insights for government operations. Analyze the following data and provide:

1. Key Findings: Most important patterns, trends, or anomalies
2. Business Impact: How these results affect operations, budgets, or compliance
3. Recommendations: Actionable suggestions for management
4. Risk Assessment: Potential risks or opportunities identified
5. Performance Metrics: Efficiency indicators and benchmarks

Analysis Context:
- Data Scope: {schema_analysis.get('sap_table_type', 'Unknown')} table with {schema_analysis.get('file_info', {}).get('total_rows', 0):,} records
- Query Results: {query_results.get('row_count', 0):,} records returned
- Business Context: {business_context or 'Government financial operations'}

Provide professional insights suitable for executive reporting and decision-making.
"""
    
    return [
        {"role": "system", "content": insights_prompt},
        {"role": "user", "content": "Please provide business insights and recommendations based on this analysis."}
    ] 