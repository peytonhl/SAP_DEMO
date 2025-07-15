"""
SAP AI Demo - Schema Mapper
Maps SAP column codes to plain language descriptions for better LLM understanding
"""

from typing import Dict, List, Optional
import logging

class SAPSchemaMapper:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Comprehensive schema mappings for SAP tables
        self.SCHEMAS = {
            'BKPF': {
                'BUKRS': 'Company Code - 4-digit code representing legal entity or department',
                'BELNR': 'Document Number - Unique accounting document identifier',
                'GJAHR': 'Fiscal Year - Year of the accounting document',
                'BLART': 'Document Type - Type of accounting document (K1=Customer Invoice, S1=Vendor Invoice, etc.)',
                'BUDAT': 'Posting Date - Date when document was posted to the system',
                'WAERS': 'Currency - Document currency code (USD, EUR, etc.)',
                'BKTXT': 'Document Header Text - Description or reference text',
                'USNAM': 'User Name - User who posted the document',
                'TCODE': 'Transaction Code - SAP transaction used to create document',
                'CPUDT': 'CPU Date - System date when document was created',
                'CPUTM': 'CPU Time - System time when document was created',
                'XBLNR': 'Reference Document Number - External reference number',
                'BKTXT': 'Document Header Text - Description or reference text',
                'AWKEY': 'Object Key - Internal system key for the document',
                'XREVERSED': 'Reversed Document - Flag indicating if document was reversed'
            },
            
            'BSEG': {
                'BUKRS': 'Company Code - 4-digit code representing legal entity',
                'BELNR': 'Document Number - Unique accounting document identifier',
                'GJAHR': 'Fiscal Year - Year of the accounting document',
                'BUZEI': 'Line Item - Sequential number within the document',
                'KOART': 'Account Type - Type of account (D=Customer, K=Vendor, S=G/L Account)',
                'KONTO': 'Account Number - G/L account, customer, or vendor number',
                'SHKZG': 'Debit/Credit Indicator - S=Debit, H=Credit',
                'DMBTR': 'Amount in Local Currency - Transaction amount in company code currency',
                'WRBTR': 'Amount in Document Currency - Transaction amount in document currency',
                'LIFNR': 'Vendor Number - Vendor account number (if vendor transaction)',
                'KUNNR': 'Customer Number - Customer account number (if customer transaction)',
                'KOSTL': 'Cost Center - Cost center for cost allocation',
                'AUFNR': 'Order Number - Internal order or project number',
                'PROJN': 'Project Number - Project identifier',
                'PSPNR': 'WBS Element - Work breakdown structure element',
                'SAKNR': 'G/L Account Number - General ledger account',
                'ZUONR': 'Assignment Number - Reference number for line item',
                'SGTXT': 'Line Item Text - Description text for the line item',
                'VALUT': 'Value Date - Date for interest calculation',
                'ZFBDT': 'Baseline Date - Payment baseline date',
                'ZTERM': 'Payment Terms - Payment terms code',
                'ZLSCH': 'Payment Method - Payment method code',
                'ZLSPR': 'Payment Block - Payment block indicator',
                'MWSKZ': 'Tax Code - Tax code for the transaction',
                'MWSTS': 'Tax Amount - Tax amount in local currency',
                'HWBAS': 'Tax Base Amount - Base amount for tax calculation',
                'FWBAS': 'Tax Base Amount in Document Currency - Tax base in document currency',
                'MENGE': 'Quantity - Quantity for material transactions',
                'MEINS': 'Unit of Measure - Unit of measure for quantity',
                'WRBTR': 'Amount in Document Currency - Amount in document currency',
                'DMBTR': 'Amount in Local Currency - Amount in local currency'
            },
            
            'LFA1': {
                'LIFNR': 'Vendor Number - Unique vendor identifier',
                'NAME1': 'Vendor Name - Primary name of the vendor',
                'NAME2': 'Vendor Name 2 - Secondary name line',
                'ORT01': 'City - City where vendor is located',
                'LAND1': 'Country - Country code for vendor location',
                'SPERR': 'Blocked - Blocking indicator for vendor',
                'LOEVM': 'Deletion Flag - Flag indicating if vendor is marked for deletion',
                'STRAS': 'Street Address - Street address of vendor',
                'PSTLZ': 'Postal Code - Postal code for vendor address',
                'REGIO': 'Region - State or region code',
                'TELF1': 'Telephone - Primary telephone number',
                'TELFX': 'Fax - Fax number',
                'XCPDK': 'One-Time Account - Flag for one-time vendor',
                'SPERR': 'Blocked - Blocking indicator',
                'LOEVM': 'Deletion Flag - Deletion indicator',
                'SPERM': 'Purchasing Block - Purchasing blocking indicator',
                'SPERR': 'Accounting Block - Accounting blocking indicator',
                'SPERZ': 'Payment Block - Payment blocking indicator'
            },
            
            'KNA1': {
                'KUNNR': 'Customer Number - Unique customer identifier',
                'NAME1': 'Customer Name - Primary name of the customer',
                'NAME2': 'Customer Name 2 - Secondary name line',
                'ORT01': 'City - City where customer is located',
                'LAND1': 'Country - Country code for customer location',
                'SPERR': 'Blocked - Blocking indicator for customer',
                'LOEVM': 'Deletion Flag - Flag indicating if customer is marked for deletion',
                'STRAS': 'Street Address - Street address of customer',
                'PSTLZ': 'Postal Code - Postal code for customer address',
                'REGIO': 'Region - State or region code',
                'TELF1': 'Telephone - Primary telephone number',
                'TELFX': 'Fax - Fax number',
                'XCPDK': 'One-Time Account - Flag for one-time customer',
                'SPERR': 'Blocked - Blocking indicator',
                'LOEVM': 'Deletion Flag - Deletion indicator',
                'SPERM': 'Sales Block - Sales blocking indicator',
                'SPERR': 'Accounting Block - Accounting blocking indicator',
                'SPERZ': 'Payment Block - Payment blocking indicator'
            },
            
            'SKAT': {
                'KTOPL': 'Chart of Accounts - Chart of accounts identifier',
                'SAKNR': 'G/L Account Number - General ledger account number',
                'TXT50': 'Account Description - Description of the G/L account',
                'XLOEV': 'Deletion Flag - Flag indicating if account is marked for deletion',
                'SPERR': 'Blocked - Blocking indicator for account',
                'KTOKS': 'Account Group - Account group classification',
                'XSPEA': 'Special G/L Account - Flag for special G/L account',
                'XLOEV': 'Deletion Flag - Deletion indicator',
                'SPERR': 'Blocked - Blocking indicator',
                'KTOKS': 'Account Group - Account group code',
                'XSPEA': 'Special G/L Account - Special G/L indicator'
            },
            
            'CSKS': {
                'KOKRS': 'Controlling Area - Controlling area identifier',
                'KOSTL': 'Cost Center - Cost center number',
                'DATBI': 'Valid To Date - Date until which cost center is valid',
                'KOSAR': 'Cost Center Category - Category of cost center',
                'VERAK': 'Person Responsible - Person responsible for cost center',
                'VERAK_USER': 'Responsible User - SAP user responsible for cost center',
                'SPERR': 'Blocked - Blocking indicator for cost center',
                'ABTEI': 'Department - Department code',
                'KOSAR': 'Cost Center Category - Category classification',
                'VERAK': 'Person Responsible - Responsible person',
                'VERAK_USER': 'Responsible User - Responsible SAP user'
            },
            
            'CSKA': {
                'KOKRS': 'Controlling Area - Controlling area identifier',
                'KOSAR': 'Cost Element Category - Cost element category',
                'DATBI': 'Valid To Date - Date until which category is valid',
                'VERAK': 'Person Responsible - Person responsible for category',
                'VERAK_USER': 'Responsible User - SAP user responsible for category',
                'SPERR': 'Blocked - Blocking indicator for category',
                'KOSAR': 'Cost Element Category - Category classification',
                'VERAK': 'Person Responsible - Responsible person',
                'VERAK_USER': 'Responsible User - Responsible SAP user'
            },
            
            'FAGLFLEXA': {
                'RBUKRS': 'Company Code - Company code for the balance',
                'RACCT': 'G/L Account - General ledger account number',
                'RYEAR': 'Fiscal Year - Fiscal year of the balance',
                'RTCUR': 'Currency - Currency of the balance',
                'RHCUR': 'Local Currency - Local currency of company code',
                'RUNIT': 'Unit - Unit of measure',
                'SEGMENT': 'Segment - Segment for segment reporting',
                'RACCT': 'G/L Account - General ledger account',
                'RYEAR': 'Fiscal Year - Fiscal year',
                'RTCUR': 'Currency - Currency code'
            },
            
            'FAGLFLEXT': {
                'RBUKRS': 'Company Code - Company code for the transaction',
                'RACCT': 'G/L Account - General ledger account number',
                'RYEAR': 'Fiscal Year - Fiscal year of the transaction',
                'RTCUR': 'Currency - Currency of the transaction',
                'RHCUR': 'Local Currency - Local currency of company code',
                'RUNIT': 'Unit - Unit of measure',
                'SEGMENT': 'Segment - Segment for segment reporting',
                'RACCT': 'G/L Account - General ledger account',
                'RYEAR': 'Fiscal Year - Fiscal year',
                'RTCUR': 'Currency - Currency code'
            }
        }
    
    def get_schema(self, report_type: str) -> Dict[str, str]:
        """
        Get schema mapping for a specific report type
        
        Args:
            report_type: SAP report type (e.g., 'BKPF', 'BSEG')
            
        Returns:
            Dictionary mapping column codes to descriptions
        """
        return self.SCHEMAS.get(report_type, {})
    
    def get_column_description(self, report_type: str, column_code: str) -> str:
        """
        Get description for a specific column in a report type
        
        Args:
            report_type: SAP report type
            column_code: Column code (e.g., 'BELNR', 'BUKRS')
            
        Returns:
            Column description or empty string if not found
        """
        schema = self.get_schema(report_type)
        return schema.get(column_code.upper(), '')
    
    def get_available_columns(self, report_type: str) -> List[str]:
        """
        Get list of available columns for a report type
        
        Args:
            report_type: SAP report type
            
        Returns:
            List of column codes
        """
        schema = self.get_schema(report_type)
        return list(schema.keys())
    
    def create_schema_summary(self, report_type: str, columns: List[str]) -> str:
        """
        Create a human-readable schema summary for LLM prompts
        
        Args:
            report_type: SAP report type
            columns: List of column names from the uploaded file
            
        Returns:
            Formatted schema summary string
        """
        schema = self.get_schema(report_type)
        if not schema:
            return f"Unknown report type: {report_type}"
        
        summary_parts = [f"The uploaded file contains {len(columns)} columns from a {report_type} table."]
        summary_parts.append("Columns:")
        
        for column in columns:
            column_upper = column.upper()
            description = schema.get(column_upper, f"Unknown column: {column}")
            summary_parts.append(f"    {column}: {description}")
        
        return "\n".join(summary_parts)
    
    def get_common_columns(self, report_types: List[str]) -> Dict[str, List[str]]:
        """
        Get common columns across multiple report types
        
        Args:
            report_types: List of SAP report types
            
        Returns:
            Dictionary mapping column codes to list of report types that contain them
        """
        common_columns = {}
        
        for report_type in report_types:
            schema = self.get_schema(report_type)
            for column in schema.keys():
                if column not in common_columns:
                    common_columns[column] = []
                common_columns[column].append(report_type)
        
        return common_columns
    
    def suggest_related_columns(self, report_type: str, column_code: str) -> List[str]:
        """
        Suggest related columns for a given column
        
        Args:
            report_type: SAP report type
            column_code: Column code
            
        Returns:
            List of related column codes
        """
        # Define relationships between columns
        relationships = {
            'BKPF': {
                'BELNR': ['BUKRS', 'GJAHR', 'BLART', 'BUDAT', 'WAERS'],
                'BUKRS': ['BELNR', 'GJAHR', 'BLART', 'BUDAT'],
                'GJAHR': ['BELNR', 'BUKRS', 'BLART', 'BUDAT'],
                'BLART': ['BELNR', 'BUKRS', 'GJAHR', 'BUDAT']
            },
            'BSEG': {
                'BELNR': ['BUKRS', 'GJAHR', 'BUZEI', 'KOART', 'KONTO'],
                'KONTO': ['KOART', 'SHKZG', 'DMBTR', 'WRBTR'],
                'LIFNR': ['KOART', 'KONTO', 'DMBTR', 'WRBTR'],
                'KUNNR': ['KOART', 'KONTO', 'DMBTR', 'WRBTR']
            }
        }
        
        return relationships.get(report_type, {}).get(column_code.upper(), [])
    
    def get_column_categories(self, report_type: str) -> Dict[str, List[str]]:
        """
        Get columns categorized by their purpose
        
        Args:
            report_type: SAP report type
            
        Returns:
            Dictionary mapping categories to column lists
        """
        categories = {
            'BKPF': {
                'Document Identification': ['BUKRS', 'BELNR', 'GJAHR'],
                'Document Details': ['BLART', 'BUDAT', 'WAERS', 'BKTXT'],
                'System Information': ['USNAM', 'TCODE', 'CPUDT', 'CPUTM'],
                'References': ['XBLNR', 'AWKEY']
            },
            'BSEG': {
                'Document Identification': ['BUKRS', 'BELNR', 'GJAHR', 'BUZEI'],
                'Account Information': ['KOART', 'KONTO', 'SAKNR'],
                'Amounts': ['SHKZG', 'DMBTR', 'WRBTR'],
                'Business Partners': ['LIFNR', 'KUNNR'],
                'Cost Objects': ['KOSTL', 'AUFNR', 'PROJN', 'PSPNR'],
                'Payment Information': ['VALUT', 'ZFBDT', 'ZTERM', 'ZLSCH']
            }
        }
        
        return categories.get(report_type, {})
    
    def validate_schema_completeness(self, report_type: str, columns: List[str]) -> Dict[str, any]:
        """
        Validate schema completeness for a report type
        
        Args:
            report_type: SAP report type
            columns: List of actual columns in the file
            
        Returns:
            Validation results
        """
        schema = self.get_schema(report_type)
        if not schema:
            return {
                'is_valid': False,
                'message': f'Unknown report type: {report_type}',
                'coverage': 0.0,
                'missing_important': []
            }
        
        normalized_columns = [col.upper() for col in columns]
        available_columns = set(schema.keys())
        actual_columns = set(normalized_columns)
        
        # Calculate coverage
        covered_columns = available_columns.intersection(actual_columns)
        coverage = len(covered_columns) / len(available_columns) if available_columns else 0.0
        
        # Identify missing important columns
        important_columns = {
            'BKPF': ['BUKRS', 'BELNR', 'GJAHR', 'BLART', 'BUDAT'],
            'BSEG': ['BUKRS', 'BELNR', 'GJAHR', 'BUZEI', 'KOART', 'KONTO']
        }
        
        missing_important = []
        if report_type in important_columns:
            missing_important = [col for col in important_columns[report_type] 
                               if col not in actual_columns]
        
        return {
            'is_valid': coverage >= 0.5 and len(missing_important) == 0,
            'message': f'Schema coverage: {coverage:.1%}',
            'coverage': coverage,
            'missing_important': missing_important,
            'covered_columns': list(covered_columns),
            'extra_columns': list(actual_columns - available_columns)
        } 