"""
SAP AI Demo - Schema Analyzer (Optimized)
Handles loading and querying mock SAP data with performance optimizations
"""

import pandas as pd
import os
from datetime import datetime, timedelta
import logging
from typing import Dict, List, Any, Optional

class SAPDataManager:
    def __init__(self, data_dir="mock_data"):
        self.data_dir = data_dir
        self.bkpf_df = None
        self.bseg_df = None
        self.lfa1_df = None
        self.kna1_df = None
        self.skat_df = None
        self.logger = logging.getLogger(__name__)
        self._cache = {}  # Simple cache for performance
        
    def load_mock_data(self):
        """Load mock SAP data from CSV files or create sample data if files don't exist"""
        try:
            # Try to load existing CSV files
            if os.path.exists(os.path.join(self.data_dir, "BKPF.csv")):
                self.bkpf_df = pd.read_csv(os.path.join(self.data_dir, "BKPF.csv"))
                self.bseg_df = pd.read_csv(os.path.join(self.data_dir, "BSEG.csv"))
                self.lfa1_df = pd.read_csv(os.path.join(self.data_dir, "LFA1.csv"))
                self.kna1_df = pd.read_csv(os.path.join(self.data_dir, "KNA1.csv"))
                self.skat_df = pd.read_csv(os.path.join(self.data_dir, "SKAT.csv"))
                self.logger.info("Loaded existing mock SAP data")
            else:
                # Create sample data if files don't exist
                self._create_sample_data()
                self.logger.info("Created sample SAP data")
                
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            self._create_sample_data()
    
    def _create_sample_data(self):
        """Create sample SAP data for demonstration"""
        # Create sample BKPF (Accounting Document Header)
        self.bkpf_df = pd.DataFrame({
            'BUKRS': ['1000', '1000', '1000', '1000', '1000'],  # Company Code
            'BELNR': ['1000000001', '1000000002', '1000000003', '1000000004', '1000000005'],  # Document Number
            'GJAHR': [2024, 2024, 2024, 2024, 2024],  # Fiscal Year
            'BLART': ['K1', 'S1', 'K2', 'S2', 'K1'],  # Document Type
            'BUDAT': ['2024-01-15', '2024-01-16', '2024-01-17', '2024-01-18', '2024-01-19'],  # Posting Date
            'WAERS': ['USD', 'USD', 'USD', 'USD', 'USD'],  # Currency
            'BKTXT': ['Customer Invoice', 'Vendor Invoice', 'Customer Payment', 'Vendor Payment', 'Customer Invoice']
        })
        
        # Create sample BSEG (Accounting Document Segment)
        self.bseg_df = pd.DataFrame({
            'BUKRS': ['1000', '1000', '1000', '1000', '1000', '1000', '1000', '1000'],
            'BELNR': ['1000000001', '1000000001', '1000000002', '1000000002', '1000000003', '1000000003', '1000000004', '1000000004'],
            'GJAHR': [2024, 2024, 2024, 2024, 2024, 2024, 2024, 2024],
            'BUZEI': [1, 2, 1, 2, 1, 2, 1, 2],  # Line Item
            'KOART': ['D', 'S', 'K', 'S', 'D', 'S', 'K', 'S'],  # Account Type (D=Customer, K=Vendor, S=G/L)
            'KONTO': ['120000', '400000', '200000', '400000', '120000', '100000', '200000', '100000'],  # Account
            'SHKZG': ['S', 'H', 'H', 'S', 'H', 'S', 'S', 'H'],  # Debit/Credit (S=Debit, H=Credit)
            'DMBTR': [5000.00, 5000.00, 2500.00, 2500.00, 3000.00, 3000.00, 1500.00, 1500.00],  # Amount in Local Currency
            'WRBTR': [5000.00, 5000.00, 2500.00, 2500.00, 3000.00, 3000.00, 1500.00, 1500.00],  # Amount in Document Currency
            'LIFNR': ['', '', 'V001', '', '', '', 'V002', ''],  # Vendor Number
            'KUNNR': ['C001', '', '', '', 'C001', '', '', ''],  # Customer Number
        })
        
        # Create sample LFA1 (Vendor Master)
        self.lfa1_df = pd.DataFrame({
            'LIFNR': ['V001', 'V002', 'V003'],  # Vendor Number
            'NAME1': ['ABC Supplies Inc', 'XYZ Manufacturing', 'Tech Solutions Ltd'],  # Vendor Name
            'ORT01': ['New York', 'Chicago', 'Los Angeles'],  # City
            'LAND1': ['US', 'US', 'US'],  # Country
            'SPERR': ['', '', ''],  # Blocked (empty = not blocked)
            'LOEVM': ['', '', '']   # Deletion Flag (empty = not deleted)
        })
        
        # Create sample KNA1 (Customer Master)
        self.kna1_df = pd.DataFrame({
            'KUNNR': ['C001', 'C002', 'C003'],  # Customer Number
            'NAME1': ['Global Corp', 'Local Business', 'Startup Inc'],  # Customer Name
            'ORT01': ['Boston', 'Seattle', 'Austin'],  # City
            'LAND1': ['US', 'US', 'US'],  # Country
            'SPERR': ['', '', ''],  # Blocked (empty = not blocked)
            'LOEVM': ['', '', '']   # Deletion Flag (empty = not deleted)
        })
        
        # Create sample SKAT (G/L Account Master)
        self.skat_df = pd.DataFrame({
            'KTOPL': ['INT1', 'INT1', 'INT1', 'INT1'],  # Chart of Accounts
            'SAKNR': ['100000', '120000', '200000', '400000'],  # G/L Account Number
            'TXT50': ['Cash', 'Accounts Receivable', 'Accounts Payable', 'Revenue'],  # Account Description
            'XLOEV': ['', '', '', ''],  # Deletion Flag (empty = not deleted)
            'SPERR': ['', '', '', '']   # Blocked (empty = not blocked)
        })
        
        # Save sample data to CSV files
        os.makedirs(self.data_dir, exist_ok=True)
        self.bkpf_df.to_csv(os.path.join(self.data_dir, "BKPF.csv"), index=False)
        self.bseg_df.to_csv(os.path.join(self.data_dir, "BSEG.csv"), index=False)
        self.lfa1_df.to_csv(os.path.join(self.data_dir, "LFA1.csv"), index=False)
        self.kna1_df.to_csv(os.path.join(self.data_dir, "KNA1.csv"), index=False)
        self.skat_df.to_csv(os.path.join(self.data_dir, "SKAT.csv"), index=False)
    
    def get_data_summary(self):
        """Get summary of available data for context"""
        if self.bkpf_df is None:
            return {}
            
        return {
            'bkpf_count': len(self.bkpf_df),
            'bseg_count': len(self.bseg_df),
            'lfa1_count': len(self.lfa1_df),
            'kna1_count': len(self.kna1_df),
            'skat_count': len(self.skat_df),
            'date_range': f"{self.bkpf_df['BUDAT'].min()} to {self.bkpf_df['BUDAT'].max()}",
            'company_codes': list(self.bkpf_df['BUKRS'].unique()),
            'document_types': list(self.bkpf_df['BLART'].unique()),
            'currencies': list(self.bkpf_df['WAERS'].unique())
        }
    
    def query_overdue_invoices(self):
        """Example query: Find overdue invoices"""
        # This would typically involve business logic to determine overdue status
        # For demo purposes, we'll show recent invoices
        recent_invoices = self.bkpf_df[self.bkpf_df['BLART'] == 'K1'].head(3)
        return recent_invoices.to_dict('records')
    
    def query_vendor_payments(self, quarter=None):
        """Example query: Get vendor payments"""
        vendor_payments = self.bkpf_df[self.bkpf_df['BLART'] == 'S2']
        if quarter:
            # Add quarter filtering logic here
            pass
        return vendor_payments.to_dict('records')
    
    def query_account_balance(self, account_number):
        """Example query: Get account balance"""
        account_transactions = self.bseg_df[self.bseg_df['KONTO'] == account_number]
        debits = account_transactions[account_transactions['SHKZG'] == 'S']['DMBTR'].sum()
        credits = account_transactions[account_transactions['SHKZG'] == 'H']['DMBTR'].sum()
        balance = debits - credits
        return {
            'account': account_number,
            'debits': debits,
            'credits': credits,
            'balance': balance,
            'transaction_count': len(account_transactions)
        }

class SAPSchemaAnalyzer:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._cache = {}  # Performance cache
        
    def analyze_csv_file(self, file_path: str, sample_size: int = 5000) -> Dict[str, Any]:
        """
        Analyze a CSV file and return comprehensive schema metadata (Optimized)
        
        Args:
            file_path: Path to the CSV file
            sample_size: Number of rows to sample for analysis (reduced for performance)
            
        Returns:
            Dictionary containing schema analysis results
        """
        try:
            # Check cache first
            cache_key = f"{file_path}_{os.path.getmtime(file_path)}"
            if cache_key in self._cache:
                self.logger.info("Using cached schema analysis")
                return self._cache[cache_key]
            
            # Read the CSV file efficiently
            df = pd.read_csv(file_path, nrows=sample_size)  # Limit initial read
            
            # Get basic file info quickly
            total_rows = len(pd.read_csv(file_path, nrows=None))  # Get actual count
            total_columns = len(df.columns)
            
            # Use smaller sample for analysis
            if total_rows > sample_size:
                df_sample = df.sample(n=min(sample_size, len(df)), random_state=42)
                self.logger.info(f"Analyzing sample of {len(df_sample)} rows from {total_rows} total rows")
            else:
                df_sample = df
                
            # Quick column analysis (optimized)
            column_analysis = self._analyze_columns_fast(df_sample, df)
            
            # Detect SAP table type
            sap_table_type = self._detect_sap_table_type_fast(df.columns, column_analysis)
            
            # Generate basic insights (minimal processing)
            data_insights = self._generate_basic_insights(df_sample, column_analysis)
            
            # Create query suggestions
            query_suggestions = self._generate_query_suggestions_fast(sap_table_type, column_analysis, data_insights)
            
            analysis_result = {
                'file_info': {
                    'total_rows': total_rows,
                    'total_columns': total_columns,
                    'file_size_mb': round(os.path.getsize(file_path) / 1024 / 1024, 2),
                    'analyzed_rows': len(df_sample)
                },
                'sap_table_type': sap_table_type,
                'column_analysis': column_analysis,
                'data_insights': data_insights,
                'query_suggestions': query_suggestions,
                'schema_summary': self._create_schema_summary_fast(column_analysis, sap_table_type)
            }
            
            # Cache the result
            self._cache[cache_key] = analysis_result
            
            self.logger.info(f"Schema analysis completed for {file_path}")
            return analysis_result
            
        except Exception as e:
            self.logger.error(f"Error analyzing CSV file {file_path}: {str(e)}")
            raise
    
    def _analyze_columns_fast(self, df_sample: pd.DataFrame, df_full: pd.DataFrame) -> Dict[str, Any]:
        """Fast column analysis with minimal processing"""
        column_analysis = {}
        
        for column in df_sample.columns:
            col_data = df_sample[column]
            
            # Basic column info (fast)
            col_info = {
                'name': column,
                'dtype': str(col_data.dtype),
                'null_count': col_data.isnull().sum(),
                'null_percentage': round((col_data.isnull().sum() / len(col_data)) * 100, 2),
                'unique_count': col_data.nunique(),
                'unique_percentage': round((col_data.nunique() / len(col_data)) * 100, 2)
            }
            
            # Quick data type detection
            col_info.update(self._detect_column_patterns_fast(col_data))
            
            # Basic statistics (only if needed)
            if col_info['data_category'] == 'numeric':
                col_info['statistics'] = self._analyze_numeric_column_fast(col_data)
            elif col_info['data_category'] == 'date':
                col_info['statistics'] = self._analyze_date_column_fast(col_data)
            
            # Quick SAP pattern detection
            col_info['sap_patterns'] = self._detect_sap_patterns_fast(column, col_data)
            
            column_analysis[column] = col_info
            
        return column_analysis
    
    def _detect_column_patterns_fast(self, col_data: pd.Series) -> Dict[str, Any]:
        """Fast column pattern detection"""
        clean_data = col_data.dropna()
        
        if len(clean_data) == 0:
            return {'data_category': 'empty', 'patterns': []}
        
        # Quick numeric check
        try:
            numeric_data = pd.to_numeric(clean_data, errors='coerce')
            if numeric_data.notna().sum() / len(clean_data) > 0.8:
                return {'data_category': 'numeric', 'patterns': ['numeric_values']}
        except:
            pass
        
        # Quick date check
        try:
            date_data = pd.to_datetime(clean_data, errors='coerce')
            if date_data.notna().sum() / len(clean_data) > 0.8:
                return {'data_category': 'date', 'patterns': ['date_values']}
        except:
            pass
        
        # Quick categorical check
        if clean_data.nunique() / len(clean_data) < 0.1:
            return {'data_category': 'categorical', 'patterns': ['categorical_values']}
        
        return {'data_category': 'text', 'patterns': ['text_values']}
    
    def _analyze_numeric_column_fast(self, col_data: pd.Series) -> Dict[str, Any]:
        """Fast numeric column analysis"""
        clean_data = pd.to_numeric(col_data, errors='coerce').dropna()
        
        if len(clean_data) == 0:
            return {}
        
        return {
            'min': float(clean_data.min()),
            'max': float(clean_data.max()),
            'mean': float(clean_data.mean()),
            'sum': float(clean_data.sum())
        }
    
    def _analyze_date_column_fast(self, col_data: pd.Series) -> Dict[str, Any]:
        """Fast date column analysis"""
        clean_data = pd.to_datetime(col_data, errors='coerce').dropna()
        
        if len(clean_data) == 0:
            return {}
        
        return {
            'min_date': clean_data.min().isoformat(),
            'max_date': clean_data.max().isoformat(),
            'date_range_days': (clean_data.max() - clean_data.min()).days
        }
    
    def _detect_sap_patterns_fast(self, column_name: str, col_data: pd.Series) -> List[str]:
        """Fast SAP pattern detection"""
        patterns = []
        column_upper = column_name.upper()
        
        # Quick pattern matching
        sap_patterns = {
            'BUKRS': 'company_code',
            'BELNR': 'document_number',
            'GJAHR': 'fiscal_year',
            'BLART': 'document_type',
            'BUDAT': 'posting_date',
            'WAERS': 'currency',
            'LIFNR': 'vendor_number',
            'KUNNR': 'customer_number',
            'KONTO': 'gl_account',
            'SHKZG': 'debit_credit_indicator',
            'DMBTR': 'local_amount',
            'WRBTR': 'document_amount'
        }
        
        if column_upper in sap_patterns:
            patterns.append(sap_patterns[column_upper])
        
        return patterns
    
    def _detect_sap_table_type_fast(self, columns: List[str], column_analysis: Dict) -> str:
        """Fast SAP table type detection"""
        column_names = [col.upper() for col in columns]
        
        # Quick pattern matching
        if all(col in column_names for col in ['BUKRS', 'BELNR', 'GJAHR', 'BLART']):
            return 'BKPF'
        elif all(col in column_names for col in ['BUKRS', 'BELNR', 'GJAHR', 'BUZEI', 'KOART']):
            return 'BSEG'
        elif all(col in column_names for col in ['LIFNR', 'NAME1']):
            return 'LFA1'
        elif all(col in column_names for col in ['KUNNR', 'NAME1']):
            return 'KNA1'
        elif all(col in column_names for col in ['KTOPL', 'SAKNR']):
            return 'SKAT'
        
        return 'UNKNOWN'
    
    def _generate_basic_insights(self, df_sample: pd.DataFrame, column_analysis: Dict) -> Dict[str, Any]:
        """Generate basic insights quickly"""
        insights = {
            'data_quality': {'null_percentage': 0},
            'business_insights': [],
            'anomalies': []
        }
        
        # Quick null percentage calculation
        total_cells = len(df_sample) * len(df_sample.columns)
        null_cells = sum(col_info['null_count'] for col_info in column_analysis.values())
        insights['data_quality']['null_percentage'] = round((null_cells / total_cells) * 100, 2)
        
        return insights
    
    def _generate_query_suggestions_fast(self, sap_table_type: str, column_analysis: Dict, data_insights: Dict) -> List[str]:
        """Generate query suggestions quickly"""
        suggestions = {
            'BKPF': [
                "Show documents posted in the last 30 days",
                "Which document types have the highest volume?",
                "Find documents with specific posting dates"
            ],
            'BSEG': [
                "Show line items with amounts over $10,000",
                "Which accounts have the most transactions?",
                "Find debit vs credit entries"
            ],
            'LFA1': [
                "Show vendors by location",
                "Find vendors with specific names"
            ],
            'KNA1': [
                "Show customers by location",
                "Find customers with specific names"
            ]
        }
        
        return suggestions.get(sap_table_type, ["Show all records", "Find records with specific criteria"])
    
    def _create_schema_summary_fast(self, column_analysis: Dict, sap_table_type: str) -> str:
        """Create fast schema summary"""
        return f"This appears to be a {sap_table_type} table with {len(column_analysis)} columns." 