"""
SAP AI Demo - Data Manager
Handles loading and querying mock SAP data
"""

import pandas as pd
import os
from datetime import datetime, timedelta
import logging

class SAPDataManager:
    def __init__(self, data_dir="mock_data"):
        self.data_dir = data_dir
        self.bkpf_df = None
        self.bseg_df = None
        self.lfa1_df = None
        self.kna1_df = None
        self.skat_df = None
        self.logger = logging.getLogger(__name__)
        
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