"""
SAP AI Demo - Query Router
Routes natural language queries to appropriate data handlers
"""

import re
from datetime import datetime, timedelta
import logging

class SAPQueryRouter:
    def __init__(self, data_manager, logger):
        self.data_manager = data_manager
        self.logger = logger
        
    def route_query(self, user_question):
        """
        Route user question to appropriate handler and return structured response
        
        Args:
            user_question (str): Natural language question
            
        Returns:
            dict: Structured response with query_type, data, and explanation
        """
        question_lower = user_question.lower()
        
        # Define query patterns and their handlers
        query_patterns = [
            {
                'pattern': r'overdue|past due|late.*invoice',
                'handler': self._handle_overdue_invoices,
                'type': 'overdue_invoices'
            },
            {
                'pattern': r'vendor.*payment|supplier.*payment',
                'handler': self._handle_vendor_payments,
                'type': 'vendor_payments'
            },
            {
                'pattern': r'customer.*payment|client.*payment',
                'handler': self._handle_customer_payments,
                'type': 'customer_payments'
            },
            {
                'pattern': r'account.*balance|balance.*account',
                'handler': self._handle_account_balance,
                'type': 'account_balance'
            },
            {
                'pattern': r'top.*vendor|highest.*vendor|vendor.*amount',
                'handler': self._handle_top_vendors,
                'type': 'top_vendors'
            },
            {
                'pattern': r'top.*customer|highest.*customer|customer.*amount',
                'handler': self._handle_top_customers,
                'type': 'top_customers'
            },
            {
                'pattern': r'trend|trends|pattern|over time',
                'handler': self._handle_trends,
                'type': 'trends'
            },
            {
                'pattern': r'error|posted.*error|incorrect',
                'handler': self._handle_error_documents,
                'type': 'error_documents'
            }
        ]
        
        # Try to match query patterns
        for pattern_info in query_patterns:
            if re.search(pattern_info['pattern'], question_lower):
                try:
                    result = pattern_info['handler'](user_question)
                    self.logger.log_data_operation(
                        'query_routed',
                        pattern_info['type'],
                        len(result.get('data', [])),
                        {'original_question': user_question}
                    )
                    return result
                except Exception as e:
                    self.logger.log_error(
                        'query_routing_error',
                        str(e),
                        {'query_type': pattern_info['type'], 'question': user_question}
                    )
                    return {
                        'query_type': 'error',
                        'data': [],
                        'explanation': f"Error processing query: {str(e)}"
                    }
        
        # Default: general query
        return self._handle_general_query(user_question)
    
    def _handle_overdue_invoices(self, question):
        """Handle overdue invoice queries"""
        data = self.data_manager.query_overdue_invoices()
        return {
            'query_type': 'overdue_invoices',
            'data': data,
            'explanation': f"Found {len(data)} recent customer invoices. In a real SAP system, this would include business logic to determine overdue status based on payment terms and due dates."
        }
    
    def _handle_vendor_payments(self, question):
        """Handle vendor payment queries"""
        # Extract quarter if mentioned
        quarter = None
        if 'q1' in question.lower() or 'first quarter' in question.lower():
            quarter = 'Q1'
        elif 'q2' in question.lower() or 'second quarter' in question.lower():
            quarter = 'Q2'
        elif 'q3' in question.lower() or 'third quarter' in question.lower():
            quarter = 'Q3'
        elif 'q4' in question.lower() or 'fourth quarter' in question.lower():
            quarter = 'Q4'
        
        data = self.data_manager.query_vendor_payments(quarter)
        return {
            'query_type': 'vendor_payments',
            'data': data,
            'explanation': f"Found {len(data)} vendor payment documents{f' for {quarter}' if quarter else ''}. These are documents with document type 'S2' (Vendor Payment)."
        }
    
    def _handle_customer_payments(self, question):
        """Handle customer payment queries"""
        data = self.data_manager.bkpf_df[self.data_manager.bkpf_df['BLART'] == 'K2'].to_dict('records')
        return {
            'query_type': 'customer_payments',
            'data': data,
            'explanation': f"Found {len(data)} customer payment documents. These are documents with document type 'K2' (Customer Payment)."
        }
    
    def _handle_account_balance(self, question):
        """Handle account balance queries"""
        # Try to extract account number from question
        account_match = re.search(r'(\d{6})', question)
        if account_match:
            account_number = account_match.group(1)
        else:
            # Default to Accounts Receivable
            account_number = '120000'
        
        data = self.data_manager.query_account_balance(account_number)
        return {
            'query_type': 'account_balance',
            'data': data,
            'explanation': f"Account {data['account']} balance: ${data['balance']:,.2f} (Debits: ${data['debits']:,.2f}, Credits: ${data['credits']:,.2f})"
        }
    
    def _handle_top_vendors(self, question):
        """Handle top vendor queries"""
        # Join BSEG with LFA1 to get vendor information
        vendor_data = self.data_manager.bseg_df[
            (self.data_manager.bseg_df['KOART'] == 'K') & 
            (self.data_manager.bseg_df['LIFNR'] != '')
        ].copy()
        
        if not vendor_data.empty:
            vendor_data = vendor_data.merge(
                self.data_manager.lfa1_df,
                left_on='LIFNR',
                right_on='LIFNR',
                how='left'
            )
            
            # Group by vendor and sum amounts
            vendor_totals = vendor_data.groupby(['LIFNR', 'NAME1'])['DMBTR'].sum().reset_index()
            vendor_totals = vendor_totals.sort_values('DMBTR', ascending=False).head(5)
            
            data = vendor_totals.to_dict('records')
        else:
            data = []
        
        return {
            'query_type': 'top_vendors',
            'data': data,
            'explanation': f"Top {len(data)} vendors by transaction amount. This analysis combines vendor master data with transaction line items."
        }
    
    def _handle_top_customers(self, question):
        """Handle top customer queries"""
        # Join BSEG with KNA1 to get customer information
        customer_data = self.data_manager.bseg_df[
            (self.data_manager.bseg_df['KOART'] == 'D') & 
            (self.data_manager.bseg_df['KUNNR'] != '')
        ].copy()
        
        if not customer_data.empty:
            customer_data = customer_data.merge(
                self.data_manager.kna1_df,
                left_on='KUNNR',
                right_on='KUNNR',
                how='left'
            )
            
            # Group by customer and sum amounts
            customer_totals = customer_data.groupby(['KUNNR', 'NAME1'])['DMBTR'].sum().reset_index()
            customer_totals = customer_totals.sort_values('DMBTR', ascending=False).head(5)
            
            data = customer_totals.to_dict('records')
        else:
            data = []
        
        return {
            'query_type': 'top_customers',
            'data': data,
            'explanation': f"Top {len(data)} customers by transaction amount. This analysis combines customer master data with transaction line items."
        }
    
    def _handle_trends(self, question):
        """Handle trend analysis queries"""
        # Simple trend analysis by document type
        doc_type_counts = self.data_manager.bkpf_df['BLART'].value_counts().to_dict()
        
        data = {
            'document_type_distribution': doc_type_counts,
            'total_documents': len(self.data_manager.bkpf_df),
            'date_range': f"{self.data_manager.bkpf_df['BUDAT'].min()} to {self.data_manager.bkpf_df['BUDAT'].max()}"
        }
        
        return {
            'query_type': 'trends',
            'data': data,
            'explanation': f"Document distribution analysis. Total documents: {data['total_documents']} across date range: {data['date_range']}"
        }
    
    def _handle_error_documents(self, question):
        """Handle error document queries"""
        # In a real system, this would look for documents with error indicators
        # For demo, we'll show recent documents
        recent_docs = self.data_manager.bkpf_df.head(3).to_dict('records')
        
        return {
            'query_type': 'error_documents',
            'data': recent_docs,
            'explanation': "In a real SAP system, this would identify documents with posting errors, blocked status, or other issues. For demo purposes, showing recent documents."
        }
    
    def _handle_general_query(self, question):
        """Handle general queries that don't match specific patterns"""
        # Get data summary for context
        data_summary = self.data_manager.get_data_summary()
        
        return {
            'query_type': 'general',
            'data': data_summary,
            'explanation': f"General SAP data overview. Available: {data_summary.get('bkpf_count', 0)} documents, {data_summary.get('bseg_count', 0)} line items across {data_summary.get('date_range', 'unknown period')}."
        } 