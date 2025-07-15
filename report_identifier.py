"""
SAP AI Demo - Report Identifier
Identifies SAP report types based on column patterns in uploaded CSV files
"""

from typing import List, Dict, Optional
import logging

class SAPReportIdentifier:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        
        # Define required column patterns for each SAP table type
        self.table_patterns = {
            'BKPF': {
                'required_columns': ['BUKRS', 'BELNR', 'GJAHR', 'BLART', 'BUDAT'],
                'optional_columns': ['WAERS', 'BKTXT', 'USNAM', 'TCODE', 'CPUDT'],
                'description': 'Accounting Document Header',
                'confidence_threshold': 0.8
            },
            'BSEG': {
                'required_columns': ['BUKRS', 'BELNR', 'GJAHR', 'BUZEI', 'KOART', 'KONTO'],
                'optional_columns': ['SHKZG', 'DMBTR', 'WRBTR', 'LIFNR', 'KUNNR', 'KOSTL'],
                'description': 'Accounting Document Segment',
                'confidence_threshold': 0.8
            },
            'LFA1': {
                'required_columns': ['LIFNR', 'NAME1'],
                'optional_columns': ['ORT01', 'LAND1', 'SPERR', 'LOEVM', 'STRAS', 'PSTLZ'],
                'description': 'Vendor Master Data',
                'confidence_threshold': 0.7
            },
            'KNA1': {
                'required_columns': ['KUNNR', 'NAME1'],
                'optional_columns': ['ORT01', 'LAND1', 'SPERR', 'LOEVM', 'STRAS', 'PSTLZ'],
                'description': 'Customer Master Data',
                'confidence_threshold': 0.7
            },
            'SKAT': {
                'required_columns': ['KTOPL', 'SAKNR', 'TXT50'],
                'optional_columns': ['XLOEV', 'SPERR', 'KTOKS', 'XSPEA'],
                'description': 'G/L Account Master Data',
                'confidence_threshold': 0.7
            },
            'CSKS': {
                'required_columns': ['KOKRS', 'KOSTL', 'DATBI'],
                'optional_columns': ['KOSAR', 'VERAK', 'VERAK_USER', 'SPERR'],
                'description': 'Cost Center Master Data',
                'confidence_threshold': 0.7
            },
            'CSKA': {
                'required_columns': ['KOKRS', 'KOSAR', 'DATBI'],
                'optional_columns': ['VERAK', 'VERAK_USER', 'SPERR'],
                'description': 'Cost Element Master Data',
                'confidence_threshold': 0.7
            },
            'FAGLFLEXA': {
                'required_columns': ['RBUKRS', 'RACCT', 'RYEAR', 'RACCT'],
                'optional_columns': ['RTCUR', 'RHCUR', 'RUNIT', 'SEGMENT'],
                'description': 'New G/L Account Balances',
                'confidence_threshold': 0.8
            },
            'FAGLFLEXT': {
                'required_columns': ['RBUKRS', 'RACCT', 'RYEAR', 'RACCT', 'RTCUR'],
                'optional_columns': ['RHCUR', 'RUNIT', 'SEGMENT'],
                'description': 'New G/L Account Line Items',
                'confidence_threshold': 0.8
            }
        }
    
    def identify_report_type(self, columns: List[str]) -> Dict[str, any]:
        """
        Identify the SAP report type based on column patterns
        
        Args:
            columns: List of column names from the CSV file
            
        Returns:
            Dictionary containing report type, confidence, and metadata
        """
        try:
            # Normalize column names (uppercase, remove spaces)
            normalized_columns = [col.upper().strip() for col in columns]
            
            best_match = None
            highest_confidence = 0.0
            
            for table_type, pattern in self.table_patterns.items():
                confidence = self._calculate_confidence(normalized_columns, pattern)
                
                if confidence > highest_confidence and confidence >= pattern['confidence_threshold']:
                    highest_confidence = confidence
                    best_match = {
                        'table_type': table_type,
                        'confidence': confidence,
                        'description': pattern['description'],
                        'matched_columns': self._get_matched_columns(normalized_columns, pattern),
                        'missing_columns': self._get_missing_columns(normalized_columns, pattern),
                        'extra_columns': self._get_extra_columns(normalized_columns, pattern)
                    }
            
            if best_match:
                self.logger.info(f"Identified report type: {best_match['table_type']} (confidence: {best_match['confidence']:.2f})")
                return best_match
            else:
                self.logger.warning(f"Could not identify report type for columns: {columns}")
                return {
                    'table_type': 'UNKNOWN',
                    'confidence': 0.0,
                    'description': 'Unknown SAP Table',
                    'matched_columns': [],
                    'missing_columns': [],
                    'extra_columns': normalized_columns
                }
                
        except Exception as e:
            self.logger.error(f"Error identifying report type: {str(e)}")
            return {
                'table_type': 'ERROR',
                'confidence': 0.0,
                'description': 'Error during identification',
                'matched_columns': [],
                'missing_columns': [],
                'extra_columns': []
            }
    
    def _calculate_confidence(self, columns: List[str], pattern: Dict) -> float:
        """
        Calculate confidence score for a table pattern match
        
        Args:
            columns: List of normalized column names
            pattern: Table pattern dictionary
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        required_columns = pattern['required_columns']
        optional_columns = pattern['optional_columns']
        
        # Count required columns present
        required_matches = sum(1 for col in required_columns if col in columns)
        required_score = required_matches / len(required_columns) if required_columns else 0.0
        
        # Count optional columns present
        optional_matches = sum(1 for col in optional_columns if col in columns)
        optional_score = optional_matches / len(optional_columns) if optional_columns else 0.0
        
        # Weight required columns more heavily (70% required, 30% optional)
        if required_columns and optional_columns:
            confidence = (required_score * 0.7) + (optional_score * 0.3)
        elif required_columns:
            confidence = required_score
        elif optional_columns:
            confidence = optional_score * 0.5  # Lower weight for optional-only matches
        else:
            confidence = 0.0
        
        return confidence
    
    def _get_matched_columns(self, columns: List[str], pattern: Dict) -> List[str]:
        """Get list of columns that match the pattern"""
        all_pattern_columns = pattern['required_columns'] + pattern['optional_columns']
        return [col for col in columns if col in all_pattern_columns]
    
    def _get_missing_columns(self, columns: List[str], pattern: Dict) -> List[str]:
        """Get list of required columns that are missing"""
        return [col for col in pattern['required_columns'] if col not in columns]
    
    def _get_extra_columns(self, columns: List[str], pattern: Dict) -> List[str]:
        """Get list of columns not in the pattern"""
        all_pattern_columns = pattern['required_columns'] + pattern['optional_columns']
        return [col for col in columns if col not in all_pattern_columns]
    
    def get_table_description(self, table_type: str) -> str:
        """
        Get description for a table type
        
        Args:
            table_type: SAP table type (e.g., 'BKPF', 'BSEG')
            
        Returns:
            Description string
        """
        return self.table_patterns.get(table_type, {}).get('description', 'Unknown table type')
    
    def get_suggested_queries(self, table_type: str) -> List[str]:
        """
        Get suggested queries for a specific table type
        
        Args:
            table_type: SAP table type
            
        Returns:
            List of suggested query strings
        """
        suggestions = {
            'BKPF': [
                "Show documents posted in the last 30 days",
                "Which document types have the highest volume?",
                "Find documents with specific posting dates",
                "Show documents by company code",
                "Identify documents posted outside business hours"
            ],
            'BSEG': [
                "Show line items with amounts over $10,000",
                "Which accounts have the most transactions?",
                "Find debit vs credit entries",
                "Show transactions by vendor or customer",
                "Analyze posting key patterns"
            ],
            'LFA1': [
                "Show vendors by location",
                "Find vendors with specific names",
                "Show vendor distribution by country",
                "Identify blocked vendors",
                "Show vendors by payment terms"
            ],
            'KNA1': [
                "Show customers by location",
                "Find customers with specific names",
                "Show customer distribution by country",
                "Identify blocked customers",
                "Show customers by payment terms"
            ],
            'SKAT': [
                "Show account descriptions",
                "Find accounts by account type",
                "Show blocked accounts",
                "Analyze account hierarchy",
                "Find accounts by text search"
            ]
        }
        
        return suggestions.get(table_type, [
            "Show all records",
            "Find records with specific criteria",
            "Analyze data patterns",
            "Generate summary statistics"
        ])
    
    def validate_table_structure(self, table_type: str, columns: List[str]) -> Dict[str, any]:
        """
        Validate if the table structure matches expected patterns
        
        Args:
            table_type: SAP table type
            columns: List of column names
            
        Returns:
            Validation results dictionary
        """
        if table_type not in self.table_patterns:
            return {
                'is_valid': False,
                'message': f'Unknown table type: {table_type}',
                'issues': []
            }
        
        pattern = self.table_patterns[table_type]
        normalized_columns = [col.upper().strip() for col in columns]
        
        missing_required = self._get_missing_columns(normalized_columns, pattern)
        extra_columns = self._get_extra_columns(normalized_columns, pattern)
        
        issues = []
        if missing_required:
            issues.append(f"Missing required columns: {', '.join(missing_required)}")
        
        if extra_columns:
            issues.append(f"Unexpected columns: {', '.join(extra_columns)}")
        
        is_valid = len(missing_required) == 0
        
        return {
            'is_valid': is_valid,
            'message': 'Valid table structure' if is_valid else 'Table structure issues found',
            'issues': issues,
            'missing_required': missing_required,
            'extra_columns': extra_columns
        } 