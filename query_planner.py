"""
SAP AI Demo - Query Planner
Translates natural language queries into executable Pandas operations
"""

import pandas as pd
import re
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime, timedelta

class SAPQueryPlanner:
    def __init__(self, schema_analysis: Dict[str, Any]):
        self.schema_analysis = schema_analysis
        self.column_analysis = schema_analysis.get('column_analysis', {})
        self.sap_table_type = schema_analysis.get('sap_table_type', 'UNKNOWN')
        self.logger = logging.getLogger(__name__)
        
    def plan_query(self, user_question: str) -> Dict[str, Any]:
        """
        Plan a query based on user question and schema analysis
        
        Args:
            user_question: Natural language question
            
        Returns:
            Dictionary containing query plan and execution details
        """
        try:
            # Parse the question to extract intent and parameters
            parsed_intent = self._parse_question_intent(user_question)
            
            # Generate query plan
            query_plan = self._generate_query_plan(parsed_intent)
            
            # Add original question for context
            query_plan['original_question'] = user_question
            
            # Validate the plan
            validation_result = self._validate_query_plan(query_plan)
            
            if not validation_result['is_valid']:
                return {
                    'status': 'ambiguous',
                    'message': validation_result['message'],
                    'clarification_questions': validation_result['clarification_questions']
                }
            
            return {
                'status': 'success',
                'query_plan': query_plan,
                'execution_steps': self._generate_execution_steps(query_plan),
                'estimated_result_size': self._estimate_result_size(query_plan),
                'explanation': self._explain_query_plan(query_plan)
            }
            
        except Exception as e:
            self.logger.error(f"Error planning query: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error planning query: {str(e)}"
            }
    
    def _parse_question_intent(self, question: str) -> Dict[str, Any]:
        """Parse natural language question to extract intent and parameters"""
        question_lower = question.lower()

        # Debug: Log the question being parsed
        self.logger.info(f"[DEBUG] Parsing question: '{question_lower}'")

        # Patch: Detect 'most frequent', 'most common', 'top N' for categorical columns
        most_freq_patterns = [
            r'most frequent ([\w\s]+)[\s\?\.!]*$',
            r'most common ([\w\s]+)[\s\?\.!]*$',
            r'top (\d+) ([\w\s]+)[\s\?\.!]*$',
            r'top ([\w\s]+)[\s\?\.!]*$',
            r'which ([\w\s]+) are used most[\s\?\.!]*$',
            r'what ([\w\s]+) are used most[\s\?\.!]*$',
            r'what are the most common ([\w\s]+)[\s\?\.!]*$',
            r'what are the most frequent ([\w\s]+)[\s\?\.!]*$',
            r'what ([\w\s]+) were used most frequently[\s\?\.!]*$',
            r'what ([\w\s]+) were used most commonly[\s\?\.!]*$',
            r'which ([\w\s]+) were used most frequently[\s\?\.!]*$',
            r'which ([\w\s]+) were used most commonly[\s\?\.!]*$',
        ]
        for pattern in most_freq_patterns:
            match = re.search(pattern, question_lower)
            # Debug: Log which pattern (if any) matches
            self.logger.info(f"[DEBUG] Testing pattern: '{pattern}' - Match: {bool(match)}")
            if match:
                if len(match.groups()) == 2 and match.group(1).isdigit():
                    n = int(match.group(1))
                    col_term = match.group(2)
                else:
                    n = 5  # default top N
                    col_term = match.groups()[-1]
                col = self._find_matching_column(col_term)
                if col:
                    return {
                        'action': 'show',
                        'filters': [],
                        'grouping': [col],
                        'aggregation': {'*': 'count'},
                        'sorting': [{'column': 'count', 'ascending': False, 'limit': n}],
                        'limit': n,
                        'time_period': None
                    }
        
        intent = {
            'action': self._extract_action(question_lower),
            'filters': self._extract_filters(question_lower),
            'grouping': self._extract_grouping(question_lower),
            'aggregation': self._extract_aggregation(question_lower),
            'sorting': self._extract_sorting(question_lower),
            'limit': self._extract_limit(question_lower),
            'time_period': self._extract_time_period(question_lower)
        }
        
        return intent
    
    def _extract_action(self, question: str) -> str:
        """Extract the main action from the question using simplified decision tree"""
        question_lower = question.lower().strip()
        
        # Simple decision tree - check in order of priority
        
        # 1. Schema/explanation questions (highest priority)
        if any(word in question_lower for word in ['explain', 'what', 'describe', 'tell me about']):
            if any(word in question_lower for word in ['report', 'table', 'data', 'schema', 'structure', 'columns', 'fields', 'file', 'this', 'does']):
                return 'explain_schema'
        
        # 2. Business analysis questions
        if any(word in question_lower for word in ['overdue', 'past due', 'vendor', 'customer', 'invoice', 'payment', 'business']):
            return 'business_analysis'
        
        # 3. Data exploration questions
        if any(word in question_lower for word in ['show', 'display', 'list', 'find', 'get', 'see', 'view']):
            return 'show'
        
        # 4. Counting questions
        if any(word in question_lower for word in ['count', 'how many', 'number of', 'total records']):
            return 'count'
        
        # 5. Aggregation questions
        if any(word in question for word in ['sum', 'total', 'average', 'mean', 'avg']):
            if 'sum' in question_lower or 'total' in question_lower:
                return 'sum'
            else:
                return 'average'
        
        # 6. Default to schema explanation for short/unclear questions
        if len(question_lower.split()) <= 5:
            return 'explain_schema'
        
        # 7. Default to show for longer questions
        return 'show'
    
    def _extract_filters(self, question: str) -> List[Dict[str, Any]]:
        """Extract filter conditions from the question"""
        filters = []
        
        # Amount filters
        amount_patterns = [
            r'over\s+\$?([\d,]+)',
            r'above\s+\$?([\d,]+)',
            r'greater\s+than\s+\$?([\d,]+)',
            r'under\s+\$?([\d,]+)',
            r'below\s+\$?([\d,]+)',
            r'less\s+than\s+\$?([\d,]+)'
        ]
        
        for pattern in amount_patterns:
            matches = re.findall(pattern, question)
            for match in matches:
                amount = float(match.replace(',', ''))
                if 'over' in pattern or 'above' in pattern or 'greater' in pattern:
                    filters.append({
                        'column': self._find_amount_column(),
                        'operator': '>',
                        'value': amount,
                        'description': f"Amount over ${amount:,.2f}"
                    })
                else:
                    filters.append({
                        'column': self._find_amount_column(),
                        'operator': '<',
                        'value': amount,
                        'description': f"Amount under ${amount:,.2f}"
                    })
        
        # Date filters
        date_patterns = [
            r'last\s+(\d+)\s+(day|week|month|quarter|year)s?',
            r'past\s+(\d+)\s+(day|week|month|quarter|year)s?',
            r'(\d{4})\s*-\s*(\d{4})',  # Year range
            r'q(\d)',  # Quarter
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, question)
            for match in matches:
                if len(match) == 2 and match[1] in ['day', 'week', 'month', 'quarter', 'year']:
                    # Relative date
                    number = int(match[0])
                    unit = match[1]
                    filters.append({
                        'column': self._find_date_column(),
                        'operator': 'relative_date',
                        'value': {'number': number, 'unit': unit},
                        'description': f"Last {number} {unit}s"
                    })
                elif len(match) == 2 and match[0].isdigit() and match[1].isdigit():
                    # Year range
                    start_year = int(match[0])
                    end_year = int(match[1])
                    filters.append({
                        'column': self._find_date_column(),
                        'operator': 'year_range',
                        'value': {'start': start_year, 'end': end_year},
                        'description': f"Years {start_year}-{end_year}"
                    })
                elif len(match) == 1 and match[0].isdigit():
                    # Quarter
                    quarter = int(match[0])
                    filters.append({
                        'column': self._find_date_column(),
                        'operator': 'quarter',
                        'value': quarter,
                        'description': f"Q{quarter}"
                    })
        
        # Document type filters
        doc_type_patterns = [
            r'(invoice|payment|credit|debit|journal)',
            r'document\s+type\s+([a-z0-9]+)',
            r'blart\s*[=:]\s*([a-z0-9]+)'
        ]
        
        for pattern in doc_type_patterns:
            matches = re.findall(pattern, question)
            for match in matches:
                doc_type = match.upper()
                filters.append({
                    'column': self._find_document_type_column(),
                    'operator': '==',
                    'value': doc_type,
                    'description': f"Document type: {doc_type}"
                })
        
        # Status filters
        if 'overdue' in question or 'past due' in question:
            filters.append({
                'column': self._find_date_column(),
                'operator': 'overdue',
                'value': None,
                'description': "Overdue items"
            })
        
        return filters
    
    def _extract_grouping(self, question: str) -> List[str]:
        """Extract grouping columns from the question"""
        grouping = []
        
        # Common grouping patterns
        grouping_patterns = [
            r'by\s+(\w+)',
            r'group\s+by\s+(\w+)',
            r'per\s+(\w+)',
            r'for\s+each\s+(\w+)'
        ]
        
        for pattern in grouping_patterns:
            matches = re.findall(pattern, question)
            for match in matches:
                column = self._find_matching_column(match)
                if column:
                    grouping.append(column)
        
        # Specific SAP groupings
        if 'vendor' in question or 'supplier' in question:
            vendor_col = self._find_vendor_column()
            if vendor_col:
                grouping.append(vendor_col)
        
        if 'customer' in question or 'client' in question:
            customer_col = self._find_customer_column()
            if customer_col:
                grouping.append(customer_col)
        
        if 'account' in question:
            account_col = self._find_account_column()
            if account_col:
                grouping.append(account_col)
        
        if 'cost center' in question:
            cost_center_col = self._find_cost_center_column()
            if cost_center_col:
                grouping.append(cost_center_col)
        
        return list(set(grouping))  # Remove duplicates
    
    def _extract_aggregation(self, question: str) -> Dict[str, str]:
        """Extract aggregation functions from the question"""
        aggregation = {}
        
        if any(word in question for word in ['sum', 'total', 'summarize']):
            amount_col = self._find_amount_column()
            if amount_col:
                aggregation[amount_col] = 'sum'
        
        if any(word in question for word in ['count', 'how many']):
            # Count all rows
            aggregation['*'] = 'count'
        
        if any(word in question for word in ['average', 'mean', 'avg']):
            amount_col = self._find_amount_column()
            if amount_col:
                aggregation[amount_col] = 'mean'
        
        return aggregation
    
    def _extract_sorting(self, question: str) -> List[Dict[str, Any]]:
        """Extract sorting criteria from the question"""
        sorting = []
        
        if 'top' in question or 'highest' in question:
            # Extract number for top N
            top_match = re.search(r'top\s+(\d+)', question)
            if top_match:
                n = int(top_match.group(1))
                amount_col = self._find_amount_column()
                if amount_col:
                    sorting.append({
                        'column': amount_col,
                        'ascending': False,
                        'limit': n
                    })
        
        if 'lowest' in question or 'bottom' in question:
            amount_col = self._find_amount_column()
            if amount_col:
                sorting.append({
                    'column': amount_col,
                    'ascending': True
                })
        
        return sorting
    
    def _extract_limit(self, question: str) -> Optional[int]:
        """Extract limit from the question"""
        limit_patterns = [
            r'first\s+(\d+)',
            r'last\s+(\d+)',
            r'limit\s+(\d+)',
            r'only\s+(\d+)'
        ]
        
        for pattern in limit_patterns:
            match = re.search(pattern, question)
            if match:
                return int(match.group(1))
        
        return None
    
    def _extract_time_period(self, question: str) -> Optional[Dict[str, Any]]:
        """Extract time period from the question"""
        if 'q1' in question or 'first quarter' in question:
            return {'quarter': 1}
        elif 'q2' in question or 'second quarter' in question:
            return {'quarter': 2}
        elif 'q3' in question or 'third quarter' in question:
            return {'quarter': 3}
        elif 'q4' in question or 'fourth quarter' in question:
            return {'quarter': 4}
        
        return None
    
    def _find_matching_column(self, search_term: str) -> Optional[str]:
        """Find a column that matches the search term, including SAP synonyms and plural forms, robustly"""
        search_term_norm = search_term.lower().replace('_', '').replace(' ', '')
        # SAP synonym mapping
        sap_synonyms = {
            'transactioncode': 'TCODE',
            'transactioncodes': 'TCODE',
            'doc type': 'BLART',
            'documenttype': 'BLART',
            'documenttypes': 'BLART',
            'user': 'USNAM',
            'users': 'USNAM',
            'companycode': 'BUKRS',
            'postingdate': 'BUDAT',
            'amount': 'DMBTR',
            'vendor': 'LIFNR',
            'customer': 'KUNNR',
            'costcenter': 'KOSTL',
            'costcenters': 'KOSTL',
        }
        if search_term_norm in sap_synonyms:
            return sap_synonyms[search_term_norm]
        # Try to match by normalized substring (case/whitespace/underscore-insensitive)
        for col_name in self.column_analysis.keys():
            col_name_norm = col_name.lower().replace('_', '').replace(' ', '')
            if search_term_norm == col_name_norm or search_term_norm in col_name_norm or col_name_norm in search_term_norm:
                return col_name
        # Try to match by SAP pattern
        for col_name, col_info in self.column_analysis.items():
            if any(pattern in col_info.get('sap_patterns', []) for pattern in [search_term_norm]):
                return col_name
        # Fallback: substring match for all columns
        for col_name in self.column_analysis.keys():
            if search_term_norm in col_name.lower():
                return col_name
        return None
    
    def _find_amount_column(self) -> Optional[str]:
        """Find the amount column"""
        for col_name, col_info in self.column_analysis.items():
            if 'local_amount' in col_info.get('sap_patterns', []) or 'document_amount' in col_info.get('sap_patterns', []):
                return col_name
        return None
    
    def _find_date_column(self) -> Optional[str]:
        """Find the date column"""
        for col_name, col_info in self.column_analysis.items():
            if 'posting_date' in col_info.get('sap_patterns', []):
                return col_name
        return None
    
    def _find_document_type_column(self) -> Optional[str]:
        """Find the document type column"""
        for col_name, col_info in self.column_analysis.items():
            if 'document_type' in col_info.get('sap_patterns', []):
                return col_name
        return None
    
    def _find_vendor_column(self) -> Optional[str]:
        """Find the vendor column"""
        for col_name, col_info in self.column_analysis.items():
            if 'vendor_number' in col_info.get('sap_patterns', []):
                return col_name
        return None
    
    def _find_customer_column(self) -> Optional[str]:
        """Find the customer column"""
        for col_name, col_info in self.column_analysis.items():
            if 'customer_number' in col_info.get('sap_patterns', []):
                return col_name
        return None
    
    def _find_account_column(self) -> Optional[str]:
        """Find the account column"""
        for col_name, col_info in self.column_analysis.items():
            if 'gl_account' in col_info.get('sap_patterns', []):
                return col_name
        return None
    
    def _find_cost_center_column(self) -> Optional[str]:
        """Find the cost center column"""
        for col_name, col_info in self.column_analysis.items():
            if 'cost_center' in col_name.lower():
                return col_name
        return None
    
    def _generate_query_plan(self, intent: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a query plan based on parsed intent"""
        # Handle schema explanation requests
        if intent['action'] == 'explain_schema':
            return {
                'action': 'explain_schema',
                'filters': [],
                'grouping': [],
                'aggregation': {},
                'sorting': [],
                'limit': None,
                'time_period': None,
                'schema_info': {
                    'table_type': self.sap_table_type,
                    'column_analysis': self.column_analysis,
                    'file_info': self.schema_analysis.get('file_info', {}),
                    'schema_summary': self.schema_analysis.get('schema_summary', ''),
                    'report_identification': self.schema_analysis.get('report_identification', {})
                }
            }
        
        # Handle regular data queries
        plan = {
            'filters': intent['filters'],
            'grouping': intent['grouping'],
            'aggregation': intent['aggregation'],
            'sorting': intent['sorting'],
            'limit': intent['limit'],
            'time_period': intent['time_period'],
            'action': intent['action']
        }
        
        return plan
    
    def _validate_query_plan(self, query_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the query plan and identify ambiguities"""
        issues = []
        clarification_questions = []
        
        # Check if required columns exist
        for filter_cond in query_plan.get('filters', []):
            if not filter_cond.get('column'):
                issues.append(f"Could not identify column for filter: {filter_cond.get('description', 'Unknown')}")
                clarification_questions.append(f"Which column should I use for {filter_cond.get('description', 'this filter')}?")
        
        for group_col in query_plan.get('grouping', []):
            if not group_col:
                issues.append("Could not identify grouping column")
                clarification_questions.append("Which column should I group by?")
        
        # Check for missing time context
        if query_plan.get('filters') and not any('date' in str(f) for f in query_plan['filters']):
            if self._find_date_column():
                clarification_questions.append("What time period are you interested in? (e.g., last 30 days, Q2 2024)")
        
        return {
            'is_valid': len(issues) == 0,
            'message': '; '.join(issues) if issues else 'Query plan is valid',
            'clarification_questions': clarification_questions
        }
    
    def _generate_execution_steps(self, query_plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate step-by-step execution plan"""
        steps = []
        
        # Step 1: Load and filter data
        if query_plan.get('filters'):
            steps.append({
                'step': 1,
                'action': 'filter',
                'description': f"Apply {len(query_plan['filters'])} filter(s)",
                'details': query_plan['filters']
            })
        
        # Step 2: Group data
        if query_plan.get('grouping'):
            steps.append({
                'step': 2,
                'action': 'group',
                'description': f"Group by {', '.join(query_plan['grouping'])}",
                'details': query_plan['grouping']
            })
        
        # Step 3: Aggregate data
        if query_plan.get('aggregation'):
            steps.append({
                'step': 3,
                'action': 'aggregate',
                'description': f"Calculate {', '.join(query_plan['aggregation'].values())}",
                'details': query_plan['aggregation']
            })
        
        # Step 4: Sort data
        if query_plan.get('sorting'):
            steps.append({
                'step': 4,
                'action': 'sort',
                'description': f"Sort by {', '.join([s['column'] for s in query_plan['sorting']])}",
                'details': query_plan['sorting']
            })
        
        # Step 5: Limit results
        if query_plan.get('limit'):
            steps.append({
                'step': 5,
                'action': 'limit',
                'description': f"Limit to {query_plan['limit']} results",
                'details': query_plan['limit']
            })
        
        return steps
    
    def _estimate_result_size(self, query_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Estimate the size of the query result"""
        base_size = self.schema_analysis['file_info']['total_rows']
        
        # Estimate filter reduction
        filter_reduction = 1.0
        for filter_cond in query_plan.get('filters', []):
            if filter_cond.get('operator') == '>':
                filter_reduction *= 0.3  # Assume 30% of data above threshold
            elif filter_cond.get('operator') == '<':
                filter_reduction *= 0.3  # Assume 30% of data below threshold
            elif filter_cond.get('operator') == '==':
                filter_reduction *= 0.1  # Assume 10% for exact match
        
        estimated_rows = int(base_size * filter_reduction)
        
        return {
            'estimated_rows': estimated_rows,
            'estimated_mb': round(estimated_rows * 0.001, 2),  # Rough estimate
            'confidence': 'medium'
        }
    
    def _explain_query_plan(self, query_plan: Dict[str, Any]) -> str:
        """Generate a human-readable explanation of the query plan"""
        explanation_parts = []
        
        if query_plan.get('filters'):
            filter_descriptions = [f['description'] for f in query_plan['filters']]
            explanation_parts.append(f"Filter by: {', '.join(filter_descriptions)}")
        
        if query_plan.get('grouping'):
            explanation_parts.append(f"Group by: {', '.join(query_plan['grouping'])}")
        
        if query_plan.get('aggregation'):
            agg_descriptions = [f"{func}({col})" for col, func in query_plan['aggregation'].items()]
            explanation_parts.append(f"Calculate: {', '.join(agg_descriptions)}")
        
        if query_plan.get('sorting'):
            sort_descriptions = []
            for sort in query_plan['sorting']:
                direction = "descending" if not sort.get('ascending', True) else "ascending"
                sort_descriptions.append(f"{sort['column']} ({direction})")
            explanation_parts.append(f"Sort by: {', '.join(sort_descriptions)}")
        
        if query_plan.get('limit'):
            explanation_parts.append(f"Limit to {query_plan['limit']} results")
        
        return "; ".join(explanation_parts) if explanation_parts else "Show all data"
    
    def _get_suggested_queries(self, intent: Dict[str, Any]) -> List[str]:
        """Get suggested queries based on the intent"""
        suggestions = []
        
        # Get base suggestions from schema analysis
        base_suggestions = self.schema_analysis.get('query_suggestions', [])
        suggestions.extend(base_suggestions[:3])
        
        # Add intent-specific suggestions
        if intent.get('action') == 'count':
            suggestions.append("Show total number of records")
        
        if intent.get('filters'):
            suggestions.append("Show filtered data summary")
        
        return suggestions 