"""
SAP AI Demo - Query Executor
Safely executes planned queries on uploaded data
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Tuple
import logging
from datetime import datetime, timedelta
import traceback

class SAPQueryExecutor:
    def __init__(self, df: pd.DataFrame, schema_analysis: Dict[str, Any]):
        self.df = df.copy()
        self.schema_analysis = schema_analysis
        self.column_analysis = schema_analysis.get('column_analysis', {})
        self.logger = logging.getLogger(__name__)
        
        # Preprocess the dataframe
        self._preprocess_dataframe()
    
    def _preprocess_dataframe(self):
        """Preprocess the dataframe for better querying"""
        try:
            # Convert date columns
            for col_name, col_info in self.column_analysis.items():
                if col_info.get('data_category') == 'date':
                    try:
                        self.df[col_name] = pd.to_datetime(self.df[col_name], errors='coerce')
                    except:
                        pass
            
            # Convert numeric columns
            for col_name, col_info in self.column_analysis.items():
                if col_info.get('data_category') == 'numeric':
                    try:
                        self.df[col_name] = pd.to_numeric(self.df[col_name], errors='coerce')
                    except:
                        pass
            
            self.logger.info("Dataframe preprocessing completed")
            
        except Exception as e:
            self.logger.error(f"Error preprocessing dataframe: {str(e)}")
    
    def execute_query(self, query_plan: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute a query plan on the dataframe
        
        Args:
            query_plan: The query plan to execute
            
        Returns:
            Dictionary containing execution results
        """
        try:
            start_time = datetime.now()
            
            # Debug: Log DataFrame columns at the start of execution
            self.logger.info(f"[DEBUG] DataFrame columns at execution: {list(self.df.columns)}")
            self.logger.info(f"[DEBUG] Query plan grouping: {query_plan.get('grouping', [])}, aggregation: {query_plan.get('aggregation', {})}")
            
            # Handle schema explanation requests
            if query_plan.get('action') == 'explain_schema':
                return self._execute_schema_explanation(query_plan, start_time)
            
            # Handle business analysis requests
            if query_plan.get('action') == 'business_analysis':
                return self._execute_business_analysis(query_plan, start_time)
            
            # Start with a copy of the dataframe
            result_df = self.df.copy()
            execution_log = []
            
            # Step 1: Apply filters
            if query_plan.get('filters'):
                result_df, filter_log = self._apply_filters(result_df, query_plan['filters'])
                execution_log.extend(filter_log)
            
            # Step 2: Apply time period filters
            if query_plan.get('time_period'):
                result_df, time_log = self._apply_time_period_filter(result_df, query_plan['time_period'])
                execution_log.extend(time_log)
            
            # Step 3: Apply grouping and aggregation
            if query_plan.get('grouping') or query_plan.get('aggregation'):
                result_df, agg_log = self._apply_grouping_aggregation(result_df, query_plan)
                execution_log.extend(agg_log)
            
            # Step 4: Apply sorting
            if query_plan.get('sorting'):
                result_df, sort_log = self._apply_sorting(result_df, query_plan['sorting'])
                execution_log.extend(sort_log)
            
            # Step 5: Apply limit
            if query_plan.get('limit'):
                result_df, limit_log = self._apply_limit(result_df, query_plan['limit'])
                execution_log.extend(limit_log)
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            # Prepare results
            results = self._prepare_results(result_df, query_plan, execution_time, execution_log)

            # --- Global Table Dump Block: Never return the full table as a fallback ---
            is_full_table = len(result_df) > 0 and len(result_df) == len(self.df) and set(result_df.columns) == set(self.df.columns)
            if is_full_table:
                return {
                    'status': 'error',
                    'message': 'Sorry, I am unable to answer your question with the current data. Please try a more specific or different question.',
                    'data': [],
                    'columns': [],
                    'row_count': 0,
                    'execution_time': execution_time,
                    'insights': [],
                    'natural_language_response': 'Sorry, I am unable to answer your question with the current data. Please try a more specific or different question.'
                }
            
            # After getting the result, check if it's just headers or empty
            if len(result_df) == 0 or len(result_df.columns) == 0:
                return {
                    'status': 'error',
                    'message': 'Sorry, I am unable to answer this question with the current data.',
                    'data': [],
                    'columns': [],
                    'row_count': 0,
                    'execution_time': execution_time,
                    'insights': [],
                    'natural_language_response': 'Sorry, I am unable to answer this question with the current data.'
                }
            # Prevent returning results that are just repeated column headers
            if all(result_df.iloc[i].tolist() == result_df.columns.tolist() for i in range(min(len(result_df), 5))):
                return {
                    'status': 'error',
                    'message': 'Sorry, I am unable to answer this question with the current data.',
                    'data': [],
                    'columns': [],
                    'row_count': 0,
                    'execution_time': execution_time,
                    'insights': [],
                    'natural_language_response': 'Sorry, I am unable to answer this question with the current data.'
                }

            self.logger.info(f"Query executed successfully in {execution_time:.2f} seconds")
            return results
            
        except Exception as e:
            self.logger.error(f"Error executing query: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error executing query: {str(e)}",
                'traceback': traceback.format_exc()
            }
    
    def _execute_schema_explanation(self, query_plan: Dict[str, Any], start_time: datetime) -> Dict[str, Any]:
        """Execute schema explanation request with natural language response"""
        try:
            # Use schema_analysis from constructor instead of query_plan
            table_type = self.schema_analysis.get('sap_table_type', 'UNKNOWN')
            column_analysis = self.schema_analysis.get('column_analysis', {})
            file_info = self.schema_analysis.get('file_info', {})
            schema_summary = self.schema_analysis.get('schema_summary', '')
            report_identification = self.schema_analysis.get('report_identification', {})
            original_question = query_plan.get('original_question', '')
            
            # Generate schema explanation
            explanation = self._generate_schema_explanation(
                table_type, column_analysis, file_info, schema_summary, report_identification
            )
            
            # Generate natural language response
            nl_response = self._generate_natural_language_response(original_question, 'schema', {
                'table_type': table_type,
                'total_rows': file_info.get('total_rows', 0),
                'total_columns': file_info.get('total_columns', 0),
                'schema_summary': schema_summary
            })
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return {
                'status': 'success',
                'data': [{'explanation': explanation, 'nl_response': nl_response}],
                'columns': ['explanation', 'nl_response'],
                'row_count': 1,
                'execution_time': execution_time,
                'query_type': 'explain_schema',
                'insights': [nl_response],
                'schema_explanation': explanation,
                'natural_language_response': nl_response
            }
            
        except Exception as e:
            self.logger.error(f"Error executing schema explanation: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error generating schema explanation: {str(e)}"
            }
    
    def _generate_schema_explanation(self, table_type: str, column_analysis: Dict, 
                                   file_info: Dict, schema_summary: str, 
                                   report_identification: Dict) -> str:
        """Generate a simplified but comprehensive schema explanation"""
        parts = []
        
        # Basic info
        parts.append(f"# 📊 {table_type} Table")
        parts.append(f"**Records:** {file_info.get('total_rows', 'N/A')} | **Columns:** {file_info.get('total_columns', 'N/A')}")
        
        # Identification confidence
        if report_identification:
            confidence = report_identification.get('confidence', 0)
            parts.append(f"**Confidence:** {confidence:.1%}")
        
        # Description
        if schema_summary:
            parts.append(f"**Description:** {schema_summary}")
        
        # Key columns table
        if column_analysis:
            parts.append("\n## 📋 Key Columns")
            parts.append("| Column | Type | Purpose |")
            parts.append("|--------|------|---------|")
            
            # Show only the most important columns (first 10)
            important_cols = list(column_analysis.items())[:10]
            for col_name, col_info in important_cols:
                sap_patterns = col_info.get('sap_patterns', [])
                data_type = col_info.get('data_category', 'unknown').title()
                
                if sap_patterns:
                    purpose = ', '.join(sap_patterns).replace('_', ' ').title()
                else:
                    purpose = f"{data_type} data"
                
                parts.append(f"| {col_name} | {data_type} | {purpose} |")
        
        # Data insights
        parts.append("\n## 📈 Data Insights")
        if column_analysis:
            numeric_count = sum(1 for info in column_analysis.values() if info.get('data_category') == 'numeric')
            date_count = sum(1 for info in column_analysis.values() if info.get('data_category') == 'date')
            categorical_count = sum(1 for info in column_analysis.values() if info.get('data_category') == 'categorical')
            
            parts.append(f"- **Numeric columns:** {numeric_count} (for calculations)")
            parts.append(f"- **Date columns:** {date_count} (for time analysis)")
            parts.append(f"- **Categorical columns:** {categorical_count} (for grouping)")
        
        # Business context
        parts.append("\n## 💼 Business Context")
        if table_type == 'BSEG':
            parts.append("- **Purpose:** Accounting line items")
            parts.append("- **Use:** Financial analysis, transaction tracking")
            parts.append("- **Key queries:** Show by vendor, analyze payments, find overdue")
        elif table_type == 'BKPF':
            parts.append("- **Purpose:** Accounting document headers")
            parts.append("- **Use:** Document analysis, approval workflows")
            parts.append("- **Key queries:** Show by type, analyze posting patterns")
        else:
            parts.append("- **Purpose:** General SAP data")
            parts.append("- **Use:** Data analysis, reporting")
            parts.append("- **Key queries:** Explore patterns, generate reports")
        
        # Query suggestions
        parts.append("\n## 🔍 Try asking:")
        parts.append("- \"Show me all records\"")
        parts.append("- \"Count total transactions\"")
        parts.append("- \"Find overdue invoices\"")
        parts.append("- \"Show vendor payments\"")
        
        return "\n".join(parts)
    
    def _execute_business_analysis(self, query_plan: Dict[str, Any], start_time: datetime) -> Dict[str, Any]:
        """Execute business analysis request with natural language response"""
        try:
            question = query_plan.get('original_question', '')
            question_lower = question.lower()
            
            # Generate business insights
            insights = self._generate_business_insights(question_lower)
            
            # Generate natural language response
            nl_response = self._generate_natural_language_response(question, 'business', {
                'insights': insights,
                'question': question
            })
            
            execution_time = (datetime.now() - start_time).total_seconds()
            
            return {
                'status': 'success',
                'data': [{'analysis': insights, 'nl_response': nl_response}],
                'columns': ['analysis', 'nl_response'],
                'row_count': 1,
                'execution_time': execution_time,
                'query_type': 'business_analysis',
                'insights': [nl_response],
                'business_analysis': insights,
                'natural_language_response': nl_response
            }
            
        except Exception as e:
            self.logger.error(f"Error executing business analysis: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error generating business analysis: {str(e)}"
            }
    
    def _generate_business_insights(self, question: str) -> str:
        """Generate simplified business insights"""
        insights = []
        
        # Simple keyword-based analysis
        if 'overdue' in question:
            insights.append(self._analyze_overdue_items())
        
        if 'vendor' in question:
            insights.append(self._analyze_vendor_data())
        
        if 'customer' in question:
            insights.append(self._analyze_customer_data())
        
        if 'invoice' in question or 'payment' in question:
            insights.append(self._analyze_financial_data())
        
        # If no specific analysis, provide general insights
        if not insights:
            insights.append(self._generate_general_insights())
        
        return "\n\n".join(insights)
    
    def _analyze_overdue_items(self) -> str:
        """Simple overdue analysis"""
        try:
            date_col = self._find_date_column()
            if date_col and date_col in self.df.columns:
                overdue_count = (pd.to_datetime(self.df[date_col], errors='coerce') < datetime.now()).sum()
                total_count = len(self.df)
                return f"**Overdue Analysis:** {overdue_count}/{total_count} items overdue ({overdue_count/total_count*100:.1f}%)"
            return "**Overdue Analysis:** No date column found"
        except:
            return "**Overdue Analysis:** Unable to analyze"
    
    def _analyze_vendor_data(self) -> str:
        """Simple vendor analysis"""
        try:
            vendor_col = self._find_vendor_column()
            if vendor_col and vendor_col in self.df.columns:
                vendor_count = self.df[vendor_col].nunique()
                return f"**Vendor Analysis:** {vendor_count} unique vendors found"
            return "**Vendor Analysis:** No vendor data found"
        except:
            return "**Vendor Analysis:** Unable to analyze"
    
    def _analyze_customer_data(self) -> str:
        """Simple customer analysis"""
        try:
            customer_col = self._find_customer_column()
            if customer_col and customer_col in self.df.columns:
                customer_count = self.df[customer_col].nunique()
                return f"**Customer Analysis:** {customer_count} unique customers found"
            return "**Customer Analysis:** No customer data found"
        except:
            return "**Customer Analysis:** Unable to analyze"
    
    def _analyze_financial_data(self) -> str:
        """Simple financial analysis"""
        try:
            amount_col = self._find_amount_column()
            if amount_col and amount_col in self.df.columns:
                total = self.df[amount_col].sum()
                avg = self.df[amount_col].mean()
                return f"**Financial Analysis:** Total: ${total:,.2f}, Average: ${avg:,.2f}"
            return "**Financial Analysis:** No amount data found"
        except:
            return "**Financial Analysis:** Unable to analyze"
    
    def _generate_general_insights(self) -> str:
        """Generate general insights"""
        total_records = len(self.df)
        table_type = self.schema_analysis.get('sap_table_type', 'UNKNOWN')
        
        insights = f"**General Insights:**\n"
        insights += f"- {total_records} records in {table_type} table\n"
        
        if table_type == 'BSEG':
            insights += "- Use for financial transaction analysis\n"
            insights += "- Try: 'Show vendor payments' or 'Find overdue invoices'"
        elif table_type == 'BKPF':
            insights += "- Use for document-level analysis\n"
            insights += "- Try: 'Show documents by type' or 'Analyze posting patterns'"
        else:
            insights += "- Use for general data exploration\n"
            insights += "- Try: 'Show all records' or 'Count transactions'"
        
        return insights
    
    def _apply_filters(self, df: pd.DataFrame, filters: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Apply filters to the dataframe"""
        result_df = df.copy()
        log_entries = []
        
        for i, filter_cond in enumerate(filters):
            try:
                column = filter_cond['column']
                operator = filter_cond['operator']
                value = filter_cond['value']
                
                if column not in result_df.columns:
                    log_entries.append({
                        'step': f'filter_{i+1}',
                        'status': 'error',
                        'message': f"Column '{column}' not found"
                    })
                    continue
                
                # Apply filter based on operator
                if operator == '>':
                    mask = result_df[column] > value
                elif operator == '<':
                    mask = result_df[column] < value
                elif operator == '>=':
                    mask = result_df[column] >= value
                elif operator == '<=':
                    mask = result_df[column] <= value
                elif operator == '==':
                    mask = result_df[column] == value
                elif operator == '!=':
                    mask = result_df[column] != value
                elif operator == 'in':
                    mask = result_df[column].isin(value)
                elif operator == 'contains':
                    mask = result_df[column].str.contains(value, case=False, na=False)
                elif operator == 'overdue':
                    mask = self._apply_overdue_filter(result_df, column)
                elif operator == 'relative_date':
                    mask = self._apply_relative_date_filter(result_df, column, value)
                elif operator == 'year_range':
                    mask = self._apply_year_range_filter(result_df, column, value)
                elif operator == 'quarter':
                    mask = self._apply_quarter_filter(result_df, column, value)
                else:
                    log_entries.append({
                        'step': f'filter_{i+1}',
                        'status': 'error',
                        'message': f"Unknown operator: {operator}"
                    })
                    continue
                
                # Apply the filter
                initial_count = len(result_df)
                result_df = result_df[mask]
                final_count = len(result_df)
                
                log_entries.append({
                    'step': f'filter_{i+1}',
                    'status': 'success',
                    'message': f"Applied {filter_cond['description']}",
                    'details': {
                        'column': column,
                        'operator': operator,
                        'value': value,
                        'rows_before': initial_count,
                        'rows_after': final_count,
                        'rows_filtered': initial_count - final_count
                    }
                })
                
            except Exception as e:
                log_entries.append({
                    'step': f'filter_{i+1}',
                    'status': 'error',
                    'message': f"Error applying filter: {str(e)}"
                })
        
        return result_df, log_entries
    
    def _apply_overdue_filter(self, df: pd.DataFrame, date_column: str) -> pd.Series:
        """Apply overdue filter (items past due date)"""
        today = datetime.now()
        return df[date_column] < today
    
    def _apply_relative_date_filter(self, df: pd.DataFrame, date_column: str, value: Dict[str, Any]) -> pd.Series:
        """Apply relative date filter (e.g., last 30 days)"""
        number = value['number']
        unit = value['unit']
        
        if unit == 'day':
            cutoff_date = datetime.now() - timedelta(days=number)
        elif unit == 'week':
            cutoff_date = datetime.now() - timedelta(weeks=number)
        elif unit == 'month':
            cutoff_date = datetime.now() - timedelta(days=number*30)
        elif unit == 'quarter':
            cutoff_date = datetime.now() - timedelta(days=number*90)
        elif unit == 'year':
            cutoff_date = datetime.now() - timedelta(days=number*365)
        else:
            cutoff_date = datetime.now() - timedelta(days=number)
        
        return df[date_column] >= cutoff_date
    
    def _apply_year_range_filter(self, df: pd.DataFrame, date_column: str, value: Dict[str, Any]) -> pd.Series:
        """Apply year range filter"""
        start_year = value['start']
        end_year = value['end']
        
        return (df[date_column].dt.year >= start_year) & (df[date_column].dt.year <= end_year)
    
    def _apply_quarter_filter(self, df: pd.DataFrame, date_column: str, quarter: int) -> pd.Series:
        """Apply quarter filter"""
        return df[date_column].dt.quarter == quarter
    
    def _apply_time_period_filter(self, df: pd.DataFrame, time_period: Dict[str, Any]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Apply time period filter"""
        result_df = df.copy()
        log_entries = []
        
        if 'quarter' in time_period:
            quarter = time_period['quarter']
            date_column = self._find_date_column()
            
            if date_column:
                mask = result_df[date_column].dt.quarter == quarter
                initial_count = len(result_df)
                result_df = result_df[mask]
                final_count = len(result_df)
                
                log_entries.append({
                    'step': 'time_period_filter',
                    'status': 'success',
                    'message': f"Filtered to Q{quarter}",
                    'details': {
                        'quarter': quarter,
                        'rows_before': initial_count,
                        'rows_after': final_count,
                        'rows_filtered': initial_count - final_count
                    }
                })
        
        return result_df, log_entries
    
    def _apply_grouping_aggregation(self, df: pd.DataFrame, query_plan: Dict[str, Any]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Apply grouping and aggregation"""
        result_df = df.copy()
        log_entries = []

        grouping = query_plan.get('grouping', [])
        aggregation = query_plan.get('aggregation', {})
        sorting = query_plan.get('sorting', [])
        limit = query_plan.get('limit', None)

        # Debug: Log columns and grouping/aggregation before applying
        self.logger.info(f"[DEBUG] _apply_grouping_aggregation: DataFrame columns: {list(result_df.columns)}")
        self.logger.info(f"[DEBUG] _apply_grouping_aggregation: grouping={grouping}, aggregation={aggregation}")

        # Patch: If grouping by a single column with count aggregation, do value_counts
        if len(grouping) == 1 and aggregation and '*' in aggregation and aggregation['*'] == 'count':
            col = grouping[0]
            if col in result_df.columns:
                vc = result_df[col].value_counts().reset_index()
                vc.columns = [col, 'count']
                # Apply sorting and limit if present
                if limit:
                    vc = vc.head(limit)
                result_df = vc
                log_entries.append({
                    'step': 'grouping_aggregation',
                    'status': 'success',
                    'message': f"Top {limit or 5} most frequent values for {col}",
                    'details': {
                        'grouping_column': col,
                        'result_rows': len(result_df)
                    }
                })
                return result_df, log_entries
        
        try:
            if grouping and aggregation:
                # Group by specified columns and apply aggregations
                agg_dict = {}
                for col, func in aggregation.items():
                    if col == '*':
                        # Count all rows
                        agg_dict['count'] = 'count'
                    else:
                        agg_dict[col] = func
                
                result_df = result_df.groupby(grouping).agg(agg_dict).reset_index()
                
                log_entries.append({
                    'step': 'grouping_aggregation',
                    'status': 'success',
                    'message': f"Grouped by {', '.join(grouping)} and calculated {', '.join(aggregation.values())}",
                    'details': {
                        'grouping_columns': grouping,
                        'aggregation_functions': aggregation,
                        'result_rows': len(result_df)
                    }
                })
            
            elif aggregation and not grouping:
                # Apply aggregations without grouping
                agg_dict = {}
                for col, func in aggregation.items():
                    if col == '*':
                        agg_dict['count'] = 'count'
                    else:
                        agg_dict[col] = func
                
                result_df = result_df.agg(agg_dict).to_frame().T
                
                log_entries.append({
                    'step': 'aggregation',
                    'status': 'success',
                    'message': f"Calculated {', '.join(aggregation.values())}",
                    'details': {
                        'aggregation_functions': aggregation,
                        'result_rows': len(result_df)
                    }
                })
            
            elif grouping and not aggregation:
                # Just group by (count by default)
                result_df = result_df.groupby(grouping).size().reset_index(name='count')
                
                log_entries.append({
                    'step': 'grouping',
                    'status': 'success',
                    'message': f"Grouped by {', '.join(grouping)}",
                    'details': {
                        'grouping_columns': grouping,
                        'result_rows': len(result_df)
                    }
                })
        
        except Exception as e:
            log_entries.append({
                'step': 'grouping_aggregation',
                'status': 'error',
                'message': f"Error in grouping/aggregation: {str(e)}"
            })
        
        return result_df, log_entries
    
    def _apply_sorting(self, df: pd.DataFrame, sorting: List[Dict[str, Any]]) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Apply sorting to the dataframe"""
        result_df = df.copy()
        log_entries = []
        
        try:
            sort_columns = []
            ascending_flags = []
            
            for sort_cond in sorting:
                column = sort_cond['column']
                ascending = sort_cond.get('ascending', True)
                
                if column in result_df.columns:
                    sort_columns.append(column)
                    ascending_flags.append(ascending)
            
            if sort_columns:
                result_df = result_df.sort_values(sort_columns, ascending=ascending_flags)
                
                log_entries.append({
                    'step': 'sorting',
                    'status': 'success',
                    'message': f"Sorted by {', '.join(sort_columns)}",
                    'details': {
                        'sort_columns': sort_columns,
                        'ascending': ascending_flags
                    }
                })
        
        except Exception as e:
            log_entries.append({
                'step': 'sorting',
                'status': 'error',
                'message': f"Error in sorting: {str(e)}"
            })
        
        return result_df, log_entries
    
    def _apply_limit(self, df: pd.DataFrame, limit: int) -> Tuple[pd.DataFrame, List[Dict[str, Any]]]:
        """Apply limit to the dataframe"""
        result_df = df.head(limit)
        log_entries = []
        
        log_entries.append({
            'step': 'limit',
            'status': 'success',
            'message': f"Limited to {limit} results",
            'details': {
                'limit': limit,
                'rows_returned': len(result_df)
            }
        })
        
        return result_df, log_entries
    
    def _prepare_results(self, result_df: pd.DataFrame, query_plan: Dict[str, Any], 
                        execution_time: float, execution_log: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Prepare the final results"""
        try:
            # Convert dataframe to records for JSON serialization
            if len(result_df) > 0:
                # Return as list of lists (row values) for frontend compatibility
                result_records = []
                for _, row in result_df.iterrows():
                    record = []
                    for col in result_df.columns:
                        value = row[col]
                        if pd.isna(value):
                            record.append(None)
                        elif isinstance(value, (datetime, pd.Timestamp)):
                            record.append(value.isoformat())
                        elif isinstance(value, (np.integer, np.floating)):
                            record.append(float(value))
                        else:
                            record.append(str(value))
                    result_records.append(record)
            else:
                result_records = []
            
            # Generate summary statistics
            summary_stats = self._generate_summary_stats(result_df, query_plan)
            
            # Generate insights
            insights = self._generate_insights(result_df, query_plan)
            
            return {
                'status': 'success',
                'data': result_records,
                'columns': list(result_df.columns),
                'row_count': len(result_df),
                'execution_time': execution_time,
                'execution_log': execution_log,
                'summary_stats': summary_stats,
                'insights': insights,
                'query_plan': query_plan
            }
            
        except Exception as e:
            self.logger.error(f"Error preparing results: {str(e)}")
            return {
                'status': 'error',
                'message': f"Error preparing results: {str(e)}"
            }
    
    def _generate_summary_stats(self, df: pd.DataFrame, query_plan: Dict[str, Any]) -> Dict[str, Any]:
        """Generate summary statistics for the results"""
        stats = {
            'total_rows': len(df),
            'total_columns': len(df.columns)
        }
        
        if len(df) == 0:
            return stats
        
        # Add column-specific statistics
        column_stats = {}
        for col in df.columns:
            col_info = self.column_analysis.get(col, {})
            
            if col_info.get('data_category') == 'numeric':
                try:
                    numeric_col = pd.to_numeric(df[col], errors='coerce')
                    column_stats[col] = {
                        'min': float(numeric_col.min()),
                        'max': float(numeric_col.max()),
                        'mean': float(numeric_col.mean()),
                        'sum': float(numeric_col.sum())
                    }
                except:
                    pass
            elif col_info.get('data_category') == 'date':
                try:
                    date_col = pd.to_datetime(df[col], errors='coerce')
                    column_stats[col] = {
                        'min_date': date_col.min().isoformat(),
                        'max_date': date_col.max().isoformat(),
                        'date_range_days': (date_col.max() - date_col.min()).days
                    }
                except:
                    pass
            elif col_info.get('data_category') == 'categorical':
                try:
                    value_counts = df[col].value_counts()
                    # Convert numpy types to native Python types for JSON serialization
                    top_values = {}
                    for key, value in value_counts.head(5).items():
                        if pd.isna(key):
                            top_values['null'] = int(value)
                        elif isinstance(key, (np.integer, np.floating)):
                            top_values[str(float(key))] = int(value)
                        else:
                            top_values[str(key)] = int(value)
                    # Fix: ensure unique_values is an int, not a dict
                    column_stats[col] = {
                        'unique_values': int(value_counts.nunique()),
                        'top_values': top_values
                    }
                except Exception as e:
                    pass
        
        # Defensive fix: ensure column_stats is a dict before assignment
        if not isinstance(column_stats, dict):
            column_stats = {}
        stats['column_stats'] = column_stats
        return stats
    
    def _generate_insights(self, df: pd.DataFrame, query_plan: Dict[str, Any]) -> List[str]:
        """Generate insights about the results"""
        insights = []
        
        if len(df) == 0:
            insights.append("No data matches the specified criteria")
            return insights
        
        # Add insights based on query type
        if query_plan.get('action') == 'count':
            insights.append(f"Found {len(df)} records matching the criteria")
        
        if query_plan.get('aggregation'):
            for col, func in query_plan['aggregation'].items():
                if col in df.columns and func in ['sum', 'mean']:
                    try:
                        value = df[col].iloc[0] if len(df) == 1 else df[col].sum()
                        insights.append(f"Total {func} of {col}: {value:,.2f}")
                    except:
                        pass
        
        # Add insights based on data patterns
        for col in df.columns:
            col_info = self.column_analysis.get(col, {})
            
            if col_info.get('data_category') == 'numeric':
                try:
                    numeric_col = pd.to_numeric(df[col], errors='coerce')
                    if numeric_col.max() > numeric_col.mean() * 3:
                        insights.append(f"High variance detected in {col} - some values are significantly above average")
                except:
                    pass
        
        return insights
    
    def _find_date_column(self) -> Optional[str]:
        """Find the date column"""
        for col_name, col_info in self.column_analysis.items():
            if 'posting_date' in col_info.get('sap_patterns', []):
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
    
    def _find_amount_column(self) -> Optional[str]:
        """Find the amount column"""
        for col_name, col_info in self.column_analysis.items():
            if any(pattern in col_info.get('sap_patterns', []) for pattern in ['local_amount', 'document_amount']):
                return col_name
        return None 

    def _generate_natural_language_response(self, question: str, response_type: str, context: Dict) -> str:
        """Generate natural language responses for different question types with Navy/DoD context"""
        try:
            if response_type == 'schema':
                table_type = context.get('table_type', 'UNKNOWN')
                total_rows = context.get('total_rows', 0)
                total_columns = context.get('total_columns', 0)
                schema_summary = context.get('schema_summary', '')
                
                # Generate natural language response based on question type with Navy context
                if 'what does' in question.lower() or 'what is' in question.lower():
                    if 'navy' in question.lower() or 'military' in question.lower() or 'dod' in question.lower():
                        if table_type == 'BSEG':
                            return f"This is a **BSEG (Accounting Document Segment)** table containing {total_rows:,} individual financial transaction line items. As a Navy professional, you can use this data for:\n• Tracking Navy procurement and vendor payments\n• Monitoring budget execution across commands\n• Auditing financial transactions for DoD compliance\n• Analyzing spending patterns by appropriation categories\n• Supporting Navy financial reporting requirements\n\n{schema_summary}"
                        elif table_type == 'BKPF':
                            return f"This is a **BKPF (Accounting Document Header)** table containing {total_rows:,} complete accounting documents. For Navy operations, this supports:\n• Document-level audit trails for DoD compliance\n• Approval workflow tracking for Navy acquisitions\n• Financial reporting for Navy commands and programs\n• Budget execution monitoring and analysis\n• Supporting Navy financial transparency initiatives\n\n{schema_summary}"
                        else:
                            return f"This is a **{table_type}** table with {total_rows:,} records and {total_columns} columns. For Navy and DoD operations, this data supports:\n• Financial analysis and reporting requirements\n• Compliance monitoring and audit preparation\n• Budget execution and appropriation tracking\n• Navy-specific procurement and vendor management\n\n{schema_summary}"
                    else:
                        if table_type == 'BSEG':
                            return f"This is a **BSEG (Accounting Document Segment)** table that contains {total_rows:,} individual line items from accounting documents. Each row represents a single financial transaction entry with {total_columns} different data fields. This table is used for detailed financial analysis, transaction tracking, and audit purposes. {schema_summary}"
                        elif table_type == 'BKPF':
                            return f"This is a **BKPF (Accounting Document Header)** table that contains {total_rows:,} complete accounting documents. Each row represents a full financial document with {total_columns} different data fields. This table is used for document-level analysis, approval workflows, and compliance reporting. {schema_summary}"
                        else:
                            return f"This is a **{table_type}** table containing {total_rows:,} records with {total_columns} columns of data. {schema_summary}"
                
                elif 'explain' in question.lower() or 'describe' in question.lower():
                    if 'navy' in question.lower() or 'military' in question.lower() or 'dod' in question.lower():
                        return f"Let me explain this {table_type} table for Navy operations: It contains {total_rows:,} records with {total_columns} columns. {schema_summary} You can use this data for Navy financial analysis, DoD compliance reporting, budget execution monitoring, and supporting Navy acquisition processes."
                    else:
                        return f"Let me explain this {table_type} table: It contains {total_rows:,} records with {total_columns} columns. {schema_summary} You can use this data for financial analysis, reporting, and business intelligence purposes."
                
                else:
                    return f"This {table_type} table has {total_rows:,} records and {total_columns} columns. {schema_summary}"
            
            elif response_type == 'business':
                insights = context.get('insights', '')
                question_lower = question.lower()
                
                if 'navy' in question_lower or 'military' in question_lower or 'dod' in question_lower:
                    if 'overdue' in question_lower:
                        return f"Based on your Navy-related question about overdue items: {insights} This analysis helps identify items that need attention for Navy payment processing, vendor management, and cash flow management across Navy commands."
                    
                    elif 'vendor' in question_lower:
                        return f"Regarding your Navy vendor-related question: {insights} This information helps you understand Navy vendor relationships, procurement patterns, and supports Navy acquisition compliance."
                    
                    elif 'customer' in question_lower:
                        return f"About your Navy customer inquiry: {insights} This data provides insights into Navy customer relationships, inter-command transactions, and Navy financial patterns."
                    
                    elif 'invoice' in question_lower or 'payment' in question_lower:
                        return f"For your Navy financial question: {insights} This analysis helps understand Navy payment patterns, budget execution, and financial performance across Navy programs."
                    
                    else:
                        return f"Here's what I found based on your Navy-related question: {insights} This information provides valuable insights for Navy decision-making, budget management, and DoD compliance."
                else:
                    if 'overdue' in question_lower:
                        return f"Based on your question about overdue items, here's what I found: {insights} This analysis helps identify items that need attention for payment processing and cash flow management."
                    
                    elif 'vendor' in question_lower:
                        return f"Regarding your vendor-related question: {insights} This information can help you understand vendor relationships and payment patterns."
                    
                    elif 'customer' in question_lower:
                        return f"About your customer inquiry: {insights} This data provides insights into customer relationships and transaction patterns."
                    
                    elif 'invoice' in question_lower or 'payment' in question_lower:
                        return f"For your financial question: {insights} This analysis helps understand payment patterns and financial performance."
                    
                    else:
                        return f"Here's what I found based on your question: {insights} This information provides valuable business insights for decision-making."
            
            else:
                return "I've analyzed your question and provided relevant insights based on the available data."
                
        except Exception as e:
            self.logger.error(f"Error generating natural language response: {str(e)}")
            return "I've analyzed your data and provided insights based on your question." 