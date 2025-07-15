"""
SAP AI Demo - Logging Configuration
Handles logging and debugging for demo traceability
"""

import logging
import os
from datetime import datetime
import json
import numpy as np
import pandas as pd

class SAPDemoLogger:
    def __init__(self, log_dir="logs"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)
        self.setup_logging()
    
    def safe_json_serialize(self, obj):
        """Safely serialize objects for JSON, handling numpy types and other non-serializable objects"""
        if isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, pd.Series):
            return obj.tolist()
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict('records')
        elif isinstance(obj, dict):
            return {key: self.safe_json_serialize(value) for key, value in obj.items()}
        elif isinstance(obj, list):
            return [self.safe_json_serialize(item) for item in obj]
        elif pd.isna(obj):
            return None
        else:
            return str(obj)
        
    def setup_logging(self):
        """Setup logging configuration"""
        # Create formatters
        detailed_formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        simple_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s'
        )
        
        # Setup file handlers
        # Detailed logs for debugging
        debug_handler = logging.FileHandler(
            os.path.join(self.log_dir, 'sap_demo_debug.log')
        )
        debug_handler.setLevel(logging.DEBUG)
        debug_handler.setFormatter(detailed_formatter)
        
        # User interaction logs
        interaction_handler = logging.FileHandler(
            os.path.join(self.log_dir, 'user_interactions.log')
        )
        interaction_handler.setLevel(logging.INFO)
        interaction_handler.setFormatter(simple_formatter)
        
        # Error logs
        error_handler = logging.FileHandler(
            os.path.join(self.log_dir, 'errors.log')
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        
        # Setup console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(simple_formatter)
        
        # Configure root logger
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(debug_handler)
        root_logger.addHandler(interaction_handler)
        root_logger.addHandler(error_handler)
        root_logger.addHandler(console_handler)
        
        # Create specific loggers
        self.app_logger = logging.getLogger('sap_demo.app')
        self.ai_logger = logging.getLogger('sap_demo.ai')
        self.data_logger = logging.getLogger('sap_demo.data')
        
    def log_user_interaction(self, user_question, ai_response, processing_time=None, data_context=None):
        """Log user interactions for demo traceability"""
        interaction_data = {
            'timestamp': datetime.now().isoformat(),
            'user_question': user_question,
            'ai_response': ai_response,
            'processing_time_seconds': processing_time,
            'data_context': self.safe_json_serialize(data_context)
        }
        
        # Log to file
        self.app_logger.info(f"User Interaction: {json.dumps(interaction_data, indent=2, default=str)}")
        
        # Also save to a structured log file
        with open(os.path.join(self.log_dir, 'interactions.jsonl'), 'a') as f:
            f.write(json.dumps(interaction_data, default=str) + '\n')
    
    def log_ai_request(self, messages, response, model_used="gpt-4"):
        """Log AI API requests and responses"""
        ai_data = {
            'timestamp': datetime.now().isoformat(),
            'model': model_used,
            'messages_count': len(messages),
            'response_length': len(response) if response else 0,
            'system_prompt': next((msg['content'] for msg in messages if msg['role'] == 'system'), ''),
            'user_message': next((msg['content'] for msg in messages if msg['role'] == 'user'), '')
        }
        
        self.ai_logger.info(f"AI Request: {json.dumps(ai_data, indent=2, default=str)}")
    
    def log_data_operation(self, operation, table_name, record_count, filters=None):
        """Log data operations for debugging"""
        data_data = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'table': table_name,
            'record_count': record_count,
            'filters': self.safe_json_serialize(filters)
        }
        
        self.data_logger.info(f"Data Operation: {json.dumps(data_data, indent=2, default=str)}")
    
    def log_error(self, error_type, error_message, context=None):
        """Log errors with context"""
        error_data = {
            'timestamp': datetime.now().isoformat(),
            'error_type': error_type,
            'error_message': str(error_message),
            'context': self.safe_json_serialize(context)
        }
        
        self.app_logger.error(f"Error: {json.dumps(error_data, indent=2, default=str)}")
    
    def get_recent_interactions(self, limit=10):
        """Get recent user interactions for demo purposes"""
        interactions = []
        interaction_file = os.path.join(self.log_dir, 'interactions.jsonl')
        
        if os.path.exists(interaction_file):
            with open(interaction_file, 'r') as f:
                lines = f.readlines()
                for line in lines[-limit:]:
                    try:
                        interactions.append(json.loads(line.strip()))
                    except json.JSONDecodeError:
                        continue
        
        return interactions
    
    def get_demo_stats(self):
        """Get demo statistics for monitoring"""
        stats = {
            'total_interactions': 0,
            'total_errors': 0,
            'avg_response_time': 0,
            'most_common_queries': []
        }
        
        # Count interactions
        interaction_file = os.path.join(self.log_dir, 'interactions.jsonl')
        if os.path.exists(interaction_file):
            with open(interaction_file, 'r') as f:
                stats['total_interactions'] = len(f.readlines())
        
        # Count errors
        error_file = os.path.join(self.log_dir, 'errors.log')
        if os.path.exists(error_file):
            with open(error_file, 'r') as f:
                stats['total_errors'] = len([line for line in f if 'ERROR' in line])
        
        return stats 