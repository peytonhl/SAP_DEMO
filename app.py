# Import Flask for web server, request for form input handling,
# and render_template_string to render a basic inline HTML page
from flask import Flask, request, render_template_string, jsonify, session, send_from_directory
import time
import os
import uuid
from werkzeug.utils import secure_filename

# Import OpenAI SDK for calling GPT-4
import openai

# Import os for accessing environment variables
import os

# Import dotenv loader to securely pull in secrets from a .env file
from dotenv import load_dotenv

# Import our custom modules
from prompt_templates import create_enterprise_query_prompt, ENTERPRISE_EXAMPLE_QUERIES, get_query_suggestions
from schema_analyzer import SAPSchemaAnalyzer
from query_planner import SAPQueryPlanner
from query_executor import SAPQueryExecutor
from logger_config import SAPDemoLogger
from report_identifier import SAPReportIdentifier
from schema_mapper import SAPSchemaMapper
import pandas as pd
import numpy as np
from typing import Dict, Any

# Load environment variables from .env file into the system
load_dotenv()

# Set the OpenAI API key from the environment variable
openai.api_key = os.getenv("OPENAI_API_KEY")

# Initialize the Flask app
app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "your-secret-key-here")

# Add static file serving
@app.route('/static/<path:filename>')
def static_files(filename):
    return send_from_directory('static', filename)

# Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_FILE_SIZE

# Initialize our components
logger = SAPDemoLogger()
schema_analyzer = SAPSchemaAnalyzer()
report_identifier = SAPReportIdentifier()
schema_mapper = SAPSchemaMapper()

# Ensure upload directory exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Define a modern, enterprise-grade HTML template
HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>SAP Assistant - Powered by ITS Consulting Inc.</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            color: #333;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            padding: 20px;
        }
        
        .header {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            padding: 30px;
            border-radius: 15px;
            margin-bottom: 30px;
            text-align: center;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        }
        
        .header h1 {
            color: #2c3e50;
            font-size: 2.5em;
            margin-bottom: 10px;
        }
        
        .header p {
            color: #7f8c8d;
            font-size: 1.1em;
        }
        
        .main-content {
            display: grid;
            grid-template-columns: 1fr 2fr;
            gap: 30px;
            margin-bottom: 30px;
        }
        
        .sidebar {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
            height: fit-content;
        }
        
        .main-panel {
            background: rgba(255, 255, 255, 0.95);
            backdrop-filter: blur(10px);
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
        }
        
        .upload-section {
            margin-bottom: 30px;
        }
        
        .upload-area {
            border: 2px dashed #bdc3c7;
            border-radius: 10px;
            padding: 40px;
            text-align: center;
            transition: all 0.3s ease;
            cursor: pointer;
        }
        
        .upload-area:hover {
            border-color: #3498db;
            background: rgba(52, 152, 219, 0.05);
        }
        
        .upload-area.dragover {
            border-color: #27ae60;
            background: rgba(39, 174, 96, 0.1);
        }
        
        .file-input {
            display: none;
        }
        
        .upload-btn {
            background: linear-gradient(135deg, #3498db, #2980b9);
            color: white;
            padding: 12px 24px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            transition: transform 0.2s;
        }
        
        .upload-btn:hover {
            transform: translateY(-2px);
        }
        
        .schema-info {
            margin-top: 20px;
            padding: 15px;
            background: #f8f9fa;
            border-radius: 8px;
            border-left: 4px solid #3498db;
        }
        
        .schema-info h3 {
            color: #2c3e50;
            margin-bottom: 10px;
        }
        
        .schema-stats {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            margin-top: 15px;
        }
        
        .stat-item {
            text-align: center;
            padding: 10px;
            background: white;
            border-radius: 6px;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }
        
        .stat-number {
            font-size: 1.5em;
            font-weight: bold;
            color: #3498db;
        }
        
        .stat-label {
            font-size: 0.9em;
            color: #7f8c8d;
        }
        
        .query-section {
            margin-bottom: 30px;
        }
        
        .query-form {
            display: flex;
            gap: 15px;
            margin-bottom: 20px;
        }
        
        .query-input {
            flex: 1;
            padding: 15px;
            border: 2px solid #e9ecef;
            border-radius: 8px;
            font-size: 16px;
            transition: border-color 0.3s;
        }
        
        .query-input:focus {
            outline: none;
            border-color: #3498db;
        }
        
        .query-btn {
            background: linear-gradient(135deg, #27ae60, #229954);
            color: white;
            padding: 15px 30px;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            transition: transform 0.2s;
        }
        
        .query-btn:hover {
            transform: translateY(-2px);
        }
        
        .query-btn:disabled {
            background: #bdc3c7;
            cursor: not-allowed;
            transform: none;
        }
        
        .suggestions {
            margin-top: 20px;
        }
        
        .suggestions h3 {
            color: #2c3e50;
            margin-bottom: 15px;
        }
        
        .suggestion-list {
            list-style: none;
        }
        
        .suggestion-item {
            padding: 10px 15px;
            margin-bottom: 8px;
            background: #f8f9fa;
            border-radius: 6px;
            cursor: pointer;
            transition: background 0.2s;
            border-left: 3px solid #3498db;
        }
        
        .suggestion-item:hover {
            background: #e9ecef;
        }
        
        .results-section {
            margin-top: 30px;
        }
        
        .results-header {
            display: flex;
            justify-content: between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 15px;
            border-bottom: 2px solid #e9ecef;
        }
        
        .results-title {
            font-size: 1.5em;
            color: #2c3e50;
        }
        
        .results-meta {
            display: flex;
            gap: 20px;
            color: #7f8c8d;
            font-size: 0.9em;
        }
        
        .results-table {
            width: 100%;
            border-collapse: collapse;
            background: white;
            border-radius: 8px;
            overflow: hidden;
            box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        }
        
        .results-table th {
            background: #34495e;
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
        }
        
        .results-table td {
            padding: 12px 15px;
            border-bottom: 1px solid #e9ecef;
        }
        
        .results-table tr:hover {
            background: #f8f9fa;
        }
        
        .insights-panel {
            margin-top: 20px;
            padding: 20px;
            background: #e8f5e8;
            border-radius: 8px;
            border-left: 4px solid #27ae60;
        }
        
        .insights-panel h3 {
            color: #27ae60;
            margin-bottom: 15px;
        }
        
        .insight-item {
            margin-bottom: 10px;
            padding-left: 20px;
            position: relative;
        }
        
        .insight-item:before {
            content: "•";
            color: #27ae60;
            font-weight: bold;
            position: absolute;
            left: 0;
        }
        
        .loading {
            display: none;
            text-align: center;
            padding: 40px;
            color: #7f8c8d;
        }
        
        .loading-spinner {
            border: 4px solid #f3f3f3;
            border-top: 4px solid #3498db;
            border-radius: 50%;
            width: 40px;
            height: 40px;
            animation: spin 1s linear infinite;
            margin: 0 auto 20px;
        }
        
        @keyframes spin {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        
        .error-message {
            background: #f8d7da;
            color: #721c24;
            padding: 15px;
            border-radius: 8px;
            border-left: 4px solid #dc3545;
            margin: 20px 0;
        }
        
        .success-message {
            background: #d4edda;
            color: #155724;
            padding: 15px;
            border-radius: 8px;
            border-left: 4px solid #28a745;
            margin: 20px 0;
        }
        
        .file-info {
            background: #e3f2fd;
            padding: 15px;
            border-radius: 8px;
            border-left: 4px solid #2196f3;
            margin-top: 15px;
        }
        
        .file-info h4 {
            color: #1976d2;
            margin-bottom: 10px;
        }
        
        .file-details {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 10px;
            font-size: 0.9em;
        }
        
        .file-detail {
            display: flex;
            justify-content: space-between;
        }
        
        .file-detail strong {
            color: #1976d2;
        }
        
        @media (max-width: 768px) {
            .main-content {
                grid-template-columns: 1fr;
            }
            
            .query-form {
                flex-direction: column;
            }
            
            .schema-stats {
                grid-template-columns: 1fr;
            }
        }
        .insight-item {
            background: #e8f5e8;
            padding: 10px;
            margin: 5px 0;
            border-radius: 5px;
            border-left: 3px solid #27ae60;
        }
        
        .schema-explanation {
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            margin: 20px 0;
            font-family: 'Courier New', monospace;
            white-space: pre-wrap;
            overflow-x: auto;
        }
        
        .schema-table {
            width: 100%;
            border-collapse: collapse;
            margin: 10px 0;
            background: white;
            border-radius: 5px;
            overflow: hidden;
            box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        }
        
        .schema-table th {
            background: #34495e;
            color: white;
            padding: 12px;
            text-align: left;
            font-weight: bold;
        }
        
        .schema-table td {
            padding: 10px;
            border-bottom: 1px solid #e9ecef;
        }
        
        .schema-table tr:hover {
            background: #f8f9fa;
        }
        
        .business-analysis {
            background: #e3f2fd;
            padding: 15px;
            border-radius: 8px;
            margin: 10px 0;
            border-left: 4px solid #2196f3;
        }
        
        .query-type-badge {
            display: inline-block;
            padding: 4px 8px;
            border-radius: 12px;
            font-size: 0.8em;
            font-weight: bold;
            text-transform: uppercase;
        }
        
        .query-type-schema {
            background: #e8f5e8;
            color: #2e7d32;
        }
        
        .query-type-business {
            background: #e3f2fd;
            color: #1565c0;
        }
        
        .query-type-data {
            background: #fff3e0;
            color: #ef6c00;
        }
        .logo {
            height: 40px;
            width: auto;
            margin-right: 15px;
            border-radius: 5px;
        }
        
        .small-logo {
            height: 24px;
            width: auto;
            margin-right: 8px;
            border-radius: 3px;
            vertical-align: middle;
        }
        
        .logo-section {
            display: flex;
            align-items: center;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <div class="header-content">
                <div class="logo-section">
                    <img src="{{ url_for('static', filename='its_consulting_logo_transparent.png') }}" alt="ITS Consulting Logo" class="logo">
                    <div>
                        <h1>SAP Assistant</h1>
                        <div style="font-size: 1.1em; color: #888; margin-top: 4px;">Powered by ITS Consulting Inc.</div>
                    </div>
                </div>
                <!-- Removed header-stats from here -->
            </div>
        </div>
        
        <div class="main-content">
            <div class="sidebar">
                <div class="upload-section">
                    <h3>📁 Upload SAP Data</h3>
                    <div class="upload-area" id="uploadArea">
                        <input type="file" id="fileInput" accept=".csv,.xlsx,.xls" 
                               onchange="handleFileUpload(this)" style="display: none;">
                        <div class="upload-content">
                            <div class="upload-icon">📄</div>
                            <p>Drag & drop your SAP export file here</p>
                            <p>or <span class="upload-link" onclick="document.getElementById('fileInput').click()">browse files</span></p>
                            <p class="upload-info">Supports: CSV, Excel (max 100MB)</p>
                        </div>
                    </div>
                    
                    {% if uploaded_file %}
                    <div class="file-info">
                        <h3>📊 Current File</h3>
                        <div class="file-details">
                            <p><strong>Name:</strong> {{ uploaded_file.name }}</p>
                            <p><strong>Size:</strong> {{ "%.2f"|format(uploaded_file.size_mb) }} MB</p>
                            <p><strong>Records:</strong> {{ uploaded_file.rows | default('N/A') }}</p>
                            <p><strong>Columns:</strong> {{ uploaded_file.columns | default('N/A') }}</p>
                        </div>
                    </div>
                    {% endif %}
                </div>
                
                {% if schema_analysis %}
                <div class="schema-info">
                    <h3>📊 Schema Analysis</h3>
                    
                    <!-- Basic Info Table -->
                    <table class="schema-table">
                        <thead>
                            <tr>
                                <th>Property</th>
                                <th>Value</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td><strong>Table Type</strong></td>
                                <td>{{ schema_analysis.sap_table_type }}</td>
                            </tr>
                            <tr>
                                <td><strong>Total Rows</strong></td>
                                <td>{{ schema_analysis.file_info.total_rows }}</td>
                            </tr>
                            <tr>
                                <td><strong>Total Columns</strong></td>
                                <td>{{ schema_analysis.file_info.total_columns }}</td>
                            </tr>
                            <tr>
                                <td><strong>File Size</strong></td>
                                <td>{{ schema_analysis.file_info.file_size_mb }} MB</td>
                            </tr>
                            <tr>
                                <td><strong>Null Values</strong></td>
                                <td>{{ schema_analysis.data_insights.data_quality.null_percentage }}%</td>
                            </tr>
                        </tbody>
                    </table>
                    
                    <!-- Column Analysis Table -->
                    {% if schema_analysis.column_analysis %}
                    <h4 style="margin-top: 20px; margin-bottom: 10px;">📋 Column Details</h4>
                    <div style="max-height: 300px; overflow-y: auto;">
                        <table class="schema-table">
                            <thead>
                                <tr>
                                    <th>Column</th>
                                    <th>Type</th>
                                    <th>SAP Pattern</th>
                                    <th>Null %</th>
                                </tr>
                            </thead>
                            <tbody>
                                {% for col_name, col_info in schema_analysis.column_analysis.items() %}
                                <tr>
                                    <td><strong>{{ col_name }}</strong></td>
                                    <td>{{ col_info.data_category.title() if col_info.data_category else 'Unknown' }}</td>
                                    <td>{{ ', '.join(col_info.sap_patterns) if col_info.sap_patterns else '-' }}</td>
                                    <td>{{ col_info.null_percentage }}%</td>
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                    </div>
                    {% endif %}
                    
                    <!-- Schema Summary -->
                    {% if schema_analysis.schema_summary %}
                    <div style="margin-top: 15px; padding: 10px; background: #f8f9fa; border-radius: 5px;">
                        <strong>Description:</strong> {{ schema_analysis.schema_summary }}
                    </div>
                    {% endif %}
                </div>
                {% endif %}
                
                <div class="suggestions">
                    <h3>💡 Try These Questions</h3>
                    <ul class="suggestion-list">
                        {% for suggestion in query_suggestions %}
                        <li class="suggestion-item" onclick="setQuery('{{ suggestion }}')">
                            {{ suggestion }}
                        </li>
                        {% endfor %}
                        {% if query_suggestions|length == 0 %}
                        <!-- Always show at least some default questions -->
                        <li class="suggestion-item" onclick="setQuery('Show me all vendor payments for the last quarter')">Show me all vendor payments for the last quarter</li>
                        <li class="suggestion-item" onclick="setQuery('List overdue invoices by customer')">List overdue invoices by customer</li>
                        <li class="suggestion-item" onclick="setQuery('Summarize expenses by cost center')">Summarize expenses by cost center</li>
                        <li class="suggestion-item" onclick="setQuery('Find duplicate payments')">Find duplicate payments</li>
                        <li class="suggestion-item" onclick="setQuery('Show top 5 vendors by spend')">Show top 5 vendors by spend</li>
                        <li class="suggestion-item" onclick="setQuery('Which invoices are missing PO numbers?')">Which invoices are missing PO numbers?</li>
                        {% endif %}
                    </ul>
                </div>

                <!-- Moved stats box here -->
                <div class="sidebar-stats" style="margin-top: 30px;">
                    <div class="stat">
                        <div class="stat-number">{{ stats.total_queries if stats else 0 }}</div>
                        <div class="stat-label">Queries</div>
                    </div>
                    <div class="stat">
                        <div class="stat-number">{{ stats.avg_response_time if stats else 0 }}</div>
                        <div class="stat-label">Avg Time (s)</div>
                    </div>
                    <div class="stat">
                        <div class="stat-number">{{ stats.success_rate if stats else 0 }}</div>
                        <div class="stat-label">Success %</div>
                    </div>
                </div>
            </div>
            
            <div class="main-panel">
                <div class="query-section">
                    <h3><img src="{{ url_for('static', filename='its_consulting_logo_small.png') }}" alt="ITS Consulting" class="small-logo"> Ask Your Question</h3>
                    <form method="post" class="query-form" id="queryForm">
                        <input type="text" name="question" class="query-input" 
                               placeholder="e.g., Show me overdue invoices from Q2 2024" 
                               value="{{ question }}" id="questionInput" {% if not uploaded_file %}disabled{% endif %}>
                        <button type="submit" class="query-btn play-btn" id="queryBtn" 
                                {% if not uploaded_file %}disabled{% endif %} title="Submit">
                            <!-- Play icon SVG -->
                            <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
                                <circle cx="12" cy="12" r="12" fill="#27ae60"/>
                                <polygon points="10,8 16,12 10,16" fill="white"/>
                            </svg>
                        </button>
</form>
                </div>
                <!-- Chat history panel -->
                <div class="chat-history" style="margin-top: 20px; background: #f8f9fa; border-radius: 8px; padding: 15px; min-height: 120px; max-height: 300px; overflow-y: auto;">
                    {% for msg in chat_history %}
                        {% if msg.role == 'user' %}
                            <div style="margin-bottom: 8px;"><strong>You:</strong> {{ msg.content }}</div>
                        {% else %}
                            <div style="margin-bottom: 8px; color: #2c3e50;"><strong>Assistant:</strong> {{ msg.content }}</div>
                        {% endif %}
                    {% endfor %}
                </div>
                
                <div class="loading" id="loading">
                    <div class="loading-spinner"></div>
                    <p>🤔 Analyzing your question and processing data...</p>
                </div>
                
                {% if error_message %}
                <div class="error-message">
                    <strong>Error:</strong> {{ error_message }}
                </div>
                {% endif %}
                
                {% if success_message %}
                <div class="success-message" style="background: #e8f5e9; color: #2e7d32; border: 1px solid #b2dfdb; padding: 10px; border-radius: 6px; margin-bottom: 15px;">
                    {{ success_message }}
                </div>
                {% endif %}
                
                {% if query_results %}
                <div class="results-section">
                    <div class="results-header">
                        <div class="results-title">
                            {% if query_results.query_type == 'explain_schema' %}
                                📊 Schema Analysis
                            {% elif query_results.query_type == 'business_analysis' %}
                                💼 Business Analysis
                            {% else %}
                                📈 Analysis Results
                            {% endif %}
                        </div>
                        <div class="results-meta">
                            <span>⏱️ {{ "%.2f"|format(query_results.execution_time) if query_results and query_results.execution_time is not none else 'N/A' }}s</span>
                            <span>📊 {{ query_results.row_count if query_results and query_results.row_count is not none else 'N/A' }} rows</span>
                            <span class="query-type-badge 
                                {% if query_results and query_results.query_type == 'explain_schema' %}query-type-schema
                                {% elif query_results and query_results.query_type == 'business_analysis' %}query-type-business
                                {% else %}query-type-data{% endif %}">
                                {{ query_results.query_type if query_results and query_results.query_type else 'N/A' }}
                            </span>
                        </div>
                    </div>
                    
                    {% if query_results and query_results.query_type == 'explain_schema' %}
                    <div class="business-analysis">
                        <h3>💬 Answer to Your Question</h3>
                        <!-- Debug info -->
                        <p style="font-size: 0.8em; color: #666; margin-bottom: 10px;">
                            Debug: query_type={{ query_results.query_type }}, 
                            has_nl_response={{ query_results.natural_language_response is not none }}, 
                            nl_response_length={{ query_results.natural_language_response|length if query_results.natural_language_response else 0 }}
                        </p>
                        {% if query_results.natural_language_response %}
                        <ul style="margin-left: 1.5em;">
                            {% for line in query_results.natural_language_response.split('\n') if line.strip() %}
                            <li>{{ line }}</li>
                            {% endfor %}
                        </ul>
                        {% else %}
                        <p>Schema explanation generated successfully.</p>
                        {% endif %}
                    </div>
                    {% if query_results.schema_explanation %}
                    <div class="schema-explanation">
                        <h3>📊 Detailed Schema Analysis</h3>
                        {{ query_results.schema_explanation | safe }}
                    </div>
                    {% endif %}
                    {% endif %}
                    
                    {% if query_results and query_results.query_type == 'business_analysis' and query_results.business_analysis %}
                    <div class="business-analysis">
                        <h3>💬 Answer to Your Question</h3>
                        {% if query_results.natural_language_response %}
                        <ul style="margin-left: 1.5em;">
                            {% for line in query_results.natural_language_response.split('\n') if line.strip() %}
                            <li>{{ line }}</li>
                            {% endfor %}
                        </ul>
                        {% else %}
                        <p>Business analysis completed successfully.</p>
                        {% endif %}
                    </div>
                    <div class="business-analysis" style="margin-top: 15px;">
                        <h3>📈 Detailed Analysis</h3>
                        {{ query_results.business_analysis | safe }}
                    </div>
                    {% endif %}
                    
                    {% if query_results.data and query_results.query_type not in ['explain_schema', 'business_analysis'] %}
                    <div style="overflow-x: auto;">
                        <table class="results-table">
                            <thead>
                                <tr>
                                    {% for column in query_results.columns if query_results.columns %}
                                    <th>{{ column }}</th>
                                    {% endfor %}
                                </tr>
                            </thead>
                            <tbody>
                                {% for row in query_results.data[:100] if query_results.data %}
                                <tr>
                                    {% for value in row %}
                                    <td>{{ value }}</td>
                                    {% endfor %}
                                </tr>
                                {% endfor %}
                            </tbody>
                        </table>
                        {% if query_results and query_results.row_count and query_results.row_count > 100 %}
                        <p style="text-align: center; margin-top: 10px; color: #7f8c8d;">
                            Showing first 100 of {{ query_results.row_count }} results
                        </p>
                        {% endif %}
                    </div>
                    {% endif %}
                    
                    {% if query_results and query_results.insights and query_results.query_type not in ['explain_schema', 'business_analysis'] %}
                    <div class="insights-panel">
                        <h3>💡 Insights</h3>
                        {% for insight in query_results.insights %}
                        <div class="insight-item">{{ insight }}</div>
                        {% endfor %}
                    </div>
                    {% endif %}
                </div>
                {% endif %}
            </div>
        </div>
    </div>
    
    <script>
        function setQuery(query) {
            document.getElementById('questionInput').value = query;
        }
        
        function showLoading() {
            document.getElementById('loading').style.display = 'block';
        }
        
        function handleFileUpload(input) {
            if (input.files && input.files[0]) {
                const file = input.files[0];
                const formData = new FormData();
                formData.append('file', file);
                
                showLoading();
                
                fetch('/upload', {
                    method: 'POST',
                    body: formData
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        location.reload();
                    } else {
                        alert('Error uploading file: ' + data.message);
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                    alert('Error uploading file');
                })
                .finally(() => {
                    document.getElementById('loading').style.display = 'none';
                });
            }
        }
        
        // Make the entire upload area clickable
        document.addEventListener('DOMContentLoaded', function() {
            const uploadArea = document.getElementById('uploadArea');
            const fileInput = document.getElementById('fileInput');
            if (uploadArea && fileInput) {
                uploadArea.addEventListener('click', function(e) {
                    // Only trigger if not clicking a link (like the browse files span)
                    if (e.target.tagName.toLowerCase() !== 'span' && e.target.tagName.toLowerCase() !== 'a') {
                        fileInput.click();
                    }
                });
            }
        });
        
        // Drag and drop functionality
        const uploadArea = document.getElementById('uploadArea');
        
        uploadArea.addEventListener('dragover', (e) => {
            e.preventDefault();
            uploadArea.classList.add('dragover');
        });
        
        uploadArea.addEventListener('dragleave', () => {
            uploadArea.classList.remove('dragover');
        });
        
        uploadArea.addEventListener('drop', (e) => {
            e.preventDefault();
            uploadArea.classList.remove('dragover');
            
            const files = e.dataTransfer.files;
            if (files.length > 0) {
                document.getElementById('fileInput').files = files;
                handleFileUpload(document.getElementById('fileInput'));
            }
        });
        
        // Form submission
        document.getElementById('queryForm').addEventListener('submit', function(e) {
            showLoading();
        });
    </script>
</body>
</html>
"""

# Global variables to store uploaded data
uploaded_files = {}

@app.route("/", methods=["GET", "POST"])
def index():
    """Main application route"""
    error_message = ""
    success_message = ""
    question = ""
    query_results = None
    uploaded_file = None
    schema_analysis = None
    query_suggestions = []
    chat_history = session.get('chat_history', [])
    
    # Get session data
    session_id = session.get('session_id')
    if session_id and session_id in uploaded_files:
        uploaded_file = uploaded_files[session_id]
        schema_analysis = uploaded_file.get('schema_analysis')
        query_suggestions = get_query_suggestions(schema_analysis) if schema_analysis else ENTERPRISE_EXAMPLE_QUERIES[:6]
    if not query_suggestions or len(query_suggestions) == 0:
        query_suggestions = ENTERPRISE_EXAMPLE_QUERIES[:6]

    # Fetch stats for sidebar
    stats = logger.get_demo_stats() if hasattr(logger, 'get_demo_stats') else None
    
    if request.method == "POST":
        if 'file' in request.files:
            # Handle file upload
            resp = handle_file_upload()
            # If upload was successful, set a success message and reload
            if resp.is_json and resp.json.get('success'):
                success_message = 'File uploaded successfully! You can now ask questions about your SAP data.'
                return render_template_string(
                    HTML_TEMPLATE,
                    error_message=error_message,
                    success_message=success_message,
                    question=question,
                    query_results=query_results,
                    uploaded_file=uploaded_file,
                    schema_analysis=schema_analysis,
                    query_suggestions=query_suggestions,
                    stats=stats,
                    chat_history=chat_history
                )
            return resp
        else:
            # Handle query submission
            question = request.form.get("question", "")
            # Detect indiscernible or irrelevant questions
            if not question or len(question.strip()) < 3 or question.lower().strip() in ["dog", "cat", "hello", "hi", "test"]:
                error_message = "Sorry, I couldn't understand your question. Please ask a specific question about your SAP data (e.g., 'Show vendor payments for Q1', 'Analyze overdue invoices', etc.)."
                chat_history.append({"role": "user", "content": question})
                chat_history.append({"role": "assistant", "content": error_message})
                session['chat_history'] = chat_history
                return render_template_string(
                    HTML_TEMPLATE,
                    error_message=error_message,
                    success_message=success_message,
                    question=question,
                    query_results=query_results,
                    uploaded_file=uploaded_file,
                    schema_analysis=schema_analysis,
                    query_suggestions=query_suggestions,
                    stats=stats,
                    chat_history=chat_history
                )
            if question and session_id and session_id in uploaded_files:
                query_results = process_query(question, uploaded_files[session_id], chat_history)
                # Add to chat history
                chat_history.append({"role": "user", "content": question})
                if query_results and 'natural_language_response' in query_results:
                    chat_history.append({"role": "assistant", "content": query_results['natural_language_response']})
                elif query_results and 'message' in query_results:
                    chat_history.append({"role": "assistant", "content": query_results['message']})
                session['chat_history'] = chat_history
            else:
                error_message = "Please upload a file first before asking questions."
    
    return render_template_string(
        HTML_TEMPLATE,
        error_message=error_message,
        success_message=success_message,
        question=question,
        query_results=query_results,
        uploaded_file=uploaded_file,
        schema_analysis=schema_analysis,
        query_suggestions=query_suggestions,
        stats=stats,
        chat_history=chat_history
    )

@app.route("/upload", methods=["POST"])
def handle_file_upload():
    """Handle file upload"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'message': 'No file provided'})
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'message': 'No file selected'})
        
        if file and allowed_file(file.filename):
            # Generate session ID if not exists
            if 'session_id' not in session:
                session['session_id'] = str(uuid.uuid4())
            
            session_id = session['session_id']
            
            # Save file
            filename = secure_filename(file.filename)
            filepath = os.path.join(app.config['UPLOAD_FOLDER'], f"{session_id}_{filename}")
            file.save(filepath)
            
            # Load data into memory first (for small files)
            df = pd.read_csv(filepath)
            
            # Analyze schema
            schema_analysis = schema_analyzer.analyze_csv_file(filepath)
            
            # Log detected columns for debugging
            logger.app_logger.info(f"[DEBUG] Detected columns in column_analysis: {list(schema_analysis.get('column_analysis', {}).keys())}")
            
            # Identify report type using new module
            report_identification = report_identifier.identify_report_type(df.columns.tolist())
            
            # Get schema mapping
            schema_mapping = schema_mapper.get_schema(report_identification['table_type'])
            
            # Create enhanced schema summary with column descriptions
            schema_summary = schema_mapper.create_schema_summary(
                report_identification['table_type'], 
                df.columns.tolist()
            )
            
            # Enhance schema analysis with new information
            schema_analysis['report_identification'] = report_identification
            schema_analysis['schema_mapping'] = schema_mapping
            schema_analysis['schema_summary'] = schema_summary
            schema_analysis['sap_table_type'] = report_identification['table_type']
            
            # Store file info
            uploaded_files[session_id] = {
                'filepath': filepath,
                'filename': filename,
                'df': df,
                'schema_analysis': schema_analysis,
                'name': filename,
                'size_mb': schema_analysis['file_info']['file_size_mb'],
                'rows': schema_analysis['file_info']['total_rows'],
                'columns': schema_analysis['file_info']['total_columns']
            }
            
            logger.log_data_operation(
                'file_uploaded',
                filename,
                schema_analysis['file_info']['total_rows'],
                {'session_id': session_id, 'file_size_mb': schema_analysis['file_info']['file_size_mb']}
            )
            
            # Clear chat history on new upload
            session['chat_history'] = []
            
            return jsonify({'success': True, 'message': 'File uploaded successfully'})
        else:
            return jsonify({'success': False, 'message': 'Invalid file type'})
    
    except Exception as e:
        logger.log_error('file_upload_error', str(e))
        return jsonify({'success': False, 'message': f'Error uploading file: {str(e)}'})

def process_query(question: str, file_data: Dict[str, Any], chat_history=None) -> Dict[str, Any]:
    """Process a natural language query"""
    try:
        start_time = time.time()
        
        # Create query planner
        query_planner = SAPQueryPlanner(file_data['schema_analysis'])
        
        # Plan the query
        plan_result = query_planner.plan_query(question)
        
        if plan_result['status'] == 'ambiguous':
            return {
                'status': 'ambiguous',
                'message': plan_result['message'],
                'clarification_questions': plan_result['clarification_questions'],
                'execution_time': time.time() - start_time,
                'row_count': 0,
                'query_type': 'ambiguous',
                'data': [],
                'columns': [],
                'insights': []
            }
        
        if plan_result['status'] == 'error':
            return {
                'status': 'error',
                'message': plan_result['message'],
                'execution_time': time.time() - start_time,
                'row_count': 0,
                'query_type': 'error',
                'data': [],
                'columns': [],
                'insights': []
            }
        
        # Execute the query
        query_executor = SAPQueryExecutor(file_data['df'], file_data['schema_analysis'])
        execution_result = query_executor.execute_query(plan_result['query_plan'])
        
        if execution_result['status'] == 'error':
            return {
                'status': 'error',
                'message': execution_result['message'],
                'execution_time': time.time() - start_time,
                'row_count': 0,
                'query_type': 'error',
                'data': [],
                'columns': [],
                'insights': []
            }
        
        # Get AI insights
        ai_response = get_ai_insights(question, file_data['schema_analysis'], execution_result)
        
        processing_time = time.time() - start_time
        
        # Log the interaction
        logger.log_user_interaction(
            user_question=question,
            ai_response=ai_response,
            processing_time=processing_time,
            data_context=file_data['schema_analysis']
        )
        
        return {
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
    
    except Exception as e:
        # Handle OpenAI quota errors gracefully
        if "insufficient_quota" in str(e) or "quota" in str(e).lower():
            logger.log_error('openai_quota_error', f"OpenAI quota exceeded: {str(e)}", {'question': question})
            return {
                'status': 'success',
                'data': execution_result.get('data', []),
                'columns': execution_result.get('columns', []),
                'row_count': execution_result.get('row_count', 0),
                'execution_time': execution_result.get('execution_time', 0),
                'query_type': plan_result['query_plan'].get('action', 'show'),
                'insights': execution_result.get('insights', []),
                'ai_response': "AI insights temporarily unavailable due to API quota limits. Please try again later or contact your administrator.",
                'natural_language_response': execution_result.get('natural_language_response', '')
            }
        
        # Handle other errors with safe serialization
        error_context = {'question': question}
        try:
            # Safely serialize context for logging
            logger.log_error('query_processing_error', str(e), error_context)
        except Exception as log_error:
            # If logging fails, use a simple string
            logger.log_error('query_processing_error', f"Error: {str(e)}. Logging failed: {str(log_error)}")
        
        return {
            'status': 'error',
            'message': f'Error processing query: {str(e)}',
            'execution_time': time.time() - start_time if 'start_time' in locals() else 0,
            'row_count': 0,
            'query_type': 'error',
            'data': [],
            'columns': [],
            'insights': []
        }

def get_ai_insights(question: str, schema_analysis: Dict[str, Any], execution_result: Dict[str, Any]) -> str:
    """Get AI insights about the query results"""
    try:
        # Create context-aware prompt
        messages = create_enterprise_query_prompt(question, schema_analysis)
        
        # Add execution results context
        execution_context = f"""
Query Execution Results:
- Rows returned: {execution_result['row_count']}
- Execution time: {execution_result['execution_time']:.2f} seconds
- Data summary: {execution_result.get('summary_stats', {})}

Please provide business insights and recommendations based on these results.
"""
        messages.append({"role": "system", "content": execution_context})
        
        # Call OpenAI (new API syntax)
        reply = openai.chat.completions.create(
            model="gpt-4",
            messages=messages,
            max_tokens=500,
            temperature=0.3
        )
        response = reply.choices[0].message.content.strip()
        
        # Log the AI request
        logger.log_ai_request(messages, response)
        
        return response
    
    except Exception as e:
        # Handle OpenAI quota errors gracefully
        if "insufficient_quota" in str(e) or "quota" in str(e).lower():
            logger.log_error('openai_quota_error', f"OpenAI quota exceeded: {str(e)}")
            return "AI insights are temporarily unavailable due to API quota limits. The data analysis results are still available below."
        
        # Handle other errors
        logger.log_error('ai_insights_error', str(e))
        return f"Unable to generate AI insights: {str(e)}"

@app.route("/api/stats")
def get_stats():
    """API endpoint to get demo statistics"""
    stats = logger.get_demo_stats()
    recent_interactions = logger.get_recent_interactions(5)
    
    return jsonify({
        'stats': stats,
        'recent_interactions': recent_interactions,
        'active_sessions': len(uploaded_files)
    })

@app.route("/api/schema/<session_id>")
def get_schema(session_id):
    """API endpoint to get schema analysis for a session"""
    if session_id in uploaded_files:
        return jsonify(uploaded_files[session_id]['schema_analysis'])
    else:
        return jsonify({'error': 'Session not found'}), 404

# Cleanup old sessions periodically
def cleanup_old_sessions():
    """Clean up old session data"""
    current_time = time.time()
    expired_sessions = []
    
    for session_id, file_data in uploaded_files.items():
        # Remove sessions older than 1 hour
        if current_time - file_data.get('created_time', 0) > 3600:
            expired_sessions.append(session_id)
    
    for session_id in expired_sessions:
        try:
            # Remove file
            if os.path.exists(uploaded_files[session_id]['filepath']):
                os.remove(uploaded_files[session_id]['filepath'])
            # Remove from memory
            del uploaded_files[session_id]
        except:
            pass

# When the file is run directly (not imported), start the Flask development server
if __name__ == "__main__":
    # Clean up old sessions on startup
    cleanup_old_sessions()
    
    # Run the app on all interfaces (important for Docker) at port 5000
    app.run(host="0.0.0.0", port=5000, debug=True)
