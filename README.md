# SAP AI Demo - Natural Language Query Interface

A demonstration application showcasing how natural language queries can be used to interact with SAP ECC financial data through an AI-powered interface.

## 🎯 Overview

This demo simulates what an AI assistant could do on top of SAP ECC, allowing users to ask natural language questions about financial data and receive intelligent responses with structured data insights.

## ✨ Features

- **Natural Language Processing**: Ask questions in plain English about SAP financial data
- **SAP-Aware AI**: GPT-4 powered responses with deep understanding of SAP ECC concepts
- **Mock Data Management**: Realistic SAP table structures (BKPF, BSEG, LFA1, KNA1, SKAT)
- **Query Routing**: Intelligent routing of queries to appropriate data handlers
- **Comprehensive Logging**: Full traceability of user interactions and system performance
- **Modern UI**: Clean, responsive web interface with real-time data visualization
- **Docker Support**: Containerized deployment for easy setup and distribution

## 🏗️ Architecture

### Core Components

1. **`app.py`** - Main Flask application with enhanced UI and API endpoints
2. **`prompt_templates.py`** - SAP-aware prompt engineering for better AI responses
3. **`data_manager.py`** - Mock SAP data loading and basic querying capabilities
4. **`query_router.py`** - Intelligent query routing and pattern matching
5. **`logger_config.py`** - Comprehensive logging and debugging system

### Data Structure

The demo uses mock SAP ECC tables:

- **BKPF** (Accounting Document Header): Document metadata, posting dates, types
- **BSEG** (Accounting Document Segment): Line items, amounts, account assignments
- **LFA1** (Vendor Master): Vendor information and master data
- **KNA1** (Customer Master): Customer information and master data
- **SKAT** (G/L Account Master): Chart of accounts and descriptions

## 🚀 Quick Start

### Prerequisites

- Python 3.11+
- OpenAI API key
- Docker (optional)

### Local Development

1. **Clone and setup**:
   ```bash
   git clone <repository-url>
   cd sap-ai-demo
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up environment**:
   ```bash
   # Create .env file
   echo "OPENAI_API_KEY=your_openai_api_key_here" > .env
   ```

4. **Run the application**:
   ```bash
   python app.py
   ```

5. **Access the demo**:
   Open http://localhost:5000 in your browser

### Docker Deployment

1. **Build the image**:
   ```bash
   docker build -t sap-ai-demo .
   ```

2. **Run the container**:
   ```bash
   docker run -p 5000:5000 --env-file .env sap-ai-demo
   ```

## 💬 Example Queries

Try these natural language questions:

- "Show me overdue invoices"
- "What are the top 5 vendors by payment amount?"
- "Summarize Q2 vendor payments"
- "Show customer payment trends for the last 6 months"
- "Which accounts have the highest transaction volume?"
- "Find documents posted in error"
- "Show me all vendor invoices over $10,000"
- "What's the total accounts receivable balance?"

## 🔧 Configuration

### Environment Variables

- `OPENAI_API_KEY`: Your OpenAI API key for GPT-4 access

### Data Configuration

The demo automatically creates sample SAP data on first run. You can:

1. **Use default sample data**: Automatically generated on startup
2. **Load custom CSV files**: Place your own SAP data in the `mock_data/` directory
3. **Modify data structure**: Edit `data_manager.py` to match your SAP schema

### Logging Configuration

Logs are stored in the `logs/` directory:

- `user_interactions.log` - User queries and AI responses
- `sap_demo_debug.log` - Detailed debug information
- `errors.log` - Error tracking and debugging
- `interactions.jsonl` - Structured interaction data

## 📊 Monitoring and Analytics

### API Endpoints

- `GET /` - Main web interface
- `GET /api/stats` - Demo statistics and recent interactions

### Demo Statistics

The interface displays:
- Total user interactions
- Error count
- Average response time
- Data summary information

## 🛠️ Development

### Adding New Query Types

1. **Extend query patterns** in `query_router.py`:
   ```python
   {
       'pattern': r'your_pattern_here',
       'handler': self._handle_your_query_type,
       'type': 'your_query_type'
   }
   ```

2. **Implement handler method**:
   ```python
   def _handle_your_query_type(self, question):
       # Your query logic here
       return {
           'query_type': 'your_query_type',
           'data': your_data,
           'explanation': 'Your explanation'
       }
   ```

### Enhancing AI Prompts

Modify `prompt_templates.py` to:
- Add new SAP concepts
- Improve context awareness
- Enhance response quality

### Data Schema Extensions

Extend `data_manager.py` to:
- Add new SAP tables
- Implement complex queries
- Support additional data types

## 🔍 Troubleshooting

### Common Issues

1. **OpenAI API Errors**:
   - Verify your API key is correct
   - Check API quota and billing
   - Ensure network connectivity

2. **Data Loading Issues**:
   - Check file permissions for `mock_data/` directory
   - Verify CSV file formats
   - Review error logs in `logs/errors.log`

3. **Performance Issues**:
   - Monitor response times in logs
   - Check OpenAI API rate limits
   - Optimize query patterns if needed

### Debug Mode

Run with debug enabled for detailed logging:
```bash
python app.py
# Debug mode is enabled by default
```

## 📈 Future Enhancements

### Planned Features

- **Real SAP Connectivity**: Direct connection to SAP systems
- **Advanced Analytics**: Trend analysis and predictive insights
- **Multi-language Support**: Internationalization for global SAP deployments
- **User Authentication**: Role-based access control
- **Export Capabilities**: PDF reports and data exports
- **Mobile Interface**: Responsive design for mobile devices

### Integration Possibilities

- **SAP Business Technology Platform (BTP)**
- **SAP Analytics Cloud**
- **SAP HANA Cloud**
- **SAP S/4HANA**

## 📄 License

This project is for demonstration purposes. Please ensure compliance with your organization's policies when using with real SAP data.

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📞 Support

For questions or issues:
- Check the logs in `logs/` directory
- Review the troubleshooting section
- Create an issue in the repository

---

**Note**: This is a demonstration application. Always follow your organization's security and data handling policies when working with real SAP systems. 
