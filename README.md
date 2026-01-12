# OCAP Agent

A FastAPI application for OCAP (Object-Capability) processing.

## Architecture

The application follows a modular architecture:

```
app/
├── main.py                 # FastAPI application entry point
├── api/                    # API endpoints
│   └── v1/                 # API version 1
│       ├── ocap.py         # OCAP processing endpoint
│       └── health.py       # Health check endpoint
├── core/                   # Core utilities
│   ├── config.py           # Configuration settings
│   ├── security.py         # Security utilities
│   ├── logging.py          # Logging configuration
│   └── dependencies.py     # FastAPI dependencies
├── services/               # Business logic services
│   ├── ocap_service.py     # OCAP processing service
│   └── audit_service.py    # Audit service (future)
├── ocap/                   # OCAP-specific modules (future)
├── infra/                  # Infrastructure integrations (future)
├── models/                 # Data models (future)
└── utils/                  # Utility functions (future)
```

## Getting Started

### Installation

1. Create a virtual environment:
```bash
python -m venv venv
```

2. Activate the virtual environment:
```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Application

```bash
python -m app.main
```

Or using uvicorn directly:
```bash
uvicorn app.main:app --reload
```

The API will be available at:
- API: http://localhost:8000
- Docs: http://localhost:8000/docs
- Health: http://localhost:8000/api/v1/health

## API Endpoints

### Health Check
- `GET /api/v1/health` - Check application health

### OCAP Processing
- `POST /api/v1/process` - Process an OCAP query
  - Request body: `{"query": "your query here"}`
  - Response: `{"query": "...", "status": "processed", "message": "..."}`

## Future Enhancements

The application is structured to support future additions:
- LangGraph integration for OCAP processing
- Elasticsearch for fact search
- Redis for caching
- Azure OpenAI integration
- Audit logging
- Advanced security features
