"""Main FastAPI application."""
from dotenv import load_dotenv
from fastapi import FastAPI
from app.core.config import settings
from app.core.logging import setup_logging, logger
from app.core.tracing import (
    setup_tracing,
    instrument_fastapi,
    instrument_redis,
    instrument_elasticsearch,
    instrument_openai,
    shutdown_tracing
)
from app.api.v1 import health, ocap

# Load environment variables from .env file
load_dotenv()

# Setup logging
setup_logging()

# Setup OpenTelemetry tracing
setup_tracing()

# Create FastAPI application
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    debug=settings.debug
)

# Instrument FastAPI with OpenTelemetry
instrument_fastapi(app)

# Include routers
app.include_router(health.router, prefix=settings.api_v1_prefix, tags=["health"])
app.include_router(ocap.router, prefix=settings.api_v1_prefix, tags=["ocap"])

# Include auth router
from app.api.v1 import auth
app.include_router(auth.router, prefix=settings.api_v1_prefix, tags=["auth"])


@app.on_event("startup")
async def startup_event():
    """Initialize infrastructure clients at application startup."""
    logger.info("Initializing infrastructure clients...")
    
    # Instrument external services with OpenTelemetry
    instrument_redis()
    instrument_elasticsearch()
    instrument_openai()
    
    try:
        # Initialize Redis client
        from app.infra.redis import get_redis_client
        redis_client = get_redis_client()
        logger.info("✓ Redis client initialized")
    except Exception as e:
        logger.warning(f"⚠ Redis client initialization failed: {e}")
        logger.warning("Redis will be initialized on first use")
    
    try:
        # Initialize Elasticsearch client
        from app.infra.elastic import get_elasticsearch_client
        elastic_client = get_elasticsearch_client()
        logger.info("✓ Elasticsearch client initialized")
    except Exception as e:
        logger.warning(f"⚠ Elasticsearch client initialization failed: {e}")
        logger.warning("Elasticsearch will be initialized on first use")
    
    try:
        # Initialize Azure OpenAI client
        from app.infra.azure_openai import get_azure_openai_client
        azure_client = get_azure_openai_client()
        logger.info("✓ Azure OpenAI client initialized")
    except Exception as e:
        logger.warning(f"⚠ Azure OpenAI client initialization failed: {e}")
        logger.warning("Azure OpenAI will be initialized on first use")
    
    try:
        # Initialize database connection
        from app.infra.database import get_engine, is_database_available
        engine = get_engine()
        if is_database_available():
            # Create tables if they don't exist
            from app.models.user import Base
            from app.models.session import Session, WorkflowExecution
            Base.metadata.create_all(bind=engine)
            logger.info("✓ PostgreSQL database connection initialized")
            logger.info("✓ Database tables created/verified (users, sessions, workflow_executions)")
        else:
            logger.warning("⚠ Database connection failed - check DB_URL in .env")
    except Exception as e:
        logger.warning(f"⚠ Database initialization failed: {e}")
        logger.warning("Database features will be disabled")
    
    # Initialize background task queue
    try:
        from app.services.background_tasks import get_background_queue
        bg_queue = get_background_queue()
        logger.info("✓ Background task queue initialized")
    except Exception as e:
        logger.warning(f"⚠ Background task queue initialization failed: {e}")
    
    logger.info("Infrastructure initialization complete")


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up infrastructure connections on application shutdown."""
    logger.info("Shutting down infrastructure clients...")
    
    try:
        from app.infra.redis import RedisClient
        RedisClient.reset()
        logger.info("✓ Redis client closed")
    except Exception as e:
        logger.warning(f"Error closing Redis client: {e}")
    
    # Shutdown background task queue
    try:
        from app.services.background_tasks import shutdown_background_queue
        shutdown_background_queue()
        logger.info("✓ Background task queue shut down")
    except Exception as e:
        logger.warning(f"Error shutting down background task queue: {e}")
    
    # Shutdown tracing
    shutdown_tracing()


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": f"Welcome to {settings.app_name}",
        "version": settings.app_version,
        "docs": "/docs"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)

