"""Health check endpoint."""
from fastapi import APIRouter
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from app.core.config import settings
from app.core.tracing import get_tracer

router = APIRouter()
tracer = get_tracer()


@router.get("/health")
async def health_check():
    """Health check endpoint with tracing verification."""
    # Create a test span to verify tracing works
    with tracer.start_as_current_span("health_check") as span:
        span.set_attribute("endpoint", "/health")
        span.set_attribute("test", True)
        span.set_attribute("service.name", settings.service_name)
        
        # Force immediate export for testing
        try:
            span.set_status(Status(StatusCode.OK))
            
            # Get current span context for verification
            current_span = trace.get_current_span()
            if current_span:
                span_context = current_span.get_span_context()
                trace_id = format(span_context.trace_id, '032x') if span_context.is_valid else "invalid"
            else:
                trace_id = "no_span"
            
            return {
                "status": "healthy",
                "app_name": settings.app_name,
                "version": settings.app_version,
                "tracing_enabled": True,
                "trace_id": trace_id,
                "service_name": settings.service_name
            }
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            return {
                "status": "healthy",
                "tracing_error": str(e)
            }

