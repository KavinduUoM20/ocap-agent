"""OpenTelemetry tracing configuration."""
import logging
from typing import Optional
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.instrumentation.redis import RedisInstrumentor
from opentelemetry.instrumentation.elasticsearch import ElasticsearchInstrumentor
from opentelemetry.instrumentation.openai import OpenAIInstrumentor
from app.core.config import settings
from app.core.logging import logger

# Global tracer instance
_tracer: Optional[trace.Tracer] = None
_tracer_provider: Optional[TracerProvider] = None


def setup_tracing() -> None:
    """
    Initialize OpenTelemetry tracing with Jaeger exporter.
    
    This sets up:
    - TracerProvider with service name and version
    - Jaeger exporter
    - Automatic instrumentation for FastAPI, Redis, Elasticsearch, OpenAI
    """
    global _tracer, _tracer_provider
    
    if not settings.enable_tracing:
        logger.info("Tracing is disabled in configuration")
        return
    
    try:
        # Create resource with service information
        resource = Resource.create({
            "service.name": settings.service_name,
            "service.version": settings.app_version,
        })
        
        # Create TracerProvider
        _tracer_provider = TracerProvider(resource=resource)
        
        # Configure OTLP exporter (modern way, works with Jaeger all-in-one)
        # Jaeger all-in-one supports OTLP on port 4317 (gRPC) or 4318 (HTTP)
        otlp_exporter = OTLPSpanExporter(
            endpoint=settings.jaeger_collector_endpoint,
            insecure=True,  # Use True for local development
        )
        
        # Add span processor with explicit flush
        span_processor = BatchSpanProcessor(
            otlp_exporter,
            max_queue_size=512,
            export_timeout_millis=30000,
            schedule_delay_millis=2000  # Flush every 2 seconds for faster visibility
        )
        _tracer_provider.add_span_processor(span_processor)
        
        # Set the global tracer provider BEFORE any instrumentation
        trace.set_tracer_provider(_tracer_provider)
        
        # Force flush on shutdown
        import atexit
        atexit.register(lambda: _tracer_provider.force_flush() if _tracer_provider else None)
        
        # Get tracer instance
        _tracer = trace.get_tracer(__name__)
        
        logger.info(
            f"OpenTelemetry tracing initialized: "
            f"service={settings.service_name}, "
            f"otlp_endpoint={settings.jaeger_collector_endpoint}"
        )
        
        # Verify tracer provider is set
        current_provider = trace.get_tracer_provider()
        if current_provider == _tracer_provider:
            logger.info("✓ TracerProvider successfully set as global provider")
        else:
            logger.warning("⚠ TracerProvider may not be set correctly")
        
    except Exception as e:
        logger.error(f"Failed to initialize OpenTelemetry tracing: {e}", exc_info=True)
        logger.warning("Tracing will be disabled - application will continue without tracing")


def instrument_fastapi(app) -> None:
    """
    Instrument FastAPI application with OpenTelemetry.
    
    Args:
        app: FastAPI application instance
    """
    if not settings.enable_tracing:
        return
    
    try:
        # Ensure tracer provider is set before instrumenting
        if _tracer_provider is None:
            logger.warning("TracerProvider not initialized, FastAPI instrumentation may not work correctly")
        
        # Instrument FastAPI - this will use the global TracerProvider we set
        # Note: instrument_app uses the global tracer provider automatically
        FastAPIInstrumentor.instrument_app(app)
        logger.info("FastAPI instrumentation enabled")
        
        # Verify the global provider is still set after instrumentation
        current_provider = trace.get_tracer_provider()
        if current_provider == _tracer_provider:
            logger.info("✓ TracerProvider maintained after FastAPI instrumentation")
        else:
            logger.warning(f"⚠ TracerProvider changed after instrumentation: {type(current_provider)}")
    except Exception as e:
        logger.error(f"Failed to instrument FastAPI: {e}", exc_info=True)


def instrument_redis() -> None:
    """Instrument Redis client with OpenTelemetry."""
    if not settings.enable_tracing:
        return
    
    try:
        RedisInstrumentor().instrument()
        logger.info("Redis instrumentation enabled")
    except Exception as e:
        logger.error(f"Failed to instrument Redis: {e}", exc_info=True)


def instrument_elasticsearch() -> None:
    """Instrument Elasticsearch client with OpenTelemetry."""
    if not settings.enable_tracing:
        return
    
    try:
        ElasticsearchInstrumentor().instrument()
        logger.info("Elasticsearch instrumentation enabled")
    except Exception as e:
        logger.error(f"Failed to instrument Elasticsearch: {e}", exc_info=True)


def instrument_openai() -> None:
    """Instrument OpenAI client with OpenTelemetry."""
    if not settings.enable_tracing:
        return
    
    try:
        OpenAIInstrumentor().instrument()
        logger.info("OpenAI instrumentation enabled")
    except Exception as e:
        logger.error(f"Failed to instrument OpenAI: {e}", exc_info=True)


def get_tracer() -> trace.Tracer:
    """
    Get the global tracer instance.
    
    Returns:
        Tracer instance, or a NoOpTracer if tracing is disabled
    """
    if _tracer is None:
        return trace.get_tracer(__name__)
    return _tracer


def shutdown_tracing() -> None:
    """Shutdown tracing and flush remaining spans."""
    global _tracer_provider
    
    if _tracer_provider:
        try:
            # Force flush before shutdown to ensure all spans are sent
            _tracer_provider.force_flush(timeout_millis=5000)
            _tracer_provider.shutdown()
            logger.info("Tracing shutdown complete")
        except Exception as e:
            logger.error(f"Error during tracing shutdown: {e}")

