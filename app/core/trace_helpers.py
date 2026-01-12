"""Helper functions for OpenTelemetry tracing in nodes."""
from typing import Dict, Any, Optional, Callable
from functools import wraps
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from app.core.tracing import get_tracer
from app.ocap.state import OCAPState

tracer = get_tracer()


def get_workflow_context(state: OCAPState) -> Dict[str, Any]:
    """
    Extract workflow context from state for tracing attributes.
    
    Args:
        state: OCAP state
        
    Returns:
        Dictionary of context attributes
    """
    metadata = state.get("metadata") or {}
    context = {
        "workflow.run_id": metadata.get("workflow_run_id", "unknown"),
        "thread.id": metadata.get("thread_id", "unknown"),
        "user.id": metadata.get("user_id"),
    }
    
    # Add query (truncated for privacy)
    query = state.get("query", "")
    if query:
        context["query.text"] = query[:100]  # Truncate to 100 chars
    
    # Add classification if available
    classification = state.get("classification")
    if classification:
        context["classification"] = classification
    
    return context


def trace_node(node_name: str):
    """
    Decorator to trace a LangGraph node function.
    
    Creates a span for the node execution with proper attributes.
    
    Args:
        node_name: Name of the node (e.g., "extract_keywords")
        
    Example:
        @trace_node("extract_keywords")
        def extract_keywords(state: OCAPState) -> Dict[str, Any]:
            ...
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(state: OCAPState) -> Dict[str, Any]:
            # Extract context
            context_attrs = get_workflow_context(state)
            
            # Create span for this node
            with tracer.start_as_current_span(f"node.{node_name}") as span:
                # Set attributes
                span.set_attribute("node.name", node_name)
                for key, value in context_attrs.items():
                    if value is not None:
                        span.set_attribute(key, value)
                
                try:
                    # Execute the node function
                    result = func(state)
                    
                    # Add result metadata if available
                    if isinstance(result, dict):
                        if "classification" in result:
                            span.set_attribute("result.classification", result["classification"])
                        if "keywords" in result:
                            span.set_attribute("result.keywords_count", len(result.get("keywords", [])))
                    
                    span.set_status(Status(StatusCode.OK))
                    return result
                    
                except Exception as e:
                    # Record exception
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    span.set_attribute("error", True)
                    raise
        
        return wrapper
    return decorator


def create_workflow_span(workflow_run_id: str, thread_id: Optional[str], user_id: Optional[int]):
    """
    Create a span for the entire workflow execution.
    
    Args:
        workflow_run_id: Unique ID for this workflow run
        thread_id: Thread ID
        user_id: User ID
        
    Returns:
        Span context manager
    """
    span = tracer.start_span("workflow.execute")
    span.set_attribute("workflow.run_id", workflow_run_id)
    if thread_id:
        span.set_attribute("thread.id", thread_id)
    if user_id:
        span.set_attribute("user.id", user_id)
    
    return span

