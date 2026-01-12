"""OCAP service for processing queries."""
import uuid
from typing import Dict, Any, Optional
from datetime import datetime
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from app.core.logging import logger
from app.core.tracing import get_tracer
from app.services.background_tasks import get_background_queue
from app.infra.redis import get_redis_client, is_redis_available
from app.ocap.graph import get_ocap_graph
from app.ocap.state import OCAPState
import json

tracer = get_tracer()


class OCAPService:
    """Service for processing OCAP queries."""
    
    def __init__(self):
        """Initialize OCAP service."""
        self.logger = logger
        self.graph = get_ocap_graph()
    
    def _store_workflow_state_in_redis(self, workflow_run_id: str, result: Dict[str, Any], thread_id: str):
        """
        Store full graph state in Redis for later retrieval.
        This is done asynchronously and doesn't block the workflow.
        
        Args:
            workflow_run_id: Unique workflow execution ID
            result: Complete graph state result
            thread_id: Thread ID for the session
        """
        if not is_redis_available():
            return
        
        try:
            redis_client = get_redis_client()
            if redis_client is None:
                return
            
            # Prepare state data to store
            state_data = {
                "workflow_run_id": workflow_run_id,
                "thread_id": thread_id,
                "query": result.get("query", ""),
                "response": result.get("response", ""),
                "classification": result.get("classification"),
                "keywords": result.get("keywords", []),
                "metadata": result.get("metadata", {}),
                # Include classify results (formatted_text, etc.)
                "classify_results": result.get("metadata", {}).get("classify", {}),
                "created_at": datetime.utcnow().isoformat()
            }
            
            # Store in Redis with key pattern: workflow:{workflow_run_id}
            workflow_key = f"workflow:{workflow_run_id}"
            # Convert to JSON string - Redis handles strings when decode_responses=True
            state_json = json.dumps(state_data, default=str)
            redis_client.setex(
                workflow_key,
                2592000,  # 30 days TTL
                state_json
            )
            
            # Also store in thread's workflow list for easy retrieval
            thread_workflows_key = f"thread:{thread_id}:workflows"
            redis_client.lpush(thread_workflows_key, workflow_run_id)
            redis_client.expire(thread_workflows_key, 2592000)  # 30 days TTL
            # Keep only last 100 workflow IDs per thread
            redis_client.ltrim(thread_workflows_key, 0, 99)
            
            self.logger.debug(f"Workflow state stored in Redis: {workflow_key}")
            
        except Exception as e:
            # Don't fail the workflow if Redis storage fails
            self.logger.warning(f"Failed to store workflow state in Redis: {e}")
    
    def process_query(
        self, 
        query: str, 
        thread_id: Optional[str] = None,
        user_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Process a user query using the OCAP graph.
        
        Args:
            query: The user's query string
            thread_id: Optional thread ID for conversation context
            user_id: Optional user ID for user-specific context
            
        Returns:
            Dictionary containing the processed response
        """
        # Generate workflow run ID for tracing
        workflow_run_id = str(uuid.uuid4())
        start_time = datetime.utcnow()
        
        self.logger.info(
            f"Processing query: {query[:50]}... "
            f"(thread_id: {thread_id}, user_id: {user_id}, workflow_run_id: {workflow_run_id})"
        )
        
        # Generate thread_id if not provided
        if not thread_id:
            # Use a combination of user_id and query hash for thread identification
            if user_id:
                thread_id = f"user_{user_id}_thread_{hash(query) % 10000}"
            else:
                thread_id = f"thread_{hash(query) % 10000}"
        
        # Submit background task to create/update session
        bg_queue = get_background_queue()
        bg_queue.submit("create_or_update_session", {
            "thread_id": thread_id,
            "user_id": user_id,
            "increment_message_count": True
        })
        
        # Submit background task to create workflow execution (pending status)
        bg_queue.submit("create_workflow_execution", {
            "workflow_run_id": workflow_run_id,
            "thread_id": thread_id,
            "user_id": user_id,
            "query": query,
            "status": "pending"
        })
        
        # Create span for the entire workflow
        with tracer.start_as_current_span("ocap_service.process_query") as span:
            # Set span attributes
            span.set_attribute("workflow.run_id", workflow_run_id)
            span.set_attribute("thread.id", thread_id)
            span.set_attribute("query.text", query[:100])  # Truncate for privacy
            if user_id:
                span.set_attribute("user.id", user_id)
            
            # Initialize state with workflow_run_id
            initial_state: OCAPState = {
                "query": query,
                "keywords": None,
                "classification": None,
                "entities": None,
                "relationships": None,
                "response": None,
                "metadata": {
                    "thread_id": thread_id,
                    "user_id": user_id,
                    "workflow_run_id": workflow_run_id
                }
            }
            
            # Run the graph
            try:
                result = self.graph.invoke(initial_state)
                
                # Format response
                metadata = result.get("metadata") or {}
                thread_summary = metadata.get("thread_memory_summary")
                
                # Handle None thread summary gracefully
                if thread_summary is None:
                    thread_summary = "No previous conversation context"
                
                # Get registry matches and query spec summary from metadata
                # These are stored in metadata by the extract_keywords node
                registry_matches = metadata.get("registry_matches", [])
                query_spec_summary = metadata.get("query_spec_summary", "")
                
                # Get classification and classify results from metadata
                # These are stored by analyze_query and classify nodes
                classification = result.get("classification")
                classify_results = metadata.get("classify", {})
                classify_formatted_text = classify_results.get("formatted_text")
                
                # Get the final response from summarize node
                final_response = result.get("response", "")
                
                # Calculate duration
                end_time = datetime.utcnow()
                duration_ms = int((end_time - start_time).total_seconds() * 1000)
                
                # Set span attributes for results
                span.set_attribute("result.classification", classification or "unknown")
                span.set_attribute("result.keywords_count", len(result.get("keywords", [])))
                span.set_attribute("result.registry_matches_count", len(registry_matches))
                span.set_attribute("result.has_response", bool(final_response))
                span.set_status(Status(StatusCode.OK))
                
                # Submit background task to update workflow execution (completed)
                bg_queue.submit("update_workflow_execution", {
                    "workflow_run_id": workflow_run_id,
                    "response": final_response,
                    "status": "completed",
                    "classification": classification,
                    "completed_at": end_time,
                    "duration_ms": duration_ms
                })
                
                # Store full graph state in Redis (async, non-blocking)
                self._store_workflow_state_in_redis(workflow_run_id, result, thread_id)
                
                response = {
                    "query": result.get("query", query),
                    "thread_id": thread_id,
                    "workflow_run_id": workflow_run_id,
                    "keywords": result.get("keywords", []),
                    "registry_matches": registry_matches,
                    "query_spec_summary": query_spec_summary,
                    "thread_memory_summary": thread_summary,
                    "classification": classification,
                    "classify_formatted_text": classify_formatted_text,
                    "response": final_response,
                    "status": "processed",
                    "message": "Query processed successfully"
                }
                
                self.logger.info(
                    f"Query processed successfully. "
                    f"Thread ID: {thread_id}, "
                    f"Workflow Run ID: {workflow_run_id}, "
                    f"Keywords: {len(result.get('keywords', []))}, "
                    f"Registry Matches: {len(registry_matches)}, "
                    f"Classification: {classification}, "
                    f"Summary: {query_spec_summary[:50] if query_spec_summary else 'None'}..."
                )
                
                return response
                
            except Exception as e:
                # Record exception in span
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                span.set_attribute("error", True)
                
                # Calculate duration
                end_time = datetime.utcnow()
                duration_ms = int((end_time - start_time).total_seconds() * 1000)
                
                # Submit background task to update workflow execution (failed)
                bg_queue.submit("update_workflow_execution", {
                    "workflow_run_id": workflow_run_id,
                    "status": "failed",
                    "error_message": str(e)[:1000],  # Truncate long error messages
                    "completed_at": end_time,
                    "duration_ms": duration_ms
                })
                
                self.logger.error(f"Error processing query: {e}", exc_info=True)
                return {
                    "query": query,
                    "thread_id": thread_id,
                    "workflow_run_id": workflow_run_id,
                    "keywords": [],
                    "registry_matches": [],
                    "query_spec_summary": "",
                    "thread_memory_summary": "Error retrieving memory",
                    "classification": None,
                    "classify_formatted_text": None,
                    "response": None,
                    "status": "error",
                    "message": f"Error processing query: {str(e)}"
                }

