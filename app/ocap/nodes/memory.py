"""Summarize thread memory node for OCAP graph."""
from typing import Dict, Any, List
from pathlib import Path
import json
from jinja2 import Environment, FileSystemLoader
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from app.ocap.state import OCAPState
from app.core.logging import logger
from app.core.config import settings
from app.core.trace_helpers import trace_node
from app.core.tracing import get_tracer
from app.infra.redis import get_redis_client, is_redis_available
from app.infra.azure_openai import get_azure_openai_client

tracer = get_tracer()


@trace_node("summarize_thread_memory")
def summarize_thread_memory(state: OCAPState) -> Dict[str, Any]:
    """
    Summarize thread memory from Redis by retrieving all workflow runs for the thread
    and using LLM to create a contextual summary.
    
    This node:
    1. Retrieves all workflow_run_ids for the current thread from Redis
    2. Fetches each workflow state (query, response, formatted_text) from Redis
    3. Sends all workflow interactions to LLM for summarization
    4. Returns a formatted summary for use in the analyze phase
    
    Args:
        state: Current OCAP state containing the query
        
    Returns:
        Updated state with thread memory summary
    """
    query = state.get("query", "")
    logger.info(f"Summarizing thread memory for query: {query}")
    
    # Initialize metadata - handle None case properly
    existing_metadata = state.get("metadata")
    if existing_metadata is None:
        metadata: Dict[str, Any] = {}
    else:
        # Create a copy to avoid mutating the original
        metadata = dict(existing_metadata)
    
    try:
        # Check if Redis is available before attempting to use it
        if not is_redis_available():
            logger.debug("Redis is not available, skipping thread memory retrieval")
            metadata["thread_memory_summary"] = None
            metadata["thread_memory_available"] = False
            return {"metadata": metadata}
        
        redis_client = get_redis_client()
        if redis_client is None:
            logger.debug("Redis client is None, skipping thread memory retrieval")
            metadata["thread_memory_summary"] = None
            metadata["thread_memory_available"] = False
            return {"metadata": metadata}
        
        # Get thread_id from metadata (provided by user) or generate one
        thread_id = metadata.get("thread_id")
        user_id = metadata.get("user_id")
        
        if not thread_id:
            # Fallback: generate thread_id based on query and user_id
            if user_id:
                thread_id = f"user_{user_id}_thread_{hash(query) % 10000}"
            else:
                thread_id = f"thread_{hash(query) % 10000}"
            logger.debug(f"Generated thread_id: {thread_id}")
        
        # Get list of workflow_run_ids for this thread with explicit tracing
        thread_workflows_key = f"thread:{thread_id}:workflows"
        with tracer.start_as_current_span("redis.lrange") as span:
            span.set_attribute("redis.key", thread_workflows_key)
            span.set_attribute("redis.command", "LRANGE")
            span.set_attribute("thread.id", thread_id)
            try:
                logger.info(f"Redis LRANGE: key={thread_workflows_key}, thread_id={thread_id}")
                workflow_run_ids = redis_client.lrange(thread_workflows_key, 0, -1)  # Get all workflow IDs
                span.set_attribute("redis.result_count", len(workflow_run_ids) if workflow_run_ids else 0)
                span.set_status(Status(StatusCode.OK))
                logger.info(f"Redis LRANGE result: found {len(workflow_run_ids) if workflow_run_ids else 0} workflow IDs")
            except Exception as e:
                span.record_exception(e)
                span.set_status(Status(StatusCode.ERROR, str(e)))
                logger.error(f"Redis LRANGE failed: {e}", exc_info=True)
                raise
        
        if not workflow_run_ids:
            logger.info(f"No previous workflow runs found for thread: {thread_id}")
            metadata["thread_memory_summary"] = None
            metadata["thread_memory_available"] = False
            metadata["workflow_count"] = 0
            metadata["historical_registry_matches"] = []
            return {"metadata": metadata}
        
        logger.info(f"Found {len(workflow_run_ids)} previous workflow runs for thread: {thread_id}")
        
        # Retrieve workflow states from Redis with explicit tracing
        workflow_interactions: List[Dict[str, Any]] = []
        historical_registry_matches: List[Dict[str, Any]] = []
        
        for workflow_run_id in workflow_run_ids:
            workflow_key = f"workflow:{workflow_run_id}"
            with tracer.start_as_current_span("redis.get") as span:
                span.set_attribute("redis.key", workflow_key)
                span.set_attribute("redis.command", "GET")
                span.set_attribute("workflow.run_id", workflow_run_id)
                try:
                    logger.debug(f"Redis GET: key={workflow_key}, workflow_run_id={workflow_run_id}")
                    workflow_data_str = redis_client.get(workflow_key)
                    span.set_attribute("redis.key_found", workflow_data_str is not None)
                    span.set_status(Status(StatusCode.OK))
                    if workflow_data_str:
                        logger.debug(f"Redis GET result: found data for workflow {workflow_run_id}")
                    else:
                        logger.debug(f"Redis GET result: no data found for workflow {workflow_run_id}")
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    logger.warning(f"Redis GET failed for {workflow_key}: {e}")
                    continue
            
            if workflow_data_str:
                try:
                    workflow_data = json.loads(workflow_data_str)
                    
                    # Extract relevant information for LLM summarization
                    # Only include query, response, and classification (no Elasticsearch results)
                    interaction = {
                        "query": workflow_data.get("query", ""),
                        "response": workflow_data.get("response", ""),
                        "classification": workflow_data.get("classification")
                    }
                    
                    # Extract registry_matches from metadata for historical tracking
                    workflow_metadata = workflow_data.get("metadata", {})
                    registry_matches = workflow_metadata.get("registry_matches", [])
                    
                    # Build historical registry matches entry (most recent first)
                    if registry_matches and isinstance(registry_matches, list):
                        historical_entry = {
                            "workflow_run_id": workflow_run_id,
                            "query": workflow_data.get("query", ""),
                            "response": workflow_data.get("response", ""),
                            "classification": workflow_data.get("classification"),
                            "registry_matches": registry_matches,
                            "created_at": workflow_data.get("created_at", "")
                        }
                        historical_registry_matches.append(historical_entry)
                        logger.debug(
                            f"Extracted {len(registry_matches)} registry matches from workflow {workflow_run_id}"
                        )
                    
                    # Only add interaction if we have at least a query
                    if interaction["query"]:
                        workflow_interactions.append(interaction)
                        
                except json.JSONDecodeError as e:
                    logger.warning(f"Failed to parse workflow data for {workflow_run_id}: {e}")
                    continue
                except Exception as e:
                    logger.warning(f"Error processing workflow {workflow_run_id}: {e}")
                    continue
        
        if not workflow_interactions:
            logger.info("No valid workflow interactions found to summarize")
            metadata["thread_memory_summary"] = None
            metadata["thread_memory_available"] = False
            metadata["workflow_count"] = 0
            metadata["historical_registry_matches"] = []
            return {"metadata": metadata}
        
        logger.info(f"Retrieved {len(workflow_interactions)} workflow interactions for summarization")
        
        # Use LLM to summarize all workflow interactions
        try:
            # Load Jinja2 template
            current_file = Path(__file__)
            prompts_dir = current_file.parent.parent / "prompts"
            env = Environment(loader=FileSystemLoader(str(prompts_dir)))
            template = env.get_template("summarize_thread_memory.j2")
            
            # Render template with workflow interactions
            prompt = template.render(workflow_interactions=workflow_interactions)
            
            # Get Azure OpenAI client
            client = get_azure_openai_client()
            deployment = settings.azure_openai_deployment
            if not deployment:
                logger.error("AZURE_OPENAI_DEPLOYMENT is not set in environment variables")
                raise ValueError("Azure OpenAI deployment name is required")
            
            logger.debug(f"Calling Azure OpenAI for thread memory summarization with deployment: {deployment}")
            
            # Call Azure OpenAI to summarize
            response = client.chat.completions.create(
                model=deployment,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert at summarizing conversation history and workflow interactions in a manufacturing context. Create concise, informative summaries that capture the essence of previous interactions."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.3,  # Lower temperature for more consistent summarization
                max_tokens=800,  # Allow enough tokens for a comprehensive summary
            )
            
            # Extract summary response
            summary = response.choices[0].message.content.strip()
            logger.info(f"Generated thread memory summary: {summary[:100]}...")
            
            # Store summary in metadata
            metadata["thread_memory_summary"] = summary
            metadata["thread_memory_available"] = True
            metadata["workflow_count"] = len(workflow_interactions)
            metadata["thread_id"] = thread_id
            
            # Store historical registry matches in structured format (most recent first)
            # Format: List of workflow runs with their registry_matches
            if historical_registry_matches:
                metadata["historical_registry_matches"] = historical_registry_matches
                total_historical_matches = sum(
                    len(entry.get("registry_matches", [])) 
                    for entry in historical_registry_matches
                )
                logger.info(
                    f"Stored {len(historical_registry_matches)} historical workflow entries "
                    f"with {total_historical_matches} total registry matches"
                )
            else:
                metadata["historical_registry_matches"] = []
                logger.debug("No historical registry matches found in previous workflows")
            
            return {
                "metadata": metadata
            }
            
        except Exception as e:
            logger.error(f"Error generating thread memory summary with LLM: {e}", exc_info=True)
            # Fallback: create a simple text summary without LLM
            fallback_summary_parts = []
            for i, interaction in enumerate(workflow_interactions, 1):
                fallback_summary_parts.append(
                    f"Interaction {i}: Query: {interaction['query'][:100]}... "
                    f"Response: {interaction['response'][:100] if interaction['response'] else 'N/A'}..."
                )
            fallback_summary = "Previous interactions:\n" + "\n".join(fallback_summary_parts)
            
            metadata["thread_memory_summary"] = fallback_summary
            metadata["thread_memory_available"] = True
            metadata["workflow_count"] = len(workflow_interactions)
            metadata["thread_id"] = thread_id
            metadata["summary_fallback"] = True
            
            # Store historical registry matches even in fallback case
            if historical_registry_matches:
                metadata["historical_registry_matches"] = historical_registry_matches
                total_historical_matches = sum(
                    len(entry.get("registry_matches", [])) 
                    for entry in historical_registry_matches
                )
                logger.info(
                    f"Stored {len(historical_registry_matches)} historical workflow entries "
                    f"with {total_historical_matches} total registry matches (fallback mode)"
                )
            else:
                metadata["historical_registry_matches"] = []
            
            return {
                "metadata": metadata
            }
        
    except Exception as e:
        logger.error(f"Error summarizing thread memory: {e}", exc_info=True)
        # On error, store error info but don't fail the entire graph
        metadata["thread_memory_summary"] = None
        metadata["thread_memory_available"] = False
        metadata["thread_memory_error"] = str(e)
        metadata["historical_registry_matches"] = []
        return {
            "metadata": metadata
        }

