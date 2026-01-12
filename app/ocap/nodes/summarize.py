"""Summarize node for OCAP graph - generates final response based on classification and query results."""
from typing import Dict, Any, Optional
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from app.ocap.state import OCAPState
from app.core.logging import logger
from app.core.config import settings
from app.core.trace_helpers import trace_node
from app.infra.azure_openai import get_azure_openai_client


@trace_node("summarize")
def summarize(state: OCAPState) -> Dict[str, Any]:
    """
    Summarize node that generates a final response based on classification and query results.
    
    This node analyzes:
    - The user's query
    - Classification (precise, error-precise, non-precise, generic)
    - Formatted Elasticsearch results
    - Registry matches
    - Query specification summary
    - Thread memory (conversation context)
    
    It determines if information is sufficient to answer the question, and either:
    - Provides a clear answer if information is sufficient
    - Asks the user to clarify if information is lacking
    
    Args:
        state: Current OCAP state containing query, classification, formatted_text, etc.
        
    Returns:
        Updated state with response field containing the generated summary
    """
    query = state.get("query", "")
    classification = state.get("classification")
    metadata = state.get("metadata") or {}
    
    # Extract information from metadata
    query_spec_summary = metadata.get("query_spec_summary", "")
    thread_memory_summary = metadata.get("thread_memory_summary")
    classify_results = metadata.get("classify", {})
    classify_formatted_text = classify_results.get("formatted_text", "")
    
    # Get classification_registry from analysis metadata (merged registry if merge was applied)
    analysis_metadata = metadata.get("analysis", {})
    classification_registry = analysis_metadata.get("classification_registry")
    merge_applied = analysis_metadata.get("merge_applied", False)
    merge_reasoning = analysis_metadata.get("merge_reasoning", "")
    analysis_reasoning = analysis_metadata.get("reasoning", "")
    
    # Fallback to registry_matches if classification_registry not available
    if not classification_registry:
        classification_registry = metadata.get("registry_matches", [])
    
    logger.info(
        f"Generating summary response for query: {query[:50]}... "
        f"(classification: {classification}, "
        f"thread_memory: {'Available' if thread_memory_summary else 'None'}, "
        f"merge_applied: {merge_applied}, "
        f"classification_registry_count: {len(classification_registry) if classification_registry else 0})"
    )
    
    if not classification:
        logger.warning("No classification found in state, generating generic response")
        return {
            "response": "I need more information to help you. Could you please provide more details about your manufacturing query?"
        }
    
    try:
        # Load Jinja2 template
        current_file = Path(__file__)
        prompts_dir = current_file.parent.parent / "prompts"
        env = Environment(loader=FileSystemLoader(str(prompts_dir)))
        template = env.get_template("summarize.j2")
        
        # Render template
        prompt = template.render(
            query=query,
            classification=classification,
            query_spec_summary=query_spec_summary,
            classification_registry=classification_registry,
            merge_applied=merge_applied,
            merge_reasoning=merge_reasoning,
            analysis_reasoning=analysis_reasoning,
            thread_memory_summary=thread_memory_summary,
            classify_formatted_text=classify_formatted_text
        )
        
        # Get Azure OpenAI client
        client = get_azure_openai_client()
        deployment = settings.azure_openai_deployment
        if not deployment:
            logger.error("AZURE_OPENAI_DEPLOYMENT is not set in environment variables")
            raise ValueError("Azure OpenAI deployment name is required")
        
        logger.debug(f"Calling Azure OpenAI for summary generation with deployment: {deployment}")
        
        # Call Azure OpenAI
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert manufacturing assistant. Generate a helpful, clear, and CONCISE response. CRITICAL RULES: (1) Use proper terminology: 'errors', 'defects', 'operations', 'styles', 'actions' - NOT 'issues', 'problems', 'things', 'items'. (2) When listing items, clearly state the type (e.g., 'The following errors:', 'Available defects:'). (3) When multiple options exist, ask which one is relevant (e.g., 'Which error is affecting you?'). (4) Always include actions when available - users seek actionable help. (5) Use analysis metadata (merge_applied) to determine if acknowledgment is needed. (6) Use numbered lists with case counts. Be precise and brief - remove verbose phrases."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.5,  # Lower temperature for more focused, precise responses
            max_tokens=600,  # Reduced for more concise responses while maintaining structure
        )
        
        # Extract response content
        summary_response = response.choices[0].message.content.strip()
        logger.debug(f"Generated summary response: {summary_response[:100]}...")
        
        logger.info(
            f"Summary response generated successfully. "
            f"Length: {len(summary_response)} characters, "
            f"Classification: {classification}"
        )
        
        # Update metadata with summary generation info
        updated_metadata = dict(metadata)
        updated_metadata["summarize"] = {
            "response_length": len(summary_response),
            "classification": classification,
            "has_classify_results": bool(classify_formatted_text)
        }
        
        return {
            "response": summary_response,
            "metadata": updated_metadata
        }
        
    except Exception as e:
        logger.error(f"Error generating summary: {e}", exc_info=True)
        # Return a fallback response
        fallback_response = (
            "I encountered an issue processing your query. "
            "Could you please rephrase your question or provide more details?"
        )
        
        updated_metadata = dict(metadata)
        updated_metadata["summarize"] = {
            "error": str(e),
            "fallback_used": True
        }
        
        return {
            "response": fallback_response,
            "metadata": updated_metadata
        }

