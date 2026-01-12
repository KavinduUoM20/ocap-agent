"""Analyze query node for OCAP graph."""
from typing import Dict, Any, List, Optional
import json
import re
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
from app.ocap.state import OCAPState
from app.core.logging import logger
from app.core.config import settings
from app.core.trace_helpers import trace_node
from app.infra.azure_openai import get_azure_openai_client


def _classify_with_llm(
    query: str,
    query_spec_summary: str,
    registry_matches: List[Dict[str, Any]],
    thread_memory_summary: Optional[str] = None,
    historical_registry_matches: Optional[List[Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Use LLM to classify query into one of the categories: precise, error-precise, non-precise, or generic.
    
    Args:
        query: Original user query
        query_spec_summary: Query specification summary from extract node
        registry_matches: List of registry matches from current query
        thread_memory_summary: Optional thread memory summary from memory node
        historical_registry_matches: Optional list of historical registry matches from previous turns
        
    Returns:
        Dictionary with classification and reasoning
    """
    try:
        # Load Jinja2 template
        current_file = Path(__file__)
        prompts_dir = current_file.parent.parent / "prompts"
        env = Environment(loader=FileSystemLoader(str(prompts_dir)))
        template = env.get_template("analyze_query.j2")
        
        # Render template
        prompt = template.render(
            query=query,
            query_spec_summary=query_spec_summary,
            registry_matches=registry_matches,
            thread_memory_summary=thread_memory_summary,
            historical_registry_matches=historical_registry_matches or []
        )
        
        # Get Azure OpenAI client
        client = get_azure_openai_client()
        deployment = settings.azure_openai_deployment
        if not deployment:
            logger.error("AZURE_OPENAI_DEPLOYMENT is not set in environment variables")
            raise ValueError("Azure OpenAI deployment name is required")
        
        logger.debug(f"Calling Azure OpenAI for query classification with deployment: {deployment}")
        
        # Call Azure OpenAI
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at classifying manufacturing queries based on registry matches and context. Always respond with valid JSON only.",
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
        )
        
        # Extract response content
        llm_output = response.choices[0].message.content.strip()
        logger.debug(f"LLM classification output: {llm_output}")
        
        # Parse JSON response
        try:
            result = json.loads(llm_output)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code blocks or text
            json_match = re.search(r'\{.*\}', llm_output, re.DOTALL)
            if json_match:
                result = json.loads(json_match.group())
            else:
                logger.warning("Could not parse LLM response, defaulting to non-precise")
                return {
                    "classification": "non-precise",
                    "reasoning": "Could not parse LLM response"
                }
        
        classification = result.get("classification", "non-precise")
        reasoning = result.get("reasoning", "")
        
        logger.info(f"LLM classification: {classification}, reasoning: {reasoning[:100]}...")
        
        return {
            "classification": classification,
            "reasoning": reasoning
        }
        
    except Exception as e:
        logger.error(f"Error classifying with LLM: {e}", exc_info=True)
        # Fallback to non-precise if LLM fails
        return {
            "classification": "non-precise",
            "reasoning": f"LLM classification failed: {str(e)}"
        }


@trace_node("analyze_query")
def analyze_query(state: OCAPState) -> Dict[str, Any]:
    """
    Analyze query and classify it based on registry matches and query content.
    
    Classification types:
    - precise: 3 node types (defect, operation, style) all with confidence 100
    - error-precise: node type error with confidence 100
    - non-precise: 1-2 nodes recognized, confidence can be < 100
    - generic: no registry matches but manufacturing technical query
    
    Args:
        state: Current OCAP state containing query, registry_matches, query_spec_summary
        
    Returns:
        Updated state with classification and analysis metadata
    """
    query = state.get("query", "")
    logger.info(f"Analyzing query for classification: {query[:50]}...")
    
    # Get registry matches, query spec summary, thread memory, and historical matches from metadata
    metadata = state.get("metadata") or {}
    registry_matches = metadata.get("registry_matches", [])
    query_spec_summary = metadata.get("query_spec_summary", "")
    thread_memory_summary = metadata.get("thread_memory_summary")
    historical_registry_matches = metadata.get("historical_registry_matches", [])
    
    logger.debug(
        f"Registry matches: {len(registry_matches)}, "
        f"Query spec summary: {query_spec_summary[:50] if query_spec_summary else 'None'}..., "
        f"Thread memory: {'Available' if thread_memory_summary else 'None'}, "
        f"Historical registry matches: {len(historical_registry_matches)} workflow(s)"
    )
    
    try:
        # Use LLM for all classifications
        logger.info("Classifying query using LLM with all context")
        llm_result = _classify_with_llm(
            query=query,
            query_spec_summary=query_spec_summary,
            registry_matches=registry_matches,
            thread_memory_summary=thread_memory_summary,
            historical_registry_matches=historical_registry_matches
        )
        
        classification = llm_result.get("classification", "non-precise")
        reasoning = llm_result.get("reasoning", "")
        
        # Build analysis metadata
        analysis_metadata = {
            "classification": classification,
            "reasoning": reasoning,
            "registry_match_count": len(registry_matches),
            "node_types_found": list(set(m.get("node_type", "") for m in registry_matches if m.get("node_type"))),
            "confidence_scores": [m.get("confidence", 0) for m in registry_matches],
            "classification_method": "llm-based",
            "thread_memory_used": thread_memory_summary is not None,
            "historical_registry_matches_count": len(historical_registry_matches) if historical_registry_matches else 0,
            "historical_context_used": len(historical_registry_matches) > 0 if historical_registry_matches else False
        }
        
        # Add detailed match breakdown
        if registry_matches:
            analysis_metadata["matches_by_type"] = {}
            for match in registry_matches:
                node_type = match.get("node_type", "")
                if node_type:
                    if node_type not in analysis_metadata["matches_by_type"]:
                        analysis_metadata["matches_by_type"][node_type] = []
                    analysis_metadata["matches_by_type"][node_type].append({
                        "value": match.get("value", ""),
                        "confidence": match.get("confidence", 0),
                        "match_type": match.get("match_type", "partial")
                    })
        
        logger.info(
            f"Query classified as: {classification} "
            f"(method: {analysis_metadata['classification_method']}, "
            f"matches: {len(registry_matches)}, "
            f"node_types: {analysis_metadata['node_types_found']}, "
            f"historical_workflows: {analysis_metadata['historical_registry_matches_count']}, "
            f"reasoning: {reasoning[:200]}...)"
        )
        
        # Log full reasoning for debugging
        logger.debug(f"Full classification reasoning: {reasoning}")
        
        # Update metadata with analysis results
        updated_metadata = dict(metadata)
        updated_metadata["analysis"] = analysis_metadata
        
        return {
            "classification": classification,
            "metadata": updated_metadata
        }
        
    except Exception as e:
        logger.error(f"Error analyzing query: {e}", exc_info=True)
        # Fallback: classify as non-precise on error
        updated_metadata = dict(metadata)
        updated_metadata["analysis"] = {
            "classification": "non-precise",
            "classification_method": "error-fallback",
            "error": str(e)
        }
        
        return {
            "classification": "non-precise",
            "metadata": updated_metadata
        }

