"""Extract keywords node for OCAP graph."""
from typing import Dict, Any, List, Optional
import json
import re
from pathlib import Path
from pprint import pformat
from jinja2 import Environment, FileSystemLoader
from app.ocap.state import OCAPState
from app.core.logging import logger
from app.core.config import settings
from app.core.trace_helpers import trace_node
from app.infra.azure_openai import get_azure_openai_client


def _get_registry_context() -> str:
    """
    Read registry data from JSON file and format it as a pretty-printed string.
    
    Returns:
        Formatted string containing all registry data
    """
    registry_context = {
        "style": [],
        "error": [],
        "defect": [],
        "operation": []
    }
    
    try:
        # Get the registry JSON file path
        current_file = Path(__file__)
        registry_file = current_file.parent.parent / "data" / "registry.json"
        
        if not registry_file.exists():
            logger.warning(f"Registry file not found at {registry_file}, returning empty registry context")
            return pformat(registry_context, width=100, indent=2)
        
        # Read registry data from JSON file
        with open(registry_file, 'r', encoding='utf-8') as f:
            registry_data = json.load(f)
        
        # Extract and sort each registry type
        registry_context["style"] = sorted(registry_data.get("style", []))
        registry_context["error"] = sorted(registry_data.get("error", []))
        registry_context["defect"] = sorted(registry_data.get("defect", []))
        registry_context["operation"] = sorted(registry_data.get("operation", []))
        
        logger.debug(f"Loaded registry context from JSON: {len(registry_context['style'])} styles, "
                    f"{len(registry_context['error'])} errors, "
                    f"{len(registry_context['defect'])} defects, "
                    f"{len(registry_context['operation'])} operations")
        
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing registry JSON file: {e}", exc_info=True)
        return pformat(registry_context, width=100, indent=2)
    except Exception as e:
        logger.error(f"Error reading registry data from JSON file: {e}", exc_info=True)
        return pformat(registry_context, width=100, indent=2)
    
    # Format as pretty-printed string
    formatted_context = pformat(registry_context, width=100, indent=2)
    logger.debug(f"Registry context formatted: {len(formatted_context)} characters")
    
    return formatted_context


def extract_keywords(state: OCAPState) -> Dict[str, Any]:
    """
    Extract keywords from the user query using Azure OpenAI LLM.
    
    Args:
        state: Current OCAP state containing the query
        
    Returns:
        Updated state with extracted keywords
    """
    query = state.get("query", "")
    logger.info(f"Extracting keywords and registry matches from query using LLM: {query}")
    
    try:
        # Get registry context from Redis
        registry_context = _get_registry_context()
        logger.debug("Registry context retrieved successfully")
        
        # Load Jinja2 template
        # Get the prompts directory relative to this file
        current_file = Path(__file__)
        prompts_dir = current_file.parent.parent / "prompts"
        env = Environment(loader=FileSystemLoader(str(prompts_dir)))
        template = env.get_template("extract_keywords.j2")
        
        # Render template with query and registry context
        prompt = template.render(query=query, registry_context=registry_context)
        
        # Get Azure OpenAI client
        client = get_azure_openai_client()
        
        # Get deployment name from settings - this is required for Azure OpenAI
        deployment = settings.azure_openai_deployment
        if not deployment:
            logger.error("AZURE_OPENAI_DEPLOYMENT is not set in environment variables")
            raise ValueError("Azure OpenAI deployment name is required. Set AZURE_OPENAI_DEPLOYMENT in .env file")
        
        logger.debug(f"Calling Azure OpenAI with deployment: {deployment}")
        
        # Call Azure OpenAI - matching reference code structure
        response = client.chat.completions.create(
            model=deployment,
            messages=[
                {
                    "role": "system",
                    "content": "You are an expert at extracting keywords and identifying registry matches. Always respond with valid JSON only.",
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
        )
        
        # Extract response content
        llm_output = response.choices[0].message.content.strip()
        logger.debug(f"LLM output: {llm_output}")
        
        # Parse JSON response
        # Try to extract JSON if there's extra text
        try:
            # Try parsing directly
            result = json.loads(llm_output)
        except json.JSONDecodeError:
            # Try to extract JSON from markdown code blocks or text
            # Look for the full JSON structure
            json_match = re.search(r'\{.*\}', llm_output, re.DOTALL)
            if json_match:
                try:
                    result = json.loads(json_match.group())
                except json.JSONDecodeError:
                    # Try to find just the keywords array as fallback
                    keywords_match = re.search(r'"keywords"\s*:\s*\[[^\]]+\]', llm_output, re.DOTALL)
                    if keywords_match:
                        # Extract just keywords for fallback
                        keywords_str = keywords_match.group()
                        keywords = json.loads("{" + keywords_str + "}").get("keywords", [])
                        result = {
                            "keywords": keywords,
                            "registry_matches": [],
                            "query_spec_summary": ""
                        }
                    else:
                        raise ValueError("Could not parse LLM response as JSON")
            else:
                raise ValueError("Could not find JSON in LLM response")
        
        # Extract and validate results
        keywords = result.get("keywords", [])
        registry_matches = result.get("registry_matches", [])
        query_spec_summary = result.get("query_spec_summary", "")
        
        # Validate keywords is a list
        if not isinstance(keywords, list):
            logger.warning(f"Keywords is not a list: {keywords}, converting...")
            keywords = [str(k) for k in keywords] if keywords else []
        
        # Validate registry_matches is a list
        if not isinstance(registry_matches, list):
            logger.warning(f"Registry matches is not a list: {registry_matches}, converting...")
            registry_matches = []
        
        # Validate each registry match has required fields
        validated_matches = []
        for match in registry_matches:
            if isinstance(match, dict):
                validated_match = {
                    "node_type": match.get("node_type", ""),
                    "value": match.get("value", ""),
                    "match_type": match.get("match_type", "partial"),
                    "confidence": int(match.get("confidence", 0))
                }
                # Only add if node_type and value are present
                if validated_match["node_type"] and validated_match["value"]:
                    validated_matches.append(validated_match)
        
        logger.info(
            f"Extracted {len(keywords)} keywords, {len(validated_matches)} registry matches using LLM. "
            f"Summary: {query_spec_summary[:100] if query_spec_summary else 'None'}..."
        )
        
        # Get existing metadata or create new
        existing_metadata = state.get("metadata") or {}
        metadata = dict(existing_metadata) if existing_metadata else {}
        
        # Store registry matches and query spec summary in metadata
        metadata["registry_matches"] = validated_matches
        metadata["query_spec_summary"] = query_spec_summary
        
        return {
            "keywords": keywords,
            "metadata": metadata
        }
        
    except Exception as e:
        logger.error(f"Error extracting keywords and registry matches with LLM: {e}", exc_info=True)
        # Fallback: return empty structure on error
        existing_metadata = state.get("metadata") or {}
        metadata = dict(existing_metadata) if existing_metadata else {}
        metadata["registry_matches"] = []
        metadata["query_spec_summary"] = ""
        
        return {
            "keywords": [],
            "metadata": metadata
        }

