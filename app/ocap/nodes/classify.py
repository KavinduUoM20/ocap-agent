"""Classify node for OCAP graph - queries Elasticsearch based on classification."""
from typing import Dict, Any, List, Optional
from app.ocap.state import OCAPState
from app.core.logging import logger
from app.core.trace_helpers import trace_node
from app.infra.elastic import get_elasticsearch_client


def normalize_value(val):
    """
    Normalizes user / LLM inputs to match index-time normalization.
    """
    if val is None:
        return ""
    v = str(val).strip()
    if v.lower() in ["nan", "none", "null"]:
        return ""
    return v


def get_full_rows(
    client,
    index_name,
    defect,
    operation_candidates,
    style,
    size=50
):
    """
    Retrieves full rows (excluding `content`) matching defect, operation(s), and style.
    Function signature and calling style remain unchanged.
    """

    # Normalize inputs
    defect = normalize_value(defect)
    style = normalize_value(style)

    if isinstance(operation_candidates, str):
        operation_candidates = [operation_candidates]

    operation_candidates = [
        normalize_value(op) for op in operation_candidates if normalize_value(op)
    ]

    # Build filters
    filters = [{"term": {"defect": defect}}]

    if operation_candidates:
        filters.append({"terms": {"operation": operation_candidates}})

    # Style handling (empty string means missing style)
    if style:
        filters.append({"term": {"style": style}})
    else:
        filters.append({"term": {"style": ""}})

    # Execute query (exclude `content`)
    response = client.search(
        index=index_name,
        query={"bool": {"filter": filters}},
        _source={"excludes": ["content"]},
        size=size
    )

    # Return rows without `content`
    return [
        hit["_source"]
        for hit in response["hits"]["hits"]
    ]


def get_rows_by_error(
    client,
    index_name,
    error,
    size=50
):
    """
    Retrieves full rows based ONLY on error.
    """

    error = normalize_value(error)

    response = client.search(
        index=index_name,
        query={
            "bool": {
                "filter": [
                    {"term": {"error": error}}
                ]
            }
        },
        _source={"excludes": ["content"]},
        size=size
    )

    return [
        hit["_source"]
        for hit in response["hits"]["hits"]
    ]


def get_rows_by_error_and_defect(
    client,
    index_name,
    error,
    defect,
    size=50
):
    """
    Retrieves full rows matching error AND defect.
    This is used when we have error + defect combination (e.g., user selects defect from error query).
    
    Args:
        client: Elasticsearch client
        index_name: Name of the index to query
        error: Error value to filter by
        defect: Defect value to filter by
        size: Maximum number of results to return
        
    Returns:
        List of matching row dictionaries
    """
    error = normalize_value(error)
    defect = normalize_value(defect)
    
    filters = []
    if error:
        filters.append({"term": {"error": error}})
    if defect:
        filters.append({"term": {"defect": defect}})
    
    if not filters:
        return []
    
    response = client.search(
        index=index_name,
        query={"bool": {"filter": filters}},
        _source={"excludes": ["content"]},
        size=size
    )
    
    return [
        hit["_source"]
        for hit in response["hits"]["hits"]
    ]


def get_rows_with_error(
    client,
    index_name,
    defect,
    operation_candidates,
    style,
    error,
    size=50
):
    """
    Retrieves full rows matching defect, operation(s), style, AND error.
    This is used when we have a merged context (defect + operation + style + error).
    
    Args:
        client: Elasticsearch client
        index_name: Name of the index to query
        defect: Defect value to filter by
        operation_candidates: List of operation values to filter by
        style: Style value to filter by
        error: Error value to filter by
        size: Maximum number of results to return
        
    Returns:
        List of matching row dictionaries
    """
    # Normalize inputs
    defect = normalize_value(defect)
    style = normalize_value(style)
    error = normalize_value(error)

    if isinstance(operation_candidates, str):
        operation_candidates = [operation_candidates]

    operation_candidates = [
        normalize_value(op) for op in operation_candidates if normalize_value(op)
    ]

    # Build filters - include defect, operation, style, AND error
    filters = []
    
    if defect:
        filters.append({"term": {"defect": defect}})
    
    if operation_candidates:
        filters.append({"terms": {"operation": operation_candidates}})
    
    # Style handling (empty string means missing style)
    if style:
        filters.append({"term": {"style": style}})
    else:
        filters.append({"term": {"style": ""}})
    
    if error:
        filters.append({"term": {"error": error}})

    # Execute query (exclude `content`)
    response = client.search(
        index=index_name,
        query={"bool": {"filter": filters}},
        _source={"excludes": ["content"]},
        size=size
    )

    # Return rows without `content`
    return [
        hit["_source"]
        for hit in response["hits"]["hits"]
    ]


def build_relationship_search_query(node_queries, size=10):
    """
    Builds an Elasticsearch query for relationship index based on node queries.
    
    Args:
        node_queries: List of node query dictionaries with:
            - node_type: Type of node (e.g., "operation", "defect", "style", "error")
            - value: The value to search for
            - match_type: "exact" or "partial"
            - confidence: Confidence score (optional)
        size: Maximum number of results to return
        
    Returns:
        Elasticsearch query dictionary
    """
    should_clauses = []

    for node in node_queries:
        node_type = node.get("node_type")
        value = node.get("value")
        match_type = node.get("match_type", "partial")

        if not node_type or not value:
            continue

        must_conditions = [
            {"term": {"node_type": node_type}}
        ]

        if match_type == "exact":
            must_conditions.append(
                {"term": {"name": value}}
            )
        else:
            must_conditions.append(
                {
                    "match": {
                        "name.text": {
                            "query": value,
                            "operator": "and"
                        }
                    }
                }
            )

        should_clauses.append({
            "bool": {
                "must": must_conditions
            }
        })

    return {
        "size": size,
        "query": {
            "bool": {
                "should": should_clauses,
                "minimum_should_match": 1
            }
        }
    }


def get_rows_non_precise(
    client,
    index_name,
    registry_matches,
    size=50
):
    """
    Retrieves rows for non-precise queries using relationship index.
    
    Args:
        client: Elasticsearch client
        index_name: Name of the relationship index
        registry_matches: List of registry match dictionaries with node_type, value, match_type, confidence
        size: Maximum number of results to return
        
    Returns:
        List of relationship node dictionaries
    """
    if not registry_matches:
        logger.warning("No registry matches provided for non-precise query")
        return []
    
    # Build node_queries from registry_matches
    # Filter out matches that don't have required fields
    node_queries = []
    for match in registry_matches:
        node_type = match.get("node_type")
        value = match.get("value")
        
        if node_type and value:
            node_queries.append({
                "node_type": node_type,
                "value": value,
                "match_type": match.get("match_type", "partial"),
                "confidence": match.get("confidence", 0)
            })
    
    if not node_queries:
        logger.warning("No valid node queries extracted from registry_matches")
        return []
    
    logger.debug(f"Building relationship search query with {len(node_queries)} node queries")
    
    # Build the query
    query_body = build_relationship_search_query(node_queries, size=size)
    
    # Execute the search
    # Note: Using body parameter for compatibility with the query structure
    # In Elasticsearch 8.x, body parameter is still supported
    response = client.search(
        index=index_name,
        body=query_body
    )
    
    # Extract hits
    hits = [hit["_source"] for hit in response["hits"]["hits"]]
    
    return hits


def _analyze_result_quality(
    query_results: List[Dict[str, Any]],
    query_method: str,
    has_defect: bool,
    has_operation: bool,
    has_style: bool,
    has_error: bool,
    filled_slots: Dict[str, List[str]]
) -> Dict[str, Any]:
    """
    Analyze query results to determine response strategy.
    
    This is critical: Classification is about QUERY strategy (which index, which parameters),
    but RESPONSE strategy depends on what the results contain and whether we can give a direct answer.
    
    Args:
        query_results: List of query results from Elasticsearch
        query_method: The query method used (get_full_rows, get_rows_by_error, etc.)
        has_defect, has_operation, has_style, has_error: What parameters were used in query
        filled_slots: What slots are already filled in the conversation
        
    Returns:
        Dictionary with response strategy information:
        - strategy: "direct_answer" | "list_options" | "ask_clarification" | "no_results"
        - can_direct_answer: bool - Can we give a direct answer?
        - needs_clarification: bool - Do we need to ask for more info?
        - clarification_type: str - What type of clarification is needed (error, defect, etc.)
        - unique_values: Dict[str, List[str]] - Unique values found in results for each node type
        - has_actions: bool - Do results contain actions?
    """
    if not query_results:
        return {
            "strategy": "no_results",
            "can_direct_answer": False,
            "needs_clarification": True,
            "clarification_type": None,
            "unique_values": {},
            "has_actions": False,
            "reasoning": "No results found in knowledge base"
        }
    
    # Extract unique values from results for each node type
    unique_values = {
        "defect": [],
        "operation": [],
        "style": [],
        "error": [],
        "action": []
    }
    
    has_actions = False
    
    for result in query_results:
        # Extract unique defects
        if result.get("defect") and result["defect"] not in unique_values["defect"]:
            unique_values["defect"].append(result["defect"])
        
        # Extract unique operations
        if result.get("operation") and result["operation"] not in unique_values["operation"]:
            unique_values["operation"].append(result["operation"])
        
        # Extract unique styles
        if result.get("style") and result["style"] not in unique_values["style"]:
            unique_values["style"].append(result["style"])
        
        # Extract unique errors
        if result.get("error") and result["error"] not in unique_values["error"]:
            unique_values["error"].append(result["error"])
        
        # Check for actions
        if result.get("action") or result.get("actions"):
            has_actions = True
            if result.get("action"):
                if result["action"] not in unique_values["action"]:
                    unique_values["action"].append(result["action"])
    
    # Determine response strategy based on results and filled slots
    # Key insight: Can we give a direct answer, or do we need to list options/ask for clarification?
    
    # Case 1: All 4 parameters specified (defect + operation + style + error)
    if has_defect and has_operation and has_style and has_error:
        # If we have results and actions, we can give direct answer
        if has_actions and len(query_results) > 0:
            return {
                "strategy": "direct_answer",
                "can_direct_answer": True,
                "needs_clarification": False,
                "clarification_type": None,
                "unique_values": unique_values,
                "has_actions": True,
                "reasoning": "All 4 parameters specified, results contain actions - can provide direct answer"
            }
        # If we have results but no actions, still provide what we have
        elif len(query_results) > 0:
            return {
                "strategy": "direct_answer",
                "can_direct_answer": True,
                "needs_clarification": False,
                "clarification_type": None,
                "unique_values": unique_values,
                "has_actions": False,
                "reasoning": "All 4 parameters specified, results available - provide available information"
            }
        else:
            return {
                "strategy": "no_results",
                "can_direct_answer": False,
                "needs_clarification": True,
                "clarification_type": None,
                "unique_values": unique_values,
                "has_actions": False,
                "reasoning": "All 4 parameters specified but no results found"
            }
    
    # Case 2: 3 parameters specified (defect + operation + style) - "precise" query
    elif has_defect and has_operation and has_style:
        # Check if results have multiple errors
        unique_errors = unique_values["error"]
        if len(unique_errors) == 0:
            # No errors in results - might have direct actions
            if has_actions:
                return {
                    "strategy": "direct_answer",
                    "can_direct_answer": True,
                    "needs_clarification": False,
                    "clarification_type": None,
                    "unique_values": unique_values,
                    "has_actions": True,
                    "reasoning": "3 parameters specified, results contain actions without errors - can provide direct answer"
                }
            else:
                return {
                    "strategy": "list_options",
                    "can_direct_answer": False,
                    "needs_clarification": True,
                    "clarification_type": "error",
                    "unique_values": unique_values,
                    "has_actions": False,
                    "reasoning": "3 parameters specified but no errors/actions in results - need to clarify"
                }
        elif len(unique_errors) == 1:
            # Single error - can provide direct answer
            if has_actions:
                return {
                    "strategy": "direct_answer",
                    "can_direct_answer": True,
                    "needs_clarification": False,
                    "clarification_type": None,
                    "unique_values": unique_values,
                    "has_actions": True,
                    "reasoning": "3 parameters specified, single error found with actions - can provide direct answer"
                }
            else:
                return {
                    "strategy": "direct_answer",
                    "can_direct_answer": True,
                    "needs_clarification": False,
                    "clarification_type": None,
                    "unique_values": unique_values,
                    "has_actions": False,
                    "reasoning": "3 parameters specified, single error found - provide available information"
                }
        else:
            # Multiple errors - need to list options and ask which one
            return {
                "strategy": "list_options",
                "can_direct_answer": False,
                "needs_clarification": True,
                "clarification_type": "error",
                "unique_values": unique_values,
                "has_actions": has_actions,
                "reasoning": f"3 parameters specified, {len(unique_errors)} errors found - need to ask which error"
            }
    
    # Case 3: Error specified (error-precise query)
    elif has_error:
        # Check if results have multiple defects
        unique_defects = unique_values["defect"]
        if len(unique_defects) == 0:
            # No defects - might have direct actions
            if has_actions:
                return {
                    "strategy": "direct_answer",
                    "can_direct_answer": True,
                    "needs_clarification": False,
                    "clarification_type": None,
                    "unique_values": unique_values,
                    "has_actions": True,
                    "reasoning": "Error specified, results contain actions without defects - can provide direct answer"
                }
            else:
                return {
                    "strategy": "list_options",
                    "can_direct_answer": False,
                    "needs_clarification": True,
                    "clarification_type": "defect",
                    "unique_values": unique_values,
                    "has_actions": False,
                    "reasoning": "Error specified but no defects/actions in results - need to clarify"
                }
        elif len(unique_defects) == 1:
            # Single defect - check if we have other context
            if has_defect and unique_defects[0] in filled_slots.get("defect", []):
                # Defect was already specified and matches - can provide direct answer
                if has_actions:
                    return {
                        "strategy": "direct_answer",
                        "can_direct_answer": True,
                        "needs_clarification": False,
                        "clarification_type": None,
                        "unique_values": unique_values,
                        "has_actions": True,
                        "reasoning": "Error + defect specified, results contain actions - can provide direct answer"
                    }
                else:
                    return {
                        "strategy": "direct_answer",
                        "can_direct_answer": True,
                        "needs_clarification": False,
                        "clarification_type": None,
                        "unique_values": unique_values,
                        "has_actions": False,
                        "reasoning": "Error + defect specified - provide available information"
                    }
            else:
                # Single defect but wasn't specified - still can provide answer
                return {
                    "strategy": "direct_answer",
                    "can_direct_answer": True,
                    "needs_clarification": False,
                    "clarification_type": None,
                    "unique_values": unique_values,
                    "has_actions": has_actions,
                    "reasoning": "Error specified, single defect found - can provide answer"
                }
        else:
            # Multiple defects - need to list options and ask which one
            return {
                "strategy": "list_options",
                "can_direct_answer": False,
                "needs_clarification": True,
                "clarification_type": "defect",
                "unique_values": unique_values,
                "has_actions": has_actions,
                "reasoning": f"Error specified, {len(unique_defects)} defects found - need to ask which defect"
            }
    
    # Case 4: Other combinations (non-precise)
    else:
        # For non-precise queries, list what we found
        return {
            "strategy": "list_options",
            "can_direct_answer": False,
            "needs_clarification": True,
            "clarification_type": "multiple",
            "unique_values": unique_values,
            "has_actions": has_actions,
            "reasoning": "Non-precise query - list available options and ask for clarification"
        }


def _extract_values_from_registry_matches(
    registry_matches: List[Dict[str, Any]],
    node_type: str
) -> List[str]:
    """
    Extract values from registry matches for a specific node type.
    
    Args:
        registry_matches: List of registry match dictionaries
        node_type: The node type to extract (e.g., "defect", "operation", "style", "error")
        
    Returns:
        List of values for the specified node type
    """
    values = []
    for match in registry_matches:
        if match.get("node_type") == node_type:
            value = match.get("value", "")
            if value:
                values.append(value)
    return values


def format_precise_results(results: List[Dict[str, Any]]) -> str:
    """
    Format precise query results into a readable text for LLM context.
    Includes all relevant fields including actions if available.
    
    Args:
        results: List of knowledge base result dictionaries
        
    Returns:
        Formatted text string
    """
    if not results:
        return "No matching records found in the knowledge base."
    
    lines = []
    lines.append(f"Found {len(results)} matching record(s) in the knowledge base:\n")
    
    for i, row in enumerate(results, 1):
        lines.append("=" * 60)
        lines.append(f"Record {i}:")
        
        if row.get("defect"):
            lines.append(f"  Defect: {row['defect']}")
        if row.get("operation"):
            lines.append(f"  Operation: {row['operation']}")
        if row.get("style"):
            lines.append(f"  Style: {row['style']}")
        if row.get("error"):
            lines.append(f"  Error: {row['error']}")
        
        # Include actions if available (critical for user help)
        if row.get("action"):
            # Single action field
            lines.append(f"  Action: {row['action']}")
        elif row.get("actions"):
            # Multiple actions (list)
            actions = row.get("actions", [])
            if isinstance(actions, list) and actions:
                lines.append(f"  Actions:")
                for action in actions:
                    if isinstance(action, dict):
                        action_text = action.get('action', action.get('name', str(action)))
                        count = action.get('count', action.get('cases', ''))
                        if count:
                            lines.append(f"    - {action_text} ({count} cases)")
                        else:
                            lines.append(f"    - {action_text}")
                    else:
                        lines.append(f"    - {action}")
        
        # Add any other relevant fields
        for key in ["case_id", "date", "status", "resolution", "case_count", "count"]:
            if row.get(key):
                lines.append(f"  {key.replace('_', ' ').title()}: {row[key]}")
        
        lines.append("")  # Empty line between records
    
    return "\n".join(lines)


def format_error_precise_results(results: List[Dict[str, Any]]) -> str:
    """
    Format error-precise query results into a readable text for LLM context.
    
    Args:
        results: List of knowledge base result dictionaries
        
    Returns:
        Formatted text string
    """
    if not results:
        return "No matching records found for this error in the knowledge base."
    
    lines = []
    lines.append(f"Found {len(results)} matching record(s) for this error:\n")
    
    for i, row in enumerate(results, 1):
        lines.append("=" * 60)
        lines.append(f"Record {i}:")
        
        if row.get("error"):
            lines.append(f"  Error: {row['error']}")
        if row.get("defect"):
            lines.append(f"  Defect: {row['defect']}")
        if row.get("operation"):
            lines.append(f"  Operation: {row['operation']}")
        if row.get("style"):
            lines.append(f"  Style: {row['style']}")
        
        # Add any other relevant fields
        for key in ["case_id", "date", "status", "resolution"]:
            if row.get(key):
                lines.append(f"  {key.replace('_', ' ').title()}: {row[key]}")
        
        lines.append("")  # Empty line between records
    
    return "\n".join(lines)


def format_non_precise_results(results: List[Dict[str, Any]]) -> str:
    """
    Format non-precise query results (relationship nodes) into a readable text for LLM context.
    Similar to pretty_print_relationship_nodes but returns formatted text.
    
    Args:
        results: List of relationship node dictionaries
        
    Returns:
        Formatted text string
    """
    if not results:
        return "No matching relationship nodes found in the relationship index."
    
    lines = []
    lines.append(f"Found {len(results)} related node(s) in the relationship index:\n")
    
    for i, node in enumerate(results, 1):
        lines.append("=" * 60)
        lines.append(f"{i}. {node.get('node_type', 'unknown').upper()} :: {node.get('name', 'N/A')}")
        
        total_cases = node.get('total_cases', 0)
        if total_cases:
            lines.append(f"   Total cases: {total_cases}")
        
        # Related operations
        related_operations = node.get("related_operations", [])
        if related_operations:
            lines.append("\n   Related Operations:")
            for rel in related_operations[:5]:  # Limit to top 5
                name = rel.get('name', 'N/A')
                count = rel.get('count', 0)
                lines.append(f"     - {name} ({count} cases)")
        
        # Related defects
        related_defects = node.get("related_defects", [])
        if related_defects:
            lines.append("\n   Related Defects:")
            for rel in related_defects[:5]:  # Limit to top 5
                name = rel.get('name', 'N/A')
                count = rel.get('count', 0)
                lines.append(f"     - {name} ({count} cases)")
        
        # Related errors
        related_errors = node.get("related_errors", [])
        if related_errors:
            lines.append("\n   Related Errors:")
            for rel in related_errors[:5]:  # Limit to top 5
                name = rel.get('name', 'N/A')
                count = rel.get('count', 0)
                lines.append(f"     - {name} ({count} cases)")
        
        # Related styles
        related_styles = node.get("related_styles", [])
        if related_styles:
            lines.append("\n   Related Styles:")
            for rel in related_styles[:5]:  # Limit to top 5
                name = rel.get('name', 'N/A')
                count = rel.get('count', 0)
                lines.append(f"     - {name} ({count} cases)")
        
        # Top actions
        top_actions = node.get("top_actions", [])
        if top_actions:
            lines.append("\n   Top Actions:")
            for action in top_actions[:5]:  # Limit to top 5
                action_text = action.get('action', 'N/A')
                count = action.get('count', 0)
                lines.append(f"     - {action_text} ({count} cases)")
        
        lines.append("")  # Empty line between nodes
    
    return "\n".join(lines)


def format_generic_results() -> str:
    """
    Format generic classification result message.
    
    Returns:
        Formatted text string
    """
    return (
        "This is a generic manufacturing query with no specific registry matches. "
        "The query is related to manufacturing processes, technical specifications, "
        "quality control, or production topics, but does not match specific items "
        "in the knowledge base or relationship index."
    )


@trace_node("classify")
def classify(state: OCAPState) -> Dict[str, Any]:
    """
    Classify node that queries Elasticsearch based on classification and registry matches.
    
    Classification routing:
    - precise: Uses get_full_rows with defect, operation, style from registry_matches
               Queries index "ocap-knowledge-base"
    - error-precise: Uses get_rows_by_error with error from registry_matches
                    Queries index "ocap-knowledge-base"
    - non-precise: Queries index "ocap-relationship-index" using relationship search
    - generic: Returns formatted message for generic queries
    
    Args:
        state: Current OCAP state containing classification and registry_matches
        
    Returns:
        Updated state with query results and formatted_text in metadata.
        The formatted_text field contains a pretty-printed, LLM-friendly representation
        of the query results for easy use in subsequent LLM context.
    """
    query = state.get("query", "")
    classification = state.get("classification")
    metadata = state.get("metadata") or {}
    
    # Use classification_registry if available (from analyze node merge decision), otherwise fall back to registry_matches
    analysis_metadata = metadata.get("analysis", {})
    classification_registry = analysis_metadata.get("classification_registry")
    registry_matches = classification_registry if classification_registry else metadata.get("registry_matches", [])
    
    merge_applied = analysis_metadata.get("merge_applied", False)
    
    # If merge_applied=True but classification_registry is missing some node types, try to get them from historical context
    if merge_applied and classification_registry:
        historical_registry_matches = metadata.get("historical_registry_matches", [])
        if historical_registry_matches:
            # Extract node types from classification_registry
            current_node_types = set(m.get("node_type") for m in classification_registry if m.get("node_type"))
            required_node_types = {"defect", "operation", "style", "error"}
            missing_node_types = required_node_types - current_node_types
            
            # If we're missing node types, try to get them from the most recent historical entry
            if missing_node_types:
                logger.info(
                    f"Merge applied but missing node types: {missing_node_types}. "
                    f"Attempting to supplement from historical context."
                )
                # Get the most recent historical entry
                most_recent = historical_registry_matches[0] if historical_registry_matches else None
                if most_recent:
                    historical_matches = most_recent.get("registry_matches", [])
                    # Add missing node types from historical matches
                    for hist_match in historical_matches:
                        hist_node_type = hist_match.get("node_type")
                        if hist_node_type in missing_node_types:
                            # Check if this node type is not already in classification_registry
                            if not any(m.get("node_type") == hist_node_type for m in classification_registry):
                                # Add it to classification_registry
                                classification_registry.append(hist_match)
                                logger.info(
                                    f"Added missing {hist_node_type}='{hist_match.get('value')}' "
                                    f"from historical context to classification_registry"
                                )
                    # Update registry_matches to use the supplemented classification_registry
                    registry_matches = classification_registry
    
    logger.info(
        f"Classifying query for Elasticsearch search: {query[:50]}... "
        f"(classification: {classification}, "
        f"using: {'classification_registry' if classification_registry else 'registry_matches'}, "
        f"merge_applied: {merge_applied}, "
        f"matches_count: {len(registry_matches)})"
    )
    
    if not classification:
        logger.warning("No classification found in state, skipping classify node")
        return {}
    
    try:
        # Get Elasticsearch client
        client = get_elasticsearch_client()
        
        query_results = []
        index_used = None
        query_method = None
        
        # Extract all available node types from registry_matches
        # This determines the query strategy, NOT the classification label
        defects = _extract_values_from_registry_matches(registry_matches, "defect")
        operations = _extract_values_from_registry_matches(registry_matches, "operation")
        styles = _extract_values_from_registry_matches(registry_matches, "style")
        errors = _extract_values_from_registry_matches(registry_matches, "error")
        
        has_defect = bool(defects)
        has_operation = bool(operations)
        has_style = bool(styles)
        has_error = bool(errors)
        
        # Log extracted values for debugging
        logger.info(
            f"Extracted from classification_registry: "
            f"defects={defects}, operations={operations}, styles={styles}, errors={errors}"
        )
        
        # Determine query strategy based on AVAILABLE NODE TYPES, not classification label
        # This handles ALL combinations correctly
        # Priority: More specific queries first (more parameters = more specific)
        
        if has_defect and has_operation and has_style and has_error:
            # All 4: defect + operation + style + error (most specific)
            defect = defects[0]
            operation_candidates = operations
            style = styles[0]
            error = errors[0]
            
            logger.info(
                f"Querying with all 4 parameters: defect={defect}, "
                f"operations={operation_candidates}, style={style}, error={error}"
            )
            
            index_used = "ocap-knowledge-base"
            query_method = "get_rows_with_error"
            query_results = get_rows_with_error(
                client=client,
                index_name=index_used,
                defect=defect,
                operation_candidates=operation_candidates,
                style=style,
                error=error,
                size=50
            )
            
            # If 4-parameter query returns 0 results, try 3-parameter query (defect + operation + style)
            # This gets the errors/actions for that combination, which is still relevant
            if len(query_results) == 0:
                logger.info(
                    f"4-parameter query returned 0 results. "
                    f"Trying 3-parameter query (defect + operation + style) to get related information."
                )
                query_method = "get_full_rows_fallback"
                query_results = get_full_rows(
                    client=client,
                    index_name=index_used,
                    defect=defect,
                    operation_candidates=operation_candidates,
                    style=style,
                    size=50
                )
                # Store that we used fallback - the results will have errors, we need to filter by the specified error
                if query_results:
                    # Filter results to only include rows with the specified error
                    filtered_results = [
                        row for row in query_results 
                        if normalize_value(row.get("error", "")) == normalize_value(error)
                    ]
                    if filtered_results:
                        query_results = filtered_results
                        logger.info(
                            f"Filtered 3-parameter results to {len(filtered_results)} rows matching error={error}"
                        )
                    else:
                        # If no rows match the error, we still want to use the 3-parameter results
                        # but mark that we couldn't filter by error - the summarize node should handle this
                        logger.info(
                            f"No rows in 3-parameter results match error={error}. "
                            f"Using all {len(query_results)} results from 3-parameter query. "
                            f"Note: These results may contain multiple errors, not just {error}."
                        )
                        # Store metadata about the fallback situation
                        updated_metadata = dict(metadata)
                        if "classify" not in updated_metadata:
                            updated_metadata["classify"] = {}
                        updated_metadata["classify"]["fallback_info"] = {
                            "original_query": "get_rows_with_error (4 params)",
                            "fallback_query": "get_full_rows (3 params)",
                            "error_filter_applied": False,
                            "reason": f"No rows matched error={error} in 3-parameter results"
                        }
                        metadata = updated_metadata
            
        elif has_error and has_defect:
            # Error + Defect: User selected defect from error query
            error = errors[0]
            defect = defects[0]
            
            logger.info(
                f"Querying with error + defect: error={error}, defect={defect}"
            )
            
            index_used = "ocap-knowledge-base"
            query_method = "get_rows_by_error_and_defect"
            query_results = get_rows_by_error_and_defect(
                client=client,
                index_name=index_used,
                error=error,
                defect=defect,
                size=50
            )
            
        elif has_defect and has_operation and has_style:
            # 3 params: defect + operation + style (precise)
            defect = defects[0]
            operation_candidates = operations
            style = styles[0]
            
            logger.info(
                f"Querying precise: defect={defect}, "
                f"operations={operation_candidates}, style={style}"
            )
            
            index_used = "ocap-knowledge-base"
            query_method = "get_full_rows"
            query_results = get_full_rows(
                client=client,
                index_name=index_used,
                defect=defect,
                operation_candidates=operation_candidates,
                style=style,
                size=50
            )
            
        elif has_error:
            # Error only: error-precise
            error = errors[0]
            
            logger.info(f"Querying error-precise: error={error}")
            
            index_used = "ocap-knowledge-base"
            query_method = "get_rows_by_error"
            query_results = get_rows_by_error(
                client=client,
                index_name=index_used,
                error=error,
                size=50
            )
            
        elif has_defect or has_operation or has_style:
            # Partial matches: use relationship index
            logger.info(
                f"Querying relationship index with partial matches: "
                f"defect={has_defect}, operation={has_operation}, style={has_style}"
            )
            index_used = "ocap-relationship-index"
            query_method = "get_rows_non_precise"
            query_results = get_rows_non_precise(
                client=client,
                index_name=index_used,
                registry_matches=registry_matches,
                size=50
            )
            
        else:
            # No matches: generic
            logger.info("No registry matches - generic query")
            index_used = None
            query_method = "generic"
            query_results = []
        
        
        logger.info(
            f"Elasticsearch query completed: "
            f"classification={classification}, "
            f"index={index_used}, "
            f"method={query_method}, "
            f"results_count={len(query_results)}"
        )
        
        # Format results into readable text for LLM context
        # Format based on query method, not classification
        formatted_text = ""
        
        if query_method == "get_rows_with_error" or query_method == "get_full_rows" or query_method == "get_full_rows_fallback":
            # Precise queries (3 or 4 parameters, including fallback)
            formatted_text = format_precise_results(query_results)
        elif query_method == "get_rows_by_error_and_defect":
            # Error + Defect combination - format as precise results
            formatted_text = format_precise_results(query_results)
        elif query_method == "get_rows_by_error":
            # Error-only queries
            formatted_text = format_error_precise_results(query_results)
        elif query_method == "get_rows_non_precise":
            # Relationship index queries
            formatted_text = format_non_precise_results(query_results)
        elif query_method == "generic":
            # Generic queries
            formatted_text = format_generic_results()
        else:
            # Fallback
            formatted_text = format_precise_results(query_results) if query_results else "No results found."
        
        logger.debug(f"Formatted text length: {len(formatted_text)} characters")
        
        # Analyze query results to determine response strategy
        # This is critical: classification is about QUERY strategy, but response strategy depends on RESULTS
        response_strategy = _analyze_result_quality(
            query_results=query_results,
            query_method=query_method,
            has_defect=has_defect,
            has_operation=has_operation,
            has_style=has_style,
            has_error=has_error,
            filled_slots=metadata.get("consolidated_slot_state", {})
        )
        
        logger.info(
            f"Result analysis: results_count={len(query_results)}, "
            f"response_strategy={response_strategy.get('strategy')}, "
            f"can_direct_answer={response_strategy.get('can_direct_answer')}, "
            f"needs_clarification={response_strategy.get('needs_clarification')}"
        )
        
        # Update metadata with query results and formatted text
        # Use metadata that may have been updated with fallback info
        final_metadata = dict(metadata)
        final_metadata["classify"] = {
            "classification": classification,  # This is query strategy classification
            "index_used": index_used,
            "query_method": query_method,
            "results_count": len(query_results),
            "results": query_results,
            "formatted_text": formatted_text,
            "fallback_used": False,
            "response_strategy": response_strategy  # This is response strategy based on results
        }
        
        return {
            "metadata": final_metadata
        }
        
    except Exception as e:
        logger.error(f"Error in classify node: {e}", exc_info=True)
        # Return error metadata but don't fail the graph
        updated_metadata = dict(metadata)
        updated_metadata["classify"] = {
            "classification": classification,
            "error": str(e),
            "results_count": 0,
            "results": [],
            "formatted_text": f"Error occurred while querying Elasticsearch: {str(e)}"
        }
        
        return {
            "metadata": updated_metadata
        }

