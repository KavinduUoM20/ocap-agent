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
        
        # Add any other relevant fields
        for key in ["case_id", "date", "status", "resolution"]:
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
    registry_matches = metadata.get("registry_matches", [])
    
    logger.info(
        f"Classifying query for Elasticsearch search: {query[:50]}... "
        f"(classification: {classification})"
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
        
        if classification == "precise":
            # Extract defect, operation, and style from registry_matches
            defects = _extract_values_from_registry_matches(registry_matches, "defect")
            operations = _extract_values_from_registry_matches(registry_matches, "operation")
            styles = _extract_values_from_registry_matches(registry_matches, "style")
            
            if not defects:
                logger.warning("Precise classification but no defect found in registry_matches")
                defect = ""
            else:
                defect = defects[0]  # Use first defect match
            
            if not operations:
                logger.warning("Precise classification but no operation found in registry_matches")
                operation_candidates = []
            else:
                operation_candidates = operations
            
            style = styles[0] if styles else ""
            
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
            
        elif classification == "error-precise":
            # Extract error from registry_matches
            errors = _extract_values_from_registry_matches(registry_matches, "error")
            
            if not errors:
                logger.warning("Error-precise classification but no error found in registry_matches")
                error = ""
            else:
                error = errors[0]  # Use first error match
            
            logger.info(f"Querying error-precise: error={error}")
            
            index_used = "ocap-knowledge-base"
            query_method = "get_rows_by_error"
            query_results = get_rows_by_error(
                client=client,
                index_name=index_used,
                error=error,
                size=50
            )
            
        elif classification == "non-precise":
            # Non-precise query logic using relationship index
            logger.info(
                f"Non-precise classification - querying relationship index "
                f"with {len(registry_matches)} registry matches"
            )
            index_used = "ocap-relationship-index"
            query_method = "get_rows_non_precise"
            query_results = get_rows_non_precise(
                client=client,
                index_name=index_used,
                registry_matches=registry_matches,
                size=50
            )
            
        elif classification == "generic":
            # Generic queries don't have specific registry matches
            logger.info("Generic classification - no specific Elasticsearch query")
            index_used = None
            query_method = "generic"
            query_results = []
            
        else:
            logger.warning(f"Unknown classification type: {classification}")
            query_results = []
        
        logger.info(
            f"Elasticsearch query completed: "
            f"classification={classification}, "
            f"index={index_used}, "
            f"method={query_method}, "
            f"results_count={len(query_results)}"
        )
        
        # Format results into readable text for LLM context
        formatted_text = ""
        if classification == "precise":
            formatted_text = format_precise_results(query_results)
        elif classification == "error-precise":
            formatted_text = format_error_precise_results(query_results)
        elif classification == "non-precise":
            formatted_text = format_non_precise_results(query_results)
        elif classification == "generic":
            formatted_text = format_generic_results()
        
        logger.debug(f"Formatted text length: {len(formatted_text)} characters")
        
        # Update metadata with query results and formatted text
        updated_metadata = dict(metadata)
        updated_metadata["classify"] = {
            "classification": classification,
            "index_used": index_used,
            "query_method": query_method,
            "results_count": len(query_results),
            "results": query_results,
            "formatted_text": formatted_text
        }
        
        return {
            "metadata": updated_metadata
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

