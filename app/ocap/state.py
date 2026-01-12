"""OCAP state definition for LangGraph."""
from typing import TypedDict, List, Optional, Any, Dict, Annotated


def merge_metadata(left: Optional[Dict[str, Any]], right: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge two metadata dictionaries.
    
    Args:
        left: First metadata dictionary (or None)
        right: Second metadata dictionary (or None)
        
    Returns:
        Merged metadata dictionary
    """
    result = {}
    
    # Start with left if it exists
    if left:
        result.update(left)
    
    # Merge right if it exists
    if right:
        result.update(right)
    
    return result


class OCAPState(TypedDict):
    """State schema for OCAP processing graph."""
    
    # Input
    query: str  # Original user query
    
    # Processing
    keywords: Optional[List[str]]  # Extracted keywords
    classification: Optional[str]  # Query classification
    entities: Optional[List[Dict[str, Any]]]  # Extracted entities
    relationships: Optional[List[Dict[str, Any]]]  # Extracted relationships
    
    # Output
    response: Optional[str]  # Final response
    metadata: Annotated[Optional[Dict[str, Any]], merge_metadata]  # Additional metadata (includes thread_id, user_id)

