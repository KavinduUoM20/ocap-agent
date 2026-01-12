"""OCAP LangGraph definition."""
from langgraph.graph import StateGraph, END, START
from app.ocap.state import OCAPState
from app.ocap.nodes.extract import extract_keywords
from app.ocap.nodes.memory import summarize_thread_memory
from app.ocap.nodes.analyze import analyze_query
from app.ocap.nodes.classify import classify
from app.ocap.nodes.summarize import summarize
from app.core.logging import logger


def create_ocap_graph() -> StateGraph:
    """
    Create and configure the OCAP processing graph.
    
    Returns:
        Configured StateGraph instance
    """
    logger.info("Creating OCAP graph")
    
    # Create the graph
    workflow = StateGraph(OCAPState)
    
    # Add nodes
    workflow.add_node("extract_keywords", extract_keywords)
    workflow.add_node("summarize_thread_memory", summarize_thread_memory)
    workflow.add_node("analyze_query", analyze_query)
    workflow.add_node("classify", classify)
    workflow.add_node("summarize", summarize)
    
    # Start with parallel execution of extract and memory
    workflow.add_edge(START, "extract_keywords")
    workflow.add_edge(START, "summarize_thread_memory")
    
    # Analyze query runs after both extract_keywords and summarize_thread_memory complete
    # (needs registry_matches from extract and thread_memory_summary from memory)
    workflow.add_edge("extract_keywords", "analyze_query")
    workflow.add_edge("summarize_thread_memory", "analyze_query")
    
    # Classify runs after analyze_query completes
    # (needs classification and registry_matches from analyze)
    workflow.add_edge("analyze_query", "classify")
    
    # Summarize runs after classify completes
    # (needs classification, formatted_text, and all context from previous nodes)
    workflow.add_edge("classify", "summarize")
    
    # Summarize completes and routes to END
    workflow.add_edge("summarize", END)
    
    # Compile the graph
    graph = workflow.compile()
    
    logger.info("OCAP graph created successfully with parallel nodes")
    
    return graph


# Create a singleton instance
_ocap_graph = None


def get_ocap_graph() -> StateGraph:
    """
    Get the OCAP graph instance (singleton).
    
    Returns:
        OCAP StateGraph instance
    """
    global _ocap_graph
    if _ocap_graph is None:
        _ocap_graph = create_ocap_graph()
    return _ocap_graph

