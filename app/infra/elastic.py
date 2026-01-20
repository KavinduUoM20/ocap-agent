"""Elasticsearch client singleton."""
from typing import Optional
from elasticsearch import Elasticsearch, helpers
from app.core.config import settings
from app.core.logging import logger


class ElasticsearchClient:
    """Singleton class for Elasticsearch client."""
    
    _instance: Optional[Elasticsearch] = None
    _initialized: bool = False
    
    @classmethod
    def get_client(cls) -> Elasticsearch:
        """
        Get or create Elasticsearch client instance.
        
        Returns:
            Elasticsearch client instance
        """
        if cls._instance is None or not cls._initialized:
            cls._initialize()
        
        return cls._instance
    
    @classmethod
    def _initialize(cls) -> None:
        """Initialize Elasticsearch client."""
        try:
            cls._instance = Elasticsearch(
                settings.elasticsearch_host,
                api_key=settings.elasticsearch_api_key,
                verify_certs=False,  # Disable SSL certificate verification for self-signed certs
                ssl_show_warn=False  # Suppress SSL warnings
            )
            # Test connection
            cls._instance.info()
            cls._initialized = True
            logger.info("Elasticsearch client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Elasticsearch client: {e}")
            raise
    
    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        cls._instance = None
        cls._initialized = False


# Convenience function for easy access
def get_elasticsearch_client() -> Elasticsearch:
    """Get Elasticsearch client instance."""
    return ElasticsearchClient.get_client()


# Export helpers for convenience
__all__ = ["get_elasticsearch_client", "helpers"]

