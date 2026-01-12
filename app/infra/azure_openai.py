"""Azure OpenAI client singleton."""
from typing import Optional
from openai import AzureOpenAI
from app.core.config import settings
from app.core.logging import logger


class AzureOpenAIClient:
    """Singleton class for Azure OpenAI client."""
    
    _instance: Optional[AzureOpenAI] = None
    _initialized: bool = False
    
    @classmethod
    def get_client(cls) -> AzureOpenAI:
        """
        Get or create Azure OpenAI client instance.
        
        Returns:
            AzureOpenAI client instance
        """
        if cls._instance is None or not cls._initialized:
            cls._initialize()
        
        return cls._instance
    
    @classmethod
    def _format_endpoint(cls, endpoint: str) -> str:
        """
        Format Azure OpenAI endpoint URL to ensure proper format.
        
        Args:
            endpoint: Raw endpoint URL from settings
            
        Returns:
            Properly formatted endpoint URL
        """
        endpoint = endpoint.strip()
        
        # Remove trailing slashes
        endpoint = endpoint.rstrip('/')
        
        # Ensure https:// protocol is present
        if not endpoint.startswith(('http://', 'https://')):
            endpoint = f"https://{endpoint}"
        
        return endpoint
    
    @classmethod
    def _initialize(cls) -> None:
        """Initialize Azure OpenAI client."""
        try:
            # Format endpoint URL
            formatted_endpoint = cls._format_endpoint(settings.azure_openai_endpoint)
            
            # Validate required settings
            if not settings.azure_openai_api_key:
                raise ValueError("AZURE_OPENAI_API_KEY is not set")
            if not settings.azure_openai_api_version:
                raise ValueError("AZURE_OPENAI_API_VERSION is not set")
            if not formatted_endpoint:
                raise ValueError("AZURE_OPENAI_ENDPOINT is not set")
            
            logger.info(f"Initializing Azure OpenAI client with endpoint: {formatted_endpoint}")
            
            cls._instance = AzureOpenAI(
                api_version=settings.azure_openai_api_version,
                azure_endpoint=formatted_endpoint,
                api_key=settings.azure_openai_api_key,
            )
            cls._initialized = True
            logger.info("Azure OpenAI client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Azure OpenAI client: {e}")
            logger.error(
                f"Endpoint: {settings.azure_openai_endpoint}, "
                f"API Version: {settings.azure_openai_api_version}, "
                f"API Key: {'***' if settings.azure_openai_api_key else 'NOT SET'}"
            )
            raise
    
    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        cls._instance = None
        cls._initialized = False


# Convenience function for easy access
def get_azure_openai_client() -> AzureOpenAI:
    """Get Azure OpenAI client instance."""
    return AzureOpenAIClient.get_client()

