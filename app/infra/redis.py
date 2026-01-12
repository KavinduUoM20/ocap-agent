"""Redis client singleton."""
from typing import Optional
import redis
from app.core.config import settings
from app.core.logging import logger


class RedisClient:
    """Singleton class for Redis client."""
    
    _instance: Optional[redis.Redis] = None
    _initialized: bool = False
    _available: bool = False  # Track if Redis is available
    
    @classmethod
    def get_client(cls) -> Optional[redis.Redis]:
        """
        Get or create Redis client instance.
        
        Returns:
            Redis client instance if available, None otherwise
        """
        if cls._instance is None or not cls._initialized:
            cls._initialize()
        
        return cls._instance if cls._available else None
    
    @classmethod
    def is_available(cls) -> bool:
        """Check if Redis is available."""
        return cls._available
    
    @classmethod
    def _initialize(cls) -> None:
        """Initialize Redis client."""
        try:
            cls._instance = redis.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                decode_responses=settings.redis_decode_responses,
                username=settings.redis_username,
                password=settings.redis_password,
                socket_connect_timeout=3,  # 3 second connection timeout
                socket_timeout=3,  # 3 second socket timeout
                retry_on_timeout=False,  # Don't retry on timeout
            )
            # Test connection with timeout
            cls._instance.ping()
            cls._initialized = True
            cls._available = True
            logger.info(
                f"Redis client initialized successfully "
                f"(host: {settings.redis_host}, port: {settings.redis_port})"
            )
        except (redis.exceptions.TimeoutError, redis.exceptions.ConnectionError) as e:
            cls._initialized = True  # Mark as initialized to prevent retries
            cls._available = False
            logger.warning(
                f"Redis connection failed (timeout/connection error): {e}. "
                f"Redis features will be disabled. Check network connectivity and Redis server status."
            )
        except Exception as e:
            cls._initialized = True
            cls._available = False
            logger.warning(
                f"Failed to initialize Redis client: {e}. "
                f"Redis features will be disabled."
            )
    
    @classmethod
    def reset(cls) -> None:
        """Reset the singleton instance (useful for testing)."""
        if cls._instance:
            try:
                cls._instance.close()
            except Exception:
                pass
        cls._instance = None
        cls._initialized = False
        cls._available = False


# Convenience function for easy access
def get_redis_client() -> Optional[redis.Redis]:
    """Get Redis client instance if available."""
    return RedisClient.get_client()


def is_redis_available() -> bool:
    """Check if Redis is available."""
    return RedisClient.is_available()

