"""Application configuration settings."""
from pydantic_settings import BaseSettings
from typing import Optional


class Settings(BaseSettings):
    """Application settings."""
    
    app_name: str = "OCAP Agent"
    app_version: str = "0.1.0"
    debug: bool = False
    
    # API Settings
    api_v1_prefix: str = "/api/v1"
    
    # Azure OpenAI Configuration
    azure_openai_api_key: str
    azure_openai_api_version: str
    azure_openai_endpoint: str
    azure_openai_deployment: Optional[str] = None
    
    # Redis Configuration
    redis_host: str
    redis_port: int
    redis_username: str = "default"
    redis_password: str
    redis_decode_responses: bool = True
    
    # Elasticsearch Configuration
    elasticsearch_host: str
    elasticsearch_api_key: str
    
    # Database Configuration
    db_url: str  # PostgreSQL database URL
    
    # JWT Configuration
    secret_key: str = "your-secret-key-here-change-in-production"  # Change in production!
    access_token_expire_minutes: int = 30
    
    # OpenTelemetry / Jaeger Configuration
    jaeger_agent_host: str = "localhost"
    jaeger_agent_port: int = 6831
    jaeger_collector_endpoint: str = "http://localhost:4317"  # OTLP gRPC endpoint
    enable_tracing: bool = True
    service_name: str = "ocap-agent"
    
    class Config:
        env_file = ".env"
        case_sensitive = False


settings = Settings()

