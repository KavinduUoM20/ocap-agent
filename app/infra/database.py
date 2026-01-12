"""PostgreSQL database connection singleton."""
from typing import Optional
from urllib.parse import urlparse
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.declarative import declarative_base
from app.core.config import settings
from app.core.logging import logger

# Create base class for models
Base = declarative_base()

# Database engine and session factory
_engine: Optional[object] = None
_SessionLocal: Optional[sessionmaker] = None
_initialized: bool = False
_available: bool = False


def get_engine():
    """Get or create database engine."""
    global _engine, _initialized, _available
    
    if _engine is None or not _initialized:
        _initialize()
    
    return _engine if _available else None


def get_session() -> Optional[Session]:
    """
    Get database session.
    
    Returns:
        Database session if available, None otherwise
    """
    global _SessionLocal, _available
    
    if not _available or _SessionLocal is None:
        return None
    
    return _SessionLocal()


def is_database_available() -> bool:
    """Check if database is available."""
    return _available


def _create_database_if_not_exists(db_url: str) -> bool:
    """
    Create database if it doesn't exist.
    
    Args:
        db_url: Database URL
        
    Returns:
        True if database exists or was created, False otherwise
    """
    try:
        # Parse the database URL
        parsed = urlparse(db_url)
        database_name = parsed.path.lstrip('/')
        
        if not database_name:
            logger.error("No database name found in DB_URL")
            return False
        
        # Create URL to connect to default postgres database
        # to check/create the target database
        default_db_url = db_url.rsplit('/', 1)[0] + '/postgres'
        
        logger.info(f"Checking if database '{database_name}' exists...")
        
        # Connect to default postgres database
        temp_engine = create_engine(
            default_db_url,
            isolation_level="AUTOCOMMIT"
        )
        
        with temp_engine.connect() as conn:
            # Check if database exists
            result = conn.execute(
                text(
                    "SELECT 1 FROM pg_database WHERE datname = :dbname"
                ),
                {"dbname": database_name}
            )
            exists = result.fetchone() is not None
            
            if not exists:
                logger.info(f"Database '{database_name}' does not exist. Creating...")
                # Create database
                conn.execute(
                    text(f'CREATE DATABASE "{database_name}"')
                )
                logger.info(f"Database '{database_name}' created successfully")
            else:
                logger.info(f"Database '{database_name}' already exists")
        
        temp_engine.dispose()
        return True
        
    except Exception as e:
        logger.error(f"Failed to create database: {e}")
        return False


def _initialize() -> None:
    """Initialize database connection."""
    global _engine, _SessionLocal, _initialized, _available
    
    try:
        # First, ensure the database exists
        if not _create_database_if_not_exists(settings.db_url):
            logger.warning("Could not ensure database exists, attempting connection anyway...")
        
        # Create engine with connection pooling
        _engine = create_engine(
            settings.db_url,
            pool_pre_ping=True,  # Verify connections before using
            pool_size=5,  # Number of connections to maintain
            max_overflow=10,  # Maximum overflow connections
            echo=settings.debug,  # Log SQL queries in debug mode
        )
        
        # Create session factory
        _SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=_engine
        )
        
        # Test connection
        with _engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        _initialized = True
        _available = True
        logger.info("PostgreSQL database connection initialized successfully")
        
    except Exception as e:
        _initialized = True
        _available = False
        logger.warning(
            f"Failed to initialize database connection: {e}. "
            f"Database features will be disabled."
        )


def reset() -> None:
    """Reset database connection (useful for testing)."""
    global _engine, _SessionLocal, _initialized, _available
    
    if _engine:
        try:
            _engine.dispose()
        except Exception:
            pass
    
    _engine = None
    _SessionLocal = None
    _initialized = False
    _available = False


# Dependency for FastAPI
def get_db():
    """
    Database dependency for FastAPI routes.
    Yields a database session and closes it after use.
    """
    db = get_session()
    if db is None:
        raise Exception("Database is not available")
    
    try:
        yield db
    finally:
        db.close()

