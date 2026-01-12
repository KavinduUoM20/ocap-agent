"""Logging configuration."""
import logging
import sys
from typing import Optional


def setup_logging(level: Optional[str] = None) -> None:
    """Setup application logging."""
    log_level = level or "INFO"
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # Suppress harmless passlib/bcrypt version compatibility warnings
    # This is a known issue where passlib tries to read bcrypt.__about__.__version__
    # which doesn't exist in newer bcrypt versions, but functionality works fine
    passlib_logger = logging.getLogger("passlib.handlers.bcrypt")
    passlib_logger.setLevel(logging.ERROR)  # Only show errors, suppress warnings


logger = logging.getLogger(__name__)

