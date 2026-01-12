"""Security utilities."""
from passlib.context import CryptContext
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from app.core.config import settings
from app.core.logging import logger

# Password hashing context
# Configure bcrypt to avoid passlib initialization issues
# The detect_wrap_bug check in passlib can fail with bcrypt 5.0.0
pwd_context = CryptContext(
    schemes=["bcrypt"],
    deprecated="auto",
    bcrypt__rounds=12,  # Explicitly set bcrypt rounds
    bcrypt__ident="2b",  # Use 2b identifier (most compatible)
)

# JWT settings
ALGORITHM = "HS256"

def get_secret_key() -> str:
    """Get secret key from settings."""
    return settings.secret_key

def get_access_token_expire_minutes() -> int:
    """Get access token expiration minutes from settings."""
    return settings.access_token_expire_minutes


def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verify a plain password against a hashed password.
    
    Args:
        plain_password: Plain text password
        hashed_password: Hashed password
        
    Returns:
        True if password matches, False otherwise
    """
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """
    Hash a password using bcrypt.
    
    Args:
        password: Plain text password
        
    Returns:
        Hashed password
        
    Raises:
        ValueError: If password validation fails or hashing fails
    """
    if not password:
        raise ValueError("Password cannot be empty")
    
    # Bcrypt has a 72-byte limit for passwords
    # Convert to bytes to check length properly
    try:
        password_bytes = password.encode('utf-8')
    except UnicodeEncodeError as e:
        raise ValueError(f"Password contains invalid characters: {str(e)}") from e
    
    password_byte_length = len(password_bytes)
    
    # Validate password length
    if password_byte_length > 72:
        raise ValueError(
            f"Password is too long. Maximum length is 72 bytes "
            f"(approximately 72 ASCII characters). "
            f"Your password is {password_byte_length} bytes."
        )
    
    # Hash the password
    try:
        hashed = pwd_context.hash(password)
        return hashed
    except ValueError as e:
        # Check if it's specifically a length-related error from bcrypt
        # Only match the EXACT bcrypt length error message to avoid false positives
        error_str = str(e).lower()
        
        # Only treat as length error if it explicitly says "cannot be longer than 72 bytes"
        # AND the password is actually longer than 72 bytes (double-check)
        is_length_error = (
            "cannot be longer than 72 bytes" in error_str and
            password_byte_length > 72
        )
        
        if is_length_error:
            # This should not happen due to validation above, but handle gracefully
            raise ValueError(
                f"Password is too long. Maximum length is 72 bytes. "
                f"Your password is {password_byte_length} bytes."
            ) from None
        
        # For other ValueError from bcrypt, this is NOT a length error
        # Log the actual error and re-raise with the real error message
        # DO NOT mention length in the error message
        logger.error(
            f"Password hashing failed with ValueError: {e}",
            exc_info=True
        )
        # Return the actual error message - don't fabricate a length error
        raise ValueError(f"Password hashing failed: {str(e)}") from e
    except Exception as e:
        # Log unexpected errors for debugging
        logger.error(
            f"Unexpected error during password hashing: {type(e).__name__}: {e}",
            exc_info=True
        )
        raise ValueError(
            f"An error occurred while hashing the password. "
            f"Please try again or contact support if the problem persists."
        ) from e


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Create a JWT access token.
    
    Args:
        data: Data to encode in the token
        expires_delta: Optional expiration time delta
        
    Returns:
        Encoded JWT token
    """
    to_encode = data.copy()
    
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=get_access_token_expire_minutes())
    
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, get_secret_key(), algorithm=ALGORITHM)
    
    return encoded_jwt


def decode_access_token(token: str) -> Optional[dict]:
    """
    Decode a JWT access token.
    
    Args:
        token: JWT token to decode
        
    Returns:
        Decoded token data or None if invalid
    """
    try:
        payload = jwt.decode(token, get_secret_key(), algorithms=[ALGORITHM])
        return payload
    except JWTError as e:
        logger.warning(f"JWT decode error: {e}")
        return None

