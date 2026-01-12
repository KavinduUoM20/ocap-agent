"""Authentication endpoints."""
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from app.infra.database import get_db
from app.models.schemas import UserCreate, User, UserLogin, Token
from app.services.user_service import UserService
from app.core.security import create_access_token
from app.core.config import settings
from app.core.logging import logger
from datetime import timedelta

router = APIRouter()


@router.post("/register", response_model=User, status_code=status.HTTP_201_CREATED)
async def register(
    user_create: UserCreate,
    db: Session = Depends(get_db)
):
    """
    Register a new user.
    
    Args:
        user_create: User registration data
        db: Database session
        
    Returns:
        Created user information
    """
    logger.info(f"Registration attempt for email: {user_create.email}")
    
    # Check if user already exists
    existing_user = UserService.get_user_by_email(db, user_create.email)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Email already registered"
        )
    
    existing_user = UserService.get_user_by_username(db, user_create.username)
    if existing_user:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already taken"
        )
    
    # Create user
    try:
        user = UserService.create_user(db, user_create)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to create user"
            )
        
        logger.info(f"User registered successfully: {user.username} ({user.email})")
        return user
    except ValueError as e:
        # Handle password validation errors
        logger.warning(f"Registration failed due to validation error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        # Handle unexpected errors
        logger.error(f"Unexpected error during registration: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred during registration"
        )


@router.post("/login", response_model=Token)
async def login(
    user_login: UserLogin,
    db: Session = Depends(get_db)
):
    """
    Login and get access token using JSON body.
    
    Args:
        user_login: User login credentials
        db: Database session
        
    Returns:
        Access token and token type
    """
    logger.info(f"Login attempt for username: {user_login.username}")
    
    # Authenticate user
    user = UserService.authenticate_user(
        db, 
        user_login.username, 
        user_login.password
    )
    
    if not user:
        logger.warning(f"Failed login attempt for: {user_login.username}")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Create access token
    access_token_expires = timedelta(minutes=settings.access_token_expire_minutes)
    access_token = create_access_token(
        data={"sub": user.username, "email": user.email, "user_id": user.id},
        expires_delta=access_token_expires
    )
    
    logger.info(f"User logged in successfully: {user.username}")
    
    return {
        "access_token": access_token,
        "token_type": "bearer"
    }

