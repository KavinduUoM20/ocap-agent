"""User service for authentication and user management."""
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from app.models.user import User
from app.models.schemas import UserCreate, UserUpdate
from app.core.logging import logger
from app.core.security import get_password_hash, verify_password


class UserService:
    """Service for user management and authentication."""
    
    @staticmethod
    def get_user_by_id(db: Session, user_id: int) -> Optional[User]:
        """Get user by ID."""
        return db.query(User).filter(User.id == user_id).first()
    
    @staticmethod
    def get_user_by_email(db: Session, email: str) -> Optional[User]:
        """Get user by email."""
        return db.query(User).filter(User.email == email).first()
    
    @staticmethod
    def get_user_by_username(db: Session, username: str) -> Optional[User]:
        """Get user by username."""
        return db.query(User).filter(User.username == username).first()
    
    @staticmethod
    def get_user_by_username_or_email(db: Session, username_or_email: str) -> Optional[User]:
        """Get user by username or email."""
        user = db.query(User).filter(
            (User.username == username_or_email) | (User.email == username_or_email)
        ).first()
        return user
    
    @staticmethod
    def create_user(db: Session, user_create: UserCreate) -> Optional[User]:
        """
        Create a new user.
        
        Args:
            db: Database session
            user_create: User creation data
            
        Returns:
            Created user or None if creation fails
            
        Raises:
            ValueError: If password validation fails
        """
        try:
            # Validate password before hashing
            password = user_create.password
            
            # Check minimum length
            if len(password) < 8:
                raise ValueError("Password must be at least 8 characters long")
            
            # Check maximum length (bcrypt limit is 72 bytes)
            password_bytes = password.encode('utf-8')
            password_byte_length = len(password_bytes)
            
            if password_byte_length > 72:
                raise ValueError(
                    f"Password is too long. Maximum length is 72 bytes "
                    f"(approximately 72 ASCII characters). "
                    f"Your password is {password_byte_length} bytes. "
                    f"Please use a shorter password."
                )
            
            # Hash the password
            hashed_password = get_password_hash(password)
            
            # Create user
            db_user = User(
                email=user_create.email,
                username=user_create.username,
                hashed_password=hashed_password,
                full_name=user_create.full_name,
            )
            
            db.add(db_user)
            db.commit()
            db.refresh(db_user)
            
            logger.info(f"User created: {db_user.username} ({db_user.email})")
            return db_user
            
        except ValueError as e:
            # Re-raise validation errors
            db.rollback()
            raise
        except IntegrityError as e:
            db.rollback()
            logger.error(f"Failed to create user (integrity error): {e}")
            return None
        except Exception as e:
            db.rollback()
            logger.error(f"Failed to create user (unexpected error): {e}", exc_info=True)
            return None
    
    @staticmethod
    def update_user(db: Session, user_id: int, user_update: UserUpdate) -> Optional[User]:
        """Update user information."""
        db_user = UserService.get_user_by_id(db, user_id)
        if not db_user:
            return None
        
        update_data = user_update.model_dump(exclude_unset=True)
        
        # Hash password if provided
        if "password" in update_data:
            update_data["hashed_password"] = get_password_hash(update_data.pop("password"))
        
        for field, value in update_data.items():
            setattr(db_user, field, value)
        
        db.commit()
        db.refresh(db_user)
        
        logger.info(f"User updated: {db_user.username}")
        return db_user
    
    @staticmethod
    def authenticate_user(db: Session, username: str, password: str) -> Optional[User]:
        """
        Authenticate a user.
        
        Args:
            db: Database session
            username: Username or email
            password: Plain text password
            
        Returns:
            User if authentication succeeds, None otherwise
        """
        user = UserService.get_user_by_username_or_email(db, username)
        if not user:
            return None
        
        if not verify_password(password, user.hashed_password):
            return None
        
        if not user.is_active:
            logger.warning(f"Attempted login for inactive user: {username}")
            return None
        
        return user
    
    @staticmethod
    def delete_user(db: Session, user_id: int) -> bool:
        """Delete a user (soft delete by setting is_active=False)."""
        db_user = UserService.get_user_by_id(db, user_id)
        if not db_user:
            return False
        
        db_user.is_active = False
        db.commit()
        
        logger.info(f"User deactivated: {db_user.username}")
        return True

