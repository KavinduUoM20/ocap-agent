"""Session and Workflow Execution models."""
from datetime import datetime
from sqlalchemy import Column, String, Integer, ForeignKey, DateTime, Text, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship
import uuid
import enum
from app.infra.database import Base


class SessionStatus(enum.Enum):
    """Session status enum."""
    ACTIVE = "active"
    ARCHIVED = "archived"
    CLOSED = "closed"


class WorkflowStatus(enum.Enum):
    """Workflow execution status enum."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


class Session(Base):
    """Session (Thread) model for user conversations."""
    
    __tablename__ = "sessions"
    
    thread_id = Column(String, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    title = Column(String, nullable=True)
    status = Column(SQLEnum(SessionStatus), default=SessionStatus.ACTIVE, nullable=False)
    message_count = Column(Integer, default=0, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    last_activity_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    # Relationship
    workflow_executions = relationship("WorkflowExecution", back_populates="session", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<Session(thread_id={self.thread_id}, user_id={self.user_id}, status={self.status})>"


class WorkflowExecution(Base):
    """Workflow execution model for tracking each workflow run."""
    
    __tablename__ = "workflow_executions"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    thread_id = Column(String, ForeignKey("sessions.thread_id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    query = Column(Text, nullable=False)
    response = Column(Text, nullable=True)
    status = Column(SQLEnum(WorkflowStatus), default=WorkflowStatus.PENDING, nullable=False, index=True)
    classification = Column(String, nullable=True, index=True)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    completed_at = Column(DateTime, nullable=True)
    duration_ms = Column(Integer, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    # Relationship
    session = relationship("Session", back_populates="workflow_executions")
    
    def __repr__(self):
        return f"<WorkflowExecution(id={self.id}, thread_id={self.thread_id}, status={self.status})>"

