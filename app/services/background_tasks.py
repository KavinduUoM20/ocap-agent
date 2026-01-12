"""Background task system for async database operations."""
import threading
import queue
import time
from typing import Optional, Dict, Any
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from app.infra.database import get_session, is_database_available
from app.models.session import Session, WorkflowExecution, SessionStatus, WorkflowStatus
from app.core.logging import logger


class BackgroundTaskQueue:
    """Thread-safe queue for background database tasks."""
    
    def __init__(self, max_workers: int = 2):
        """
        Initialize background task queue.
        
        Args:
            max_workers: Maximum number of worker threads
        """
        self.task_queue = queue.Queue()
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="bg_task")
        self.running = True
        self._start_workers()
        logger.info(f"Background task queue initialized with {max_workers} workers")
    
    def _start_workers(self):
        """Start worker threads to process tasks."""
        for i in range(self.executor._max_workers):
            self.executor.submit(self._worker)
    
    def _worker(self):
        """Worker thread that processes tasks from the queue."""
        while self.running:
            try:
                task = self.task_queue.get(timeout=1)
                if task is None:  # Shutdown signal
                    break
                
                task_type, task_data = task
                
                try:
                    if task_type == "create_or_update_session":
                        self._create_or_update_session(task_data)
                    elif task_type == "create_workflow_execution":
                        self._create_workflow_execution(task_data)
                    elif task_type == "update_workflow_execution":
                        self._update_workflow_execution(task_data)
                    else:
                        logger.warning(f"Unknown task type: {task_type}")
                    
                    self.task_queue.task_done()
                except Exception as e:
                    logger.error(f"Error processing background task {task_type}: {e}", exc_info=True)
                    self.task_queue.task_done()
                    
            except queue.Empty:
                continue
            except Exception as e:
                logger.error(f"Error in background worker: {e}", exc_info=True)
    
    def submit(self, task_type: str, task_data: Dict[str, Any]):
        """
        Submit a task to the background queue.
        
        Args:
            task_type: Type of task (create_or_update_session, create_workflow_execution, etc.)
            task_data: Task data dictionary
        """
        if not self.running:
            logger.warning("Background task queue is not running, task discarded")
            return
        
        try:
            self.task_queue.put((task_type, task_data), block=False)
        except queue.Full:
            logger.warning("Background task queue is full, task discarded")
    
    def _create_or_update_session(self, data: Dict[str, Any]):
        """Create or update a session in the database."""
        if not is_database_available():
            logger.debug("Database not available, skipping session update")
            return
        
        db = get_session()
        if db is None:
            logger.debug("Could not get database session, skipping session update")
            return
        
        try:
            thread_id = data.get("thread_id")
            user_id = data.get("user_id")
            
            if not thread_id or not user_id:
                logger.warning("Missing thread_id or user_id for session update")
                return
            
            # Try to get existing session
            session = db.query(Session).filter(Session.thread_id == thread_id).first()
            
            if session:
                # Update existing session
                session.last_activity_at = datetime.utcnow()
                session.updated_at = datetime.utcnow()
                if data.get("increment_message_count"):
                    session.message_count += 1
                if data.get("title"):
                    session.title = data.get("title")
                if data.get("status"):
                    session.status = SessionStatus(data.get("status"))
            else:
                # Create new session
                session = Session(
                    thread_id=thread_id,
                    user_id=user_id,
                    title=data.get("title"),
                    status=SessionStatus(data.get("status", "active")),
                    message_count=1,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                    last_activity_at=datetime.utcnow()
                )
                db.add(session)
            
            db.commit()
            logger.debug(f"Session {thread_id} updated in database")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating/updating session: {e}", exc_info=True)
        finally:
            db.close()
    
    def _create_workflow_execution(self, data: Dict[str, Any]):
        """Create a new workflow execution record."""
        if not is_database_available():
            logger.debug("Database not available, skipping workflow execution creation")
            return
        
        db = get_session()
        if db is None:
            logger.debug("Could not get database session, skipping workflow execution creation")
            return
        
        try:
            import uuid as uuid_lib
            workflow_run_id = data.get("workflow_run_id")
            # Convert string UUID to UUID object if needed
            if isinstance(workflow_run_id, str):
                workflow_run_id = uuid_lib.UUID(workflow_run_id)
            
            workflow_execution = WorkflowExecution(
                id=workflow_run_id,
                thread_id=data.get("thread_id"),
                user_id=data.get("user_id"),
                query=data.get("query", ""),
                response=data.get("response"),
                status=WorkflowStatus(data.get("status", "pending")),
                classification=data.get("classification"),
                started_at=datetime.utcnow(),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            
            db.add(workflow_execution)
            db.commit()
            logger.debug(f"Workflow execution {data.get('workflow_run_id')} created in database")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error creating workflow execution: {e}", exc_info=True)
        finally:
            db.close()
    
    def _update_workflow_execution(self, data: Dict[str, Any]):
        """Update an existing workflow execution record."""
        if not is_database_available():
            logger.debug("Database not available, skipping workflow execution update")
            return
        
        db = get_session()
        if db is None:
            logger.debug("Could not get database session, skipping workflow execution update")
            return
        
        try:
            import uuid as uuid_lib
            workflow_run_id = data.get("workflow_run_id")
            if not workflow_run_id:
                logger.warning("Missing workflow_run_id for update")
                return
            
            # Convert string UUID to UUID object if needed
            if isinstance(workflow_run_id, str):
                workflow_run_id = uuid_lib.UUID(workflow_run_id)
            
            workflow_execution = db.query(WorkflowExecution).filter(
                WorkflowExecution.id == workflow_run_id
            ).first()
            
            if not workflow_execution:
                logger.warning(f"Workflow execution {workflow_run_id} not found for update")
                return
            
            # Update fields
            if "response" in data:
                workflow_execution.response = data.get("response")
            if "status" in data:
                workflow_execution.status = WorkflowStatus(data.get("status"))
            if "classification" in data:
                workflow_execution.classification = data.get("classification")
            if "error_message" in data:
                workflow_execution.error_message = data.get("error_message")
            if "completed_at" in data:
                workflow_execution.completed_at = data.get("completed_at")
            if "duration_ms" in data:
                workflow_execution.duration_ms = data.get("duration_ms")
            
            workflow_execution.updated_at = datetime.utcnow()
            
            db.commit()
            logger.debug(f"Workflow execution {workflow_run_id} updated in database")
            
        except Exception as e:
            db.rollback()
            logger.error(f"Error updating workflow execution: {e}", exc_info=True)
        finally:
            db.close()
    
    def shutdown(self, wait: bool = True):
        """
        Shutdown the background task queue.
        
        Args:
            wait: Whether to wait for pending tasks to complete
        """
        self.running = False
        
        # Send shutdown signals to workers
        for _ in range(self.executor._max_workers):
            self.task_queue.put(None)
        
        if wait:
            # Wait for queue to empty
            self.task_queue.join()
        
        self.executor.shutdown(wait=wait)
        logger.info("Background task queue shut down")


# Global background task queue instance
_background_queue: Optional[BackgroundTaskQueue] = None


def get_background_queue() -> BackgroundTaskQueue:
    """Get or create the global background task queue."""
    global _background_queue
    if _background_queue is None:
        _background_queue = BackgroundTaskQueue(max_workers=2)
    return _background_queue


def shutdown_background_queue():
    """Shutdown the global background task queue."""
    global _background_queue
    if _background_queue:
        _background_queue.shutdown()
        _background_queue = None

