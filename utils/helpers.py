"""
Database connection utilities and helper functions
"""
import logging
from contextlib import contextmanager
from sqlalchemy import create_engine, pool
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
from config import config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create database engine with connection pooling
engine = create_engine(
    config.DATABASE_URL,
    poolclass=pool.QueuePool,
    pool_size=5,
    max_overflow=10,
    pool_pre_ping=True,  # Verify connections before using them
    pool_recycle=3600,   # Recycle connections after 1 hour
    echo=config.DEBUG    # Log SQL queries in debug mode
)

# Create SessionLocal class for database sessions
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db_connection():
    """
    Get a database connection session.
    
    Returns:
        Session: SQLAlchemy database session
        
    Usage:
        with get_db_connection() as db:
            # Use db session here
            results = db.execute(query)
    """
    db = SessionLocal()
    try:
        yield db
        db.commit()
    except SQLAlchemyError as e:
        logger.error(f"Database error occurred: {str(e)}")
        db.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error occurred: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()


@contextmanager
def get_db_session():
    """
    Context manager for database sessions.
    Provides automatic commit/rollback and connection cleanup.
    
    Usage:
        with get_db_session() as session:
            # Use session here
            session.execute(query)
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        session.rollback()
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()


def test_db_connection():
    """
    Test database connectivity.
    
    Returns:
        bool: True if connection is successful, False otherwise
    """
    try:
        with engine.connect() as connection:
            connection.execute("SELECT 1")
        logger.info("Database connection successful")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {str(e)}")
        return False


def close_db_connection():
    """
    Close all database connections and dispose of the engine.
    Should be called on application shutdown.
    """
    try:
        engine.dispose()
        logger.info("Database connections closed successfully")
    except Exception as e:
        logger.error(f"Error closing database connections: {str(e)}")
