"""
Database connection utilities and helper functions

Production-level module with comprehensive logging, error handling, and monitoring.
"""
import logging
import logging.handlers
import os
import sys
import time
import traceback
from pathlib import Path
from datetime import datetime
from typing import Optional, Union, Dict, Any, List
from contextlib import contextmanager
from sqlalchemy import create_engine, pool
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import json
from config import config

# ============================================
# PRODUCTION LOGGING CONFIGURATION
# ============================================

def setup_production_logger(name: str = __name__) -> logging.Logger:
    """
    Configure production-level logger with structured logging, rotation, and formatting.
    
    Features:
        - Multiple log levels (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        - Rotating file handler with size and time-based rotation
        - Console output with color coding
        - Structured log format with timestamps, module, function, line numbers
        - Exception tracking with full stack traces
        - Performance metrics integration
    
    Args:
        name: Logger name (typically __name__ of the module)
    
    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)
    
    # Prevent duplicate handlers
    if logger.handlers:
        return logger
    
    # Set base logging level from config or environment
    log_level = getattr(logging, os.getenv('LOG_LEVEL', 'INFO').upper(), logging.INFO)
    logger.setLevel(log_level)
    
    # Create logs directory if it doesn't exist
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    
    # Detailed formatter for file logs
    file_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(name)s:%(funcName)s:%(lineno)d | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # Simpler formatter for console
    console_formatter = logging.Formatter(
        fmt='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # Rotating File Handler - creates new file when size exceeds limit
    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_dir / f'{name.replace(".", "_")}.log',
        maxBytes=10 * 1024 * 1024,  # 10 MB per file
        backupCount=5,  # Keep 5 backup files
        encoding='utf-8'
    )
    file_handler.setLevel(logging.DEBUG)  # Capture all levels in file
    file_handler.setFormatter(file_formatter)
    
    # Time-based rotating handler for daily logs
    time_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_dir / 'app.log',
        when='midnight',
        interval=1,
        backupCount=30,  # Keep 30 days of logs
        encoding='utf-8'
    )
    time_handler.setLevel(logging.INFO)
    time_handler.setFormatter(file_formatter)
    
    # Console Handler for real-time monitoring
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)
    
    # Error file handler - separate file for errors only
    error_handler = logging.handlers.RotatingFileHandler(
        filename=log_dir / 'errors.log',
        maxBytes=10 * 1024 * 1024,
        backupCount=10,
        encoding='utf-8'
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(file_formatter)
    
    # Add all handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(time_handler)
    logger.addHandler(console_handler)
    logger.addHandler(error_handler)
    
    return logger


# Initialize production logger
logger = setup_production_logger(__name__)
logger.info("=" * 80)
logger.info(f"Production logger initialized for {__name__}")
logger.info(f"Log level: {logging.getLevelName(logger.level)}")
logger.info(f"Python version: {sys.version.split()[0]}")
logger.info(f"Working directory: {os.getcwd()}")
logger.info("=" * 80)


def log_function_call(func_name: str, **kwargs):
    """Log function call with parameters (excluding sensitive data)."""
    # Mask sensitive parameters
    masked_params = {}
    sensitive_keys = {'password', 'token', 'api_key', 'secret', 'auth', 'credential'}
    
    for key, value in kwargs.items():
        if any(sens in key.lower() for sens in sensitive_keys):
            masked_params[key] = "***REDACTED***"
        elif isinstance(value, (str, int, float, bool)) or value is None:
            masked_params[key] = value
        else:
            masked_params[key] = f"<{type(value).__name__}>"
    
    logger.debug(f"Function call: {func_name}({', '.join(f'{k}={v}' for k, v in masked_params.items())})")


def log_performance(operation: str, start_time: float, **context):
    """Log operation performance metrics."""
    duration = time.time() - start_time
    
    if duration > 5.0:
        logger.warning(f"SLOW OPERATION: {operation} took {duration:.3f}s | Context: {context}")
    elif duration > 1.0:
        logger.info(f"Operation: {operation} completed in {duration:.3f}s | Context: {context}")
    else:
        logger.debug(f"Operation: {operation} completed in {duration:.3f}s | Context: {context}")
    
    return duration


def log_error_with_context(error: Exception, operation: str, **context):
    """Log error with full context and stack trace."""
    error_details = {
        'operation': operation,
        'error_type': type(error).__name__,
        'error_message': str(error),
        'context': context,
        'timestamp': datetime.now().isoformat()
    }
    
    logger.error(
        f"ERROR in {operation}: {type(error).__name__}: {str(error)} | "
        f"Context: {context}",
        exc_info=True  # This includes full stack trace
    )
    
    return error_details

# ============================================
# DATABASE ENGINE CONFIGURATION
# ============================================

try:
    logger.info("Initializing database engine...")
    logger.debug(f"Database URL configured: {config.DATABASE_URL.split('@')[-1] if '@' in config.DATABASE_URL else 'Local Database'}")
    
    # Create database engine with production-grade connection pooling
    engine = create_engine(
        config.DATABASE_URL,
        poolclass=pool.QueuePool,
        pool_size=5,  # Number of permanent connections
        max_overflow=10,  # Additional connections when pool is exhausted
        pool_pre_ping=True,  # Verify connections before using them
        pool_recycle=3600,  # Recycle connections after 1 hour (3600 seconds)
        pool_timeout=30,  # Timeout for getting connection from pool
        echo=config.DEBUG,  # Log SQL queries in debug mode
        echo_pool=False,  # Set to True for connection pool debugging
        connect_args={
            "connect_timeout": 10,  # Connection timeout in seconds
        }
    )
    
    # Create SessionLocal class for database sessions
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    logger.info("Database engine initialized successfully")
    logger.info(f"Connection pool: size={engine.pool.size()}, overflow={engine.pool._max_overflow}")
    
except Exception as e:
    logger.critical(f"Failed to initialize database engine: {str(e)}", exc_info=True)
    raise RuntimeError(f"Database initialization failed: {str(e)}") from e


def get_db_connection():
    """
    Get a database connection session with comprehensive logging and error handling.
    
    This function provides a database session with automatic transaction management,
    connection pooling, error handling, and performance monitoring.
    
    Features:
        - Automatic transaction commit on success
        - Automatic rollback on error
        - Connection cleanup in finally block
        - Performance monitoring
        - Detailed error logging
        - Connection pool status tracking
    
    Returns:
        Session: SQLAlchemy database session
    
    Yields:
        Session: Database session for use in with statement
    
    Usage:
        with get_db_connection() as db:
            # Use db session here
            results = db.execute(query)
            # Automatic commit on success, rollback on error
    
    Raises:
        SQLAlchemyError: For database-specific errors
        Exception: For unexpected errors
    
    Example:
        >>> with get_db_connection() as db:
        ...     users = db.execute("SELECT * FROM users").fetchall()
        ...     logger.info(f"Retrieved {len(users)} users")
    """
    db = SessionLocal()
    start_time = time.time()
    
    try:
        # Log connection pool status
        pool_status = engine.pool.status()
        logger.debug(f"DB connection acquired | Pool status: {pool_status}")
        
        yield db
        
        # Commit transaction on success
        db.commit()
        duration = log_performance("db_connection", start_time)
        logger.debug(f"DB transaction committed successfully | Duration: {duration:.3f}s")
        
    except SQLAlchemyError as e:
        logger.error(
            f"Database error occurred: {type(e).__name__}: {str(e)}",
            exc_info=True
        )
        logger.warning("Rolling back transaction...")
        db.rollback()
        logger.info("Transaction rolled back successfully")
        
        # Re-raise with additional context
        raise SQLAlchemyError(f"Database operation failed: {str(e)}") from e
        
    except Exception as e:
        logger.error(
            f"Unexpected error in database operation: {type(e).__name__}: {str(e)}",
            exc_info=True
        )
        logger.warning("Rolling back transaction...")
        db.rollback()
        logger.info("Transaction rolled back successfully")
        
        # Re-raise with context
        raise RuntimeError(f"Unexpected database error: {str(e)}") from e
        
    finally:
        # Always close the session
        try:
            db.close()
            logger.debug("DB connection closed")
        except Exception as e:
            logger.error(f"Error closing database connection: {str(e)}", exc_info=True)


@contextmanager
def get_db_session():
    """
    Context manager for database sessions with production-level error handling.
    
    Provides automatic commit/rollback, connection cleanup, performance monitoring,
    and comprehensive error logging.
    
    Features:
        - Automatic transaction management
        - Connection pool monitoring
        - Performance metrics
        - Detailed error logging with stack traces
        - Graceful cleanup
    
    Yields:
        Session: Database session
    
    Usage:
        with get_db_session() as session:
            # Use session here
            session.execute(query)
            # Automatic commit on success, rollback on error
    
    Raises:
        SQLAlchemyError: For database-specific errors
        Exception: For unexpected errors
    
    Example:
        >>> with get_db_session() as session:
        ...     result = session.execute("SELECT COUNT(*) FROM products")
        ...     count = result.scalar()
        ...     logger.info(f"Total products: {count}")
    """
    session = SessionLocal()
    start_time = time.time()
    operation_id = f"session_{int(time.time() * 1000)}"
    
    try:
        pool_status = engine.pool.status()
        logger.debug(f"[{operation_id}] DB session created | Pool: {pool_status}")
        
        yield session
        
        # Commit transaction
        session.commit()
        duration = log_performance(f"db_session_{operation_id}", start_time)
        logger.debug(f"[{operation_id}] Session committed | Duration: {duration:.3f}s")
        
    except SQLAlchemyError as e:
        log_error_with_context(
            error=e,
            operation=f"db_session_{operation_id}",
            error_type="SQLAlchemyError",
            session_id=operation_id
        )
        
        logger.warning(f"[{operation_id}] Rolling back transaction...")
        session.rollback()
        logger.info(f"[{operation_id}] Transaction rolled back")
        
        raise
        
    except Exception as e:
        log_error_with_context(
            error=e,
            operation=f"db_session_{operation_id}",
            error_type="UnexpectedError",
            session_id=operation_id
        )
        
        logger.warning(f"[{operation_id}] Rolling back transaction...")
        session.rollback()
        logger.info(f"[{operation_id}] Transaction rolled back")
        
        raise
        
    finally:
        try:
            session.close()
            logger.debug(f"[{operation_id}] Session closed")
        except Exception as e:
            logger.error(
                f"[{operation_id}] Error closing session: {str(e)}",
                exc_info=True
            )


def test_db_connection() -> Dict[str, Any]:
    """
    Test database connectivity with comprehensive diagnostics.
    
    Performs connection test, latency measurement, and pool status check.
    Provides detailed information for troubleshooting connection issues.
    
    Returns:
        dict: Test results including:
            - success: bool - Connection successful
            - latency_ms: float - Connection latency in milliseconds
            - pool_status: str - Connection pool status
            - database_version: str - Database version info
            - error: str - Error message if failed
    
    Example:
        >>> result = test_db_connection()
        >>> if result['success']:
        ...     logger.info(f"Database connected | Latency: {result['latency_ms']:.2f}ms")
        ... else:
        ...     logger.error(f"Connection failed: {result['error']}")
    """
    start_time = time.time()
    result = {
        'success': False,
        'latency_ms': 0,
        'pool_status': None,
        'database_version': None,
        'error': None,
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        logger.info("Testing database connection...")
        
        with engine.connect() as connection:
            # Test query
            connection.execute("SELECT 1")
            
            # Get database version
            try:
                version_result = connection.execute("SELECT version()")
                result['database_version'] = version_result.scalar()
            except:
                result['database_version'] = "Version info not available"
        
        # Calculate latency
        latency = (time.time() - start_time) * 1000  # Convert to milliseconds
        result['latency_ms'] = round(latency, 2)
        
        # Get pool status
        result['pool_status'] = engine.pool.status()
        
        result['success'] = True
        
        logger.info(f"✓ Database connection successful | Latency: {latency:.2f}ms")
        logger.debug(f"Pool status: {result['pool_status']}")
        logger.debug(f"Database: {result['database_version']}")
        
        return result
        
    except Exception as e:
        result['error'] = str(e)
        result['latency_ms'] = round((time.time() - start_time) * 1000, 2)
        
        logger.error(
            f"✗ Database connection failed | Error: {type(e).__name__}: {str(e)} | "
            f"Latency: {result['latency_ms']:.2f}ms",
            exc_info=True
        )
        
        return result


def close_db_connection():
    """
    Close all database connections and dispose of the engine.
    
    Should be called on application shutdown to ensure graceful cleanup.
    Disposes of connection pool and releases all database resources.
    
    Features:
        - Closes all active connections
        - Disposes of connection pool
        - Logs connection pool statistics
        - Handles errors gracefully
    
    Example:
        >>> # On application shutdown
        >>> close_db_connection()
        >>> logger.info("Application shutdown complete")
    """
    try:
        logger.info("Closing database connections...")
        
        # Get pool statistics before closing
        pool_status = engine.pool.status()
        logger.info(f"Final pool status: {pool_status}")
        
        # Dispose of engine and close all connections
        engine.dispose()
        
        logger.info("✓ Database connections closed successfully")
        logger.info("✓ Connection pool disposed")
        
    except Exception as e:
        logger.error(
            f"✗ Error closing database connections: {type(e).__name__}: {str(e)}",
            exc_info=True
        )
        raise


def save_processed_data(
    df: pd.DataFrame,
    filename: Optional[str] = None,
    output_dir: str = "data/processed",
    file_format: str = "csv",
    include_timestamp: bool = True,
    include_index: bool = False,
    compression: Optional[str] = None,
    create_backup: bool = False,
    metadata: Optional[Dict[str, Any]] = None,
    validate_before_save: bool = True,
    overwrite: bool = False
) -> Dict[str, Any]:
    """
    Save processed data to file with production-level features and error handling.
    
    This function provides comprehensive data persistence capabilities including:
    - Multiple file format support (CSV, Excel, Parquet, JSON, Pickle)
    - Automatic directory creation
    - Timestamped filenames for version control
    - Data validation before saving
    - Backup creation for existing files
    - Compression support
    - Metadata storage
    - Detailed operation reporting
    - Performance monitoring
    - Comprehensive error handling and logging
    
    Args:
        df: DataFrame to save
        filename: Base filename (without extension). If None, auto-generates from timestamp
        output_dir: Output directory path (default: 'data/processed')
        file_format: Output format - 'csv', 'excel', 'parquet', 'json', 'pickle' (default: 'csv')
        include_timestamp: Add timestamp to filename for versioning
        include_index: Whether to include DataFrame index in output
        compression: Compression method - 'gzip', 'bz2', 'zip', 'xz', None
        create_backup: Create backup of existing file before overwriting
        metadata: Dictionary of metadata to save alongside data
        validate_before_save: Validate DataFrame before saving
        overwrite: Allow overwriting existing files
    
    Returns:
        Dictionary containing:
            - 'success': bool - Whether save was successful
            - 'filepath': str - Full path to saved file
            - 'filename': str - Name of saved file
            - 'format': str - File format used
            - 'size_bytes': int - File size in bytes
            - 'size_mb': float - File size in MB
            - 'rows': int - Number of rows saved
            - 'columns': int - Number of columns saved
            - 'timestamp': str - ISO timestamp of save operation
            - 'backup_created': bool - Whether backup was created
            - 'metadata_saved': bool - Whether metadata was saved
            - 'processing_time_seconds': float - Time taken to save
    
    Raises:
        ValueError: If invalid parameters or empty DataFrame
        IOError: If file operations fail
        Exception: For other unexpected errors
    
    Examples:
        >>> # Basic CSV save
        >>> result = save_processed_data(df, filename='cleaned_data')
        >>> print(f"Saved to: {result['filepath']}")
        
        >>> # Save with compression and metadata
        >>> metadata = {
        ...     'source': 'raw_data.csv',
        ...     'processing_date': '2026-05-06',
        ...     'transformations': ['clean', 'normalize', 'aggregate']
        ... }
        >>> result = save_processed_data(
        ...     df,
        ...     filename='processed_sales',
        ...     file_format='parquet',
        ...     compression='gzip',
        ...     metadata=metadata
        ... )
        >>> print(f"Saved {result['rows']} rows to {result['filename']}")
    """
    start_time = time.time()
    operation_id = f"save_{int(time.time() * 1000)}"
    
    # Log function call
    log_function_call(
        "save_processed_data",
        filename=filename,
        output_dir=output_dir,
        file_format=file_format,
        rows=len(df) if df is not None else 0,
        columns=len(df.columns) if df is not None else 0
    )
    
    # Input validation
    try:
        if df is None or not isinstance(df, pd.DataFrame):
            raise ValueError("Input must be a pandas DataFrame")
        
        if df.empty:
            raise ValueError("Cannot save empty DataFrame")
        
        logger.info(
            f"[{operation_id}] Starting save operation | "
            f"Rows: {len(df):,} | Columns: {len(df.columns)} | Format: {file_format}"
        )
        
    except ValueError as e:
        log_error_with_context(e, operation_id, validation="input_check")
        raise
    
    # Validate file format
    valid_formats = ['csv', 'excel', 'xlsx', 'parquet', 'json', 'pickle', 'pkl']
    if file_format.lower() not in valid_formats:
        error_msg = f"Invalid file format: {file_format}. Valid options: {valid_formats}"
        logger.error(f"[{operation_id}] {error_msg}")
        raise ValueError(error_msg)
    
    # Normalize format names
    format_map = {
        'xlsx': 'excel',
        'pkl': 'pickle'
    }
    file_format = format_map.get(file_format.lower(), file_format.lower())
    logger.debug(f"[{operation_id}] Normalized format: {file_format}")
    
    try:
        # Validate data if requested
        if validate_before_save:
            logger.info(f"[{operation_id}] Validating DataFrame before save...")
            _validate_dataframe_for_save(df, file_format)
            logger.info(f"[{operation_id}] ✓ Validation passed")
        
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"[{operation_id}] Output directory ensured: {output_path.absolute()}")
        
        # Generate filename if not provided
        if filename is None:
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"processed_data_{timestamp_str}"
            logger.debug(f"[{operation_id}] Auto-generated filename: {filename}")
        elif include_timestamp:
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{filename}_{timestamp_str}"
            logger.debug(f"[{operation_id}] Timestamped filename: {filename}")
        
        # Add appropriate file extension
        extension_map = {
            'csv': '.csv',
            'excel': '.xlsx',
            'parquet': '.parquet',
            'json': '.json',
            'pickle': '.pkl'
        }
        
        if compression:
            compression_ext = {
                'gzip': '.gz',
                'bz2': '.bz2',
                'zip': '.zip',
                'xz': '.xz'
            }
            ext = extension_map[file_format] + compression_ext.get(compression, '')
            logger.debug(f"[{operation_id}] Using compression: {compression}")
        else:
            ext = extension_map[file_format]
        
        full_filename = f"{filename}{ext}"
        filepath = output_path / full_filename
        
        logger.info(f"[{operation_id}] Target file: {filepath}")
        
        # Check if file exists and handle accordingly
        backup_created = False
        if filepath.exists():
            logger.warning(f"[{operation_id}] File already exists: {filepath}")
            
            if not overwrite:
                # Add counter to filename
                counter = 1
                while filepath.exists():
                    filepath = output_path / f"{filename}_{counter}{ext}"
                    counter += 1
                full_filename = filepath.name
                logger.info(f"[{operation_id}] Using unique filename: {full_filename}")
                
            elif create_backup:
                # Create backup of existing file
                backup_filename = f"{filename}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
                backup_path = output_path / backup_filename
                try:
                    import shutil
                    shutil.copy2(filepath, backup_path)
                    backup_created = True
                    logger.info(f"[{operation_id}] ✓ Backup created: {backup_filename}")
                except Exception as e:
                    logger.warning(f"[{operation_id}] Failed to create backup: {str(e)}")
        
        # Save based on format
        logger.info(f"[{operation_id}] Writing file to disk...")
        write_start = time.time()
        
        if file_format == 'csv':
            df.to_csv(
                filepath,
                index=include_index,
                compression=compression
            )
        
        elif file_format == 'excel':
            # Excel doesn't support compression parameter directly
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                df.to_excel(writer, index=include_index, sheet_name='Data')
        
        elif file_format == 'parquet':
            # Parquet has built-in compression
            compression_param = compression if compression else 'snappy'
            df.to_parquet(
                filepath,
                index=include_index,
                compression=compression_param
            )
        
        elif file_format == 'json':
            df.to_json(
                filepath,
                orient='records',
                indent=2,
                compression=compression
            )
        
        elif file_format == 'pickle':
            df.to_pickle(
                filepath,
                compression=compression
            )
        
        write_duration = time.time() - write_start
        logger.info(f"[{operation_id}] ✓ File written | Duration: {write_duration:.3f}s")
        
        # Get file size
        file_size_bytes = filepath.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        logger.info(
            f"[{operation_id}] File size: {file_size_bytes:,} bytes ({file_size_mb:.2f} MB)"
        )
        
        # Save metadata if provided
        metadata_saved = False
        if metadata:
            logger.info(f"[{operation_id}] Saving metadata...")
            metadata_path = output_path / f"{filename}_metadata.json"
            try:
                # Add automatic metadata
                full_metadata = {
                    'filename': full_filename,
                    'filepath': str(filepath.absolute()),
                    'format': file_format,
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': list(df.columns),
                    'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
                    'size_bytes': file_size_bytes,
                    'size_mb': round(file_size_mb, 2),
                    'created_timestamp': datetime.now().isoformat(),
                    'compression': compression,
                    'user_metadata': metadata
                }
                
                with open(metadata_path, 'w') as f:
                    json.dump(full_metadata, f, indent=2, default=str)
                
                metadata_saved = True
                logger.info(f"[{operation_id}] ✓ Metadata saved: {metadata_path.name}")
            except Exception as e:
                logger.warning(f"[{operation_id}] Failed to save metadata: {str(e)}")
        
        # Calculate total processing time
        total_duration = log_performance(operation_id, start_time, 
                                        filename=full_filename, 
                                        size_mb=file_size_mb)
        
        # Create result report
        result = {
            'success': True,
            'filepath': str(filepath.absolute()),
            'filename': full_filename,
            'format': file_format,
            'size_bytes': file_size_bytes,
            'size_mb': round(file_size_mb, 4),
            'rows': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'timestamp': datetime.now().isoformat(),
            'backup_created': backup_created,
            'metadata_saved': metadata_saved,
            'compression': compression,
            'processing_time_seconds': round(total_duration, 3)
        }
        
        logger.info(
            f"[{operation_id}] ✓ SAVE SUCCESSFUL | "
            f"File: {full_filename} | "
            f"Size: {result['size_mb']:.2f} MB | "
            f"Rows: {result['rows']:,} | "
            f"Duration: {total_duration:.3f}s"
        )
        
        return result
        
    except Exception as e:
        log_error_with_context(
            error=e,
            operation=operation_id,
            filename=filename,
            format=file_format,
            output_dir=output_dir
        )
        
        return {
            'success': False,
            'error': str(e),
            'timestamp': datetime.now().isoformat()
        }


def _validate_dataframe_for_save(df: pd.DataFrame, file_format: str) -> None:
    """
    Validate DataFrame before saving to ensure data quality.
    
    Args:
        df: DataFrame to validate
        file_format: Target file format
    
    Raises:
        ValueError: If validation fails
    """
    # Check for all-null columns
    null_columns = df.columns[df.isnull().all()].tolist()
    if null_columns:
        logger.warning(
            f"DataFrame contains {len(null_columns)} all-null columns: {null_columns}"
        )
    
    # Check for duplicate column names
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        raise ValueError(
            f"DataFrame contains duplicate column names: {duplicate_cols}"
        )
    
    # Format-specific validation
    if file_format == 'parquet':
        # Parquet doesn't handle certain dtypes well
        problematic_dtypes = df.select_dtypes(include=['object', 'category']).columns
        if len(problematic_dtypes) > 0:
            logger.info(
                f"Note: {len(problematic_dtypes)} object/category columns in Parquet format"
            )
    
    # Check memory usage
    memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    if memory_mb > 1000:  # More than 1GB
        logger.warning(
            f"Large DataFrame detected: {memory_mb:.2f} MB in memory. "
            "Consider using compression or chunking."
        )


def load_processed_data(
    filepath: str,
    file_format: Optional[str] = None,
    **kwargs
) -> pd.DataFrame:
    """
    Load processed data from file with automatic format detection and comprehensive logging.
    
    Features:
        - Automatic format detection from file extension
        - Multiple format support (CSV, Excel, Parquet, JSON, Pickle)
        - Performance monitoring
        - Detailed error logging
        - Memory usage tracking
    
    Args:
        filepath: Path to file to load
        file_format: File format (auto-detected from extension if None)
        **kwargs: Additional arguments to pass to pandas read function
    
    Returns:
        DataFrame with loaded data
    
    Raises:
        FileNotFoundError: If file doesn't exist
        ValueError: If unsupported format
        Exception: For loading errors
    
    Example:
        >>> df = load_processed_data('data/processed/cleaned_data.csv')
        >>> df = load_processed_data('data/processed/export.parquet', file_format='parquet')
    """
    start_time = time.time()
    operation_id = f"load_{int(time.time() * 1000)}"
    
    filepath = Path(filepath)
    
    logger.info(f"[{operation_id}] Loading data from: {filepath}")
    
    if not filepath.exists():
        error_msg = f"File not found: {filepath}"
        logger.error(f"[{operation_id}] {error_msg}")
        raise FileNotFoundError(error_msg)
    
    # Get file size for logging
    file_size_mb = filepath.stat().st_size / (1024 * 1024)
    logger.debug(f"[{operation_id}] File size: {file_size_mb:.2f} MB")
    
    # Auto-detect format from extension if not provided
    if file_format is None:
        ext = filepath.suffix.lower()
        format_map = {
            '.csv': 'csv',
            '.xlsx': 'excel',
            '.xls': 'excel',
            '.parquet': 'parquet',
            '.json': 'json',
            '.pkl': 'pickle',
            '.pickle': 'pickle'
        }
        file_format = format_map.get(ext, 'csv')
        logger.debug(f"[{operation_id}] Auto-detected format: {file_format}")
    
    try:
        logger.info(f"[{operation_id}] Reading file with format: {file_format}")
        
        if file_format == 'csv':
            df = pd.read_csv(filepath, **kwargs)
        elif file_format == 'excel':
            df = pd.read_excel(filepath, **kwargs)
        elif file_format == 'parquet':
            df = pd.read_parquet(filepath, **kwargs)
        elif file_format == 'json':
            df = pd.read_json(filepath, **kwargs)
        elif file_format == 'pickle':
            df = pd.read_pickle(filepath, **kwargs)
        else:
            error_msg = f"Unsupported file format: {file_format}"
            logger.error(f"[{operation_id}] {error_msg}")
            raise ValueError(error_msg)
        
        # Log DataFrame info
        memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
        
        duration = log_performance(
            operation_id,
            start_time,
            rows=len(df),
            columns=len(df.columns),
            memory_mb=memory_mb
        )
        
        logger.info(
            f"[{operation_id}] ✓ Data loaded successfully | "
            f"Rows: {len(df):,} | Columns: {len(df.columns)} | "
            f"Memory: {memory_mb:.2f} MB | Duration: {duration:.3f}s"
        )
        
        return df
        
    except Exception as e:
        log_error_with_context(
            error=e,
            operation=operation_id,
            filepath=str(filepath),
            format=file_format
        )
        raise


def list_processed_files(
    directory: str = "data/processed",
    file_format: Optional[str] = None,
    sort_by: str = "modified"
) -> List[Dict[str, Any]]:
    """
    List all processed data files in directory with metadata and comprehensive logging.
    
    Features:
        - File filtering by format
        - Sorting by name, modified time, or size
        - Detailed file metadata
        - Error handling for missing directories
    
    Args:
        directory: Directory to search
        file_format: Filter by specific format (None for all)
        sort_by: Sort by 'name', 'modified', or 'size'
    
    Returns:
        List of dictionaries with file information:
            - name: File name
            - path: Absolute path
            - size_bytes: Size in bytes
            - size_mb: Size in MB
            - modified: Last modified timestamp
            - extension: File extension
    
    Example:
        >>> files = list_processed_files('data/processed', file_format='csv')
        >>> for file in files:
        ...     print(f"{file['name']}: {file['size_mb']:.2f} MB")
    """
    operation_id = f"list_{int(time.time() * 1000)}"
    
    logger.info(f"[{operation_id}] Listing files in: {directory}")
    if file_format:
        logger.debug(f"[{operation_id}] Filtering by format: {file_format}")
    
    dir_path = Path(directory)
    
    if not dir_path.exists():
        logger.warning(f"[{operation_id}] Directory does not exist: {directory}")
        return []
    
    # Define patterns for different formats
    patterns = {
        'csv': '*.csv*',
        'excel': '*.xlsx',
        'parquet': '*.parquet',
        'json': '*.json',
        'pickle': '*.pkl'
    }
    
    files_info = []
    
    try:
        if file_format:
            pattern = patterns.get(file_format, '*.*')
            files = list(dir_path.glob(pattern))
        else:
            files = [f for f in dir_path.iterdir() if f.is_file() and not f.name.endswith('_metadata.json')]
        
        logger.debug(f"[{operation_id}] Found {len(files)} files")
        
        for file in files:
            try:
                stat = file.stat()
                files_info.append({
                    'name': file.name,
                    'path': str(file.absolute()),
                    'size_bytes': stat.st_size,
                    'size_mb': round(stat.st_size / (1024 * 1024), 2),
                    'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
                    'extension': file.suffix
                })
            except Exception as e:
                logger.warning(f"[{operation_id}] Error reading file {file.name}: {str(e)}")
                continue
        
        # Sort files
        if sort_by == 'name':
            files_info.sort(key=lambda x: x['name'])
        elif sort_by == 'size':
            files_info.sort(key=lambda x: x['size_bytes'], reverse=True)
        else:  # modified
            files_info.sort(key=lambda x: x['modified'], reverse=True)
        
        logger.info(f"[{operation_id}] ✓ Listed {len(files_info)} files | Sorted by: {sort_by}")
        
        return files_info
        
    except Exception as e:
        log_error_with_context(
            error=e,
            operation=operation_id,
            directory=directory,
            format=file_format
        )
        return []
