"""
Database connection utilities and helper functions
"""
import logging
import os
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
        
        >>> # Save multiple formats with backup
        >>> for fmt in ['csv', 'excel', 'parquet']:
        ...     result = save_processed_data(
        ...         df,
        ...         filename='export',
        ...         file_format=fmt,
        ...         create_backup=True
        ...     )
    """
    start_time = datetime.now()
    
    # Input validation
    if df is None or not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")
    
    if df.empty:
        raise ValueError("Cannot save empty DataFrame")
    
    # Validate file format
    valid_formats = ['csv', 'excel', 'xlsx', 'parquet', 'json', 'pickle', 'pkl']
    if file_format.lower() not in valid_formats:
        raise ValueError(
            f"Invalid file format: {file_format}. "
            f"Valid options: {valid_formats}"
        )
    
    # Normalize format names
    format_map = {
        'xlsx': 'excel',
        'pkl': 'pickle'
    }
    file_format = format_map.get(file_format.lower(), file_format.lower())
    
    try:
        logger.info(
            f"Saving processed data: {len(df)} rows, {len(df.columns)} columns, "
            f"format={file_format}"
        )
        
        # Validate data if requested
        if validate_before_save:
            _validate_dataframe_for_save(df, file_format)
        
        # Create output directory if it doesn't exist
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        logger.info(f"Output directory ensured: {output_path.absolute()}")
        
        # Generate filename if not provided
        if filename is None:
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"processed_data_{timestamp_str}"
        elif include_timestamp:
            timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"{filename}_{timestamp_str}"
        
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
        else:
            ext = extension_map[file_format]
        
        full_filename = f"{filename}{ext}"
        filepath = output_path / full_filename
        
        # Check if file exists and handle accordingly
        backup_created = False
        if filepath.exists():
            if not overwrite:
                # Add counter to filename
                counter = 1
                while filepath.exists():
                    filepath = output_path / f"{filename}_{counter}{ext}"
                    counter += 1
                full_filename = filepath.name
                logger.info(f"File exists, using: {full_filename}")
            elif create_backup:
                # Create backup of existing file
                backup_filename = f"{filename}_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
                backup_path = output_path / backup_filename
                try:
                    import shutil
                    shutil.copy2(filepath, backup_path)
                    backup_created = True
                    logger.info(f"Backup created: {backup_filename}")
                except Exception as e:
                    logger.warning(f"Failed to create backup: {e}")
        
        # Save based on format
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
        
        # Get file size
        file_size_bytes = filepath.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        # Save metadata if provided
        metadata_saved = False
        if metadata:
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
                logger.info(f"Metadata saved: {metadata_path.name}")
            except Exception as e:
                logger.warning(f"Failed to save metadata: {e}")
        
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
            'processing_time_seconds': round(
                (datetime.now() - start_time).total_seconds(), 3
            )
        }
        
        logger.info(
            f"Data saved successfully: {full_filename} "
            f"({result['rows']} rows, {result['size_mb']:.2f} MB)"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Failed to save processed data: {str(e)}", exc_info=True)
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
    Load processed data from file with automatic format detection.
    
    Args:
        filepath: Path to file to load
        file_format: File format (auto-detected from extension if None)
        **kwargs: Additional arguments to pass to pandas read function
    
    Returns:
        DataFrame with loaded data
    
    Example:
        >>> df = load_processed_data('data/processed/cleaned_data.csv')
        >>> df = load_processed_data('data/processed/export.parquet', file_format='parquet')
    """
    filepath = Path(filepath)
    
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")
    
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
    
    logger.info(f"Loading data from: {filepath.name} (format: {file_format})")
    
    try:
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
            raise ValueError(f"Unsupported file format: {file_format}")
        
        logger.info(f"Data loaded: {len(df)} rows, {len(df.columns)} columns")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load data: {str(e)}", exc_info=True)
        raise


def list_processed_files(
    directory: str = "data/processed",
    file_format: Optional[str] = None,
    sort_by: str = "modified"
) -> List[Dict[str, Any]]:
    """
    List all processed data files in directory with metadata.
    
    Args:
        directory: Directory to search
        file_format: Filter by specific format (None for all)
        sort_by: Sort by 'name', 'modified', or 'size'
    
    Returns:
        List of dictionaries with file information
    
    Example:
        >>> files = list_processed_files('data/processed', file_format='csv')
        >>> for file in files:
        ...     print(f"{file['name']}: {file['size_mb']:.2f} MB")
    """
    dir_path = Path(directory)
    
    if not dir_path.exists():
        logger.warning(f"Directory does not exist: {directory}")
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
    
    if file_format:
        pattern = patterns.get(file_format, '*.*')
        files = list(dir_path.glob(pattern))
    else:
        files = [f for f in dir_path.iterdir() if f.is_file() and not f.name.endswith('_metadata.json')]
    
    for file in files:
        stat = file.stat()
        files_info.append({
            'name': file.name,
            'path': str(file.absolute()),
            'size_bytes': stat.st_size,
            'size_mb': round(stat.st_size / (1024 * 1024), 2),
            'modified': datetime.fromtimestamp(stat.st_mtime).isoformat(),
            'extension': file.suffix
        })
    
    # Sort files
    if sort_by == 'name':
        files_info.sort(key=lambda x: x['name'])
    elif sort_by == 'size':
        files_info.sort(key=lambda x: x['size_bytes'], reverse=True)
    else:  # modified
        files_info.sort(key=lambda x: x['modified'], reverse=True)
    
    return files_info
