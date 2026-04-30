"""
Data Ingestion Module
=====================
Production-level data ingestion utilities for the KPI Intelligence Backend.

This module provides functionality to load, validate, and preprocess raw data
from various file formats for further processing in the KPI Intelligence system.

Author: KPI Intelligence Team
Created: 2026-04-30
"""

import os
import logging
import hashlib
import mimetypes
import shutil
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Tuple
from datetime import datetime
import json

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataIngestionError(Exception):
    """Custom exception for data ingestion errors."""
    pass


class FileFormatNotSupportedError(DataIngestionError):
    """Exception raised when file format is not supported."""
    pass


class DataValidationError(DataIngestionError):
    """Exception raised when data validation fails."""
    pass


def load_raw_data(
    file_path: Union[str, Path],
    file_format: Optional[str] = None,
    encoding: str = 'utf-8',
    validate: bool = True,
    max_file_size_mb: int = 500,
    **kwargs
) -> Dict[str, Any]:
    """
    Load raw data from a file with comprehensive validation and error handling.
    
    This function provides production-level data ingestion with support for
    multiple file formats, automatic format detection, data validation,
    and detailed metadata extraction.
    
    Supported formats:
        - CSV (.csv)
        - JSON (.json)
        - Excel (.xlsx, .xls)
        - Parquet (.parquet)
        - TSV (.tsv)
    
    Args:
        file_path: Path to the data file to load
        file_format: Explicit file format (if None, will auto-detect from extension)
        encoding: Character encoding for text files (default: 'utf-8')
        validate: Whether to perform data validation (default: True)
        max_file_size_mb: Maximum allowed file size in MB (default: 500)
        **kwargs: Additional arguments passed to the underlying read function
                 (e.g., sep=';' for CSV, sheet_name='Sheet1' for Excel)
    
    Returns:
        Dictionary containing:
            - data: pandas DataFrame with the loaded data
            - metadata: Dictionary with file and data metadata
            - validation_results: Validation results if validate=True
            - status: 'success' or 'warning'
            - warnings: List of warning messages if any
    
    Raises:
        FileNotFoundError: If the specified file does not exist
        FileFormatNotSupportedError: If the file format is not supported
        DataValidationError: If validation fails and data is invalid
        DataIngestionError: For other ingestion-related errors
    
    Example:
        >>> result = load_raw_data('data/sales_2024.csv')
        >>> df = result['data']
        >>> print(f"Loaded {len(df)} rows")
        >>> print(f"File hash: {result['metadata']['file_hash']}")
    """
    start_time = datetime.now()
    file_path = Path(file_path)
    warnings = []
    
    try:
        # Step 1: Validate file existence and accessibility
        logger.info(f"Starting data ingestion for file: {file_path}")
        _validate_file_exists(file_path)
        
        # Step 2: Validate file size
        file_size_mb = _get_file_size_mb(file_path)
        if file_size_mb > max_file_size_mb:
            raise DataIngestionError(
                f"File size ({file_size_mb:.2f} MB) exceeds maximum allowed size ({max_file_size_mb} MB)"
            )
        
        # Step 3: Detect or validate file format
        detected_format = _detect_file_format(file_path, file_format)
        logger.info(f"Detected file format: {detected_format}")
        
        # Step 4: Calculate file hash for integrity
        file_hash = _calculate_file_hash(file_path)
        
        # Step 5: Load data based on format
        logger.info(f"Loading data from {detected_format} file...")
        df = _load_data_by_format(
            file_path=file_path,
            file_format=detected_format,
            encoding=encoding,
            **kwargs
        )
        
        # Step 6: Basic data cleaning
        df = _clean_dataframe(df)
        
        # Step 7: Extract metadata
        metadata = _extract_metadata(
            file_path=file_path,
            dataframe=df,
            file_format=detected_format,
            file_hash=file_hash,
            file_size_mb=file_size_mb
        )
        
        # Step 8: Validate data if requested
        validation_results = {}
        if validate:
            logger.info("Performing data validation...")
            validation_results = _validate_dataframe(df)
            
            if validation_results['has_critical_issues']:
                raise DataValidationError(
                    f"Data validation failed with {validation_results['critical_issue_count']} critical issues"
                )
            
            if validation_results['has_warnings']:
                warnings.extend(validation_results['warnings'])
        
        # Step 9: Calculate processing duration
        end_time = datetime.now()
        processing_duration = (end_time - start_time).total_seconds()
        
        # Step 10: Prepare response
        result = {
            'status': 'warning' if warnings else 'success',
            'data': df,
            'metadata': {
                **metadata,
                'processing_duration_seconds': round(processing_duration, 2),
                'ingestion_timestamp': datetime.now().isoformat(),
            },
            'validation_results': validation_results,
            'warnings': warnings,
            'row_count': len(df),
            'column_count': len(df.columns)
        }
        
        logger.info(
            f"Data ingestion completed successfully. "
            f"Loaded {len(df)} rows and {len(df.columns)} columns "
            f"in {processing_duration:.2f} seconds"
        )
        
        return result
        
    except FileNotFoundError as e:
        logger.error(f"File not found: {file_path}")
        raise
    except FileFormatNotSupportedError as e:
        logger.error(f"Unsupported file format: {e}")
        raise
    except DataValidationError as e:
        logger.error(f"Data validation failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during data ingestion: {str(e)}", exc_info=True)
        raise DataIngestionError(f"Failed to load data from {file_path}: {str(e)}") from e


def _validate_file_exists(file_path: Path) -> None:
    """Validate that the file exists and is accessible."""
    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    
    if not file_path.is_file():
        raise DataIngestionError(f"Path is not a file: {file_path}")
    
    if not os.access(file_path, os.R_OK):
        raise DataIngestionError(f"File is not readable: {file_path}")


def _get_file_size_mb(file_path: Path) -> float:
    """Get file size in megabytes."""
    return file_path.stat().st_size / (1024 * 1024)


def _detect_file_format(file_path: Path, explicit_format: Optional[str] = None) -> str:
    """
    Detect file format from extension or explicit parameter.
    
    Args:
        file_path: Path to the file
        explicit_format: Explicitly specified format (takes precedence)
    
    Returns:
        Normalized file format string
    
    Raises:
        FileFormatNotSupportedError: If format is not supported
    """
    if explicit_format:
        format_lower = explicit_format.lower().strip('.')
    else:
        format_lower = file_path.suffix.lower().strip('.')
    
    # Map of supported formats
    format_mapping = {
        'csv': 'csv',
        'json': 'json',
        'xlsx': 'excel',
        'xls': 'excel',
        'parquet': 'parquet',
        'tsv': 'tsv',
        'txt': 'csv',  # Treat .txt as CSV by default
    }
    
    if format_lower not in format_mapping:
        raise FileFormatNotSupportedError(
            f"File format '.{format_lower}' is not supported. "
            f"Supported formats: {', '.join(format_mapping.keys())}"
        )
    
    return format_mapping[format_lower]


def _calculate_file_hash(file_path: Path, algorithm: str = 'sha256') -> str:
    """
    Calculate hash of the file for integrity verification.
    
    Args:
        file_path: Path to the file
        algorithm: Hash algorithm to use (default: sha256)
    
    Returns:
        Hexadecimal hash string
    """
    hash_func = hashlib.new(algorithm)
    
    with open(file_path, 'rb') as f:
        # Read file in chunks to handle large files efficiently
        for chunk in iter(lambda: f.read(8192), b''):
            hash_func.update(chunk)
    
    return hash_func.hexdigest()


def _load_data_by_format(
    file_path: Path,
    file_format: str,
    encoding: str = 'utf-8',
    **kwargs
) -> pd.DataFrame:
    """
    Load data using the appropriate pandas function based on format.
    
    Args:
        file_path: Path to the file
        file_format: Detected file format
        encoding: Character encoding
        **kwargs: Additional arguments for the pandas read function
    
    Returns:
        pandas DataFrame with loaded data
    
    Raises:
        DataIngestionError: If loading fails
    """
    try:
        if file_format == 'csv':
            # CSV with automatic delimiter detection if not specified
            if 'sep' not in kwargs and 'delimiter' not in kwargs:
                kwargs['sep'] = ','
            df = pd.read_csv(file_path, encoding=encoding, **kwargs)
            
        elif file_format == 'tsv':
            kwargs['sep'] = '\t'
            df = pd.read_csv(file_path, encoding=encoding, **kwargs)
            
        elif file_format == 'json':
            df = pd.read_json(file_path, encoding=encoding, **kwargs)
            
        elif file_format == 'excel':
            df = pd.read_excel(file_path, **kwargs)
            
        elif file_format == 'parquet':
            df = pd.read_parquet(file_path, **kwargs)
            
        else:
            raise FileFormatNotSupportedError(f"Format '{file_format}' is not implemented")
        
        return df
        
    except pd.errors.EmptyDataError:
        raise DataIngestionError(f"File is empty: {file_path}")
    except pd.errors.ParserError as e:
        raise DataIngestionError(f"Failed to parse file: {str(e)}")
    except Exception as e:
        raise DataIngestionError(f"Failed to load {file_format} file: {str(e)}") from e


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Perform basic cleaning operations on the dataframe.
    
    Args:
        df: Input DataFrame
    
    Returns:
        Cleaned DataFrame
    """
    # Remove completely empty rows and columns
    df = df.dropna(how='all', axis=0)  # Remove all-NA rows
    df = df.dropna(how='all', axis=1)  # Remove all-NA columns
    
    # Strip whitespace from column names
    df.columns = df.columns.str.strip()
    
    # Reset index
    df = df.reset_index(drop=True)
    
    return df


def _extract_metadata(
    file_path: Path,
    dataframe: pd.DataFrame,
    file_format: str,
    file_hash: str,
    file_size_mb: float
) -> Dict[str, Any]:
    """
    Extract comprehensive metadata about the file and data.
    
    Args:
        file_path: Path to the file
        dataframe: Loaded DataFrame
        file_format: Detected file format
        file_hash: Calculated file hash
        file_size_mb: File size in MB
    
    Returns:
        Dictionary containing metadata
    """
    # Get file statistics
    file_stat = file_path.stat()
    
    # Extract schema information
    schema_info = []
    for col in dataframe.columns:
        col_info = {
            'name': col,
            'dtype': str(dataframe[col].dtype),
            'null_count': int(dataframe[col].isnull().sum()),
            'null_percentage': round((dataframe[col].isnull().sum() / len(dataframe)) * 100, 2),
            'unique_count': int(dataframe[col].nunique()),
        }
        
        # Add sample values for better understanding
        non_null_values = dataframe[col].dropna()
        if len(non_null_values) > 0:
            col_info['sample_values'] = non_null_values.head(3).tolist()
        
        schema_info.append(col_info)
    
    metadata = {
        'file_name': file_path.name,
        'file_path': str(file_path.absolute()),
        'file_format': file_format,
        'file_size_bytes': file_stat.st_size,
        'file_size_mb': round(file_size_mb, 2),
        'file_hash': file_hash,
        'file_created_time': datetime.fromtimestamp(file_stat.st_ctime).isoformat(),
        'file_modified_time': datetime.fromtimestamp(file_stat.st_mtime).isoformat(),
        'row_count': len(dataframe),
        'column_count': len(dataframe.columns),
        'columns': dataframe.columns.tolist(),
        'schema_info': schema_info,
        'memory_usage_mb': round(dataframe.memory_usage(deep=True).sum() / (1024 * 1024), 2),
    }
    
    return metadata


def _validate_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Perform comprehensive validation on the DataFrame.
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Dictionary containing validation results
    """
    validation_results = {
        'is_valid': True,
        'has_warnings': False,
        'has_critical_issues': False,
        'warnings': [],
        'errors': [],
        'critical_issue_count': 0,
        'warning_count': 0,
    }
    
    # Check 1: Empty DataFrame
    if len(df) == 0:
        validation_results['errors'].append("DataFrame is empty (no rows)")
        validation_results['has_critical_issues'] = True
        validation_results['critical_issue_count'] += 1
    
    # Check 2: No columns
    if len(df.columns) == 0:
        validation_results['errors'].append("DataFrame has no columns")
        validation_results['has_critical_issues'] = True
        validation_results['critical_issue_count'] += 1
    
    # Check 3: Duplicate column names
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        validation_results['warnings'].append(
            f"Duplicate column names found: {duplicate_cols}"
        )
        validation_results['has_warnings'] = True
        validation_results['warning_count'] += 1
    
    # Check 4: Completely empty columns
    empty_cols = [col for col in df.columns if df[col].isnull().all()]
    if empty_cols:
        validation_results['warnings'].append(
            f"Completely empty columns: {empty_cols}"
        )
        validation_results['has_warnings'] = True
        validation_results['warning_count'] += 1
    
    # Check 5: High percentage of missing values
    high_null_cols = []
    for col in df.columns:
        null_pct = (df[col].isnull().sum() / len(df)) * 100
        if null_pct > 50:
            high_null_cols.append(f"{col} ({null_pct:.1f}%)")
    
    if high_null_cols:
        validation_results['warnings'].append(
            f"Columns with >50% missing values: {high_null_cols}"
        )
        validation_results['has_warnings'] = True
        validation_results['warning_count'] += 1
    
    # Check 6: Single value columns (no variance)
    single_value_cols = [col for col in df.columns if df[col].nunique() <= 1]
    if single_value_cols:
        validation_results['warnings'].append(
            f"Columns with single unique value: {single_value_cols}"
        )
        validation_results['has_warnings'] = True
        validation_results['warning_count'] += 1
    
    # Update overall validity
    validation_results['is_valid'] = not validation_results['has_critical_issues']
    
    return validation_results


def save_to_database(
    data: pd.DataFrame,
    table_name: str,
    database_url: str,
    if_exists: str = 'append',
    chunk_size: int = 1000
) -> Dict[str, Any]:
    """
    Save DataFrame to database table with error handling.
    
    Args:
        data: DataFrame to save
        table_name: Name of the target table
        database_url: Database connection URL
        if_exists: How to behave if table exists ('fail', 'replace', 'append')
        chunk_size: Number of rows to insert per batch
    
    Returns:
        Dictionary with save operation results
    
    Raises:
        DataIngestionError: If database operation fails
    """
    start_time = datetime.now()
    
    try:
        logger.info(f"Saving {len(data)} rows to table '{table_name}'...")
        
        engine = create_engine(database_url)
        
        # Save data to database
        data.to_sql(
            name=table_name,
            con=engine,
            if_exists=if_exists,
            index=False,
            chunksize=chunk_size,
            method='multi'
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info(
            f"Successfully saved {len(data)} rows to '{table_name}' "
            f"in {duration:.2f} seconds"
        )
        
        return {
            'status': 'success',
            'rows_saved': len(data),
            'table_name': table_name,
            'duration_seconds': round(duration, 2),
            'timestamp': datetime.now().isoformat()
        }
        
    except SQLAlchemyError as e:
        logger.error(f"Database error while saving data: {str(e)}")
        raise DataIngestionError(f"Failed to save data to database: {str(e)}") from e
    except Exception as e:
        logger.error(f"Unexpected error while saving to database: {str(e)}")
        raise DataIngestionError(f"Failed to save data: {str(e)}") from e


def get_data_profile(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Generate a statistical profile of the DataFrame.
    
    Args:
        df: DataFrame to profile
    
    Returns:
        Dictionary containing statistical profile
    """
    profile = {
        'shape': {'rows': len(df), 'columns': len(df.columns)},
        'memory_usage_mb': round(df.memory_usage(deep=True).sum() / (1024 * 1024), 2),
        'missing_values': {
            'total': int(df.isnull().sum().sum()),
            'percentage': round((df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100, 2)
        },
        'columns': []
    }
    
    for col in df.columns:
        col_profile = {
            'name': col,
            'dtype': str(df[col].dtype),
            'null_count': int(df[col].isnull().sum()),
            'unique_count': int(df[col].nunique()),
        }
        
        # Add numeric statistics if column is numeric
        if pd.api.types.is_numeric_dtype(df[col]):
            col_profile['statistics'] = {
                'mean': float(df[col].mean()) if not df[col].isnull().all() else None,
                'median': float(df[col].median()) if not df[col].isnull().all() else None,
                'std': float(df[col].std()) if not df[col].isnull().all() else None,
                'min': float(df[col].min()) if not df[col].isnull().all() else None,
                'max': float(df[col].max()) if not df[col].isnull().all() else None,
            }
        
        profile['columns'].append(col_profile)
    
    return profile


def read_csv_data(
    file_path: Union[str, Path],
    encoding: str = 'utf-8',
    delimiter: str = ',',
    header: Union[int, str, None] = 'infer',
    skiprows: Optional[Union[int, List[int]]] = None,
    nrows: Optional[int] = None,
    dtype: Optional[Dict[str, Any]] = None,
    parse_dates: Optional[Union[bool, List[str]]] = None,
    **kwargs
) -> pd.DataFrame:
    """
    Read CSV file using pandas with production-level configuration.
    
    This function provides a focused CSV reading interface with common options
    and robust error handling for production environments.
    
    Args:
        file_path: Path to the CSV file
        encoding: Character encoding (default: 'utf-8', common: 'latin1', 'iso-8859-1')
        delimiter: Field delimiter (default: ',', also use ';', '|', '\t')
        header: Row number(s) to use as column names, or 'infer' for automatic detection
        skiprows: Line numbers to skip (0-indexed) or number of lines to skip at start
        nrows: Number of rows to read (useful for testing large files)
        dtype: Dictionary of column names to data types for type enforcement
        parse_dates: Columns to parse as dates (bool or list of column names)
        **kwargs: Additional arguments passed to pd.read_csv()
    
    Returns:
        pandas DataFrame with the loaded CSV data
    
    Raises:
        FileNotFoundError: If the file doesn't exist
        DataIngestionError: If reading fails
    
    Example:
        >>> # Basic usage
        >>> df = read_csv_data('data/raw/sales_2024.csv')
        >>> 
        >>> # With specific options
        >>> df = read_csv_data(
        ...     'data/raw/sales.csv',
        ...     delimiter=';',
        ...     parse_dates=['date_column'],
        ...     dtype={'customer_id': str}
        ... )
        >>> 
        >>> # Read first 1000 rows for testing
        >>> df = read_csv_data('data/raw/large_file.csv', nrows=1000)
    """
    file_path = Path(file_path)
    
    try:
        # Validate file exists
        _validate_file_exists(file_path)
        
        logger.info(f"Reading CSV file: {file_path}")
        
        # Configure header parameter
        if header == 'infer':
            header = 0  # Use first row as header
        
        # Read CSV with specified options
        df = pd.read_csv(
            file_path,
            encoding=encoding,
            sep=delimiter,
            header=header,
            skiprows=skiprows,
            nrows=nrows,
            dtype=dtype,
            parse_dates=parse_dates,
            low_memory=False,  # Read entire file at once for consistent dtypes
            **kwargs
        )
        
        logger.info(f"Successfully read CSV: {len(df)} rows, {len(df.columns)} columns")
        
        return df
        
    except FileNotFoundError:
        logger.error(f"CSV file not found: {file_path}")
        raise
    except pd.errors.EmptyDataError:
        raise DataIngestionError(f"CSV file is empty: {file_path}")
    except pd.errors.ParserError as e:
        raise DataIngestionError(f"Failed to parse CSV file: {str(e)}")
    except UnicodeDecodeError as e:
        raise DataIngestionError(
            f"Encoding error. Try different encoding (current: {encoding}). "
            f"Common alternatives: 'latin1', 'iso-8859-1', 'cp1252'. Error: {str(e)}"
        )
    except Exception as e:
        logger.error(f"Unexpected error reading CSV: {str(e)}", exc_info=True)
        raise DataIngestionError(f"Failed to read CSV file: {str(e)}") from e


def save_file_to_raw(
    source_path: Union[str, Path],
    destination_name: Optional[str] = None,
    raw_data_dir: Union[str, Path] = "data/raw",
    overwrite: bool = False,
    add_timestamp: bool = True,
    create_dir: bool = True,
    copy_mode: bool = True
) -> Dict[str, Any]:
    """
    Save/copy a file to the data/raw directory with production-level handling.
    
    This function handles file operations for ingesting data files into the
    raw data directory with proper validation, naming conventions, and metadata tracking.
    
    Args:
        source_path: Path to the source file to save
        destination_name: Custom name for destination file (if None, uses source filename)
        raw_data_dir: Path to the raw data directory (default: 'data/raw')
        overwrite: Whether to overwrite if destination file exists (default: False)
        add_timestamp: Add timestamp to filename to prevent overwrites (default: True)
        create_dir: Create raw_data_dir if it doesn't exist (default: True)
        copy_mode: If True, copy the file; if False, move the file (default: True)
    
    Returns:
        Dictionary containing:
            - status: 'success'
            - source_path: Original file path
            - destination_path: Final destination path
            - file_size_mb: File size in MB
            - file_hash: SHA-256 hash of the file
            - timestamp: ISO format timestamp of operation
            - operation: 'copy' or 'move'
    
    Raises:
        FileNotFoundError: If source file doesn't exist
        DataIngestionError: If file operation fails
    
    Example:
        >>> # Copy file to data/raw with timestamp
        >>> result = save_file_to_raw('uploads/new_data.csv')
        >>> print(f"Saved to: {result['destination_path']}")
        >>> 
        >>> # Move file with custom name, no timestamp
        >>> result = save_file_to_raw(
        ...     'temp/data.csv',
        ...     destination_name='sales_data.csv',
        ...     add_timestamp=False,
        ...     copy_mode=False
        ... )
        >>> 
        >>> # Overwrite existing file
        >>> result = save_file_to_raw(
        ...     'new_file.csv',
        ...     destination_name='existing.csv',
        ...     overwrite=True,
        ...     add_timestamp=False
        ... )
    """
    source_path = Path(source_path)
    raw_data_dir = Path(raw_data_dir)
    
    try:
        # Step 1: Validate source file
        logger.info(f"Saving file to raw directory: {source_path}")
        _validate_file_exists(source_path)
        
        # Step 2: Create destination directory if needed
        if create_dir and not raw_data_dir.exists():
            logger.info(f"Creating raw data directory: {raw_data_dir}")
            raw_data_dir.mkdir(parents=True, exist_ok=True)
        
        if not raw_data_dir.exists():
            raise DataIngestionError(
                f"Raw data directory does not exist: {raw_data_dir}. "
                f"Set create_dir=True to create it automatically."
            )
        
        # Step 3: Determine destination filename
        if destination_name:
            base_name = destination_name
        else:
            base_name = source_path.name
        
        # Step 4: Add timestamp if requested
        if add_timestamp and not overwrite:
            timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
            name_parts = base_name.rsplit('.', 1)
            if len(name_parts) == 2:
                base_name = f"{name_parts[0]}_{timestamp_str}.{name_parts[1]}"
            else:
                base_name = f"{base_name}_{timestamp_str}"
        
        destination_path = raw_data_dir / base_name
        
        # Step 5: Check if destination exists and handle accordingly
        if destination_path.exists() and not overwrite:
            raise DataIngestionError(
                f"Destination file already exists: {destination_path}. "
                f"Set overwrite=True or add_timestamp=True to avoid conflicts."
            )
        
        # Step 6: Calculate file metadata before operation
        file_size_mb = _get_file_size_mb(source_path)
        file_hash = _calculate_file_hash(source_path)
        
        # Step 7: Perform file operation
        operation_start = datetime.now()
        
        if copy_mode:
            logger.info(f"Copying file to: {destination_path}")
            shutil.copy2(source_path, destination_path)  # copy2 preserves metadata
            operation = 'copy'
        else:
            logger.info(f"Moving file to: {destination_path}")
            shutil.move(str(source_path), str(destination_path))
            operation = 'move'
        
        operation_end = datetime.now()
        operation_duration = (operation_end - operation_start).total_seconds()
        
        # Step 8: Verify destination file
        if not destination_path.exists():
            raise DataIngestionError(f"File operation failed: destination file not found")
        
        # Verify integrity by comparing hash
        dest_hash = _calculate_file_hash(destination_path)
        if dest_hash != file_hash:
            logger.error("File integrity check failed: hash mismatch")
            # Clean up corrupted file
            if destination_path.exists():
                destination_path.unlink()
            raise DataIngestionError("File integrity verification failed")
        
        logger.info(
            f"Successfully {operation}ed file to {destination_path} "
            f"({file_size_mb:.2f} MB) in {operation_duration:.2f} seconds"
        )
        
        # Step 9: Return operation metadata
        return {
            'status': 'success',
            'operation': operation,
            'source_path': str(source_path.absolute()),
            'destination_path': str(destination_path.absolute()),
            'file_name': destination_path.name,
            'file_size_bytes': destination_path.stat().st_size,
            'file_size_mb': round(file_size_mb, 2),
            'file_hash': file_hash,
            'timestamp': datetime.now().isoformat(),
            'operation_duration_seconds': round(operation_duration, 2),
        }
        
    except FileNotFoundError:
        logger.error(f"Source file not found: {source_path}")
        raise
    except PermissionError as e:
        logger.error(f"Permission denied: {str(e)}")
        raise DataIngestionError(f"Permission denied while saving file: {str(e)}") from e
    except OSError as e:
        logger.error(f"OS error during file operation: {str(e)}")
        raise DataIngestionError(f"File system error: {str(e)}") from e
    except Exception as e:
        logger.error(f"Unexpected error saving file: {str(e)}", exc_info=True)
        raise DataIngestionError(f"Failed to save file to raw directory: {str(e)}") from e


def ingest_csv_to_raw(
    source_path: Union[str, Path],
    validate_data: bool = True,
    save_to_db: bool = False,
    db_table: Optional[str] = None,
    database_url: Optional[str] = None,
    **save_options
) -> Dict[str, Any]:
    """
    Complete CSV ingestion workflow: read, validate, save to raw, and optionally to database.
    
    This is a high-level function that orchestrates the complete ingestion process
    for CSV files in a production environment.
    
    Args:
        source_path: Path to the source CSV file
        validate_data: Whether to validate the CSV data (default: True)
        save_to_db: Whether to save data to database (default: False)
        db_table: Database table name (required if save_to_db=True)
        database_url: Database connection URL (if None, uses config)
        **save_options: Additional options for save_file_to_raw()
    
    Returns:
        Dictionary containing complete ingestion results including:
            - file_operation: Results from save_file_to_raw()
            - data_load: Results from load_raw_data()
            - database_save: Results from save_to_database() if applicable
            - status: Overall status
    
    Raises:
        DataIngestionError: If any step of the ingestion fails
    
    Example:
        >>> # Basic ingestion: validate and save to raw
        >>> result = ingest_csv_to_raw('uploads/sales.csv')
        >>> print(f"File saved to: {result['file_operation']['destination_path']}")
        >>> print(f"Rows loaded: {result['data_load']['row_count']}")
        >>> 
        >>> # Full ingestion: save to raw and database
        >>> result = ingest_csv_to_raw(
        ...     'uploads/sales.csv',
        ...     save_to_db=True,
        ...     db_table='raw_sales_data',
        ...     destination_name='sales_latest.csv'
        ... )
    """
    try:
        logger.info(f"Starting complete CSV ingestion workflow for: {source_path}")
        start_time = datetime.now()
        
        result = {
            'status': 'in_progress',
            'source_file': str(source_path),
            'steps_completed': []
        }
        
        # Step 1: Save file to raw directory
        logger.info("Step 1: Saving file to raw directory...")
        file_operation = save_file_to_raw(source_path, **save_options)
        result['file_operation'] = file_operation
        result['steps_completed'].append('file_saved')
        
        destination_path = file_operation['destination_path']
        
        # Step 2: Load and validate data
        logger.info("Step 2: Loading and validating data...")
        data_load = load_raw_data(
            file_path=destination_path,
            validate=validate_data
        )
        result['data_load'] = {
            'row_count': data_load['row_count'],
            'column_count': data_load['column_count'],
            'file_hash': data_load['metadata']['file_hash'],
            'validation_passed': data_load['validation_results'].get('is_valid', True),
            'warnings': data_load['warnings']
        }
        result['steps_completed'].append('data_loaded')
        
        # Step 3: Save to database if requested
        if save_to_db:
            if not db_table:
                raise DataIngestionError("db_table parameter is required when save_to_db=True")
            
            logger.info(f"Step 3: Saving data to database table '{db_table}'...")
            
            # Use provided database_url or fall back to config
            if database_url is None:
                try:
                    from config import config
                    database_url = config.DATABASE_URL
                except ImportError:
                    raise DataIngestionError(
                        "database_url not provided and config module not available"
                    )
            
            db_save = save_to_database(
                data=data_load['data'],
                table_name=db_table,
                database_url=database_url
            )
            result['database_save'] = db_save
            result['steps_completed'].append('data_saved_to_db')
        
        # Calculate total duration
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        
        result['status'] = 'success'
        result['total_duration_seconds'] = round(total_duration, 2)
        result['completed_at'] = datetime.now().isoformat()
        
        logger.info(
            f"CSV ingestion workflow completed successfully in {total_duration:.2f} seconds. "
            f"Steps: {', '.join(result['steps_completed'])}"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"CSV ingestion workflow failed: {str(e)}", exc_info=True)
        result['status'] = 'failed'
        result['error'] = str(e)
        raise DataIngestionError(f"CSV ingestion workflow failed: {str(e)}") from e


if __name__ == "__main__":
    """
    Example usage and testing of the data ingestion module.
    """
    print("\n" + "="*70)
    print("Data Ingestion Module - Production Examples")
    print("="*70 + "\n")
    
    # Example 1: Read CSV data
    print("Example 1: Read CSV data")
    print("-" * 70)
    try:
        example_csv = Path("data/raw/example_data.csv")
        
        if example_csv.exists():
            # Read CSV with default settings
            df = read_csv_data(example_csv)
            print(f"✓ Successfully read CSV: {len(df)} rows, {len(df.columns)} columns")
            print(f"  Columns: {', '.join(df.columns.tolist()[:5])}")
            if len(df.columns) > 5:
                print(f"           ... and {len(df.columns) - 5} more")
            print(f"\n  First 3 rows:")
            print(df.head(3).to_string(index=False))
        else:
            print(f"ℹ Example CSV not found: {example_csv}")
            print("  Create a CSV file in data/raw/ to test CSV reading")
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    print("\n")
    
    # Example 2: Save file to raw directory
    print("Example 2: Save file to data/raw directory")
    print("-" * 70)
    try:
        # Create a sample CSV for demonstration
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, newline='') as f:
            f.write("id,name,value\n")
            f.write("1,Product A,100\n")
            f.write("2,Product B,200\n")
            f.write("3,Product C,300\n")
            temp_file = Path(f.name)
        
        # Save to raw directory with timestamp
        result = save_file_to_raw(
            source_path=temp_file,
            destination_name="sample_data.csv",
            add_timestamp=True,
            copy_mode=True
        )
        
        print(f"✓ File saved successfully")
        print(f"  Operation: {result['operation']}")
        print(f"  Destination: {result['file_name']}")
        print(f"  Size: {result['file_size_mb']} MB")
        print(f"  Hash: {result['file_hash'][:16]}...")
        print(f"  Duration: {result['operation_duration_seconds']} seconds")
        
        # Clean up temp file
        temp_file.unlink(missing_ok=True)
        
    except Exception as e:
        print(f"✗ Error: {str(e)}")
        if 'temp_file' in locals():
            temp_file.unlink(missing_ok=True)
    
    print("\n")
    
    # Example 3: Complete ingestion workflow
    print("Example 3: Complete CSV ingestion workflow")
    print("-" * 70)
    try:
        example_file = Path("data/raw/example_data.csv")
        
        if example_file.exists():
            # Load with full validation
            result = load_raw_data(
                file_path=example_file,
                validate=True
            )
            
            print(f"✓ Complete ingestion workflow")
            print(f"  Rows: {result['row_count']}")
            print(f"  Columns: {result['column_count']}")
            print(f"  File size: {result['metadata']['file_size_mb']} MB")
            print(f"  Processing time: {result['metadata']['processing_duration_seconds']}s")
            print(f"  File hash: {result['metadata']['file_hash'][:16]}...")
            print(f"  Validation: {'✓ Passed' if result['validation_results'].get('is_valid', True) else '✗ Failed'}")
            
            if result['warnings']:
                print(f"\n  ⚠ Warnings ({len(result['warnings'])}):")
                for warning in result['warnings'][:3]:  # Show first 3 warnings
                    print(f"    - {warning}")
                if len(result['warnings']) > 3:
                    print(f"    ... and {len(result['warnings']) - 3} more")
            
            # Display data statistics
            print(f"\n  Data Statistics:")
            print(f"    Memory usage: {result['metadata']['memory_usage_mb']} MB")
            
            # Show column info
            print(f"\n  Column Information:")
            for col_info in result['metadata']['schema_info'][:5]:
                null_pct = col_info['null_percentage']
                null_indicator = f" ({null_pct}% null)" if null_pct > 0 else ""
                print(f"    - {col_info['name']}: {col_info['dtype']}{null_indicator}")
            
            if len(result['metadata']['schema_info']) > 5:
                print(f"    ... and {len(result['metadata']['schema_info']) - 5} more columns")
            
        else:
            print(f"ℹ Example file not found: {example_file}")
            print("  Place a CSV file in data/raw/ to test the complete workflow")
            
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    print("\n")
    
    # Example 4: Advanced CSV reading options
    print("Example 4: Advanced CSV reading options")
    print("-" * 70)
    print("Available options for read_csv_data():")
    print("  • encoding: 'utf-8', 'latin1', 'iso-8859-1', 'cp1252'")
    print("  • delimiter: ',', ';', '|', '\\t' (tab)")
    print("  • nrows: Read only first N rows (useful for testing)")
    print("  • parse_dates: Parse date columns automatically")
    print("  • dtype: Enforce specific data types for columns")
    print("\nExample usage:")
    print("  df = read_csv_data('file.csv', delimiter=';', nrows=1000)")
    print("  df = read_csv_data('file.csv', parse_dates=['date_col'])")
    print("  df = read_csv_data('file.csv', dtype={'id': str, 'value': float})")
    
    print("\n" + "="*70)
    print("Production Features:")
    print("  ✓ Multi-format support (CSV, JSON, Excel, Parquet)")
    print("  ✓ Automatic file validation and integrity checks")
    print("  ✓ Data quality validation and profiling")
    print("  ✓ File saving with timestamp and hash verification")
    print("  ✓ Comprehensive error handling and logging")
    print("  ✓ Database integration support")
    print("="*70 + "\n")