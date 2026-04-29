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


if __name__ == "__main__":
    """
    Example usage and testing of the data ingestion module.
    """
    # Example 1: Load a CSV file
    print("\n" + "="*60)
    print("Data Ingestion Module - Example Usage")
    print("="*60 + "\n")
    
    try:
        # Example file path (adjust as needed)
        example_file = Path("data/raw/example_data.csv")
        
        if example_file.exists():
            result = load_raw_data(
                file_path=example_file,
                validate=True
            )
            
            print(f"✓ Successfully loaded {result['row_count']} rows")
            print(f"✓ Columns: {result['column_count']}")
            print(f"✓ File size: {result['metadata']['file_size_mb']} MB")
            print(f"✓ Processing time: {result['metadata']['processing_duration_seconds']} seconds")
            print(f"✓ File hash: {result['metadata']['file_hash'][:16]}...")
            
            if result['warnings']:
                print(f"\n⚠ Warnings ({len(result['warnings'])}):")
                for warning in result['warnings']:
                    print(f"  - {warning}")
            
            # Display data profile
            print(f"\nData Preview:")
            print(result['data'].head())
            
        else:
            print(f"ℹ Example file not found: {example_file}")
            print("  Place a CSV file in data/raw/ to test the ingestion module")
            
    except Exception as e:
        print(f"✗ Error: {str(e)}")
    
    print("\n" + "="*60)