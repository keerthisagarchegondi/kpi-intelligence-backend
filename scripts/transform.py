"""
Data Transformation Module
===========================
Production-level data transformation and cleaning utilities for KPI Intelligence Backend.

This module provides comprehensive data cleaning, transformation, and quality
improvement functions for preparing raw data for analysis and KPI calculations.

Author: KPI Intelligence Team
Created: 2026-05-02
"""

import logging
import warnings
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Tuple, Callable
from datetime import datetime
from enum import Enum

import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataTransformationError(Exception):
    """Custom exception for data transformation errors."""
    pass


class NullHandlingStrategy(Enum):
    """Enumeration of null value handling strategies."""
    DROP = "drop"  # Remove rows/columns with nulls
    FILL_MEAN = "fill_mean"  # Fill with column mean (numeric only)
    FILL_MEDIAN = "fill_median"  # Fill with column median (numeric only)
    FILL_MODE = "fill_mode"  # Fill with most frequent value
    FILL_FORWARD = "fill_forward"  # Forward fill (propagate last valid value)
    FILL_BACKWARD = "fill_backward"  # Backward fill (use next valid value)
    FILL_INTERPOLATE = "fill_interpolate"  # Interpolate missing values
    FILL_CONSTANT = "fill_constant"  # Fill with a constant value
    FILL_ZERO = "fill_zero"  # Fill with zero
    KEEP = "keep"  # Keep null values as-is


def clean_data(
    df: pd.DataFrame,
    null_strategy: Union[str, NullHandlingStrategy] = NullHandlingStrategy.DROP,
    null_threshold: float = 0.5,
    drop_duplicates: bool = True,
    duplicate_subset: Optional[List[str]] = None,
    duplicate_keep: Union[str, bool] = 'first',
    mark_duplicates_only: bool = False,
    remove_whitespace: bool = True,
    standardize_columns: bool = True,
    remove_empty_rows: bool = True,
    remove_empty_columns: bool = True,
    fill_value: Any = None,
    column_specific_strategies: Optional[Dict[str, str]] = None,
    validate_output: bool = True,
    inplace: bool = False
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Comprehensive data cleaning function with advanced null value handling and duplicate removal.
    
    This production-level function provides multiple strategies for handling
    null values, removing duplicates, standardizing formats, and improving
    data quality for downstream processing.
    
    Args:
        df: Input DataFrame to clean
        null_strategy: Strategy for handling null values (see NullHandlingStrategy enum)
        null_threshold: For DROP strategy - drop columns with null ratio > threshold (0-1)
        drop_duplicates: Whether to remove duplicate rows
        duplicate_subset: List of column names to consider for duplicate detection (None = all columns)
        duplicate_keep: Which duplicates to keep: 'first', 'last', or False (remove all duplicates)
        mark_duplicates_only: If True, mark duplicates with '_is_duplicate' column instead of removing
        remove_whitespace: Strip whitespace from string columns
        standardize_columns: Standardize column names (lowercase, replace spaces)
        remove_empty_rows: Remove completely empty rows
        remove_empty_columns: Remove completely empty columns
        fill_value: Value to use for FILL_CONSTANT strategy
        column_specific_strategies: Dict mapping column names to specific strategies
        validate_output: Validate cleaned data before returning
        inplace: Modify DataFrame in place (default: False, returns copy)
    
    Returns:
        Tuple of (cleaned_dataframe, cleaning_report)
        - cleaned_dataframe: The cleaned DataFrame
        - cleaning_report: Dictionary with cleaning statistics and actions taken
    
    Raises:
        DataTransformationError: If cleaning fails or validation fails
        ValueError: If invalid parameters are provided
    
    Example:
        >>> df = pd.DataFrame({'A': [1, 2, None, 4], 'B': [5, None, 7, 8]})
        >>> cleaned_df, report = clean_data(df, null_strategy='fill_mean')
        >>> print(f"Rows before: {report['rows_before']}, after: {report['rows_after']}")
        >>> print(f"Null values removed: {report['nulls_handled']}")
        >>>
        >>> # Remove duplicates based on specific columns
        >>> cleaned_df, report = clean_data(df, duplicate_subset=['email', 'user_id'])
        >>> print(f"Duplicates removed: {report['duplicate_report']['duplicates_removed']}")
        >>>
        >>> # Mark duplicates instead of removing
        >>> cleaned_df, report = clean_data(df, mark_duplicates_only=True)
        >>> print(cleaned_df['_is_duplicate'].sum())  # Count of duplicates
        >>>
        >>> # Column-specific strategies
        >>> strategies = {'age': 'fill_median', 'name': 'fill_mode'}
        >>> cleaned_df, report = clean_data(df, column_specific_strategies=strategies)
    """
    start_time = datetime.now()
    
    # Validate input
    if df is None or not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")
    
    if df.empty:
        logger.warning("Input DataFrame is empty")
        return df, {'status': 'skipped', 'reason': 'empty_dataframe'}
    
    # Convert string strategy to enum
    if isinstance(null_strategy, str):
        try:
            null_strategy = NullHandlingStrategy(null_strategy.lower())
        except ValueError:
            raise ValueError(
                f"Invalid null_strategy: {null_strategy}. "
                f"Valid options: {[s.value for s in NullHandlingStrategy]}"
            )
    
    # Work on copy unless inplace is True
    if not inplace:
        df = df.copy()
    
    # Initialize cleaning report
    report = {
        'timestamp': datetime.now().isoformat(),
        'rows_before': len(df),
        'columns_before': len(df.columns),
        'nulls_before': int(df.isnull().sum().sum()),
        'actions_taken': [],
        'warnings': [],
        'column_changes': {}
    }
    
    try:
        logger.info(f"Starting data cleaning: {len(df)} rows, {len(df.columns)} columns")
        
        # Step 1: Remove completely empty rows
        if remove_empty_rows:
            initial_rows = len(df)
            df = df.dropna(how='all', axis=0)
            removed_rows = initial_rows - len(df)
            if removed_rows > 0:
                report['actions_taken'].append(f"Removed {removed_rows} completely empty rows")
                logger.info(f"Removed {removed_rows} empty rows")
        
        # Step 2: Remove completely empty columns
        if remove_empty_columns:
            initial_cols = len(df.columns)
            empty_cols = df.columns[df.isnull().all()].tolist()
            df = df.dropna(how='all', axis=1)
            removed_cols = initial_cols - len(df.columns)
            if removed_cols > 0:
                report['actions_taken'].append(f"Removed {removed_cols} empty columns: {empty_cols}")
                logger.info(f"Removed {removed_cols} empty columns")
        
        # Step 3: Standardize column names
        if standardize_columns:
            original_columns = df.columns.tolist()
            df.columns = (
                df.columns
                .str.strip()
                .str.lower()
                .str.replace(' ', '_')
                .str.replace('[^a-z0-9_]', '', regex=True)
            )
            if list(df.columns) != original_columns:
                report['actions_taken'].append("Standardized column names")
                report['column_changes']['renamed'] = dict(zip(original_columns, df.columns))
                logger.info("Standardized column names")
        
        # Step 4: Remove whitespace from string columns
        if remove_whitespace:
            string_columns = df.select_dtypes(include=['object', 'string']).columns
            for col in string_columns:
                if df[col].dtype == 'object':
                    df.loc[:, col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
            if len(string_columns) > 0:
                report['actions_taken'].append(f"Stripped whitespace from {len(string_columns)} text columns")
                logger.info(f"Removed whitespace from {len(string_columns)} columns")
        
        # Step 5: Handle null values
        nulls_handled = _handle_null_values(
            df=df,
            strategy=null_strategy,
            threshold=null_threshold,
            fill_value=fill_value,
            column_specific_strategies=column_specific_strategies,
            report=report
        )
        
        # Step 6: Remove or mark duplicate rows
        if drop_duplicates or mark_duplicates_only:
            duplicate_report = _handle_duplicates(
                df=df,
                subset=duplicate_subset,
                keep=duplicate_keep,
                mark_only=mark_duplicates_only,
                report=report
            )
            report['duplicate_report'] = duplicate_report
        
        # Step 7: Calculate final statistics
        report['rows_after'] = len(df)
        report['columns_after'] = len(df.columns)
        report['nulls_after'] = int(df.isnull().sum().sum())
        report['nulls_handled'] = report['nulls_before'] - report['nulls_after']
        report['rows_removed'] = report['rows_before'] - report['rows_after']
        report['columns_removed'] = report['columns_before'] - report['columns_after']
        
        # Calculate data quality score
        total_cells_before = report['rows_before'] * report['columns_before']
        total_cells_after = report['rows_after'] * report['columns_after']
        
        if total_cells_after > 0:
            null_percentage = (report['nulls_after'] / total_cells_after) * 100
            report['null_percentage_after'] = round(null_percentage, 2)
            report['data_quality_score'] = round(100 - null_percentage, 2)
        else:
            report['null_percentage_after'] = 0
            report['data_quality_score'] = 0
        
        # Step 8: Validate output if requested
        if validate_output:
            _validate_cleaned_data(df, report)
        
        # Calculate processing duration
        end_time = datetime.now()
        report['processing_duration_seconds'] = round((end_time - start_time).total_seconds(), 2)
        report['status'] = 'success'
        
        logger.info(
            f"Data cleaning completed: {report['rows_after']} rows, "
            f"{report['columns_after']} columns, "
            f"{report['nulls_handled']} nulls handled, "
            f"quality score: {report['data_quality_score']:.1f}%"
        )
        
        return df, report
        
    except Exception as e:
        logger.error(f"Data cleaning failed: {str(e)}", exc_info=True)
        report['status'] = 'failed'
        report['error'] = str(e)
        raise DataTransformationError(f"Data cleaning failed: {str(e)}") from e


def _handle_null_values(
    df: pd.DataFrame,
    strategy: NullHandlingStrategy,
    threshold: float,
    fill_value: Any,
    column_specific_strategies: Optional[Dict[str, str]],
    report: Dict[str, Any]
) -> int:
    """
    Internal function to handle null values based on specified strategy.
    
    Args:
        df: DataFrame to process (modified in place)
        strategy: Default null handling strategy
        threshold: Threshold for dropping columns
        fill_value: Value for constant fill
        column_specific_strategies: Column-specific strategies
        report: Report dictionary to update
    
    Returns:
        Number of null values handled
    """
    nulls_before = int(df.isnull().sum().sum())
    
    if nulls_before == 0:
        report['actions_taken'].append("No null values found")
        return 0
    
    logger.info(f"Handling {nulls_before} null values using strategy: {strategy.value}")
    
    # Apply column-specific strategies first
    if column_specific_strategies:
        for col, col_strategy in column_specific_strategies.items():
            if col not in df.columns:
                report['warnings'].append(f"Column '{col}' not found for specific strategy")
                continue
            
            col_strategy_enum = NullHandlingStrategy(col_strategy.lower())
            _apply_null_strategy(df, col, col_strategy_enum, fill_value)
            report['actions_taken'].append(
                f"Applied '{col_strategy}' strategy to column '{col}'"
            )
    
    # Apply default strategy to remaining columns with nulls
    columns_with_nulls = df.columns[df.isnull().any()].tolist()
    
    if strategy == NullHandlingStrategy.DROP:
        # Drop columns exceeding threshold
        cols_to_drop = []
        for col in columns_with_nulls:
            null_ratio = df[col].isnull().sum() / len(df)
            if null_ratio > threshold:
                cols_to_drop.append(col)
        
        if cols_to_drop:
            df.drop(columns=cols_to_drop, inplace=True)
            report['actions_taken'].append(
                f"Dropped {len(cols_to_drop)} columns exceeding {threshold*100}% null threshold"
            )
            report['column_changes']['dropped'] = cols_to_drop
        
        # Drop rows with remaining nulls
        initial_rows = len(df)
        df.dropna(inplace=True)
        rows_dropped = initial_rows - len(df)
        if rows_dropped > 0:
            report['actions_taken'].append(f"Dropped {rows_dropped} rows with null values")
    
    elif strategy == NullHandlingStrategy.KEEP:
        report['actions_taken'].append("Kept null values as-is")
    
    else:
        # Apply strategy to each column with nulls
        for col in columns_with_nulls:
            if column_specific_strategies and col in column_specific_strategies:
                continue  # Already handled
            
            _apply_null_strategy(df, col, strategy, fill_value)
        
        report['actions_taken'].append(
            f"Applied '{strategy.value}' strategy to {len(columns_with_nulls)} columns"
        )
    
    nulls_after = int(df.isnull().sum().sum())
    return nulls_before - nulls_after


def _apply_null_strategy(
    df: pd.DataFrame,
    column: str,
    strategy: NullHandlingStrategy,
    fill_value: Any = None
) -> None:
    """
    Apply a specific null handling strategy to a column.
    
    Args:
        df: DataFrame (modified in place)
        column: Column name to process
        strategy: Strategy to apply
        fill_value: Value for constant fill
    """
    if strategy == NullHandlingStrategy.FILL_MEAN:
        if pd.api.types.is_numeric_dtype(df[column]):
            df.loc[:, column] = df[column].fillna(df[column].mean())
        else:
            logger.warning(f"Cannot apply mean fill to non-numeric column '{column}'")
    
    elif strategy == NullHandlingStrategy.FILL_MEDIAN:
        if pd.api.types.is_numeric_dtype(df[column]):
            df.loc[:, column] = df[column].fillna(df[column].median())
        else:
            logger.warning(f"Cannot apply median fill to non-numeric column '{column}'")
    
    elif strategy == NullHandlingStrategy.FILL_MODE:
        mode_value = df[column].mode()
        if len(mode_value) > 0:
            df.loc[:, column] = df[column].fillna(mode_value[0])
    
    elif strategy == NullHandlingStrategy.FILL_FORWARD:
        df.loc[:, column] = df[column].fillna(method='ffill')
    
    elif strategy == NullHandlingStrategy.FILL_BACKWARD:
        df.loc[:, column] = df[column].fillna(method='bfill')
    
    elif strategy == NullHandlingStrategy.FILL_INTERPOLATE:
        if pd.api.types.is_numeric_dtype(df[column]):
            df.loc[:, column] = df[column].interpolate(method='linear')
        else:
            logger.warning(f"Cannot interpolate non-numeric column '{column}'")
    
    elif strategy == NullHandlingStrategy.FILL_CONSTANT:
        if fill_value is not None:
            df.loc[:, column] = df[column].fillna(fill_value)
        else:
            logger.warning("FILL_CONSTANT strategy requires fill_value parameter")
    
    elif strategy == NullHandlingStrategy.FILL_ZERO:
        df.loc[:, column] = df[column].fillna(0)


def _validate_cleaned_data(df: pd.DataFrame, report: Dict[str, Any]) -> None:
    """
    Validate cleaned data for quality issues.
    
    Args:
        df: Cleaned DataFrame
        report: Report dictionary to update with warnings
    
    Raises:
        DataTransformationError: If critical validation fails
    """
    # Check if DataFrame is empty
    if len(df) == 0:
        raise DataTransformationError("Cleaning resulted in empty DataFrame")
    
    # Check if all columns were removed
    if len(df.columns) == 0:
        raise DataTransformationError("All columns were removed during cleaning")
    
    # Warn about high null percentage
    total_cells = len(df) * len(df.columns)
    null_count = df.isnull().sum().sum()
    if total_cells > 0:
        null_percentage = (null_count / total_cells) * 100
        if null_percentage > 30:
            report['warnings'].append(
                f"High null percentage after cleaning: {null_percentage:.1f}%"
            )
    
    # Warn about low row retention
    if 'rows_before' in report:
        retention_rate = (len(df) / report['rows_before']) * 100
        if retention_rate < 50:
            report['warnings'].append(
                f"Low row retention rate: {retention_rate:.1f}%"
            )


def _handle_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]],
    keep: Union[str, bool],
    mark_only: bool,
    report: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Handle duplicate rows with advanced options.
    
    Args:
        df: DataFrame to process (modified in place)
        subset: Columns to consider for duplicate detection
        keep: Which duplicates to keep ('first', 'last', False)
        mark_only: If True, mark duplicates instead of removing
        report: Main report dictionary to update
    
    Returns:
        Dictionary with duplicate handling details
    """
    duplicate_report = {
        'duplicates_found': 0,
        'duplicates_removed': 0,
        'duplicates_marked': 0,
        'subset_columns': subset if subset else 'all columns',
        'keep_strategy': keep if not isinstance(keep, bool) else ('none' if not keep else 'all'),
        'duplicate_groups': 0
    }
    
    # Validate subset columns
    if subset:
        invalid_cols = [col for col in subset if col not in df.columns]
        if invalid_cols:
            logger.warning(f"Duplicate subset columns not found: {invalid_cols}")
            subset = [col for col in subset if col in df.columns]
            if not subset:
                logger.warning("No valid columns in duplicate subset, using all columns")
                subset = None
    
    # Identify duplicates
    duplicate_mask = df.duplicated(subset=subset, keep=False)
    duplicates_found = int(duplicate_mask.sum())
    duplicate_report['duplicates_found'] = duplicates_found
    
    if duplicates_found == 0:
        report['actions_taken'].append("No duplicate rows found")
        logger.info("No duplicates found")
        return duplicate_report
    
    # Count duplicate groups
    if duplicates_found > 0:
        if subset:
            duplicate_groups = df[duplicate_mask].groupby(subset).size()
        else:
            # For full row duplicates, count groups by all columns
            duplicate_groups = df[duplicate_mask].groupby(list(df.columns), dropna=False).size()
        duplicate_report['duplicate_groups'] = len(duplicate_groups)
    
    if mark_only:
        # Mark duplicates with a boolean column
        df['_is_duplicate'] = df.duplicated(subset=subset, keep=keep)
        duplicates_marked = int(df['_is_duplicate'].sum())
        duplicate_report['duplicates_marked'] = duplicates_marked
        
        action_msg = f"Marked {duplicates_marked} duplicate rows in '_is_duplicate' column"
        report['actions_taken'].append(action_msg)
        logger.info(action_msg)
        
        # Add details about duplicate distribution
        if duplicates_marked > 0:
            action_msg += f" ({duplicate_report['duplicate_groups']} duplicate groups)"
    
    else:
        # Remove duplicates
        initial_rows = len(df)
        
        # Get indices to keep
        if keep == 'first':
            keep_mask = ~df.duplicated(subset=subset, keep='first')
        elif keep == 'last':
            keep_mask = ~df.duplicated(subset=subset, keep='last')
        elif keep is False:
            # Remove all duplicates including first occurrence
            keep_mask = ~df.duplicated(subset=subset, keep=False)
        else:
            raise ValueError(f"Invalid keep parameter: {keep}. Use 'first', 'last', or False")
        
        # Apply mask to remove duplicates
        df.drop(df.index[~keep_mask], inplace=True)
        df.reset_index(drop=True, inplace=True)
        
        duplicates_removed = initial_rows - len(df)
        duplicate_report['duplicates_removed'] = duplicates_removed
        
        action_msg = f"Removed {duplicates_removed} duplicate rows"
        if subset:
            action_msg += f" (based on columns: {', '.join(subset[:3])}{'...' if len(subset) > 3 else ''})"
        action_msg += f" (keep={keep}, {duplicate_report['duplicate_groups']} groups)"
        
        report['actions_taken'].append(action_msg)
        logger.info(f"Removed {duplicates_removed} duplicates (keep={keep})")
    
    return duplicate_report


def find_duplicates(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: Union[str, bool] = False,
    return_groups: bool = False
) -> Union[pd.DataFrame, Dict[str, pd.DataFrame]]:
    """
    Find and return duplicate rows with detailed analysis.
    
    This function helps analyze duplicates before deciding how to handle them.
    
    Args:
        df: Input DataFrame
        subset: Columns to consider for duplicate detection (None = all columns)
        keep: Which duplicates to mark as duplicate:
              - 'first': Mark all except first occurrence
              - 'last': Mark all except last occurrence
              - False: Mark all duplicates including first
        return_groups: If True, return dict of duplicate groups instead of flat DataFrame
    
    Returns:
        If return_groups=False: DataFrame containing only duplicate rows
        If return_groups=True: Dictionary where keys are tuple of duplicate values,
                              values are DataFrames of rows in that group
    
    Example:
        >>> # Find all duplicate rows
        >>> duplicates = find_duplicates(df)
        >>> print(f"Found {len(duplicates)} duplicate rows")
        >>>
        >>> # Find duplicates based on email column
        >>> duplicates = find_duplicates(df, subset=['email'])
        >>>
        >>> # Get duplicate groups for analysis
        >>> groups = find_duplicates(df, subset=['email'], return_groups=True)
        >>> for key, group_df in groups.items():
        >>>     print(f"Email {key}: {len(group_df)} duplicates")
    """
    # Identify duplicate rows
    duplicate_mask = df.duplicated(subset=subset, keep=keep)
    
    if not return_groups:
        # Return flat DataFrame of duplicates
        duplicates_df = df[duplicate_mask].copy()
        logger.info(f"Found {len(duplicates_df)} duplicate rows")
        return duplicates_df
    
    else:
        # Return grouped duplicates
        if duplicate_mask.sum() == 0:
            logger.info("No duplicates found")
            return {}
        
        # Get all rows that are part of any duplicate group
        all_duplicates_mask = df.duplicated(subset=subset, keep=False)
        duplicate_rows = df[all_duplicates_mask].copy()
        
        # Group by the subset columns (or all columns if subset is None)
        group_cols = subset if subset else list(df.columns)
        
        groups_dict = {}
        for name, group in duplicate_rows.groupby(group_cols, dropna=False):
            if len(group) > 1:  # Only include actual duplicates
                groups_dict[name] = group
        
        logger.info(f"Found {len(groups_dict)} duplicate groups with {len(duplicate_rows)} total rows")
        return groups_dict


def remove_duplicates_advanced(
    df: pd.DataFrame,
    subset: Optional[List[str]] = None,
    keep: Union[str, bool] = 'first',
    priority_column: Optional[str] = None,
    priority_order: str = 'max',
    inplace: bool = False
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Advanced duplicate removal with priority-based selection.
    
    When duplicates exist, this function can keep the row with the highest/lowest
    value in a priority column (e.g., keep the most recent record, or the one
    with the highest quality score).
    
    Args:
        df: Input DataFrame
        subset: Columns to consider for duplicate detection
        keep: Basic keep strategy ('first', 'last', False)
        priority_column: Column to use for priority selection (overrides keep)
        priority_order: 'max' to keep highest value, 'min' to keep lowest value
        inplace: Modify DataFrame in place
    
    Returns:
        Tuple of (deduplicated_dataframe, removal_report)
    
    Example:
        >>> # Keep the duplicate with the most recent timestamp
        >>> df_clean, report = remove_duplicates_advanced(
        ...     df,
        ...     subset=['user_id'],
        ...     priority_column='last_login_date',
        ...     priority_order='max'
        ... )
        >>>
        >>> # Keep the duplicate with the highest quality score
        >>> df_clean, report = remove_duplicates_advanced(
        ...     df,
        ...     subset=['email'],
        ...     priority_column='data_quality_score',
        ...     priority_order='max'
        ... )
    """
    if not inplace:
        df = df.copy()
    
    initial_rows = len(df)
    
    report = {
        'rows_before': initial_rows,
        'duplicates_found': 0,
        'duplicates_removed': 0,
        'method': 'priority-based' if priority_column else 'standard',
        'subset_columns': subset if subset else 'all columns',
        'keep_strategy': keep if not priority_column else f'priority:{priority_column}',
    }
    
    # Check for duplicates
    duplicate_mask = df.duplicated(subset=subset, keep=False)
    duplicates_found = int(duplicate_mask.sum())
    report['duplicates_found'] = duplicates_found
    
    if duplicates_found == 0:
        report['rows_after'] = len(df)
        logger.info("No duplicates found")
        return df, report
    
    if priority_column:
        # Priority-based deduplication
        if priority_column not in df.columns:
            raise ValueError(f"Priority column '{priority_column}' not found in DataFrame")
        
        logger.info(f"Removing duplicates using priority column: {priority_column} ({priority_order})")
        
        # Sort by priority column
        ascending = (priority_order == 'min')
        df_sorted = df.sort_values(by=priority_column, ascending=ascending)
        
        # Keep first after sorting (which is the highest/lowest priority)
        df_dedup = df_sorted.drop_duplicates(subset=subset, keep='first')
        
        # Restore original order by index
        df_dedup = df_dedup.sort_index()
        
        # Update the original df
        df.drop(df.index, inplace=True)
        df = pd.concat([df, df_dedup], ignore_index=False)
        df.reset_index(drop=True, inplace=True)
        
    else:
        # Standard deduplication
        df.drop_duplicates(subset=subset, keep=keep, inplace=True)
        df.reset_index(drop=True, inplace=True)
    
    report['rows_after'] = len(df)
    report['duplicates_removed'] = initial_rows - len(df)
    report['retention_rate'] = round((len(df) / initial_rows) * 100, 2)
    
    logger.info(
        f"Removed {report['duplicates_removed']} duplicates "
        f"({report['retention_rate']}% retention)"
    )
    
    return df, report


def remove_outliers(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    method: str = 'iqr',
    threshold: float = 1.5,
    inplace: bool = False
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Remove outliers from numeric columns using specified method.
    
    Args:
        df: Input DataFrame
        columns: List of columns to check (None = all numeric columns)
        method: Outlier detection method ('iqr', 'zscore')
        threshold: Threshold for outlier detection (1.5 for IQR, 3 for z-score)
        inplace: Modify DataFrame in place
    
    Returns:
        Tuple of (cleaned_dataframe, outlier_report)
    
    Example:
        >>> df_clean, report = remove_outliers(df, method='iqr', threshold=1.5)
        >>> print(f"Outliers removed: {report['total_outliers']}")
    """
    if not inplace:
        df = df.copy()
    
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    report = {
        'method': method,
        'threshold': threshold,
        'outliers_by_column': {},
        'rows_before': len(df),
        'rows_removed': 0
    }
    
    outlier_mask = pd.Series([False] * len(df))
    
    for col in columns:
        if col not in df.columns:
            continue
        
        if not pd.api.types.is_numeric_dtype(df[col]):
            continue
        
        if method == 'iqr':
            Q1 = df[col].quantile(0.25)
            Q3 = df[col].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            col_outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
        
        elif method == 'zscore':
            z_scores = np.abs((df[col] - df[col].mean()) / df[col].std())
            col_outliers = z_scores > threshold
        
        else:
            raise ValueError(f"Invalid method: {method}. Use 'iqr' or 'zscore'")
        
        outlier_count = col_outliers.sum()
        if outlier_count > 0:
            report['outliers_by_column'][col] = int(outlier_count)
            outlier_mask = outlier_mask | col_outliers
    
    # Remove rows with outliers
    df_clean = df[~outlier_mask]
    report['rows_after'] = len(df_clean)
    report['rows_removed'] = report['rows_before'] - report['rows_after']
    report['total_outliers'] = int(outlier_mask.sum())
    
    logger.info(f"Removed {report['rows_removed']} rows with outliers")
    
    return df_clean, report


def normalize_numeric_columns(
    df: pd.DataFrame,
    columns: Optional[List[str]] = None,
    method: str = 'minmax',
    inplace: bool = False
) -> pd.DataFrame:
    """
    Normalize numeric columns using specified method.
    
    Args:
        df: Input DataFrame
        columns: Columns to normalize (None = all numeric)
        method: Normalization method ('minmax', 'zscore', 'robust')
        inplace: Modify DataFrame in place
    
    Returns:
        Normalized DataFrame
    
    Example:
        >>> df_norm = normalize_numeric_columns(df, method='minmax')
    """
    if not inplace:
        df = df.copy()
    
    if columns is None:
        columns = df.select_dtypes(include=[np.number]).columns.tolist()
    
    for col in columns:
        if col not in df.columns or not pd.api.types.is_numeric_dtype(df[col]):
            continue
        
        if method == 'minmax':
            min_val = df[col].min()
            max_val = df[col].max()
            if max_val != min_val:
                normalized_values = (df[col] - min_val) / (max_val - min_val)
                df[col] = normalized_values.astype('float64')
        
        elif method == 'zscore':
            mean_val = df[col].mean()
            std_val = df[col].std()
            if std_val != 0:
                normalized_values = (df[col] - mean_val) / std_val
                df[col] = normalized_values.astype('float64')
        
        elif method == 'robust':
            median_val = df[col].median()
            q75 = df[col].quantile(0.75)
            q25 = df[col].quantile(0.25)
            iqr = q75 - q25
            if iqr != 0:
                normalized_values = (df[col] - median_val) / iqr
                df[col] = normalized_values.astype('float64')
        
        else:
            raise ValueError(f"Invalid method: {method}")
    
    logger.info(f"Normalized {len(columns)} columns using {method} method")
    
    return df


def convert_data_types(
    df: pd.DataFrame,
    type_mapping: Dict[str, str],
    errors: str = 'coerce',
    inplace: bool = False
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Convert column data types with error handling.
    
    Args:
        df: Input DataFrame
        type_mapping: Dictionary mapping column names to target types
        errors: How to handle conversion errors ('coerce', 'raise', 'ignore')
        inplace: Modify DataFrame in place
    
    Returns:
        Tuple of (converted_dataframe, conversion_report)
    
    Example:
        >>> type_map = {'age': 'int', 'price': 'float', 'date': 'datetime'}
        >>> df_converted, report = convert_data_types(df, type_map)
    """
    if not inplace:
        df = df.copy()
    
    report = {
        'conversions': {},
        'failures': {},
        'success_count': 0,
        'failure_count': 0
    }
    
    for col, target_type in type_mapping.items():
        if col not in df.columns:
            report['failures'][col] = f"Column not found"
            report['failure_count'] += 1
            continue
        
        try:
            original_type = str(df[col].dtype)
            
            if target_type.lower() in ['int', 'integer', 'int64']:
                df[col] = pd.to_numeric(df[col], errors=errors).astype('Int64')
            
            elif target_type.lower() in ['float', 'float64']:
                df[col] = pd.to_numeric(df[col], errors=errors)
            
            elif target_type.lower() in ['str', 'string', 'object']:
                df[col] = df[col].astype(str)
            
            elif target_type.lower() in ['datetime', 'datetime64']:
                df[col] = pd.to_datetime(df[col], errors=errors)
            
            elif target_type.lower() in ['bool', 'boolean']:
                df[col] = df[col].astype(bool)
            
            elif target_type.lower() == 'category':
                df[col] = df[col].astype('category')
            
            else:
                df[col] = df[col].astype(target_type)
            
            report['conversions'][col] = {
                'from': original_type,
                'to': str(df[col].dtype)
            }
            report['success_count'] += 1
            
        except Exception as e:
            report['failures'][col] = str(e)
            report['failure_count'] += 1
            logger.warning(f"Failed to convert column '{col}' to {target_type}: {e}")
    
    logger.info(
        f"Data type conversion: {report['success_count']} succeeded, "
        f"{report['failure_count']} failed"
    )
    
    return df, report


if __name__ == "__main__":
    """
    Example usage and testing of the data transformation module.
    """
    print("\n" + "="*70)
    print("Data Transformation Module - Production Examples")
    print("="*70 + "\n")
    
    # Example 1: Basic data cleaning with null handling
    print("Example 1: Basic data cleaning with null handling")
    print("-" * 70)
    
    # Create sample data with nulls
    sample_data = {
        'id': [1, 2, 3, 4, 5, 6, 7, 8],
        'name': ['Alice', 'Bob', None, 'David', 'Eve', 'Frank', 'Grace', 'Hank'],
        'age': [25, None, 30, 35, None, 40, 45, 50],
        'salary': [50000, 60000, None, 70000, 80000, None, 90000, 100000],
        'department': ['IT', 'HR', 'IT', None, 'Finance', 'IT', 'HR', 'Finance']
    }
    df = pd.DataFrame(sample_data)
    
    print(f"Original data: {len(df)} rows, {df.isnull().sum().sum()} nulls")
    print(df)
    
    # Clean data using mean fill for numeric columns
    cleaned_df, report = clean_data(
        df,
        null_strategy='fill_mean',
        drop_duplicates=True,
        standardize_columns=True
    )
    
    print(f"\n✓ Cleaned data: {len(cleaned_df)} rows, {cleaned_df.isnull().sum().sum()} nulls")
    print(f"  Data quality score: {report['data_quality_score']}%")
    print(f"  Actions taken: {len(report['actions_taken'])}")
    for action in report['actions_taken']:
        print(f"    - {action}")
    
    print("\nCleaned DataFrame:")
    print(cleaned_df)
    
    print("\n" + "="*70)
    
    # Example 2: Column-specific null handling strategies
    print("\nExample 2: Column-specific null handling strategies")
    print("-" * 70)
    
    strategies = {
        'name': 'fill_mode',
        'age': 'fill_median',
        'salary': 'fill_mean',
        'department': 'fill_mode'
    }
    
    cleaned_df2, report2 = clean_data(
        df,
        null_strategy='keep',  # Default strategy (won't be used)
        column_specific_strategies=strategies,
        standardize_columns=False
    )
    
    print(f"✓ Cleaned with column-specific strategies")
    print(f"  Nulls handled: {report2['nulls_handled']}")
    print(f"  Quality score: {report2['data_quality_score']}%")
    print("\nResult:")
    print(cleaned_df2)
    
    print("\n" + "="*70)
    print("Available null handling strategies:")
    for strategy in NullHandlingStrategy:
        print(f"  • {strategy.value}")
    print("="*70 + "\n")
    
    # Example 3: Advanced duplicate removal
    print("Example 3: Advanced duplicate removal")
    print("-" * 70)
    
    # Create data with duplicates
    duplicate_data = {
        'user_id': [1, 2, 3, 2, 4, 3, 5],
        'email': ['a@mail.com', 'b@mail.com', 'c@mail.com', 'b@mail.com', 'd@mail.com', 'c@mail.com', 'e@mail.com'],
        'name': ['Alice', 'Bob', 'Charlie', 'Bob', 'David', 'Charlie', 'Eve'],
        'score': [85, 90, 75, 95, 80, 70, 88],
        'date': pd.to_datetime(['2026-01-01', '2026-01-02', '2026-01-03', '2026-01-04', '2026-01-05', '2026-01-06', '2026-01-07'])
    }
    df_dup = pd.DataFrame(duplicate_data)
    
    print(f"Data with duplicates: {len(df_dup)} rows")
    print(df_dup)
    print(f"\nDuplicates based on user_id: {df_dup.duplicated(subset=['user_id'], keep=False).sum()}")
    
    # Remove duplicates keeping first occurrence
    cleaned_df3, report3 = clean_data(
        df_dup,
        drop_duplicates=True,
        duplicate_subset=['user_id'],
        duplicate_keep='first',
        standardize_columns=False,
        null_strategy='keep'
    )
    
    print(f"\n✓ After removing duplicates (keep='first'):")
    print(f"  Rows: {len(cleaned_df3)}")
    print(f"  Duplicates removed: {report3['duplicate_report']['duplicates_removed']}")
    print(f"  Duplicate groups: {report3['duplicate_report']['duplicate_groups']}")
    print(cleaned_df3[['user_id', 'email', 'name', 'score']])
    
    print("\n" + "-" * 70)
    
    # Mark duplicates instead of removing
    marked_df, report4 = clean_data(
        df_dup,
        mark_duplicates_only=True,
        duplicate_subset=['user_id'],
        duplicate_keep='first',
        standardize_columns=False,
        null_strategy='keep'
    )
    
    print(f"\n✓ After marking duplicates:")
    print(f"  Duplicates marked: {report4['duplicate_report']['duplicates_marked']}")
    print(marked_df[['user_id', 'email', 'name', '_is_duplicate']])
    
    print("\n" + "="*70)
    
    # Example 4: Priority-based duplicate removal
    print("\nExample 4: Priority-based duplicate removal")
    print("-" * 70)
    
    print("Removing duplicates based on highest score:")
    priority_df, priority_report = remove_duplicates_advanced(
        df_dup,
        subset=['user_id'],
        priority_column='score',
        priority_order='max'
    )
    
    print(f"✓ Kept rows with highest scores:")
    print(f"  Rows: {len(priority_df)}")
    print(f"  Duplicates removed: {priority_report['duplicates_removed']}")
    print(priority_df[['user_id', 'email', 'name', 'score']])
    
    print("\n" + "-" * 70)
    
    # Find and analyze duplicates
    print("\nFinding duplicate groups:")
    dup_groups = find_duplicates(df_dup, subset=['user_id'], return_groups=True)
    
    print(f"✓ Found {len(dup_groups)} duplicate groups:")
    for key, group in dup_groups.items():
        print(f"  User ID {key}: {len(group)} occurrences")
        print(f"    Scores: {group['score'].tolist()}")
    
    print("\n" + "="*70)
    print("Duplicate Removal Features:")
    print("  ✓ Remove duplicates based on subset of columns")
    print("  ✓ Choose which duplicate to keep (first/last/none)")
    print("  ✓ Mark duplicates instead of removing")
    print("  ✓ Priority-based selection (keep highest/lowest value)")
    print("  ✓ Detailed duplicate analysis and reporting")
    print("="*70 + "\n")
