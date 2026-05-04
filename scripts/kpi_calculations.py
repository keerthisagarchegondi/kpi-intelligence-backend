"""
KPI Calculations Module
========================
Production-level KPI calculation functions for business intelligence and analytics.

This module provides comprehensive functions for calculating key performance indicators (KPIs)
including revenue metrics, growth rates, customer analytics, and business performance indicators.

Author: KPI Intelligence Team
Created: 2026-05-04
"""

import logging
import warnings
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Tuple, Callable
from datetime import datetime, timedelta
from enum import Enum
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
import numpy as np

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class KPICalculationError(Exception):
    """Custom exception for KPI calculation errors."""
    pass


class RevenueMetricType(Enum):
    """Enumeration of revenue metric calculation types."""
    TOTAL = "total"  # Total revenue sum
    AVERAGE = "average"  # Average revenue per transaction
    MEDIAN = "median"  # Median revenue
    CUMULATIVE = "cumulative"  # Cumulative revenue over time
    GROWTH_RATE = "growth_rate"  # Period-over-period growth rate
    MOVING_AVERAGE = "moving_average"  # Moving average revenue
    BY_CATEGORY = "by_category"  # Revenue grouped by category
    BY_PERIOD = "by_period"  # Revenue grouped by time period


class AggregationPeriod(Enum):
    """Time period aggregation options."""
    DAILY = "D"
    WEEKLY = "W"
    MONTHLY = "M"
    QUARTERLY = "Q"
    YEARLY = "Y"


def calculate_revenue(
    df: pd.DataFrame,
    revenue_column: str = 'value',
    date_column: Optional[str] = None,
    category_column: Optional[str] = None,
    metric_type: Union[str, RevenueMetricType] = RevenueMetricType.TOTAL,
    period: Optional[Union[str, AggregationPeriod]] = None,
    moving_window: int = 7,
    growth_period: int = 1,
    include_breakdown: bool = False,
    exclude_zero: bool = True,
    exclude_negative: bool = False,
    round_decimals: int = 2,
    validate_data: bool = True
) -> Union[float, pd.DataFrame, Dict[str, Any]]:
    """
    Calculate revenue metrics with comprehensive options and production-level error handling.
    
    This function provides flexible revenue calculation capabilities including:
    - Total, average, and median revenue
    - Revenue by category or time period
    - Growth rates and moving averages
    - Cumulative revenue tracking
    - Data validation and quality checks
    
    Args:
        df: Input DataFrame containing revenue data
        revenue_column: Name of the column containing revenue values (default: 'value')
        date_column: Name of the date column for time-based analysis (optional)
        category_column: Name of the category column for segmented analysis (optional)
        metric_type: Type of revenue metric to calculate (see RevenueMetricType enum)
        period: Time period for aggregation (e.g., 'D', 'W', 'M', 'Q', 'Y')
        moving_window: Window size for moving average calculation (default: 7)
        growth_period: Number of periods to calculate growth rate (default: 1)
        include_breakdown: Include detailed breakdown in results
        exclude_zero: Exclude zero values from calculations
        exclude_negative: Exclude negative values from calculations
        round_decimals: Number of decimal places for rounding (default: 2)
        validate_data: Perform data validation before calculation
    
    Returns:
        - float: Single revenue value for simple metrics (TOTAL, AVERAGE, MEDIAN)
        - DataFrame: Time series or categorical breakdown for complex metrics
        - Dict: Comprehensive report when include_breakdown=True
    
    Raises:
        KPICalculationError: If calculation fails or data validation fails
        ValueError: If invalid parameters or missing required columns
    
    Examples:
        >>> # Calculate total revenue
        >>> total_rev = calculate_revenue(df, revenue_column='sales')
        >>> print(f"Total Revenue: ${total_rev:,.2f}")
        
        >>> # Calculate monthly revenue with breakdown
        >>> monthly_rev = calculate_revenue(
        ...     df, 
        ...     revenue_column='sales',
        ...     date_column='order_date',
        ...     metric_type='by_period',
        ...     period='M'
        ... )
        >>> print(monthly_rev)
        
        >>> # Calculate revenue by category with detailed report
        >>> result = calculate_revenue(
        ...     df,
        ...     revenue_column='amount',
        ...     category_column='product_category',
        ...     metric_type='by_category',
        ...     include_breakdown=True
        ... )
        >>> print(f"Total: ${result['total_revenue']:,.2f}")
        >>> print(result['breakdown'])
        
        >>> # Calculate revenue growth rate
        >>> growth = calculate_revenue(
        ...     df,
        ...     revenue_column='sales',
        ...     date_column='date',
        ...     metric_type='growth_rate',
        ...     period='M',
        ...     growth_period=1
        ... )
        >>> print(growth)
    """
    start_time = datetime.now()
    
    # Input validation
    if df is None or not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")
    
    if df.empty:
        logger.warning("Input DataFrame is empty")
        return 0.0 if not include_breakdown else {
            'total_revenue': 0.0,
            'status': 'no_data',
            'message': 'Empty DataFrame'
        }
    
    # Check if revenue column exists
    if revenue_column not in df.columns:
        raise ValueError(
            f"Revenue column '{revenue_column}' not found. "
            f"Available columns: {list(df.columns)}"
        )
    
    # Convert string metric type to enum
    if isinstance(metric_type, str):
        try:
            metric_type = RevenueMetricType(metric_type.lower())
        except ValueError:
            raise ValueError(
                f"Invalid metric_type: {metric_type}. "
                f"Valid options: {[m.value for m in RevenueMetricType]}"
            )
    
    # Convert string period to enum if provided
    if period and isinstance(period, str):
        try:
            period = AggregationPeriod(period.upper())
        except ValueError:
            # Allow pandas frequency strings directly
            pass
    
    try:
        logger.info(
            f"Calculating revenue: metric={metric_type.value}, "
            f"column={revenue_column}, records={len(df)}"
        )
        
        # Work on a copy to avoid modifying original
        df_work = df.copy()
        
        # Data validation and cleaning
        if validate_data:
            _validate_revenue_data(df_work, revenue_column, date_column, category_column)
        
        # Ensure revenue column is numeric
        df_work[revenue_column] = pd.to_numeric(
            df_work[revenue_column], 
            errors='coerce'
        )
        
        # Handle missing values
        initial_count = len(df_work)
        df_work = df_work.dropna(subset=[revenue_column])
        dropped_count = initial_count - len(df_work)
        
        if dropped_count > 0:
            logger.warning(
                f"Dropped {dropped_count} rows with null revenue values"
            )
        
        # Filter zero values if requested
        if exclude_zero:
            df_work = df_work[df_work[revenue_column] != 0]
        
        # Filter negative values if requested
        if exclude_negative:
            negative_count = (df_work[revenue_column] < 0).sum()
            if negative_count > 0:
                logger.warning(
                    f"Excluding {negative_count} negative revenue values"
                )
                df_work = df_work[df_work[revenue_column] >= 0]
        
        # Check if we have data left after filtering
        if len(df_work) == 0:
            logger.warning("No valid revenue data after filtering")
            return 0.0 if not include_breakdown else {
                'total_revenue': 0.0,
                'status': 'no_valid_data',
                'message': 'No valid data after filtering'
            }
        
        # Calculate based on metric type
        if metric_type == RevenueMetricType.TOTAL:
            result = _calculate_total_revenue(
                df_work, revenue_column, round_decimals
            )
        
        elif metric_type == RevenueMetricType.AVERAGE:
            result = _calculate_average_revenue(
                df_work, revenue_column, round_decimals
            )
        
        elif metric_type == RevenueMetricType.MEDIAN:
            result = _calculate_median_revenue(
                df_work, revenue_column, round_decimals
            )
        
        elif metric_type == RevenueMetricType.CUMULATIVE:
            if date_column is None:
                raise ValueError("date_column is required for cumulative revenue calculation")
            result = _calculate_cumulative_revenue(
                df_work, revenue_column, date_column, period, round_decimals
            )
        
        elif metric_type == RevenueMetricType.GROWTH_RATE:
            if date_column is None:
                raise ValueError("date_column is required for growth rate calculation")
            result = _calculate_revenue_growth_rate(
                df_work, revenue_column, date_column, period, growth_period, round_decimals
            )
        
        elif metric_type == RevenueMetricType.MOVING_AVERAGE:
            if date_column is None:
                raise ValueError("date_column is required for moving average calculation")
            result = _calculate_moving_average_revenue(
                df_work, revenue_column, date_column, moving_window, round_decimals
            )
        
        elif metric_type == RevenueMetricType.BY_CATEGORY:
            if category_column is None:
                raise ValueError("category_column is required for category-based calculation")
            result = _calculate_revenue_by_category(
                df_work, revenue_column, category_column, round_decimals
            )
        
        elif metric_type == RevenueMetricType.BY_PERIOD:
            if date_column is None:
                raise ValueError("date_column is required for period-based calculation")
            result = _calculate_revenue_by_period(
                df_work, revenue_column, date_column, period, round_decimals
            )
        
        else:
            raise ValueError(f"Unsupported metric type: {metric_type}")
        
        # Add detailed breakdown if requested
        if include_breakdown:
            if isinstance(result, (int, float)):
                breakdown = {
                    'total_revenue': float(result),
                    'metric_type': metric_type.value,
                    'records_processed': len(df_work),
                    'records_excluded': initial_count - len(df_work),
                    'min_revenue': round(float(df_work[revenue_column].min()), round_decimals),
                    'max_revenue': round(float(df_work[revenue_column].max()), round_decimals),
                    'avg_revenue': round(float(df_work[revenue_column].mean()), round_decimals),
                    'std_deviation': round(float(df_work[revenue_column].std()), round_decimals),
                    'processing_time_seconds': round(
                        (datetime.now() - start_time).total_seconds(), 3
                    ),
                    'status': 'success'
                }
                result = breakdown
            elif isinstance(result, pd.DataFrame):
                breakdown = {
                    'breakdown': result,
                    'total_revenue': round(float(df_work[revenue_column].sum()), round_decimals),
                    'metric_type': metric_type.value,
                    'records_processed': len(df_work),
                    'records_excluded': initial_count - len(df_work),
                    'processing_time_seconds': round(
                        (datetime.now() - start_time).total_seconds(), 3
                    ),
                    'status': 'success'
                }
                result = breakdown
        
        logger.info(
            f"Revenue calculation completed: metric={metric_type.value}, "
            f"result_type={type(result).__name__}"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Revenue calculation failed: {str(e)}", exc_info=True)
        raise KPICalculationError(
            f"Revenue calculation failed: {str(e)}"
        ) from e


def _validate_revenue_data(
    df: pd.DataFrame,
    revenue_column: str,
    date_column: Optional[str],
    category_column: Optional[str]
) -> None:
    """
    Validate revenue data for quality and completeness.
    
    Args:
        df: DataFrame to validate
        revenue_column: Revenue column name
        date_column: Date column name (optional)
        category_column: Category column name (optional)
    
    Raises:
        ValueError: If validation fails
    """
    # Check required columns exist
    if revenue_column not in df.columns:
        raise ValueError(f"Revenue column '{revenue_column}' not found")
    
    if date_column and date_column not in df.columns:
        raise ValueError(f"Date column '{date_column}' not found")
    
    if category_column and category_column not in df.columns:
        raise ValueError(f"Category column '{category_column}' not found")
    
    # Check for empty columns
    if df[revenue_column].isnull().all():
        raise ValueError(f"Revenue column '{revenue_column}' contains only null values")
    
    # Validate date column if provided
    if date_column:
        try:
            pd.to_datetime(df[date_column], errors='coerce')
        except Exception as e:
            logger.warning(f"Date column validation failed: {e}")


def _calculate_total_revenue(
    df: pd.DataFrame,
    revenue_column: str,
    round_decimals: int
) -> float:
    """Calculate total revenue."""
    total = df[revenue_column].sum()
    return round(float(total), round_decimals)


def _calculate_average_revenue(
    df: pd.DataFrame,
    revenue_column: str,
    round_decimals: int
) -> float:
    """Calculate average revenue."""
    average = df[revenue_column].mean()
    return round(float(average), round_decimals)


def _calculate_median_revenue(
    df: pd.DataFrame,
    revenue_column: str,
    round_decimals: int
) -> float:
    """Calculate median revenue."""
    median = df[revenue_column].median()
    return round(float(median), round_decimals)


def _calculate_cumulative_revenue(
    df: pd.DataFrame,
    revenue_column: str,
    date_column: str,
    period: Optional[Union[str, AggregationPeriod]],
    round_decimals: int
) -> pd.DataFrame:
    """Calculate cumulative revenue over time."""
    # Ensure date column is datetime
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    
    # Sort by date
    df_sorted = df.sort_values(date_column).copy()
    
    # Calculate cumulative sum
    df_sorted['cumulative_revenue'] = df_sorted[revenue_column].cumsum()
    
    # Group by period if specified
    if period:
        period_str = period.value if isinstance(period, AggregationPeriod) else period
        result = df_sorted.set_index(date_column).resample(period_str).agg({
            revenue_column: 'sum',
            'cumulative_revenue': 'last'
        }).reset_index()
        result.columns = ['period', 'period_revenue', 'cumulative_revenue']
    else:
        result = df_sorted[[date_column, revenue_column, 'cumulative_revenue']].copy()
        result.columns = ['date', 'revenue', 'cumulative_revenue']
    
    # Round values
    numeric_cols = result.select_dtypes(include=[np.number]).columns
    result[numeric_cols] = result[numeric_cols].round(round_decimals)
    
    return result


def _calculate_revenue_growth_rate(
    df: pd.DataFrame,
    revenue_column: str,
    date_column: str,
    period: Optional[Union[str, AggregationPeriod]],
    growth_period: int,
    round_decimals: int
) -> pd.DataFrame:
    """Calculate revenue growth rate over time."""
    # Ensure date column is datetime
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    
    # Determine aggregation period
    if period is None:
        period = AggregationPeriod.MONTHLY
    
    period_str = period.value if isinstance(period, AggregationPeriod) else period
    
    # Aggregate revenue by period
    revenue_by_period = df.set_index(date_column).resample(period_str)[revenue_column].sum()
    
    # Calculate growth rate
    growth_rate = revenue_by_period.pct_change(periods=growth_period) * 100
    
    # Create result DataFrame
    result = pd.DataFrame({
        'period': revenue_by_period.index,
        'revenue': revenue_by_period.values,
        'growth_rate_pct': growth_rate.values
    })
    
    # Round values
    result['revenue'] = result['revenue'].round(round_decimals)
    result['growth_rate_pct'] = result['growth_rate_pct'].round(round_decimals)
    
    return result


def _calculate_moving_average_revenue(
    df: pd.DataFrame,
    revenue_column: str,
    date_column: str,
    window: int,
    round_decimals: int
) -> pd.DataFrame:
    """Calculate moving average revenue."""
    # Ensure date column is datetime
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    
    # Sort by date
    df_sorted = df.sort_values(date_column).copy()
    
    # Calculate moving average
    df_sorted['moving_avg_revenue'] = (
        df_sorted[revenue_column].rolling(window=window, min_periods=1).mean()
    )
    
    # Create result
    result = df_sorted[[date_column, revenue_column, 'moving_avg_revenue']].copy()
    result.columns = ['date', 'revenue', f'ma_{window}_revenue']
    
    # Round values
    numeric_cols = result.select_dtypes(include=[np.number]).columns
    result[numeric_cols] = result[numeric_cols].round(round_decimals)
    
    return result


def _calculate_revenue_by_category(
    df: pd.DataFrame,
    revenue_column: str,
    category_column: str,
    round_decimals: int
) -> pd.DataFrame:
    """Calculate revenue grouped by category."""
    result = df.groupby(category_column).agg({
        revenue_column: [
            ('total_revenue', 'sum'),
            ('avg_revenue', 'mean'),
            ('count', 'count'),
            ('min_revenue', 'min'),
            ('max_revenue', 'max')
        ]
    }).reset_index()
    
    # Flatten column names
    result.columns = [category_column, 'total_revenue', 'avg_revenue', 'count', 'min_revenue', 'max_revenue']
    
    # Calculate percentage of total
    total = result['total_revenue'].sum()
    result['percentage'] = (result['total_revenue'] / total * 100).round(round_decimals)
    
    # Sort by total revenue descending
    result = result.sort_values('total_revenue', ascending=False)
    
    # Round numeric columns
    numeric_cols = ['total_revenue', 'avg_revenue', 'min_revenue', 'max_revenue']
    result[numeric_cols] = result[numeric_cols].round(round_decimals)
    
    return result


def _calculate_revenue_by_period(
    df: pd.DataFrame,
    revenue_column: str,
    date_column: str,
    period: Optional[Union[str, AggregationPeriod]],
    round_decimals: int
) -> pd.DataFrame:
    """Calculate revenue grouped by time period."""
    # Ensure date column is datetime
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    
    # Determine aggregation period
    if period is None:
        period = AggregationPeriod.MONTHLY
    
    period_str = period.value if isinstance(period, AggregationPeriod) else period
    
    # Aggregate by period
    result = df.set_index(date_column).resample(period_str).agg({
        revenue_column: [
            ('total_revenue', 'sum'),
            ('avg_revenue', 'mean'),
            ('count', 'count'),
            ('min_revenue', 'min'),
            ('max_revenue', 'max')
        ]
    }).reset_index()
    
    # Flatten column names
    result.columns = ['period', 'total_revenue', 'avg_revenue', 'count', 'min_revenue', 'max_revenue']
    
    # Round numeric columns
    numeric_cols = ['total_revenue', 'avg_revenue', 'min_revenue', 'max_revenue']
    result[numeric_cols] = result[numeric_cols].round(round_decimals)
    
    return result


# Additional KPI calculation functions

def calculate_profit_margin(
    df: pd.DataFrame,
    revenue_column: str = 'revenue',
    cost_column: str = 'cost',
    round_decimals: int = 2
) -> float:
    """
    Calculate profit margin percentage.
    
    Args:
        df: DataFrame with revenue and cost data
        revenue_column: Name of revenue column
        cost_column: Name of cost column
        round_decimals: Decimal places for rounding
    
    Returns:
        Profit margin as percentage
    
    Example:
        >>> margin = calculate_profit_margin(df, 'revenue', 'cost')
        >>> print(f"Profit Margin: {margin}%")
    """
    total_revenue = df[revenue_column].sum()
    total_cost = df[cost_column].sum()
    
    if total_revenue == 0:
        logger.warning("Total revenue is zero, cannot calculate profit margin")
        return 0.0
    
    profit_margin = ((total_revenue - total_cost) / total_revenue) * 100
    return round(float(profit_margin), round_decimals)


def calculate_arpu(
    df: pd.DataFrame,
    revenue_column: str = 'revenue',
    user_column: Optional[str] = None,
    round_decimals: int = 2
) -> float:
    """
    Calculate Average Revenue Per User (ARPU).
    
    Args:
        df: DataFrame with revenue data
        revenue_column: Name of revenue column
        user_column: Name of user/customer ID column (if None, uses row count)
        round_decimals: Decimal places for rounding
    
    Returns:
        Average revenue per user
    
    Example:
        >>> arpu = calculate_arpu(df, 'revenue', 'user_id')
        >>> print(f"ARPU: ${arpu:.2f}")
    """
    total_revenue = df[revenue_column].sum()
    
    if user_column:
        unique_users = df[user_column].nunique()
    else:
        unique_users = len(df)
    
    if unique_users == 0:
        logger.warning("No users found, cannot calculate ARPU")
        return 0.0
    
    arpu = total_revenue / unique_users
    return round(float(arpu), round_decimals)


def calculate_conversion_rate(
    df: pd.DataFrame,
    conversion_column: str = 'converted',
    total_column: Optional[str] = None,
    round_decimals: int = 2
) -> float:
    """
    Calculate conversion rate percentage.
    
    Args:
        df: DataFrame with conversion data
        conversion_column: Column indicating conversions (boolean or 1/0)
        total_column: Column for total events (if None, uses all rows)
        round_decimals: Decimal places for rounding
    
    Returns:
        Conversion rate as percentage
    
    Example:
        >>> conv_rate = calculate_conversion_rate(df, 'purchased')
        >>> print(f"Conversion Rate: {conv_rate}%")
    """
    conversions = df[conversion_column].sum()
    
    if total_column:
        total = df[total_column].sum()
    else:
        total = len(df)
    
    if total == 0:
        logger.warning("Total is zero, cannot calculate conversion rate")
        return 0.0
    
    conversion_rate = (conversions / total) * 100
    return round(float(conversion_rate), round_decimals)


if __name__ == "__main__":
    """
    Example usage and testing of the KPI calculations module.
    """
    print("\n" + "="*70)
    print("KPI Calculations Module - Production Examples")
    print("="*70 + "\n")
    
    # Example 1: Calculate total revenue
    print("Example 1: Calculate Total Revenue")
    print("-" * 70)
    
    # Create sample sales data
    sample_data = {
        'id': range(1, 11),
        'product': ['Product A', 'Product B', 'Product C'] * 3 + ['Product A'],
        'category': ['Electronics', 'Furniture', 'Clothing'] * 3 + ['Electronics'],
        'value': [1500, 750, 350, 2200, 1800, 420, 950, 3100, 580, 1250],
        'date': pd.date_range('2026-04-01', periods=10, freq='D')
    }
    df = pd.DataFrame(sample_data)
    
    print("Sample Data:")
    print(df)
    
    # Calculate total revenue
    total_revenue = calculate_revenue(df, revenue_column='value')
    print(f"\n✓ Total Revenue: ${total_revenue:,.2f}")
    
    # Calculate with detailed breakdown
    result = calculate_revenue(df, revenue_column='value', include_breakdown=True)
    print(f"\n✓ Revenue Metrics:")
    print(f"  Total: ${result['total_revenue']:,.2f}")
    print(f"  Average: ${result['avg_revenue']:,.2f}")
    print(f"  Min: ${result['min_revenue']:,.2f}")
    print(f"  Max: ${result['max_revenue']:,.2f}")
    print(f"  Std Dev: ${result['std_deviation']:,.2f}")
    
    print("\n" + "="*70)
    
    # Example 2: Revenue by category
    print("\nExample 2: Revenue by Category")
    print("-" * 70)
    
    revenue_by_category = calculate_revenue(
        df,
        revenue_column='value',
        category_column='category',
        metric_type='by_category'
    )
    
    print("\nRevenue by Category:")
    print(revenue_by_category)
    
    print("\n" + "="*70)
    
    # Example 3: Daily revenue with moving average
    print("\nExample 3: Revenue with Moving Average")
    print("-" * 70)
    
    moving_avg = calculate_revenue(
        df,
        revenue_column='value',
        date_column='date',
        metric_type='moving_average',
        moving_window=3
    )
    
    print("\nRevenue with 3-day Moving Average:")
    print(moving_avg)
    
    print("\n" + "="*70)
    
    # Example 4: Calculate ARPU
    print("\nExample 4: Calculate ARPU")
    print("-" * 70)
    
    arpu = calculate_arpu(df, revenue_column='value')
    print(f"\nAverage Revenue Per Transaction: ${arpu:,.2f}")
    
    print("\n" + "="*70)
    print("\n✓ All KPI calculations completed successfully!")
