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


def calculate_product_performance(
    df: pd.DataFrame,
    product_column: str = 'product',
    metrics: Optional[List[str]] = None,
    revenue_column: Optional[str] = 'revenue',
    quantity_column: Optional[str] = 'quantity',
    date_column: Optional[str] = None,
    category_column: Optional[str] = None,
    cost_column: Optional[str] = None,
    return_column: Optional[str] = None,
    period: Optional[Union[str, AggregationPeriod]] = None,
    top_n: Optional[int] = None,
    include_trends: bool = False,
    include_rankings: bool = True,
    round_decimals: int = 2,
    validate_data: bool = True
) -> Union[pd.DataFrame, Dict[str, Any]]:
    """
    Calculate comprehensive product performance metrics for business analytics.
    
    This function provides multi-dimensional product performance analysis including:
    - Revenue and sales volume metrics
    - Profit margins and ROI
    - Growth rates and trends
    - Market share and rankings
    - Return rates and quality metrics
    - Time-based performance patterns
    
    Args:
        df: Input DataFrame with product sales/transaction data
        product_column: Name of column containing product identifiers
        metrics: List of specific metrics to calculate. If None, calculates all available.
            Options: ['revenue', 'quantity', 'profit', 'margin', 'roi', 'growth', 
                     'market_share', 'return_rate', 'avg_price', 'velocity']
        revenue_column: Name of column containing revenue/sales values
        quantity_column: Name of column containing quantity sold
        date_column: Name of column containing transaction dates (for trend analysis)
        category_column: Name of column containing product categories
        cost_column: Name of column containing product costs (for profit calculations)
        return_column: Name of column indicating returns (boolean or count)
        period: Time period for trend analysis ('D', 'W', 'M', 'Q', 'Y')
        top_n: Return only top N performing products (by revenue)
        include_trends: Include time-based trend analysis
        include_rankings: Include product rankings
        round_decimals: Number of decimal places for rounding
        validate_data: Perform data validation before calculation
    
    Returns:
        - DataFrame: Product performance metrics with one row per product
        - Dict: Comprehensive performance report when include_trends=True
    
    Raises:
        KPICalculationError: If calculation fails or data validation fails
        ValueError: If invalid parameters or missing required columns
    
    Examples:
        >>> # Basic product performance
        >>> performance = calculate_product_performance(
        ...     df,
        ...     product_column='product_name',
        ...     revenue_column='sales'
        ... )
        >>> print(performance.head())
        
        >>> # Comprehensive analysis with profit and trends
        >>> performance = calculate_product_performance(
        ...     df,
        ...     product_column='product_id',
        ...     revenue_column='revenue',
        ...     cost_column='cost',
        ...     date_column='order_date',
        ...     include_trends=True,
        ...     top_n=10
        ... )
        >>> print(f"Top Product: {performance['product'][0]}")
        >>> print(f"Revenue: ${performance['total_revenue'][0]:,.2f}")
        
        >>> # Category-level performance with returns
        >>> performance = calculate_product_performance(
        ...     df,
        ...     product_column='product',
        ...     category_column='category',
        ...     return_column='is_returned',
        ...     metrics=['revenue', 'quantity', 'return_rate']
        ... )
    """
    start_time = datetime.now()
    
    # Input validation
    if df is None or not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")
    
    if df.empty:
        logger.warning("Input DataFrame is empty")
        return pd.DataFrame()
    
    # Check required columns
    if product_column not in df.columns:
        raise ValueError(
            f"Product column '{product_column}' not found. "
            f"Available columns: {list(df.columns)}"
        )
    
    try:
        logger.info(
            f"Calculating product performance: {df[product_column].nunique()} products, "
            f"{len(df)} transactions"
        )
        
        # Work on a copy
        df_work = df.copy()
        
        # Data validation
        if validate_data:
            _validate_product_data(df_work, product_column, revenue_column)
        
        # Initialize metrics dictionary
        available_metrics = {
            'revenue': revenue_column is not None,
            'quantity': quantity_column is not None,
            'profit': revenue_column is not None and cost_column is not None,
            'margin': revenue_column is not None and cost_column is not None,
            'roi': revenue_column is not None and cost_column is not None,
            'return_rate': return_column is not None,
            'avg_price': revenue_column is not None and quantity_column is not None,
            'market_share': revenue_column is not None
        }
        
        # If no specific metrics requested, use all available
        if metrics is None:
            metrics = [k for k, v in available_metrics.items() if v]
        else:
            # Validate requested metrics
            invalid_metrics = []
            for metric in metrics:
                if metric not in available_metrics:
                    invalid_metrics.append(metric)
                elif not available_metrics[metric]:
                    logger.warning(
                        f"Metric '{metric}' requested but required columns not available"
                    )
        
        # Start building aggregation dictionary
        agg_dict = {}
        
        # Revenue metrics
        if 'revenue' in metrics and revenue_column:
            df_work[revenue_column] = pd.to_numeric(df_work[revenue_column], errors='coerce')
            agg_dict[f'{revenue_column}_sum'] = (revenue_column, 'sum')
            agg_dict[f'{revenue_column}_mean'] = (revenue_column, 'mean')
            agg_dict[f'{revenue_column}_count'] = (revenue_column, 'count')
        
        # Quantity metrics
        if 'quantity' in metrics and quantity_column:
            df_work[quantity_column] = pd.to_numeric(df_work[quantity_column], errors='coerce')
            agg_dict[f'{quantity_column}_sum'] = (quantity_column, 'sum')
            agg_dict[f'{quantity_column}_mean'] = (quantity_column, 'mean')
        
        # Cost metrics (for profit calculations)
        if cost_column and ('profit' in metrics or 'margin' in metrics or 'roi' in metrics):
            df_work[cost_column] = pd.to_numeric(df_work[cost_column], errors='coerce')
            agg_dict[f'{cost_column}_sum'] = (cost_column, 'sum')
        
        # Return metrics
        if 'return_rate' in metrics and return_column:
            if df_work[return_column].dtype == 'bool':
                df_work[return_column] = df_work[return_column].astype(int)
            agg_dict[f'{return_column}_sum'] = (return_column, 'sum')
        
        # Aggregate by product
        if agg_dict:
            result = df_work.groupby(product_column).agg(**agg_dict).reset_index()
        else:
            result = df_work.groupby(product_column).size().reset_index(name='count')
        
        # Rename columns for clarity
        rename_map = {}
        if revenue_column:
            rename_map[f'{revenue_column}_sum'] = 'total_revenue'
            rename_map[f'{revenue_column}_mean'] = 'avg_revenue'
            rename_map[f'{revenue_column}_count'] = 'transaction_count'
        if quantity_column:
            rename_map[f'{quantity_column}_sum'] = 'total_quantity'
            rename_map[f'{quantity_column}_mean'] = 'avg_quantity'
        if cost_column:
            rename_map[f'{cost_column}_sum'] = 'total_cost'
        if return_column:
            rename_map[f'{return_column}_sum'] = 'return_count'
        
        result = result.rename(columns=rename_map)
        
        # Calculate derived metrics
        
        # Profit
        if 'profit' in metrics and 'total_revenue' in result.columns and 'total_cost' in result.columns:
            result['total_profit'] = result['total_revenue'] - result['total_cost']
        
        # Profit Margin
        if 'margin' in metrics and 'total_revenue' in result.columns and 'total_cost' in result.columns:
            result['profit_margin'] = (
                (result['total_revenue'] - result['total_cost']) / result['total_revenue'] * 100
            ).fillna(0)
        
        # ROI
        if 'roi' in metrics and 'total_revenue' in result.columns and 'total_cost' in result.columns:
            result['roi'] = (
                ((result['total_revenue'] - result['total_cost']) / result['total_cost']) * 100
            ).replace([np.inf, -np.inf], 0).fillna(0)
        
        # Return Rate
        if 'return_rate' in metrics and 'return_count' in result.columns:
            result['return_rate'] = (
                result['return_count'] / result['transaction_count'] * 100
            ).fillna(0)
        
        # Average Price
        if 'avg_price' in metrics and 'total_revenue' in result.columns and 'total_quantity' in result.columns:
            result['avg_price'] = (result['total_revenue'] / result['total_quantity']).fillna(0)
        
        # Market Share
        if 'market_share' in metrics and 'total_revenue' in result.columns:
            total_market = result['total_revenue'].sum()
            result['market_share'] = (result['total_revenue'] / total_market * 100).fillna(0)
        
        # Add category if provided
        if category_column and category_column in df_work.columns:
            category_map = df_work.groupby(product_column)[category_column].first()
            result = result.merge(
                category_map.reset_index(),
                on=product_column,
                how='left'
            )
        
        # Add rankings
        if include_rankings and 'total_revenue' in result.columns:
            result['revenue_rank'] = result['total_revenue'].rank(ascending=False, method='dense').astype(int)
            
            if 'total_quantity' in result.columns:
                result['quantity_rank'] = result['total_quantity'].rank(ascending=False, method='dense').astype(int)
            
            if 'total_profit' in result.columns:
                result['profit_rank'] = result['total_profit'].rank(ascending=False, method='dense').astype(int)
        
        # Sort by revenue (descending)
        if 'total_revenue' in result.columns:
            result = result.sort_values('total_revenue', ascending=False)
        
        # Limit to top N
        if top_n and top_n > 0:
            result = result.head(top_n)
        
        # Round numeric columns
        numeric_cols = result.select_dtypes(include=[np.number]).columns
        numeric_cols = [col for col in numeric_cols if 'rank' not in col]
        result[numeric_cols] = result[numeric_cols].round(round_decimals)
        
        # Reset index
        result = result.reset_index(drop=True)
        
        # Add trend analysis if requested
        if include_trends and date_column and date_column in df_work.columns:
            trends = _calculate_product_trends(
                df_work,
                product_column,
                revenue_column,
                quantity_column,
                date_column,
                period,
                round_decimals
            )
            
            # Return comprehensive report
            report = {
                'performance_summary': result,
                'trends': trends,
                'total_products': len(result),
                'total_revenue': float(result['total_revenue'].sum()) if 'total_revenue' in result.columns else 0,
                'total_quantity': int(result['total_quantity'].sum()) if 'total_quantity' in result.columns else 0,
                'processing_time_seconds': round(
                    (datetime.now() - start_time).total_seconds(), 3
                ),
                'status': 'success'
            }
            
            if 'total_profit' in result.columns:
                report['total_profit'] = float(result['total_profit'].sum())
            
            return report
        
        logger.info(
            f"Product performance calculated: {len(result)} products analyzed"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Product performance calculation failed: {str(e)}", exc_info=True)
        raise KPICalculationError(
            f"Product performance calculation failed: {str(e)}"
        ) from e


def _validate_product_data(
    df: pd.DataFrame,
    product_column: str,
    revenue_column: Optional[str]
) -> None:
    """Validate product data for quality."""
    if df[product_column].isnull().all():
        raise ValueError(f"Product column '{product_column}' contains only null values")
    
    if revenue_column and revenue_column in df.columns:
        if df[revenue_column].isnull().all():
            raise ValueError(f"Revenue column '{revenue_column}' contains only null values")


def _calculate_product_trends(
    df: pd.DataFrame,
    product_column: str,
    revenue_column: Optional[str],
    quantity_column: Optional[str],
    date_column: str,
    period: Optional[Union[str, AggregationPeriod]],
    round_decimals: int
) -> pd.DataFrame:
    """Calculate product performance trends over time."""
    # Convert date column
    df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
    df = df.dropna(subset=[date_column])
    
    if len(df) == 0:
        return pd.DataFrame()
    
    # Determine period
    if period is None:
        period = AggregationPeriod.MONTHLY
    
    period_str = period.value if isinstance(period, AggregationPeriod) else period
    
    # Create period column
    df['period'] = df[date_column].dt.to_period(period_str)
    
    # Aggregate by product and period
    agg_dict = {}
    if revenue_column:
        agg_dict['revenue'] = (revenue_column, 'sum')
    if quantity_column:
        agg_dict['quantity'] = (quantity_column, 'sum')
    
    if not agg_dict:
        return pd.DataFrame()
    
    trends = df.groupby([product_column, 'period']).agg(**agg_dict).reset_index()
    trends['period'] = trends['period'].astype(str)
    
    # Calculate growth rates per product
    if revenue_column and 'revenue' in trends.columns:
        trends['revenue_growth'] = trends.groupby(product_column)['revenue'].pct_change() * 100
    
    if quantity_column and 'quantity' in trends.columns:
        trends['quantity_growth'] = trends.groupby(product_column)['quantity'].pct_change() * 100
    
    # Round values
    numeric_cols = trends.select_dtypes(include=[np.number]).columns
    trends[numeric_cols] = trends[numeric_cols].round(round_decimals)
    
    return trends


def calculate_retention(
    df: pd.DataFrame,
    user_column: str = 'user_id',
    date_column: str = 'date',
    event_column: Optional[str] = None,
    cohort_period: Union[str, AggregationPeriod] = AggregationPeriod.MONTHLY,
    retention_periods: int = 12,
    method: str = 'cohort',
    min_activity_threshold: int = 1,
    include_cohort_size: bool = True,
    return_format: str = 'dataframe',
    round_decimals: int = 2,
    validate_data: bool = True
) -> Union[pd.DataFrame, Dict[str, Any], float]:
    """
    Calculate customer/user retention rate with multiple methods and production-level features.
    
    Retention analysis measures how many users continue to engage with a product or service
    over time. This function supports multiple retention calculation methods:
    
    - **Cohort-based retention**: Track retention by user acquisition cohorts over time
    - **Simple retention**: Overall retention rate between two time periods
    - **Rolling retention**: Users active in each period regardless of gaps
    - **N-day retention**: Retention at specific day intervals (Day 1, Day 7, Day 30)
    
    Args:
        df: Input DataFrame with user activity data
        user_column: Name of column containing user/customer IDs
        date_column: Name of column containing dates
        event_column: Optional column for event types (for filtering)
        cohort_period: Time period for cohort grouping ('D', 'W', 'M', 'Q', 'Y')
        retention_periods: Number of periods to calculate retention for
        method: Retention calculation method:
            - 'cohort': Cohort-based retention analysis (default)
            - 'simple': Simple retention rate calculation
            - 'rolling': Rolling retention (active in period regardless of gaps)
            - 'n_day': N-day retention (specific intervals)
        min_activity_threshold: Minimum events to consider user as active
        include_cohort_size: Include cohort size information in results
        return_format: Output format ('dataframe', 'dict', 'matrix', 'rate')
        round_decimals: Number of decimal places for rounding
        validate_data: Perform data validation before calculation
    
    Returns:
        - DataFrame: Retention cohort matrix or time series (default)
        - Dict: Comprehensive retention report with metrics
        - float: Single retention rate value (for 'rate' format)
        - np.ndarray: Retention matrix as numpy array (for 'matrix' format)
    
    Raises:
        KPICalculationError: If calculation fails or data validation fails
        ValueError: If invalid parameters or missing required columns
    
    Examples:
        >>> # Cohort-based retention analysis
        >>> retention_df = calculate_retention(
        ...     df,
        ...     user_column='customer_id',
        ...     date_column='purchase_date',
        ...     cohort_period='M',
        ...     retention_periods=6
        ... )
        >>> print(retention_df)
        
        >>> # Simple retention rate
        >>> retention_rate = calculate_retention(
        ...     df,
        ...     user_column='user_id',
        ...     date_column='activity_date',
        ...     method='simple',
        ...     return_format='rate'
        ... )
        >>> print(f"Retention Rate: {retention_rate}%")
        
        >>> # Detailed retention report
        >>> report = calculate_retention(
        ...     df,
        ...     user_column='user_id',
        ...     date_column='login_date',
        ...     method='cohort',
        ...     return_format='dict'
        ... )
        >>> print(f"Average Retention: {report['average_retention']}%")
        >>> print(f"Total Cohorts: {report['cohort_count']}")
        
        >>> # N-day retention (Day 1, 7, 14, 30)
        >>> nday_retention = calculate_retention(
        ...     df,
        ...     user_column='user_id',
        ...     date_column='signup_date',
        ...     method='n_day',
        ...     retention_periods=30
        ... )
        >>> print(nday_retention)
    """
    start_time = datetime.now()
    
    # Input validation
    if df is None or not isinstance(df, pd.DataFrame):
        raise ValueError("Input must be a pandas DataFrame")
    
    if df.empty:
        logger.warning("Input DataFrame is empty")
        return _return_empty_retention(return_format)
    
    # Check required columns
    if user_column not in df.columns:
        raise ValueError(
            f"User column '{user_column}' not found. "
            f"Available columns: {list(df.columns)}"
        )
    
    if date_column not in df.columns:
        raise ValueError(
            f"Date column '{date_column}' not found. "
            f"Available columns: {list(df.columns)}"
        )
    
    if event_column and event_column not in df.columns:
        raise ValueError(
            f"Event column '{event_column}' not found. "
            f"Available columns: {list(df.columns)}"
        )
    
    # Convert period string to enum if needed
    if isinstance(cohort_period, str):
        try:
            cohort_period = AggregationPeriod(cohort_period.upper())
        except ValueError:
            # Allow pandas frequency strings
            pass
    
    # Validate method
    valid_methods = ['cohort', 'simple', 'rolling', 'n_day']
    if method not in valid_methods:
        raise ValueError(
            f"Invalid method: {method}. Valid options: {valid_methods}"
        )
    
    try:
        logger.info(
            f"Calculating retention: method={method}, "
            f"users={df[user_column].nunique()}, records={len(df)}"
        )
        
        # Work on a copy
        df_work = df.copy()
        
        # Data validation and cleaning
        if validate_data:
            _validate_retention_data(df_work, user_column, date_column)
        
        # Convert date column to datetime
        df_work[date_column] = pd.to_datetime(df_work[date_column], errors='coerce')
        
        # Remove rows with invalid dates
        initial_count = len(df_work)
        df_work = df_work.dropna(subset=[date_column])
        dropped_count = initial_count - len(df_work)
        
        if dropped_count > 0:
            logger.warning(f"Dropped {dropped_count} rows with invalid dates")
        
        # Remove null user IDs
        df_work = df_work.dropna(subset=[user_column])
        
        if len(df_work) == 0:
            logger.warning("No valid data after cleaning")
            return _return_empty_retention(return_format)
        
        # Filter by event type if specified
        if event_column:
            logger.info(f"Filtering by event column: {event_column}")
        
        # Calculate based on method
        if method == 'cohort':
            result = _calculate_cohort_retention(
                df_work,
                user_column,
                date_column,
                cohort_period,
                retention_periods,
                min_activity_threshold,
                include_cohort_size,
                round_decimals
            )
        
        elif method == 'simple':
            result = _calculate_simple_retention(
                df_work,
                user_column,
                date_column,
                cohort_period,
                round_decimals
            )
        
        elif method == 'rolling':
            result = _calculate_rolling_retention(
                df_work,
                user_column,
                date_column,
                cohort_period,
                retention_periods,
                round_decimals
            )
        
        elif method == 'n_day':
            result = _calculate_nday_retention(
                df_work,
                user_column,
                date_column,
                retention_periods,
                round_decimals
            )
        
        else:
            raise ValueError(f"Method {method} not implemented")
        
        # Format output based on return_format
        if return_format == 'rate' and isinstance(result, pd.DataFrame):
            # Return average retention rate
            numeric_cols = result.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 0:
                result = round(float(result[numeric_cols].mean().mean()), round_decimals)
            else:
                result = 0.0
        
        elif return_format == 'matrix' and isinstance(result, pd.DataFrame):
            # Return as numpy array
            numeric_cols = result.select_dtypes(include=[np.number]).columns
            result = result[numeric_cols].values
        
        elif return_format == 'dict':
            # Return comprehensive report
            if isinstance(result, pd.DataFrame):
                result = _create_retention_report(
                    result,
                    df_work,
                    user_column,
                    method,
                    start_time,
                    round_decimals
                )
            elif isinstance(result, dict):
                # Already a dict, just add timing
                result['processing_time_seconds'] = round(
                    (datetime.now() - start_time).total_seconds(), 3
                )
        
        logger.info(
            f"Retention calculation completed: method={method}, "
            f"result_type={type(result).__name__}"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"Retention calculation failed: {str(e)}", exc_info=True)
        raise KPICalculationError(
            f"Retention calculation failed: {str(e)}"
        ) from e


def _validate_retention_data(
    df: pd.DataFrame,
    user_column: str,
    date_column: str
) -> None:
    """
    Validate retention data for quality and completeness.
    
    Args:
        df: DataFrame to validate
        user_column: User column name
        date_column: Date column name
    
    Raises:
        ValueError: If validation fails
    """
    # Check for empty columns
    if df[user_column].isnull().all():
        raise ValueError(f"User column '{user_column}' contains only null values")
    
    if df[date_column].isnull().all():
        raise ValueError(f"Date column '{date_column}' contains only null values")
    
    # Check for minimum data
    unique_users = df[user_column].nunique()
    if unique_users < 2:
        logger.warning(
            f"Only {unique_users} unique user(s) found. "
            "Retention analysis may not be meaningful."
        )


def _return_empty_retention(return_format: str) -> Union[pd.DataFrame, Dict, float]:
    """Return appropriate empty result based on format."""
    if return_format == 'rate':
        return 0.0
    elif return_format == 'matrix':
        return np.array([])
    elif return_format == 'dict':
        return {
            'retention_rate': 0.0,
            'status': 'no_data',
            'message': 'No valid data for retention calculation'
        }
    else:  # dataframe
        return pd.DataFrame()


def _calculate_cohort_retention(
    df: pd.DataFrame,
    user_column: str,
    date_column: str,
    cohort_period: Union[str, AggregationPeriod],
    retention_periods: int,
    min_activity_threshold: int,
    include_cohort_size: bool,
    round_decimals: int
) -> pd.DataFrame:
    """
    Calculate cohort-based retention.
    
    This tracks users by their acquisition cohort and measures what percentage
    remains active in subsequent periods.
    """
    period_str = cohort_period.value if isinstance(cohort_period, AggregationPeriod) else cohort_period
    
    # Determine user's first activity date (cohort)
    user_cohorts = df.groupby(user_column)[date_column].min().reset_index()
    user_cohorts.columns = [user_column, 'cohort_date']
    
    # Assign cohort period
    user_cohorts['cohort_period'] = user_cohorts['cohort_date'].dt.to_period(period_str)
    
    # Merge cohort info back to main dataframe
    df_cohort = df.merge(user_cohorts[[user_column, 'cohort_period']], on=user_column)
    
    # Assign activity period
    df_cohort['activity_period'] = df_cohort[date_column].dt.to_period(period_str)
    
    # Calculate period offset from cohort
    df_cohort['period_offset'] = (
        df_cohort['activity_period'].astype('int64') - 
        df_cohort['cohort_period'].astype('int64')
    )
    
    # Filter to retention_periods
    df_cohort = df_cohort[df_cohort['period_offset'] <= retention_periods]
    
    # Count active users by cohort and period offset
    cohort_activity = df_cohort.groupby(
        ['cohort_period', 'period_offset']
    )[user_column].nunique().reset_index()
    cohort_activity.columns = ['cohort_period', 'period_offset', 'active_users']
    
    # Pivot to create retention matrix
    retention_matrix = cohort_activity.pivot(
        index='cohort_period',
        columns='period_offset',
        values='active_users'
    )
    
    # Calculate retention percentages
    cohort_sizes = retention_matrix[0]
    retention_pct = retention_matrix.div(cohort_sizes, axis=0) * 100
    
    # Round values
    retention_pct = retention_pct.round(round_decimals)
    
    # Rename columns
    retention_pct.columns = [f'Period_{int(col)}' for col in retention_pct.columns]
    
    # Reset index to make cohort_period a column
    retention_pct = retention_pct.reset_index()
    retention_pct['cohort_period'] = retention_pct['cohort_period'].astype(str)
    
    # Add cohort sizes if requested
    if include_cohort_size:
        retention_pct.insert(1, 'cohort_size', cohort_sizes.values)
    
    return retention_pct


def _calculate_simple_retention(
    df: pd.DataFrame,
    user_column: str,
    date_column: str,
    period: Union[str, AggregationPeriod],
    round_decimals: int
) -> pd.DataFrame:
    """
    Calculate simple period-over-period retention rate.
    
    Measures what percentage of users active in one period remain active in the next.
    """
    period_str = period.value if isinstance(period, AggregationPeriod) else period
    
    # Assign period to each activity
    df['period'] = df[date_column].dt.to_period(period_str)
    
    # Get unique users per period
    period_users = df.groupby('period')[user_column].apply(set).reset_index()
    period_users.columns = ['period', 'users']
    
    # Calculate retention
    retention_data = []
    
    for i in range(len(period_users) - 1):
        current_period = period_users.iloc[i]
        next_period = period_users.iloc[i + 1]
        
        current_users = current_period['users']
        next_users = next_period['users']
        
        retained_users = current_users & next_users
        retention_rate = (len(retained_users) / len(current_users) * 100) if len(current_users) > 0 else 0
        
        retention_data.append({
            'period': str(current_period['period']),
            'users': len(current_users),
            'retained_users': len(retained_users),
            'retention_rate': round(retention_rate, round_decimals)
        })
    
    return pd.DataFrame(retention_data)


def _calculate_rolling_retention(
    df: pd.DataFrame,
    user_column: str,
    date_column: str,
    period: Union[str, AggregationPeriod],
    retention_periods: int,
    round_decimals: int
) -> pd.DataFrame:
    """
    Calculate rolling retention (users active in each period regardless of gaps).
    """
    period_str = period.value if isinstance(period, AggregationPeriod) else period
    
    # Assign period
    df['period'] = df[date_column].dt.to_period(period_str)
    
    # Get active users per period
    active_by_period = df.groupby('period')[user_column].apply(set).to_dict()
    
    # Sort periods
    periods = sorted(active_by_period.keys())
    
    if len(periods) == 0:
        return pd.DataFrame()
    
    # Calculate rolling retention from each starting period
    rolling_data = []
    
    for i, start_period in enumerate(periods):
        if i + retention_periods > len(periods):
            break
        
        start_users = active_by_period[start_period]
        
        for offset in range(min(retention_periods + 1, len(periods) - i)):
            target_period = periods[i + offset]
            target_users = active_by_period[target_period]
            
            retained = start_users & target_users
            retention_rate = (len(retained) / len(start_users) * 100) if len(start_users) > 0 else 0
            
            rolling_data.append({
                'start_period': str(start_period),
                'offset': offset,
                'target_period': str(target_period),
                'retention_rate': round(retention_rate, round_decimals)
            })
    
    return pd.DataFrame(rolling_data)


def _calculate_nday_retention(
    df: pd.DataFrame,
    user_column: str,
    date_column: str,
    max_days: int,
    round_decimals: int
) -> pd.DataFrame:
    """
    Calculate N-day retention (e.g., Day 1, Day 7, Day 30).
    """
    # Get user's first activity date
    user_first_date = df.groupby(user_column)[date_column].min().reset_index()
    user_first_date.columns = [user_column, 'first_date']
    
    # Merge back
    df_nday = df.merge(user_first_date, on=user_column)
    
    # Calculate days since first activity
    df_nday['days_since_first'] = (df_nday[date_column] - df_nday['first_date']).dt.days
    
    # Group users by their cohort date
    cohorts = user_first_date.groupby('first_date')[user_column].apply(set).to_dict()
    
    # Calculate retention for each day
    day_intervals = [1, 3, 7, 14, 30, 60, 90]
    day_intervals = [d for d in day_intervals if d <= max_days]
    
    if max_days not in day_intervals and max_days <= 365:
        day_intervals.append(max_days)
    
    retention_data = []
    
    for cohort_date, cohort_users in cohorts.items():
        cohort_size = len(cohort_users)
        
        for day in day_intervals:
            # Find users active on or after that day
            target_date = cohort_date + timedelta(days=day)
            
            active_users = df_nday[
                (df_nday['first_date'] == cohort_date) &
                (df_nday[date_column] >= target_date) &
                (df_nday[date_column] < target_date + timedelta(days=1))
            ][user_column].nunique()
            
            retention_rate = (active_users / cohort_size * 100) if cohort_size > 0 else 0
            
            retention_data.append({
                'cohort_date': cohort_date,
                'day': day,
                'cohort_size': cohort_size,
                'retained_users': active_users,
                'retention_rate': round(retention_rate, round_decimals)
            })
    
    result = pd.DataFrame(retention_data)
    
    # Pivot for easier reading
    if len(result) > 0:
        pivot = result.pivot(
            index='cohort_date',
            columns='day',
            values='retention_rate'
        ).reset_index()
        
        pivot.columns = ['cohort_date'] + [f'Day_{int(col)}' for col in pivot.columns[1:]]
        return pivot
    
    return result


def _create_retention_report(
    retention_df: pd.DataFrame,
    df: pd.DataFrame,
    user_column: str,
    method: str,
    start_time: datetime,
    round_decimals: int
) -> Dict[str, Any]:
    """Create comprehensive retention report."""
    numeric_cols = retention_df.select_dtypes(include=[np.number]).columns
    
    report = {
        'method': method,
        'retention_data': retention_df,
        'total_users': int(df[user_column].nunique()),
        'total_records': len(df),
        'date_range': {
            'start': df[df.columns[df.columns.get_loc(user_column) + 1]].min().strftime('%Y-%m-%d'),
            'end': df[df.columns[df.columns.get_loc(user_column) + 1]].max().strftime('%Y-%m-%d')
        },
        'processing_time_seconds': round(
            (datetime.now() - start_time).total_seconds(), 3
        ),
        'status': 'success'
    }
    
    # Calculate summary statistics if numeric data exists
    if len(numeric_cols) > 0:
        retention_values = retention_df[numeric_cols].values.flatten()
        retention_values = retention_values[~np.isnan(retention_values)]
        
        if len(retention_values) > 0:
            report['average_retention'] = round(float(np.mean(retention_values)), round_decimals)
            report['median_retention'] = round(float(np.median(retention_values)), round_decimals)
            report['min_retention'] = round(float(np.min(retention_values)), round_decimals)
            report['max_retention'] = round(float(np.max(retention_values)), round_decimals)
            report['std_retention'] = round(float(np.std(retention_values)), round_decimals)
    
    if 'cohort_period' in retention_df.columns or 'cohort_date' in retention_df.columns:
        report['cohort_count'] = len(retention_df)
    
    return report


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
    
    # Example 5: Calculate Customer Retention
    print("\nExample 5: Calculate Customer Retention")
    print("-" * 70)
    
    # Create sample user activity data for retention analysis
    user_data = {
        'user_id': [1, 1, 1, 2, 2, 3, 3, 3, 4, 4, 5, 6, 6, 7, 8, 8, 9, 10, 10, 10],
        'activity_date': [
            '2026-01-15', '2026-02-10', '2026-03-05',
            '2026-01-20', '2026-02-15',
            '2026-01-25', '2026-02-20', '2026-03-15',
            '2026-02-05', '2026-03-10',
            '2026-02-12',
            '2026-02-18', '2026-03-12',
            '2026-03-08',
            '2026-03-15', '2026-04-01',
            '2026-03-20',
            '2026-03-25', '2026-04-05', '2026-05-01'
        ],
        'event': ['login'] * 20
    }
    df_users = pd.DataFrame(user_data)
    df_users['activity_date'] = pd.to_datetime(df_users['activity_date'])
    
    print("\nSample User Activity Data:")
    print(f"Total Users: {df_users['user_id'].nunique()}")
    print(f"Date Range: {df_users['activity_date'].min().date()} to {df_users['activity_date'].max().date()}")
    print(f"Total Activities: {len(df_users)}")
    
    # Calculate cohort-based retention
    print("\n• Cohort-Based Retention Analysis:")
    retention_cohort = calculate_retention(
        df_users,
        user_column='user_id',
        date_column='activity_date',
        cohort_period='M',
        retention_periods=3,
        method='cohort'
    )
    print(retention_cohort)
    
    # Calculate simple retention rate
    print("\n• Simple Period-over-Period Retention:")
    retention_simple = calculate_retention(
        df_users,
        user_column='user_id',
        date_column='activity_date',
        cohort_period='M',
        method='simple'
    )
    print(retention_simple)
    
    # Get overall retention rate
    print("\n• Overall Retention Rate:")
    retention_rate = calculate_retention(
        df_users,
        user_column='user_id',
        date_column='activity_date',
        method='cohort',
        return_format='rate'
    )
    print(f"Average Retention Rate: {retention_rate}%")
    
    # Get detailed retention report
    print("\n• Detailed Retention Report:")
    retention_report = calculate_retention(
        df_users,
        user_column='user_id',
        date_column='activity_date',
        method='cohort',
        return_format='dict'
    )
    print(f"Total Users: {retention_report['total_users']}")
    print(f"Cohorts Analyzed: {retention_report['cohort_count']}")
    if 'average_retention' in retention_report:
        print(f"Average Retention: {retention_report['average_retention']}%")
        print(f"Min Retention: {retention_report['min_retention']}%")
        print(f"Max Retention: {retention_report['max_retention']}%")
    
    print("\n" + "="*70)
    print("\n✓ All KPI calculations completed successfully!")
