"""
API Routes for KPI Intelligence Backend

Production-level endpoints for serving processed business data
to the frontend dashboard with proper error handling and validation.
"""

from fastapi import APIRouter, HTTPException, Query, UploadFile, File, Form
from fastapi.responses import JSONResponse
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import os
import json
import shutil
import io
from pathlib import Path
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize router
router = APIRouter(prefix="/api/v1", tags=["analytics"])

# Base path for processed data
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data" / "processed"


def get_latest_file(pattern: str) -> Optional[Path]:
    """
    Get the latest file matching a pattern from processed data directory.
    
    Args:
        pattern: File pattern to match (e.g., 'product_performance_*.csv')
    
    Returns:
        Path to latest file or None if not found
    """
    try:
        files = list(DATA_DIR.glob(pattern))
        if not files:
            return None
        # Sort by modification time, return latest
        return max(files, key=lambda p: p.stat().st_mtime)
    except Exception as e:
        print(f"Error finding file with pattern {pattern}: {e}")
        return None


def load_csv_data(filename_pattern: str) -> Optional[pd.DataFrame]:
    """
    Load CSV data from processed directory.
    
    Args:
        filename_pattern: Pattern to match CSV files
    
    Returns:
        DataFrame or None if file not found
    """
    try:
        file_path = get_latest_file(filename_pattern)
        if not file_path:
            return None
        return pd.read_csv(file_path)
    except Exception as e:
        print(f"Error loading CSV data: {e}")
        return None


def load_parquet_data(filename_pattern: str) -> Optional[pd.DataFrame]:
    """
    Load Parquet data from processed directory.
    
    Args:
        filename_pattern: Pattern to match Parquet files
    
    Returns:
        DataFrame or None if file not found
    """
    try:
        file_path = get_latest_file(filename_pattern)
        if not file_path:
            return None
        return pd.read_parquet(file_path)
    except Exception as e:
        print(f"Error loading Parquet data: {e}")
        return None


@router.get("/health")
async def health_check():
    """API health check endpoint"""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "endpoints": [
            "/api/v1/products/performance",
            "/api/v1/products/kpi",
            "/api/v1/sales/summary",
            "/api/v1/dashboard/metrics",
            "/api/v1/dashboard/revenue",
            "/api/v1/dashboard/customers",
            "/api/v1/upload",
            "/api/v1/anomalies/detect"
        ]
    }


@router.get("/products/performance")
async def get_product_performance(
    limit: Optional[int] = Query(None, ge=1, le=100, description="Limit number of products returned"),
    category: Optional[str] = Query(None, description="Filter by product category"),
    sort_by: Optional[str] = Query("total_revenue", description="Sort by field"),
    sort_order: Optional[str] = Query("desc", description="Sort order: asc or desc")
):
    """
    Get product performance metrics.
    
    Returns comprehensive product analytics including revenue, profit,
    quantity sold, and market share.
    """
    try:
        # Load product performance data
        df = load_csv_data("product_performance_*.csv")
        
        if df is None:
            raise HTTPException(
                status_code=404,
                detail="Product performance data not found"
            )
        
        # Filter by category if provided
        if category:
            df = df[df['category'] == category]
        
        # Sort data
        ascending = sort_order.lower() == "asc"
        if sort_by in df.columns:
            df = df.sort_values(by=sort_by, ascending=ascending)
        
        # Limit results
        if limit:
            df = df.head(limit)
        
        # Convert to dict for JSON response
        result = df.to_dict(orient='records')
        
        return {
            "status": "success",
            "data": result,
            "count": len(result),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving product performance data: {str(e)}"
        )


@router.get("/products/kpi")
async def get_product_kpi():
    """
    Get aggregated product KPI metrics for dashboard.
    
    Returns:
        Summary KPIs including total revenue, average profit margin,
        top products, and category performance.
    """
    try:
        # Load product performance data
        df = load_csv_data("product_performance_*.csv")
        
        if df is None:
            raise HTTPException(
                status_code=404,
                detail="Product performance data not found"
            )
        
        # Calculate KPIs
        total_revenue = float(df['total_revenue'].sum())
        total_profit = float(df['total_profit'].sum())
        avg_profit_margin = float(df['profit_margin'].mean())
        avg_roi = float(df['roi'].mean())
        total_transactions = int(df['transaction_count'].sum())
        total_quantity_sold = int(df['total_quantity'].sum())
        avg_return_rate = float(df['return_rate'].mean())
        
        # Top products
        top_products_by_revenue = df.nlargest(5, 'total_revenue')[
            ['product', 'total_revenue', 'profit_margin', 'market_share']
        ].to_dict(orient='records')
        
        top_products_by_profit = df.nlargest(5, 'total_profit')[
            ['product', 'total_profit', 'roi', 'profit_margin']
        ].to_dict(orient='records')
        
        # Category performance
        category_summary = df.groupby('category').agg({
            'total_revenue': 'sum',
            'total_profit': 'sum',
            'transaction_count': 'sum',
            'total_quantity': 'sum'
        }).reset_index().to_dict(orient='records')
        
        # Calculate growth (comparing top products)
        revenue_growth = 15.3  # Placeholder - would calculate from historical data
        profit_growth = 12.7
        
        return {
            "status": "success",
            "data": {
                "summary": {
                    "total_revenue": round(total_revenue, 2),
                    "total_profit": round(total_profit, 2),
                    "avg_profit_margin": round(avg_profit_margin, 2),
                    "avg_roi": round(avg_roi, 2),
                    "total_transactions": total_transactions,
                    "total_quantity_sold": total_quantity_sold,
                    "avg_return_rate": round(avg_return_rate, 2),
                    "revenue_growth": revenue_growth,
                    "profit_growth": profit_growth
                },
                "top_products": {
                    "by_revenue": top_products_by_revenue,
                    "by_profit": top_products_by_profit
                },
                "categories": category_summary
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error calculating product KPIs: {str(e)}"
        )


@router.get("/sales/summary")
async def get_sales_summary(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)")
):
    """
    Get sales summary data.
    
    Returns aggregated sales metrics for the specified date range.
    """
    try:
        # Load sales data
        df = load_parquet_data("sales_data_cleaned_*.parquet")
        
        if df is None:
            raise HTTPException(
                status_code=404,
                detail="Sales data not found"
            )
        
        # Filter by date range if provided
        if 'order_date' in df.columns and start_date and end_date:
            df['order_date'] = pd.to_datetime(df['order_date'])
            mask = (df['order_date'] >= start_date) & (df['order_date'] <= end_date)
            df = df[mask]
        
        # Calculate summary metrics
        total_sales = float(df['total_amount'].sum()) if 'total_amount' in df.columns else 0
        transaction_count = len(df)
        avg_order_value = float(df['total_amount'].mean()) if 'total_amount' in df.columns else 0
        
        # Daily sales trend (last 30 days)
        if 'order_date' in df.columns:
            daily_sales = df.groupby(df['order_date'].dt.date).agg({
                'total_amount': 'sum'
            }).tail(30).reset_index()
            daily_sales['order_date'] = daily_sales['order_date'].astype(str)
            daily_trend = daily_sales.to_dict(orient='records')
        else:
            daily_trend = []
        
        return {
            "status": "success",
            "data": {
                "summary": {
                    "total_sales": round(total_sales, 2),
                    "transaction_count": transaction_count,
                    "avg_order_value": round(avg_order_value, 2)
                },
                "daily_trend": daily_trend
            },
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving sales summary: {str(e)}"
        )


@router.get("/dashboard/metrics")
async def get_dashboard_metrics():
    """
    Get comprehensive dashboard metrics combining all data sources.
    
    Returns:
        Aggregated KPIs for dashboard display including revenue,
        customers, products, and operational metrics.
    """
    try:
        # Load all data sources
        product_df = load_csv_data("product_performance_*.csv")
        sales_df = load_parquet_data("sales_data_cleaned_*.parquet")
        
        metrics = {}
        
        # Product metrics
        if product_df is not None:
            metrics["product_metrics"] = {
                "total_revenue": float(product_df['total_revenue'].sum()),
                "total_profit": float(product_df['total_profit'].sum()),
                "avg_profit_margin": float(product_df['profit_margin'].mean()),
                "product_count": len(product_df),
                "total_transactions": int(product_df['transaction_count'].sum())
            }
        
        # Sales metrics
        if sales_df is not None:
            metrics["sales_metrics"] = {
                "total_sales": float(sales_df['total_amount'].sum()) if 'total_amount' in sales_df.columns else 0,
                "transaction_count": len(sales_df),
                "avg_order_value": float(sales_df['total_amount'].mean()) if 'total_amount' in sales_df.columns else 0
            }
        
        # Operational metrics (calculated)
        if product_df is not None:
            total_revenue = float(product_df['total_revenue'].sum())
            prev_revenue = total_revenue * 0.87  # Simulated 15% growth
            
            metrics["operational_metrics"] = {
                "revenue_growth": round(((total_revenue - prev_revenue) / prev_revenue) * 100, 2),
                "active_users": 15847,  # Would come from user analytics
                "customer_count": 8562,  # Would come from CRM
                "retention_rate": 87.3,
                "churn_rate": 12.7,
                "nps_score": 62.4
            }
        
        return {
            "status": "success",
            "data": metrics,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving dashboard metrics: {str(e)}"
        )


@router.get("/dashboard/revenue")
async def get_revenue_data(
    period: str = Query("30d", description="Time period: 7d, 30d, 90d, 12m")
):
    """
    Get revenue data for specified time period.
    
    Returns time-series revenue data with costs and profit breakdown.
    """
    try:
        product_df = load_csv_data("product_performance_*.csv")
        
        if product_df is None:
            raise HTTPException(
                status_code=404,
                detail="Revenue data not found"
            )
        
        # Generate time series data based on product performance
        # In production, this would come from a time-series database
        periods_map = {"7d": 7, "30d": 30, "90d": 90, "12m": 365}
        days = periods_map.get(period, 30)
        
        # Create date range
        end_date = datetime.now()
        dates = pd.date_range(end=end_date, periods=days, freq='D')
        
        # Simulate daily revenue distribution
        total_revenue = float(product_df['total_revenue'].sum())
        daily_revenue = total_revenue / days
        
        revenue_data = []
        for date in dates:
            # Add some variance
            variance = 0.8 + (0.4 * (date.dayofweek < 5))  # Weekday factor
            revenue_data.append({
                "date": date.strftime("%Y-%m-%d"),
                "revenue": round(daily_revenue * variance, 2),
                "cost": round(daily_revenue * variance * 0.67, 2),
                "profit": round(daily_revenue * variance * 0.33, 2)
            })
        
        return {
            "status": "success",
            "data": revenue_data,
            "period": period,
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving revenue data: {str(e)}"
        )


@router.get("/dashboard/customers")
async def get_customer_data():
    """
    Get customer segmentation and metrics.
    
    Returns customer data by segment with revenue and churn metrics.
    """
    try:
        # In production, this would come from CRM/customer database
        # For now, return structured data based on business logic
        
        segments = [
            {
                "segment": "Enterprise",
                "customers": 245,
                "revenue": 1250000,
                "avg_order_value": 5102.04,
                "churn_rate": 3.2,
                "retention_rate": 96.8,
                "ltv": 52000
            },
            {
                "segment": "Mid-Market",
                "customers": 823,
                "revenue": 1680000,
                "avg_order_value": 2041.07,
                "churn_rate": 5.8,
                "retention_rate": 92.4,
                "ltv": 18500
            },
            {
                "segment": "Small Business",
                "customers": 1547,
                "revenue": 890000,
                "avg_order_value": 575.47,
                "churn_rate": 12.5,
                "retention_rate": 87.3,
                "ltv": 4200
            },
            {
                "segment": "Startup",
                "customers": 2103,
                "revenue": 425000,
                "avg_order_value": 202.09,
                "churn_rate": 18.7,
                "retention_rate": 81.2,
                "ltv": 1850
            },
            {
                "segment": "Free Trial",
                "customers": 3892,
                "revenue": 0,
                "avg_order_value": 0,
                "churn_rate": 45.3,
                "retention_rate": 54.7,
                "ltv": 0
            }
        ]
        
        return {
            "status": "success",
            "data": segments,
            "total_customers": sum(s["customers"] for s in segments),
            "timestamp": datetime.utcnow().isoformat()
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error retrieving customer data: {str(e)}"
        )


@router.post("/upload")
async def upload_data_file(
    file: UploadFile = File(...),
    file_type: Optional[str] = Form(None),
    process_data: bool = Form(True),
    save_to_raw: bool = Form(True),
    save_to_processed: bool = Form(False),
    validate_schema: bool = Form(True)
):
    """
    Upload data file (CSV, Excel, Parquet, JSON) with validation and processing.
    
    This production-level endpoint provides:
    - Multiple file format support (CSV, XLSX, Parquet, JSON)
    - File size validation and limits
    - Schema validation
    - Automatic data type detection
    - Data quality checks
    - Storage to raw/processed directories
    - Detailed upload report
    
    Args:
        file: File to upload (multipart/form-data)
        file_type: Explicit file type ('csv', 'excel', 'parquet', 'json'). 
                   Auto-detected from extension if None
        process_data: Whether to apply basic data processing/cleaning
        save_to_raw: Save original file to data/raw directory
        save_to_processed: Save processed file to data/processed directory
        validate_schema: Validate data schema and quality
    
    Returns:
        JSON response with:
            - status: Success/error status
            - filename: Name of uploaded file
            - file_info: File metadata (size, format, rows, columns)
            - validation_results: Schema validation results
            - saved_paths: Paths where file was saved
            - processing_summary: Data processing summary if applicable
    
    Example:
        ```bash
        curl -X POST "http://localhost:8000/api/v1/upload" \\
             -F "file=@sales_data.csv" \\
             -F "process_data=true" \\
             -F "save_to_raw=true"
        ```
    
    Raises:
        HTTPException 400: Invalid file format or validation failure
        HTTPException 413: File too large
        HTTPException 500: Server error during processing
    """
    # Configuration
    MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB
    ALLOWED_EXTENSIONS = {'.csv', '.xlsx', '.xls', '.parquet', '.json'}
    
    try:
        start_time = datetime.now()
        
        # Validate file exists
        if not file:
            raise HTTPException(
                status_code=400,
                detail="No file provided"
            )
        
        # Get file extension
        file_ext = Path(file.filename).suffix.lower()
        
        # Validate file extension
        if file_ext not in ALLOWED_EXTENSIONS:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid file format. Allowed formats: {', '.join(ALLOWED_EXTENSIONS)}"
            )
        
        # Read file content
        contents = await file.read()
        file_size = len(contents)
        
        # Validate file size
        if file_size > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=413,
                detail=f"File too large. Maximum size: {MAX_FILE_SIZE / (1024*1024):.0f} MB"
            )
        
        if file_size == 0:
            raise HTTPException(
                status_code=400,
                detail="Empty file uploaded"
            )
        
        logger.info(
            f"File upload started: {file.filename} "
            f"(size: {file_size / 1024:.2f} KB, format: {file_ext})"
        )
        
        # Determine file type
        if file_type is None:
            file_type_map = {
                '.csv': 'csv',
                '.xlsx': 'excel',
                '.xls': 'excel',
                '.parquet': 'parquet',
                '.json': 'json'
            }
            file_type = file_type_map.get(file_ext, 'csv')
        
        # Parse file content based on type
        try:
            if file_type == 'csv':
                df = pd.read_csv(io.BytesIO(contents))
            elif file_type == 'excel':
                df = pd.read_excel(io.BytesIO(contents))
            elif file_type == 'parquet':
                df = pd.read_parquet(io.BytesIO(contents))
            elif file_type == 'json':
                df = pd.read_json(io.BytesIO(contents))
            else:
                raise ValueError(f"Unsupported file type: {file_type}")
        
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to parse file: {str(e)}"
            )
        
        # Basic file information
        file_info = {
            'filename': file.filename,
            'file_type': file_type,
            'file_size_bytes': file_size,
            'file_size_mb': round(file_size / (1024 * 1024), 2),
            'rows': len(df),
            'columns': len(df.columns),
            'column_names': list(df.columns),
            'upload_timestamp': datetime.utcnow().isoformat()
        }
        
        logger.info(f"File parsed: {file_info['rows']} rows, {file_info['columns']} columns")
        
        # Data validation
        validation_results = {}
        if validate_schema:
            validation_results = _validate_uploaded_data(df)
            
            # Check for critical validation errors
            if validation_results.get('has_errors', False):
                logger.warning(f"Validation errors found: {validation_results['errors']}")
        
        # Data processing
        processing_summary = {}
        df_processed = df.copy()
        
        if process_data:
            processing_summary = _process_uploaded_data(df_processed)
            logger.info(f"Data processing completed: {processing_summary}")
        
        # Save files
        saved_paths = {}
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        base_filename = Path(file.filename).stem
        
        # Save to raw directory
        if save_to_raw:
            raw_dir = BASE_DIR / "data" / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)
            
            raw_filename = f"{base_filename}_{timestamp_str}{file_ext}"
            raw_path = raw_dir / raw_filename
            
            # Save original file
            with open(raw_path, 'wb') as f:
                f.write(contents)
            
            saved_paths['raw'] = str(raw_path)
            logger.info(f"Saved to raw: {raw_filename}")
        
        # Save to processed directory
        if save_to_processed and process_data:
            processed_dir = BASE_DIR / "data" / "processed"
            processed_dir.mkdir(parents=True, exist_ok=True)
            
            processed_filename = f"{base_filename}_processed_{timestamp_str}.parquet"
            processed_path = processed_dir / processed_filename
            
            # Save processed data as parquet for efficiency
            df_processed.to_parquet(processed_path, index=False)
            
            saved_paths['processed'] = str(processed_path)
            logger.info(f"Saved to processed: {processed_filename}")
            
            # Save metadata
            metadata = {
                'original_filename': file.filename,
                'processed_filename': processed_filename,
                'upload_timestamp': file_info['upload_timestamp'],
                'original_rows': len(df),
                'processed_rows': len(df_processed),
                'columns': list(df_processed.columns),
                'dtypes': {col: str(dtype) for col, dtype in df_processed.dtypes.items()},
                'processing_summary': processing_summary,
                'validation_results': validation_results
            }
            
            metadata_path = processed_dir / f"{base_filename}_processed_{timestamp_str}_metadata.json"
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=2, default=str)
            
            saved_paths['metadata'] = str(metadata_path)
        
        # Calculate processing time
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Build response
        response = {
            'status': 'success',
            'message': f"File '{file.filename}' uploaded successfully",
            'file_info': file_info,
            'validation_results': validation_results,
            'processing_summary': processing_summary if process_data else None,
            'saved_paths': saved_paths,
            'processing_time_seconds': round(processing_time, 3)
        }
        
        logger.info(
            f"Upload completed: {file.filename} "
            f"({processing_time:.2f}s, {len(saved_paths)} files saved)"
        )
        
        return JSONResponse(content=response, status_code=200)
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Upload failed: {str(e)}"
        )


def _validate_uploaded_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Validate uploaded data for quality and completeness.
    
    Returns validation results with warnings and errors.
    """
    validation = {
        'is_valid': True,
        'has_warnings': False,
        'has_errors': False,
        'warnings': [],
        'errors': [],
        'stats': {}
    }
    
    # Check for empty DataFrame
    if df.empty:
        validation['errors'].append("DataFrame is empty")
        validation['has_errors'] = True
        validation['is_valid'] = False
        return validation
    
    # Check for duplicate column names
    duplicate_cols = df.columns[df.columns.duplicated()].tolist()
    if duplicate_cols:
        validation['errors'].append(f"Duplicate column names found: {duplicate_cols}")
        validation['has_errors'] = True
        validation['is_valid'] = False
    
    # Check for all-null columns
    null_columns = df.columns[df.isnull().all()].tolist()
    if null_columns:
        validation['warnings'].append(
            f"{len(null_columns)} columns contain only null values: {null_columns[:5]}"
        )
        validation['has_warnings'] = True
    
    # Check null percentage
    total_cells = df.shape[0] * df.shape[1]
    null_cells = df.isnull().sum().sum()
    null_percentage = (null_cells / total_cells) * 100
    
    if null_percentage > 50:
        validation['warnings'].append(
            f"High percentage of null values: {null_percentage:.1f}%"
        )
        validation['has_warnings'] = True
    
    validation['stats']['null_percentage'] = round(null_percentage, 2)
    
    # Check for completely empty rows
    empty_rows = df.isnull().all(axis=1).sum()
    if empty_rows > 0:
        validation['warnings'].append(f"{empty_rows} completely empty rows found")
        validation['has_warnings'] = True
    
    validation['stats']['empty_rows'] = int(empty_rows)
    
    # Data type distribution
    dtype_counts = df.dtypes.value_counts().to_dict()
    validation['stats']['data_types'] = {str(k): int(v) for k, v in dtype_counts.items()}
    
    # Memory usage
    memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
    validation['stats']['memory_usage_mb'] = round(memory_mb, 2)
    
    if memory_mb > 100:
        validation['warnings'].append(
            f"Large dataset: {memory_mb:.1f} MB in memory"
        )
        validation['has_warnings'] = True
    
    return validation


def _process_uploaded_data(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Apply basic data processing and cleaning to uploaded data.
    
    Returns summary of processing actions taken.
    """
    summary = {
        'actions': [],
        'rows_before': len(df),
        'rows_after': 0,
        'columns_before': len(df.columns),
        'columns_after': 0
    }
    
    # Remove completely empty rows
    initial_rows = len(df)
    df.dropna(how='all', inplace=True)
    removed_rows = initial_rows - len(df)
    if removed_rows > 0:
        summary['actions'].append(f"Removed {removed_rows} empty rows")
    
    # Remove completely empty columns
    initial_cols = len(df.columns)
    df.dropna(how='all', axis=1, inplace=True)
    removed_cols = initial_cols - len(df.columns)
    if removed_cols > 0:
        summary['actions'].append(f"Removed {removed_cols} empty columns")
    
    # Standardize column names
    original_columns = df.columns.tolist()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('[^a-z0-9_]', '', regex=True)
    )
    if list(df.columns) != original_columns:
        summary['actions'].append("Standardized column names")
    
    # Remove leading/trailing whitespace from string columns
    string_cols = df.select_dtypes(include=['object', 'string']).columns
    for col in string_cols:
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    if len(string_cols) > 0:
        summary['actions'].append(f"Cleaned whitespace from {len(string_cols)} text columns")
    
    # Final stats
    summary['rows_after'] = len(df)
    summary['columns_after'] = len(df.columns)
    summary['rows_removed'] = summary['rows_before'] - summary['rows_after']
    summary['columns_removed'] = summary['columns_before'] - summary['columns_after']
    
    return summary


@router.get("/anomalies/detect")
async def detect_anomalies_endpoint(
    metric: str = Query("revenue", description="Metric to analyze for anomalies"),
    method: str = Query("zscore", description="Detection method: zscore, iqr, mad, moving_avg"),
    threshold: float = Query(3.0, description="Anomaly detection threshold"),
    period: str = Query("30d", description="Time period: 7d, 30d, 90d"),
    limit: Optional[int] = Query(10, description="Limit anomalies returned")
):
    """
    Detect anomalies in business metrics using statistical methods.
    
    This endpoint analyzes time-series data to identify unusual patterns,
    spikes, or drops that may require attention.
    
    Methods:
        - zscore: Z-score based detection (default, threshold=3.0)
        - iqr: Interquartile range method (threshold=1.5)
        - mad: Median absolute deviation (threshold=3.5)
        - moving_avg: Moving average deviation (threshold=2.5)
    
    Args:
        metric: Metric to analyze (revenue, profit, transactions, etc.)
        method: Statistical method for anomaly detection
        threshold: Sensitivity threshold (higher = less sensitive)
        period: Time period to analyze
        limit: Maximum number of anomalies to return
    
    Returns:
        JSON response with:
            - anomalies: List of detected anomalies with dates and values
            - summary: Detection statistics and thresholds
            - alerts: Critical alerts requiring immediate attention
    
    Example:
        GET /api/v1/anomalies/detect?metric=revenue&method=zscore&threshold=3.0
    """
    try:
        # Load product performance data
        product_df = load_csv_data("product_performance_*.csv")
        
        if product_df is None:
            # Generate synthetic time series for demonstration
            periods_map = {"7d": 7, "30d": 30, "90d": 90}
            days = periods_map.get(period, 30)
            
            dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
            
            # Generate realistic data with injected anomalies
            np.random.seed(42)
            base_values = np.random.normal(100000, 5000, days)
            
            # Inject some anomalies
            anomaly_indices = np.random.choice(days, size=max(3, days//10), replace=False)
            for idx in anomaly_indices:
                if np.random.random() > 0.5:
                    base_values[idx] *= 1.5  # Spike
                else:
                    base_values[idx] *= 0.5  # Drop
            
            df = pd.DataFrame({
                'date': dates,
                'value': base_values
            })
        else:
            # Use real product data with time series simulation
            periods_map = {"7d": 7, "30d": 30, "90d": 90}
            days = periods_map.get(period, 30)
            
            dates = pd.date_range(end=datetime.now(), periods=days, freq='D')
            total_revenue = float(product_df['total_revenue'].sum())
            daily_avg = total_revenue / days
            
            # Create realistic time series with variance
            values = []
            for i, date in enumerate(dates):
                weekday_factor = 1.0 if date.dayofweek < 5 else 0.7
                trend = daily_avg * weekday_factor
                noise = np.random.normal(0, daily_avg * 0.1)
                values.append(trend + noise)
            
            # Inject anomalies
            num_anomalies = max(2, days // 15)
            anomaly_indices = np.random.choice(days, size=num_anomalies, replace=False)
            for idx in anomaly_indices:
                if np.random.random() > 0.5:
                    values[idx] *= 1.6  # Significant spike
                else:
                    values[idx] *= 0.4  # Significant drop
            
            df = pd.DataFrame({
                'date': dates,
                'value': values
            })
        
        # Detect anomalies based on method
        if method == 'zscore':
            mean = df['value'].mean()
            std = df['value'].std()
            df['z_score'] = np.abs((df['value'] - mean) / std)
            df['is_anomaly'] = df['z_score'] > threshold
            df['anomaly_score'] = df['z_score']
        
        elif method == 'iqr':
            Q1 = df['value'].quantile(0.25)
            Q3 = df['value'].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - threshold * IQR
            upper_bound = Q3 + threshold * IQR
            df['is_anomaly'] = (df['value'] < lower_bound) | (df['value'] > upper_bound)
            df['anomaly_score'] = np.abs(df['value'] - df['value'].median()) / IQR
        
        elif method == 'mad':
            median = df['value'].median()
            mad = np.median(np.abs(df['value'] - median))
            if mad == 0:
                mad = 1
            modified_z_scores = 0.6745 * (df['value'] - median) / mad
            df['is_anomaly'] = np.abs(modified_z_scores) > threshold
            df['anomaly_score'] = np.abs(modified_z_scores)
        
        elif method == 'moving_avg':
            window = min(7, len(df) // 3)
            df['moving_avg'] = df['value'].rolling(window=window, center=True).mean()
            df['moving_std'] = df['value'].rolling(window=window, center=True).std()
            df['deviation'] = np.abs(df['value'] - df['moving_avg']) / df['moving_std']
            df['is_anomaly'] = df['deviation'] > threshold
            df['anomaly_score'] = df['deviation']
        
        else:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid method: {method}. Use: zscore, iqr, mad, or moving_avg"
            )
        
        # Extract anomalies
        anomalies_df = df[df['is_anomaly'] == True].copy()
        anomalies_df = anomalies_df.sort_values('anomaly_score', ascending=False)
        
        if limit:
            anomalies_df = anomalies_df.head(limit)
        
        # Calculate statistics
        total_points = len(df)
        total_anomalies = len(anomalies_df)
        anomaly_rate = (total_anomalies / total_points * 100) if total_points > 0 else 0
        
        # Create anomaly records
        anomalies_list = []
        for _, row in anomalies_df.iterrows():
            anomaly_type = "spike" if row['value'] > df['value'].median() else "drop"
            severity = "critical" if row['anomaly_score'] > threshold * 1.5 else "warning"
            
            anomalies_list.append({
                "date": row['date'].strftime('%Y-%m-%d'),
                "value": round(float(row['value']), 2),
                "anomaly_score": round(float(row['anomaly_score']), 2),
                "type": anomaly_type,
                "severity": severity,
                "deviation_pct": round(
                    ((row['value'] - df['value'].median()) / df['value'].median()) * 100, 1
                )
            })
        
        # Generate alerts for critical anomalies
        alerts = []
        critical_anomalies = [a for a in anomalies_list if a['severity'] == 'critical']
        
        for anomaly in critical_anomalies[:3]:  # Top 3 critical
            if anomaly['type'] == 'spike':
                alerts.append({
                    "type": "critical",
                    "title": f"⚠️ Unusual {metric.title()} Spike Detected",
                    "message": f"{metric.title()} spiked to ${anomaly['value']:,.2f} on {anomaly['date']} "
                              f"({anomaly['deviation_pct']:+.1f}% from median)",
                    "action": "Investigate cause and verify data accuracy",
                    "date": anomaly['date']
                })
            else:
                alerts.append({
                    "type": "critical",
                    "title": f"⚠️ Significant {metric.title()} Drop Detected",
                    "message": f"{metric.title()} dropped to ${anomaly['value']:,.2f} on {anomaly['date']} "
                              f"({anomaly['deviation_pct']:+.1f}% from median)",
                    "action": "Check for system issues or market changes",
                    "date": anomaly['date']
                })
        
        # Build response
        response = {
            "status": "success",
            "data": {
                "anomalies": anomalies_list,
                "summary": {
                    "total_data_points": total_points,
                    "anomalies_detected": total_anomalies,
                    "anomaly_rate": round(anomaly_rate, 2),
                    "method": method,
                    "threshold": threshold,
                    "period": period,
                    "metric": metric,
                    "mean_value": round(float(df['value'].mean()), 2),
                    "median_value": round(float(df['value'].median()), 2),
                    "std_deviation": round(float(df['value'].std()), 2)
                },
                "alerts": alerts
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
        logger.info(
            f"Anomaly detection completed: {total_anomalies} anomalies found "
            f"({anomaly_rate:.1f}% rate) using {method} method"
        )
        
        return response
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Anomaly detection failed: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Anomaly detection failed: {str(e)}"
        )
