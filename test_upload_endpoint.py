"""
Test file upload endpoint
This creates test data files and demonstrates how to use the /upload endpoint
"""
import pandas as pd
import numpy as np
from pathlib import Path

print("="*80)
print("FILE UPLOAD ENDPOINT - TEST DATA GENERATION")
print("="*80)

# Create test directory
test_dir = Path('test_upload_files')
test_dir.mkdir(exist_ok=True)

# Generate sample CSV file
print("\n📁 Generating test CSV file...")
np.random.seed(42)

products = ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones']
categories = ['Electronics', 'Accessories', 'Accessories', 'Electronics', 'Audio']

sales_data = []
for i in range(100):
    sales_data.append({
        'order_id': f'ORD-{1000 + i}',
        'product': np.random.choice(products),
        'category': np.random.choice(categories),
        'quantity': np.random.randint(1, 5),
        'price': round(np.random.uniform(10, 1000), 2),
        'order_date': pd.Timestamp('2026-05-01') + pd.Timedelta(days=np.random.randint(0, 30))
    })

df = pd.DataFrame(sales_data)
df['revenue'] = df['quantity'] * df['price']

csv_path = test_dir / 'test_sales_data.csv'
df.to_csv(csv_path, index=False)
print(f"✅ Created: {csv_path}")
print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")

# Generate Excel file
excel_path = test_dir / 'test_sales_data.xlsx'
df.to_excel(excel_path, index=False, sheet_name='Sales')
print(f"✅ Created: {excel_path}")

# Generate JSON file
json_path = test_dir / 'test_sales_data.json'
df.to_json(json_path, orient='records', indent=2)
print(f"✅ Created: {json_path}")

# Generate Parquet file
parquet_path = test_dir / 'test_sales_data.parquet'
df.to_parquet(parquet_path, index=False)
print(f"✅ Created: {parquet_path}")

print("\n" + "="*80)
print("TEST FILES CREATED")
print("="*80)

print("\n📝 HOW TO TEST THE /upload ENDPOINT:")
print("-" * 80)
print("""
Using curl (Linux/Mac/Windows with curl):
------------------------------------------
# Upload CSV file
curl -X POST "http://localhost:8000/api/v1/upload" \\
     -F "file=@test_upload_files/test_sales_data.csv" \\
     -F "process_data=true" \\
     -F "save_to_raw=true" \\
     -F "save_to_processed=true"

# Upload Excel file
curl -X POST "http://localhost:8000/api/v1/upload" \\
     -F "file=@test_upload_files/test_sales_data.xlsx" \\
     -F "process_data=true"

# Upload with validation only
curl -X POST "http://localhost:8000/api/v1/upload" \\
     -F "file=@test_upload_files/test_sales_data.json" \\
     -F "validate_schema=true" \\
     -F "save_to_raw=false"

Using Python requests:
----------------------
import requests

# Upload file
files = {'file': open('test_upload_files/test_sales_data.csv', 'rb')}
data = {
    'process_data': 'true',
    'save_to_raw': 'true',
    'save_to_processed': 'true',
    'validate_schema': 'true'
}

response = requests.post(
    'http://localhost:8000/api/v1/upload',
    files=files,
    data=data
)

print(response.json())

Using PowerShell:
-----------------
$uri = "http://localhost:8000/api/v1/upload"
$filePath = "test_upload_files\\test_sales_data.csv"
$form = @{
    file = Get-Item -Path $filePath
    process_data = "true"
    save_to_raw = "true"
    save_to_processed = "true"
}
Invoke-RestMethod -Uri $uri -Method Post -Form $form

Expected Response:
------------------
{
  "status": "success",
  "message": "File 'test_sales_data.csv' uploaded successfully",
  "file_info": {
    "filename": "test_sales_data.csv",
    "file_type": "csv",
    "file_size_mb": 0.01,
    "rows": 100,
    "columns": 7,
    "column_names": ["order_id", "product", "category", "quantity", "price", "order_date", "revenue"]
  },
  "validation_results": {
    "is_valid": true,
    "has_warnings": false,
    "has_errors": false,
    "warnings": [],
    "errors": [],
    "stats": {
      "null_percentage": 0.0,
      "empty_rows": 0
    }
  },
  "processing_summary": {
    "actions": ["Standardized column names", "Cleaned whitespace from 3 text columns"],
    "rows_before": 100,
    "rows_after": 100,
    "columns_before": 7,
    "columns_after": 7
  },
  "saved_paths": {
    "raw": "data/raw/test_sales_data_20260507_120000.csv",
    "processed": "data/processed/test_sales_data_processed_20260507_120000.parquet",
    "metadata": "data/processed/test_sales_data_processed_20260507_120000_metadata.json"
  },
  "processing_time_seconds": 0.125
}
""")

print("\n" + "="*80)
print("ENDPOINT FEATURES:")
print("="*80)
print("""
✅ Multi-format Support:
   - CSV (.csv)
   - Excel (.xlsx, .xls)
   - Parquet (.parquet)
   - JSON (.json)

✅ Validation:
   - File size limits (max 50MB)
   - Format validation
   - Schema validation
   - Data quality checks
   - Duplicate column detection
   - Null value analysis

✅ Processing:
   - Remove empty rows/columns
   - Standardize column names
   - Clean whitespace
   - Data type detection

✅ Storage Options:
   - Save to raw directory (original)
   - Save to processed directory (cleaned)
   - Generate metadata files
   - Timestamped filenames

✅ Response Details:
   - File information
   - Validation results
   - Processing summary
   - Saved file paths
   - Processing time

✅ Error Handling:
   - 400: Invalid file format
   - 413: File too large
   - 500: Server processing error
""")

print("\n" + "="*80)
print("✅ TEST FILES READY FOR UPLOAD!")
print("="*80)
print(f"\nFiles location: {test_dir.absolute()}")
print(f"Total files created: 4")
print("\nStart the API server with:")
print("  uvicorn api.main:app --reload")
print("\nThen test the upload endpoint using one of the methods above.")
print("="*80)
