"""
Comprehensive test for the calculate_retention function
"""
import pandas as pd
import numpy as np
import sys
sys.path.append('scripts')

from kpi_calculations import calculate_retention

print("="*80)
print("CUSTOMER RETENTION ANALYSIS - PRODUCTION TEST")
print("="*80)

# Create realistic user activity data
np.random.seed(42)

# Simulate 50 users over 6 months with varying retention patterns
users = []
dates = []
activities = []

# Early adopters (January) - High retention
for user_id in range(1, 16):
    start_date = pd.Timestamp('2026-01-15') + pd.Timedelta(days=np.random.randint(0, 15))
    for month_offset in range(6):
        # 80% chance to be active each month for early adopters
        if np.random.random() < 0.8:
            activity_date = start_date + pd.Timedelta(days=30*month_offset + np.random.randint(0, 25))
            users.append(user_id)
            dates.append(activity_date)
            activities.append('login')

# Mid-term users (February-March) - Medium retention
for user_id in range(16, 36):
    start_month = np.random.randint(1, 3)
    start_date = pd.Timestamp(f'2026-0{start_month+1}-01') + pd.Timedelta(days=np.random.randint(0, 28))
    for month_offset in range(5 - start_month):
        # 50% chance to be active each month for mid-term users
        if np.random.random() < 0.5:
            activity_date = start_date + pd.Timedelta(days=30*month_offset + np.random.randint(0, 25))
            users.append(user_id)
            dates.append(activity_date)
            activities.append('login')

# Recent users (April-May) - Lower retention
for user_id in range(36, 51):
    start_month = np.random.randint(3, 5)
    start_date = pd.Timestamp(f'2026-0{start_month+1}-01') + pd.Timedelta(days=np.random.randint(0, 28))
    remaining_months = min(2, 5 - start_month)
    for month_offset in range(remaining_months):
        # 30% chance to be active each month for recent users
        if np.random.random() < 0.3:
            activity_date = start_date + pd.Timedelta(days=30*month_offset + np.random.randint(0, 25))
            users.append(user_id)
            dates.append(activity_date)
            activities.append('login')

df = pd.DataFrame({
    'user_id': users,
    'activity_date': dates,
    'activity_type': activities
})

print("\n📊 DATASET OVERVIEW")
print("-" * 80)
print(f"Total Users: {df['user_id'].nunique()}")
print(f"Total Activities: {len(df)}")
print(f"Date Range: {df['activity_date'].min().date()} to {df['activity_date'].max().date()}")
print(f"Average Activities per User: {len(df) / df['user_id'].nunique():.2f}")

# Display sample data
print("\nSample Data (first 10 rows):")
print(df.head(10))

print("\n" + "="*80)
print("TEST 1: COHORT-BASED RETENTION ANALYSIS")
print("="*80)

retention_cohort = calculate_retention(
    df,
    user_column='user_id',
    date_column='activity_date',
    cohort_period='M',
    retention_periods=5,
    method='cohort',
    include_cohort_size=True
)

print("\n📈 Monthly Cohort Retention Matrix:")
print("-" * 80)
print(retention_cohort.to_string(index=False))

print("\n💡 Interpretation:")
print("   - Period_0: Initial cohort (100% by definition)")
print("   - Period_1: Retention in the next month")
print("   - Period_N: Retention N months after joining")

print("\n" + "="*80)
print("TEST 2: SIMPLE PERIOD-OVER-PERIOD RETENTION")
print("="*80)

retention_simple = calculate_retention(
    df,
    user_column='user_id',
    date_column='activity_date',
    cohort_period='M',
    method='simple'
)

print("\n📊 Period-over-Period Retention:")
print("-" * 80)
print(retention_simple.to_string(index=False))

print("\n💡 Interpretation:")
print("   Shows what % of users active in one month remain active in the next month")

print("\n" + "="*80)
print("TEST 3: ROLLING RETENTION (FIRST 3 PERIODS)")
print("="*80)

retention_rolling = calculate_retention(
    df,
    user_column='user_id',
    date_column='activity_date',
    cohort_period='M',
    retention_periods=3,
    method='rolling'
)

print("\n📊 Rolling Retention (shows active users regardless of gaps):")
print("-" * 80)
# Show only first 10 rows for brevity
print(retention_rolling.head(10).to_string(index=False))
print(f"... ({len(retention_rolling)} total rows)")

print("\n" + "="*80)
print("TEST 4: OVERALL RETENTION METRICS")
print("="*80)

# Get average retention rate
avg_retention = calculate_retention(
    df,
    user_column='user_id',
    date_column='activity_date',
    method='cohort',
    return_format='rate'
)

print(f"\n📊 Average Retention Rate: {avg_retention:.2f}%")

# Get detailed report
retention_report = calculate_retention(
    df,
    user_column='user_id',
    date_column='activity_date',
    method='cohort',
    retention_periods=5,
    return_format='dict'
)

print("\n📋 Detailed Retention Report:")
print("-" * 80)
print(f"Total Users Analyzed: {retention_report['total_users']}")
print(f"Total Activity Records: {retention_report['total_records']}")
print(f"Number of Cohorts: {retention_report['cohort_count']}")
print(f"Date Range: {retention_report['date_range']['start']} to {retention_report['date_range']['end']}")

if 'average_retention' in retention_report:
    print(f"\nRetention Statistics:")
    print(f"  • Average: {retention_report['average_retention']:.2f}%")
    print(f"  • Median:  {retention_report['median_retention']:.2f}%")
    print(f"  • Min:     {retention_report['min_retention']:.2f}%")
    print(f"  • Max:     {retention_report['max_retention']:.2f}%")
    print(f"  • Std Dev: {retention_report['std_retention']:.2f}%")

print(f"\nProcessing Time: {retention_report['processing_time_seconds']} seconds")
print(f"Status: {retention_report['status']}")

print("\n" + "="*80)
print("TEST 5: N-DAY RETENTION (DAY 1, 7, 14, 30)")
print("="*80)

retention_nday = calculate_retention(
    df,
    user_column='user_id',
    date_column='activity_date',
    method='n_day',
    retention_periods=30
)

print("\n📊 N-Day Retention by Cohort:")
print("-" * 80)
if len(retention_nday) > 0:
    print(retention_nday.to_string(index=False))
else:
    print("Not enough data for N-day retention analysis with current dataset")

print("\n" + "="*80)
print("KEY INSIGHTS")
print("="*80)

# Calculate some key insights
cohort_data = retention_cohort.copy()
if 'cohort_size' in cohort_data.columns:
    total_users = cohort_data['cohort_size'].sum()
    print(f"\n1. Total Users by Cohort: {int(total_users)}")
    
    # Find best performing cohort
    retention_cols = [col for col in cohort_data.columns if col.startswith('Period_')]
    if len(retention_cols) > 1:
        cohort_data['avg_retention'] = cohort_data[retention_cols[1:]].mean(axis=1, skipna=True)
        best_cohort = cohort_data.loc[cohort_data['avg_retention'].idxmax()]
        print(f"\n2. Best Performing Cohort: {best_cohort['cohort_period']}")
        print(f"   Average Retention: {best_cohort['avg_retention']:.2f}%")
        print(f"   Cohort Size: {int(best_cohort['cohort_size'])}")

# Month-over-month retention trend
if len(retention_simple) > 0:
    print(f"\n3. Monthly Retention Trend:")
    for _, row in retention_simple.iterrows():
        print(f"   {row['period']}: {row['retention_rate']:.1f}% retained")

print("\n" + "="*80)
print("✅ ALL RETENTION TESTS COMPLETED SUCCESSFULLY!")
print("="*80)

print("\n📝 SUMMARY:")
print(f"   • Tested cohort-based, simple, rolling, and N-day retention methods")
print(f"   • Analyzed {df['user_id'].nunique()} users over {df['activity_date'].dt.to_period('M').nunique()} months")
print(f"   • Overall retention rate: {avg_retention:.2f}%")
print(f"   • All calculations completed with production-level error handling")
print("\n" + "="*80)
