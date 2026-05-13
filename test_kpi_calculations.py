"""
Production-Level Test Suite for KPI Calculations Module
========================================================
Comprehensive tests for all KPI calculation functions with edge cases,
error handling, data validation, and performance testing.

Test Coverage:
- Revenue calculations (all metric types)
- Product performance analytics
- Profit margins and ARPU
- Conversion rates
- Anomaly detection (all methods)
- Data validation and error handling
- Edge cases and boundary conditions
- Performance benchmarks

Author: KPI Intelligence Team
Created: 2026-05-13
"""

import sys
import unittest
from datetime import datetime, timedelta
from decimal import Decimal
import warnings

import pandas as pd
import numpy as np

# Add scripts directory to path
sys.path.insert(0, 'scripts')

from kpi_calculations import (
    calculate_revenue,
    calculate_product_performance,
    calculate_profit_margin,
    calculate_arpu,
    calculate_conversion_rate,
    detect_anomalies,
    KPICalculationError,
    RevenueMetricType,
    AggregationPeriod
)


class TestCalculateRevenue(unittest.TestCase):
    """Test suite for calculate_revenue function."""
    
    def setUp(self):
        """Set up test data."""
        np.random.seed(42)
        dates = pd.date_range('2026-01-01', periods=100, freq='D')
        self.df = pd.DataFrame({
            'date': dates,
            'revenue': np.random.uniform(100, 1000, 100),
            'category': np.random.choice(['A', 'B', 'C'], 100),
            'quantity': np.random.randint(1, 50, 100)
        })
    
    def test_total_revenue_basic(self):
        """Test basic total revenue calculation."""
        result = calculate_revenue(self.df, revenue_column='revenue')
        self.assertIsInstance(result, float)
        self.assertGreater(result, 0)
        expected = self.df['revenue'].sum()
        self.assertAlmostEqual(result, expected, places=2)
    
    def test_average_revenue(self):
        """Test average revenue calculation."""
        result = calculate_revenue(
            self.df,
            revenue_column='revenue',
            metric_type=RevenueMetricType.AVERAGE
        )
        self.assertIsInstance(result, float)
        expected = self.df['revenue'].mean()
        self.assertAlmostEqual(result, expected, places=2)
    
    def test_median_revenue(self):
        """Test median revenue calculation."""
        result = calculate_revenue(
            self.df,
            revenue_column='revenue',
            metric_type=RevenueMetricType.MEDIAN
        )
        self.assertIsInstance(result, float)
        expected = self.df['revenue'].median()
        self.assertAlmostEqual(result, expected, places=2)
    
    def test_revenue_by_category(self):
        """Test revenue grouped by category."""
        result = calculate_revenue(
            self.df,
            revenue_column='revenue',
            category_column='category',
            metric_type=RevenueMetricType.BY_CATEGORY
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 3)  # 3 categories
        self.assertIn('category', result.columns)
        self.assertIn('total_revenue', result.columns)
        self.assertIn('percentage', result.columns)
    
    def test_revenue_by_period(self):
        """Test revenue grouped by time period."""
        result = calculate_revenue(
            self.df,
            revenue_column='revenue',
            date_column='date',
            metric_type=RevenueMetricType.BY_PERIOD,
            period='W'  # Use 'W' for weekly instead of 'M' for pandas compatibility
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertGreater(len(result), 0)
        self.assertIn('period', result.columns)
        self.assertIn('total_revenue', result.columns)
    
    def test_cumulative_revenue(self):
        """Test cumulative revenue calculation."""
        result = calculate_revenue(
            self.df,
            revenue_column='revenue',
            date_column='date',
            metric_type=RevenueMetricType.CUMULATIVE
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('cumulative_revenue', result.columns)
        # Verify cumulative is monotonically increasing
        self.assertTrue((result['cumulative_revenue'].diff().dropna() >= 0).all())
    
    def test_growth_rate(self):
        """Test revenue growth rate calculation."""
        result = calculate_revenue(
            self.df,
            revenue_column='revenue',
            date_column='date',
            metric_type=RevenueMetricType.GROWTH_RATE,
            period=AggregationPeriod.WEEKLY
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('growth_rate_pct', result.columns)
    
    def test_moving_average(self):
        """Test moving average calculation."""
        result = calculate_revenue(
            self.df,
            revenue_column='revenue',
            date_column='date',
            metric_type=RevenueMetricType.MOVING_AVERAGE,
            moving_window=7
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result.columns), 3)  # date, revenue, ma_7_revenue
    
    def test_with_breakdown(self):
        """Test revenue calculation with detailed breakdown."""
        result = calculate_revenue(
            self.df,
            revenue_column='revenue',
            include_breakdown=True
        )
        self.assertIsInstance(result, dict)
        self.assertIn('total_revenue', result)
        self.assertIn('records_processed', result)
        self.assertIn('min_revenue', result)
        self.assertIn('max_revenue', result)
        self.assertIn('status', result)
        self.assertEqual(result['status'], 'success')
    
    def test_exclude_zero_values(self):
        """Test excluding zero values."""
        df_with_zeros = self.df.copy()
        # Explicitly set some values to zero
        zero_indices = df_with_zeros.index[:11]  # First 11 rows
        df_with_zeros.loc[zero_indices, 'revenue'] = 0
        
        result_with_zeros = calculate_revenue(
            df_with_zeros,
            revenue_column='revenue',
            exclude_zero=False,
            include_breakdown=True
        )
        
        result_without_zeros = calculate_revenue(
            df_with_zeros,
            revenue_column='revenue',
            exclude_zero=True,
            include_breakdown=True
        )
        
        # Verify that excluding zeros processes fewer records
        self.assertLess(
            result_without_zeros['records_processed'],
            result_with_zeros['records_processed']
        )
        # Revenue totals should be the same since zeros don't contribute
        self.assertEqual(
            result_with_zeros['total_revenue'],
            result_without_zeros['total_revenue']
        )
    
    def test_exclude_negative_values(self):
        """Test excluding negative values."""
        df_with_negatives = self.df.copy()
        df_with_negatives.loc[0:10, 'revenue'] = -100
        
        result = calculate_revenue(
            df_with_negatives,
            revenue_column='revenue',
            exclude_negative=True
        )
        
        self.assertGreater(result, 0)
    
    def test_empty_dataframe(self):
        """Test with empty DataFrame."""
        empty_df = pd.DataFrame()
        result = calculate_revenue(empty_df, revenue_column='revenue')
        self.assertEqual(result, 0.0)
    
    def test_invalid_column(self):
        """Test with invalid column name."""
        with self.assertRaises(ValueError):
            calculate_revenue(self.df, revenue_column='invalid_column')
    
    def test_invalid_metric_type(self):
        """Test with invalid metric type."""
        with self.assertRaises(ValueError):
            calculate_revenue(self.df, revenue_column='revenue', metric_type='invalid')
    
    def test_missing_date_column_for_time_series(self):
        """Test that time-based metrics require date column."""
        with self.assertRaises((ValueError, KPICalculationError)):
            calculate_revenue(
                self.df,
                revenue_column='revenue',
                metric_type=RevenueMetricType.CUMULATIVE
            )
    
    def test_rounding_decimals(self):
        """Test decimal rounding."""
        result = calculate_revenue(
            self.df,
            revenue_column='revenue',
            round_decimals=4
        )
        # Check that result has at most 4 decimal places
        result_str = str(result)
        if '.' in result_str:
            decimal_places = len(result_str.split('.')[1])
            self.assertLessEqual(decimal_places, 4)


class TestCalculateProductPerformance(unittest.TestCase):
    """Test suite for calculate_product_performance function."""
    
    def setUp(self):
        """Set up test data."""
        np.random.seed(42)
        dates = pd.date_range('2026-01-01', periods=200, freq='D')
        products = ['Product_A', 'Product_B', 'Product_C', 'Product_D', 'Product_E']
        
        self.df = pd.DataFrame({
            'date': np.random.choice(dates, 200),
            'product': np.random.choice(products, 200),
            'revenue': np.random.uniform(50, 500, 200),
            'cost': np.random.uniform(20, 300, 200),
            'quantity': np.random.randint(1, 20, 200),
            'category': np.random.choice(['Electronics', 'Clothing', 'Food'], 200),
            'is_returned': np.random.choice([True, False], 200, p=[0.05, 0.95])
        })
    
    def test_basic_product_performance(self):
        """Test basic product performance calculation."""
        result = calculate_product_performance(
            self.df,
            product_column='product',
            revenue_column='revenue'
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 5)  # 5 products
        self.assertIn('product', result.columns)
        self.assertIn('total_revenue', result.columns)
    
    def test_product_performance_with_all_metrics(self):
        """Test with all available metrics."""
        result = calculate_product_performance(
            self.df,
            product_column='product',
            revenue_column='revenue',
            quantity_column='quantity',
            cost_column='cost',
            return_column='is_returned',
            category_column='category'
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertIn('total_revenue', result.columns)
        self.assertIn('total_quantity', result.columns)
        self.assertIn('total_profit', result.columns)
        self.assertIn('profit_margin', result.columns)
        self.assertIn('roi', result.columns)
        self.assertIn('return_rate', result.columns)
        self.assertIn('category', result.columns)
    
    def test_product_performance_with_rankings(self):
        """Test product rankings."""
        result = calculate_product_performance(
            self.df,
            product_column='product',
            revenue_column='revenue',
            quantity_column='quantity',
            cost_column='cost',
            include_rankings=True
        )
        self.assertIn('revenue_rank', result.columns)
        self.assertIn('quantity_rank', result.columns)
        self.assertIn('profit_rank', result.columns)
        # Check that rank 1 is the highest revenue
        self.assertEqual(result.loc[0, 'revenue_rank'], 1)
    
    def test_product_performance_top_n(self):
        """Test limiting to top N products."""
        result = calculate_product_performance(
            self.df,
            product_column='product',
            revenue_column='revenue',
            top_n=3
        )
        self.assertEqual(len(result), 3)
    
    def test_product_performance_with_trends(self):
        """Test with trend analysis."""
        result = calculate_product_performance(
            self.df,
            product_column='product',
            revenue_column='revenue',
            date_column='date',
            include_trends=True,
            period=AggregationPeriod.MONTHLY
        )
        self.assertIsInstance(result, dict)
        self.assertIn('performance_summary', result)
        self.assertIn('trends', result)
        self.assertIn('total_revenue', result)
        self.assertIn('status', result)
    
    def test_product_performance_specific_metrics(self):
        """Test with specific metric selection."""
        result = calculate_product_performance(
            self.df,
            product_column='product',
            revenue_column='revenue',
            quantity_column='quantity',
            metrics=['revenue', 'quantity']
        )
        self.assertIn('total_revenue', result.columns)
        self.assertIn('total_quantity', result.columns)
    
    def test_empty_dataframe_product(self):
        """Test with empty DataFrame."""
        empty_df = pd.DataFrame()
        result = calculate_product_performance(
            empty_df,
            product_column='product'
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)
    
    def test_invalid_product_column(self):
        """Test with invalid product column."""
        with self.assertRaises(ValueError):
            calculate_product_performance(
                self.df,
                product_column='invalid_product'
            )


class TestCalculateProfitMargin(unittest.TestCase):
    """Test suite for calculate_profit_margin function."""
    
    def setUp(self):
        """Set up test data."""
        self.df = pd.DataFrame({
            'revenue': [1000, 2000, 1500, 3000],
            'cost': [600, 1200, 900, 1800]
        })
    
    def test_profit_margin_basic(self):
        """Test basic profit margin calculation."""
        result = calculate_profit_margin(
            self.df,
            revenue_column='revenue',
            cost_column='cost'
        )
        self.assertIsInstance(result, float)
        self.assertGreater(result, 0)
        self.assertLess(result, 100)
        
        # Calculate expected
        total_revenue = self.df['revenue'].sum()
        total_cost = self.df['cost'].sum()
        expected = ((total_revenue - total_cost) / total_revenue) * 100
        self.assertAlmostEqual(result, expected, places=2)
    
    def test_profit_margin_zero_revenue(self):
        """Test with zero revenue."""
        df_zero = pd.DataFrame({
            'revenue': [0, 0],
            'cost': [100, 200]
        })
        result = calculate_profit_margin(df_zero)
        self.assertEqual(result, 0.0)
    
    def test_profit_margin_negative(self):
        """Test with negative profit margin (loss)."""
        df_loss = pd.DataFrame({
            'revenue': [100, 200],
            'cost': [200, 400]
        })
        result = calculate_profit_margin(df_loss)
        self.assertLess(result, 0)


class TestCalculateARPU(unittest.TestCase):
    """Test suite for calculate_arpu function."""
    
    def setUp(self):
        """Set up test data."""
        self.df = pd.DataFrame({
            'user_id': ['U1', 'U2', 'U3', 'U1', 'U2'],
            'revenue': [100, 200, 150, 50, 100]
        })
    
    def test_arpu_with_user_column(self):
        """Test ARPU with user column."""
        result = calculate_arpu(
            self.df,
            revenue_column='revenue',
            user_column='user_id'
        )
        self.assertIsInstance(result, float)
        self.assertGreater(result, 0)
        
        # Expected: total revenue / unique users
        total_revenue = self.df['revenue'].sum()
        unique_users = self.df['user_id'].nunique()
        expected = total_revenue / unique_users
        self.assertAlmostEqual(result, expected, places=2)
    
    def test_arpu_without_user_column(self):
        """Test ARPU without user column (uses row count)."""
        result = calculate_arpu(
            self.df,
            revenue_column='revenue'
        )
        self.assertIsInstance(result, float)
        
        expected = self.df['revenue'].sum() / len(self.df)
        self.assertAlmostEqual(result, expected, places=2)
    
    def test_arpu_zero_users(self):
        """Test with empty DataFrame."""
        empty_df = pd.DataFrame({'revenue': []})
        result = calculate_arpu(empty_df, revenue_column='revenue')
        self.assertEqual(result, 0.0)


class TestCalculateConversionRate(unittest.TestCase):
    """Test suite for calculate_conversion_rate function."""
    
    def setUp(self):
        """Set up test data."""
        self.df = pd.DataFrame({
            'converted': [1, 0, 1, 0, 0, 1, 1, 0, 0, 1],
            'total': [1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
        })
    
    def test_conversion_rate_basic(self):
        """Test basic conversion rate calculation."""
        result = calculate_conversion_rate(
            self.df,
            conversion_column='converted'
        )
        self.assertIsInstance(result, float)
        self.assertGreaterEqual(result, 0)
        self.assertLessEqual(result, 100)
        
        # Expected: (conversions / total) * 100
        expected = (self.df['converted'].sum() / len(self.df)) * 100
        self.assertAlmostEqual(result, expected, places=2)
    
    def test_conversion_rate_boolean(self):
        """Test with boolean values."""
        df_bool = pd.DataFrame({
            'converted': [True, False, True, False, True]
        })
        result = calculate_conversion_rate(
            df_bool,
            conversion_column='converted'
        )
        self.assertEqual(result, 60.0)  # 3 out of 5
    
    def test_conversion_rate_zero_total(self):
        """Test with zero total."""
        empty_df = pd.DataFrame({'converted': []})
        result = calculate_conversion_rate(
            empty_df,
            conversion_column='converted'
        )
        self.assertEqual(result, 0.0)


class TestDetectAnomalies(unittest.TestCase):
    """Test suite for detect_anomalies function."""
    
    def setUp(self):
        """Set up test data with known anomalies."""
        np.random.seed(42)
        # Normal data
        normal_data = np.random.normal(100, 10, 90)
        # Anomalies
        anomalies = [150, 160, 30, 25, 180, 20, 175, 35, 155, 28]
        # Combine
        all_values = np.concatenate([normal_data, anomalies])
        np.random.shuffle(all_values)
        
        dates = pd.date_range('2026-01-01', periods=100, freq='D')
        self.df = pd.DataFrame({
            'date': dates,
            'value': all_values
        })
    
    def test_zscore_anomaly_detection(self):
        """Test Z-score method."""
        result = detect_anomalies(
            self.df,
            value_column='value',
            method='zscore',
            threshold=3.0,
            return_scores=True
        )
        
        if isinstance(result, dict):
            self.assertIn('data', result)
            self.assertIn('summary', result)
            self.assertGreater(result['summary']['total_anomalies'], 0)
            df_result = result['data']
        else:
            df_result = result
        
        self.assertIn('is_anomaly', df_result.columns)
        self.assertIn('anomaly_score', df_result.columns)
    
    def test_iqr_anomaly_detection(self):
        """Test IQR method."""
        result = detect_anomalies(
            self.df,
            value_column='value',
            method='iqr',
            threshold=1.5
        )
        
        if isinstance(result, dict):
            df_result = result['data']
        else:
            df_result = result
        
        self.assertIn('is_anomaly', df_result.columns)
        self.assertTrue(df_result['is_anomaly'].sum() > 0)
    
    def test_mad_anomaly_detection(self):
        """Test Modified Z-score (MAD) method."""
        result = detect_anomalies(
            self.df,
            value_column='value',
            method='mad',
            threshold=3.5
        )
        
        if isinstance(result, dict):
            df_result = result['data']
        else:
            df_result = result
        
        self.assertIn('is_anomaly', df_result.columns)
    
    def test_moving_average_anomaly_detection(self):
        """Test moving average method."""
        result = detect_anomalies(
            self.df,
            value_column='value',
            date_column='date',
            method='moving_avg',
            window_size=7,
            threshold=2.5
        )
        
        if isinstance(result, dict):
            df_result = result['data']
        else:
            df_result = result
        
        self.assertIn('is_anomaly', df_result.columns)
    
    def test_anomaly_detection_with_boundaries(self):
        """Test with boundary calculation."""
        result = detect_anomalies(
            self.df,
            value_column='value',
            method='zscore',
            include_boundaries=True
        )
        
        if isinstance(result, dict):
            df_result = result['data']
        else:
            df_result = result
        
        self.assertIn('upper_bound', df_result.columns)
        self.assertIn('lower_bound', df_result.columns)
    
    def test_anomaly_detection_empty_dataframe(self):
        """Test with empty DataFrame."""
        empty_df = pd.DataFrame()
        result = detect_anomalies(
            empty_df,
            value_column='value',
            method='zscore'
        )
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 0)
    
    def test_anomaly_detection_invalid_column(self):
        """Test with invalid column."""
        with self.assertRaises(ValueError):
            detect_anomalies(
                self.df,
                value_column='invalid_column',
                method='zscore'
            )
    
    def test_anomaly_detection_invalid_method(self):
        """Test with invalid method."""
        with self.assertRaises(ValueError):
            detect_anomalies(
                self.df,
                value_column='value',
                method='invalid_method'
            )
    
    def test_anomaly_detection_handle_missing(self):
        """Test handling of missing values."""
        df_with_nulls = self.df.copy()
        df_with_nulls.loc[0:5, 'value'] = np.nan
        
        result = detect_anomalies(
            df_with_nulls,
            value_column='value',
            method='zscore',
            handle_missing='drop'
        )
        
        if isinstance(result, dict):
            df_result = result['data']
        else:
            df_result = result
        
        # Should have fewer rows after dropping nulls
        self.assertLess(len(df_result), len(df_with_nulls))


class TestEdgeCasesAndErrors(unittest.TestCase):
    """Test suite for edge cases and error handling."""
    
    def test_null_input(self):
        """Test with None as input."""
        with self.assertRaises(ValueError):
            calculate_revenue(None, revenue_column='revenue')
    
    def test_non_dataframe_input(self):
        """Test with non-DataFrame input."""
        with self.assertRaises(ValueError):
            calculate_revenue([1, 2, 3], revenue_column='revenue')
    
    def test_all_null_values(self):
        """Test with all null values in revenue column."""
        df_all_null = pd.DataFrame({
            'revenue': [np.nan, np.nan, np.nan]
        })
        # Production code validates data and raises error for all-null columns
        with self.assertRaises((ValueError, KPICalculationError)):
            calculate_revenue(df_all_null, revenue_column='revenue', validate_data=True)
    
    def test_mixed_data_types(self):
        """Test with mixed data types that can be coerced."""
        df_mixed = pd.DataFrame({
            'revenue': ['100', '200.5', 300, '400']
        })
        result = calculate_revenue(df_mixed, revenue_column='revenue')
        self.assertIsInstance(result, float)
        self.assertGreater(result, 0)
    
    def test_very_large_numbers(self):
        """Test with very large numbers."""
        df_large = pd.DataFrame({
            'revenue': [1e10, 2e10, 3e10]
        })
        result = calculate_revenue(df_large, revenue_column='revenue')
        self.assertGreaterEqual(result, 6e10)  # Sum is 6e10
    
    def test_very_small_numbers(self):
        """Test with very small numbers."""
        df_small = pd.DataFrame({
            'revenue': [0.001, 0.002, 0.003]
        })
        result = calculate_revenue(df_small, revenue_column='revenue')
        self.assertGreater(result, 0)
        self.assertLessEqual(result, 0.01)  # Sum is exactly 0.006, rounded to 0.01


class TestPerformance(unittest.TestCase):
    """Test suite for performance benchmarks."""
    
    def test_large_dataset_revenue(self):
        """Test performance with large dataset."""
        np.random.seed(42)
        # Create large dataset (10,000 rows)
        dates = pd.date_range('2020-01-01', periods=10000, freq='D')
        df_large = pd.DataFrame({
            'date': dates,
            'revenue': np.random.uniform(100, 1000, 10000),
            'category': np.random.choice(['A', 'B', 'C', 'D', 'E'], 10000)
        })
        
        start_time = datetime.now()
        result = calculate_revenue(
            df_large,
            revenue_column='revenue',
            include_breakdown=True
        )
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        
        self.assertIsInstance(result, dict)
        self.assertEqual(result['records_processed'], 10000)
        # Should complete in reasonable time (< 5 seconds)
        self.assertLess(duration, 5.0)
        print(f"\nLarge dataset revenue calculation: {duration:.3f}s")
    
    def test_large_dataset_product_performance(self):
        """Test performance with large product dataset."""
        np.random.seed(42)
        products = [f'Product_{i}' for i in range(100)]
        df_large = pd.DataFrame({
            'product': np.random.choice(products, 5000),
            'revenue': np.random.uniform(50, 500, 5000),
            'cost': np.random.uniform(20, 300, 5000),
            'quantity': np.random.randint(1, 20, 5000)
        })
        
        start_time = datetime.now()
        result = calculate_product_performance(
            df_large,
            product_column='product',
            revenue_column='revenue',
            cost_column='cost',
            quantity_column='quantity'
        )
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        
        self.assertIsInstance(result, pd.DataFrame)
        self.assertEqual(len(result), 100)
        # Should complete in reasonable time (< 5 seconds)
        self.assertLess(duration, 5.0)
        print(f"Large product performance calculation: {duration:.3f}s")


def run_test_suite():
    """Run the complete test suite with detailed reporting."""
    print("=" * 80)
    print("KPI CALCULATIONS - PRODUCTION TEST SUITE")
    print("=" * 80)
    print(f"Test Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestCalculateRevenue))
    suite.addTests(loader.loadTestsFromTestCase(TestCalculateProductPerformance))
    suite.addTests(loader.loadTestsFromTestCase(TestCalculateProfitMargin))
    suite.addTests(loader.loadTestsFromTestCase(TestCalculateARPU))
    suite.addTests(loader.loadTestsFromTestCase(TestCalculateConversionRate))
    suite.addTests(loader.loadTestsFromTestCase(TestDetectAnomalies))
    suite.addTests(loader.loadTestsFromTestCase(TestEdgeCasesAndErrors))
    suite.addTests(loader.loadTestsFromTestCase(TestPerformance))
    
    # Run tests with verbose output
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)
    print(f"Tests Run: {result.testsRun}")
    print(f"Successes: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    if result.wasSuccessful():
        print("\n✓ ALL TESTS PASSED")
    else:
        print("\n✗ SOME TESTS FAILED")
    
    print("=" * 80)
    print(f"Test Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    return result


if __name__ == '__main__':
    # Suppress warnings for cleaner output
    warnings.filterwarnings('ignore')
    
    # Run the test suite
    result = run_test_suite()
    
    # Exit with appropriate code
    sys.exit(0 if result.wasSuccessful() else 1)
