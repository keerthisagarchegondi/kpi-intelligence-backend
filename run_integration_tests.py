#!/usr/bin/env python3
"""
Integration Test Runner for KPI Intelligence Backend
=====================================================
Production-level integration test suite that runs all tests in sequence,
generates reports, and provides exit codes for CI/CD pipelines.

Features:
- Run all unit and integration tests
- Generate detailed test reports
- Performance metrics
- Exit codes for CI/CD
- Color-coded console output
- Test coverage summary

Author: KPI Intelligence Team
Created: 2026-05-15
"""

import unittest
import sys
import os
import time
from io import StringIO
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import json

# ============================================
# CONFIGURATION
# ============================================

class TestConfig:
    """Configuration for test execution."""
    TEST_FILES = [
        'test_ingestion.py',
        'test_kpi_calculations.py',
        'test_retention.py',
        'test_anomaly_detection.py',
        'test_product_and_save.py'
    ]
    
    REPORT_DIR = Path('test_reports')
    VERBOSITY = 2
    FAILFAST = False


# ============================================
# COLOR CODES FOR CONSOLE OUTPUT
# ============================================

class Colors:
    """ANSI color codes for terminal output."""
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_header(text: str) -> None:
    """Print formatted header."""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{text.center(80)}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 80}{Colors.ENDC}\n")


def print_section(text: str) -> None:
    """Print formatted section header."""
    print(f"\n{Colors.CYAN}{Colors.BOLD}{text}{Colors.ENDC}")
    print(f"{Colors.CYAN}{'-' * len(text)}{Colors.ENDC}")


def print_success(text: str) -> None:
    """Print success message."""
    print(f"{Colors.GREEN}✓ {text}{Colors.ENDC}")


def print_error(text: str) -> None:
    """Print error message."""
    print(f"{Colors.RED}✗ {text}{Colors.ENDC}")


def print_warning(text: str) -> None:
    """Print warning message."""
    print(f"{Colors.YELLOW}⚠ {text}{Colors.ENDC}")


def print_info(text: str) -> None:
    """Print info message."""
    print(f"{Colors.BLUE}ℹ {text}{Colors.ENDC}")


# ============================================
# TEST DISCOVERY AND EXECUTION
# ============================================

class IntegrationTestRunner:
    """Manages test discovery, execution, and reporting."""
    
    def __init__(self):
        self.config = TestConfig()
        self.results = {}
        self.start_time = None
        self.end_time = None
        
    def setup_environment(self) -> bool:
        """Setup test environment and dependencies."""
        print_section("Environment Setup")
        
        try:
            # Create report directory
            self.config.REPORT_DIR.mkdir(exist_ok=True)
            print_success(f"Created report directory: {self.config.REPORT_DIR}")
            
            # Check for required test files
            missing_files = []
            for test_file in self.config.TEST_FILES:
                if not Path(test_file).exists():
                    missing_files.append(test_file)
            
            if missing_files:
                print_warning(f"Missing test files: {', '.join(missing_files)}")
                self.config.TEST_FILES = [f for f in self.config.TEST_FILES if f not in missing_files]
            
            print_success(f"Found {len(self.config.TEST_FILES)} test files")
            
            return True
            
        except Exception as e:
            print_error(f"Environment setup failed: {str(e)}")
            return False
    
    def run_test_file(self, test_file: str) -> Dict:
        """Run tests from a single test file."""
        print_section(f"Running: {test_file}")
        
        start_time = time.time()
        
        try:
            # Import test module
            module_name = test_file.replace('.py', '')
            spec = __import__(module_name)
            
            # Discover and run tests
            loader = unittest.TestLoader()
            suite = loader.loadTestsFromModule(spec)
            
            # Run tests with custom result collector
            stream = StringIO()
            runner = unittest.TextTestRunner(stream=stream, verbosity=self.config.VERBOSITY)
            result = runner.run(suite)
            
            end_time = time.time()
            duration = end_time - start_time
            
            # Collect results
            test_result = {
                'file': test_file,
                'tests_run': result.testsRun,
                'failures': len(result.failures),
                'errors': len(result.errors),
                'skipped': len(result.skipped),
                'success': result.wasSuccessful(),
                'duration': duration,
                'output': stream.getvalue()
            }
            
            # Print summary
            if test_result['success']:
                print_success(f"All {result.testsRun} tests passed in {duration:.2f}s")
            else:
                print_error(f"{result.testsRun} tests run, {len(result.failures)} failures, {len(result.errors)} errors")
            
            return test_result
            
        except Exception as e:
            print_error(f"Failed to run {test_file}: {str(e)}")
            return {
                'file': test_file,
                'tests_run': 0,
                'failures': 0,
                'errors': 1,
                'skipped': 0,
                'success': False,
                'duration': 0,
                'error': str(e)
            }
    
    def run_all_tests(self) -> bool:
        """Run all configured test files."""
        print_header("KPI Intelligence Backend - Integration Test Suite")
        
        if not self.setup_environment():
            return False
        
        self.start_time = time.time()
        
        # Run each test file
        for test_file in self.config.TEST_FILES:
            result = self.run_test_file(test_file)
            self.results[test_file] = result
            
            # Stop if failfast is enabled and test failed
            if self.config.FAILFAST and not result['success']:
                print_warning("Failfast enabled - stopping test execution")
                break
        
        self.end_time = time.time()
        
        return self.generate_report()
    
    def generate_report(self) -> bool:
        """Generate test execution report."""
        print_section("Test Execution Summary")
        
        total_tests = sum(r['tests_run'] for r in self.results.values())
        total_failures = sum(r['failures'] for r in self.results.values())
        total_errors = sum(r['errors'] for r in self.results.values())
        total_skipped = sum(r['skipped'] for r in self.results.values())
        total_duration = self.end_time - self.start_time
        
        all_passed = all(r['success'] for r in self.results.values())
        
        # Console summary
        print(f"\n{Colors.BOLD}Overall Results:{Colors.ENDC}")
        print(f"  Total Test Files: {len(self.results)}")
        print(f"  Total Tests: {total_tests}")
        print(f"  Passed: {total_tests - total_failures - total_errors}")
        print(f"  Failed: {total_failures}")
        print(f"  Errors: {total_errors}")
        print(f"  Skipped: {total_skipped}")
        print(f"  Duration: {total_duration:.2f}s")
        
        # Per-file results
        print(f"\n{Colors.BOLD}Per-File Results:{Colors.ENDC}")
        for test_file, result in self.results.items():
            status_icon = "✓" if result['success'] else "✗"
            status_color = Colors.GREEN if result['success'] else Colors.RED
            print(f"  {status_color}{status_icon} {test_file}: {result['tests_run']} tests in {result['duration']:.2f}s{Colors.ENDC}")
        
        # Generate JSON report
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'environment': os.getenv('ENVIRONMENT', 'test'),
            'summary': {
                'total_files': len(self.results),
                'total_tests': total_tests,
                'passed': total_tests - total_failures - total_errors,
                'failed': total_failures,
                'errors': total_errors,
                'skipped': total_skipped,
                'duration': total_duration,
                'success': all_passed
            },
            'results': self.results
        }
        
        report_file = self.config.REPORT_DIR / f"test_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        print_info(f"Detailed report saved to: {report_file}")
        
        # Final status
        print()
        if all_passed:
            print_success("All integration tests passed!")
            return True
        else:
            print_error("Some integration tests failed!")
            return False


# ============================================
# MAIN EXECUTION
# ============================================

def main():
    """Main entry point for test execution."""
    try:
        runner = IntegrationTestRunner()
        success = runner.run_all_tests()
        
        # Exit with appropriate code for CI/CD
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        print_warning("\nTest execution interrupted by user")
        sys.exit(130)
    except Exception as e:
        print_error(f"Unexpected error: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(2)


if __name__ == "__main__":
    main()
