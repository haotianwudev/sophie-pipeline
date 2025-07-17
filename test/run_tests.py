#!/usr/bin/env python3
import unittest
import os
import sys

# Add the project root to the path so we can import modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Discover and load all test files
loader = unittest.TestLoader()
test_suite = loader.discover(os.path.dirname(__file__), pattern='test_*.py')

# Run the tests
runner = unittest.TextTestRunner(verbosity=2)
result = runner.run(test_suite)

# Exit with error code if any tests failed
sys.exit(not result.wasSuccessful()) 