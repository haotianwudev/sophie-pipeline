"""
Test utilities to simplify test setup and imports.
This file handles adding the project root to sys.path so all tests can properly import modules.
"""

import os
import sys

# Add project root to Python path
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
sys.path.insert(0, project_root)

# Export any shared test fixtures or utility functions here 