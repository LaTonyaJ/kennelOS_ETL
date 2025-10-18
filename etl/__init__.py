"""
KennelOS ETL Package

This package contains the Extract, Transform, Load (ETL) modules for KennelOS Analytics.

Modules:
- extract: Data extraction from various sources
- transform: Data cleaning and transformation
- load: Data loading to output destinations
"""

from .extract import DataExtractor
from .transform import DataTransformer
from .load import DataLoader

__version__ = "1.0.0"
__author__ = "KennelOS Analytics Team"

__all__ = ["DataExtractor", "DataTransformer", "DataLoader"]