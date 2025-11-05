"""
Data loader module - imports from unified DataManager for backward compatibility.

This module provides backward compatibility by re-exporting the DataManager class.
All data loading functionality has been consolidated into the DataManager class.
"""

from .manager import DataManager

__all__ = ["DataManager"]
