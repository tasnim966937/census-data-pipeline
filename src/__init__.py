"""
Census Data Pipeline

A comprehensive ETL pipeline for U.S. Census Bureau data.

Author: Mir Md Tasnim Alam
https://github.com/tasnim966937
"""

from .census_pipeline import (
    CensusPipeline,
    get_variable_metadata,
    search_variables
)
from .api_client import CensusAPIClient
from .geography import (
    GeographyManager,
    FIPS_CODES,
    STATE_NAME_TO_FIPS,
    parse_geoid,
    build_geoid
)
from .transformers import DataTransformer
from .exporters import DataExporter, PostGISExporter

__version__ = "1.0.0"
__author__ = "Mir Md Tasnim Alam"

__all__ = [
    "CensusPipeline",
    "CensusAPIClient",
    "GeographyManager",
    "DataTransformer",
    "DataExporter",
    "PostGISExporter",
    "FIPS_CODES",
    "STATE_NAME_TO_FIPS",
    "get_variable_metadata",
    "search_variables",
    "parse_geoid",
    "build_geoid"
]
