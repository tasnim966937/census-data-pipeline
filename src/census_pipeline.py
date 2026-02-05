"""
Census Data Pipeline - Main Pipeline Class

A comprehensive ETL pipeline for U.S. Census Bureau data at multiple geographic levels.

Author: Mir Md Tasnim Alam
"""

import os
import logging
from typing import Dict, List, Optional, Union
from pathlib import Path

import pandas as pd
import geopandas as gpd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from .api_client import CensusAPIClient
from .geography import GeographyManager, FIPS_CODES
from .transformers import DataTransformer
from .exporters import DataExporter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CensusPipeline:
    """
    Main pipeline class for fetching, processing, and exporting Census data.
    
    Supports:
    - American Community Survey (ACS) 1-year and 5-year estimates
    - Decennial Census (2000, 2010, 2020)
    - Population Estimates Program (PEP)
    - TIGER/Line geometry integration
    
    Example:
        >>> pipeline = CensusPipeline(api_key="your_key")
        >>> data = pipeline.fetch_acs5(
        ...     variables=["B01003_001E"],
        ...     geography="tract",
        ...     state="39"
        ... )
    """
    
    # Census variable groups for common analyses
    DEMOGRAPHIC_VARS = {
        "B01003_001E": "total_population",
        "B01002_001E": "median_age",
        "B02001_002E": "white_alone",
        "B02001_003E": "black_alone",
        "B03003_003E": "hispanic_latino",
    }
    
    ECONOMIC_VARS = {
        "B19013_001E": "median_household_income",
        "B19301_001E": "per_capita_income",
        "B17001_002E": "below_poverty_level",
        "B23025_005E": "unemployed",
    }
    
    HOUSING_VARS = {
        "B25001_001E": "total_housing_units",
        "B25002_002E": "occupied_units",
        "B25002_003E": "vacant_units",
        "B25077_001E": "median_home_value",
        "B25064_001E": "median_gross_rent",
    }
    
    def __init__(
        self,
        api_key: Optional[str] = None,
        cache_dir: Optional[str] = None,
        parallel_workers: int = 4
    ):
        """
        Initialize the Census Pipeline.
        
        Args:
            api_key: Census API key. If None, reads from CENSUS_API_KEY env var.
            cache_dir: Directory for caching downloaded data.
            parallel_workers: Number of parallel workers for batch processing.
        """
        self.api_key = api_key or os.environ.get("CENSUS_API_KEY")
        if not self.api_key:
            logger.warning("No API key provided. Some endpoints may be rate-limited.")
        
        self.cache_dir = Path(cache_dir) if cache_dir else Path.home() / ".census_cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.parallel_workers = parallel_workers
        
        # Initialize components
        self.api_client = CensusAPIClient(self.api_key)
        self.geography = GeographyManager(self.cache_dir)
        self.transformer = DataTransformer()
        self.exporter = DataExporter()
        
        logger.info(f"Census Pipeline initialized. Cache dir: {self.cache_dir}")
    
    def fetch_acs5(
        self,
        variables: Union[List[str], Dict[str, str]],
        geography: str,
        state: Optional[str] = None,
        county: Optional[str] = None,
        year: int = 2022,
        include_moe: bool = False
    ) -> gpd.GeoDataFrame:
        """
        Fetch American Community Survey 5-Year estimates.
        
        Args:
            variables: List of variable codes or dict mapping codes to names.
            geography: Geographic level (state, county, tract, block group).
            state: State FIPS code (required for tract/block group).
            county: County FIPS code (optional filter).
            year: Data year (2009-2022).
            include_moe: Include margin of error variables.
            
        Returns:
            GeoDataFrame with requested variables and geometries.
        
        Example:
            >>> data = pipeline.fetch_acs5(
            ...     variables=["B01003_001E", "B19013_001E"],
            ...     geography="tract",
            ...     state="39",  # Ohio
            ...     year=2022
            ... )
        """
        logger.info(f"Fetching ACS 5-Year {year} data at {geography} level")
        
        # Normalize variables to dict
        if isinstance(variables, list):
            var_dict = {v: v for v in variables}
        else:
            var_dict = variables
        
        # Add MOE variables if requested
        if include_moe:
            moe_vars = {
                v.replace("E", "M"): f"{name}_moe" 
                for v, name in var_dict.items() 
                if v.endswith("E")
            }
            var_dict.update(moe_vars)
        
        # Build API request
        var_list = list(var_dict.keys())
        
        # Fetch data from API
        raw_data = self.api_client.get_acs5(
            variables=var_list,
            geography=geography,
            state=state,
            county=county,
            year=year
        )
        
        # Convert to DataFrame
        df = self._parse_api_response(raw_data, var_dict)
        
        # Add GEOID for joining
        df = self._create_geoid(df, geography)
        
        logger.info(f"Fetched {len(df)} records")
        return df
    
    def fetch_acs1(
        self,
        variables: Union[List[str], Dict[str, str]],
        geography: str,
        state: Optional[str] = None,
        year: int = 2022
    ) -> pd.DataFrame:
        """
        Fetch American Community Survey 1-Year estimates.
        
        Note: ACS 1-Year only available for areas with 65,000+ population.
        """
        logger.info(f"Fetching ACS 1-Year {year} data at {geography} level")
        
        if isinstance(variables, list):
            var_dict = {v: v for v in variables}
        else:
            var_dict = variables
        
        raw_data = self.api_client.get_acs1(
            variables=list(var_dict.keys()),
            geography=geography,
            state=state,
            year=year
        )
        
        df = self._parse_api_response(raw_data, var_dict)
        df = self._create_geoid(df, geography)
        
        return df
    
    def fetch_decennial(
        self,
        variables: Union[List[str], Dict[str, str]],
        geography: str,
        state: Optional[str] = None,
        year: int = 2020
    ) -> pd.DataFrame:
        """
        Fetch Decennial Census data.
        
        Args:
            year: Census year (2000, 2010, 2020)
        """
        if year not in [2000, 2010, 2020]:
            raise ValueError("Decennial Census only available for 2000, 2010, 2020")
        
        logger.info(f"Fetching Decennial Census {year} data at {geography} level")
        
        if isinstance(variables, list):
            var_dict = {v: v for v in variables}
        else:
            var_dict = variables
        
        raw_data = self.api_client.get_decennial(
            variables=list(var_dict.keys()),
            geography=geography,
            state=state,
            year=year
        )
        
        df = self._parse_api_response(raw_data, var_dict)
        df = self._create_geoid(df, geography)
        
        return df
    
    def join_tiger_geometries(
        self,
        df: pd.DataFrame,
        geography: str,
        year: int = 2022,
        resolution: str = "500k"
    ) -> gpd.GeoDataFrame:
        """
        Join TIGER/Line geometries to Census data.
        
        Args:
            df: DataFrame with GEOID column.
            geography: Geographic level for geometry matching.
            year: TIGER/Line vintage year.
            resolution: Cartographic boundary resolution (500k, 5m, 20m).
            
        Returns:
            GeoDataFrame with geometry column added.
        """
        logger.info(f"Joining TIGER/Line {geography} geometries (year={year})")
        
        # Download or load cached TIGER geometries
        tiger_gdf = self.geography.get_tiger_boundaries(
            geography=geography,
            year=year,
            resolution=resolution
        )
        
        # Standardize GEOID column names
        geoid_col = self._get_tiger_geoid_col(geography)
        tiger_gdf = tiger_gdf.rename(columns={geoid_col: "GEOID"})
        
        # Perform join
        gdf = tiger_gdf[["GEOID", "geometry"]].merge(
            df, on="GEOID", how="right"
        )
        
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs="EPSG:4326")
        
        logger.info(f"Joined geometries for {len(gdf)} features")
        return gdf
    
    def fetch_batch_states(
        self,
        variables: Union[List[str], Dict[str, str]],
        geography: str,
        states: List[str],
        year: int = 2022,
        data_product: str = "acs5"
    ) -> gpd.GeoDataFrame:
        """
        Fetch data for multiple states in parallel.
        
        Args:
            states: List of state FIPS codes, or ["*"] for all states.
        """
        if states == ["*"]:
            states = list(FIPS_CODES.keys())
        
        logger.info(f"Batch fetching {len(states)} states with {self.parallel_workers} workers")
        
        results = []
        
        with ThreadPoolExecutor(max_workers=self.parallel_workers) as executor:
            futures = {
                executor.submit(
                    self._fetch_single_state,
                    variables, geography, state, year, data_product
                ): state
                for state in states
            }
            
            for future in as_completed(futures):
                state = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                    logger.info(f"Completed state {state}")
                except Exception as e:
                    logger.error(f"Error fetching state {state}: {e}")
        
        # Combine results
        combined = pd.concat(results, ignore_index=True)
        logger.info(f"Batch fetch complete: {len(combined)} total records")
        
        return combined
    
    def export(
        self,
        data: Union[pd.DataFrame, gpd.GeoDataFrame],
        output: str,
        format: str = "geopackage",
        layer_name: Optional[str] = None
    ) -> None:
        """
        Export data to various formats.
        
        Args:
            data: DataFrame or GeoDataFrame to export.
            output: Output file path.
            format: Output format (csv, geopackage, geojson, shapefile, parquet).
            layer_name: Layer name for GeoPackage output.
        """
        self.exporter.export(data, output, format, layer_name)
        logger.info(f"Exported to {output}")
    
    def _parse_api_response(
        self,
        response: List[List],
        var_dict: Dict[str, str]
    ) -> pd.DataFrame:
        """Parse Census API JSON response into DataFrame."""
        if not response:
            return pd.DataFrame()
        
        headers = response[0]
        data = response[1:]
        
        df = pd.DataFrame(data, columns=headers)
        
        # Rename variables to friendly names
        df = df.rename(columns=var_dict)
        
        # Convert numeric columns
        for col in var_dict.values():
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        
        return df
    
    def _create_geoid(self, df: pd.DataFrame, geography: str) -> pd.DataFrame:
        """Create standardized GEOID from component FIPS codes."""
        if geography == "state":
            df["GEOID"] = df["state"]
        elif geography == "county":
            df["GEOID"] = df["state"] + df["county"]
        elif geography == "tract":
            df["GEOID"] = df["state"] + df["county"] + df["tract"]
        elif geography == "block group":
            df["GEOID"] = df["state"] + df["county"] + df["tract"] + df["block group"]
        
        return df
    
    def _get_tiger_geoid_col(self, geography: str) -> str:
        """Get TIGER/Line GEOID column name for geography level."""
        mapping = {
            "state": "STATEFP",
            "county": "GEOID",
            "tract": "GEOID",
            "block group": "GEOID"
        }
        return mapping.get(geography, "GEOID")
    
    def _fetch_single_state(
        self,
        variables: Union[List[str], Dict[str, str]],
        geography: str,
        state: str,
        year: int,
        data_product: str
    ) -> pd.DataFrame:
        """Fetch data for a single state (used in parallel processing)."""
        if data_product == "acs5":
            return self.fetch_acs5(variables, geography, state, year=year)
        elif data_product == "acs1":
            return self.fetch_acs1(variables, geography, state, year=year)
        elif data_product == "decennial":
            return self.fetch_decennial(variables, geography, state, year=year)
        else:
            raise ValueError(f"Unknown data product: {data_product}")


def get_variable_metadata(variable_code: str, year: int = 2022) -> Dict:
    """
    Fetch metadata for a Census variable.
    
    Args:
        variable_code: Census variable code (e.g., "B01003_001E")
        year: Data year
        
    Returns:
        Dict with variable label, concept, and predicateType
    """
    url = f"https://api.census.gov/data/{year}/acs/acs5/variables/{variable_code}.json"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


def search_variables(keyword: str, year: int = 2022) -> pd.DataFrame:
    """
    Search for Census variables by keyword.
    
    Args:
        keyword: Search term
        year: Data year
        
    Returns:
        DataFrame with matching variables
    """
    url = f"https://api.census.gov/data/{year}/acs/acs5/variables.json"
    response = requests.get(url)
    response.raise_for_status()
    
    variables = response.json()["variables"]
    
    results = []
    keyword_lower = keyword.lower()
    
    for var_id, var_info in variables.items():
        label = var_info.get("label", "")
        concept = var_info.get("concept", "")
        
        if keyword_lower in label.lower() or keyword_lower in concept.lower():
            results.append({
                "variable": var_id,
                "label": label,
                "concept": concept
            })
    
    return pd.DataFrame(results)
