"""
Geography Manager - TIGER/Line boundaries and FIPS code utilities.

Author: Mir Md Tasnim Alam
"""

import logging
from pathlib import Path
from typing import Optional
import zipfile
import io

import geopandas as gpd
import requests

logger = logging.getLogger(__name__)


# State FIPS codes
FIPS_CODES = {
    "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
    "06": "California", "08": "Colorado", "09": "Connecticut", "10": "Delaware",
    "11": "District of Columbia", "12": "Florida", "13": "Georgia", "15": "Hawaii",
    "16": "Idaho", "17": "Illinois", "18": "Indiana", "19": "Iowa",
    "20": "Kansas", "21": "Kentucky", "22": "Louisiana", "23": "Maine",
    "24": "Maryland", "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
    "28": "Mississippi", "29": "Missouri", "30": "Montana", "31": "Nebraska",
    "32": "Nevada", "33": "New Hampshire", "34": "New Jersey", "35": "New Mexico",
    "36": "New York", "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
    "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania", "44": "Rhode Island",
    "45": "South Carolina", "46": "South Dakota", "47": "Tennessee", "48": "Texas",
    "49": "Utah", "50": "Vermont", "51": "Virginia", "53": "Washington",
    "54": "West Virginia", "55": "Wisconsin", "56": "Wyoming", "72": "Puerto Rico"
}

# State name to FIPS lookup
STATE_NAME_TO_FIPS = {v.lower(): k for k, v in FIPS_CODES.items()}


class GeographyManager:
    """
    Manager for geographic boundaries and FIPS code utilities.
    
    Handles:
    - TIGER/Line shapefile downloading
    - Cartographic boundary files
    - FIPS code lookups
    - Geometry caching
    """
    
    TIGER_BASE_URL = "https://www2.census.gov/geo/tiger"
    CB_BASE_URL = "https://www2.census.gov/geo/tiger/GENZ{year}/shp"
    
    def __init__(self, cache_dir: Optional[Path] = None):
        """
        Initialize geography manager.
        
        Args:
            cache_dir: Directory for caching downloaded shapefiles.
        """
        self.cache_dir = cache_dir or Path.home() / ".census_cache" / "tiger"
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def get_tiger_boundaries(
        self,
        geography: str,
        year: int = 2022,
        resolution: str = "500k",
        state: Optional[str] = None
    ) -> gpd.GeoDataFrame:
        """
        Download and load TIGER/Line cartographic boundaries.
        
        Args:
            geography: Geographic level (state, county, tract, etc.)
            year: TIGER vintage year
            resolution: Cartographic boundary resolution (500k, 5m, 20m)
            state: State FIPS for tract/block group (required for sub-county)
            
        Returns:
            GeoDataFrame with boundaries
        """
        cache_path = self._get_cache_path(geography, year, resolution, state)
        
        if cache_path.exists():
            logger.info(f"Loading cached boundaries: {cache_path}")
            return gpd.read_file(cache_path)
        
        # Download boundaries
        url = self._build_tiger_url(geography, year, resolution, state)
        gdf = self._download_shapefile(url)
        
        # Cache for future use
        gdf.to_file(cache_path)
        logger.info(f"Cached boundaries: {cache_path}")
        
        return gdf
    
    def get_state_fips(self, state: str) -> str:
        """
        Get FIPS code for a state.
        
        Args:
            state: State name, abbreviation, or FIPS code.
            
        Returns:
            Two-digit FIPS code.
        """
        # Already a FIPS code
        if state in FIPS_CODES:
            return state
        
        # Check by name
        state_lower = state.lower()
        if state_lower in STATE_NAME_TO_FIPS:
            return STATE_NAME_TO_FIPS[state_lower]
        
        # Check abbreviations
        abbrev_to_fips = {
            "al": "01", "ak": "02", "az": "04", "ar": "05", "ca": "06",
            "co": "08", "ct": "09", "de": "10", "dc": "11", "fl": "12",
            "ga": "13", "hi": "15", "id": "16", "il": "17", "in": "18",
            "ia": "19", "ks": "20", "ky": "21", "la": "22", "me": "23",
            "md": "24", "ma": "25", "mi": "26", "mn": "27", "ms": "28",
            "mo": "29", "mt": "30", "ne": "31", "nv": "32", "nh": "33",
            "nj": "34", "nm": "35", "ny": "36", "nc": "37", "nd": "38",
            "oh": "39", "ok": "40", "or": "41", "pa": "42", "ri": "44",
            "sc": "45", "sd": "46", "tn": "47", "tx": "48", "ut": "49",
            "vt": "50", "va": "51", "wa": "53", "wv": "54", "wi": "55",
            "wy": "56", "pr": "72"
        }
        
        if state_lower in abbrev_to_fips:
            return abbrev_to_fips[state_lower]
        
        raise ValueError(f"Unknown state: {state}")
    
    def get_county_fips(self, state: str, county: str) -> str:
        """
        Get county FIPS code.
        
        This requires downloading the county list for the state.
        For a full implementation, consider using a local FIPS database.
        """
        # This would typically use a local database or API lookup
        raise NotImplementedError("County FIPS lookup requires additional data")
    
    def _build_tiger_url(
        self,
        geography: str,
        year: int,
        resolution: str,
        state: Optional[str] = None
    ) -> str:
        """Build URL for TIGER cartographic boundary file."""
        
        # Mapping of geography to file naming convention
        file_codes = {
            "state": "state",
            "county": "county",
            "tract": "tract",
            "block group": "bg",
            "place": "place",
            "zcta": "zcta520"  # 2020 ZCTAs
        }
        
        geo_code = file_codes.get(geography)
        if not geo_code:
            raise ValueError(f"Unsupported geography for TIGER: {geography}")
        
        # National vs state-level files
        if geography in ["state", "county"]:
            # National file
            filename = f"cb_{year}_us_{geo_code}_{resolution}.zip"
        else:
            # State-level file
            if not state:
                raise ValueError(f"State required for {geography} boundaries")
            filename = f"cb_{year}_{state}_{geo_code}_{resolution}.zip"
        
        url = f"{self.CB_BASE_URL.format(year=year)}/{filename}"
        return url
    
    def _get_cache_path(
        self,
        geography: str,
        year: int,
        resolution: str,
        state: Optional[str]
    ) -> Path:
        """Get cache file path for boundaries."""
        if state:
            filename = f"{geography}_{year}_{resolution}_{state}.gpkg"
        else:
            filename = f"{geography}_{year}_{resolution}.gpkg"
        
        return self.cache_dir / filename
    
    def _download_shapefile(self, url: str) -> gpd.GeoDataFrame:
        """Download and extract shapefile from URL."""
        logger.info(f"Downloading: {url}")
        
        response = requests.get(url, timeout=60)
        response.raise_for_status()
        
        # Extract from zip
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            # Find the shapefile
            shp_files = [f for f in zf.namelist() if f.endswith('.shp')]
            if not shp_files:
                raise ValueError("No shapefile found in archive")
            
            # Read directly from zip
            gdf = gpd.read_file(f"zip+{url}!{shp_files[0]}")
        
        return gdf


def parse_geoid(geoid: str) -> dict:
    """
    Parse a GEOID into component FIPS codes.
    
    Args:
        geoid: Full GEOID string
        
    Returns:
        Dict with state, county, tract, block_group as applicable
    """
    result = {}
    
    if len(geoid) >= 2:
        result["state"] = geoid[:2]
    if len(geoid) >= 5:
        result["county"] = geoid[2:5]
    if len(geoid) >= 11:
        result["tract"] = geoid[5:11]
    if len(geoid) >= 12:
        result["block_group"] = geoid[11:12]
    
    return result


def build_geoid(
    state: str,
    county: Optional[str] = None,
    tract: Optional[str] = None,
    block_group: Optional[str] = None
) -> str:
    """
    Build a GEOID from component FIPS codes.
    
    Args:
        state: 2-digit state FIPS
        county: 3-digit county FIPS
        tract: 6-digit tract code
        block_group: 1-digit block group code
        
    Returns:
        Concatenated GEOID
    """
    geoid = state
    if county:
        geoid += county
    if tract:
        geoid += tract
    if block_group:
        geoid += block_group
    
    return geoid
