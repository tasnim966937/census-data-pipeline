"""
Census API Client - Low-level API wrapper for Census Bureau endpoints.

Author: Mir Md Tasnim Alam
"""

import time
import logging
from typing import Dict, List, Optional
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class CensusAPIClient:
    """
    Low-level client for Census Bureau APIs.
    
    Handles:
    - Request construction and parameter encoding
    - Rate limiting and retry logic
    - Error handling and response validation
    """
    
    BASE_URL = "https://api.census.gov/data"
    
    # Rate limiting: Census API has 500 requests/day without key
    RATE_LIMIT_DELAY = 0.5  # seconds between requests
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the API client.
        
        Args:
            api_key: Census API key for higher rate limits.
        """
        self.api_key = api_key
        self._last_request_time = 0
        
        # Configure session with retry logic
        self.session = requests.Session()
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504]
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
    
    def get_acs5(
        self,
        variables: List[str],
        geography: str,
        state: Optional[str] = None,
        county: Optional[str] = None,
        year: int = 2022
    ) -> List[List]:
        """
        Fetch ACS 5-Year estimates.
        
        Args:
            variables: List of variable codes to fetch.
            geography: Geographic level (state, county, tract, block group).
            state: State FIPS code (required for sub-state geographies).
            county: County FIPS code (optional filter).
            year: Data year.
            
        Returns:
            Raw API response as list of lists (first row is headers).
        """
        endpoint = f"{year}/acs/acs5"
        return self._make_request(endpoint, variables, geography, state, county)
    
    def get_acs1(
        self,
        variables: List[str],
        geography: str,
        state: Optional[str] = None,
        year: int = 2022
    ) -> List[List]:
        """Fetch ACS 1-Year estimates."""
        endpoint = f"{year}/acs/acs1"
        return self._make_request(endpoint, variables, geography, state)
    
    def get_decennial(
        self,
        variables: List[str],
        geography: str,
        state: Optional[str] = None,
        year: int = 2020
    ) -> List[List]:
        """
        Fetch Decennial Census data.
        
        Note: Variable naming differs between census years.
        - 2020: Uses PL (redistricting) or DHC (demographic) tables
        - 2010: SF1 or SF2 files
        """
        if year == 2020:
            endpoint = f"{year}/dec/dhc"
        elif year == 2010:
            endpoint = f"{year}/dec/sf1"
        else:
            endpoint = f"{year}/dec/sf1"
        
        return self._make_request(endpoint, variables, geography, state)
    
    def get_pep(
        self,
        variables: List[str],
        geography: str,
        year: int = 2022
    ) -> List[List]:
        """Fetch Population Estimates Program data."""
        endpoint = f"{year}/pep/population"
        return self._make_request(endpoint, variables, geography)
    
    def _make_request(
        self,
        endpoint: str,
        variables: List[str],
        geography: str,
        state: Optional[str] = None,
        county: Optional[str] = None
    ) -> List[List]:
        """
        Make API request with rate limiting.
        
        Args:
            endpoint: API endpoint path.
            variables: Variables to fetch.
            geography: Geographic level.
            state: State FIPS filter.
            county: County FIPS filter.
            
        Returns:
            Parsed JSON response.
        """
        # Rate limiting
        self._apply_rate_limit()
        
        # Build URL
        url = f"{self.BASE_URL}/{endpoint}"
        
        # Build parameters
        params = self._build_params(variables, geography, state, county)
        
        logger.debug(f"Requesting: {url}?{urlencode(params)}")
        
        try:
            response = self.session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 400:
                logger.error(f"Bad request - check variable codes: {response.text}")
            elif response.status_code == 404:
                logger.error(f"Endpoint not found - check year/product: {endpoint}")
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
    
    def _build_params(
        self,
        variables: List[str],
        geography: str,
        state: Optional[str] = None,
        county: Optional[str] = None
    ) -> Dict:
        """Build API request parameters."""
        params = {
            "get": ",".join(["NAME"] + variables),
        }
        
        # Add API key if available
        if self.api_key:
            params["key"] = self.api_key
        
        # Build geography clause
        params["for"] = self._build_for_clause(geography, county)
        
        # Add state filter for sub-state geographies
        if state and geography in ["county", "tract", "block group"]:
            params["in"] = f"state:{state}"
            if county and geography in ["tract", "block group"]:
                params["in"] += f" county:{county}"
        
        return params
    
    def _build_for_clause(self, geography: str, county: Optional[str] = None) -> str:
        """Build the 'for' clause for geographic filtering."""
        geo_mapping = {
            "state": "state:*",
            "county": "county:*",
            "tract": "tract:*",
            "block group": "block group:*",
            "place": "place:*",
            "zcta": "zip code tabulation area:*",
            "congressional district": "congressional district:*"
        }
        
        clause = geo_mapping.get(geography)
        if not clause:
            raise ValueError(f"Unsupported geography: {geography}")
        
        return clause
    
    def _apply_rate_limit(self) -> None:
        """Apply rate limiting between requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < self.RATE_LIMIT_DELAY:
            time.sleep(self.RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()


class CensusAPIError(Exception):
    """Custom exception for Census API errors."""
    pass
