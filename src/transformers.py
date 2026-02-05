"""
Data Transformers - Cleaning, normalization, and derived variable calculation.

Author: Mir Md Tasnim Alam
"""

import logging
from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd
import geopandas as gpd

logger = logging.getLogger(__name__)


class DataTransformer:
    """
    Transformer class for Census data cleaning and enrichment.
    
    Provides methods for:
    - Missing value handling
    - Derived variable calculation
    - Rate and percentage computation
    - Data normalization
    - Index creation
    """
    
    # Census Bureau codes for missing/suppressed data
    MISSING_CODES = {
        -666666666: "too few sample observations",
        -999999999: "no sample observations",
        -888888888: "not applicable",
        -222222222: "too many sample cases",
        -333333333: "median in top/bottom interval"
    }
    
    def __init__(self):
        """Initialize transformer."""
        pass
    
    def clean_missing_values(
        self,
        df: pd.DataFrame,
        strategy: str = "nan",
        fill_value: Optional[float] = None
    ) -> pd.DataFrame:
        """
        Handle Census missing value codes.
        
        Args:
            df: Input DataFrame
            strategy: 'nan' (convert to NaN), 'fill' (fill with value), 'drop' (drop rows)
            fill_value: Value to use when strategy='fill'
            
        Returns:
            Cleaned DataFrame
        """
        df = df.copy()
        
        # Replace Census missing codes
        for code in self.MISSING_CODES.keys():
            df = df.replace(code, np.nan)
        
        if strategy == "nan":
            pass  # Already converted
        elif strategy == "fill":
            df = df.fillna(fill_value)
        elif strategy == "drop":
            df = df.dropna()
        
        return df
    
    def calculate_rates(
        self,
        df: pd.DataFrame,
        numerator: str,
        denominator: str,
        rate_name: str,
        per: int = 100,
        handle_zero: str = "nan"
    ) -> pd.DataFrame:
        """
        Calculate rates (e.g., percentage, per 1000, etc.)
        
        Args:
            df: Input DataFrame
            numerator: Column name for numerator
            denominator: Column name for denominator
            rate_name: Name for new rate column
            per: Rate multiplier (100 for percent, 1000 for per-1000, etc.)
            handle_zero: How to handle zero denominators ('nan', 'zero', 'inf')
            
        Returns:
            DataFrame with rate column added
        """
        df = df.copy()
        
        # Calculate rate
        with np.errstate(divide='ignore', invalid='ignore'):
            rate = (df[numerator] / df[denominator]) * per
        
        # Handle zeros/infinites
        if handle_zero == "nan":
            rate = rate.replace([np.inf, -np.inf], np.nan)
        elif handle_zero == "zero":
            rate = rate.replace([np.inf, -np.inf], 0)
        
        df[rate_name] = rate
        
        return df
    
    def calculate_derived_demographics(
        self,
        df: pd.DataFrame,
        total_pop_col: str = "total_population"
    ) -> pd.DataFrame:
        """
        Calculate common derived demographic variables.
        
        Calculates percentages for race, age, education, etc. if source
        columns are present.
        """
        df = df.copy()
        
        # Race percentages
        race_cols = {
            "white_alone": "pct_white",
            "black_alone": "pct_black",
            "hispanic_latino": "pct_hispanic"
        }
        
        for source, target in race_cols.items():
            if source in df.columns:
                df = self.calculate_rates(
                    df, source, total_pop_col, target, per=100
                )
        
        # Economic indicators
        if "below_poverty_level" in df.columns:
            df = self.calculate_rates(
                df, "below_poverty_level", total_pop_col, "poverty_rate", per=100
            )
        
        if all(c in df.columns for c in ["unemployed", "labor_force"]):
            df = self.calculate_rates(
                df, "unemployed", "labor_force", "unemployment_rate", per=100
            )
        
        return df
    
    def normalize_column(
        self,
        df: pd.DataFrame,
        column: str,
        method: str = "minmax",
        output_col: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Normalize a column.
        
        Args:
            df: Input DataFrame
            column: Column to normalize
            method: Normalization method ('minmax', 'zscore', 'robust')
            output_col: Name for normalized column (default: column_normalized)
        """
        df = df.copy()
        output_col = output_col or f"{column}_normalized"
        
        values = df[column]
        
        if method == "minmax":
            # Scale to 0-1
            df[output_col] = (values - values.min()) / (values.max() - values.min())
        
        elif method == "zscore":
            # Z-score normalization
            df[output_col] = (values - values.mean()) / values.std()
        
        elif method == "robust":
            # Robust scaling using median and IQR
            median = values.median()
            q1, q3 = values.quantile([0.25, 0.75])
            iqr = q3 - q1
            df[output_col] = (values - median) / iqr
        
        return df
    
    def create_index(
        self,
        df: pd.DataFrame,
        components: Dict[str, float],
        index_name: str,
        normalize: bool = True
    ) -> pd.DataFrame:
        """
        Create a composite index from multiple variables.
        
        Args:
            df: Input DataFrame
            components: Dict mapping column names to weights
            index_name: Name for the index column
            normalize: Whether to normalize components before combining
            
        Returns:
            DataFrame with index column added
        """
        df = df.copy()
        
        # Normalize components if requested
        if normalize:
            for col in components.keys():
                df = self.normalize_column(df, col, method="minmax")
                col_norm = f"{col}_normalized"
        
        # Calculate weighted sum
        index_values = pd.Series(0.0, index=df.index)
        total_weight = sum(components.values())
        
        for col, weight in components.items():
            if normalize:
                col_use = f"{col}_normalized"
            else:
                col_use = col
            
            index_values += df[col_use] * (weight / total_weight)
        
        df[index_name] = index_values
        
        return df
    
    def calculate_change(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        variable: str,
        join_on: str = "GEOID",
        absolute: bool = True,
        percent: bool = True
    ) -> pd.DataFrame:
        """
        Calculate change between two time periods.
        
        Args:
            df1: Earlier period data
            df2: Later period data
            variable: Variable to compare
            join_on: Column to join on
            absolute: Include absolute change
            percent: Include percent change
            
        Returns:
            DataFrame with change columns
        """
        merged = df1[[join_on, variable]].merge(
            df2[[join_on, variable]],
            on=join_on,
            suffixes=("_t1", "_t2")
        )
        
        if absolute:
            merged[f"{variable}_change"] = (
                merged[f"{variable}_t2"] - merged[f"{variable}_t1"]
            )
        
        if percent:
            with np.errstate(divide='ignore', invalid='ignore'):
                merged[f"{variable}_pct_change"] = (
                    (merged[f"{variable}_t2"] - merged[f"{variable}_t1"]) /
                    merged[f"{variable}_t1"] * 100
                )
            merged[f"{variable}_pct_change"] = merged[f"{variable}_pct_change"].replace(
                [np.inf, -np.inf], np.nan
            )
        
        return merged
    
    def aggregate_to_geography(
        self,
        df: pd.DataFrame,
        from_geo: str,
        to_geo: str,
        agg_columns: Dict[str, str]
    ) -> pd.DataFrame:
        """
        Aggregate data from finer to coarser geography.
        
        Args:
            df: Input DataFrame with GEOID
            from_geo: Source geography level
            to_geo: Target geography level
            agg_columns: Dict mapping columns to aggregation functions
            
        Returns:
            Aggregated DataFrame
        """
        df = df.copy()
        
        # Create target GEOID
        geoid_lengths = {
            "state": 2,
            "county": 5,
            "tract": 11,
            "block_group": 12
        }
        
        target_len = geoid_lengths.get(to_geo)
        if not target_len:
            raise ValueError(f"Unknown target geography: {to_geo}")
        
        df["target_geoid"] = df["GEOID"].str[:target_len]
        
        # Aggregate
        result = df.groupby("target_geoid").agg(agg_columns).reset_index()
        result = result.rename(columns={"target_geoid": "GEOID"})
        
        return result
