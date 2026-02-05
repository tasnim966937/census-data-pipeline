"""
Example: Tract-Level Demographic Analysis

This example demonstrates fetching and analyzing Census tract-level data
for Ohio, including joining geometries and exporting to GeoPackage.

Author: Mir Md Tasnim Alam
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.census_pipeline import CensusPipeline

def main():
    """Run tract-level analysis for Ohio."""
    
    # Initialize pipeline
    # Get your API key from: https://api.census.gov/data/key_signup.html
    pipeline = CensusPipeline(
        api_key=os.environ.get("CENSUS_API_KEY")
    )
    
    # Define variables of interest
    variables = {
        # Demographics
        "B01003_001E": "total_population",
        "B01002_001E": "median_age",
        "B02001_002E": "white_alone",
        "B02001_003E": "black_alone",
        "B03003_003E": "hispanic_latino",
        
        # Economics
        "B19013_001E": "median_household_income",
        "B19301_001E": "per_capita_income",
        "B17001_002E": "below_poverty_level",
        
        # Housing
        "B25001_001E": "total_housing_units",
        "B25077_001E": "median_home_value",
        "B25002_003E": "vacant_units",
        
        # Education
        "B15003_022E": "bachelors_degree",
        "B15003_023E": "masters_degree",
        "B15003_025E": "doctorate_degree"
    }
    
    print("Fetching ACS 5-Year tract data for Ohio...")
    
    # Fetch data
    ohio_tracts = pipeline.fetch_acs5(
        variables=variables,
        geography="tract",
        state="39",  # Ohio FIPS code
        year=2022,
        include_moe=False
    )
    
    print(f"Retrieved {len(ohio_tracts)} tracts")
    
    # Calculate derived metrics
    print("Calculating derived metrics...")
    
    # Race percentages
    ohio_tracts["pct_white"] = (
        ohio_tracts["white_alone"] / ohio_tracts["total_population"] * 100
    )
    ohio_tracts["pct_black"] = (
        ohio_tracts["black_alone"] / ohio_tracts["total_population"] * 100
    )
    ohio_tracts["pct_hispanic"] = (
        ohio_tracts["hispanic_latino"] / ohio_tracts["total_population"] * 100
    )
    
    # Poverty rate
    ohio_tracts["poverty_rate"] = (
        ohio_tracts["below_poverty_level"] / ohio_tracts["total_population"] * 100
    )
    
    # Vacancy rate
    ohio_tracts["vacancy_rate"] = (
        ohio_tracts["vacant_units"] / ohio_tracts["total_housing_units"] * 100
    )
    
    # College education rate
    ohio_tracts["college_educated"] = (
        ohio_tracts["bachelors_degree"] + 
        ohio_tracts["masters_degree"] + 
        ohio_tracts["doctorate_degree"]
    )
    ohio_tracts["pct_college"] = (
        ohio_tracts["college_educated"] / ohio_tracts["total_population"] * 100
    )
    
    # Join TIGER geometries
    print("Joining TIGER/Line geometries...")
    ohio_tracts_geo = pipeline.join_tiger_geometries(
        ohio_tracts,
        geography="tract",
        year=2022
    )
    
    # Export results
    output_file = "ohio_tracts_2022.gpkg"
    print(f"Exporting to {output_file}...")
    
    pipeline.export(
        ohio_tracts_geo,
        output=output_file,
        format="geopackage",
        layer_name="ohio_tracts_acs5_2022"
    )
    
    # Print summary statistics
    print("\n" + "="*50)
    print("OHIO TRACT SUMMARY STATISTICS (ACS 2022)")
    print("="*50)
    
    print(f"\nTotal Tracts: {len(ohio_tracts_geo):,}")
    print(f"Total Population: {ohio_tracts_geo['total_population'].sum():,.0f}")
    
    print(f"\nMedian Household Income:")
    print(f"  Mean: ${ohio_tracts_geo['median_household_income'].mean():,.0f}")
    print(f"  Median: ${ohio_tracts_geo['median_household_income'].median():,.0f}")
    print(f"  Min: ${ohio_tracts_geo['median_household_income'].min():,.0f}")
    print(f"  Max: ${ohio_tracts_geo['median_household_income'].max():,.0f}")
    
    print(f"\nPoverty Rate:")
    print(f"  Mean: {ohio_tracts_geo['poverty_rate'].mean():.1f}%")
    print(f"  Max: {ohio_tracts_geo['poverty_rate'].max():.1f}%")
    
    print(f"\nMedian Home Value:")
    print(f"  Mean: ${ohio_tracts_geo['median_home_value'].mean():,.0f}")
    print(f"  Median: ${ohio_tracts_geo['median_home_value'].median():,.0f}")
    
    print(f"\nCollege Education Rate:")
    print(f"  Mean: {ohio_tracts_geo['pct_college'].mean():.1f}%")
    
    print("\n" + "="*50)
    print(f"Output saved to: {output_file}")
    print("="*50)


if __name__ == "__main__":
    main()
