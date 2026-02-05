"""
Example: Multi-State County Comparison

This example demonstrates batch processing multiple states,
calculating economic indicators, and creating comparison visualizations.

Author: Mir Md Tasnim Alam
"""

import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.census_pipeline import CensusPipeline
from src.geography import FIPS_CODES

import pandas as pd


def main():
    """Compare economic indicators across Midwest states."""
    
    pipeline = CensusPipeline(
        api_key=os.environ.get("CENSUS_API_KEY"),
        parallel_workers=4
    )
    
    # Define Midwest states to compare
    midwest_states = {
        "39": "Ohio",
        "18": "Indiana", 
        "26": "Michigan",
        "17": "Illinois",
        "55": "Wisconsin",
        "27": "Minnesota"
    }
    
    # Economic and demographic variables
    variables = {
        "B01003_001E": "total_population",
        "B19013_001E": "median_household_income",
        "B19301_001E": "per_capita_income",
        "B23025_003E": "labor_force",
        "B23025_005E": "unemployed",
        "B25077_001E": "median_home_value",
        "B17001_002E": "below_poverty"
    }
    
    print("Fetching county-level data for Midwest states...")
    print(f"States: {', '.join(midwest_states.values())}")
    
    # Batch fetch all states
    all_counties = pipeline.fetch_batch_states(
        variables=variables,
        geography="county",
        states=list(midwest_states.keys()),
        year=2022,
        data_product="acs5"
    )
    
    print(f"Retrieved {len(all_counties)} counties")
    
    # Add state names
    all_counties["state_name"] = all_counties["state"].map(midwest_states)
    
    # Calculate derived metrics
    all_counties["unemployment_rate"] = (
        all_counties["unemployed"] / all_counties["labor_force"] * 100
    )
    all_counties["poverty_rate"] = (
        all_counties["below_poverty"] / all_counties["total_population"] * 100
    )
    
    # State-level summary
    print("\n" + "="*70)
    print("MIDWEST STATE COMPARISON - COUNTY-LEVEL AGGREGATES (ACS 2022)")
    print("="*70)
    
    state_summary = all_counties.groupby("state_name").agg({
        "total_population": "sum",
        "median_household_income": "median",
        "median_home_value": "median",
        "unemployment_rate": "mean",
        "poverty_rate": "mean",
        "GEOID": "count"
    }).rename(columns={"GEOID": "num_counties"})
    
    state_summary = state_summary.sort_values("total_population", ascending=False)
    
    print("\n{:<15} {:>12} {:>10} {:>12} {:>8} {:>8} {:>8}".format(
        "State", "Population", "Counties", "Med Income", "Med Home", "Unemp%", "Pov%"
    ))
    print("-"*70)
    
    for state, row in state_summary.iterrows():
        print("{:<15} {:>12,} {:>10} ${:>11,} ${:>7,} {:>7.1f}% {:>7.1f}%".format(
            state,
            int(row["total_population"]),
            int(row["num_counties"]),
            int(row["median_household_income"]),
            int(row["median_home_value"]),
            row["unemployment_rate"],
            row["poverty_rate"]
        ))
    
    # Find interesting counties
    print("\n" + "="*70)
    print("NOTABLE COUNTIES")
    print("="*70)
    
    # Highest income
    top_income = all_counties.nlargest(5, "median_household_income")
    print("\nTop 5 Counties by Median Household Income:")
    for _, row in top_income.iterrows():
        print(f"  {row['NAME']}, {row['state_name']}: ${row['median_household_income']:,}")
    
    # Highest unemployment
    top_unemp = all_counties.nlargest(5, "unemployment_rate")
    print("\nTop 5 Counties by Unemployment Rate:")
    for _, row in top_unemp.iterrows():
        print(f"  {row['NAME']}, {row['state_name']}: {row['unemployment_rate']:.1f}%")
    
    # Most populous
    top_pop = all_counties.nlargest(5, "total_population")
    print("\nTop 5 Most Populous Counties:")
    for _, row in top_pop.iterrows():
        print(f"  {row['NAME']}, {row['state_name']}: {row['total_population']:,}")
    
    # Export results
    output_file = "midwest_counties_2022.csv"
    all_counties.to_csv(output_file, index=False)
    print(f"\nFull dataset exported to: {output_file}")
    
    # Join geometries and export GeoPackage
    print("\nJoining geometries...")
    counties_geo = pipeline.join_tiger_geometries(
        all_counties,
        geography="county",
        year=2022
    )
    
    geo_output = "midwest_counties_2022.gpkg"
    pipeline.export(counties_geo, geo_output, format="geopackage")
    print(f"GeoPackage exported to: {geo_output}")


if __name__ == "__main__":
    main()
