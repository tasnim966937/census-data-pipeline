# Census Data Pipeline

A Python-based ETL pipeline for fetching, processing, and analyzing U.S. Census Bureau data at multiple geographic levels. Built for scalability and integration with GIS platforms.

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![Census API](https://img.shields.io/badge/Census-API-green.svg)
![GeoPandas](https://img.shields.io/badge/GeoPandas-enabled-orange.svg)

## Features

- **Multi-level Geographic Data**: Fetch data at national, state, county, tract, and block group levels
- **ACS & Decennial Support**: Works with American Community Survey (1-year, 5-year) and Decennial Census
- **TIGER/Line Integration**: Automatic shapefile downloading and geometry joining
- **Data Transformation**: Built-in cleaning, normalization, and derived variable calculation
- **Multiple Output Formats**: Export to CSV, GeoJSON, GeoPackage, or PostgreSQL/PostGIS
- **Batch Processing**: Process multiple states/counties in parallel

## Installation

```bash
pip install -r requirements.txt
```

## Quick Start

```python
from census_pipeline import CensusPipeline

# Initialize pipeline with your Census API key
pipeline = CensusPipeline(api_key="YOUR_CENSUS_API_KEY")

# Fetch ACS 5-year population data at tract level for Ohio
data = pipeline.fetch_acs5(
    variables=["B01003_001E", "B19013_001E"],  # Total pop, Median income
    geography="tract",
    state="39",  # Ohio FIPS
    year=2022
)

# Join with TIGER geometries and export
pipeline.join_tiger_geometries(data, year=2022)
pipeline.export(data, format="geopackage", output="ohio_tracts.gpkg")
```

## Geographic Hierarchy

```
Nation
└── Region
    └── Division
        └── State
            └── County
                └── Tract
                    └── Block Group
                        └── Block
```

## Supported Data Products

| Product | Description | Years Available |
|---------|-------------|-----------------|
| ACS 5-Year | Detailed estimates, all geographies | 2009-2022 |
| ACS 1-Year | Annual estimates, 65k+ population areas | 2005-2022 |
| Decennial | Complete count, every 10 years | 2000, 2010, 2020 |
| PEP | Population estimates | Annual |

## Configuration

Create a `config.yaml` file:

```yaml
census:
  api_key: ${CENSUS_API_KEY}
  base_url: https://api.census.gov/data

output:
  default_format: geopackage
  crs: EPSG:4326

processing:
  parallel_workers: 4
  chunk_size: 1000
```

## Project Structure

```
census-data-pipeline/
├── src/
│   ├── census_pipeline.py    # Main pipeline class
│   ├── api_client.py         # Census API wrapper
│   ├── geography.py          # Geographic utilities
│   ├── transformers.py       # Data transformation functions
│   └── exporters.py          # Output format handlers
├── examples/
│   ├── fetch_state_demographics.py
│   ├── tract_level_analysis.py
│   └── multi_year_comparison.py
├── tests/
├── config.yaml
├── requirements.txt
└── README.md
```

## Example: County-Level Demographics

```python
from census_pipeline import CensusPipeline
import geopandas as gpd

pipeline = CensusPipeline()

# Fetch key demographic variables for all US counties
counties = pipeline.fetch_acs5(
    variables={
        "B01003_001E": "total_population",
        "B19013_001E": "median_household_income",
        "B25077_001E": "median_home_value",
        "B23025_005E": "unemployed",
        "B15003_022E": "bachelors_degree"
    },
    geography="county",
    state="*",
    year=2022
)

# Calculate derived metrics
counties["unemployment_rate"] = counties["unemployed"] / counties["total_population"]
counties["college_rate"] = counties["bachelors_degree"] / counties["total_population"]

# Export with geometries
pipeline.export(counties, format="geopackage", output="us_counties_2022.gpkg")
```

## PostGIS Integration

```python
from census_pipeline import CensusPipeline
from census_pipeline.exporters import PostGISExporter

pipeline = CensusPipeline()
data = pipeline.fetch_acs5(...)

# Export directly to PostGIS
exporter = PostGISExporter(
    host="localhost",
    database="census_db",
    user="postgres"
)
exporter.to_postgis(data, table_name="acs_tracts_2022", if_exists="replace")
```

## License

MIT License

## Author

Mir Md Tasnim Alam  
[GitHub](https://github.com/tasnim966937) | [ORCID](https://orcid.org/0009-0003-3619-6367)
