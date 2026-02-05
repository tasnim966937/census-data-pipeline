"""
Data Exporters - Output handlers for various formats.

Author: Mir Md Tasnim Alam
"""

import logging
from pathlib import Path
from typing import Optional, Union

import pandas as pd
import geopandas as gpd

logger = logging.getLogger(__name__)


class DataExporter:
    """
    Export Census data to various formats.
    
    Supported formats:
    - CSV (tabular data only)
    - GeoJSON (with geometries)
    - GeoPackage (with geometries, recommended)
    - Shapefile (with geometries, legacy)
    - Parquet (efficient columnar storage)
    """
    
    def export(
        self,
        data: Union[pd.DataFrame, gpd.GeoDataFrame],
        output: str,
        format: str = "geopackage",
        layer_name: Optional[str] = None
    ) -> None:
        """
        Export data to specified format.
        
        Args:
            data: DataFrame or GeoDataFrame to export
            output: Output file path
            format: Output format
            layer_name: Layer name for GeoPackage
        """
        output_path = Path(output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        format_lower = format.lower()
        
        if format_lower == "csv":
            self._to_csv(data, output_path)
        elif format_lower in ["geopackage", "gpkg"]:
            self._to_geopackage(data, output_path, layer_name)
        elif format_lower == "geojson":
            self._to_geojson(data, output_path)
        elif format_lower in ["shapefile", "shp"]:
            self._to_shapefile(data, output_path)
        elif format_lower == "parquet":
            self._to_parquet(data, output_path)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        logger.info(f"Exported to {output_path}")
    
    def _to_csv(self, data: pd.DataFrame, path: Path) -> None:
        """Export to CSV (drops geometry if present)."""
        if isinstance(data, gpd.GeoDataFrame):
            data = pd.DataFrame(data.drop(columns="geometry"))
        data.to_csv(path, index=False)
    
    def _to_geopackage(
        self,
        data: Union[pd.DataFrame, gpd.GeoDataFrame],
        path: Path,
        layer_name: Optional[str] = None
    ) -> None:
        """Export to GeoPackage."""
        if not isinstance(data, gpd.GeoDataFrame):
            raise ValueError("GeoPackage export requires GeoDataFrame with geometries")
        
        layer = layer_name or path.stem
        data.to_file(path, driver="GPKG", layer=layer)
    
    def _to_geojson(
        self,
        data: Union[pd.DataFrame, gpd.GeoDataFrame],
        path: Path
    ) -> None:
        """Export to GeoJSON."""
        if not isinstance(data, gpd.GeoDataFrame):
            raise ValueError("GeoJSON export requires GeoDataFrame with geometries")
        
        data.to_file(path, driver="GeoJSON")
    
    def _to_shapefile(
        self,
        data: Union[pd.DataFrame, gpd.GeoDataFrame],
        path: Path
    ) -> None:
        """Export to Shapefile (legacy format)."""
        if not isinstance(data, gpd.GeoDataFrame):
            raise ValueError("Shapefile export requires GeoDataFrame with geometries")
        
        # Shapefile has 10-char field name limit
        logger.warning("Shapefile format truncates column names to 10 characters")
        data.to_file(path, driver="ESRI Shapefile")
    
    def _to_parquet(
        self,
        data: Union[pd.DataFrame, gpd.GeoDataFrame],
        path: Path
    ) -> None:
        """Export to Parquet format."""
        if isinstance(data, gpd.GeoDataFrame):
            data.to_parquet(path)
        else:
            data.to_parquet(path, index=False)


class PostGISExporter:
    """
    Export Census data directly to PostGIS database.
    
    Example:
        >>> exporter = PostGISExporter(
        ...     host="localhost",
        ...     database="census_db",
        ...     user="postgres"
        ... )
        >>> exporter.to_postgis(gdf, "acs_tracts_2022")
    """
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        database: str = "census",
        user: str = "postgres",
        password: Optional[str] = None
    ):
        """
        Initialize PostGIS connection.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password (reads from PGPASSWORD env var if not provided)
        """
        import os
        
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password or os.environ.get("PGPASSWORD", "")
        
        self._connection_string = (
            f"postgresql://{self.user}:{self.password}@"
            f"{self.host}:{self.port}/{self.database}"
        )
    
    def to_postgis(
        self,
        data: gpd.GeoDataFrame,
        table_name: str,
        schema: str = "public",
        if_exists: str = "replace",
        index: bool = True
    ) -> None:
        """
        Export GeoDataFrame to PostGIS table.
        
        Args:
            data: GeoDataFrame to export
            table_name: Target table name
            schema: Database schema
            if_exists: Action if table exists ('fail', 'replace', 'append')
            index: Create spatial index
        """
        from sqlalchemy import create_engine
        
        engine = create_engine(self._connection_string)
        
        data.to_postgis(
            table_name,
            engine,
            schema=schema,
            if_exists=if_exists,
            index=index,
            index_label="gid"
        )
        
        logger.info(f"Exported to PostGIS: {schema}.{table_name}")
    
    def execute_sql(self, sql: str) -> None:
        """Execute arbitrary SQL statement."""
        from sqlalchemy import create_engine, text
        
        engine = create_engine(self._connection_string)
        with engine.connect() as conn:
            conn.execute(text(sql))
            conn.commit()
