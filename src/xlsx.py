import datetime
import os
import json
import geopandas as gpd
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Load environment variables
load_dotenv()


def get_shapefile(shapefile_path: Path) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(shapefile_path)

    # Ensure IDs are strings for reliable joining
    gdf["SITE_ID"] = gdf["SITE_ID"].astype(str)
    gdf = gdf.drop_duplicates(subset="SITE_ID")

    # Convert to EPSG:4326 to standardize
    if gdf.crs is None:
        raise ValueError("Shapefile has no CRS defined")
    gdf = gdf.to_crs(epsg=4326)
    return gdf


def merge(left: pd.DataFrame, right: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    left_key = left.columns[0]  # first column in left DF

    # Ensure both columns are the same type (string)
    left[left_key] = left[left_key].astype(str)
    right["SITE_ID"] = right["SITE_ID"].astype(str)

    return right.merge(left, how="inner", left_on="SITE_ID", right_on=left_key)


def glob_xlsx():
    shapes = get_shapefile(Path(__file__).parent.parent / "Shape")
    data_dir = Path(__file__).parent.parent / "Data_Tables"
    for file in os.listdir(data_dir):
        if not file.endswith(".xlsx"):
            continue
        df = pd.read_excel(data_dir / file)

        merged_df = merge(df, shapes)
        post_to_postgis(merged_df)


def ensure_postgis_geometry_crs(
    engine, schema: str, table: str, geom_column="geometry", srid=4326
):
    """
    Alters an existing table's geometry column to have the correct SRID if needed.
    """
    with engine.begin() as conn:
        result = conn.execute(
            text(f"""
            SELECT f_geometry_column, srid 
            FROM geometry_columns 
            WHERE f_table_schema='{schema}' 
              AND f_table_name='{table}' 
              AND f_geometry_column='{geom_column}';
            """)
        ).fetchone()

        if result is None:
            # Geometry column doesn't exist; GeoPandas will create it
            return

        current_srid = result[1]
        if current_srid != srid:
            print(
                f"Altering {schema}.{table}.{geom_column} SRID from {current_srid} to {srid}"
            )
            conn.execute(
                text(f"""
                ALTER TABLE {schema}.{table} 
                ALTER COLUMN {geom_column} TYPE geometry(Geometry, {srid})
                USING ST_Transform({geom_column}, {srid});
                """)
            )


def post_to_postgis(df: gpd.GeoDataFrame, schema="edr_quickstart", table="locations"):
    """
    Inserts a GeoDataFrame into a PostGIS table, converting CRS to EPSG:4326 and handling existing table SRID.
    """
    host = os.environ.get("POSTGRES_HOST")
    db = os.environ.get("POSTGRES_DB")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")

    if not all([host, db, user, password]):
        raise ValueError("Missing PostgreSQL connection info in environment variables")

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db}")

    # Ensure GeoDataFrame CRS
    if df.crs is None:
        raise ValueError("GeoDataFrame has no CRS defined")
    df = df.to_crs(epsg=4326)

    # Ensure PostGIS table geometry column is correct SRID
    ensure_postgis_geometry_crs(
        engine, schema=schema, table=table, geom_column="geometry", srid=4326
    )

    # Prepare data
    id_col = df.columns[0]
    df_copy = df.copy()

    # Build properties JSONB safely
    props_cols = [c for c in df_copy.columns if c not in [id_col, "geometry"]]

    def serialize_for_json(obj):
        if isinstance(obj, (pd.Timestamp, datetime.datetime)):
            return obj.isoformat()
        elif isinstance(obj, (pd.Timedelta,)):
            return str(obj)
        else:
            return obj

    def row_to_json(row):
        return json.dumps({k: serialize_for_json(v) for k, v in row.to_dict().items()})

    df_copy["properties"] = df_copy[props_cols].apply(row_to_json, axis=1)

    # Keep only id, properties, geometry
    df_copy = df_copy[[id_col, "properties", "geometry"]]

    # Rename id column to 'name' for locations table
    df_copy = df_copy.rename(columns={id_col: "name"})


    try:
        df_copy.to_postgis(
            name=table, con=engine, schema=schema, if_exists="append", index=False
        )
        print(f"Inserted {len(df_copy)} rows into {schema}.{table}")
    except SQLAlchemyError as e:
        print(f"Error inserting into PostGIS: {e}")
