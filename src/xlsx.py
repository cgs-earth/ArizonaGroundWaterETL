
import os
import geopandas as gpd
from pathlib import Path

from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.
import pandas as pd
from sqlalchemy import create_engine

def get_shapefile(
    shapefile_path: Path
):
    gdf = gpd.read_file(shapefile_path)

    # Ensure IDs are strings for reliable joining
    gdf["SITE_ID"] = gdf["SITE_ID"].astype(str)
    gdf = gdf.drop_duplicates(subset="SITE_ID")
    return gdf

def merge(left: pd.DataFrame, right: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    left_key = left.columns[0]  # first column in left DF

    # Ensure both columns are the same type (string)
    left[left_key] = left[left_key].astype(str)
    right["SITE_ID"] = right["SITE_ID"].astype(str)

    return right.merge(left, how="inner", left_on="SITE_ID", right_on=left_key)


def glob_xlsx():
    shapes = get_shapefile(Path(__file__).parent.parent / "Shape")
    data_Dir = Path(__file__).parent.parent / "Data_Tables"
    for file in os.listdir(data_Dir):
        if not file.endswith(".xlsx"):
            continue 
        df = pd.read_excel(data_Dir / file)

        merged_df = merge(df, shapes)
        post_to_postgis(merged_df)

import os
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import json


def post_to_postgis(df: gpd.GeoDataFrame, schema="edr_quickstart", table="locations"):
    """
    Inserts a GeoDataFrame into a PostGIS table, converting CRS to EPSG:4326.
    """
    import os
    import json
    from sqlalchemy import create_engine
    from sqlalchemy.exc import SQLAlchemyError

    # Get connection info from env
    host = os.environ.get("POSTGRES_HOST")
    db = os.environ.get("POSTGRES_DB")
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")

    if not all([host, db, user, password]):
        raise ValueError("Missing PostgreSQL connection info in environment variables")

    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db}")

    # Ensure GeoDataFrame is in EPSG:4326
    if df.crs is None:
        raise ValueError("GeoDataFrame has no CRS defined. Set it before posting.")
    df = df.to_crs(epsg=4326)

    # Prepare data
    id_col = df.columns[0]
    df_copy = df.copy()

    # Build properties JSONB (all columns except ID and geometry)
    props_cols = [c for c in df_copy.columns if c not in [id_col, "geometry"]]
    df_copy["properties"] = df_copy[props_cols].apply(lambda row: row.to_dict(), axis=1)

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
