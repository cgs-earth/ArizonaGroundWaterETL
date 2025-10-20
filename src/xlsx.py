import os
import json
from typing import Optional
import geopandas as gpd
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import text

from db import DB, row_to_json
from lib import get_all_wells_55_metadata
from mapping import is_timeseries_dataset

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


def merge(left: gpd.GeoDataFrame, right: pd.DataFrame) -> gpd.GeoDataFrame:
    firstCol_left, firstCol_right = left.columns[0], right.columns[0]
    left[firstCol_left] = left[firstCol_left].astype(str)
    right[firstCol_right] = right[firstCol_right].astype(str)
    merged = left.merge(
        right, how="left", left_on=firstCol_left, right_on=firstCol_right
    )
    # don't include the merged index column since it is redundant
    # and will be the same as the left index
    merged = merged.drop(columns=[firstCol_right])
    # Ensure merged result stays a GeoDataFrame
    if not isinstance(merged, gpd.GeoDataFrame):
        merged = gpd.GeoDataFrame(merged, geometry=left.geometry, crs=left.crs)
    return merged



def load_xlsx_files():
    shapes = get_shapefile(Path(__file__).parent.parent / "Shape")
    data_dir = Path(__file__).parent.parent / "Data_Tables"

    merged_df: Optional[gpd.GeoDataFrame] = None
    db = DB()
    for file in os.listdir(data_dir):
        if not file.endswith(".xlsx") and not file.endswith(".csv"):
            continue
        if file.endswith(".xlsx"):
            df = pd.read_excel(data_dir / file)
        else:
            df = pd.read_csv(data_dir / file)

        print(f"Processing {file}...")
        is_timeseries, dataset_def = is_timeseries_dataset(file)

        if is_timeseries:
            assert dataset_def is not None
            firstCol = df.columns[0]  # location column
            timeCol = dataset_def.time_field

            # Ensure all locations exist
            # Convert to string for consistency
            df[firstCol] = df[firstCol].astype(str)
            with db.engine.connect() as conn:
                existing_loc_ids = {
                    row['location_id']
                    for row in conn.execute(
                        text("SELECT location_id FROM edr_quickstart.locations")
                    ).mappings()
                }

            new_locs = set(df[firstCol]) - existing_loc_ids
            for loc_name in new_locs:
                # Insert a barebones location (no geometry available)
                db.insert_location(
                    location_id=loc_name,
                    properties=json.dumps({}),  # empty properties
                    geometry_wkt="POINT(0 0)"  # placeholder
                )

            for field in dataset_def.timeseries_fields:
                db.insert_parameter(
                    parameter_id=field, 
                    symbol=field,
                    label=field,
                )
                df["_PARAM_NAME"] = field
                db.insert_observations_from_df(
                    df=df,
                    location_id_col=firstCol,
                    parameter_col="_PARAM_NAME",
                    value_col=field,
                    time_col=timeCol
                )


        else:
            if merged_df is None:
                # Start with shapes as GeoDataFrame
                merged_df = merge(shapes, df)
                assert merged_df.crs is not None
                firstColName = merged_df.columns[0]
                for _, row in merged_df.iterrows():
                    db.insert_location(
                        location_id=row[firstColName],
                        properties=row_to_json(row),
                        geometry_wkt=row["geometry"].wkt,
                    )
            db.update_location_properties(df=df)

            assert merged_df is not None

def add_wells_55_metadata():
    db = DB()

    for feature in get_all_wells_55_metadata():
        print("Adding wells55 metadata for", feature["id"])
        assert feature["geometry"]["type"] == "Point"
        assert len(feature["geometry"]["coordinates"]) == 2
        longitude, latitude = feature["geometry"]["coordinates"]
        db.add_wells_55_metadata(feature["id"], feature["properties"], longitude, latitude)