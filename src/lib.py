import os
from typing import Generator, Literal, TypedDict
import requests
import geopandas as gpd
from pathlib import Path

class Response(TypedDict):
    type: Literal["Feature"] 
    geometry: dict 
    properties: dict
    id: str 
    link: dict

def get_all_wells_55_metadata() -> Generator[Response, None, None]:
    url = "http://localhost:5001/collections/wells_55/items?limit=4000000&f=json"
    # offset appears to be broken with esri shapefiles thus we need to just make one
    # huge request and loop through it instead of using the 'next' link
    r = requests.get(url)
    r.raise_for_status()
    if r.status_code == 200:
        for feature in r.json()["features"]:
            yield feature


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

def add_shapefile_to_postgis(shapefile_path: Path):
    command= f"ogr2ogr -f 'PostgreSQL' PG:'host=localhost port=5432 dbname=edr user=postgres password=changeMe' -nln wells_55 -a_srs 'EPSG:4326' -overwrite {shapefile_path}"
    os.system(command)