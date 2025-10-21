from typing import Generator, Literal, TypedDict
import requests


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
