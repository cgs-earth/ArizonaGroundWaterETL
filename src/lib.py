from typing import Generator, Literal, TypedDict
import requests


class Response(TypedDict):
    type: Literal["Feature"] 
    geometry: dict 
    properties: dict
    id: str 
    link: dict

def get_all_wells_55_metadata() -> Generator[Response, None, None]:
    url = "http://localhost:5001/collections/wells_55/items?f=json"
    while True:
        r = requests.get(url)
        r.raise_for_status()
        if r.status_code == 200:
            for feature in r.json()["features"]:
                yield feature
        links = r.json()["links"]
        assert links and isinstance(links, list)
        url = None 
        for link in links:
            if link["rel"] == "next":
                url = link["href"]

        if url is None:
            break