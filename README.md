# ArizonaGroundWaterETL

This repository generates a postgis datadump for the ADWR TABULAR Data Download. It puts this data in a database schema that can be used with [pgedr](https://github.com/internetofwater/pgedr/) to serve the items using the OGC Environmental Data Retrieval Standard (EDR) 
 
To download the source dataset you must go to https://www.azwater.gov/gis-data-and-maps and download the ADWR Tabular Data Download 

![an image showing where to go on the azwater.gov website to download the tabular data](image.png)

## Retrieving the Data

You don't need to run the entire pipeline to get the data. Run `docker pull ghcr.io/cgs-earth/arizona-groundwater-dump:latest` and look for the `edr_backup.edr` file that will be created. This can be restored into a postgis database. An example is in the makefile

## Reproducing the Data Dump

To generate the data dump you need to:

1. Download the data from https://www.azwater.gov/gis-data-and-maps
2. Ingest all the xlsx/csv data by running `make load_xlsx`
3. Join all the metadata from wells 55 by running `make add_metadata`
    - This step requires that pygeoapi is running and serving the Wells 55 shapefile
    - This step is serpate from the xlsx loading since it can take a long time
    - You can start pygeoapi by running `make dev`
    - You can get the wells 55 shapefile by running `docker pull ghcr.io/cgs-earth/arizona-wells-55-registry:latest` 
4. Generate the data dump by running `make backup`
5. Run `make push_to_registry` once your database is dumped to push to ghcr.io

# Wells 55 Background

Wells 55 is a well registry database which contains all wells registered in the state of Arizona. In this data dump we get the info for the well name and description by joining the ID in the xlsx files with the REGISTRY_ID field in https://services.arcgis.com/C34zQ7veRS0V1t04/ArcGIS/rest/services/Well_Registry_2024/FeatureServer/0 