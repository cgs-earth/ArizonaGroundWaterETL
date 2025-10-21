



import datetime
import json
import math
import os

import pandas as pd
from sqlalchemy import create_engine, text


def serialize_for_json(obj):
    if isinstance(obj, (pd.Timestamp, datetime.datetime)):
        return obj.isoformat()
    elif isinstance(obj, (pd.Timedelta,)):
        return str(obj)
    elif obj is pd.NA or ((isinstance(obj, float) and math.isnan(obj))):
        return None
    else:
        return obj


def row_to_json(row: pd.Series):
    props = {
        k: serialize_for_json(v) for k, v in row.to_dict().items() if k != "geometry"
    }
    return json.dumps(props)


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

class DB():


    def __init__(self) -> None:
        host = os.environ.get("POSTGRES_HOST")

        db = os.environ.get("POSTGRES_DB")
        user = os.environ.get("POSTGRES_USER")
        password = os.environ.get("POSTGRES_PASSWORD")
        self.engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}/{db}")

        ensure_postgis_geometry_crs(
            self.engine,
            schema="edr_quickstart",
            table="locations",
            geom_column="geometry",
            srid=4326,
        )

    def insert_location(self, location_id: str, properties: str, geometry_wkt: str) -> None:
        """
        Inserts a location into the locations table.
        geometry_wkt should be a WKT string, e.g., 'POINT(1 2)'
        """
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                INSERT INTO edr_quickstart.locations (location_id, properties, geometry)
                VALUES (:location_id, CAST(:properties AS JSONB), ST_GeomFromText(:geometry, 4326))
                ON CONFLICT (location_id)
                DO UPDATE
                SET 
                    properties = locations.properties || EXCLUDED.properties,
                    geometry = EXCLUDED.geometry
                """),
                {
                    "location_id": location_id,
                    "properties": properties,
                    "geometry": geometry_wkt,
                },
            )

    def update_location_properties(self, df: pd.DataFrame) -> None:
        """
        Batch update locations from a DataFrame in a single SQL statement.
        Assumes first column is 'location_id' and uses `row_to_json` for properties.
        """
        first_col = df.columns[0]

        # Build a VALUES clause
        values_clause = ",".join(
            f"('{row[first_col]}', '{row_to_json(row).replace("'", "''")}')"
            for _, row in df.iterrows()
        )

        sql = f"""
            UPDATE edr_quickstart.locations AS l
            SET properties = l.properties || v.properties::jsonb
            FROM (VALUES {values_clause}) AS v(location_id, properties)
            WHERE l.location_id = v.location_id;
        """

        with self.engine.begin() as conn:
            conn.execute(text(sql))

    def insert_parameter(self, parameter_id: str, symbol: str, label: str) -> None:
        """
        Inserts a parameter into the edr_quickstart.parameters table.
        Generates a unique parameter_id.
        """
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO edr_quickstart.parameters
                        (parameter_id, parameter_name, parameter_unit_symbol, parameter_unit_label)
                    VALUES (:parameter_id, :parameter_id, :parameter_unit_symbol, :parameter_unit_label)
                """),
                {
                    "parameter_id": parameter_id,
                    "parameter_name": parameter_id,
                    "parameter_unit_symbol": symbol,
                    "parameter_unit_label": label,
                },
            )

    def insert_observations_from_df(
        self,
        df: pd.DataFrame,
        location_id_col: str,
        parameter_col: str,
        value_col: str,
        time_col: str,
    ) -> None:
        """
        Batch insert/update observations from a DataFrame.

        Parameters:
            df: DataFrame containing observations
            location_col: column location_id with location location_ids
            parameter_col: column location_id with parameter location_ids
            value_col: column location_id with observation values
            time_col: column location_id with observation times (datetime)
        """
        if df.empty:
            return

        # Ensure datetime
        df[time_col] = pd.to_datetime(df[time_col], errors="coerce")

        # Drop rows with missing required fields
        df = df.dropna(subset=[location_id_col, parameter_col, value_col, time_col])
        if df.empty:
            return

        with self.engine.begin() as conn:
            # Fetch mapping of location location_ids -> location_id
            location_location_ids = [str(location_id) for location_id in df[location_id_col].unique().tolist()]

            loc_res = conn.execute(
                text("""
                    SELECT location_id, location_id
                    FROM edr_quickstart.locations
                    WHERE location_id = ANY(:location_ids)
                """),
                {"location_ids": location_location_ids},
            ).fetchall()
            loc_map = {location_id: loc_id for loc_id, location_id in loc_res}

            missing_locations = set(location_location_ids) - set(loc_map.keys())
            if missing_locations:
                raise ValueError(f"Locations not found: {missing_locations}")

            # Fetch mapping of parameter location_ids -> parameter_id
            param_location_ids = [str(location_id) for location_id in df[parameter_col].unique().tolist()]

            param_res = conn.execute(
                text("""
                    SELECT parameter_id, parameter_name
                    FROM edr_quickstart.parameters
                    WHERE parameter_name = ANY(:location_ids)
                """),
                {"location_ids": param_location_ids},
            ).fetchall()
            param_map = {location_id: pid for pid, location_id in param_res}

            missing_params = set(param_location_ids) - set(param_map.keys())
            if missing_params:
                raise ValueError(f"Parameters not found: {missing_params}")


            # Prepare batch insertion data
            insert_data = [
                {
                    "loc_id": loc_map[row[location_id_col]],
                    "param_id": param_map[row[parameter_col]],
                    "val": row[value_col],
                    "obs_time": row[time_col],
                }
                for _, row in df.iterrows()
            ]

            # Execute batch insert/upsert safely
            conn.execute(
                text("""
                    INSERT INTO edr_quickstart.observations
                        (location_id, parameter_id, observation_value, observation_time)
                    VALUES (:loc_id, :param_id, :val, :obs_time)
                    ON CONFLICT (location_id, parameter_id, observation_time)
                    DO UPDATE SET observation_value = EXCLUDED.observation_value
                """),
                insert_data,
            )

    def add_wells_55_metadata(
        self, reg_id: str, properties: dict, longitude: float, latitude: float
    ):
        """
        Append WELLS_55_ prefixed properties to an existing location's properties if a location
        exists with a matching REG_ID in its properties JSONB. If no such location exists,
        create a new one with these properties.
        """
        # Prepare new properties with prefix
        new_properties: dict = {}
        assert reg_id
        for prop in properties:
            new_properties[f"WELLS_55_{prop}"] = properties[prop]

        new_properties_json = json.dumps(new_properties)

        with self.engine.begin() as conn:
            # Try to find a location where properties->>'REG_ID' matches the provided reg_id
            existing = conn.execute(
                text("""
                    SELECT location_id, properties
                    FROM edr_quickstart.locations
                    WHERE properties->>'REG_ID' = :reg_id
                """),
                {"reg_id": reg_id},
            ).fetchone()

            if existing:
                print(f"A location with the same registry id {reg_id} already exists, updating...")
                conn.execute(
                    text("""
                        UPDATE edr_quickstart.locations
                        SET properties = properties || CAST(:new_props AS JSONB)
                        WHERE location_id = :location_id
                    """),
                    {"location_id": existing.location_id, "new_props": new_properties_json},
                )
            else:
                # Insert new location with default geometry
                assert longitude and latitude, (
                    "You must specify a geometry for a new location"
                )
                print(f"Creating new location with registry id {reg_id} and using that as the location_id...")
                conn.execute(
                    text("""
                        INSERT INTO edr_quickstart.locations (location_id, properties, geometry)
                        VALUES (
                            :location_id,
                            CAST(:props AS JSONB),
                            ST_Transform(
                                ST_SetSRID(ST_MakePoint(:x, :y), 26912), -- input is UTM zone 12N
                                4326                                      -- transform to lat/lon
                            )
                        )
                    """),
                    {
                        "location_id": reg_id,  # using reg_id as location_id if new
                        "props": new_properties_json,
                        "x": float(longitude),  # these are UTM Eastings
                        "y": float(latitude),  # these are UTM Northings
                    },
                )
