



import datetime
import json
import math
import os
from typing import Any
import uuid

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


def row_to_json(row):
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

    def insert_location(self, name: str, properties: str, geometry_wkt: str) -> None:
        """
        Inserts a location into the locations table.
        geometry_wkt should be a WKT string, e.g., 'POINT(1 2)'
        """
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO edr_quickstart.locations (name, properties, geometry)
                    VALUES (:name, CAST(:properties AS JSONB), ST_GeomFromText(:geometry, 4326))
                """),
                {
                    "name": name,
                    "properties": properties,
                    "geometry": geometry_wkt,
                },
            )

    def update_location_properties(self, df: pd.DataFrame) -> None:
        """
        Batch update locations from a DataFrame in a single SQL statement.
        Assumes first column is 'name' and uses `row_to_json` for properties.
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
            FROM (VALUES {values_clause}) AS v(name, properties)
            WHERE l.name = v.name;
        """

        with self.engine.begin() as conn:
            conn.execute(text(sql))

    def insert_parameter(self, name: str, symbol: str, label: str) -> None:
        """
        Inserts a parameter into the edr_quickstart.parameters table.
        Generates a unique parameter_id.
        """
        with self.engine.begin() as conn:
            conn.execute(
                text("""
                    INSERT INTO edr_quickstart.parameters
                        (parameter_id, parameter_name, parameter_unit_symbol, parameter_unit_label)
                    VALUES (:parameter_id, :parameter_name, :parameter_unit_symbol, :parameter_unit_label)
                """),
                {
                    "parameter_id": name,
                    "parameter_name": name,
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
            location_col: column name with location names
            parameter_col: column name with parameter names
            value_col: column name with observation values
            time_col: column name with observation times (datetime)
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
            # Fetch mapping of location names -> location_id
            location_names = [str(name) for name in df[location_id_col].unique().tolist()]

            loc_res = conn.execute(
                text("""
                    SELECT location_id, name
                    FROM edr_quickstart.locations
                    WHERE name = ANY(:names)
                """),
                {"names": location_names},
            ).fetchall()
            loc_map = {name: loc_id for loc_id, name in loc_res}

            missing_locations = set(location_names) - set(loc_map.keys())
            if missing_locations:
                raise ValueError(f"Locations not found: {missing_locations}")

            # Fetch mapping of parameter names -> parameter_id
            param_names = [str(name) for name in df[parameter_col].unique().tolist()]

            param_res = conn.execute(
                text("""
                    SELECT parameter_id, parameter_name
                    FROM edr_quickstart.parameters
                    WHERE parameter_name = ANY(:names)
                """),
                {"names": param_names},
            ).fetchall()
            param_map = {name: pid for pid, name in param_res}

            missing_params = set(param_names) - set(param_map.keys())
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
