from typing import Literal, NamedTuple, Tuple

class TimeseriesFieldDef(NamedTuple):
    id: str
    unit_name: str
    unit_description: str
    unit_symbol: Literal["gpm", "ft", "hr", "ft / gpm", "C", "V", "psi"]

class DatasetDef(NamedTuple):
    file: str
    time_field: str
    timeseries_fields: list[TimeseriesFieldDef]



class SkipDef(NamedTuple):
    file: str


timeseries_datasets = [
    DatasetDef(
        file="GWSI_FLOWING_DISCHARGES.xlsx",
        time_field="FLWD_MEASURE_DATE",
        timeseries_fields=[
            TimeseriesFieldDef(
                id="FLWD_DISCHARGE_RATE",
                unit_name="Gallons per minute",
                unit_description="is the discharge rate of the site in gallons per minute. If discharge is determined in other units such as cfs or other metric units, convert to gallons per minute. Two decimal places are provided for very small discharges.",
                unit_symbol="gpm",
            )
        ],
    ),
    DatasetDef(
        file="GWSI_PUMPING_DISCHARGES.xlsx",
        time_field="PMPD_MEASURE_DATE",
        timeseries_fields=[
            TimeseriesFieldDef(
                id="PMPD_DISCHARGE_RATE",
                unit_name="Gallons per minute",
                unit_description="contains the measured discharge rate of the site in gallons per minute. If discharge is determined in other units (such as cfs or other metric units) convert to gallons per minute. Two decimal places are provided for very small discharges.",
                unit_symbol="gpm",
            ),
            TimeseriesFieldDef(
                id="PMPD_PRODUCTION_WATER_LEVEL",
                unit_name="Feet Below Land Surface",
                unit_description="records the depth to water measurement in feet below land surface, taken while the well was discharging. The difference between this value and the static water level is the well's production drawdown.",
                unit_symbol="ft",
            ),
            TimeseriesFieldDef(
                id="PMPD_STATIC_WATER_LEVEL",
                unit_name="Feet Below Land Surface",
                unit_description="records the static water level in feet below land surface, measured before pumping. If the static water level is above the land surface, the head (if measurable) is preceded by a minus sign (-).",
                unit_symbol="ft",
            ),
            TimeseriesFieldDef(
                id="PMPD_PUMPING_PERIOD",
                unit_name="Hours",
                unit_description="contains the length of time, in hours, that the well was pumped prior to the collection of the production depth to water measurement. Two decimal points are provided for fractions of an hour.",
                unit_symbol="hr",
            ),
            TimeseriesFieldDef(
                id="PMPD_SPECIFIC_CAPACITY",
                unit_description="is rate of discharge of a production well per unit of drawdown. This field is calculated by Oracle based on the PMPD_WELL_DRAWDOWN and PMPD_DISCHARGE_RATE fields.",
                unit_name="Feet per gallon per minute",
                unit_symbol="ft / gpm",
            ),
            TimeseriesFieldDef(
                id="PMPD_WELL_DRAWDOWN",
                unit_name="Feet",
                unit_description="contains the drawdown, in feet, of the pumping well. Oracle calculates the field by subtracting the production water level from the static water level.",
                unit_symbol="ft",
            ),
        ],
    ),
    DatasetDef(
        file="GWSI_TRANSDUCER_LEVELS.csv",
        time_field="MEASUREMENT_DATE",
        timeseries_fields=[
            TimeseriesFieldDef(
                id="DEPTH_TO_WATER",
                unit_description="records the depth to water in feet below land surface. Depth to water can include up to two decimal places. If the water level is above land surface, the water level is preceded by a minus (-) sign. If the head at a flowing site is unknown, if the water level cannot be measured, the site is dry, or the well is destroyed, this field is left blank and the appropriate code is placed in the REMARK_CODE field.",
                unit_name="Feet",
                unit_symbol="ft",
            ),
            TimeseriesFieldDef(
                id="WATER_LEVEL_ELEVATION",
                unit_description="contains the elevation of the water table above vertical datum. This field is calculated by subtracting the depth to water from the well altitude as entered in the GWSI_SITES data table. Except for flowing wells, water level elevations are blank for records that have no depth to water measurements.",
                unit_name="Feet",
                unit_symbol="ft",
            ),
            TimeseriesFieldDef(
                id="TEMPERATURE",
                unit_description="recorded in this field varies by the type of equipment installed at the site. For sites equipped with transducers, this field records the water temperature in degrees Celsius at the time the discrete water level was recorded. Automated sites equipped with bubblers record the atmospheric temperature. Sites with shaft encoders record “0” in this field.",
                unit_name="Degrees Celsius",
                unit_symbol="C",
            ),
            TimeseriesFieldDef(
                id="BATTERY_VOLTAGE",
                unit_description="records the battery voltage of the digital recorder at the time the discrete water level was recorded for the site.",
                unit_name="Volts",
                unit_symbol="V",
            ),
            TimeseriesFieldDef(
                id="PSI",
                unit_name="Pounds per square inch",
                unit_description="records the water pressure in pounds per square inch at the time the discrete water level was recorded for the site. Sites equipped with shaft encoders record “0” in this field.",
                unit_symbol="psi",
            ),
        ],
    ),
]

def is_timeseries_dataset(file: str) -> Tuple[bool, DatasetDef | None]:
    for dataset in timeseries_datasets:
        if dataset.file == file:
            return True, dataset
    return False, None


# GWSI_OWNER_SITE_NAMES.xlsx
# GWSI_PERFORATION_COMPLETIONS.xlsx
# GWSI_PUMPING_DISCHARGES.xlsx
# GWSI_SITE_ALTITUDE_HISTORY.xlsx
# GWSI_SITE_CADASTRAL_HISTORY.xlsx
# GWSI_SITE_INVENTORIES.xlsx
# GWSI_SITE_LOCATION_HISTORY.xlsx
# GWSI_SITES.xlsx
# GWSI_SPRING_NAMES.xlsx
# GWSI_TRS.xlsx
# GWSI_WELL_COMPLETIONS.xlsx
# GWSI_WELL_LIFTS.xlsx
# GWSI_WELL_LOGS.xlsx
# GWSI_WM_POINTS.xlsx
# GWSI_WQ_REPORTS.xlsx
# GWSI_WW_LEVELS.xlsx
