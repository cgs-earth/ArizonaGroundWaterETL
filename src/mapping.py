from typing import NamedTuple, Tuple


class DatasetDef(NamedTuple):
    file: str
    time_field: str
    timeseries_fields: list[str]


class SkipDef(NamedTuple):
    file: str


timeseries_datasets = [
    DatasetDef(
        file="GWSI_FLOWING_DISCHARGES.xlsx",
        time_field="FLWD_MEASURE_DATE",
        timeseries_fields=["FLWD_DISCHARGE_RATE"],
    ),
    DatasetDef(
        file="GWSI_PUMPING_DISCHARGES.xlsx",
        time_field="PMPD_MEASURE_DATE",
        timeseries_fields=[
            "PMPD_DISCHARGE_RATE",
            "PMPD_PRODUCTION_WATER_LEVEL",
            "PMPD_STATIC_WATER_LEVEL",
            "PMPD_PUMPING_PERIOD",
            "PMPD_SPECIFIC_CAPACITY",
            "PMPD_WELL_DRAWDOWN",
        ],
    ),
    DatasetDef(
        file="GWSI_TRANSDUCER_LEVELS.csv",
        time_field="MEASUREMENT_DATE",
        timeseries_fields=[
            "DEPTH_TO_WATER",
            "WATER_LEVEL_ELEVATION",
            "TEMPERATURE",
            "BATTERY_VOLTAGE",
            "PSI",
        ],
    ),
    DatasetDef(
        file="GWSI_TRANSDUCER_LEVELS.csv",
        time_field="WLWA_MEASUREMENT_DATE",
        timeseries_fields=[
            "WLWA_DEPTH_TO_WATER",
            "PSI",
        ],
    ),
    DatasetDef(
        file="GWSI_TRANSDUCER_LEVELS.csv",
        time_field="WLWA_MEASUREMENT_DATE",
        timeseries_fields=[
            "WLWA_DEPTH_TO_WATER",
            "PSI",
        ],
    ),
    DatasetDef(
        file="GWSI_WELL_LIFTS.csv",
        time_field="WLLI_ENTRY_DATE",
        timeseries_fields=[
            "WLWA_DEPTH_TO_WATER",
            "PSI",
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
