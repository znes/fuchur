# -*- coding: utf-8 -*-
"""
"""
import os

from oemof.tabular.datapackage import building
import pandas as pd

import fuchur


def pv(buses, weather_year, scenario_year, datapackage_dir,
       raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    Parameter
    ---------
    buses: array like
        List with buses represented by iso country code
    weather_year: integer or string
        Year to select from raw data source
    scenario_year: integer or string
        Year to use for timeindex in tabular resource
    datapackage_dir: string
        Directory for tabular resource
    raw_data_path: string
        Path where raw data file `ninja_pv_europe_v1.1_merra2.csv`
        is located
    """
    filepath = os.path.join(
        raw_data_path, "ninja_pv_europe_v1.1_merra2.csv"
    )
    year = str(weather_year)

    countries = buses

    raw_data = pd.read_csv(filepath, index_col=[0], parse_dates=True)
    # for leap year...
    raw_data = raw_data[
        ~((raw_data.index.month == 2) & (raw_data.index.day == 29))
    ]

    df = raw_data.loc[year]

    sequences_df = pd.DataFrame(index=df.index)

    for c in countries:
        sequence_name = c + "-pv-profile"
        sequences_df[sequence_name] = raw_data.loc[year][c].values

    sequences_df.index = building.timeindex(
        year=str(scenario_year)
    )
    building.write_sequences(
        "volatile_profile.csv",
        sequences_df,
        directory=os.path.join(datapackage_dir, "data", "sequences"),
    )


def wind(buses, weather_year, scenario_year, datapackage_dir,
         raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    Parameter
    ---------
    buses: array like
        List with buses represented by iso country code
    weather_year: integer or string
        Year to select from raw data source
    scenario_year: integer or string
        Year to use for timeindex in tabular resource
    datapackage_dir: string
        Directory for tabular resource
    raw_data_path: string
        Path where raw data file `ninja_wind_europe_v1.1_current_national.csv`
        and `ninja_wind_europe_v1.1_current_national.csv`
        is located
    """
    off_filepath = os.path.join(
        raw_data_path,
        "ninja_wind_europe_v1.1_future_nearterm_on-offshore.csv",
    )

    near_term_path = os.path.join(
        raw_data_path, "ninja_wind_europe_v1.1_current_national.csv"
    )

    year = str(weather_year)

    near_term = pd.read_csv(near_term_path, index_col=[0], parse_dates=True)
    # for lead year...
    near_term = near_term[
        ~((near_term.index.month == 2) & (near_term.index.day == 29))
    ]

    offshore_data = pd.read_csv(off_filepath, index_col=[0], parse_dates=True)
    offshore_data = offshore_data[
        ~((offshore_data.index.month == 2) & (offshore_data.index.day == 29))
    ]

    sequences_df = pd.DataFrame(index=near_term.loc[year].index)

    NorthSea = ["DE", "DK", "NO", "NL", "BE", "GB", "SE"]

    for c in buses:
        # add offshore profile if country exists in offshore data columns
        # and if its in NorthSea
        if [
            col for col in offshore_data.columns if c + "_OFF" in col
        ] and c in NorthSea:
            sequences_df[c + "-wind-off-profile"] = offshore_data[c + "_OFF"]

        sequence_name = c + "-wind-on-profile"
        sequences_df[sequence_name] = near_term.loc[year][c].values

    sequences_df.index = building.timeindex(
        year=str(scenario_year)
    )

    building.write_sequences(
        "volatile_profile.csv",
        sequences_df,
        directory=os.path.join(datapackage_dir, "data", "sequences"),
    )
