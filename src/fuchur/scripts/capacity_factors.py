# -*- coding: utf-8 -*-
"""
"""
import os

import pandas as pd

from oemof.tabular.datapackage import building

import fuchur


def pv(config, datapackage_dir):
    """
    """
    filepath = os.path.join(fuchur.__RAW_DATA_PATH__,
                            "ninja_pv_europe_v1.1_merra2.csv")
    year = str(config["temporal"]["weather_year"])

    countries = config["buses"]["electricity"]

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
        year=str(config["temporal"]["scenario_year"]))
    building.write_sequences(
        "volatile_profile.csv",
        sequences_df,
        directory=os.path.join(datapackage_dir, "data", "sequences"),
    )


def wind(config, datapackage_dir):
    """
    """
    off_filepath = os.path.join(
        fuchur.__RAW_DATA_PATH__,
        "ninja_wind_europe_v1.1_future_nearterm_on-offshore.csv")

    near_term_path = os.path.join(
        fuchur.__RAW_DATA_PATH__,
        "ninja_wind_europe_v1.1_current_national.csv")

    year = str(config["temporal"]["weather_year"])

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

    for c in config["buses"]["electricity"]:
        # add offshore profile if country exists in offshore data columns
        # and if its in NorthSea
        if [col for col in offshore_data.columns if c + "_OFF" in col] and \
        c in NorthSea:
            sequences_df[c + "-wind-off-profile"] = offshore_data[c + "_OFF"]


        sequence_name = c + "-wind-on-profile"
        sequences_df[sequence_name] = near_term.loc[year][c].values

    sequences_df.index = building.timeindex(
        year=str(config['temporal']["scenario_year"]))

    building.write_sequences(
        "volatile_profile.csv",
        sequences_df,
        directory=os.path.join(datapackage_dir, "data", "sequences"),
    )
