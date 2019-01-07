# -*- coding: utf-8 -*-
"""
"""
import os
import json
import pandas as pd

from oemof.tabular.datapackage import building

import fuchur

def pv(config, datapackage_dir):
    """
    """
    # filepath = building.download_data(
    #     "https://www.renewables.ninja/static/downloads/ninja_europe_pv_v1.1.zip",
    #     unzip_file="ninja_pv_europe_v1.1_merra2.csv",
    #     local_path=os.path.join(datapackage_dir, "cache"),
    # )
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

    sequences_df.index = building.timeindex(year=config["temporal"]["scenario_year"])
    path = building.write_sequences(
        "volatile_profile.csv",
        sequences_df,
        directory=os.path.join(datapackage_dir, "data", "sequences"),
    )


def wind(config, datapackage_dir):
    """
    """
    # off_filepath = building.download_data(
    #     "https://www.renewables.ninja/static/downloads/ninja_europe_wind_v1.1.zip",
    #     unzip_file="ninja_wind_europe_v1.1_future_nearterm_on-offshore.csv",
    #     local_path=os.path.join(datapackage_dir, "cache"),
    # )
    #
    # near_term_path = building.download_data(
    #     "https://www.renewables.ninja/static/downloads/ninja_europe_wind_v1.1.zip",
    #     unzip_file="ninja_wind_europe_v1.1_current_national.csv",
    #     local_path=os.path.join(datapackage_dir, "cache"),
    # )

    off_filepath = os.path.join(fuchur.__RAW_DATA_PATH__,
                                "ninja_wind_europe_v1.1_future_nearterm_on-offshore.csv")

    near_term_path = os.path.join(fuchur.__RAW_DATA_PATH__,
                                "ninja_wind_europe_v1.1_current_national.csv")

    year = str(config["temporal"]["weather_year"])

    # not in ninja dataset, as new market zones? (replace by german factor)
    missing = ["LU" "CZ" "AT" "CH"]

    countries = list(set(config["buses"]["electricity"]) - set(missing))

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

    for c in config["buses"]["electricity"]:
        # add offshore profile if country exists in offshore data columns
        if [col for col in offshore_data.columns if c + "_OFF" in col]:
            sequences_df[c + "-wind-off-profile"] = offshore_data[c + "_OFF"]
        # hack as poland is not in ninja, therfore we take SE offshore profile
        elif c == "PL":
            sequences_df[c + "-wind-off-profile"] = offshore_data["SE_OFF"]

        sequence_name = c + "-wind-on-profile"

        sequences_df[sequence_name] = near_term.loc[year][c].values

    sequences_df.index = building.timeindex(
        year=config['temporal']["scenario_year"])

    path = building.write_sequences(
        "volatile_profile.csv",
        sequences_df,
        directory=os.path.join(datapackage_dir, "data", "sequences"),
    )
