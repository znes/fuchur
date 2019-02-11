# -*- coding: utf-8 -*-
"""
"""
import os

from oemof.tabular.datapackage import building

import pandas as pd

import fuchur


def tyndp(buses, scenario, datapackage_dir,
               raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    """
    filepath = building.download_data(
        "https://www.entsoe.eu/Documents/TYNDP%20documents/TYNDP2018/"
        "Scenarios%20Data%20Sets/Input%20Data.xlsx",
        directory=raw_data_path)

    df = pd.read_excel(filepath, sheet_name='Demand')
    df['countries'] = [i[0:2] for i in df.index]  # for aggregation by country

    elements = df.groupby('countries').sum()[scenario].to_frame()
    elements.index.name = "bus"
    elements = elements.loc[buses]
    elements.reset_index(inplace=True)
    elements["name"] = elements.apply(
        lambda row: row.bus + "-electricity-load", axis=1
    )
    elements["profile"] = elements.apply(
        lambda row: row.bus + "-electricity-load-profile", axis=1
    )
    elements["type"] = "load"
    elements["carrier"] = "electricity"
    elements.set_index("name", inplace=True)
    elements.bus = [b + "-electricity" for b in elements.bus]
    elements["amount"] = elements[scenario] * 1000   # MWh -> GWh

    building.write_elements(
        "load.csv",
        elements,
        directory=os.path.join(datapackage_dir, "data", "elements"),
    )

def ehighway(buses, year, datapackage_dir, scenario="100% RES",
                  raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    Parameter
    ---------
    buses: array like
        List with buses represented by iso country code
    year: integer
        Scenario year to select. One of: 2040, 2050
    datapackage_dir: string
        Directory for tabular resource
    scenario:
        Name of ehighway scenario to select. One of:
        ["Large Scale RES", "100% RES", "Big & Market", "Fossil & Nuclear",
         "Small & Local"], default: "100% RES"
    raw_data_path: string
        Path where raw data file `e-Highway_database_per_country-08022016.xlsx`
        is located
    """
    filename = "e-Highway_database_per_country-08022016.xlsx"
    filepath = os.path.join(raw_data_path, filename)

    if year == 2050:
        sheet = "T40"
    elif year == 2040:
        sheet = "T39"
    else:
        raise ValueError(
            "Value of argument `year` must be integer 2040 or 2050!")

    if os.path.exists(filepath):
        df = pd.read_excel(filepath, sheet_name=sheet, index_col=[0],
                           skiprows=[0,1])
    else:
        raise FileNotFoundError(
            "File for e-Highway loads does not exist. Did you download data?"
        )

    df.set_index("Scenario", inplace=True)  # Scenario in same colum as ctrcode
    df.drop(df.index[0:1], inplace=True)  # remove row with units
    df.dropna(how="all", axis=1, inplace=True)

    elements = df.loc[buses, scenario].to_frame()
    elements = elements.rename(columns={scenario: "amount"})
    elements.index.name = "bus"
    elements.reset_index(inplace=True)
    elements["name"] = elements.apply(
        lambda row: row.bus + "-electricity-load", axis=1
    )
    elements["profile"] = elements.apply(
        lambda row: row.bus + "-electricity-load-profile", axis=1
    )
    elements["type"] = "load"
    elements["carrier"] = "electricity"
    elements.set_index("name", inplace=True)
    elements.bus = [b + "-electricity" for b in elements.bus]
    elements["amount"] = elements["amount"] * 1000  # to MWh

    path = os.path.join(datapackage_dir, "data", "elements")
    building.write_elements("load.csv", elements, directory=path)


def opsd_profile(buses, demand_year, scenario_year, datapackage_dir,
                      raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    Parameter
    ---------
    buses: array like
        List with buses represented by iso country code
    demand_year: integer or string
        Demand year to select
    scenario_year: integer or string
        Year of scenario to use for timeindex to resource
    datapackage_dir: string
        Directory for tabular resource
    raw_data_path: string
        Path where raw data file
        is located
    """

    filepath = building.download_data(
        "https://data.open-power-system-data.org/time_series/2018-06-30/time_series_60min_singleindex.csv",
        directory=raw_data_path
    )

    if os.path.exists(filepath):
        raw_data = pd.read_csv(filepath, index_col=[0], parse_dates=True)
    else:
        raise FileNotFoundError(
            "File for OPSD loads does not exist. Did you download data?"
        )

    suffix = "_load_old"

    countries = buses

    columns = [c + suffix for c in countries]

    timeseries = raw_data[str(demand_year)][columns]

    if timeseries.isnull().values.any():
        raise ValueError(
            "Timeseries for load has NaN values. Select "
            + "another demand year or use another data source."
        )

    load_total = timeseries.sum()

    load_profile = timeseries / load_total

    sequences_df = pd.DataFrame(index=load_profile.index)

    elements = building.read_elements(
        "load.csv", directory=os.path.join(datapackage_dir, "data", "elements")
    )

    for c in countries:
        # get sequence name from elements edge_parameters
        # (include re-exp to also check for 'elec' or similar)
        sequence_name = elements.at[
            elements.index[elements.index.str.contains(c)][0], "profile"
        ]

        sequences_df[sequence_name] = load_profile[c + suffix].values

    if sequences_df.index.is_leap_year[0]:
        sequences_df = sequences_df.loc[
            ~((sequences_df.index.month == 2) & (sequences_df.index.day == 29))
        ]

    sequences_df.index = building.timeindex(
        year=str(scenario_year)
    )

    building.write_sequences(
        "load_profile.csv",
        sequences_df,
        directory=os.path.join(datapackage_dir, "data/sequences"),
    )
