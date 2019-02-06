# -*- coding: utf-8 -*-
"""
"""
import os
import re

from oemof.tabular.datapackage import building
import pandas as pd

import fuchur


def _prepare_frame(df):
    """ Prepare dataframe from ehighway excel sheet, see function:
    ehighway_grid()
    """
    df.dropna(how="all", axis=1, inplace=True)
    df.drop(df.tail(1).index, inplace=True)
    df.reset_index(inplace=True)
    df["Links"] = df["Links"].apply(lambda row: row.upper())

    df["Links"] = [i.replace("UK", "GB") for i in df["Links"]]  # for ISO code

    # remove all links inside countries
    df = df.loc[df["Links"].apply(_remove_links)]

    # strip down to letters only for grouping
    df["Links"] = df["Links"].apply(lambda row: re.sub(r"[^a-zA-Z]+", "", row))

    df = df.groupby(df["Links"]).sum()

    df.reset_index(inplace=True)

    df = pd.concat(
        [
            pd.DataFrame(
                df["Links"].apply(lambda row: [row[0:2], row[2:4]]).tolist(),
                columns=["from", "to"],
            ),
            df,
        ],
        axis=1,
    )

    return df


# helper function for transshipment
def _remove_links(row):
    """ Takes a row of the dataframe and returns True if the
    link is within the country.
    """
    r = row.split("-")
    if r[0].split("_")[1].strip() == r[1].split("_")[1].strip():
        return False
    else:
        return True


def ehighway_grid(buses, year, datapackage_dir, scenario = "100% RES",
                  raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    Parameter
    ---------
    buses: array like
        List with buses represented by iso country code
    year: integer
        Scenario year to select. One of: 2030, 2050. If year is 2030, the
        starting grid will be used, meaning the scenario argument will have no
        impact
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

    if os.path.exists(filepath):
        # if file exist in archive use this file
        df_2030 = pd.read_excel(
            filepath, sheet_name="T93", index_col=[1], skiprows=[0, 1, 3]
        ).fillna(0)

        df_2050 = pd.read_excel(
            filepath, sheet_name="T94", index_col=[1], skiprows=[0, 1, 3]
        ).fillna(0)
    else:
        raise FileNotFoundError(
            "File for e-Highway capacities does not exist. Did you download?"
        )

    df_2050 = _prepare_frame(df_2050).set_index(["Links"])
    df_2030 = _prepare_frame(df_2030).set_index(["Links"])

    elements = {}
    for idx, row in df_2030.iterrows():
        if (
            row["from"] in buses
            and row["to"] in buses
        ):

            predecessor = row["from"] + "-electricity"
            successor = row["to"] + "-electricity"
            element_name = predecessor + "-" + successor

            if year == 2030:
                capacity = row[scenario]
            elif year == 2050:
                capacity = (row[scenario]
                + df_2050.to_dict()[scenario].get(idx, 0))

            element = {
                "type": "link",
                "loss": 0.05,
                "from_bus": predecessor,
                "to_bus": successor,
                "tech": "transshipment",
                "capacity": capacity,
                #"length": row["Length"],
            }

            elements[element_name] = element

    building.write_elements(
        "link.csv",
        pd.DataFrame.from_dict(elements, orient="index"),
        directory=os.path.join(datapackage_dir, "data/elements"),
    )


def tyndp_grid(buses, datapackage_dir, raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    Parameter
    ---------
    buses: array like
        List with buses represented by iso country code
    datapackage_dir: string
        Directory for tabular resource
    raw_data_path: string
        Path where raw data file `e-Highway_database_per_country-08022016.xlsx`
        is located
    """
    filepath = building.download_data(
        "https://www.entsoe.eu/Documents/TYNDP%20documents/TYNDP2018/"
        "Scenarios%20Data%20Sets/Input%20Data.xlsx",
        directory=raw_data_path)

    df = pd.read_excel(filepath, sheet_name='NTC', index_col=[0],
                       skiprows=[1,2])[["CBA Capacities", "Unnamed: 3"]]
    df.columns = ["=>", "<="]
    df["links"] = df.index.astype(str)
    df["links"] =  df["links"].apply(
        lambda row: (row.split('-')[0][0:2], row.split('-')[1][0:2]))
    df = df.groupby(df["links"]).sum()
    df.reset_index(inplace=True)

    df = pd.concat([
        pd.DataFrame(df["links"].apply(lambda row: [row[0], row[1]]).tolist(),
                     columns=['from', 'to']),
        df[["=>", "<="]]], axis=1)

    elements = {}
    for idx, row in df.iterrows():
        if (
            row["from"] in buses
            and row["to"] in buses
        ) and row["from"] != row["to"]:

            predecessor = row["from"] + "-electricity"
            successor = row["to"] + "-electricity"
            element_name = predecessor + "-" + successor

            element = {
                "type": "link",
                "loss": 0.05,
                "from_bus": predecessor,
                "to_bus": successor,
                "tech": "transshipment",
                "capacity": row["=>"]  # still need to think how to
            }

            elements[element_name] = element

    building.write_elements(
        "link.csv",
        pd.DataFrame.from_dict(elements, orient="index"),
        directory=os.path.join(datapackage_dir, "data", "elements"),
    )
