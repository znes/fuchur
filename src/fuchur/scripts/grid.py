# -*- coding: utf-8 -*-
"""
"""
import json
import os
import re
import pandas as pd

from geojson import FeatureCollection, Feature
from oemof.tabular.datapackage import building
from oemof.tabular.tools import geometry
from oemof.tools.economics import annuity

import fuchur

def _prepare_frame(df):
    """ prepare dataframe
    """
    df.dropna(how="all", axis=1, inplace=True)
    df.drop(df.tail(1).index, inplace=True)
    df.reset_index(inplace=True)
    df["Links"] = df["Links"].apply(lambda row: row.upper())

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


def add(buses, datapackage_dir, raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    """

    filename = "e-Highway_database_per_country-08022016.xlsx"

    filepath = os.path.join(raw_data_path,
                            filename)

    if os.path.exists(filepath):
        # if file exist in archive use this file
        df_2030 = pd.read_excel(
            filepath, sheet_name="T93", index_col=[1], skiprows=[0, 1, 3]
        ).fillna(0)

        df_2050 = pd.read_excel(
            filepath, sheet_name="T94", index_col=[1], skiprows=[0, 1, 3]
        ).fillna(0)
    else:
        # if file does not exist, try to download and check if valid xlsx file
        print("File for e-Highway capacities does not exist. Download..")
        filepath = building.download_data(
            "http://www.e-highway2050.eu/fileadmin/documents/" + filename,
            local_path=os.path.join(datapackage_dir, "cache"),
        )
        try:
            book = open_workbook(filepath)
            df_2030 = pd.read_excel(
                filepath, sheet_name="T93", index_col=[1], skiprows=[0, 1, 3]
            ).fillna(0)

            df_2050 = pd.read_excel(
                filepath, sheet_name="T94", index_col=[1], skiprows=[0, 1, 3]
            ).fillna(0)
        except XLRDError as e:
            raise XLRDError("Downloaded file not valid xlsx file.")

    df_2050 = _prepare_frame(df_2050).set_index(["Links"])
    df_2030 = _prepare_frame(df_2030).set_index(["Links"])

    scenario = "100% RES"

    elements = {}
    for idx, row in df_2030.iterrows():
        if row["from"] in buses["electricity"] and \
            row["to"] in buses["electricity"]:

            predecessor = row["from"] + "-electricity"
            successor = row["to"] + "-electricity"
            element_name = predecessor + "-" + successor

            element = {
                "type": "link",
                "loss": 0.05,
                "from_bus": predecessor,
                "to_bus": successor,
                "tech": "transshipment",
                "capacity": row[scenario]
                + df_2050.to_dict()[scenario].get(idx, 0),
                "length": row["Length"],
            }

            elements[element_name] = element

    path = building.write_elements(
        "link.csv",
        pd.DataFrame.from_dict(elements, orient="index"),
        directory=os.path.join(datapackage_dir, "data/elements"),
    )


# create_resource(path)
