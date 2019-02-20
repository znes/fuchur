# -*- coding: utf-8 -*-
"""
This script constructs a pandas.Series `buses` with hub-names as index and
polygons of these buses as values. It uses the NUTS shapefile.

"""

import os

from oemof.tabular.datapackage import building
from oemof.tabular.tools import geometry
import pandas as pd

import fuchur


def electricity(buses, datapackage_dir, raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    Parameters
    ----------
    buses: dict
        Dictionary with two keys: decentral and central and their values
        beeing the names of the buses
    datapackage_dir: str
        Directory of datapackage where resources are stored
    raw_data_path: str
        Path to directory where raw data can be found
    """

    filepath = building.download_data(
        'http://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/'
        'NUTS_2013_10M_SH.zip',
        unzip_file='NUTS_2013_10M_SH/data/NUTS_RG_10M_2013.shp',
        directory=raw_data_path)

    building.download_data(
        'http://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/'
        'NUTS_2013_10M_SH.zip',
        unzip_file='NUTS_2013_10M_SH/data/NUTS_RG_10M_2013.dbf',
        directory=raw_data_path)

    if not os.path.exists(filepath):
        print("Shapefile data not found. Did you download raw data?")
    # get nuts 1 regions for german neighbours

    nuts0 = pd.Series(geometry.nuts(filepath, nuts=0, tolerance=0.1))

    nuts0.index = [i.replace("UK", "GB") for i in nuts0.index]

    el_buses = pd.Series(name="geometry")
    el_buses.index.name = "name"

    for r in buses:
        el_buses[r + "-electricity"] = nuts0[r]
    building.write_geometries(
        "bus.geojson",
        el_buses,
        os.path.join(datapackage_dir, "data", "geometries"),
    )
    # Add electricity buses
    bus_elements = {}
    for b in el_buses.index:
        bus_elements[b] = {
            "type": "bus",
            "carrier": "electricity",
            "geometry": b,
            "balanced": True,
        }

    building.write_elements(
        "bus.csv",
        pd.DataFrame.from_dict(bus_elements, orient="index"),
        os.path.join(datapackage_dir, "data", "elements"),
    )


def heat(buses, datapackage_dir):
    """
    Parameters
    ----------
    buses: dict
        Dictionary with two keys: decentral and central and their values
        beeing the names of the buses
    datapackage_dir: str
        Directory of datapackage where resources are stored
    """
    bus_elements = {}
    # Add heat buses per  sub_bus and region (r)
    for sub_bus, regions in buses.items():
        for region in regions:
            bus_elements["-".join([region, "heat", sub_bus])] = {
                "type": "bus",
                "carrier": "heat",
                "geometry": None,
                "balanced": True,
            }

    building.write_elements(
        "bus.csv",
        pd.DataFrame.from_dict(bus_elements, orient="index"),
        os.path.join(datapackage_dir, "data", "elements"),
    )
