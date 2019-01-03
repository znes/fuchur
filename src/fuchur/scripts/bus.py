# -*- coding: utf-8 -*-
"""
This script constructs a pandas.Series `buses` with hub-names as index and
polygons of these buses as values. It uses the NUTS shapefile.

"""
import json
import os

from datapackage import Package
import pandas as pd
from geojson import FeatureCollection, Feature

from oemof.tabular.datapackage import building
from oemof.tabular.tools import geometry

import fuchur

def add(buses, datapackage_dir, raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    """

    filepath = os.path.join(raw_data_path,
                            'NUTS_2013_10M_SH/data/NUTS_RG_10M_2013.shp')

    if not os.path.exists(filepath):
        #TODO Adapt path for storing downloaded data
        filepath = building.download_data(
            "http://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/"
            "NUTS_2013_10M_SH.zip",
            unzip_file="NUTS_2013_10M_SH/data/NUTS_RG_10M_2013.shp",
        )

        building.download_data(
            "http://ec.europa.eu/eurostat/cache/GISCO/geodatafiles/"
            "NUTS_2013_10M_SH.zip",
            unzip_file="NUTS_2013_10M_SH/data/NUTS_RG_10M_2013.dbf",
        )

    # get nuts 1 regions for german neighbours

    nuts0 = pd.Series(geometry.nuts(filepath, nuts=0, tolerance=0.1))

    el_buses = pd.Series(name="geometry")
    el_buses.index.name = "name"

    for r in buses['electricity']:
        el_buses[r + "-electricity"] = nuts0[r]
    building.write_geometries(
        "bus.geojson", el_buses,
        os.path.join(datapackage_dir, "data/geometries"))

    # Add electricity buses
    bus_elements = {}
    for b in el_buses.index:
        bus_elements[b] = {
            "type": "bus",
            "carrier": "electricity",
            "geometry": b,
            "balanced": True,
        }

    # Add carrier buses
    commodities = {}

    bio_potential = (
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages/technology-potential/master/datapackage.json"
        )
        .get_resource("carrier")
        .read(keyed=True)
    )
    bio_potential = pd.DataFrame(bio_potential).set_index(
        ["country", "carrier"]
    )
    bio_potential = bio_potential.loc[
        bio_potential["source"] == "hotmaps"
    ].to_dict()

    if buses.get('biomass'):
        for b in buses['biomass']:
            bus_name = b + "-biomass-bus"
            commodity_name = b + "biomass-commodity"

            commodities[commodity_name] = {
                "type": "dispatchable",
                "carrier": 'biomass',
                "bus": bus_name,
                "capacity": float(bio_potential["value"].get((r, 'biomass'), 0))
                * 1e6,  # TWh -> MWh
                "output_parameters": json.dumps({"summed_max": 1}),
            }

            bus_elements[bus_name] = {
                "type": "bus",
                "carrier": 'biomass',
                "geometry": None,
                "balanced": True,
            }

    # Add heat buses per  sub_bus and region (r)
    for sub_bus, regions in buses["heat"].items():
        for region in regions:
            bus_elements['-'.join([region, "heat", sub_bus])] = {
                "type": "bus",
                "carrier": "heat",
                "geometry": None,
                "balanced": True,
            }

    path = building.write_elements(
        "commodity.csv",
        pd.DataFrame.from_dict(commodities, orient="index"),
        os.path.join(datapackage_dir, "data/elements"),
    )

    path = building.write_elements(
        "bus.csv",
        pd.DataFrame.from_dict(bus_elements, orient="index"),
        os.path.join(datapackage_dir, "data/elements"),
    )
