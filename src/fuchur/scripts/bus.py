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


def add(config, datapackage_dir):
    """
    """
    # Add bus geomtries
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

    buses = pd.Series(name="geometry")
    buses.index.name = "name"

    for r in config["regions"]:
        buses[r + "-electricity"] = nuts0[r]
    building.write_geometries("bus.geojson", buses)

    # Add electricity buses
    hub_elements = {}
    for b in buses.index:
        hub_elements[b] = {
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

    for p in config["primary_carrier"]:
        for r in config["regions"]:
            bus_name = r + "-" + p + "-bus"
            commodity_name = r + "-" + p + "-commodity"
            if p == "biomass":
                balanced = True
                commodities[commodity_name] = {
                    "type": "dispatchable",
                    "carrier": p,
                    "bus": bus_name,
                    "capacity": float(bio_potential["value"].get((r, p), 0))
                    * 1e6,  # TWh -> MWh
                    "output_parameters": json.dumps({"summed_max": 1}),
                }
            else:
                balanced = False

            hub_elements[bus_name] = {
                "type": "bus",
                "carrier": p,
                "geometry": None,
                "balanced": balanced,
            }

    # Add heat buses
    if config["heating"]:
        for b in config.get("central_heat_buses", []):
            hub_elements[b] = {
                "type": "bus",
                "carrier": "heat",
                "geometry": None,
                "balanced": True,
            }

        for b in config.get("decentral_heat_buses", []):
            hub_elements[b] = {
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
        pd.DataFrame.from_dict(hub_elements, orient="index"),
        os.path.join(datapackage_dir, "data/elements"),
    )
