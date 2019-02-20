

import json
import os

from datapackage import Package
import pandas as pd

from oemof.tabular.datapackage import building


def add(buses, datapackage_dir):
    """
    """
    commodities = {}
    bus_elements = {}

    bio_potential = (
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages/"
            "technology-potential/master/datapackage.json"
        )
        .get_resource("carrier")
        .read(keyed=True)
    )
    bio_potential = pd.DataFrame(bio_potential).set_index(
        ["country", "carrier"]
    )
    bio_potential.rename(index={"UK": "GB"}, inplace=True)

    bio_potential = bio_potential.loc[
        bio_potential["source"] == "hotmaps"
    ].to_dict()

    if buses.get("biomass"):
        for b in buses["biomass"]:
            bus_name = '-'.join([b,"biomass", "bus"])
            commodity_name = '-'.join([b, "biomass", "commodity"])

            commodities[commodity_name] = {
                "type": "dispatchable",
                "carrier": "biomass",
                "bus": bus_name,
                "capacity": float(
                    bio_potential["value"].get((b, "biomass"), 0)
                )
                * 1e6,  # TWh -> MWh
                "output_parameters": json.dumps({"summed_max": 1}),
            }

            bus_elements[bus_name] = {
                "type": "bus",
                "carrier": "biomass",
                "geometry": None,
                "balanced": True,
            }

    if commodities:
        building.write_elements(
            "commodity.csv",
            pd.DataFrame.from_dict(commodities, orient="index"),
            os.path.join(datapackage_dir, "data/elements"),
        )

    building.write_elements(
        "bus.csv",
        pd.DataFrame.from_dict(bus_elements, orient="index"),
        os.path.join(datapackage_dir, "data/elements"),
    )
