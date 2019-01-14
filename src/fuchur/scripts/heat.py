# -*- coding: utf-8 -*-
"""
"""
import json
import os
import pandas as pd

from datapackage import Package

from oemof.tools.economics import annuity
from oemof.tabular.datapackage import building

import fuchur


def load(config, datapackage_dir):
    """
    """
    # heat_load = pd.read_csv(
    #     os.path.join(fuchur.__RAW_DATA_PATH__, 'heat_load.csv'))

    elements = []
    for b in config["buses"]["heat"].get("central", []):
        elements.append(
            {
                "name": b + "-central-heat-load",
                "type": "load",
                "bus": b + "-central-heat",
                "amount": 200 * 1e6,
                "profile": b + "-central-heat-load-profile",
                "carrier": "heat",
            }
        )

    # add decentral heat load
    for b in config["buses"]["heat"].get("decentral", []):
        elements.append(
            {
                "name": b + "-decentral-heat-load",
                "type": "load",
                "bus": b + "-decentral-heat",
                "amount": 200 * 1e6,
                "profile": b + "-decentral-heat-load-profile",
                "carrier": "heat",
            }
        )

    if elements:
        building.write_elements(
            "load.csv", pd.DataFrame(elements).set_index("name"),
            directory=os.path.join(datapackage_dir, "data/elements")
        )


        central_heat_load_profile = pd.DataFrame(
            data=pd.read_csv(
                os.path.join(fuchur.__RAW_DATA_PATH__,
                             "central_heat_load_profiles.csv"),
                sep=";")[config["buses"]["heat"].get("central", [])].values,
                columns=[
                    p + "-central-heat-load-profile" for p in
                    config["buses"]["heat"].get("central", [])
                    ],
                index=pd.date_range(
                    str(config["temporal"]["scenario_year"]), periods=8760, freq="H"),
                )

        building.write_sequences(
            "load_profile.csv",
            central_heat_load_profile,
            directory=os.path.join(datapackage_dir, "data/sequences"))

        decentral_heat_load_profile = pd.DataFrame(
            data=pd.read_csv(
                os.path.join(fuchur.__RAW_DATA_PATH__,
                             "central_heat_load_profiles.csv"),
            sep=";")[config["buses"]["heat"].get("decentral", [])].values,
            columns=[
                p + "-decentral-heat-load-profile" for p in
                config["buses"]["heat"].get("decentral", [])
                ],
            index=pd.date_range(
                str(config["temporal"]["scenario_year"]), periods=8760, freq="H"),
            )

    building.write_sequences(
        "load_profile.csv",
        decentral_heat_load_profile,
        directory=os.path.join(datapackage_dir, "data/sequences"))


def decentral(config, datapackage_dir, techmap = {
        "backpressure": "backpressure",
        "boiler_decentral": "dispatchable",
        "heatpump_decentral": "conversion",
        "hotwatertank_decentral": "storage",
    }):

    wacc = config["cost"]["wacc"]

    technology_cost = Package(
        "https://raw.githubusercontent.com/ZNES-datapackages/"
        "technology-cost/master/datapackage.json"
    )

    technologies = pd.DataFrame(
        technology_cost.get_resource("decentral_heat").read(keyed=True)
    )

    technologies = (
        technologies.groupby(["year", "tech", "carrier"])
        .apply(lambda x: dict(zip(x.parameter, x.value)))
        .reset_index("carrier")
        .apply(lambda x: dict({"carrier": x.carrier}, **x[0]), axis=1)
    )
    technologies = technologies.loc[config["temporal"]["scenario_year"]].to_dict()


    carrier = pd.DataFrame(
        technology_cost.get_resource("carrier").read(keyed=True)
    ).set_index(['carrier', 'parameter'])

    # maybe we should prepare emission factors for scenario year...
    emission = carrier[carrier.year == 2015]  # 2015 as emission not change

    carrier = carrier[carrier.year ==
                      config["temporal"]["scenario_year"]]

    elements = dict()

    for b in config["buses"]["heat"].get("decentral", []):
        for tech, entry in technologies.items():
            element_name = b + "-" + tech
            heat_bus = b + "-decentral-heat"

            element = entry.copy()

            elements[element_name] = element

            if techmap.get(tech) == "backpressure":
                element.update(
                    {
                        "type": techmap[tech],
                        "fuel_bus": "GL-" + entry["carrier"],
                        "carrier": entry["carrier"],
                        "fuel_cost": carrier.at[(entry["carrier"], "cost"),
                                                "value"],
                        "electricity_bus": b + "-electricity",
                        "heat_bus": heat_bus,
                        "thermal_efficiency": entry["thermal_efficiency"],
                        "input_parameters": json.dumps(
                            {
                                "emission_factor": float(emission.at[
                                    (entry["carrier"], "emission-factor"),
                                    "value"
                                ])
                            }
                        ),
                        "electric_efficiency": entry["electrical_efficiency"],
                        "capacity_potential": "Infinity",
                        "tech": tech,
                        "capacity_cost": annuity(
                            float(entry["capacity_cost"]),
                            float(entry["lifetime"]),
                            wacc,
                        )
                        * 1000, # €/kW -> €/MW
                    }
                )

            elif techmap.get(tech) == "dispatchable":
                element.update(
                    {
                        "type": techmap[tech],
                        "carrier": entry["carrier"],
                        "marginal_cost": (
                            float(carrier.at[(entry["carrier"], "cost"),
                                             "value"])
                            / float(entry["efficiency"])
                        ),
                        "bus": heat_bus,
                        "output_parameters": json.dumps(
                            {
                                "emission_factor": float(emission.at[
                                    (entry["carrier"], "emission-factor"),
                                    "value"
                                ]) / float(entry["efficiency"])
                            }
                        ),
                        "capacity_potential": "Infinity",
                        "tech": tech,
                        "capacity_cost": annuity(
                            float(entry["capacity_cost"]),
                            float(entry["lifetime"]),
                            wacc,
                        )
                        * 1000,
                    }
                )

            elif techmap.get(tech) == "conversion":
                element.update(
                    {
                        "type": techmap[tech],
                        "carrier": entry["carrier"],
                        "from_bus": b + "-electricity",
                        "to_bus": heat_bus,
                        "efficiency": entry["efficiency"],
                        "capacity_potential": "Infinity",
                        "tech": tech,
                        "capacity_cost": annuity(
                            float(entry["capacity_cost"]),
                            float(entry["lifetime"]),
                            wacc,
                        )
                        * 1000,
                    }
                )

            elif techmap.get(tech) == "storage":
                element.update(
                    {
                        "storage_capacity_cost": annuity(
                            float(entry["storage_capacity_cost"]),
                            float(entry["lifetime"]),
                            wacc,
                        )
                        * 1000,
                        "bus": heat_bus,
                        "tech": tech,
                        "type": "storage",
                        "capacity_potential": "Infinity",
                        # rounttrip -> to in / out efficiency
                        "efficiency": float(entry["efficiency"])**0.5,
                        "capacity_ratio": entry["capacity_ratio"],
                    }
                )


    elements = pd.DataFrame.from_dict(elements, orient="index")

    for type in set(techmap.values()):
        building.write_elements(
            type + ".csv",
            elements.loc[elements["type"] == type].dropna(how="all", axis=1),
            directory=os.path.join(datapackage_dir, 'data', 'elements')
        )


def central(config, datapackage_dir,
            techmap={
                "extraction": "extraction",
                "boiler_central": "dispatchable",
                "hotwatertank_central": "storage",
                "heatpump_central": "conversion"
    }):
    """
    """

    wacc = config["cost"]["wacc"]

    technology_cost = Package(
        "https://raw.githubusercontent.com/ZNES-datapackages/"
        "technology-cost/master/datapackage.json"
    )

    technologies = pd.DataFrame(
        technology_cost.get_resource("central_heat").read(keyed=True)
    )

    technologies = (
        technologies.groupby(["year", "tech", "carrier"])
        .apply(lambda x: dict(zip(x.parameter, x.value)))
        .reset_index("carrier")
        .apply(lambda x: dict({"carrier": x.carrier}, **x[0]), axis=1)
    )
    technologies = technologies.loc[config["temporal"]["scenario_year"]].to_dict()


    carrier = pd.DataFrame(
        technology_cost.get_resource("carrier").read(keyed=True)
    ).set_index(['carrier', 'parameter'])

    # maybe we should prepare emission factors for scenario year...
    emission = carrier[carrier.year == 2015]  # 2015 as emission not change

    carrier = carrier[carrier.year ==
                      config["temporal"]["scenario_year"]]

    elements = dict()

    for b in config["buses"]["heat"].get("central", []):
        for tech, entry in technologies.items():
            element_name = b + "-" + tech
            heat_bus = b + "-central-heat"

            element = entry.copy()

            elements[element_name] = element

            if techmap.get(tech) == "extraction":
                element.update(
                    {
                        "type": techmap[tech],
                        "carrier": entry["carrier"],
                        "fuel_bus": "GL-" + entry["carrier"],
                        "carrier_cost": carrier.at[
                            (entry["carrier"], "cost"), "value"],
                        "electricity_bus": "DE-electricity",
                        "heat_bus": heat_bus,
                        "thermal_efficiency": entry["thermal_efficiency"],
                        "input_parameters": json.dumps(
                            {
                                "emission_factor": float(emission.at[
                                    (entry["carrier"], "emission-factor"),
                                    "value"
                                ])
                            }
                        ),
                        "electric_efficiency": entry["electrical_efficiency"],
                        "condensing_efficiency": entry["condensing_efficiency"],
                        "capacity_potential": "Infinity",
                        "tech": tech,
                        "capacity_cost": annuity(
                            float(entry["capacity_cost"]),
                            float(entry["lifetime"]),
                            wacc,
                        )
                        * 1000,
                    }
                )

            elif techmap.get(tech) == "backpressure":
                element.update(
                    {
                        "type": techmap[tech],
                        "fuel_bus": "GL-" + entry["carrier"],
                        "carrier": entry["carrier"],
                        "fuel_cost": carrier.at[(entry["carrier"], "cost"),
                                                "value"],
                        "electricity_bus": b + "-electricity",
                        "heat_bus": heat_bus,
                        "thermal_efficiency": entry["thermal_efficiency"],
                        "input_parameters": json.dumps(
                            {
                                "emission_factor": float(emission.at[
                                    (entry["carrier"], "emission-factor"),
                                    "value"
                                ])
                            }
                        ),
                        "electric_efficiency": entry["electrical_efficiency"],
                        "capacity_potential": "Infinity",
                        "tech": tech,
                        "capacity_cost": annuity(
                            float(entry["capacity_cost"]),
                            float(entry["lifetime"]),
                            wacc,
                        )
                        * 1000, # €/kW -> €/MW
                    }
                )

            elif techmap.get(tech) == "dispatchable":
                element.update(
                    {
                        "type": techmap[tech],
                        "carrier": entry["carrier"],
                        "marginal_cost": (
                            float(carrier.at[(entry["carrier"], "cost"),
                                             "value"])
                            / float(entry["efficiency"])
                        ),
                        "bus": heat_bus,
                        "output_parameters": json.dumps(
                            {
                                "emission_factor": float(emission.at[
                                    (entry["carrier"], "emission-factor"),
                                    "value"
                                ]) / float(entry["efficiency"])
                            }
                        ),
                        "capacity_potential": "Infinity",
                        "tech": tech,
                        "capacity_cost": annuity(
                            float(entry["capacity_cost"]),
                            float(entry["lifetime"]),
                            wacc,
                        )
                        * 1000,
                    }
                )

            elif techmap.get(tech) == "conversion":
                element.update(
                    {
                        "type": techmap[tech],
                        "carrier": entry["carrier"],
                        "from_bus": b + "-electricity",
                        "to_bus": heat_bus,
                        "efficiency": entry["efficiency"],
                        "capacity_potential": "Infinity",
                        "tech": tech,
                        "capacity_cost": annuity(
                            float(entry["capacity_cost"]),
                            float(entry["lifetime"]),
                            wacc,
                        )
                        * 1000,
                    }
                )

            elif techmap.get(tech) == "storage":
                element.update(
                    {
                        "storage_capacity_cost": annuity(
                            float(entry["storage_capacity_cost"]),
                            float(entry["lifetime"]),
                            wacc,
                        )
                        * 1000,
                        "bus": heat_bus,
                        "tech": tech,
                        "type": "storage",
                        "capacity_potential": "Infinity",
                        # rounttrip -> to in / out efficiency
                        "efficiency": float(entry["efficiency"])**0.5,
                        "capacity_ratio": entry["capacity_ratio"],
                    }
                )


    elements = pd.DataFrame.from_dict(elements, orient="index")

    for type in set(techmap.values()):
        building.write_elements(
            type + ".csv",
            elements.loc[elements["type"] == type].dropna(how="all", axis=1),
            directory=os.path.join(datapackage_dir, "data", "elements")
        )
