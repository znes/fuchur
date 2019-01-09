# -*- coding: utf-8 -*-
"""
"""
import json
from datapackage import Resource, Package
import pandas as pd
from datapackage_utilities import building

config = building.get_config()

def load():

    elements = []
    for b in config.get("central_heat_buses", []):
        elements.append(
            {
                "name": b + "-load",
                "type": "load",
                "bus": b,
                "amount": 200 * 1e6,
                "profile": "DE-heat-profile",
                "carrier": "heat",
            }
        )

    for b in config.get("decentral_heat_buses", []):
        elements.append(
            {
                "name": b + "-load",
                "type": "load",
                "bus": b,
                "amount": 200 * 1e6,
                "profile": "DE-heat-profile",
                "carrier": "heat",
            }
        )

    path = building.write_elements(
        "load.csv", pd.DataFrame(elements).set_index("name")
    )

    heat_demand_profile = pd.Series(
        data=pd.read_csv("archive/thermal_load_profile.csv", sep=";")[
            "thermal_load"
        ].values,
        index=pd.date_range(str(config["year"]), periods=8760, freq="H"),
    )
    heat_demand_profile.rename("DE-heat-profile", inplace=True)

    path = building.write_sequences("load_profile.csv", heat_demand_profile)

def decentral():
    techmap = {
        "backpressure": "backpressure",
        "boiler_decentral": "dispatchable",
        "electricity_heatpump": "conversion",
        "gas_heatpump": "dispatchable",
        "hotwatertank_decentral": "storage",
    }

    config = building.get_config()
    wacc = config["wacc"]

    technologies = pd.DataFrame(
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages/technology-cost/master/datapackage.json"
        )
        .get_resource("decentral_heat")
        .read(keyed=True)
    )
    technologies = (
        technologies.groupby(["year", "tech", "carrier"])
        .apply(lambda x: dict(zip(x.parameter, x.value)))
        .reset_index("carrier")
        .apply(lambda x: dict({"carrier": x.carrier}, **x[0]), axis=1)
    )
    technologies = technologies.loc[config["year"]].to_dict()


    carrier = pd.read_csv("archive/carrier.csv", index_col=[0, 1]).loc[
        ("base", config["year"])
    ]
    carrier.set_index("carrier", inplace=True)

    elements = dict()

    for b in config.get("decentral_heat_buses", []):
        for tech, entry in technologies.items():
            element_name = tech + "-" + b

            element = entry.copy()

            elements[element_name] = element

            if techmap.get(tech) == "backpressure":
                element.update(
                    {
                        "type": techmap[tech],
                        "fuel_bus": "GL-gas",
                        "carrier": entry["carrier"],
                        "fuel_cost": carrier.at[entry["carrier"], "cost"],
                        "electricity_bus": "DE-electricity",
                        "heat_bus": b,
                        "thermal_efficiency": entry["thermal_efficiency"],
                        "input_edge_parameters": json.dumps(
                            {
                                "emission_factor": carrier.at[
                                    entry["carrier"], "emission"
                                ]
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
                        * 1000,
                    }
                )

            elif techmap.get(tech) == "dispatchable":
                element.update(
                    {
                        "type": techmap[tech],
                        "carrier": entry["carrier"],
                        "marginal_cost": (
                            carrier.at[entry["carrier"], "cost"]
                            / float(entry["efficiency"])
                        ),
                        "bus": b,
                        "edge_parameters": json.dumps(
                            {
                                "emission_factor": carrier.at[
                                    entry["carrier"], "emission"
                                ]
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
                        "from_bus": "DE-electricity",
                        "to_bus": b,
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
                        "bus": b,
                        "tech": tech,
                        "type": "storage",
                        "capacity_potential": "Infinity",
                        "efficiency": entry["efficiency"],
                        "capacity_ratio": entry["capacity_ratio"],
                    }
                )


    elements = pd.DataFrame.from_dict(elements, orient="index")


    for type in set(techmap.values()):
        building.write_elements(
            type + ".csv",
            elements.loc[elements["type"] == type].dropna(how="all", axis=1),
        )


def central():
    """
    """
    techmap = {
        "extraction": "extraction",
        "boiler_central": "dispatchable",
        "hotwatertank_central": "storage",
    }

    config = building.get_config()
    wacc = config["wacc"]

    technologies = pd.DataFrame(
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages/technology-cost/master/datapackage.json"
        )
        .get_resource("central_heat")
        .read(keyed=True)
    )
    technologies = (
        technologies.groupby(["year", "tech", "carrier"])
        .apply(lambda x: dict(zip(x.parameter, x.value)))
        .reset_index("carrier")
        .apply(lambda x: dict({"carrier": x.carrier}, **x[0]), axis=1)
    )
    technologies = technologies.loc[config["year"]].to_dict()

    carrier = pd.read_csv("archive/carrier.csv", index_col=[0, 1]).loc[
        ("base", config["year"])
    ]
    carrier.set_index("carrier", inplace=True)

    elements = dict()

    for b in config.get("central_heat_buses", []):
        for tech, entry in technologies.items():
            element_name = tech + "-" + b

            element = entry.copy()

            elements[element_name] = element

            if techmap.get(tech) == "backpressure":
                element.update(
                    {
                        "type": techmap[tech],
                        "fuel_bus": "GL-gas",
                        "carrier": entry["carrier"],
                        "fuel_cost": carrier.at[entry["carrier"], "cost"],
                        "electricity_bus": "DE-electricity",
                        "heat_bus": b,
                        "thermal_efficiency": entry["thermal_efficiency"],
                        "input_edge_parameters": json.dumps(
                            {
                                "emission_factor": carrier.at[
                                    entry["carrier"], "emission"
                                ]
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
                        * 1000,
                    }
                )

            elif techmap.get(tech) == "extraction":
                element.update(
                    {
                        "type": techmap[tech],
                        "carrier": entry["carrier"],
                        "fuel_bus": "GL-gas",
                        "carrier_cost": carrier.at[entry["carrier"], "cost"],
                        "electricity_bus": "DE-electricity",
                        "heat_bus": b,
                        "thermal_efficiency": entry["thermal_efficiency"],
                        "input_edge_parameters": json.dumps(
                            {
                                "emission_factor": carrier.at[
                                    entry["carrier"], "emission"
                                ]
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

            elif techmap.get(tech) == "dispatchable":
                element.update(
                    {
                        "type": techmap[tech],
                        "carrier": entry["carrier"],
                        "marginal_cost": carrier.at[entry["carrier"], "cost"]
                        / float(entry["efficiency"]),
                        "bus": b,
                        "edge_parameters": json.dumps(
                            {
                                "emission_factor": carrier.at[
                                    entry["carrier"], "emission"
                                ]
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
                        "from_bus": "DE-electricity",
                        "to_bus": b,
                        "efficiency": entry["thermal_efficiency"],
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
                        "bus": b,
                        "tech": tech,
                        "type": "storage",
                        "capacity_potential": "Infinity",
                        "efficiency": entry["efficiency"],
                        "capacity_ratio": entry["capacity_ratio"],
                    }
                )


    elements = pd.DataFrame.from_dict(elements, orient="index")


    for type in set(techmap.values()):
        building.write_elements(
            type + ".csv",
            elements.loc[elements["type"] == type].dropna(how="all", axis=1),
        )
