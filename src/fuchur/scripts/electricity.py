# -*- coding: utf-8 -*-
"""
"""
import json
import os

from datapackage import Package

from oemof.tabular.datapackage import building
from oemof.tools.economics import annuity
import pandas as pd

import fuchur



def generic_investment(buses, investment_technologies, year, wacc,
    potential_source, datapackage_dir, cost_factor={}, techmap = {
        "ocgt": "dispatchable",
        "ccgt": "dispatchable",
        "st": "dispatchable",
        "ce": "dispatchable",
        "pv": "volatile",
        "wind_onshore": "volatile",
        "wind_offshore": "volatile",
        "biomass": "conversion",
        "lithium_battery": "storage",
        "acaes": "storage"}):
    """
    """


    technologies = pd.DataFrame(
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages/"
            "technology-cost/master/datapackage.json"
        )
        .get_resource("electricity")
        .read(keyed=True)
    )
    technologies = (
        technologies.groupby(["year", "tech", "carrier"])
        .apply(lambda x: dict(zip(x.parameter, x.value)))
        .reset_index("carrier")
        .apply(lambda x: dict({"carrier": x.carrier}, **x[0]), axis=1)
    )
    technologies = technologies.loc[year].to_dict()

    potential = (
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages/"
            "technology-potential/master/datapackage.json"
        )
        .get_resource("renewable")
        .read(keyed=True)
    )
    potential = pd.DataFrame(potential).set_index(["country", "tech"])
    potential = potential.loc[
        potential["source"] == potential_source
    ].to_dict()

    for tech in technologies:
        technologies[tech]["capacity_cost"] = technologies[tech][
            "capacity_cost"
        ] * cost_factor.get(tech, 1)
        if "storage_capacity_cost" in technologies[tech]:
            technologies[tech]["storage_capacity_cost"] = technologies[tech][
                "storage_capacity_cost"
            ] * cost_factor.get(tech, 1)

    # TODO: replace by datapackage
    carrier = pd.read_csv(
        os.path.join(fuchur.__RAW_DATA_PATH__, "carrier.csv"), index_col=[0, 1]
    ).loc[("base", year)]
    carrier.set_index("carrier", inplace=True)

    elements = {}

    for r in buses:
        for tech, data in technologies.items():
            if tech in investment_technologies:

                element = data.copy()
                elements[r + "-" + tech] = element

                if techmap.get(tech) == "dispatchable":
                    element.update(
                        {
                            "capacity_cost": annuity(
                                float(data["capacity_cost"]),
                                float(data["lifetime"]),
                                wacc,
                            )
                            * 1000,  # €/kW -> €/MW
                            "bus": r + "-electricity",
                            "type": "dispatchable",
                            "marginal_cost": (
                                carrier.loc[data["carrier"]].cost
                                + carrier.loc[data["carrier"]].emission
                                * carrier.loc["co2"].cost
                            )
                            / float(data["efficiency"]),
                            "tech": tech,
                            "capacity_potential": potential[
                                "capacity_potential"
                            ].get((r, tech), "Infinity"),
                            "output_parameters": json.dumps(
                                {
                                    "emission_factor": (
                                        carrier.loc[data["carrier"]].emission
                                        / float(data["efficiency"])
                                    )
                                }
                            ),
                        }
                    )

                if techmap.get(tech) == "conversion":
                    element.update(
                        {
                            "capacity_cost": annuity(
                                float(data["capacity_cost"]),
                                float(data["lifetime"]),
                                wacc,
                            )
                            * 1000,  # €/kW -> €/M
                            "to_bus": r + "-electricity",
                            "from_bus": r + "-" + data["carrier"] + "-bus",
                            "type": "conversion",
                            "marginal_cost": (
                                carrier.loc[data["carrier"]].cost
                                + carrier.loc[data["carrier"]].emission
                                * carrier.loc["co2"].cost
                            )
                            / float(data["efficiency"]),
                            "tech": tech,
                        }
                    )

                    # ep = {'summed_max': float(bio_potential['value'].get(
                    #     (r, tech), 0)) * 1e6}) # TWh to MWh

                if techmap.get(tech) == "volatile":
                    NorthSea = ["DE", "DK", "NO", "NL", "BE", "GB", "SE"]

                    if "wind_off" in tech:
                        profile = r + "-wind-off-profile"
                    elif "wind_on" in tech:
                        profile = r + "-wind-on-profile"
                    elif "pv" in tech:
                        profile = r + "-pv-profile"

                    e = {
                        "capacity_cost": annuity(
                            float(data["capacity_cost"]),
                            float(data["lifetime"]),
                            wacc,
                        )
                        * 1000,
                        "capacity_potential": potential[
                            "capacity_potential"
                        ].get((r, tech), 0),
                        "bus": r + "-electricity",
                        "tech": tech,
                        "type": "volatile",
                        "profile": profile,
                    }
                    # only add all technologies that are not offshore or
                    # if offshore in the NorthSea list
                    if "wind_off" in tech and r not in NorthSea:
                        pass
                    else:
                        element.update(e)

                elif techmap[tech] == "storage":
                    if tech == "acaes" and r != "DE":
                        capacity_potential = 0
                    else:
                        capacity_potential = "Infinity"
                    element.update(
                        {
                            "capacity_cost": annuity(
                                float(data["capacity_cost"])
                                + float(data["storage_capacity_cost"])
                                / float(data["capacity_ratio"]),
                                float(data["lifetime"]),
                                wacc,
                            )
                            * 1000,
                            "bus": r + "-electricity",
                            "tech": tech,
                            "type": "storage",
                            "efficiency": float(data["efficiency"])
                            ** 0.5,  # convert roundtrip to
                            # input / output efficiency
                            "marginal_cost": 0.0000001,
                            "loss": 0.01,
                            "capacity_potential": capacity_potential,
                            "capacity_ratio": data["capacity_ratio"],
                        }
                    )

    df = pd.DataFrame.from_dict(elements, orient="index")
    # drop storage capacity cost to avoid duplicate investment
    df = df.drop("storage_capacity_cost", axis=1)

    df = df[(df[["capacity_potential"]] != 0).all(axis=1)]

    # write elements to CSV-files
    for element_type in set(techmap.values()):
        building.write_elements(
            element_type + ".csv",
            df.loc[df["type"] == element_type].dropna(how="all", axis=1),
            directory=os.path.join(datapackage_dir, "data/elements"),
        )


def tyndp_generation(buses, vision, scenario_year, datapackage_dir,
                     raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    """
    filepath = building.download_data(
        "https://www.entsoe.eu/Documents/TYNDP%20documents/TYNDP%202016/rgips/"
        "TYNDP2016%20market%20modelling%20data.xlsx",
        directory=raw_data_path)
    df = pd.read_excel(filepath, sheet_name="NGC")

    efficiencies = {
        'biomass': 0.45,
        'coal': 0.45,
        'gas': 0.5,
        'uranium': 0.35,
        'oil': 0.35,
        'lignite': 0.4}

    max = {
        'biomass': 0.85,
        'coal': 0.85,
        'gas': 0.85,
        'uranium': 0.85,
        'oil': 0.85,
        'lignite': 0.85
    }

    visions = {
        'vision1': 41,
        'vision2': 80,
        'vision3': 119,
        'vision4': 158
     }
    # 41:77 for 2030 vision 1
    # 80:116 for 2030 vision 2 or from ehighway scenario?
    # ....
    x = df.iloc[
        visions[vision]: visions[vision] + 36
    ]
    x.columns = x.iloc[0,:]
    x.drop(x.index[0], inplace=True)
    x.rename(columns={
        x.columns[0]: 'country',
        'Hard coal': 'coal',
        'Nuclear': 'uranium'},
             inplace=True)
    x.set_index('country', inplace=True)
    x.dropna(axis=1, how='all', inplace=True) # drop unwanted cols
    x['biomass'] = x['Biofuels'] + x['Others RES']
    x.drop(['Biofuels', 'Others RES'], axis=1, inplace=True)
    x.columns = [i.lower().replace(' ', '-') for i in x.columns]

    carriers = pd.DataFrame(
        Package('https://raw.githubusercontent.com/ZNES-datapackages/technology-cost/master/datapackage.json')
        .get_resource('carrier').read(keyed=True)).set_index(
            ['year', 'carrier', 'parameter']).sort_index()

    elements = {}

    for b in buses:
        for carrier in x.columns:
            element = {}

            if carrier in ['wind', 'solar']:
                if "wind" in carrier:
                    profile = b + "-onshore-profile"
                    tech = 'onshore'
                elif "solar" in carrier:
                    profile = b + "-pv-profile"
                    tech = 'pv'

                elements['-'.join([b, carrier, tech])] = element
                e = {
                    "bus": b + "-electricity",
                    "tech": tech,
                    "carrier": carrier,
                    "capacity": x.at[b, carrier],
                    "type": "volatile",
                    "profile": profile,
                }

                element.update(e)

            elif carrier in ['gas', 'coal', 'lignite', 'oil', 'uranium']:
                if carrier == 'gas':
                    tech = 'gt'
                else:
                    tech = 'st'
                elements['-'.join([b, carrier, tech])] = element
                marginal_cost = float(
                    carriers.at[(scenario_year, carrier, 'cost'), 'value']
                    + carriers.at[(2014, carrier, 'emission-factor'), 'value']
                    * carriers.at[(scenario_year, 'co2', 'cost'), 'value']
                ) / efficiencies[carrier]

                element.update({
                    "carrier": carrier,
                    "capacity": x.at[b, carrier],
                    "bus": b + "-electricity",
                    "type": "dispatchable",
                    "marginal_cost": marginal_cost,
                    "output_parameters": json.dumps(
                        {"max": max[carrier]}
                    ),
                    "tech": tech,
                }
            )

            elif carrier == 'others-non-res':
                elements[b + "-" + carrier] = element

                element.update({
                    "carrier": 'other',
                    "capacity": x.at[b, carrier],
                    "bus": b + "-electricity",
                    "type": "dispatchable",
                    "marginal_cost": 0,
                    "tech": 'other',
                    "output_parameters": json.dumps(
                        {"summed_max": 2000}
                    )
                }
            )

            elif carrier == "biomass":
                elements["-".join([b, carrier, 'ce'])] = element

                element.update({
                    "carrier": carrier,
                    "capacity": x.at[b, carrier],
                    "to_bus": b + "-electricity",
                    "efficiency": efficiencies[carrier],
                    "from_bus": b + "-biomass-bus",
                    "type": "conversion",
                    "carrier_cost": float(
                        carriers.at[(2030, carrier, 'cost'), 'value']
                    ),
                    "tech": 'ce',
                    }
                )

    df = pd.DataFrame.from_dict(elements, orient="index")
    df = df[df.capacity != 0]

    # write elements to CSV-files
    for element_type in ['dispatchable', 'volatile', 'conversion']:
        building.write_elements(
            element_type + ".csv",
            df.loc[df["type"] == element_type].dropna(how="all", axis=1),
            directory=os.path.join(datapackage_dir, "data", "elements"),
        )


def excess(datapackage_dir):
    """
    """
    path = os.path.join(datapackage_dir, "data", "elements")
    buses = building.read_elements("bus.csv", directory=path)

    buses.index.name = "bus"
    buses = buses.loc[buses['carrier'] == 'electricity']

    elements = pd.DataFrame(buses.index)
    elements["type"] = "excess"
    elements["name"] = elements["bus"] + "-excess"
    elements["marginal_cost"] = 0

    elements.set_index("name", inplace=True)

    building.write_elements("excess.csv", elements, directory=path)

def shortage(datapackage_dir):
    """
    """
    path = os.path.join(datapackage_dir, "data", "elements")
    buses = building.read_elements("bus.csv", directory=path)

    buses = buses.loc[buses['carrier'] == 'electricity']
    buses.index.name = "bus"

    elements = pd.DataFrame(buses.index)
    elements["capacity"] = 10e10
    elements["type"] = "shortage"
    elements["name"] = elements["bus"] + "-shortage"
    elements["marginal_cost"] = 300

    elements.set_index("name", inplace=True)

    building.write_elements("shortage.csv", elements, directory=path)
