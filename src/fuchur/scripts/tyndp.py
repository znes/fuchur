# -*- coding: utf-8 -*-
"""
"""
import os
import json

from datapackage import Package
from oemof.tabular.datapackage import building
import pandas as pd

import fuchur


def grid(buses, datapackage_dir):
    """
    """
    filepath = building.download_data(
        "https://www.entsoe.eu/Documents/TYNDP%20documents/TYNDP2018/"
        "Scenarios%20Data%20Sets/Input%20Data.xlsx",
        directory=fuchur.__RAW_DATA_PATH__)

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
            row["from"] in buses["electricity"]
            and row["to"] in buses["electricity"]
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

    return df


def load(buses, tyndp, datapackage_dir):
    """
    """
    scenario = tyndp['load']
    filepath = building.download_data(
        "https://www.entsoe.eu/Documents/TYNDP%20documents/TYNDP2018/"
        "Scenarios%20Data%20Sets/Input%20Data.xlsx",
        directory=fuchur.__RAW_DATA_PATH__)

    df = pd.read_excel(filepath, sheet_name='Demand')
    df['countries'] = [i[0:2] for i in df.index]  # for aggregation by country


    elements = df.groupby('countries').sum()[scenario].to_frame()
    elements.index.name = "bus"
    elements = elements.loc[buses['electricity']]
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


def generation(buses, tyndp, temporal, datapackage_dir):
    """
    """
    filepath = building.download_data(
        "https://www.entsoe.eu/Documents/TYNDP%20documents/TYNDP%202016/rgips/"
        "TYNDP2016%20market%20modelling%20data.xlsx",
        directory=fuchur.__RAW_DATA_PATH__)
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
        visions[tyndp['generation']]: visions[tyndp['generation']] + 36
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

    for b in buses["electricity"]:
        if b != 'DE':
            for carrier in x.columns:

                element = {}

                if carrier in ['wind', 'solar']:
                    if "wind" in carrier:
                        profile = b + "-wind-on-profile"
                        tech = 'wind-on'
                    elif "solar" in carrier:
                        profile = b + "-pv-profile"
                        tech = 'pv'

                    elements[b + "-" + tech] = element
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
                    elements[b + "-" + carrier] = element
                    marginal_cost = float(
                        carriers.at[(temporal['scenario_year'], carrier, 'cost'), 'value']
                        + carriers.at[(2014, carrier, 'emission-factor'), 'value']
                        * carriers.at[(temporal['scenario_year'], 'co2', 'cost'), 'value']
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
                        "tech": carrier,
                    }
                )

                elif carrier == 'others-non-res':
                    elements[b + "-" + carrier] = element

                    element.update({
                        "carrier": carrier,
                        "capacity": x.at[b, carrier],
                        "bus": b + "-electricity",
                        "type": "dispatchable",
                        "marginal_cost": 0,
                        "tech": carrier,
                        "output_parameters": json.dumps(
                            {"summed_max": 2000}
                        )
                    }
                )

                elif carrier == "biomass":
                    elements[b + "-" + carrier] = element

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
                        "tech": carrier,
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
