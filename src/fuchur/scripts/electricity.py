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




def nep_2019(year, datapackage_dir, bins=2, eaf=0.95,
             raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    """

    technologies = pd.DataFrame(
        #Package('/home/planet/data/datapackages/technology-cost/datapackage.json')
        Package('https://raw.githubusercontent.com/ZNES-datapackages/technology-cost/master/datapackage.json')
        .get_resource('electricity').read(keyed=True)).set_index(
            ['year', 'carrier', 'tech', 'parameter'])

    carriers = pd.DataFrame(
        #Package('/home/planet/data/datapackages/technology-cost/datapackage.json')
        Package('https://raw.githubusercontent.com/ZNES-datapackages/technology-cost/master/datapackage.json')
        .get_resource('carrier').read(keyed=True)).set_index(
            ['year', 'carrier', 'parameter', 'unit']).sort_index()

    sq = pd.read_csv(building.download_data(
        "https://data.open-power-system-data.org/conventional_power_plants/"
        "2018-12-20/conventional_power_plants_DE.csv",
        directory=raw_data_path)
        , encoding='utf-8')

    sq.set_index("id", inplace=True)

    nep = pd.read_excel(building.download_data(
        "https://www.netzentwicklungsplan.de/sites/default/files/"
        "paragraphs-files/Kraftwerksliste_%C3%9CNB_Entwurf_Szenariorahmen_2030_V2019.xlsx",
        directory=raw_data_path)
        , encoding='utf-8')

    pp = nep.loc[nep["Nettonennleistung B2030 [MW]"] != 0]["BNetzA-ID"]
    pp = list(set([i for i in pp.values if  not pd.isnull(i)]))
    df = sq.loc[pp]

    cond1 = df['country_code'] == 'DE'
    cond2 = df['fuel'].isin(['Hydro'])
    cond3 = (df['fuel'] == 'Other fuels') & (df['technology'] == 'Storage technologies')

    df = df.loc[cond1 & ~cond2 & ~cond3, :].copy()

    mapper = {('Biomass and biogas', 'Steam turbine'): ('biomass', 'st'),
              ('Biomass and biogas', 'Combustion Engine'): ('biomass', 'ce'),
              ('Hard coal', 'Steam turbine'): ('coal', 'st'),
              ('Hard coal', 'Combined cycle'): ('coal', 'ccgt'),
              ('Lignite', 'Steam turbine'): ('lignite', 'st'),
              ('Natural gas', 'Gas turbine'): ('gas', 'ocgt'),
              ('Natural gas', 'Steam turbine'): ('gas', 'st'),
              ('Natural gas', 'Combined cycle'): ('gas', 'ccgt'),
              ('Natural gas', 'Combustion Engine'): ('gas', 'st'),  # other technology
              ('Nuclear', 'Steam turbine'): ('uranium', 'st'),
              ('Oil', 'Steam turbine'): ('oil', 'st'),
              ('Oil', 'Gas turbine'): ('oil', 'st'),
              ('Oil', 'Combined cycle'): ('oil', 'st'),
              ('Other fuels', 'Steam turbine'): ('waste', 'chp'),
              ('Other fuels', 'Combined cycle'): ('gas', 'ccgt'),
              ('Other fuels', 'Gas turbine'): ('gas', 'ocgt'),
              ('Waste', 'Steam turbine'): ('waste', 'chp'),
              ('Waste', 'Combined cycle'): ('waste', 'chp'),
              ('Other fossil fuels', 'Steam turbine'): ('coal', 'st'),
              ('Other fossil fuels', 'Combustion Engine'): ('gas', 'st'),
              ('Mixed fossil fuels', 'Steam turbine'): ('gas', 'st')}

    df['carrier'], df['tech'] = zip(*[mapper[tuple(i)] for i in df[['fuel', 'technology']].values])

    etas = df.groupby(['carrier', 'tech']).mean()['efficiency_estimate'].to_dict()
    index = df['efficiency_estimate'].isna()
    df.loc[index, 'efficiency_estimate'] = \
        [etas[tuple(i)] for i in df.loc[index, ('carrier', 'tech')].values]

    index = df['carrier'].isin(['gas', 'coal', 'lignite'])

    df.loc[index, 'bins'] = df[index].groupby(['carrier', 'tech'])['capacity_net_bnetza']\
        .apply(lambda i: pd.qcut(i, bins, labels=False, duplicates='drop'))

    df['bins'].fillna(0, inplace=True)

    s = df.groupby(['country_code', 'carrier', 'tech', 'bins']).\
        agg({'capacity_net_bnetza': sum, 'efficiency_estimate': np.mean})

    elements = {}

    co2 = carriers.at[(year, 'co2', 'cost', 'EUR/t'), 'value']

    for (country, carrier, tech, bins), (capacity, eta) in s.iterrows():
        name = country + '-' + carrier + '-' + tech + '-' + str(bins)

        vom = technologies.at[(year, carrier, tech, 'vom'), 'value']
        ef = carriers.at[(2015, carrier, 'emission-factor', 't (CO2)/MWh'), 'value']
        fuel = carriers.at[(year, carrier, 'cost', 'EUR/MWh'), 'value']

        marginal_cost = (fuel + vom + co2 * ef) / Decimal(eta)

        output_parameters = {"max": eaf}

        if carrier == "waste":
            output_parameters.update({"summed_max": 2500})

        element = {
            'bus': country + '-electricity',
            'tech': tech,
            'carrier': carrier,
            'capacity': capacity,
            'marginal_cost': float(marginal_cost),
            'output_parameters': json.dumps(output_parameters),
            'type': 'dispatchable'}

        elements[name] = element


    building.write_elements(
        'dispatchable.csv',
        pd.DataFrame.from_dict(elements, orient='index'),
        directory=os.path.join(datapackage_dir, 'data', 'elements'))

    # add renewables
    elements =  {}

    b = 'DE'
    for carrier, tech in [('wind', 'offshore'), ('wind', 'onshore'),
    ('solar', 'pv'), ('biomass', 'ce')]:
        element = {}
        if carrier in ['wind', 'solar']:
            if "onshore" == tech:
                profile = b + "-onshore-profile"
                capacity = 85500
            elif "offshore" == tech:
                profile = b + "-offshore-profile"
                capacity = 17000
            elif "pv" in tech:
                profile = b + "-pv-profile"
                capacity = 104500

            elements["-".join([b, carrier, tech])] = element
            e = {
                "bus": b + "-electricity",
                "tech": tech,
                "carrier": carrier,
                "capacity": capacity,
                "type": "volatile",
                "profile": profile,
            }

            element.update(e)


        elif carrier == "biomass":
            elements["-".join([b, carrier, tech])] = element

            element.update({
                "carrier": carrier,
                "capacity": 6000,
                "to_bus": b + "-electricity",
                "efficiency": 0.4,
                "from_bus": b + "-biomass-bus",
                "type": "conversion",
                "carrier_cost": float(
                    carriers.at[(2030, carrier, 'cost'), 'value']
                ),
                "tech": 'ce',
                }
            )

    elements['DE-battery'] =    {
            "storage_capacity": 8 * 10000,  # 8 h
            "capacity": 10000,
            "bus": "DE-electricity",
            "tech": 'battery',
            "carrier": 'electricity',
            "type": "storage",
            "efficiency": 0.9
            ** 0.5,  # convert roundtrip to input / output efficiency
            "marginal_cost": 0.0000001,
            "loss": 0.01
        }


    df = pd.DataFrame.from_dict(elements, orient="index")

    for element_type in ['volatile', 'conversion', 'storage']:
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
