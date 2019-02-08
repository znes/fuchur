# -*- coding: utf-8 -*-
"""
"""
import json
import os

from datapackage import Package
from decimal import Decimal
import numpy as np

from oemof.tabular.datapackage import building
from oemof.tools.economics import annuity
import pandas as pd

import fuchur



def tyndp_load(buses, scenario, datapackage_dir,
               raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    """
    filepath = building.download_data(
        "https://www.entsoe.eu/Documents/TYNDP%20documents/TYNDP2018/"
        "Scenarios%20Data%20Sets/Input%20Data.xlsx",
        directory=raw_data_path)

    df = pd.read_excel(filepath, sheet_name='Demand')
    df['countries'] = [i[0:2] for i in df.index]  # for aggregation by country

    elements = df.groupby('countries').sum()[scenario].to_frame()
    elements.index.name = "bus"
    elements = elements.loc[buses]
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

def ehighway_load(buses, year, datapackage_dir, scenario="100% RES",
                  raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    Parameter
    ---------
    buses: array like
        List with buses represented by iso country code
    year: integer
        Scenario year to select. One of: 2040, 2050
    datapackage_dir: string
        Directory for tabular resource
    scenario:
        Name of ehighway scenario to select. One of:
        ["Large Scale RES", "100% RES", "Big & Market", "Fossil & Nuclear",
         "Small & Local"], default: "100% RES"
    raw_data_path: string
        Path where raw data file `e-Highway_database_per_country-08022016.xlsx`
        is located
    """
    filename = "e-Highway_database_per_country-08022016.xlsx"
    filepath = os.path.join(raw_data_path, filename)

    if year == 2050:
        sheet = "T40"
    elif year == 2040:
        sheet = "T39"
    else:
        raise ValueError(
            "Value of argument `year` must be integer 2040 or 2050!")

    if os.path.exists(filepath):
        df = pd.read_excel(filepath, sheet_name=sheet, index_col=[0],
                           skiprows=[0,1])
    else:
        raise FileNotFoundError(
            "File for e-Highway loads does not exist. Did you download data?"
        )

    df.set_index("Scenario", inplace=True)  # Scenario in same colum as ctrcode
    df.drop(df.index[0:1], inplace=True)  # remove row with units
    df.dropna(how="all", axis=1, inplace=True)

    elements = df.loc[buses, scenario].to_frame()
    elements = elements.rename(columns={scenario: "amount"})
    elements.index.name = "bus"
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
    elements["amount"] = elements["amount"] * 1000  # to MWh

    path = os.path.join(datapackage_dir, "data", "elements")
    building.write_elements("load.csv", elements, directory=path)


def opsd_load_profile(buses, demand_year, scenario_year, datapackage_dir,
                      raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    Parameter
    ---------
    buses: array like
        List with buses represented by iso country code
    demand_year: integer or string
        Demand year to select
    scenario_year: integer or string
        Year of scenario to use for timeindex to resource
    datapackage_dir: string
        Directory for tabular resource
    raw_data_path: string
        Path where raw data file `e-Highway_database_per_country-08022016.xlsx`
        is located
    """
    filepath = os.path.join(raw_data_path, "time_series_60min_singleindex.csv")
    if os.path.exists(filepath):
        raw_data = pd.read_csv(filepath, index_col=[0], parse_dates=True)
    else:
        raise FileNotFoundError(
            "File for OPSD loads does not exist. Did you download data?"
        )

    suffix = "_load_old"

    countries = buses

    columns = [c + suffix for c in countries]

    timeseries = raw_data[str(demand_year)][columns]

    if timeseries.isnull().values.any():
        raise ValueError(
            "Timeseries for load has NaN values. Select "
            + "another demand year or use another data source."
        )

    load_total = timeseries.sum()

    load_profile = timeseries / load_total

    sequences_df = pd.DataFrame(index=load_profile.index)

    elements = building.read_elements(
        "load.csv", directory=os.path.join(datapackage_dir, "data", "elements")
    )

    for c in countries:
        # get sequence name from elements edge_parameters
        # (include re-exp to also check for 'elec' or similar)
        sequence_name = elements.at[
            elements.index[elements.index.str.contains(c)][0], "profile"
        ]

        sequences_df[sequence_name] = load_profile[c + suffix].values

    if sequences_df.index.is_leap_year[0]:
        sequences_df = sequences_df.loc[
            ~((sequences_df.index.month == 2) & (sequences_df.index.day == 29))
        ]

    sequences_df.index = building.timeindex(
        year=str(scenario_year)
    )

    building.write_sequences(
        "load_profile.csv",
        sequences_df,
        directory=os.path.join(datapackage_dir, "data/sequences"),
    )


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


def _get_hydro_inflow(inflow_dir=None):
    """ Adapted from:

            https://github.com/FRESNA/vresutils/blob/master/vresutils/hydro.py
    """

    def read_inflow(country):
        return pd.read_csv(
            os.path.join(inflow_dir, "Hydro_Inflow_{}.csv".format(country)),
            parse_dates={"date": [0, 1, 2]},
        ).set_index("date")["Inflow [GWh]"]

    europe = [
        "AT",
        "BA",
        "BE",
        "BG",
        "CH",
        "CZ",
        "DE",
        "ES",
        "FI",
        "FR",
        "HR",
        "HU",
        "IE",
        "IT",
        "KV",
        "LT",
        "LV",
        "ME",
        "MK",
        "NL",
        "NO",
        "PL",
        "PT",
        "RO",
        "RS",
        "SE",
        "SI",
        "SK",
        "UK",
    ]

    hyd = pd.DataFrame({cname: read_inflow(cname) for cname in europe})

    hyd.rename(columns={"UK": "GB"}, inplace=True)  # for ISO country code

    hydro = hyd.resample("H").interpolate("cubic")

    # add last day of the dataset that is missing from resampling
    last_day = pd.DataFrame(
        index=pd.DatetimeIndex(start="20121231", freq="H", periods=24),
        columns=hydro.columns,
    )
    data = hyd.loc["2012-12-31"]
    for c in last_day:
        last_day.loc[:, c] = data[c]

    # need to drop last day because it comes in last day...
    hydro = pd.concat([hydro.drop(hydro.tail(1).index), last_day])

    # remove last day in Feb for leap years
    hydro = hydro[~((hydro.index.month == 2) & (hydro.index.day == 29))]

    if True:  # default norm
        normalization_factor = hydro.index.size / float(
            hyd.index.size
        )  # normalize to new sampling frequency
    # else:
    #     # conserve total inflow for each country separately
    #    normalization_factor = hydro.sum() / hyd.sum()
    hydro /= normalization_factor

    return hydro


def hydro_generation(config, datapackage_dir,
                     raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    """
    countries, year = (
        config["buses"]["electricity"],
        config["temporal"]["scenario_year"],
    )

    capacities = pd.read_csv(
        os.path.join(raw_data_path, "hydropower.csv"),
        index_col=["ctrcode"],
    )
    capacities.rename(index={"UK": "GB"}, inplace=True)  # for iso code

    capacities.loc["CH"] = [8.8, 12, 1.9]  # add CH elsewhere

    inflows = _get_hydro_inflow(
        inflow_dir=os.path.join(raw_data_path, "Hydro_Inflow")
    )

    inflows = inflows.loc[
        inflows.index.year == config["temporal"]["weather_year"], :
    ]
    inflows["DK"], inflows["LU"] = 0, inflows["BE"]

    technologies = pd.DataFrame(
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages"
            "/technology-cost/master/datapackage.json"
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

    ror_shares = pd.read_csv(
        os.path.join(raw_data_path, "ror_ENTSOe_Restore2050.csv"),
        index_col="Country Code (ISO 3166-1)",
    )["ror ENTSO-E\n+ Restore"]

    # ror
    ror = pd.DataFrame(index=countries)
    ror["type"], ror["tech"], ror["bus"], ror["capacity"] = (
        "volatile",
        "ror",
        ror.index.astype(str) + "-electricity",
        (
            capacities.loc[ror.index, " installed hydro capacities [GW]"]
            - capacities.loc[
                ror.index, " installed pumped hydro capacities [GW]"
            ]
        )
        * ror_shares[ror.index]
        * 1000,
    )

    ror = ror.assign(**technologies["ror"])[ror["capacity"] > 0].dropna()
    ror["profile"] = ror["bus"] + "-" + ror["tech"] + "-profile"

    ror_sequences = (inflows[ror.index] * ror_shares[ror.index] * 1000) / ror[
        "capacity"
    ]
    ror_sequences.columns = ror_sequences.columns.map(ror["profile"])

    # phs
    phs = pd.DataFrame(index=countries)
    phs["type"], phs["tech"], phs["bus"], phs["loss"], phs["capacity"], phs[
        "marginal_cost"
    ] = (
        "storage",
        "phs",
        phs.index.astype(str) + "-electricity",
        0,
        capacities.loc[phs.index, " installed pumped hydro capacities [GW]"]
        * 1000,
        0.0000001,
    )

    phs["storage_capacity"] = phs["capacity"] * 6  # Brown et al.
    # as efficieny in data is roundtrip use sqrt of roundtrip
    phs["efficiency"] = float(technologies["phs"]["efficiency"]) ** 0.5
    phs = phs.assign(**technologies["phs"])[phs["capacity"] > 0].dropna()

    # other hydro / reservoir
    rsv = pd.DataFrame(index=countries)
    rsv["type"], rsv["tech"], rsv["bus"], rsv["loss"], rsv["capacity"], rsv[
        "storage_capacity"
    ] = (
        "reservoir",
        "reservoir",
        rsv.index.astype(str) + "-electricity",
        0,
        (
            capacities.loc[ror.index, " installed hydro capacities [GW]"]
            - capacities.loc[
                ror.index, " installed pumped hydro capacities [GW]"
            ]
        )
        * (1 - ror_shares[ror.index])
        * 1000,
        capacities.loc[rsv.index, " reservoir capacity [TWh]"] * 1e6,
    )  # to MWh

    rsv = rsv.assign(**technologies["rsv"])[rsv["capacity"] > 0].dropna()
    rsv["profile"] = rsv["bus"] + "-" + rsv["tech"] + "-profile"
    rsv[
        "efficiency"
    ] = 1  # as inflow is already in MWelec -> no conversion needed
    rsv_sequences = (
        inflows[rsv.index] * (1 - ror_shares[rsv.index]) * 1000
    )  # GWh -> MWh
    rsv_sequences.columns = rsv_sequences.columns.map(rsv["profile"])

    # write sequences to different files for better automatic foreignKey
    # handling in meta data
    building.write_sequences(
        "reservoir_profile.csv",
        rsv_sequences.set_index(
            building.timeindex(year=str(config["temporal"]["scenario_year"]))
        ),
        directory=os.path.join(datapackage_dir, "data", "sequences"),
    )

    building.write_sequences(
        "ror_profile.csv",
        ror_sequences.set_index(
            building.timeindex(year=str(config["temporal"]["scenario_year"]))
        ),
        directory=os.path.join(datapackage_dir, "data", "sequences"),
    )

    filenames = ["ror.csv", "phs.csv", "reservoir.csv"]

    for fn, df in zip(filenames, [ror, phs, rsv]):
        df.index = df.index.astype(str) + "-" + df["tech"]
        df["capacity_cost"] = df.apply(
            lambda x: annuity(
                float(x["capacity_cost"]) * 1000,
                float(x["lifetime"]),
                config["cost"]["wacc"],
            ),
            axis=1,
        )
        building.write_elements(
            fn, df, directory=os.path.join(datapackage_dir, "data", "elements")
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
    for carrier in ['wind_offshore', 'wind_onshore', 'solar', 'biomass']:
        element = {}
        if carrier in ['wind_offshore', 'wind_onshore', 'solar']:
            if "on" in carrier:
                profile = b + "-wind-on-profile"
                tech = 'wind-on'
                capacity = 85500
            elif "off" in carrier:
                profile = b + "-wind-off-profile"
                tech = 'wind-off'
                capacity = 17000
            elif "solar" in carrier:
                profile = b + "-pv-profile"
                tech = 'pv'
                capacity = 104500

            elements[b + "-" + tech] = element
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
            elements[b + "-" + carrier] = element

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
                "tech": carrier,
                }
            )


    df = pd.DataFrame.from_dict(elements, orient="index")

    for element_type in ['volatile', 'conversion']:
        building.write_elements(
            element_type + ".csv",
            df.loc[df["type"] == element_type].dropna(how="all", axis=1),
            directory=os.path.join(datapackage_dir, "data", "elements"),
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
