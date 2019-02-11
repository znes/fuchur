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


def generation(config, datapackage_dir,
                     raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    """
    countries, year = (
        config["buses"]["electricity"],
        config["temporal"]["scenario_year"],
    )

    filepath = building.download_data(
        'https://zenodo.org/record/804244/files/hydropower.csv?download=1',
        directory=raw_data_path)

    capacities = pd.read_csv(
        filepath,
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
    ror["type"], ror["tech"], ror["bus"], ror["capacity"], ror['carrier'] = (
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
        'hydro'
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
        "marginal_cost"], phs['carrier'] = (
        "storage",
        "phs",
        phs.index.astype(str) + "-electricity",
        0,
        capacities.loc[phs.index, " installed pumped hydro capacities [GW]"]
        * 1000,
        0.0000001,
        "hydro"
    )

    phs["storage_capacity"] = phs["capacity"] * 6  # Brown et al.
    # as efficieny in data is roundtrip use sqrt of roundtrip
    phs["efficiency"] = float(technologies["phs"]["efficiency"]) ** 0.5
    phs = phs.assign(**technologies["phs"])[phs["capacity"] > 0].dropna()

    # other hydro / reservoir
    rsv = pd.DataFrame(index=countries)
    rsv["type"], rsv["tech"], rsv["bus"], rsv["loss"], rsv["capacity"], rsv[
        "storage_capacity"], rsv['carrier'] = (
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
        "hydro"
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
        df.index = df.index.astype(str) + "-hydro-" + df["tech"]
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
