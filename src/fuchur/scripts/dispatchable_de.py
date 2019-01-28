# -*- coding: utf-8 -*-
"""
"""

import json
import os

import pandas as pd
import numpy as np

from oemof.tabular.datapackage import building
from datapackage import Package
from decimal import Decimal

import fuchur


year = 2030


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
    directory=fuchur.__RAW_DATA_PATH__)
    , encoding='utf-8')

sq.set_index("id", inplace=True)

nep = pd.read_excel(building.download_data(
    "https://www.netzentwicklungsplan.de/sites/default/files/"
    "paragraphs-files/Kraftwerksliste_%C3%9CNB_Entwurf_Szenariorahmen_2030_V2019.xlsx",
    directory=fuchur.__RAW_DATA_PATH__)
    , encoding='utf-8')

pp = nep.loc[nep["Nettonennleistung B2030 [MW]"] != 0]["BNetzA-ID"]
pp = [i for i in pp.values if  not pd.isnull(i)]
df = sq.loc[pp]

cond1 = df['country_code'] == 'DE'
cond2 = df['fuel'].isin(['Hydro'])
cond3 = (df['fuel'] == 'Other fuels') & (df['technology'] == 'Storage technologies')

df = df.loc[cond1 & ~cond2 & ~cond3, :].copy()

mapper = {('Biomass and biogas', 'Steam turbine'): ('biomass', 'biomass'),
          ('Biomass and biogas', 'Combustion Engine'): ('biomass', 'biomass'),
          ('Hard coal', 'Steam turbine'): ('coal', 'st'),
          ('Hard coal', 'Combined cycle'): ('coal', 'ccgt'),
          ('Lignite', 'Steam turbine'): ('lignite', 'boa'),
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

bins = 4
df.loc[index, 'bins'] = df[index].groupby(['carrier', 'tech'])['capacity_net_bnetza']\
    .apply(lambda i: pd.qcut(i, bins, labels=range(1, bins+1)))
df['bins'].fillna(1, inplace=True)

s = df.groupby(['country_code', 'carrier', 'tech', 'bins']).\
    agg({'capacity_net_bnetza': sum, 'efficiency_estimate': np.mean})

elements = {}

co2 = carriers.at[(year, 'co2', 'cost', 'EUR/t'), 'value']

# energy availability factor
eaf = 0.85

for (country, carrier, tech, bins), (capacity, eta) in s.iterrows():
    name = country + '-' + carrier + '-' + tech + '-' + str(bins)

    vom = technologies.at[(year, carrier, tech, 'vom'), 'value']
    ef = carriers.at[(year, carrier, 'emission-factor', 't (CO2)/MWh'), 'value']
    fuel = carriers.at[(year, carrier, 'cost', 'EUR/MWh'), 'value']

    marginal_cost = (fuel + vom + co2 * ef) / Decimal(eta)

    output_parameters = {"max": eaf}

    # if carrier == 'gas':
    #     output_parameters.update({"summed_min": 2000})

    element = {
        'bus': country + '-electricity',
        'tech': tech,
        'carrier': carrier,
        'capacity': capacity,
        'marginal_cost': float(marginal_cost),
        'output_parameters': json.dumps(output_parameters),
        'type': 'dispatchable'}

    elements[name] = element

# update biomass capacity
# elements['DE-biomass-biomass-1']['capacity'] = literature.loc[
#     pd.IndexSlice[year, 'DE', 'biomass', np.nan, 'capacity'], 'value']


building.write_elements('dispatchable.csv', pd.DataFrame.from_dict(elements, orient='index'))
