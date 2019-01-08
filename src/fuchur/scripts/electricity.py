# -*- coding: utf-8 -*-
"""
"""
import logging
import json
import os

from datapackage import Package
import pandas as pd

from oemof.tabular.datapackage import building
from oemof.tools.economics import annuity

import fuchur

def load(buses, temporal, datapackage_dir,
         raw_data_path=fuchur.__RAW_DATA_PATH__):
    """
    """
    # first we build the elements ---------------
    filename = "e-Highway_database_per_country-08022016.xlsx"
    filepath = os.path.join(raw_data_path,
                            filename)

    if temporal["scenario_year"] == 2050:
        sheet = "T40"
    if os.path.exists(filepath):
        df = pd.read_excel(filepath, sheet_name=sheet, index_col=[0])
    else:
        logging.info(
            "File for e-Highway loads does not exist. Did you download data?"
        )

    df.set_index("Unnamed: 1", inplace=True)
    df.drop(df.index[0:1], inplace=True)
    df.dropna(how="all", axis=1, inplace=True)

    elements = df.loc[buses["electricity"],
                      df.loc["Scenario"] == "100% RES"]
    elements = elements.rename(columns={"Unnamed: 3": "amount"})
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

    building.write_elements(
        "load.csv",
        elements,
        directory=os.path.join(datapackage_dir, "data/elements"),
    )

    filepath = os.path.join(raw_data_path,
                            "time_series_60min_singleindex.csv")
    if os.path.exists(filepath):
        raw_data = pd.read_csv(filepath, index_col=[0], parse_dates=True)
    else:
        logging.info(
            "File for OPSD loads does not exist. Did you download data?"
        )


    suffix = "_load_old"

    year = str(temporal["demand_year"])

    countries = buses["electricity"]

    columns = [c + suffix for c in countries]

    timeseries = raw_data[year][columns]

    if timeseries.isnull().values.any():
        raise ValueError(
            "Timeseries for load has NaN values. Select "
            + "another demand year or use another data source."
        )

    load_total = timeseries.sum()

    load_profile = timeseries / load_total

    sequences_df = pd.DataFrame(index=load_profile.index)
    elements = building.read_elements(
        "load.csv",
        directory=os.path.join(datapackage_dir, "data/elements"))

    for c in countries:
        # get sequence name from elements edge_parameters (include re-exp to also
        # check for 'elec' or similar)
        sequence_name = elements.at[
            elements.index[elements.index.str.contains(c)][0], "profile"
        ]

        sequences_df[sequence_name] = load_profile[c + suffix].values

    if sequences_df.index.is_leap_year[0]:
        sequences_df = sequences_df.loc[
            ~((sequences_df.index.month == 2) & (sequences_df.index.day == 29))
        ]

    sequences_df.index = building.timeindex(
        year=str(temporal["scenario_year"]))

    building.write_sequences(
        "load_profile.csv",
        sequences_df,
        directory=os.path.join(datapackage_dir, "data/sequences"),
    )


def generation(config, datapackage_dir):
    """
    """
    techmap = {
        "ocgt": "dispatchable",
        "ccgt": "dispatchable",
        "pv": "volatile",
        "wind_onshore": "volatile",
        "wind_offshore": "volatile",
        "biomass": "conversion",
        "lithium_battery": "storage",
        "acaes": "storage",
    }

    wacc = config["cost"]["wacc"]

    technologies = pd.DataFrame(
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages/technology-cost/master/datapackage.json"
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
    technologies = technologies.loc[config["temporal"]["scenario_year"]].to_dict()

    potential = (
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages/technology-potential/master/datapackage.json"
        )
        .get_resource("renewable")
        .read(keyed=True)
    )
    potential = pd.DataFrame(potential).set_index(["country", "tech"])
    potential = potential.loc[
        potential["source"] == config["potential"]
    ].to_dict()

    for tech in technologies:
        technologies[tech]["capacity_cost"] = technologies[tech][
            "capacity_cost"
        ] * config["cost"]["factor"].get(tech, 1)
        if "storage_capacity_cost" in technologies[tech]:
            technologies[tech]["storage_capacity_cost"] = technologies[tech][
                "storage_capacity_cost"
            ] * config["cost"]["factor"].get(tech, 1)

    carrier = pd.read_csv(
        os.path.join(fuchur.__RAW_DATA_PATH__,
                     "carrier.csv"), index_col=[0, 1]
    ).loc[("base", config["temporal"]["scenario_year"])]
    carrier.set_index("carrier", inplace=True)

    elements = {}

    for r in config["buses"]["electricity"]:
        for tech, data in technologies.items():
            if tech in config["technologies"]["investment"]:

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

                elif techmap.get(tech) == "volatile":
                    if "wind_off" in tech:
                        profile = r + "-wind-off-profile"
                    elif "wind_on" in tech:
                        profile = r + "-wind-on-profile"
                    elif "pv" in tech:
                        profile = r + "-pv-profile"

                    element.update(
                        {
                            "capacity_cost": annuity(
                                float(data["capacity_cost"]),
                                float(data["lifetime"]),
                                wacc,
                            )
                            * 1000,
                            "capacity_potential": potential[
                                "capacity_potential"
                            ].get((r, tech), "Infinity"),
                            "bus": r + "-electricity",
                            "tech": tech,
                            "type": "volatile",
                            "profile": profile,
                        }
                    )

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
                            ** 0.5,  # convert roundtrip to input / output efficiency
                            "marginal_cost": 0.0000001,
                            "loss": 0.01,
                            "capacity_potential": capacity_potential,
                            "capacity_ratio": data["capacity_ratio"],
                        }
                    )

    df = pd.DataFrame.from_dict(elements, orient="index")
    # drop storage capacity cost to avoid duplicat investment
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
    """ Adapted from https://github.com/FRESNA/vresutils/blob/master/vresutils/hydro.py
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
    ]

    hyd = pd.DataFrame({cname: read_inflow(cname) for cname in europe})

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
    #    normalization_factor = hydro.sum() / hyd.sum() #conserve total inflow for each country separately
    hydro /= normalization_factor

    return hydro


def hydro_generation(config, datapackage_dir):
    """
    """
    countries, year = config["buses"]["electricity"], config["temporal"]["scenario_year"]

    capacities = pd.read_csv(
        # building.download_data(
        #     "https://zenodo.org/record/804244/files/hydropower.csv?download=1",
        #     local_path=os.path.join(datapackage_dir, "cache"),
        # ),
        os.path.join(fuchur.__RAW_DATA_PATH__, 'hydropower.csv'),
        index_col=["ctrcode"],
    )

    capacities.loc["CH"] = [8.8, 12, 1.9]  # add CH elsewhere

    inflows = _get_hydro_inflow(
        inflow_dir=os.path.join(fuchur.__RAW_DATA_PATH__, 'Hydro_Inflow')
        # building.download_data(
        #     "https://zenodo.org/record/804244/files/Hydro_Inflow.zip?download=1",
        #     unzip_file="Hydro_Inflow/",
        #     local_path=os.path.join(datapackage_dir, "cache"),
        # )
    )

    inflows = inflows.loc[inflows.index.year == config["temporal"]["weather_year"], :]
    inflows["DK"], inflows["LU"] = 0, inflows["BE"]

    technologies = pd.DataFrame(
        Package(
            "https://raw.githubusercontent.com/ZNES-datapackages/technology-cost/master/datapackage.json"
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
        os.path.join(fuchur.__RAW_DATA_PATH__, "ror_ENTSOe_Restore2050.csv"),
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
        "storage_capacity"] = (
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

    rsv = rsv.assign(**technologies["reservoir"])[rsv["capacity"] > 0].dropna()
    rsv["profile"] = rsv["bus"] + "-" + rsv["tech"] + "-profile"
    rsv["efficiency"] = 1 # as inflow is already in MWelec -> no conversion needed
    rsv_sequences = (
        inflows[rsv.index] * (1 - ror_shares[rsv.index]) * 1000
    )  # GWh -> MWh
    rsv_sequences.columns = rsv_sequences.columns.map(rsv["profile"])

    # write sequences to different files for better automatic foreignKey handling
    # in meta data
    building.write_sequences(
        "reservoir_profile.csv",
        rsv_sequences.set_index(building.timeindex(year=
            str(config["temporal"]["scenario_year"]))),
        directory=os.path.join(datapackage_dir, "data", "sequences"),
    )

    building.write_sequences(
        "ror_profile.csv",
        ror_sequences.set_index(building.timeindex(year=
            str(config["temporal"]["scenario_year"]))),
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


def excess(config, datapackage_dir):
    """
    """
    buses = building.read_elements(
        "bus.csv", directory=os.path.join(datapackage_dir, "data/elements")
    )
    buses.index.name = "bus"

    elements = pd.DataFrame(buses.index)
    elements["type"] = "excess"
    elements["name"] = elements["bus"] + "-excess"
    elements["marginal_cost"] = 0

    elements.set_index("name", inplace=True)

    building.write_elements(
        "excess.csv",
        elements,
        directory=os.path.join(datapackage_dir, "data/elements"),
    )
