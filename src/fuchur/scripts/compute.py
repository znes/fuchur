import datetime
import os
import json
import logging

from datapackage import Package
import numpy as np
import pandas as pd

from oemof.tabular import facades
from oemof.tabular.datapackage import aggregation, building, processing
from oemof.tabular.tools import postprocessing as pp
import oemof.outputlib as outputlib
from oemof.solph import EnergySystem, Model, Bus, Sink, constraints
from oemof.solph.components import GenericStorage


def compute(ctx):
    """
    """
    p = Package(
        os.path.join(
            ctx.obj["DATPACKAGE_DIR"], 'datapackage.json')
        )

    temporal_resolution = ctx.obj["TEMPORAL_RESOLUTION"]
    emission_limit = ctx.obj["EMISSION_LIMIT"]

    # create results path
    scenario_path = os.path.join(ctx.obj["RESULTS_DIR"], p.descriptor["name"])
    if not os.path.exists(scenario_path):
        os.makedirs(scenario_path)

    output_path = os.path.join(scenario_path, "output")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # store used config file
    # with open(os.path.join(scenario_path, "config.json"), "w") as outfile:
    #     json.dump(config, outfile, indent=4)

    # copy package either aggregated or the original one (only data!)
    if temporal_resolution > 1:
        logging.info("Aggregating for temporal aggregation ... ")
        path = aggregation.temporal_skip(
            os.path.join(ctx.obj["DATPACKAGE_DIR"], "datapackage.json"),
            temporal_resolution,
            path=scenario_path,
            name="input"
        )
    else:
        path = processing.copy_datapackage(
            os.path.join(ctx.obj["DATPACKAGE_DIR"], "datapackage.json"),
            os.path.abspath(os.path.join(scenario_path, "input")),
            subset="data",
        )

    es = EnergySystem.from_datapackage(
        os.path.join(path, "datapackage.json"),
        attributemap={},
        typemap=facades.TYPEMAP,
    )

    m = Model(es)

    constraints.emission_limit(m, limit=emission_limit)

    m.receive_duals()

    m.solve(ctx.obj["SOLVER"])

    m.results = m.results()

    pp.write_results(m, output_path)

    modelstats = outputlib.processing.meta_results(m)
    modelstats.pop("solver")
    modelstats["problem"].pop("Sense")
    with open(os.path.join(scenario_path, "modelstats.json"), "w") as outfile:
        json.dump(modelstats, outfile, indent=4)


    supply_sum = (
        pp.supply_results(
            results=m.results,
            es=m.es,
            bus=[b.label for b in es.nodes if isinstance(b, Bus)],
            types=[
                "dispatchable",
                "volatile",
                "conversion",
                "backpressure",
                "extraction",
                "reservoir",
            ],
        )
        .sum()
        .reset_index()
    )
    supply_sum["from"] = supply_sum.apply(
        lambda x: "-".join(x["from"].label.split("-")[1::]), axis=1
    )
    supply_sum.drop("type", axis=1, inplace=True)
    supply_sum = (
        supply_sum.set_index(["from", "to"]).unstack("from")
        / 1e6
        * temporal_resolution
    )
    supply_sum.columns = supply_sum.columns.droplevel(0)

    # excess_share = (
    #     excess.sum() * config["temporal-resolution"] / 1e6
    # ) / supply_sum.sum(axis=1)
    # excess_share.name = "excess"

    summary = supply_sum #pd.concat([supply_sum, excess_share], axis=1)
    summary.to_csv(os.path.join(scenario_path, 'summary.csv'))
