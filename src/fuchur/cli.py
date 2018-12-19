"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will cause
  problems: the code will get executed twice:

  - When you run `python -mfuchur` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``fuchur.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``fuchur.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import os

import click
from oemof.tabular import datapackage

from fuchur.scripts import bus, electricity, grid, capacity_factors, compute


def construct(config_path, datapackage_dir):
    """

    config_path: string
        Path to config file for contructing the datapackage
    datapackage_dir: string
        Path to the root directory of the datapackage to be constructed
    """
    config = datapackage.building.get_config(config_path)

    datapackage.processing.clean_datapackage(
        path=datapackage_dir, directories=["data", "resources"]
    )

    datapackage.building.initialize_datapackage(config=config)

    bus.add(config, datapackage_dir)

    grid.add(config, datapackage_dir)

    electricity.load(config, datapackage_dir)

    electricity.generation(config, datapackage_dir)

    electricity.excess(config, datapackage_dir)

    electricity.hydro_generation(config, datapackage_dir)

    capacity_factors.pv(config, datapackage_dir)

    capacity_factors.wind(config, datapackage_dir)

    datapackage.building.infer_metadata(
        package_name="angus2",
        foreign_keys={
            "bus": [
                "volatile",
                "dispatchable",
                "storage",
                "heat_storage",
                "load",
                "ror",
                "reservoir",
                "phs",
                "excess",
                "boiler",
                "commodity",
            ],
            "profile": ["load", "volatile", "heat_load", "ror", "reservoir"],
            "from_to_bus": ["link", "conversion", "line"],
            "chp": ["backpressure", "extraction"],
        },
    )


@click.command(
    help=("Fuchur helps you to create model data and caluculation " "the model")
)
@click.option(
    "--config",
    default="config.json",
    help="Config file to create model input data.",
)
@click.option(
    "--datapackage-dir",
    default=os.getcwd(),
    help="Data directory to store the datapackage",
)
# @click.option('--run-model', default=False, prompt='Run model?',
#                help="Run the model that has been constrcuted.")
def main(config, datapackage_dir):
    config = datapackage.building.get_config(config)
    # construct(config, datapackage_dir)
    compute.compute(config, datapackage_dir, "results")
