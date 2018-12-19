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

from fuchur.scripts import bus, electricity, grid, capacity_factors
from fuchur.scripts import compute as _compute


def _construct(config, ctx):
    """

    config: dict
        Config dict for contructing the datapackage
    """

    datapackage.processing.clean_datapackage(
        path=ctx.obj["DATPACKAGE_DIR"],
        directories=["data", "resources"]
    )

    datapackage.building.initialize_datapackage(config=config)

    bus.add(config, ctx.obj["DATPACKAGE_DIR"])

    grid.add(config, ctx.obj["DATPACKAGE_DIR"])

    electricity.load(config, ctx.obj["DATPACKAGE_DIR"])

    electricity.generation(config, ctx.obj["DATPACKAGE_DIR"])

    electricity.excess(config, ctx.obj["DATPACKAGE_DIR"])

    electricity.hydro_generation(config, ctx.obj["DATPACKAGE_DIR"])

    capacity_factors.pv(config, ctx.obj["DATPACKAGE_DIR"])

    capacity_factors.wind(config, ctx.obj["DATPACKAGE_DIR"])

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


@click.group(chain=True)
@click.option("--solver", default="gurobi", help="Choose solver")
@click.option(
    "--datapackage-dir",
    default=os.getcwd(),
    help="Data to root directory of datapackage",
)
@click.option(
    "--results-dir",
    default=os.path.join(os.getcwd(), "results"),
    help="Data for results",
)
@click.option(
    "--compute",
    default=True,
    type=bool,
    help="Flag if constructed model is computed.",
)


@click.pass_context
def cli(ctx, solver, datapackage_dir, results_dir, compute):
    ctx.obj["SOLVER"] = solver
    ctx.obj["DATPACKAGE_DIR"] = datapackage_dir
    ctx.obj["RESULTS_DIR"] = results_dir
    ctx.obj["COMPUTE"] = compute
@cli.command()
@click.argument("config", type=str, default="config.json")
@click.pass_context
def run(ctx, config):
    config = datapackage.building.get_config(config)

    _construct(config, ctx)

    if ctx.obj["COMPUTE"]:
        _compute.compute(config, ctx)

# @cli.command()
# @click.argument("config", type=str, default="config.json")
# @click.pass_context
# def compute(ctx, config):
#     config = datapackage.building.get_config(config)


def main():
    cli(obj={})
