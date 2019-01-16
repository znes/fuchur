"""
Module that contains the command line app.

Why does this file exist, and why not put this in __main__?

  You might be tempted to import things from __main__ later, but that will
  cause problems: the code will get executed twice:

  - When you run `python -mfuchur` python will execute
    ``__main__.py`` as a script. That means there won't be any
    ``fuchur.__main__`` in ``sys.modules``.
  - When you import __main__ it will get executed again (as a module) because
    there's no ``fuchur.__main__`` in ``sys.modules``.

  Also see (1) from http://click.pocoo.org/5/setuptools/#setuptools-integration
"""
import os

from oemof.tabular import datapackage
import click

from fuchur.scripts import bus, capacity_factors, electricity, grid, heat
import fuchur
import fuchur.scripts.compute


def _download_rawdata():
    datapackage.building.download_data(
        "sftp://5.35.252.104/home/rutherford/fuchur-raw-data.zip",
        username="rutherford",
        directory=fuchur.__RAW_DATA_PATH__,
        unzip_file="fuchur-raw-data/",
    )


def _construct(config, ctx):
    """

    config: dict
        Config dict for contructing the datapackage
    """

    datapackage.processing.clean(
        path=ctx.obj["datapackage_dir"], directories=["data", "resources"]
    )

    datapackage.building.initialize(
        config=config, directory=ctx.obj["datapackage_dir"]
    )

    bus.add(config["buses"], ctx.obj["datapackage_dir"])

    grid.add(config["buses"], ctx.obj["datapackage_dir"])

    electricity.load(
        config["buses"], config["temporal"], ctx.obj["datapackage_dir"]
    )

    electricity.generation(config, ctx.obj["datapackage_dir"])

    electricity.excess(config, ctx.obj["datapackage_dir"])

    electricity.hydro_generation(config, ctx.obj["datapackage_dir"])

    capacity_factors.pv(config, ctx.obj["datapackage_dir"])

    capacity_factors.wind(config, ctx.obj["datapackage_dir"])

    if (
        config["buses"]["heat"]["decentral"]
        or config["buses"]["heat"]["central"]
    ):
        heat.load(config, ctx.obj["datapackage_dir"])

    if config["buses"]["heat"]["decentral"]:
        heat.decentral(config, ctx.obj["datapackage_dir"])

    if config["buses"]["heat"]["central"]:
        heat.central(config, ctx.obj["datapackage_dir"])

    datapackage.building.infer_metadata(
        package_name=config["name"],
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
        path=ctx.obj["datapackage_dir"],
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
    help="Data directory for results",
)
@click.option(
    "--temporal-resolution",
    default=1,
    help="Temporal resolution used for calculation.",
)
@click.option(
    "--emission-limit", default=50e6, help="Limit for CO2 emission in tons"
)
@click.option(
    "--safe", default=True, help="Protect results from being overwritten."
)
@click.pass_context
def cli(ctx, **kwargs):
    ctx.obj = kwargs


@cli.command()
@click.argument("config", type=str, default="config.json")
@click.pass_context
def construct(ctx, config):
    config = datapackage.building.read_build_config(config)
    _construct(config, ctx)


@cli.command()
@click.pass_context
def compute(ctx):
    fuchur.scripts.compute.compute(ctx)


@cli.command()
@click.pass_context
def download(ctx):
    _download_rawdata(ctx)


def main():
    cli(obj={})
