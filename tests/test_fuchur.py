from click.testing import CliRunner
import os

from fuchur.cli import cli
import fuchur


def test_main():
    runner = CliRunner()
    result = runner.invoke(cli, [])
    help = runner.invoke(cli, ["--help"])

    assert result.output == help.output
    assert result.exit_code == 0


def test_builtin_scenario_availability():
    assert fuchur.scenarios.keys() == set(
        ["el-2pv-cost", "el-base", "el-no-biomass", "scenario2", "test"]
    )


def test_builtin_scenario_construction():
    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(cli, ["construct", "test"])
        assert result.exit_code == 0
        assert os.listdir(os.curdir) == [], (
            "\nIf you see this message, a test started failing that was"
            "\nasserting the wrong fact anyway. The working directory should"
            "\nnot be empty after running `fuchur construct test`.\n"
            "\nNow that the working directory contains something, you can"
            "\nstart correcting the test."
        )
