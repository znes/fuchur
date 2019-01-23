from click.testing import CliRunner

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
