
from click.testing import CliRunner

from fuchur.cli import cli


def test_main():
    runner = CliRunner()
    result = runner.invoke(cli, [])
    help = runner.invoke(cli, ["--help"])

    assert result.output == help.output
    assert result.exit_code == 0
