import os

import pkg_resources as pkg
import toml

__version__ = "0.0.0"
__RAW_DATA_PATH__ = os.path.join(os.path.expanduser("~"), "fuchur-raw-data")

if not os.path.exists(__RAW_DATA_PATH__):
    os.makedirs(__RAW_DATA_PATH__)

scenarios = {
    scenario["name"]: scenario
    for resource in pkg.resource_listdir("fuchur", "scenarios")
    for scenario in [
        toml.loads(
            pkg.resource_string(
                "fuchur", os.path.join("scenarios", resource)
            ).decode("UTF-8")
        )
    ]
}
