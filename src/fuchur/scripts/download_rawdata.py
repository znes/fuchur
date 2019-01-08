
from oemof.tabular.datapackage import building

import fuchur

building.download_data(
    "sftp://5.35.252.104/home/rutherford/fuchur-raw-data.zip",
    username="rutherford", directory=fuchur.__RAW_DATA_PATH__,
    unzip_file="fuchur-raw-data/")
