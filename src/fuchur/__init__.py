import os

__version__ = "0.0.0"
__RAW_DATA_PATH__ = os.path.join(os.path.expanduser("~"), "fuchur-raw-data")

if not os.path.exists(__RAW_DATA_PATH__):
    os.makedirs(__RAW_DATA_PATH__)
