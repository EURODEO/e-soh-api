
import xarray as xr

from pygeoapi.formatter.base import BaseFormatter


class netCDFFormatter(BaseFormatter):

    def __init__(self, formatter_def: dict):
        super.__init__({"name": "netcdf", "geom": False})

        self.mimetype = "application/x-netcdf"

    def write(self, options: dict = {}, data: dict = None) -> xarray.Dataset:
        return data
