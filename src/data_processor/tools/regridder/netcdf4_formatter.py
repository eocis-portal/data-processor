# -*- coding: utf-8 -*-

#    EOCIS data-processor
#    Copyright (C) 2023  National Centre for Earth Observation (NCEO)
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <https://www.gnu.org/licenses/>.

import os.path
import datetime

from .formatter import Formatter

class NetCDF4Formatter(Formatter):
    """
    Create a formatter for writing either timeseries or region data to a netcdf4 output file
    """

    def __init__(self,path:str="", name_pattern:str=""):
        """
        Construct the netcdf4 formatter using options

        :param path: path of an output folder in which to create the output files
        :param name_pattern: a pattern to use to create the output file names

        """
        super().__init__(path, name_pattern)

    def write(self,start_dt,mid_dt,end_dt,data,variable_names):
        """
        Write an entry to the output file covering a time period
        :param start_dt: start date of the period
        :param mid_dt: mid date of the period
        :param end_dt: end date of the period
        :param data: an xarray dataset
        :param variable_names: list of variable names
        """
        output_path = os.path.join(self.output_folder,self.get_output_filename(mid_dt)+".nc")
        data.to_netcdf(output_path)

    def close(self):
        pass

