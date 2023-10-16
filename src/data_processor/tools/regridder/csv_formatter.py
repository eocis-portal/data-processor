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

import csv

from .formatter import Formatter
import os

HEADER2a = "Time series of %(time_resolution)s %(var_name)s averaged across ocean surface"+ \
    " within latitudes %(south_lat)0.2f and %(north_lat)0.2f and longitudes %(west_lon)0.2f and %(east_lat)0.2f," + \
    " in kelvin and degree Celsius. "
HEADER2b = "Areas where sea-ice concentration exceeded %(fmax_pct)d %% were excluded in the average temperature calculation. "

class CSVFormatter(Formatter):
    """
    Create a formatter for writing either timeseries to a netcdf4 output file
    """

    def __init__(self, path: str = "", name_pattern: str = ""):
        """
        Construct the CSV formatter using options

        :param path: path of an output folder in which to create the output files
        :param name_pattern: a pattern to use to create the output file names

        """
        super().__init__(path, name_pattern)
        self.outfile = None
        self.writer = None


    def write(self, start_dt, mid_dt, end_dt, data, variable_names):
        """
        Write an entry to the output file covering a time period
        :param start_dt: start date of the period
        :param mid_dt: mid date of the period
        :param end_dt: end date of the period
        :param data: an xarray dataset
        :param variable_names: list of variable names
        """
        if self.outfile is None:
            output_path = os.path.join(self.output_folder,self.get_output_filename(mid_dt) + ".csv")
            self.outfile = open(output_path,"w")
            self.writer = csv.writer(self.outfile)
            outrow = ["year","month","day"]+variable_names
            self.writer.writerow(outrow)

        outrow = [mid_dt.year, mid_dt.month, mid_dt.day]
        for variable in variable_names:
            outrow.append(float(data[variable].mean(skipna=True)))
        self.writer.writerow(outrow)

    def close(self):
        """close the formatter and flush changes to disk"""
        del self.writer
        self.writer = None
        self.outfile.close()