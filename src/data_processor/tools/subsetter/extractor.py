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

"""
This module handles the extraction of a data subset, with aggregation if necessary
"""

import datetime
import xarray as xr
import os
import glob

class Extractor(object):

    """
    This class handles the efficient extraction of data from the input datset
    for the exact time and space bounded regions to be processed, and providing an iterator to lazily return data
    for each discrete time period.
    """

    def __init__(self, location:str, variable_names:list[str], y_dim_name:str, x_dim_name:str, t_dim_name:str,
                 lon_min, lat_min, lon_max, lat_max):
        """
        Constructor

        :param location:
            the location of the input dataset
        :param variable_names:
            list of variable names to include
        :param y_dim_name:
            the name of the y dimension
        :param x_dim_name:
            the name of the x dimension
        :param t_dim_name:
            the name of the time dimension
        :param lon_min:
            minimum longitude of box
        :param lat_min:
            minimum latitude of box
        :param lon_max:
            maximum longitude of box
        :param lat_max:
            maximum latitude of box
        """
        self.location = location
        self.variable_names = variable_names
        self.y_dim_name = y_dim_name
        self.x_dim_name = x_dim_name
        self.t_dim_name = t_dim_name
        self.lat_min = lat_min
        self.lat_max = lat_max
        self.lon_min = lon_min
        self.lon_max = lon_max

    def generate_year_data(self, start_date:datetime.datetime, end_date:datetime.datetime):
        """Generator that yields the time period within a year

        :param start_date: the datetime of the start day (inclusive).  Time must be set to mid day.
        :param end_date: the datetime of the end day (inclusive).  Time must be set to mid day.  Must be in same year as start_date.

        The generator yields (mid_dt,dataset,filename) tuples
        """

        dt = start_date
        while dt <= end_date:
            input_pattern = self.location.replace("{YEAR}",f"{dt.year:04d}")\
                .replace("{MONTH}",f"{dt.month:02d}")\
                .replace("{DAY}",f"{dt.day:02d}")

            matched_paths = glob.glob(input_pattern)
            if len(matched_paths) == 1:
                input_path = matched_paths[0]
                input_filename = os.path.split(input_path)[-1]
                data = xr.open_dataset(input_path)
                drop_variables = [name for name in data.variables.keys() if name not in self.variable_names and name not in data.dims]
                data = data.drop_vars(drop_variables)
                reverse_lat_order = data[self.y_dim_name].values[0] > data[self.y_dim_name].values[-1]
                data = data.sel({self.y_dim_name:slice(self.lat_max,self.lat_min) if reverse_lat_order else slice(self.lat_min,self.lat_max),
                    self.x_dim_name:slice(self.lon_min, self.lon_max)})
                yield (dt,data,input_filename)
            elif len(matched_paths) > 1:
                raise Exception("Incorrect configuration - more than 1 files match: "+input_pattern)
            dt += datetime.timedelta(days=1)

    def generate_data(self, start_dt:datetime.datetime, end_dt:datetime.datetime):
        """Generator that lazily yields the time period data for a given time and space range

        :param start_date: the datetime of the start day (inclusive).  Time must be set to mid day.
        :param end_date: the datetime of the end day (inclusive).  Time must be set to mid day.

        The generator yields ((start_dt,mid_dt,end_dt),xr.Dataset) tuples
        """
        year = start_dt.year
        while year <= end_dt.year:
            # go through each year in turn...
            slice_end_dt = datetime.datetime(year,12,31,12,0,0) if year < end_dt.year else end_dt
            slice_start_dt = datetime.datetime(year,1,1,12,0,0) if year > start_dt.year else start_dt
            # yield from that year's generator until exhausted
            yield from self.generate_year_data(slice_start_dt, slice_end_dt)
            # move to the next year
            year += 1



