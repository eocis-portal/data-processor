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
This module handles the extraction of SST, uncertainty and sea-ice fraction data from L4 SST data saved in zarr format
(see the "timeseries_region.reslicing" module for the code that creates the zarr format data)
"""

import datetime

import xarray
import os.path

from .utils import createTimePeriods

class Extractor(object):

    """
    This class handles the efficient extraction of data from the input datset
    for the exact time and space bounded regions to be processed, and providing an iterator to lazily return data
    for each discrete time period.
    """

    def __init__(self, location, variable_names:list[str], t_dim_name:str):
        """
        Constructor

        :param location:
            the location of the input dataset
        :param variable_names:
            list of variable names to include
        :param t_dim_name:
            the name of the time dimension
        """
        self.location = location
        self.variable_names = variable_names
        self.t_dim_name = t_dim_name


    def generateYearData(self,start_date,end_date,temporal_resolution,min_lon,min_lat,max_lon,max_lat):
        """Generator that yields the time period within a year

        :param start_date: the datetime of the start day (inclusive).  Time must be set to mid day.
        :param end_date: the datetime of the end day (inclusive).  Time must be set to mid day.  Must be in same year as start_date.
        :param temporal_resolution:  the time resolution as "daily"|"pentad"|"dekad"|"N" where N is an integer number of days
        :param min_lon:  minimum longitude of box, must be aligned on 0.05 degree boundary
        :param min_lat:  minimum latitude of box, must be aligned on 0.05 degree boundary
        :param max_lon:  maximum longitude of box, must be aligned on 0.05 degree boundary
        :param max_lat:  maximum latitude of box, must be aligned on 0.05 degree boundary

        The generator yields ((start_dt,mid_dt,end_dt),dataset) tuples
        """

        input_path = self.location.replace("{YEAR}",str(start_date.year))
        z = xarray.open_mfdataset(input_path)

        drop_variables = [name for name in z.variables.keys() if name not in self.variable_names and name not in z.dims]
        z = z.drop_vars(drop_variables)

        time_periods = createTimePeriods(temporal_resolution,start_date,end_date)

        for(period_start_dt,period_mid_dt,period_end_dt) in time_periods:
            yield ((period_start_dt,period_mid_dt,period_end_dt),z)

    def generateData(self, start_dt, end_dt, temporal_resolution, min_lon, min_lat, max_lon, max_lat):
        """Generator that lazily yields the time period data for a given time and space range

        :param start_date: the datetime of the start day (inclusive).  Time must be set to mid day.
        :param end_date: the datetime of the end day (inclusive).  Time must be set to mid day.
        :param temporal_resolution:  the time resolution as "daily"|"pentad"|"dekad"|"N" where N is an integer number of days
        :param min_lon:  minimum longitude of box, must be aligned on 0.05 degree boundary
        :param min_lat:  minimum latitude of box, must be aligned on 0.05 degree boundary
        :param max_lon:  maximum longitude of box, must be aligned on 0.05 degree boundary
        :param max_lat:  maximum latitude of box, must be aligned on 0.05 degree boundary

        The generator yields ((start_dt,mid_dt,end_dt),cf.FieldList) tuples
        """
        year = start_dt.year
        while year <= end_dt.year:
            # go through each year in turn...
            slice_end_dt = datetime.datetime(year,12,31,12,0,0) if year < end_dt.year else end_dt
            slice_start_dt = datetime.datetime(year,1,1,12,0,0) if year > start_dt.year else start_dt
            # yield from that year's generator until exhausted
            yield from self.generateYearData(slice_start_dt,slice_end_dt,temporal_resolution,min_lon,min_lat,max_lon,max_lat)
            # move to the next year
            year += 1



