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
This module slices and aggregates input spatiotemporal data to the desired spatial and temporal resolution
"""
import datetime
import xarray as xr
import numpy as np


def mean_log_reducer(data, axis):
    return np.power(10, np.nanmean(np.log10(data), axis))


class Aggregator(object):
    """
    Aggregate data from a 3 dimensional box (bounded by time, latitude and longitude) to yield a 2D or scalar value

    The yielded values are 2D numpy arrays, in timeseries mode they are scalar values

    """

    def __init__(self, spatial_resolution:float,
                 y_dim_name:str, x_dim_name:str, t_dim_name:str):
        """
        Construct the aggregator with the given spatial parameters

        :param spatial_resolution: the spatial resolution for the output (set to zero for timeseries, None for original resolution)
        :param y_dim_name: the name of the y dimension
        :param x_dim_name: the name of the x dimension
        :param t_dim_name: the name of the time dimension

        """
        self.spatial_resolution = spatial_resolution
        self.lat_weighting = None
        self.y_dim_name = y_dim_name
        self.x_dim_name = x_dim_name
        self.t_dim_name = t_dim_name

    def format_dt(self, dt: datetime.datetime) -> str:
        return dt.strftime("%Y-%m-%d")

    def aggregate(self, start_dt:datetime.datetime, end_dt:datetime.datetime, data:xr.Dataset, methods) -> xr.Dataset:
        """
        Perform aggregation on the data pertaining to a particular time period

        :param start_dt: the start date of the period (inclusive)
        :param end_dt: the end date of the period (inclusive)
        :param data: an xarray dataset with the input cube
        :param methods: the aggregation methods, map from variable name to method eg "SST"=>"mean"

        :return: xarray dataset containing aggregated values
        """

        data = data.sel({self.t_dim_name:slice(self.format_dt(start_dt),self.format_dt(end_dt))})

        nlat = data.sizes[self.y_dim_name]
        nlon = data.sizes[self.x_dim_name]
        ntime = data.sizes[self.t_dim_name]

        # FIXME just use the first aggregation method for everything
        # need to move to per DataArray aggregation
        aggregation_method = "mean"
        for (name,method) in methods.items():
            aggregation_method = method
            break

        print("Using aggregation method:"+aggregation_method)

        if self.spatial_resolution is not None and self.spatial_resolution > 0:
            coarsen_factor_x = round(nlon/((self.lon_max-self.lon_min)/self.spatial_resolution))
            coarsen_factor_y = round(nlat/((self.lat_max - self.lat_min)/self.spatial_resolution))
            coarsen_factor_t = ntime

            coarsened_data = data.coarsen({self.y_dim_name:coarsen_factor_y,
                                 self.x_dim_name:coarsen_factor_x,
                                 self.t_dim_name:coarsen_factor_t})
            if aggregation_method == "mean":
                data = coarsened_data.mean(skipna=True)
            elif aggregation_method == "mean-log":
                data = coarsened_data.reduce(mean_log_reducer,keep_attrs=True)
            else:
                raise Exception(f"Unsupported aggregation method: {method}")
        elif self.spatial_resolution is None:
            pass
        else:
            # time series, aggregate the whole cube
            if aggregation_method == "mean":
                data = data.mean(skipna=True)
            elif aggregation_method == "mean-log":
                data = data.reduce(mean_log_reducer, dims=(self.t_dim_name,self.x_dim_name,self.y_dim_name),
                                   keep_attrs=True, keepdims=False)
        return data

