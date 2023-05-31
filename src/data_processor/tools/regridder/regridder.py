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
This module extracts regions of data from the L4 C3S/CCI dataset.  The time series:

* aggregates data over a number of different time granularities (daily, N-daily, pentads, dekads, monthly)
* region is defined using a spatial bounding box aligned on 0.05 degree boundaries and up to 5 degrees on each side
* can compute climatology-based anomalies or absolute SSTs
* allows the user to ignore input cells with sea ice fractions above a user specified value (f_max)
* outputs a a standard error based on user specified parameters tau and spatial lambda
* optionally, outputs the sea ice fraction of the ocean area
* outputs to netcdf4 or comma separated variable formats

This data accepts as input climatology and C3S/CCI L4 SST files that have been spatially resliced from the original
one-file-per-day format.  The resliced format stores data in zarr format, with 5 degree spatial and 7 day temporal chunking
and is efficient for the retrieval of time series data for a small area.

This module can be invoked from the command line, use --help to show the options.
This module can also be invoked by importing and calling the makeTimeSeriesSSTs function
"""

import os.path
import datetime
from .extractor import Extractor
from .aggregator import Aggregator
from .netcdf4formatter import NetCDF4Formatter


def regrid(variables: list[str], lon_min: float, lon_max: float, lat_min: float, lat_max: float,
           temporal_resolution: str, spatial_resolution: float,
           start_date: datetime.datetime,
           end_date: datetime.datetime,
           input_path: str, output_path: str,
           output_name_pattern: str,
           y_dim_name: str = "lat", x_dim_name: str = "lon", t_dim_name: str = "time"):
    """
    Obtain a time series from SST data.

    :param variables:
        A list of variable names to extract

    :param lon_min:
        The minimum longitude value of the spatial area to aggregate.  Must be aligned on 0.05 degree boundary.

    :param lon_max:
        The maximum longitude value of the spatial area to aggregate.  Must be aligned on 0.05 degree boundary.

    :param lat_min:
        The minimum latitude value of the spatial area to aggregate.  Must be aligned on 0.05 degree boundary.

    :param lat_max:
        The maximum latitude value of the spatial area to aggregate.  Must be aligned on 0.05 degree boundary.

    :param temporal_resolution:
        The temporal resolution to aggregate, one of "daily","pentad","dekad","monthly" or "N" where N is a number of days >= 1

    :param spatial_resolution:
        The spatial resolution to aggregate, in degrees lat/lon.  Set to 0 to generate a time series.

    :param start_date:
        The date at which the extracted data should begin.

    :param end_date:
        The date at which the extracted data should end.

    :param input_path:
        Path of the folder containing spatially resliced SST and climatology data providing the input data.

    :param output_path:
        Path to write the time series data.  Must be a filename, data will be written in CSV format if the file extension is .csv

    :param output_name_pattern:
        Filename pattern to write.

    :param y_dim_name:
        Name of the y/lat dimension in the dataset

    :param x_dim_name:
        Name of the x/lon dimension in the dataset

    :param t_dim_name:
        Name of the time dimension in the dataset
    """

    os.makedirs(output_path, exist_ok=True)

    # create an extractor to read the relevant part of the input data covering the extraction times and spatial boundaries
    extractor = Extractor(location=input_path, variable_names=variables, t_dim_name=t_dim_name)

    # create an aggregator to aggregate each period in the extracted data
    aggregator = Aggregator(lon_min=lon_min, lat_min=lat_min, lon_max=lon_max,
                            lat_max=lat_max, spatial_resolution=spatial_resolution, x_dim_name=x_dim_name,
                            y_dim_name=y_dim_name,
                            t_dim_name=t_dim_name)

    # create a formatter (either CSV or netcdf4 based) to handle writing the aggregated data to file
    formatter = NetCDF4Formatter(output_path, output_name_pattern)

    period_duration = end_date.timestamp() - start_date.timestamp()

    # loop over each time period in the required date range...
    for (dates, slice_data) in extractor.generate_data(start_dt=start_date, end_dt=end_date,
                                                       temporal_resolution=temporal_resolution,
                                                       min_lon=lon_min, min_lat=lat_min, max_lon=lon_max,
                                                       max_lat=lat_max):
        # get the first,middle and end date of the period
        (s_dt, mid_dt, e_dt) = dates

        # aggregate this time period...
        aggregated_data = aggregator.aggregate(start_dt=s_dt, end_dt=e_dt, data=slice_data)
        # print("slice:",mid_dt,sst_or_anomaly,uncertainty,sea_ice_fraction)

        # and append it to the output file
        formatter.write(s_dt, mid_dt, e_dt, aggregated_data)

    formatter.close()


def createParser():
    import argparse
    parser = argparse.ArgumentParser(description='extract regridded data.')

    parser.add_argument('--temporal-resolution', default="5-day",
                        help="The target time resolution. This can be 'monthly', 'daily'," +
                             "'10-day' for dekads, '5-day' for pentads or an integer for regular " +
                             " N day regridding aligned with the start of the year, or daily.")

    parser.add_argument('--spatial-resolution', type=float, default=0.05,
                        help="The target spatial resolution, in degrees.")

    parser.add_argument('--lon-min', type=float, default=-180,
                        help='The minimum longitude value in degrees. This must ' +
                             'range between -180.00 and +179.95. It must ' +
                             'also be less than the maximum longitude value.')

    parser.add_argument('--lon-max', type=float, default=180,
                        help='The maximum longitude value in degrees. This must ' +
                             'range between -180.00 and +180.00. It must ' +
                             'also be greater than the minimum longitude value.')

    parser.add_argument('--lat-min', type=float, default=-90,
                        help='The minimum latitude value in degrees. This must ' +
                             'range between -90.00 and +90. It must ' +
                             'also be less than the maximum latitude value.')

    parser.add_argument('--lat-max', type=float, default=90,
                        help='The maximum latitude value in degrees. This must ' +
                             'range between -89.95 and +90.00. It must ' +
                             'also be greater than the minimum latitude value.')

    parser.add_argument('--start-year', type=int,
                        help='The start year of the time series.')

    parser.add_argument('--start-month', type=int,
                        help='The start month of the time series.')

    parser.add_argument('--start-day', type=int,
                        help='The start day of the time series.')

    parser.add_argument('--end-year', type=int,
                        help='The end year of the time series.')

    parser.add_argument('--end-month', type=int,
                        help='The end month of the time series.')

    parser.add_argument('--end-day', type=int,
                        help='The end day of the time series.')

    parser.add_argument('--in-path',
                        help='Path to the input dataset.')

    parser.add_argument('--out-path',
                        help='The path in which to write the output.')

    parser.add_argument(
        "--output-name-pattern",
        metavar="<FILE-PATTERN>",
        help="define a pattern for creating the output file names",
        default="{Y}{m}{d}{H}{M}{S}-EOCIS-LEVEL-PRODUCT-vVERSION-fv01.0.nc"
    )

    parser.add_argument('--variables', default="",
                        help='Supply a comma separated list of variables.')

    return parser


def dispatch(args):
    # assemble the start and end dates from the input/output year/month/day components
    start_dt = datetime.datetime(args.start_year, args.start_month, args.start_day, 12, 0, 0)
    end_dt = datetime.datetime(args.end_year, args.end_month, args.end_day, 12, 0, 0)

    variables = list(map(lambda name: name.strip(), args.variables.split(",")))
    regrid(variables=variables, lon_min=args.lon_min, lat_min=args.lat_min, lon_max=args.lon_max, lat_max=args.lat_max,
           temporal_resolution=args.temporal_resolution, spatial_resolution=args.spatial_resolution,
           start_date=start_dt, end_date=end_dt,
           output_path=args.out_path, input_path=args.in_path,
           output_name_pattern=args.output_name_pattern)


def main():
    parser = createParser()
    args = parser.parse_args()
    dispatch(args)


if __name__ == '__main__':
    main()
