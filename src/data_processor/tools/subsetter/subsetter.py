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

from ..common.netcdf4_formatter import NetCDF4Formatter
from ..common.geotiff_formatter import GeotiffFormatter

def subset(variables: list[str], lon_min: float, lon_max: float, lat_min: float, lat_max: float,
           start_date: datetime.datetime,
           end_date: datetime.datetime,
           input_path: str, output_path: str,
           output_name_pattern: str,
           output_format: str,
           y_dim_name: str = "lat", x_dim_name: str = "lon", t_dim_name: str = "time"):
    """
    Obtain an extract from a dataset.

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

    :param output_format:
        Name of the output format, eg "csv", "netcdf4", "geotiff"

    :param y_dim_name:
        Name of the y/lat dimension in the dataset

    :param x_dim_name:
        Name of the x/lon dimension in the dataset

    :param t_dim_name:
        Name of the time dimension in the dataset
    """

    os.makedirs(output_path, exist_ok=True)

    # create an extractor to read the relevant part of the input data covering the extraction times and spatial boundaries
    extractor = Extractor(location=input_path, variable_names=variables, y_dim_name=y_dim_name,
                            x_dim_name=x_dim_name, t_dim_name=t_dim_name, lon_min=lon_min, lat_min=lat_min, lon_max=lon_max,
                            lat_max=lat_max)

    if output_format == "netcdf":
        formatter = NetCDF4Formatter(output_path, output_name_pattern)
    elif output_format == "geotiff":
        formatter = GeotiffFormatter(output_path, output_name_pattern)
    else:
        raise Exception(f"Export format {output_format} is not supported")

    # loop over each time period in the required date range...
    for (mid_dt, slice_data, filename) in extractor.generate_data(start_dt=start_date, end_dt=end_date):
        formatter.write(data=slice_data,variable_names=variables,original_filename=filename)

    formatter.close()


def createParser():
    import argparse
    parser = argparse.ArgumentParser(description='extract regridded data.')

    parser.add_argument('--lon-min', type=float, default=-180,
                        help='The minimum longitude value in degrees. This must ' +
                             'range between -180.00 and +180. It must ' +
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
                             'range between -90.00 and +90.00. It must ' +
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
        default="{Y}{m}{d}{H}{M}{S}-EOCIS-LEVEL-PRODUCT-vVERSION-fv01.0"
    )

    parser.add_argument(
        "--output-format",
        metavar="<FORMAT>",
        help="define the output format",
        default="netcdf4"
    )

    parser.add_argument('--variables', default="",
                        help='Supply a comma separated list of variables.')


    return parser


def dispatch(args):
    # assemble the start and end dates from the input/output year/month/day components
    start_dt = datetime.datetime(args.start_year, args.start_month, args.start_day, 12, 0, 0)
    end_dt = datetime.datetime(args.end_year, args.end_month, args.end_day, 12, 0, 0)

    variables = list(map(lambda name: name.strip(), args.variables.split(",")))
    subset(variables=variables, lon_min=args.lon_min, lat_min=args.lat_min, lon_max=args.lon_max, lat_max=args.lat_max,
           start_date=start_dt, end_date=end_dt,
           output_path=args.out_path, input_path=args.in_path,
           output_name_pattern=args.output_name_pattern, output_format=args.output_format)

def main():
    parser = createParser()
    args = parser.parse_args()
    dispatch(args)


if __name__ == '__main__':
    main()
