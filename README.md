# data-processor

create conda environment for regridding tasks

```
conda create -n data_processor_env python=3.9 rioxarray netcdf4 psycopg2 dask flask
```

start the daemon, environment needs psycopg2 and data_manager installed

```
python -m data_processor.daemon.daemon
```