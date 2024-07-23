import numpy as np
import pandas as pd
from numpy import arccos, arcsin, arctan2, cos, deg2rad, pi, sin
import xarray as xr
import shutil
import os
import dask
from dask.diagnostics import ProgressBar
import sys


def handle_calcu_single_year(scenario,year,mount_method):
    
    chunk_size = {'lon': 50}
    ds = xr.open_dataset(f'/scratch/negishi/wu2411/Scratch/Vct_result/PVTemp_{mount_method}_era5_{year}.nc').chunk(chunk_size)

    with ProgressBar():
        specific_time_data = ds.quantile(0.98, dim='time')
        specific_time_data.to_netcdf(f'/scratch/negishi/wu2411/Scratch/Vct_result/PVT98_Vec_{mount_method}_ERA5_{year}.nc')

    return 0  
    
def main(model_index, mounting_config, years_list):

    scenarios = ['era5'] #
    years = years_list
    for year in years:
        print(f'handle_calcu_single_year_{year}')
        handle_calcu_single_year(scenarios[0],year,'close_mount_glass_glass')
        
if __name__ == "__main__":
    #
    if len(sys.argv) != 4:
        print("Usage: python my_script.py 1model_index,2mounting_config,3year_start,4year_end")
        sys.exit(1)
    model_index = int(sys.argv[1])  # 5
    mounting_config = sys.argv[2]  # "close_mount_glass_glass"
    years_list = sys.argv[3]  # 2010
    years_str_list = years_list.split(',')

    main(model_index, mounting_config, years_str_list)
