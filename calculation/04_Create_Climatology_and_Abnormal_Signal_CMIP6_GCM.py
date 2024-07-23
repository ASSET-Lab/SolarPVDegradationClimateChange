# calculate the 3 hourly climatology for 1950-1976 baseline

import xarray as xr
import numpy as np
import dask
import glob
import os
from dask.diagnostics import ProgressBar
import shutil
import pandas as pd
import sys
# index=int(os.environ.get('SLURM_ARRAY_TASK_ID'))
models = ['ACCESS-CM2', 'BCC-CSM2-MR', 'CanESM5', 'CNRM-CM6-1', 'CMCC-CM2-SR5', 'CMCC-ESM2', 'EC-Earth3_r1i1p1f1',
          'EC-Earth3_r3i1p1f1', 'EC-Earth3_r4i1p1f1',
          'GFDL-ESM4', 'HadGEM3-GC31-LL', 'KACE-1-0-G', 'MIROC6', 'MIROC-ES2L', 'MPI-ESM1-2-HR_r1i1p1f1',
          'MPI-ESM1-2-HR_r2i1p1f1',
          'MPI-ESM1-2-LR', 'HadGEM3-GC31-MM', 'KIOST-ESM','MRI-ESM2-0']

csv_file_path_ssp = "/nfs/turbo/seas-mtcraig-climate/Haochi/PVTemp_Bias_Corr/tas_abnormal_from_qin.csv"  
csv_file_path_his = "/nfs/turbo/seas-mtcraig-climate/Haochi/PVTemp_Bias_Corr/tas_abnormal_from_qin.csv"

if len(sys.argv) != 2:
    sys.exit(1)
model_index = int(sys.argv[1])

model=models[model_index]

def read_hist_tas(model,year):
    ipath=glob.glob('/scratch/negishi/wu2411/Scratch/Vct_result_gcm/' + f'{model}/' + 'PVTemp_close_mount_glass_glass_*' + str(year) + '.nc')[0]
    tmp=xr.open_dataset(ipath)['pvtemp_deg°C'].chunk({'time': 240})
    return tmp
    
# latitude issue for model 'EC-Earth3_r3i1p1f1' and 'MPI-ESM1-2-HR_r2i1p1f1'
sample=read_hist_tas(model,2014)

def isleap(year):
    if (year % 4) == 0:
        if (year % 100) == 0:
            if (year % 400) == 0:
                return True
            else:
                return False
        else:
            return True
    else:
        return False

def read_tw(model, years):
    result = {}
    for year in years:
        ipath = glob.glob(
            '/scratch/negishi/wu2411/Scratch/Vct_result_gcm/' + f'{model}/' + 'PVTemp_close_mount_glass_glass_*' + str(
                year) + '.nc')[0]
        tmp = xr.open_dataset(ipath)['pvtemp_deg°C'].chunk({'time': 240})
        tmp['lat']=sample.lat.values
        tmp['lon']=sample.lon.values
        result[year] = tmp
        print(f'handleing {ipath}')
    result = xr.concat(list(result.values()), dim='time', coords='minimal', compat='override')
    return result


def hourly(data):
    # calculate hourly climatology
    data['hourofyear'] = xr.DataArray(data.time.dt.strftime('%m-%d %H'), coords=data.time.coords)
    data = data.groupby('hourofyear').mean('time')
    return data


def defattrs(data, var):
    data.attrs['long_name'] = 'climatological 3-hourly cmip6 ' + var + ' during 1950-1976'
    return data

def find_warming_years(csv_file, csv_file2, gcm_model, warming_target):
   
    data = pd.read_csv(csv_file)

    if gcm_model not in data.columns:
        return f"GCM model {gcm_model} not found in the dataset."

   
    lower_bound = warming_target - 0.25
    upper_bound = warming_target + 0.25

    
    years = data[(data[gcm_model] >= lower_bound) & (data[gcm_model] <= upper_bound)]["year"]
    years1 = years.tolist()
    years_final = years1

    return years_final

csv_file_path_ssp = "/home/wu2411/PVTemp_Bias_Corr/tas_abnormal_from_qin.csv"  
csv_file_path_his = "/home/wu2411/PVTemp_Bias_Corr/tas_abnormal_from_qin.csv"  

## Step 1 ###
##### Doing manual calculation of climatology
#####
#####
###################
targets = [0, 0.8, 1, 1.5, 2, 2.5, 3, 3.5, 4]

for target in targets:
    if target == 0:
        if model == 'MRI-ESM2-0':
            years = [str(year) for year in range(1960, 1977)]
        else:
            years = [str(year) for year in range(1950, 1977)]
    else:
        years = find_warming_years(csv_file_path_his, csv_file_path_ssp, model, target)
    print(years)
    tw = (read_tw(model, years)).pipe(hourly).pipe(defattrs, 'Tpv')
    path = f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/'
    if not os.path.exists(path):
        os.makedirs(path)
    with ProgressBar():
        tw.astype('float32').to_netcdf(f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/Tpv_hourly_'+model+'_'+str(target)+'_C.nc')

    print('finish')

## Step 2 ###
##### Doing manual calculation of hourly anormaly
targets = [0, 1, 1.5, 2, 2.5, 3, 3.5, 4]
for target in targets:
    tas_xc = xr.open_dataset(        f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/Tpv_hourly_' + model + '_' + str(target) + '_C.nc')
    tas_baseline = xr.open_dataset(        f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/Tpv_hourly_{model}_0_C.nc')
    tas_anomaly = (tas_xc - tas_baseline)
    tas_anomaly.astype('float32').to_netcdf(        f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/Tpv_hourly_anomaly_' + model + '_' + str(target) + '_C.nc')