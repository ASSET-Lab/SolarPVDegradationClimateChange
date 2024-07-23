#########################################################
#########################################################
# Do the hourly bias correction for each 27 years of ERA5, with 7 warming levels, for 20 CMIP6 GCMs
#########################################################
#########################################################

import xarray as xr
import numpy as np
import dask
import glob
import os
from dask.diagnostics import ProgressBar
import shutil
import pandas as pd

index = int(os.environ.get('SLURM_ARRAY_TASK_ID'))
models = ['ACCESS-CM2', 'BCC-CSM2-MR', 'CanESM5', 'CNRM-CM6-1', 'CMCC-CM2-SR5', 'CMCC-ESM2', 'EC-Earth3_r1i1p1f1',
          'EC-Earth3_r3i1p1f1', 'EC-Earth3_r4i1p1f1',
          'GFDL-ESM4', 'HadGEM3-GC31-LL', 'KACE-1-0-G', 'MIROC6', 'MIROC-ES2L', 'MPI-ESM1-2-HR_r1i1p1f1',
          'MPI-ESM1-2-HR_r2i1p1f1',
          'MPI-ESM1-2-LR', 'HadGEM3-GC31-MM', 'KIOST-ESM', 'MRI-ESM2-0']

csv_file_path_ssp = "/nfs/turbo/seas-mtcraig-climate/Haochi/PVTemp_Bias_Corr/tas_abnormal_from_qin.csv" 
csv_file_path_his = "/nfs/turbo/seas-mtcraig-climate/Haochi/PVTemp_Bias_Corr/tas_abnormal_from_qin.csv" 

years = np.arange(1950, 1977)
models = np.repeat(models, len(years))
years = np.tile(years, 20)

year = years[index]
model = models[index]

targets = [1, 1.5, 2, 2.5, 3, 3.5, 4]
targets = [3.5, 4]
for target in targets:
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
    
    
    print(f'handling year:{year}')
    
    
    def addindex(data, addnum):
        data['index'] = data['index'] + addnum
        return data
    
    
    def calendar(data):
        if len(data.hourofyear) == 2928:  # leap year under real world calendar: 366*8=2928
            # tmp=data.drop_sel(hourofyear=['02-29 00','02-29 03','02-29 06','02-29 09','02-29 12', '02-29 15','02-29 18', '02-29 21'])
            tmp = data.drop_sel(
                hourofyear=['02-29 01', '02-29 04', '02-29 07', '02-29 10', '02-29 13', '02-29 16', '02-29 19',
                            '02-29 22'])
            tmp = tmp.rename({'hourofyear': 'index'})
            tmp['index'] = np.arange(1, len(tmp.index) + 1)
        elif len(data.hourofyear) == 2880:  # 360-day calendar (some CMIP6 models use this calendar)
            tmp0 = data.rename({'hourofyear': 'index'})
            tmp0['index'] = np.arange(1, len(tmp0.index) + 1)
            tmp1 = tmp0.sel(index=slice(1, 72 * 8)).pipe(addindex, 0)
            tmpinsert1 = tmp0.sel(index=slice(71 * 8 + 1, 72 * 8)).pipe(addindex,
                                                                        8)  # here and below we insert one day every 72 days which give us extra five days; addindex function adjust the "index" coordinate since we manually insert extra days.
            tmp2 = tmp0.sel(index=slice(72 * 8 + 1, 144 * 8)).pipe(addindex, 8)
            tmpinsert2 = tmp0.sel(index=slice(143 * 8 + 1, 144 * 8)).pipe(addindex, 8 * 2)
            tmp3 = tmp0.sel(index=slice(144 * 8 + 1, 72 * 3 * 8)).pipe(addindex, 8 * 2)
            tmpinsert3 = tmp0.sel(index=slice(72 * 3 * 8 - 7, 72 * 3 * 8)).pipe(addindex, 8 * 3)
            tmp4 = tmp0.sel(index=slice(72 * 3 * 8 + 1, 72 * 4 * 8)).pipe(addindex, 8 * 3)
            tmpinsert4 = tmp0.sel(index=slice(72 * 4 * 8 - 7, 72 * 4 * 8)).pipe(addindex, 8 * 4)
            tmp5 = tmp0.sel(index=slice(72 * 4 * 8 + 1, 72 * 5 * 8)).pipe(addindex, 8 * 4)
            tmpinsert5 = tmp0.sel(index=slice(72 * 5 * 8 - 7, 72 * 5 * 8)).pipe(addindex, 8 * 5)
            tmp = xr.concat([tmp1, tmpinsert1, tmp2, tmpinsert2, tmp3, tmpinsert3, tmp4, tmpinsert4, tmp5, tmpinsert5],
                            dim='index')
        elif len(data.hourofyear) == 2920:  # nonleap years good to go
            tmp = data.rename({'hourofyear': 'index'})
            tmp['index'] = np.arange(1, len(tmp.index) + 1)
        return tmp
    
    # interp CMIP6 anomaly to ERA5 resolution
    def interp_anomaly(data, sample):
        data = data.interp(lat=sample.lat.values, lon=sample.lon.values)
        return data
    
    # address the NaN near 0E caused by interpolation
    def interp(data):
        longitude = data.lon.values
        longitude = xr.where(longitude > 180, longitude - 360, longitude)
        data['lon'] = longitude
        data = data.sortby('lon')
        longitude = data.lon.values
        data = data.dropna(dim='lon', how='all').interp(lon=longitude)
        longitude = xr.where(longitude < 0, longitude + 360, longitude)
        data['lon'] = longitude
        data = data.sortby('lon')
        return data
    
    Tpv_anomal = xr.open_dataset(
        f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/Tpv_hourly_anomaly_' + model + '_' + str(
            target) + '_C.nc').chunk(
        {'hourofyear': 1})
    Tpv_anomal = Tpv_anomal.pipe(calendar)
    new_index = np.arange(1, len(Tpv_anomal.index) * 3 + 1)
    
    # Tpv_anomal['pvtemp_deg°C'].sel(index=1).plot()
    # from matplotlib import pyplot as plt
    # plt.show()
    
    index_map = np.repeat(np.arange(1, 2921), 3)[:8760]
    Tpv_anomal_extd = Tpv_anomal.sel(index=index_map)
    Tpv_anomal_extd['index'] = new_index
    
    Tpv_era5 = xr.open_dataset(
        f'/scratch/negishi/wu2411/Scratch/Vct_result/PVTemp_close_mount_glass_glass_era5_{year}.nc').chunk(
        {'time': 2})
    
    if isleap(year):  # drop 2-29 if it's leap year
        date_era5 = np.arange(str(year) + '-02-29', str(year) + '-03-01', np.timedelta64(3, 'h'),
                              dtype='datetime64[h]')
        Tpv_era5 = Tpv_era5.drop_sel(time=date_era5).chunk({'time': 2})
    
    Tpv_era5 = Tpv_era5.rename({'lat': 'lat', 'lon': 'lon', 'time': 'index'})
    Tpv_era5['index'] = np.arange(1, len(Tpv_era5.index) + 1)
    
    Tpv_anomal_extd = Tpv_anomal_extd.pipe(interp_anomaly, Tpv_era5).pipe(interp)
    
    # get bias-corrected tas and Tw by adding cmip6 anomaly to ERA5 baseline, and do the post-processing
    Tpv_bc = (Tpv_era5 + Tpv_anomal_extd)
    with ProgressBar():
        Tpv_bc.astype('float32').to_netcdf(
            f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/Tpv_hourly_era5_{year}_gcm_' + model + '_' + str(
                target) + '_C.nc')
    
    
    ## Step 4   Plot
    ############
    
    
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import xarray as xr
    import regionmask
    import cartopy.feature as cfeature
    import matplotlib as mpl
    import numpy as np
    
    chunk_size = {'lon': 50}
    
    color1 = np.array([
        [225, 225, 225],
        [255, 245, 204],
        [255, 230, 112],
        [255, 204, 51],
        [255, 175, 51],
        [255, 153, 51],
        [255, 111, 51],
        [255, 85, 0],
        [230, 40, 30],
        [200, 30, 20]])
    cmap1 = mpl.colors.ListedColormap(color1 / 255.)
    
    levels1 = [60, 70, 72, 74, 76, 78, 80, 82, 84, 90]
    norm1 = mpl.colors.BoundaryNorm(levels1, ncolors=len(levels1), extend='max')
    
    ds = xr.open_dataset(        f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/Tpv_hourly_era5_{year}_gcm_' + model + '_' + str(
            target) + '_C.nc').chunk(
        chunk_size)
    with ProgressBar():
        specific_time_data = ds.quantile(0.98, dim='index')
        specific_time_data.to_netcdf(            f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/TPV_98_hourly_ERA5_{year}_GCM_{target}_C.nc')
    
    ## Step 5 Plot T98
    ####
    ####
    #######################################
    
    import matplotlib.pyplot as plt
    import cartopy.crs as ccrs
    import xarray as xr
    import regionmask
    import cartopy.feature as cfeature
    import matplotlib as mpl
    import numpy as np
    
    chunk_size = {'lon': 50}
    
    color1 = np.array([
        [225, 225, 225],
        [255, 245, 204],
        [255, 230, 112],
        [255, 204, 51],
        [255, 175, 51],
        [255, 153, 51],
        [255, 111, 51],
        [255, 85, 0],
        [230, 40, 30],
        [200, 30, 20]])
    cmap1 = mpl.colors.ListedColormap(color1 / 255.)
    
    levels1 = [60, 70, 72, 74, 76, 78, 80, 82, 84, 90]
    norm1 = mpl.colors.BoundaryNorm(levels1, ncolors=len(levels1), extend='max')
    ds = xr.open_dataset(
        f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/TPV_98_hourly_ERA5_{year}_GCM_{target}_C.nc')
    
    specific_time_data = ds
    land = regionmask.defined_regions.natural_earth_v5_0_0.land_110
    land_mask = land.mask(specific_time_data, lon_name='lon', lat_name='lat')
    
    land_solar_zenith = specific_time_data["pvtemp_deg°C"].where(land_mask == 0)
    
    fig = plt.figure(figsize=(24, 12))
    ax = plt.axes(projection=ccrs.Robinson(central_longitude=10))
    
    land_solar_zenith.plot(ax=ax, transform=ccrs.PlateCarree(),
                           x='lon', y='lat', norm=norm1, rasterized=True,
                           add_colorbar=True, cmap=cmap1)
    
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.COASTLINE)
    ax.set_global()
    
    subregions = [
        {'lon1': -130, 'lon2': -70, 'lat1': 20, 'lat2': 55}, 
       
    ]
    
    # plt.show()
    plt.savefig(
        f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/TPV_98_hourly_ERA5_{year}_GCM_{target}_C.pdf',
        format='pdf',
        bbox_inches='tight')
