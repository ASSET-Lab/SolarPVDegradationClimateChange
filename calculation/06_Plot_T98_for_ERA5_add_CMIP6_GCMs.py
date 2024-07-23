#########################################################
#########################################################
# Do the T98 calculation for hourly bias correction for each 27 years of ERA5, with 7 warming levels, for 20 CMIP6 GCMs
#########################################################
#########################################################
import os
import numpy as np
import pandas as pd
import xarray as xr

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import xarray as xr
import regionmask
import cartopy.feature as cfeature
import matplotlib as mpl
import numpy as np

# Define models
models = ['ACCESS-CM2', 'BCC-CSM2-MR', 'CanESM5', 'CNRM-CM6-1', 'CMCC-CM2-SR5', 'CMCC-ESM2', 'EC-Earth3_r1i1p1f1',
          'EC-Earth3_r3i1p1f1', 'EC-Earth3_r4i1p1f1', 'GFDL-ESM4', 'HadGEM3-GC31-LL', 'KACE-1-0-G', 'MIROC6',
          'MIROC-ES2L', 'MPI-ESM1-2-HR_r1i1p1f1', 'MPI-ESM1-2-HR_r2i1p1f1', 'MPI-ESM1-2-LR', 'HadGEM3-GC31-MM',
          'KIOST-ESM', 'MRI-ESM2-0']

index = int(os.environ.get('SLURM_ARRAY_TASK_ID'))
# index = 19

targets = [1, 1.5, 2, 2.5, 3, 3.5, 4]
models = np.repeat(models, len(targets))
targets = np.tile(targets, 20)
target = targets[index]
model = models[index]

years = np.arange(1950, 1977)

# Repeat models and tile years to match the full index range
expanded_models = np.repeat(models, len(years))
expanded_years = np.tile(years, len(models))

# Targets
targets = [int(target)]
for year in years:
    for target in targets:
        print(f'{model}:_{year}_{target}')
        
        temp=xr.open_dataset(f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/TPV_98_hourly_ERA5_{year}_GCM_{target}_C.nc')
        
        
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
        ds = temp
        
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
        # ax.add_feature(cfeature.BORDERS)
        
        ax.set_global()
        
        subregions = [
            {'lon1': -130, 'lon2': -70, 'lat1': 20, 'lat2': 55},  # 示例区域1
            # 添加更多区域
        ]
        plt.title(f'{model}:_{year}_GCM_{target}_C')
        plt.savefig(
            f'/scratch/negishi/wu2411/Scratch/CMIP6_hourly_climatology_Tpv_tas_baseline_preindXC/climatology/{model}/TPV_98_hourly_ERA5_{year}_GCM_{target}_C.pdf',
            format='pdf',
            bbox_inches='tight')