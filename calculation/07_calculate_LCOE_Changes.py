import xarray as xr
import numpy as np
import matplotlib.pyplot as plt

def calculate_single_years_gap_and_compare_lcoe_changes(model,year,target):
    # chunks = {'lon':24}
    # .chunk(chunks)

    cf_datafile = xr.open_dataset(
        f'/gpfs/accounts/mtcraig_root/mtcraig0/haochiw/Vct_result_gcm/{model}/CummuCFs_close_mount_glass_glass_hourly_ERA5_{year}_GCM_{target}_C.nc') \
        .__xarray_dataarray_variable__.load()
    CFs_accumu = cf_datafile.sum(dim='time') * 3
    tpv_close_datafile = xr.open_dataset(
         f'/gpfs/accounts/mtcraig_root/mtcraig0/haochiw/Vct_result_gcm/{model}/TPV_close_mount_glass_glass_hourly_ERA5_{year}_GCM_{target}_C.nc')[
        'pvtemp_deg°C'].load()
    tpv_open_datafile = xr.open_dataset(
         f'/gpfs/accounts/mtcraig_root/mtcraig0/haochiw/Vct_result_gcm/{model}/TPV_open_rack_glass_glass_hourly_ERA5_{year}_GCM_{target}_C.nc')[
        'pvtemp_deg°C'].load()
    T98_0 = tpv_close_datafile.quantile(0.98, dim='time')
    T98_inf = tpv_open_datafile.quantile(0.98, dim='time')
    T98_modi = 70
    x_0 = 6.1

    def calculate_gap(T98_0, T98_inf, T98_modi, x_0):
        denominator = T98_0 - T98_inf
        safe_denominator = np.where(denominator != 0, denominator, np.nan)
        numerator = T98_0 - T98_modi
        valid_numerator = np.where(numerator < safe_denominator, numerator, safe_denominator - 1)
        with np.errstate(divide='ignore', invalid='ignore'):
            Gap = -x_0 * np.log(1 - valid_numerator / safe_denominator)
        Gap = xr.DataArray(Gap, dims=T98_0.dims, coords=T98_0.coords)
        return Gap

    x_0_example = 6.1

    Gap_example = calculate_gap(T98_0, T98_inf, T98_modi, x_0)
    Gap_example = Gap_example.where(Gap_example > 0,  0)

    #2 calculate single year temp difference (T_open T_close T_modi_98=70)
    T_modi = np.where(
        Gap_example > 0,
        tpv_close_datafile - (tpv_close_datafile - tpv_open_datafile) * (1 - np.exp(-Gap_example / 6.1)),
        tpv_close_datafile
    )
    T_modi = xr.DataArray(T_modi, dims=tpv_close_datafile.dims, coords=tpv_close_datafile.coords)

    # 3 calculate single year LCOE increase due to degradation
    tas = xr.open_dataset(
        f'/gpfs/accounts/mtcraig_root/mtcraig0/haochiw/Vct_result_gcm/{model}/tas_hourly_ERA5_{year}_GCM_{target}_C.nc').tas
    huss = xr.open_dataset(
        f'/gpfs/accounts/mtcraig_root/mtcraig0/haochiw/Vct_result_gcm/{model}/huss_hourly_ERA5_{year}_GCM_{target}_C.nc').huss

    def calculate_relative_humidity(tas, huss):
        T_celsius = tas - 273.15

        e_s = 6.112 * np.exp(17.67 * T_celsius / (T_celsius + 243.5))

        e = huss * 1013.25

        RH = (e / e_s) * 100

        return RH

    RH_example = calculate_relative_humidity(tas, huss)
    RH_example["time"] = T_modi["time"]

    Ea = 0.89
    k = 8.617333262145e-5
    alpha = -2.2
    A = 6.4e-10
    T_test = 85
    RH_test = 85
    TTF = A * np.exp(Ea / (k * (T_modi + 273.15))) * (RH_example / 100) ** alpha
    TTF_mean = (1 / TTF).mean(dim='time')  
    TTF_test = A * np.exp(Ea / (k * (T_test + 273.15))) * (RH_test / 100) ** alpha
    TTF_test = 1 / TTF_test 

    TTF_0 = A * np.exp(Ea / (k * (tpv_close_datafile + 273.15))) * (RH_example / 100) ** alpha
    TTF_mean_0 = (1 / TTF_0).mean(dim='time')  
    AF_relative = TTF_mean_0 / TTF_mean

    def lcoe3(cost_module=2.55, cost_om=10, r_degradation=0.36,
              r_discount=6.3, service_life=30, energy_yield=1475., pow_threshold=30):
        cost_module = cost_module * 1e3  # $/kW

        # calculate service_life for each locations
        service_life = xr.DataArray(np.round(pow_threshold / r_degradation), dims=r_degradation.dims)

        def cost(year):
            return xr.where(year == 0, cost_module, cost_om)

        def energy(year, r_degradation):
            degradation_factor = (1. - r_degradation / 100. * (year - 1))
            return xr.where(year == 0, 0, energy_yield * degradation_factor)

        
        max_service_life = service_life.max().item()
        years = xr.DataArray(np.arange(max_service_life + 1), dims=['year'])
        discount_factor = (1. + r_discount / 100.) ** years

        total_cost = sum(cost(int(year)) / discount_factor.sel(year=int(year)) for year in years)
        total_energy = sum(energy(int(year), r_degradation) / discount_factor.sel(year=int(year)) for year in years)

        lcoe_values = total_cost / total_energy
        return lcoe_values

    import cartopy.crs as ccrs

    import regionmask
    import cartopy.feature as cfeature


    LCOE_hightemp = lcoe3(energy_yield=CFs_accumu, r_degradation=AF_relative * 0.66)
    LCOE_modi = lcoe3(energy_yield=CFs_accumu, r_degradation=AF_relative * 0.0001 + 0.66)

    change_lcoe = (LCOE_hightemp - LCOE_modi) / LCOE_modi * 100


    land = regionmask.defined_regions.natural_earth.land_110
    land_mask2 = (land.mask(Gap_example, lon_name='lon', lat_name='lat'))
    fig = plt.figure(figsize=(8, 4))
    ax = plt.axes(projection=ccrs.Robinson(central_longitude=10))
    (change_lcoe).where(land_mask2 == 0).plot(ax=ax,
                                              cmap='hot',
                                              transform=ccrs.PlateCarree(), vmax=30, vmin=0,
                                              x='lon', y='lat')
    plt.title(f'{year}')
    ax.add_feature(cfeature.LAND)
    ax.add_feature(cfeature.COASTLINE)
    ax.add_feature(cfeature.BORDERS)
    plt.show()
    return LCOE_hightemp,LCOE_modi

years = range(1950,1976) # after bias correction, the total 27 ERA5 sample years 
models = ['ACCESS-CM2', 'BCC-CSM2-MR', 'CanESM5', 'CNRM-CM6-1', 'CMCC-CM2-SR5', 'CMCC-ESM2', 'EC-Earth3_r1i1p1f1',
            'EC-Earth3_r3i1p1f1', 'EC-Earth3_r4i1p1f1',
            'GFDL-ESM4', 'HadGEM3-GC31-LL', 'KACE-1-0-G', 'MIROC6', 'MIROC-ES2L', 'MPI-ESM1-2-HR_r1i1p1f1',
            'MPI-ESM1-2-HR_r2i1p1f1',
            'MPI-ESM1-2-LR', 'HadGEM3-GC31-MM', 'KIOST-ESM','MRI-ESM2-0']
targets = [0,0.8,1,1.5,2,2.5,3,3.5,4]

for year in years:
    for model in models:
        for target in targets:
            for year in years:
                LCOE_hightemp,LCOE_modi=calculate_single_years_gap_and_compare_lcoe_changes(model,year,target)
