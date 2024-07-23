import numpy as np
import pandas as pd
from numpy import arccos, arcsin, arctan2, cos, deg2rad, pi, sin
import xarray as xr
import shutil
import os
import dask
from dask.diagnostics import ProgressBar
import glob
import matplotlib.pyplot as plt
import sys
import warnings
import datetime
from numpy import cos, deg2rad, fmax, fmin, sin, sqrt
## total 20 CMIP6 GCMs models, with 3 hours resolution
# #    models = ['ACCESS-CM2', 'BCC-CSM2-MR', 'CanESM5', 'CNRM-CM6-1', 'CMCC-CM2-SR5', 'CMCC-ESM2', 'EC-Earth3_r1i1p1f1',
#               'EC-Earth3_r3i1p1f1', 'EC-Earth3_r4i1p1f1',
#               'GFDL-ESM4', 'HadGEM3-GC31-LL', 'KACE-1-0-G', 'MIROC6', 'MIROC-ES2L', 'MPI-ESM1-2-HR_r1i1p1f1',
#               'MPI-ESM1-2-HR_r2i1p1f1',
#               'MPI-ESM1-2-LR', 'HadGEM3-GC31-MM', 'KIOST-ESM','MRI-ESM2-0']

## Total 150 years:
## from 1950-2100



def DiffuseHorizontalIrrad(ds, solar_position, clearsky_model, influx):
    # Clearsky model from Reindl 1990 to split downward radiation into direct
    # and diffuse contributions. Should switch to more up-to-date model, f.ex.
    # Ridley et al. (2010) http://dx.doi.org/10.1016/j.renene.2009.07.018 ,
    # Lauret et al. (2013):http://dx.doi.org/10.1016/j.renene.2012.01.049
    sinaltitude = sin(solar_position["altitude"])
    influx_toa = ds["influx_toa"]

    if clearsky_model is None:
        clearsky_model = (
            "enhanced" if "temperature" in ds and "humidity" in ds else "simple"
        )
    # Reindl 1990 clearsky model
    k = influx / influx_toa  # clearsky index
    # k.values[k.values > 1.0] = 1.0
    # k = k.rename('clearsky index')

    if clearsky_model == "simple":
        # Simple Reindl model without ambient air temperature and
        # relative humidity
        fraction = (
            ((k > 0.0) & (k <= 0.3))
            * fmin(1.0, 1.020 - 0.254 * k + 0.0123 * sinaltitude)
            + ((k > 0.3) & (k < 0.78))
            * fmin(0.97, fmax(0.1, 1.400 - 1.749 * k + 0.177 * sinaltitude))
            + (k >= 0.78) * fmax(0.1, 0.486 * k - 0.182 * sinaltitude)
        )
    elif clearsky_model == "enhanced":
        # Enhanced Reindl model with ambient air temperature and relative
        # humidity
        T = ds["temperature"]
        rh = ds["humidity"]

        fraction = (
            ((k > 0.0) & (k <= 0.3))
            * fmin(
                1.0,
                1.000 - 0.232 * k + 0.0239 * sinaltitude - 0.000682 * T + 0.0195 * rh,
            )
            + ((k > 0.3) & (k < 0.78))
            * fmin(
                0.97,
                fmax(
                    0.1,
                    1.329 - 1.716 * k + 0.267 * sinaltitude - 0.00357 * T + 0.106 * rh,
                ),
            )
            + (k >= 0.78)
            * fmax(0.1, 0.426 * k - 0.256 * sinaltitude + 0.00349 * T + 0.0734 * rh)
        )
    else:
        raise KeyError(
            "`clearsky model` must be chosen from 'simple' and " "'enhanced'"
        )
    return (influx * fraction).rename("diffuse horizontal")

def handle_calcu_single_year(gcm_path, model, year, mount_method, save_path):
    if int(year) >= 2015:
        scenario = 'ssp585'
    else:
        scenario = 'historical'

    print('opening .nc file')
    chunk_size = 100
    ds = xr.open_dataset(glob.glob(
        gcm_path + f'/{model}/{scenario}/rsds_3hr_*_{year}.nc')[0]).chunk({'time': chunk_size})
    dtemp = xr.open_dataset(glob.glob(
        gcm_path + f'/{model}/{scenario}/tas_3hr_*_{year}.nc')[0]).chunk({'time': chunk_size})
    uas = xr.open_dataset(glob.glob(
        gcm_path + f'/{model}/{scenario}/uas_3hr_*_{year}.nc')[0]).uas.chunk({'time': chunk_size})
    vas = xr.open_dataset(glob.glob(
        gcm_path + f'/{model}/{scenario}/vas_3hr_*_{year}.nc')[0]).vas.chunk({'time': chunk_size})


    ds = ds.rename({f"rsds": "influx"})
    vas = vas.pipe(interp_anomaly, uas).pipe(interp)
    wind = xrsqrt(uas, vas)
    solar_position = SolarPosition_modi(ds)
    DHI = DiffuseHorizontalIrrad(solar_position, solar_position, "simple", ds['influx'])
    surface_orientation = SurfaceOrientation(ds, solar_position)
    surface_orientation = SurfaceOrientation(ds, solar_position)
    POA = TiltedIrradiation(ds, DHI, solar_position, surface_orientation)
    POA = POA.transpose('time', 'lat', 'lon')

    t = ds.indexes["time"]
    if isinstance(t, xr.coding.cftimeindex.CFTimeIndex):
        dtemp['time'] = POA['time']
        wind['time'] = POA['time']
    pvtemp, CapFact = power_huld(POA,
                                 dtemp['tas'].pipe(interp_anomaly, POA).pipe(interp).interp_like(POA),
                                 wind.pipe(interp_anomaly, POA).pipe(interp).interp_like(POA),
                                 mount_method)
    

    directory = f'/scratch/negishi/wu2411/Scratch/Vct_result_gcm/{model}'
    if not os.path.exists(directory):
        os.makedirs(directory)
    try:
        # with ProgressBar():
            # pvtemp.astype('float32').to_netcdf(f'Era5_base_Vect/PVTemp_{mount_method}_era5_{year}.nc')
        start_time = datetime.datetime.now()
        pvtemp.astype('float32').to_netcdf(
            save_path + f'/PVTemp_{mount_method}_3hr_{model}_{scenario}_{year}.nc')
        end_time = datetime.datetime.now()
        print(f'saving PVTemp_3hr_{year}', flush=True)
        print(f'Time taken for saving PVTemp: {end_time - start_time}', flush=True)
    except Exception as e:
        print(f'Error saving PVTemp for {year}: {e}')

    try:
        # with ProgressBar():
            # CapFact.astype('float32').to_netcdf(f'Era5_base_Vect/CummuCFs_{mount_method}_era5_{year}.nc')
        start_time = datetime.datetime.now()
        CapFact.astype('float32').to_netcdf(
            save_path + f'/CummuCFs_{mount_method}_3hr_{model}_{scenario}_{year}.nc')
        end_time = datetime.datetime.now()
        print(f'saving CummuCFs_3hr_{year}', flush=True)
        print(f'Time taken for saving PVTemp: {end_time - start_time}', flush=True)
    except Exception as e:
        print(f'Error saving CummuCFs for {year}: {e}')

    del pvtemp, CapFact, ds, dtemp, uas, vas
    return 0


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


def interp_anomaly(data, sample):
    data = data.interp(lat=sample.lat.values, lon=sample.lon.values)
    return data


def xrsqrt(a, b):
    func = lambda x, y: np.sqrt(x ** 2 + y ** 2)
    return xr.apply_ufunc(func, a, b, dask="parallelized")


def SolarPosition_modi(ds, time_shift="0H"):
    rvs = {
        "solar_azimuth",
        "solar_altitude",
    }

    if rvs.issubset(set(ds.data_vars)):
        solar_position = ds[rvs]
        solar_position = solar_position.rename(
            {v: v.replace("solar_", "") for v in rvs}
        )
        return solar_position

    time_shift = pd.to_timedelta(time_shift)

    t = ds.indexes["time"] + time_shift
    if isinstance(t, xr.coding.cftimeindex.CFTimeIndex):
        import cftime
        # CFTimeIndex to DatetimeIndex
        standard_calendar_base = cftime.date2num(t[0], 'days since 2000-01-01', calendar='standard')
        julian_dates_360 = [cftime.date2num(date, 'days since 2000-01-01', calendar=t.calendar) for date in t]
        # Correct calendar
        if t.calendar == '360_day':
            julian_dates = [(julian_date - julian_dates_360[0])*365/360 + standard_calendar_base for julian_date in julian_dates_360]
        if t.calendar == 'noleap':
            julian_dates = [(julian_date - julian_dates_360[0]) + standard_calendar_base for julian_date in
                            julian_dates_360]
        n = xr.DataArray(julian_dates, coords=ds["time"].coords)  # 调整为自儒略日起的天数
        tofyear = ds.indexes["time"] + time_shift
        dayofyear = n - julian_dates[0]
    else:
        n = xr.DataArray(t.to_julian_date(), coords=ds["time"].coords) - 2451545.0
        tofyear = ds.indexes["time"] + time_shift
        dayofyear = xr.DataArray(tofyear.to_julian_date(), coords=ds["time"].coords) - tofyear[0].to_julian_date()

    hour = (ds["time"] + time_shift).dt.hour
    minute = (ds["time"] + time_shift).dt.minute

    # Operations make new DataArray eager; reconvert to lazy dask arrays
    chunks = ds.chunksizes.get("time", "auto")
    n = n.chunk(chunks)
    hour = hour.chunk(chunks)
    minute = minute.chunk(chunks)

    L = 280.460 + 0.9856474 * n  # mean longitude (deg)
    g = deg2rad(357.528 + 0.9856003 * n)  # mean anomaly (rad)
    l = deg2rad(L + 1.915 * sin(g) + 0.020 * sin(2 * g))  # ecliptic long. (rad)
    ep = deg2rad(23.439 - 4e-7 * n)  # obliquity of the ecliptic (rad)

    ra = arctan2(cos(ep) * sin(l), cos(l))  # right ascencion (rad)
    lmst = (6.697375 + (hour + minute / 60.0) + 0.0657098242 * n) * 15.0 + ds[
        "lon"
    ]  # local mean sidereal time (deg)
    h = (deg2rad(lmst) - ra + pi) % (2 * pi) - pi  # hour angle (rad)

    dec = arcsin(sin(ep) * sin(l))  # declination (rad)

    # alt and az from [2]
    lat = deg2rad(ds["lat"])
    # Clip before arcsin to prevent values < -1. from rounding errors; can
    # cause NaNs later
    alt = arcsin(
        (sin(dec) * sin(lat) + cos(dec) * cos(lat) * cos(h)).clip(min=-1.0, max=1.0)
    ).rename("altitude")
    alt.attrs["time shift"] = f"{time_shift}"
    alt.attrs["units"] = "rad"

    az = arccos(
        ((sin(dec) * cos(lat) - cos(dec) * sin(lat) * cos(h)) / cos(alt)).clip(
            min=-1.0, max=1.0
        )
    )
    az = az.where(h <= 0, 2 * pi - az).rename("azimuth")
    az.attrs["time shift"] = f"{time_shift}"
    az.attrs["units"] = "rad"

    B = (2. * np.pi / 365.) * (dayofyear - 1)
    RoverR0sqrd = (1.00011 + 0.034221 * np.cos(B) + 0.00128 * np.sin(B) +
                   0.000719 * np.cos(2 * B) + 7.7e-05 * np.sin(2 * B))
    solar_constant = 1366.1
    Ea = solar_constant * RoverR0sqrd
    GHI0 = (sin(dec) * sin(lat) + cos(dec) * cos(lat) * cos(h)).clip(min=-1.0, max=1.0) * Ea
    GHI0 = GHI0.where(GHI0 >= 0, 0.0).rename("influx_toa")
    vars = {da.name: da for da in [alt, az, GHI0]}
    solar_position = xr.Dataset(vars)
    return solar_position


def SurfaceOrientation(ds, solar_position, tracking=None):
    lon = deg2rad(ds["lon"])
    lat = deg2rad(ds["lat"])

    def orientation(lon, lat, solar_position):
        slope = np.empty_like(lat.values)
        slope = np.deg2rad(20.0)
        azimuth = np.where(lat.values < 0, 0, pi)

        return dict(
            slope=xr.DataArray(slope, coords=lat.coords),
            azimuth=xr.DataArray(azimuth, coords=lat.coords),
        )

    orientation = orientation(lon, lat, solar_position)
    surface_slope = orientation["slope"]
    surface_azimuth = orientation["azimuth"]

    sun_altitude = solar_position["altitude"]
    sun_azimuth = solar_position["azimuth"]

    if tracking == None:
        cosincidence = sin(surface_slope) * cos(sun_altitude) * cos(
            surface_azimuth - sun_azimuth
        ) + cos(surface_slope) * sin(sun_altitude)

    cosincidence = cosincidence.clip(min=0)

    return xr.Dataset(
        {
            "cosincidence": cosincidence,
            "slope": surface_slope,
            "azimuth": surface_azimuth,
        },
        coords={"time": ds["time"], "lat": ds["lat"], "lon": ds["lon"]}
    )


def TiltedIrradiation(
        ds,
        diffuse,
        solar_position,
        surface_orientation,
        altitude_threshold=1.0,
        irradiation="total",
):
    influx = ds['influx']

    direct = influx - diffuse

    k = surface_orientation["cosincidence"] / sin(solar_position["altitude"])

    cos_surface_slope = cos(surface_orientation["slope"])

    direct_t = k * direct
    diffuse_t = (1.0 + cos_surface_slope) / 2.0 * diffuse
    ground_t = 0.25 * influx * ((1.0 - cos_surface_slope) / 2.0)

    total_t = direct_t.fillna(0.0) + diffuse_t.fillna(0.0) + ground_t.fillna(0.0)

    if irradiation == "total":
        result = total_t.rename("total tilted")

    cap_alt = solar_position["altitude"] < deg2rad(altitude_threshold)
    result = result.where(~(cap_alt | (direct + diffuse <= 0.01)), 0)
    result = result.where(total_t > 0, 0)
    result.attrs["units"] = "W m**-2"

    return result


def power_huld(poa_global, temp_air, wind_speed, mount_method):
    TEMPERATURE_MODEL_PARAMETERS = {
        'sapm': {
            'open_rack_glass_glass': {'a': -3.47, 'b': -.0594, 'deltaT': 3},
            'close_mount_glass_glass': {'a': -2.98, 'b': -.0471, 'deltaT': 1},
            'open_rack_glass_polymer': {'a': -3.56, 'b': -.0750, 'deltaT': 3},
            'insulated_back_glass_polymer': {'a': -2.81, 'b': -.0455, 'deltaT': 0},
        },
        'pvsyst': {'freestanding': {'u_c': 29.0, 'u_v': 0},
                   'insulated': {'u_c': 15.0, 'u_v': 0}}
    }

    pc = {}
    pc["k_1"] = -0.017162
    pc["k_2"] = -0.040289
    pc["k_3"] = -0.004681
    pc["k_4"] = 0.000148
    pc["k_5"] = 0.000169
    pc["k_6"] = 0.000005
    pc["r_irradiance"] = 1000
    pc['inverter_efficiency'] = 0.9

    param = TEMPERATURE_MODEL_PARAMETERS['sapm'][mount_method]
    # normalized module temperature
    print('power_huld_calculating pvtemp')
    T_ = temp_air + poa_global * np.exp(param['a'] + param['b'] * wind_speed)
    dT = T_ - 273.15 - 25

    # # normalized irradiance

    print('power_huld_calculating CFs')
    G_ = poa_global / pc["r_irradiance"]

    log_G_ = np.log(G_.where(G_ > 0))
    # # NB: np.log without base implies base e or ln
    eff = (
            1
            + pc["k_1"] * log_G_
            + pc["k_2"] * (log_G_) ** 2
            + dT * (pc["k_3"] + pc["k_4"] * log_G_ + pc["k_5"] * log_G_ ** 2)
            + pc["k_6"] * (dT ** 2)
    )

    eff = eff.fillna(0.0).clip(min=0)

    da = G_ * eff * pc.get("inverter_efficiency", 1.0)

    T_ = T_ - 273.15
    T_.attrs["units"] = "degC"
    T_ = T_.rename("pvtemp_deg°C")

    return T_, da

import os


def clear_folder(directory):
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)
        if os.path.isfile(file_path):
            print(f"Deleting {filename}")
            os.remove(file_path)


def handle_calcu_single_year_wrapper(args):
    return handle_calcu_single_year(*args)


from multiprocessing import Pool
from tqdm import tqdm


def main(model_index, mounting_config, years_list):
    # # Call the function
    models = ['ACCESS-CM2', 'BCC-CSM2-MR', 'CanESM5', 'CNRM-CM6-1', 'CMCC-CM2-SR5', 'CMCC-ESM2', 'EC-Earth3_r1i1p1f1',
              'EC-Earth3_r3i1p1f1', 'EC-Earth3_r4i1p1f1',
              'GFDL-ESM4', 'HadGEM3-GC31-LL', 'KACE-1-0-G', 'MIROC6', 'MIROC-ES2L', 'MPI-ESM1-2-HR_r1i1p1f1',
              'MPI-ESM1-2-HR_r2i1p1f1',
              'MPI-ESM1-2-LR', 'HadGEM3-GC31-MM', 'KIOST-ESM','MRI-ESM2-0']
    model = models[model_index]

    years = years_list

    file_folder_name = 'CMIP6_OpenRack'

    # # Specify the directory to clear
    directory = (f'/tmp_data/{file_folder_name}/ACCESS-CM2') 

    gcm_rootpath = '/scratch/negishi/wu2411/GCM/'
    save_path = f'/scratch/negishi/wu2411/Scratch/Vct_result_gcm/{model}'
    for year in years:
        handle_calcu_single_year(gcm_rootpath, model, year, mounting_config, save_path)
        print(f'handle_calcu_single_year_{year}')

if __name__ == "__main__":
    #
    if len(sys.argv) != 4:
        print("not correct input num")
        sys.exit(1)
    model_index = int(sys.argv[1])  # 5
    mounting_config = sys.argv[2]  # "close_mount_glass_glass"
    years_list = sys.argv[3]  # 2010
    years_str_list = years_list.split(',')

    main(model_index, mounting_config, years_str_list)
