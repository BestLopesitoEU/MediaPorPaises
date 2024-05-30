import geopandas
import rioxarray 
import xarray as xr
from shapely.geometry import mapping
import csv
import numpy as np
import os
import pandas as pd
import requests


def get_csv_from_country_ncdf() :
    with open("/home/pablo807/workspace/NetcdfClimatico/PercentilesPorPaises/Data_media_CostaDeMarfil.csv", "w", newline="") as f:
        writer = csv.writer(f, delimiter=",")
        writer.writerow(["Climate_ID", "Indicator_ID", "Ncdf_var", "Mean"])
        filepath_mask = "/home/pablo807/workspace/NetcdfClimatico/PercentilesPorPaises/Data/MasksCountries/WB_GAD_ADM0.shp"
        ###indicators_ids = [125, 155, 120, 59, 121, 34, 122, 60, 58, 56, 57, 123, 117, 25, 124, 119, 118, 28, 102, 104, 103, 135, 50, 26, 100, 101, 148, 147, 145,
        ###                   146, 150, 151, 142, 143, 144, 141, 140, 139, 138, 149, 129, 22, 130, 23, 35, 88, 89, 90, 27, 91 , 87, 16, 132, 133, 131, 113, 114, 115,
        ###                     116, 112, 86, 83, 84, 85, 39, 48, 49, 4, 38, 78, 75, 76, 77, 82, 79, 80, 81, 72, 63, 64, 65, 73, 68, 66, 67, 74, 71, 69, 70, 6, 62, 61,
        ###                       31, 2, 126, 128, 127, 32, 134, 14, 15, 51, 52, 11, 12, 13, 30, 99, 136, 137, 53, 98, 95, 96, 97, 3, 154, 153, 29, 152, 10, 92, 94, 93,
        ###                         33, 110, 111, 109, 108, 105, 106, 107]
        indicators_ids = pd.read_excel("/home/pablo807/workspace/NetcdfClimatico/PercentilesPorPaises/Data/Indicadores_forestry.xlsx", sheet_name="Hoja 2", usecols="B", header=0)
        indicators_ids = indicators_ids["indicator_id"].tolist()
        climate_ids = [9, 10, 11, 19, 20, 21]
        path_especial = ['climatology_totalwaterlevelreturnlevel100_period', 'climatology_totalwaterlevelreturnlevel10_period',
                        'climatology_totalwaterlevelreturnlevel20_period', 'climatology_totalwaterlevelreturnlevel50_period']
        cs  = geopandas.read_file(filepath_mask, crs="epsg:4326")
        country_shape = cs[cs["NAM_0"] == "Côte d'Ivoire"]
        j = 0
        for climate_id in climate_ids :
            for indicator_id in indicators_ids:
                    indicator = requests.get("https://climatehubdev.ihcantabria.com/v1/public/open-dap/metadata?indicator-id={0}&climate-case-id={1}".format(indicator_id, climate_id)).json()
                    if len(indicator[0]["climateCases"]) != 0 :
                        if indicator_id == 153 and climate_id == 9 :
                            writer.writerow([climate_id, indicator_id, "No hay datos"])
                        else :
                            path_ncdf = os.path.basename(os.path.normpath(indicator[0]["climateCases"][0]["url"]))
                            path_ncdf = path_ncdf.replace(".nc", "")
                            print("Climate_id = ", climate_id,", Indicator_id = ", indicator_id)
                            ds = xr.open_dataset(indicator[0]["climateCases"][0]["url"], engine="netcdf4", decode_cf=False, drop_variables=["latitude_bounds", 
                                                                                                                                            "longitude_bounds", 
                                                                                                                                            "climatology_bounds", 
                                                                                                                                            "DATA_spatially_aggregated"])
                            try :
                                ds = ds.rename({"lat" : "latitude", "lon" : "longitude"})
                            except:
                                pass
                            mask_lon = (ds.longitude >= float(country_shape["MIN_X"].iloc[0])) & (ds.longitude <= float(country_shape["MAX_X"].iloc[0]))
                            mask_lat = (ds.latitude > float(country_shape["MIN_Y"].iloc[0])) & (ds.latitude < float(country_shape["MAX_Y"].iloc[0]))
                            ds = ds.where(mask_lon & mask_lat, drop=True)
                            if indicator_id in [98, 95, 96, 97] and climate_id == 11 :
                                path_ncdf = path_especial[j]
                                j += 1
                            else :
                                pass
                            ds.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude", inplace=True)
                            ds.rio.write_crs("epsg:4326", inplace=True)             
                            ds_final = ds.rio.clip(country_shape.geometry.apply(mapping), country_shape.crs, drop=False)
                            ds_final[path_ncdf] = xr.where((ds_final[path_ncdf] < -998), np.nan, ds_final[path_ncdf])
                            ds_final[path_ncdf] = xr.where((ds_final[path_ncdf] > 10**10), np.nan, ds_final[path_ncdf])
                            value_mean = np.nanmean(ds_final.variables[path_ncdf])
                            writer.writerow([climate_id, indicator_id, path_ncdf, value_mean])
                    else :
                        print("Climate_id = ", climate_id,", Indicator_id = ", indicator_id,", Se ha pasado")
                        writer.writerow([climate_id, indicator_id, "No hay datos"])


get_csv_from_country_ncdf()

