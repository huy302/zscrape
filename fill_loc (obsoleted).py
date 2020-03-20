import numpy as np
import pandas as pd
# from geopy.geocoders import Nominatim
# from geopy.extra.rate_limiter import RateLimiter
import requests
import time
import tqdm

# Enter your API key
gkey = "AIzaSyA1EJSEF2XLTEKlAMdcu_7qGONEyc76tn8"

def get_lat_lon(address):
    target_url = ('https://maps.googleapis.com/maps/api/geocode/json?address={0}&key={1}').format(address, gkey)
    geo_data = requests.get(target_url).json()
    return geo_data["results"][0]["geometry"]["location"]["lat"], geo_data["results"][0]["geometry"]["location"]["lng"]

# read data
df = pd.read_csv('data.csv', index_col=['zpid'])
df_missing_loc = df[pd.isnull(df['lat'])]

# # get lat lon
# geolocator = Nominatim(user_agent="zscrape")
# geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
# df_missing_loc['location'] = df_missing_loc['address full'].apply(geocode)
# df_missing_loc['point'] = df_missing_loc['location'].apply(lambda loc: tuple(loc.point) if loc else (None, None, None))
# df_missing_loc[['lat', 'lon', 'altitude']] = pd.DataFrame(df_missing_loc['point'].tolist(), index=df_missing_loc.index)
# # df_missing_loc[['lat', 'lon', 'altitude']] = pd.DataFrame(df_missing_loc['point'].tolist())

# # merge back to df and update csv file
# for idx, row in df_missing_loc.iterrows():
#     df.loc[idx, 'lat'] = row['lat']
#     df.loc[idx, 'lon'] = row['lon']

for idx, row in tqdm.tqdm(list(df.iterrows())):
    if pd.isnull(row['lat']) or pd.isnull(row['lon']):
        for trial in range(10): # retry 10 times
            try:
                lat, lon = get_lat_lon(row['address full'].replace("#", "APT"))
                df.loc[idx, 'lat'] = lat
                df.loc[idx, 'lon'] = lon
                time.sleep(0.05 * (trial + 1))
                break
            except:
                # ignore
                pass

df.to_csv('data.csv')