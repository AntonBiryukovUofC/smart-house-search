import numpy as np
import pandas as pd
import bokeh.tile_providers
# Bokeh-related contants:
from bokeh.models import WMTSTileSource

tile_options = {
    'url': 'https://tiles.basemaps.cartocdn.com/light_all/{z}/{x}/{y}@2x.png',
    'attribution': """
'Tiles &copy; Esri &mdash; Esri, DeLorme, NAVTEQ, TomTom,
Intermap, iPC, USGS, FAO, NPS, NRCAN, GeoBase, Kadaster NL,
Ordnance Survey, Esri Japan, METI, Esri China (Hong Kong), and the GIS User Community'
    """}
OSM_tile_source = WMTSTileSource(**tile_options)

def get_latitude_longitude_listing(listing_key, redis):
    if listing_key is None:
        return False
    else:
        lat = float(redis.get(listing_key + "/latitude"))
        long = float(redis.get(listing_key + "/longitude"))
        return lat, long


def get_price_range():
    price_range_1 = np.arange(0, 500000, 25000)
    price_range_2 = np.arange(500000, 1000000, 50000)
    price_range_3 = np.arange(1000000, 2000000, 100000)
    price_range_4 = np.arange(2000000, 8000000, 500000)
    price_range_5 = np.array([100000, 15000000, 20000000])
    price_range = np.concatenate((price_range_1, price_range_2, price_range_3, price_range_4, price_range_5))
    return price_range


def get_dummy_house_df():
    data = [[51.0478, -114.0593, 100000, 2, 1, '500 sqft', '123 King St.'],
            [51.1095, -114.1089, 400000, 3, 2, '1100 sqft', '456 Queen Ave.'],
            [51.06, -114.08, 1000000, 4, 4, '3000 sqft', '789 Jack Blvd.']]
    df = pd.DataFrame(data, columns=['lat', 'lon', 'price', 'bedrooms', 'bathrooms', 'size', 'address'])
    return df
