import panel as pn
import numpy as np
import holoviews as hv
import hvplot.pandas
from panel.template import DarkTheme
import pandas as pd
import geoviews as gv
from holoviews.util.transform import easting_northing_to_lon_lat, lon_lat_to_easting_northing
from bokeh.models.formatters import NumeralTickFormatter
from bokeh.models import HoverTool
from vega_datasets import data as vds

tooltips = [("Price", "@price")]
hover = HoverTool(tooltips=tooltips)

gv.extension('bokeh')

data = [[51.0478, -114.0593, 100000], [51.1095, -114.1089, 400000], [51.06, -114.08, 1000000]]
df = pd.DataFrame(data, columns=['lat', 'lon', 'price'])
df['easting'] = df.apply(lambda x: lon_lat_to_easting_northing(x['lon'], x['lat'])[0], axis=1)
df['northing'] = df.apply(lambda x: lon_lat_to_easting_northing(x['lon'], x['lat'])[1], axis=1)

# bootstrap = pn.template.MaterialTemplate(title='Smart House Search', theme=DarkTheme)

pn.extension()

bootstrap = pn.template.BootstrapTemplate(title='Smart House Search')

pn.config.sizing_mode = 'stretch_width'

price_range_1 = np.arange(0, 500000, 25000)
price_range_2 = np.arange(500000, 1000000, 50000)
price_range_3 = np.arange(1000000, 2000000, 100000)
price_range_4 = np.arange(2000000, 8000000, 500000)
price_range_5 = np.array([100000, 15000000, 20000000])
price_range = np.concatenate((price_range_1, price_range_2, price_range_3, price_range_4, price_range_5))
min_price = pn.widgets.Select(name='Minimum Price', options=list(price_range))
max_price = pn.widgets.Select(name='Maximum Price', options=list(price_range), value=price_range[-1])

map = hv.element.tiles.OSM().opts(width=600, height=550)
# house_points = hv.Points(df, ['easting', 'northing'], ['price']).opts(tools=[hover, 'tap'], alpha=0.7,
#                                                                       hover_fill_alpha=0.4, size=10)

pins = []

def house_plot(price_min, price_max, x_pin=None, y_pin=None):
    df_filtered = df[(df['price'] <= price_max) & (df['price'] >= price_min)]
    house_points = hv.Points(df_filtered, ['easting', 'northing'], ['price']).opts(tools=[hover, 'tap'], alpha=0.7,
                                                                           hover_fill_alpha=0.4, size=10, width=600,
                                                                           height=550)
    if x_pin and y_pin:
        pins.append([x_pin, y_pin])
        pin_points = hv.Points(pins).opts(color='r', size=10, alpha=0.7)
        return (house_points * pin_points)
    else:
        return house_points
    # return df_filtered.hvplot('easting', 'northing', kind='scatter')


houses = house_plot(min_price.value, max_price.value)

stream_map = hv.streams.Tap(source=map, x=np.nan, y=np.nan)


layout_map = pn.Row(map * houses)

# make a function that displays the location when called.
def location(x, y):
    # lon, lat = easting_northing_to_lon_lat(x, y)
    # return pn.pane.Str('Click at %0.3f, %0.3f' % (lon, lat), width=200)
    layout_map[0] = pn.Row(map * house_plot(min_price.value, max_price.value, x, y))


layout_map = pn.Row(map * houses, pn.bind(location, x=stream_map.param.x, y=stream_map.param.y))


def update_houses(event):
    layout_map[0] = pn.Row(map * house_plot(min_price.value, max_price.value))


min_price.param.watch(update_houses, "value")
max_price.param.watch(update_houses, "value")

bootstrap.sidebar.append(min_price)
bootstrap.sidebar.append(max_price)

bootstrap.main.append(
    pn.Row(
        layout_map
    )
)

pn.serve({"panel_main": bootstrap}, port=5006)
