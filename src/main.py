import json

import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn
import param
from bokeh.models import HoverTool
from holoviews.util.transform import lon_lat_to_easting_northing
from redis_dict import RedisDict
from tqdm import tqdm
from loguru import logger as log
from constants import CSS_CLASS_CARD
#from location import geocode_destination_here
from utils import get_price_range, get_dummy_house_df

r_dic = RedisDict(namespace='house-search', host="10.30.40.132")
bootstrap = pn.template.BootstrapTemplate(title='Smart House Search')
pn.config.sizing_mode = "stretch_width"
pn.extension(raw_css=[CSS_CLASS_CARD])


def pull_redis(redis_client):
    dataframes = []
    listings_honestdoor_addresses = redis_client.smembers('%s:listings_honestdoor' % namespace)
    for a in tqdm(list(listings_honestdoor_addresses)[:100]):
        # log.info(a)
        key_to_data = f'{namespace}:listings/{a}'
        log.info(key_to_data)
        listing_data = pd.DataFrame(json.loads(r_dic.redis.get(key_to_data)), index=[a])
        dataframes.append(listing_data)
    df = pd.concat(dataframes)
    return df

namespace = 'house-search'
house_df_default = pull_redis(r_dic.redis)

class ReactiveDashboard(param.Parameterized):
    title = pn.pane.Markdown("# Smart House Search")
    pins = param.List(default=[])
    #house_df = get_dummy_house_df()
    house_df = house_df_default
    tooltips = [("Price", "@price")]
    hover = HoverTool(tooltips=tooltips)
    price_range = get_price_range()
    minimum_price = param.Selector(objects=list(price_range))
    maximum_price = param.Selector(objects=list(price_range), default=price_range[-1])
    price_slider = param.Range(default=(0, house_df['price'].max()), bounds=(0, house_df['price'].max()))
    rooms_slider = param.Range(default=(0, 7), bounds=(0, 7))
    bathrooms_slider = param.Range(default=(0, 7), bounds=(0, 7))

    map_background = hv.element.tiles.OSM().opts(width=600, height=550)
    stream = hv.streams.Tap(source=map_background, x=np.nan, y=np.nan)


    def get_easting_northing(self):
        self.house_df['easting'] = self.house_df.apply(lambda x: lon_lat_to_easting_northing(x['long'], x['lat'])[0],
                                                       axis=1)
        self.house_df['northing'] = self.house_df.apply(lambda x: lon_lat_to_easting_northing(x['long'], x['lat'])[1],
                                                        axis=1)

    @pn.depends('price_slider', 'rooms_slider','bathrooms_slider', 'pins', watch=False)
    def house_plot(self):

        if 'northing' not in self.house_df.columns:
            self.get_easting_northing()

        df_filtered = self.house_df[
            (self.house_df['price'] <= self.price_slider[1]) & (self.house_df['price'] >= self.price_slider[0])]
        df_filtered = df_filtered[
            (df_filtered['bedrooms'] <= self.rooms_slider[1]) & (df_filtered['bedrooms'] >= self.rooms_slider[0])]
        df_filtered = df_filtered[
            (df_filtered['bathrooms'] <= self.rooms_slider[1]) & (df_filtered['bathrooms'] >= self.rooms_slider[0])]

        house_points = hv.Points(df_filtered, ['easting', 'northing'], ['price']).opts(tools=[self.hover, 'tap'],
                                                                                       alpha=0.99,
                                                                                       hover_fill_alpha=0.99, size=20,
                                                                                       hover_fill_color='firebrick',
                                                                                       width=600,
                                                                                       height=550)
        if self.pins:
            pin_points = hv.Points(self.pins).opts(color='r', size=20, alpha=0.7)
            return self.map_background * house_points * pin_points
        else:
            return self.map_background * house_points

    def filter_df(self):
        if 'northing' not in self.house_df.columns:
            self.get_easting_northing()

        display_df = self.house_df[
            (self.house_df['price'] <= self.maximum_price) & (self.house_df['price'] >= self.minimum_price)].drop(
            columns=['lat', 'long', 'easting', 'northing'])
        display_df = display_df.set_index('address')
        return display_df

    @pn.depends("stream")
    def location(self, x, y):
        if x and y:
            self.pins.append([x, y])
        return self.house_plot

    def pin_dataframe(self):
        pins = np.array(self.pins)

    def pull_redis(self):
        dataframes = []
        listings_honestdoor_addresses = r_dic.redis.smembers('%s:listings_honestdoor' % namespace)
        for a in tqdm(listings_honestdoor_addresses):
            #log.info(a)
            key_to_data = f'{namespace}:listings/{a}'
            log.info(key_to_data)
            listing_data = pd.DataFrame(json.loads(r_dic.redis.get(key_to_data)),index=[a])
            dataframes.append(listing_data)
        df = pd.concat(dataframes)
        self.house_df = df

    def panel(self):
        result = bootstrap

        # self.pull_redis()

        result.sidebar.append(self.param.price_slider)
        result.sidebar.append(self.param.rooms_slider)

        df_widget = pn.widgets.Tabulator(self.filter_df())
        df_widget.add_filter(self.param.price_slider, 'price')
        df_widget.add_filter(self.param.rooms_slider, 'bedrooms')

        # houses = self.house_plot

        # tap_dmap = hv.DynamicMap(self.location, streams=[self.stream])

        # stream_map = hv.streams.Tap(source=houses, x=np.nan, y=np.nan)
        # pn.bind(self.location, x=stream_map.param.x, y=stream_map.param.y)
        # layout = pn.Row(houses, df_widget, pn.bind(self.location, x=stream_map.param.x, y=stream_map.param.y))

        layout = pn.Row(
            pn.Card(pn.bind(self.location, x=self.stream.param.x, y=self.stream.param.y), title="Map"),
            pn.Column(pn.Card(df_widget, title="Properties"), pn.Card(df_widget, title="Properties"))
        )
        bootstrap.main.append(layout)

        return result


res = ReactiveDashboard(name="").panel()
res.servable()