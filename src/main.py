import json
import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn
import param
from bokeh.models import HoverTool
from holoviews.util.transform import lon_lat_to_easting_northing, easting_northing_to_lon_lat
from redis_dict import RedisDict
from tqdm import tqdm
from loguru import logger as log
from constants import CSS_CLASS_CARD
from utils import get_price_range
from bokeh.models.widgets.tables import HTMLTemplateFormatter, NumberFormatter

r_dic = RedisDict(namespace='house-search', host="10.30.40.132")
bootstrap = pn.template.BootstrapTemplate(title='Smart House Search')
pn.config.sizing_mode = "stretch_width"
pn.extension(raw_css=[CSS_CLASS_CARD])

TOOLTIPS = """
    <div width="520" style="width:350px;">
        <div>
            <img
                src="@photo" height="128" alt="@photo" width="128"
                style="float: left; margin: 0px 15px 15px 0px;"
                border="2"
            ></img>
        </div>
        <div>
            <span style="font-size: 12px; font-weight: bold;">@address</span>
        </div>
        <div>
            <span style="font-size: 12px; font-weight: bold;">Size: @size Price: $@price</span>
        </div>
    </div>
"""
HONESTDOOR_COLS = ['DateSold','PriceLastSold','property_id','Assessment Price']

def pull_redis(redis_client):
    dataframes = []
    listings_honestdoor_addresses = redis_client.smembers('%s:listings_honestdoor' % namespace)
    # Pull realtorca records
    for a in tqdm(list(listings_honestdoor_addresses)[:10]):
        # log.info(a)
        key_to_data = f'{namespace}:listings/{a}'
        key_to_hd_data = f'{namespace}:listings_honestdoor/{a}'
        log.info(key_to_data)
        listing_data = pd.DataFrame(json.loads(r_dic.redis.get(key_to_data)), index=[a])
        honestdoor_data = pd.read_json(r_dic.redis.get(key_to_hd_data))
        honestdoor_data.columns = HONESTDOOR_COLS
        honestdoor_data.index = [a] * honestdoor_data.shape[0]
        merged_data = pd.concat([listing_data,honestdoor_data.head(1)],axis=1)
        dataframes.append(merged_data)
    df = pd.concat(dataframes)
    df['photo'] = df['photo_url']
    df['DateSold'] = df['DateSold'].dt.date
    df['address'] = df['address'].apply(lambda x: x.split('|')[0])

    df.drop(columns='photo_url', inplace=True)


    return df



namespace = 'house-search'
house_df_default = pull_redis(r_dic.redis)


class ReactiveDashboard(param.Parameterized):
    title = pn.pane.Markdown("# Smart House Search")
    pins = param.List(default=[])
    lat_longs = param.List(default=[])
    # house_df = get_dummy_house_df()
    house_df = house_df_default
    hover = HoverTool(tooltips=TOOLTIPS)

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

    @pn.depends('price_slider', 'rooms_slider', 'bathrooms_slider', 'pins', watch=False)
    def house_plot(self):

        if 'northing' not in self.house_df.columns:
            self.get_easting_northing()

        df_filtered = self.house_df[
            (self.house_df['price'] <= self.price_slider[1]) & (self.house_df['price'] >= self.price_slider[0])]
        df_filtered = df_filtered[
            (df_filtered['bedrooms'] <= self.rooms_slider[1]) & (df_filtered['bedrooms'] >= self.rooms_slider[0])]
        df_filtered = df_filtered[
            (df_filtered['bathrooms'] <= self.rooms_slider[1]) & (df_filtered['bathrooms'] >= self.rooms_slider[0])]

        house_points = hv.Points(df_filtered, ['easting', 'northing'],
                                 ['price', 'photo', 'address', 'size']).opts(tools=[self.hover, 'tap'],
                                                                                                                   alpha=0.99,
                                                                                                                   hover_fill_alpha=0.99, size=15,
                                                                                                                   hover_fill_color='firebrick',
                                                                                                                   width=600,
                                                                                                                   height=550)
        if self.pins:
            pin_points = hv.Points(self.pins).opts(color='r', size=10, alpha=0.7)
            return self.map_background * house_points * pin_points
        else:
            return self.map_background * house_points

    def filter_df(self):
        if 'northing' not in self.house_df.columns:
            self.get_easting_northing()

        display_df = self.house_df[
            (self.house_df['price'] <= self.maximum_price) & (self.house_df['price'] >= self.minimum_price)]
        cols = ['lat', 'long', 'easting', 'northing', 'detail_url', 'key', 'mls_number', 'id', 'photo_url']
        for col in cols:
            if col in display_df.columns:
                display_df = display_df.drop(columns=col, axis=1)
        display_df['size'] = display_df['size'].apply(lambda x: x.split()[0] if x else -999).astype(float)
        display_df = display_df.set_index('address')

        return display_df[['photo', 'price','DateSold','PriceLastSold','Assessment Price',
                           'bedrooms', 'bathrooms', 'size', 'lot_size', 'type', 'stories']]

    @pn.depends("stream", watch=False)
    def distance_df(self, x, y):
        lat = easting_northing_to_lon_lat(x, y)[1]
        long = easting_northing_to_lon_lat(x, y)[0]
        self.lat_longs.append(['enter name', lat, long])
        df = pd.DataFrame(self.lat_longs, columns=['Name', 'Latitude', 'Longitude']).dropna().style.hide_index()
        return pn.widgets.Tabulator(df.data, pagination='remote', page_size=10, sizing_mode='scale_both', show_index=False)

    @pn.depends("stream", "pins")
    def location(self, x, y):
        if x and y:
            self.pins.append([x, y])
        return self.house_plot

    def panel(self):
        result = bootstrap

        result.sidebar.append(self.param.price_slider)
        result.sidebar.append(self.param.rooms_slider)

        image_format = r'<div> <img src="<%= value %>" height="70" alt="<%= value %>" width="70" style="float: left; margin: 0px 15px 15px 0px;" border="2" ></img> </div>'
        tabulator_formatters = {
            'price': NumberFormatter(format='$0,0'),
            'size': NumberFormatter(format='0,0 sqft'),
            'photo': HTMLTemplateFormatter(template=image_format)
        }

        df_widget = pn.widgets.Tabulator(self.filter_df(), pagination='remote', page_size=10,
                                         formatters=tabulator_formatters, sizing_mode='scale_both')
        df_widget.add_filter(self.param.price_slider, 'price')
        df_widget.add_filter(self.param.rooms_slider, 'bedrooms')

        # df_pins = pn.widgets.Tabulator(self.distance_df(), pagination='remote', page_size=10, sizing_mode='scale_both')

        layout = pn.Row(
            pn.Card(pn.bind(self.location, x=self.stream.param.x, y=self.stream.param.y), title="Map",
                    sizing_mode='stretch_height'),
            pn.Column(pn.Card(df_widget, title="Properties", sizing_mode='scale_both'))
        )

        result.sidebar.append(pn.Card(pn.bind(self.distance_df, x=self.stream.param.x, y=self.stream.param.y),
                              title="Pins", sizing_mode='scale_both'))

        bootstrap.main.append(layout)

        return result


res = ReactiveDashboard(name="").panel()
res.servable()
