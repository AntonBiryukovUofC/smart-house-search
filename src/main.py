import json
import holoviews as hv
import numpy as np
import pandas as pd
import panel as pn
import param
from bokeh.models import HoverTool, ResetTool, PanTool, WheelZoomTool, ColumnDataSource, TapTool, Circle
from bokeh.plotting import figure
from holoviews.util.transform import lon_lat_to_easting_northing, easting_northing_to_lon_lat
from redis_dict import RedisDict
from tqdm import tqdm
from loguru import logger as log
from constants import CSS_CLASS_CARD
from utils import get_price_range, OSM_tile_source
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
HONESTDOOR_COLS = ['DateSold', 'PriceLastSold', 'property_id', 'Assessment Price']


def pull_redis(redis_client):
    dataframes = []
    listings_honestdoor_addresses = redis_client.smembers('%s:listings_honestdoor' % namespace)
    # Pull realtorca records
    for a in tqdm(list(listings_honestdoor_addresses)[:5]):
        # log.info(a)
        key_to_data = f'{namespace}:listings/{a}'
        key_to_hd_data = f'{namespace}:listings_honestdoor/{a}'
        log.info(key_to_data)


        listing_data = pd.DataFrame(json.loads(r_dic.redis.get(key_to_data)), index=[a])
        honestdoor_data = pd.read_json(r_dic.redis.get(key_to_hd_data))
        # Calculated score data
        score_data = {'Downtown Commute': round(float(redis_client.get(key_to_data + "/downtown_commute_score")), 2)}
        custom_score = redis_client.get(key_to_data + "/custom_commute_score")

        # if this isn't set could consider calculating
        if custom_score:
            score_data['Custom Commute'] = round(float(redis_client.get(key_to_data + "/custom_commute_score")), 2)
        score_pd = pd.DataFrame(score_data, index=[a])

        # travel info for downtown
        downtown_travel = {'Downtown Travel': redis_client.get(key_to_data + "/downtown")}
        dt_pd=pd.DataFrame(downtown_travel, index=[a])

        honestdoor_data.columns = HONESTDOOR_COLS
        honestdoor_data.index = [a] * honestdoor_data.shape[0]
        merged_data = pd.concat([listing_data, honestdoor_data.head(1), score_pd, dt_pd], axis=1)
        dataframes.append(merged_data)
    df = pd.concat(dataframes)
    df['photo'] = df['photo_url']
    df['DateSold'] = df['DateSold'].dt.date
    df['address'] = df['address'].apply(lambda x: x.split('|')[0])

    df.drop(columns='photo_url', inplace=True)

    return df


namespace = 'house-search'
house_df_default = pull_redis(r_dic.redis)
options = {}
options['type'] = list(house_df_default['type'].unique())
options['price_max'] = house_df_default['price'].max()
options['price_min'] = house_df_default['price'].min()

options['transit_time_max'] = 180

class ReactiveDashboard(param.Parameterized):
    title = pn.pane.Markdown("# Smart House Search")
    pins = param.List(default=[])
    lat_longs = param.List(default=[])
    # house_df = get_dummy_house_df()
    house_df = house_df_default
    hover = HoverTool(tooltips=TOOLTIPS)
    details_area = pn.pane.Markdown("# Details")
    price_range = get_price_range()
    minimum_price = param.Selector(objects=list(price_range))
    maximum_price = param.Selector(objects=list(price_range), default=price_range[-1])
    price_slider = param.Range(label='Price range',
                               default=(options['price_min'], options['price_max']), bounds=(0, options['price_max']),
                               )
    rooms_slider = param.Range(label='Bedrooms',
                               default=(0, 7), bounds=(0, 7))
    bathrooms_slider = param.Range(label = 'Bathrooms',
                                   default=(0, 7), bounds=(0, 7))
    type = param.ListSelector(label='Type of property',
                              default=options['type'], objects=options['type'])
    transit_time = param.Range(label='Transit time [mins]',
                               default = (0,options['transit_time_max']), bounds=(0, options['transit_time_max']))

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
        # Create a bokeh figure and source here:

        # range bounds supplied in web mercator coordinates
        xrange = (df_filtered['easting'].round(decimals=2).min()*0.999,
                  df_filtered['easting'].round(decimals=2).max()*1.001)
        yrange = (df_filtered['northing'].round(decimals=2).min()*0.999,
                  df_filtered['northing'].round(decimals=2).max()*1.001)
        df_source = ColumnDataSource(df_filtered)
        tools = [ResetTool(), PanTool(), WheelZoomTool()]
        p = figure(x_range=xrange,
                   y_range=yrange,
                   x_axis_type="mercator", y_axis_type="mercator",
                   plot_width=600, plot_height=600,
                   tools=tools
                   )
        p.add_tile(OSM_tile_source)
        print(df_filtered.shape)
        circle_renderer = p.circle(x='easting', y='northing',
                                   fill_color='midnightblue',
                                   fill_alpha=0.95,
                                   hover_fill_color='firebrick',
                                   line_alpha=0.91,
                                   source=df_source,
                                   size=10,
                                   # hover_line_color='black',
                                   line_width=0)
        circle_renderer.selection_glyph = Circle(fill_color="firebrick", line_color=None)
        circle_renderer.nonselection_glyph = Circle(fill_color="midnightblue", line_color=None)
        tool_circle_hover = HoverTool(renderers=[circle_renderer],
                                      tooltips=TOOLTIPS)
        tool_circle_tap = TapTool(renderers=[circle_renderer])
        p.add_tools(tool_circle_hover)
        p.add_tools(tool_circle_tap)



        def callback_id(attr, old, new):
            print('Indices Hello!')
            print(df_filtered.iloc[new[0],:])
        df_source.selected.on_change('indices',callback_id)

        return p

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
        if 'Custom Commute' in display_df:
            ret = display_df[['photo', 'price', 'DateSold', 'PriceLastSold', 'Assessment Price',
                              'bedrooms', 'bathrooms', 'size', 'lot_size', 'type', 'stories', 'Custom Commute',
                              'Downtown Commute']]
        else:
            ret = display_df[['photo', 'price', 'DateSold', 'PriceLastSold', 'Assessment Price',
                              'bedrooms', 'bathrooms', 'size', 'lot_size', 'type', 'stories', 'Downtown Commute']]

        return ret

    @pn.depends("stream", watch=False)
    def distance_df(self, x, y):
        lat = easting_northing_to_lon_lat(x, y)[1]
        long = easting_northing_to_lon_lat(x, y)[0]
        self.lat_longs.append(['enter name', lat, long])
        df = pd.DataFrame(self.lat_longs, columns=['Name', 'Latitude', 'Longitude']).dropna().style.hide_index()
        return pn.widgets.Tabulator(df.data, pagination='remote', page_size=10, sizing_mode='scale_both',
                                    show_index=False)

    @pn.depends("stream", "pins")
    def location(self, x, y):
        if x and y:
            self.pins.append([x, y])
        return self.house_plot

    def panel(self):
        result = bootstrap
        price_slider = pn.widgets.RangeSlider.from_param(self.param.price_slider, step=10000,format='0.0a')
        result.sidebar.append(price_slider)
        result.sidebar.append(self.param.rooms_slider)
        result.sidebar.append(self.param.bathrooms_slider)
        result.sidebar.append(self.param.type)
        result.sidebar.append(self.param.transit_time)


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
        bootstrap.main.append(pn.Card(self.details_area,title='Details'))

        return result


res = ReactiveDashboard(name="").panel()
res.servable()
