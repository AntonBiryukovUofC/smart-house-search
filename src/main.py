import panel as pn
import numpy as np
import holoviews as hv
import hvplot.pandas
from panel.template import DarkTheme
import pandas as pd
import geoviews as gv
from holoviews.util.transform import easting_northing_to_lon_lat, lon_lat_to_easting_northing
from bokeh.models.formatters import PrintfTickFormatter
from bokeh.models.formatters import NumeralTickFormatter
from bokeh.models import HoverTool, TapTool
from bokeh.events import Tap
from vega_datasets import data as vds
import param
from utils import get_price_range, get_dummy_house_df
from redis_dict import RedisDict
from constants import CSS_CLASS_CARD

r_dic = RedisDict(namespace='house-search')
bootstrap = pn.template.BootstrapTemplate(title='Smart House Search')
pn.config.sizing_mode = "stretch_width"
pn.extension(raw_css=[CSS_CLASS_CARD])


class ReactiveDashboard(param.Parameterized):
    title = pn.pane.Markdown("# Smart House Search")
    pins = param.List(default=[])
    house_df = get_dummy_house_df()
    tooltips = [("Price", "@price")]
    hover = HoverTool(tooltips=tooltips)
    price_range = get_price_range()
    minimum_price = param.Selector(objects=list(price_range))
    maximum_price = param.Selector(objects=list(price_range), default=price_range[-1])
    price_slider = param.Range(default=(0, house_df['price'].max()), bounds=(0, house_df['price'].max()))
    rooms_slider = param.Range(default=(0, house_df['bedrooms'].max()), bounds=(0, house_df['bedrooms'].max()))
    map_background = hv.element.tiles.OSM().opts(width=600, height=550)
    stream = hv.streams.Tap(source=map_background, x=np.nan, y=np.nan)

    def get_easting_northing(self):
        self.house_df['easting'] = self.house_df.apply(lambda x: lon_lat_to_easting_northing(x['lon'], x['lat'])[0],
                                                       axis=1)
        self.house_df['northing'] = self.house_df.apply(lambda x: lon_lat_to_easting_northing(x['lon'], x['lat'])[1],
                                                        axis=1)

    @pn.depends('price_slider', 'rooms_slider', 'pins', watch=False)
    def house_plot(self):


        if 'northing' not in self.house_df.columns:
            self.get_easting_northing()

        df_filtered = self.house_df[
            (self.house_df['price'] <= self.price_slider[1]) & (self.house_df['price'] >= self.price_slider[0])]
        df_filtered = df_filtered[
            (df_filtered['bedrooms'] <= self.rooms_slider[1]) & (df_filtered['bedrooms'] >= self.rooms_slider[0])]
        house_points = hv.Points(df_filtered, ['easting', 'northing'], ['price']).opts(tools=[self.hover, 'tap'],
                                                                                       alpha=0.7,
                                                                                       hover_fill_alpha=0.4, size=20,
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
            (self.house_df['price'] <= self.maximum_price) & (self.house_df['price'] >= self.minimum_price)].drop(columns=['lat', 'lon', 'easting', 'northing'])
        display_df = display_df.set_index('address')
        return display_df

    @pn.depends("stream")
    def location(self, x, y):
        if x and y:
            self.pins.append([x, y])
        return self.house_plot

    def pin_dataframe(self):
        pins = np.array(self.pins)


    def panel(self):
        result = bootstrap
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
pn.serve({"panel_main": res}, port=5006)
