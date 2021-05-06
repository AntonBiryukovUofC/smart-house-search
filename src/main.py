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
from bokeh.models import HoverTool
from vega_datasets import data as vds
import param
from utils import get_price_range, get_dummy_house_df

from constants import CSS_CLASS_CARD

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
    # price_slider = pn.widgets.RangeSlider(start=0, end=house_df['price'].max(), name='Price', format=PrintfTickFormatter(format='$ %d'))
    # rooms_slider = pn.widgets.RangeSlider(start=0, end=house_df['bedrooms'].max(), name='Bedrooms', format=PrintfTickFormatter(format='$ %d'))

    def get_easting_northing(self):
        self.house_df['easting'] = self.house_df.apply(lambda x: lon_lat_to_easting_northing(x['lon'], x['lat'])[0],
                                                       axis=1)
        self.house_df['northing'] = self.house_df.apply(lambda x: lon_lat_to_easting_northing(x['lon'], x['lat'])[1],
                                                        axis=1)

    @pn.depends('price_slider', 'rooms_slider', 'pins', watch=False)
    def house_plot(self):
        map_background = hv.element.tiles.OSM().opts(width=600, height=550)

        if 'northing' not in self.house_df.columns:
            self.get_easting_northing()

        df_filtered = self.house_df[
            (self.house_df['price'] <= self.price_slider[1]) & (self.house_df['price'] >= self.price_slider[0])]
        df_filtered = df_filtered[
            (df_filtered['bedrooms'] <= self.rooms_slider[1]) & (df_filtered['bedrooms'] >= self.rooms_slider[0])]
        house_points = hv.Points(df_filtered, ['easting', 'northing'], ['price']).opts(tools=[self.hover, 'tap'],
                                                                                       alpha=0.7,
                                                                                       hover_fill_alpha=0.4, size=10,
                                                                                       width=600,
                                                                                       height=550)
        if self.pins:
            pin_points = hv.Points(self.pins).opts(color='r', size=10, alpha=0.7)
            return map_background * house_points * pin_points
        else:
            return map_background * house_points

    def filter_df(self):
        if 'northing' not in self.house_df.columns:
            self.get_easting_northing()

        display_df = self.house_df[
            (self.house_df['price'] <= self.maximum_price) & (self.house_df['price'] >= self.minimum_price)].drop(columns=['lat', 'lon', 'easting', 'northing'])
        display_df = display_df.set_index('address')
        return display_df

    def location(self, x, y):
        self.pins.append([x, y])
        # return self.house_plot

    def panel(self):
        result = bootstrap
        result.sidebar.append(self.param.price_slider)
        result.sidebar.append(self.param.rooms_slider)

        df_widget = pn.widgets.Tabulator(self.filter_df())
        df_widget.add_filter(self.param.price_slider, 'price')
        df_widget.add_filter(self.param.rooms_slider, 'bedrooms')

        houses = self.house_plot
        layout = pn.Row(houses, df_widget)

        # stream_map = hv.streams.Tap(source=houses, x=np.nan, y=np.nan)
        # pn.bind(self.location, x=stream_map.param.x, y=stream_map.param.y)
        # layout = pn.Row(houses, df_widget, pn.bind(self.location, x=stream_map.param.x, y=stream_map.param.y))

        bootstrap.main.append(layout)



        return result


res = ReactiveDashboard(name="").panel()
pn.serve({"panel_main": res}, port=5006)
