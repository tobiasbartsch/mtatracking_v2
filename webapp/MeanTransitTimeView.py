from holoviews.streams import Stream
import param
from holoviews.streams import Pipe
import colorcet as cc
import numpy as np
import holoviews as hv
hv.extension("bokeh")
from bokeh.models import HoverTool


class MeanTransitTimes(Stream):

    # class variables
    direction_list = ['N', 'S']
    lines_list = ['1', '2', '3', '4', '5', '6', '7',
                  'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H',
                  'J', 'L', 'M', 'N', 'Q', 'R', 'SI', 'W']
    # Selector class variables (parameters) for holoviews panel
    direction = param.ObjectSelector(objects=direction_list)
    line = param.ObjectSelector(objects=lines_list)

    def callback(self, data):
        (linedef,
         latest_transit_times,
         best_transit_times,
         station_names) = data

        segs, stations, station_names_hv = self.makeHvObjects(
            linedef,
            latest_transit_times,
            best_transit_times,
            station_names
            )
        lo = self.makeHvLayout(segs, stations, station_names_hv)
        return lo

    def __init__(self, data, def_line='5', def_dir='N'):
        '''initialize a MeanTransitTime object

        Args:
            data = (linedef,
                    latest_transit_times,
                    best_transit_times,
                    station_names)
        '''
        Stream.__init__(self)
        self.data = data
        self.pipe = Pipe(data=[])
        self.direction = def_dir
        self.line = def_line
        direct = self.direction
        line = self.line
        if not line or not direct:
            k = '5 N'
        else:
            k = line + ' ' + direct

        data_selected = (self.data[0][k], self.data[1][k],
                         self.data[2][k], self.data[3][k])

        self.plt = hv.DynamicMap(self.callback, streams=[self.pipe]).opts(framewise=True)
        self.pipe.send(data_selected)

    def makeHvObjects(self, linedef, latest_transit_times,
                      best_transit_times, station_names):
        numstations = len(linedef)
        numsegments = numstations - 1
        width = 18 / (numstations/2 + numsegments)
        station_pairs = zip(linedef[:-1], linedef[1:])
        keys = [p[0] + ' ' + p[1] for p in station_pairs]

        levels = {k: best_transit_times[k][0] - latest_transit_times[k][0]
                  if k in best_transit_times and k in latest_transit_times
                  else 0 for k in keys}
        segs = [{('x', 'y'): self.rectangle(
            0, x * 1.5 * width - 9, width=0.3, height=width),
            'level': levels[k]} for x, k in enumerate(keys)]
        stations = [{('x', 'y'): hv.Ellipse(
            width/2, x * 1.5 * width - width / 4 - 9, (2*width, width / 2))
                .array()} for x in range(numstations)]
        station_names_hv = [hv.Text(1, x * 1.5 * width - width / 4 - 9, station)
                            .opts(text_align='left', text_font='Arial',
                            text_font_size='14px', text_color='white') for x, station
                            in enumerate(station_names)]
        return segs, stations, station_names_hv

    def makeHvLayout(self, segs, stations, station_names):

        polys = hv.Polygons(segs, vdims='level', label=self.line + ' ' + self.direction).redim.range(level=(-120, 0))

        layout = polys.opts(
                color='level',
                line_width=0,
                padding=0.1,
                height=1000,
                cmap=cc.CET_L4[125:5:-1],
                xlim=(-1, 19),
                ylim=(-10, 10),
                tools=[MeanTransitTimes.hover_delays])\
            * hv.Polygons(stations).opts(
                fill_color=None,
                line_width=2,
                line_color='grey',
                xaxis=None,
                yaxis=None)\
            * hv.Overlay(station_names)
        return layout

    def rectangle(self, x=0, y=0, width=0.5, height=0.2):
        return np.array([(x, y),
                         (x+width, y),
                         (x+width, y+height),
                         (x, y+height)])

    def view(self):
        return self.plt

    # https://panel.pyviz.org/user_guide/Param.html
    @param.depends('direction', 'line', watch=True)
    def update(self):
        direct = self.direction
        line = self.line
        if not direct or not line:
            k = '5 N'
        else:
            k = line + ' ' + direct
        if self.data:
            data_selected = (self.data[0][k], self.data[1][k],
                             self.data[2][k], self.data[3][k])
            self.pipe.send(data_selected)
        # add on parameter changed logic here

    hover_delays = HoverTool(tooltips=[('Delay (seconds)', "@level")])
