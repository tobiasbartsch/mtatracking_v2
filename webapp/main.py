from threading import Thread

from flask import Flask, render_template
from tornado.ioloop import IOLoop

from bokeh.embed import server_document, components
from bokeh.layouts import column
from bokeh.models import ColumnDataSource, Slider
from bokeh.plotting import figure
from bokeh.sampledata.sea_surface_temperature import sea_surface_temperature
from bokeh.server.server import Server
from bokeh.themes import Theme

import holoviews as hv
hv.extension('bokeh')
from bokeh.themes import Theme

from getMeanTransitData import getData, initDB
from MeanTransitTimeView import MeanTransitTimes
from transitTimePrediction import makePlot
from queryMTAnow import makePrediction
import parambokeh
import panel as pp

app = Flask(__name__)

session = initDB()
data = getData(session)
mttv = MeanTransitTimes(data, def_line='Q', def_dir='N')
mttv2 = MeanTransitTimes(data, def_line='4', def_dir='N')


hv.renderer('bokeh').theme = Theme(filename="theme.yaml")


def predict_transit_time(doc):
    panel = pp.Row(makePlot())
    doc.theme = Theme(filename="theme_scatter.yaml")
    return panel.server_doc(doc=doc)


def mean_transit_times(doc):
    parambokeh.Widgets(
        mttv,
        on_init=True,
        mode='server')
    panel = pp.Row(pp.Column(mttv, mttv.view), pp.Column(mttv2, mttv2.view))
    print(mttv)
    # doc.theme = Theme(filename="theme.yaml")
    return panel.server_doc(doc=doc)


@app.route('/systematic_delays', methods=['GET'])
def systematic_delays():
    script = server_document('http://localhost:5006/mtt')
    return render_template("systematic_delays.html.j2", script=script)


@app.route('/', methods=['GET'])
def realtime_delays():
    script = server_document('http://localhost:9998')
    return render_template("realtime_delays.html.j2", script=script)


@app.route('/transit_time_predicition', methods=['GET'])
def transit_time_prediction():
    script = server_document('http://localhost:5006/predict')
    my_prediction, mta_prediction = makePrediction(session)
    return render_template("transit_time_prediction.html.j2",
                           script=script,
                           my_prediction=my_prediction,
                           mta_prediction=mta_prediction)


@app.route('/about', methods=['GET'])
def about():
    return render_template("about.html.j2")


def bk_worker():
    # Can't pass num_procs > 1 in this configuration. If you need to run
    # multiple processes, see e.g. flask_gunicorn_embed.py
    server = Server({'/mtt': mean_transit_times,
                    '/predict': predict_transit_time},
                    io_loop=IOLoop(),
                    allow_websocket_origin=["localhost:8000", "*"])
    server.start()
    server.io_loop.start()


Thread(target=bk_worker).start()

if __name__ == '__main__':
    print('Opening single process Flask app with embedded Bokeh application on http://localhost:8000/')
    print()
    print('Multiple connections may block the Bokeh app in this configuration!')
    print('See "flask_gunicorn_embed.py" for one way to run multi-process')
    app.run(port=8000)
