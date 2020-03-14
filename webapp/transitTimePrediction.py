import holoviews as hv
import pandas as pd


def makePlot():
    df_test = pd.read_csv('static/GAP_to_125TestData.csv')
    pred_vs_actual_transit_time = hv.Scatter(
        df_test, ['x', 'y'], label='My model').opts(
                        width=600, height=600,
                        xlabel='actual transit time [min]',
                        ylabel='predicted transit time [min]',
                        size=4, color='lightblue', alpha=0.9,
                        xlim=(33, 66), ylim=(33, 66))\
        * \
        hv.Scatter(df_test, ['x', 'y_mta'], label='MTA/Google Maps').opts(
                        width=600, height=600,
                        xlabel='actual transit time [min]',
                        ylabel='predicted transit time [min]',
                        size=4, color='orange', alpha=0.9,
                        xlim=(33, 66), ylim=(33, 66))\
        * \
        hv.Curve(([33, 66], [33, 66])).opts(color='white')

    layout = pred_vs_actual_transit_time.opts(
        legend_position='top_left',
        title='Grand Army Plaza to 125th St (2 line)')
    plot = hv.render(layout, backend='bokeh')

    return plot
